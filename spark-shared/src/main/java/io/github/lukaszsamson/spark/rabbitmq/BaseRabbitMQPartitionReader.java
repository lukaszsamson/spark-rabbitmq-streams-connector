package io.github.lukaszsamson.spark.rabbitmq;

import com.rabbitmq.stream.*;
import org.apache.spark.SparkEnv;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.metric.CustomTaskMetric;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.util.LongAccumulator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Push-to-pull bridge that reads messages from a RabbitMQ stream consumer
 * and presents them as Spark {@link InternalRow}s.
 *
 * <p>The RabbitMQ stream client is push-based (messages arrive via
 * {@link MessageHandler}). Spark readers are pull-based ({@link #next()}/{@link #get()}).
 * This reader bridges the two by pushing messages into a bounded
 * {@link BlockingQueue} and pulling from it on demand.
 *
 * <p>This is the base implementation shared across all Spark versions.
 * Spark-version-specific subclasses (e.g. for {@code SupportsRealTimeRead} in
 * Spark 4.1) extend this class and add only the version-specific methods.
 */
class BaseRabbitMQPartitionReader implements PartitionReader<InternalRow> {

    static final Logger LOG = LoggerFactory.getLogger(BaseRabbitMQPartitionReader.class);
    private static final AtomicBoolean SAC_SPECULATION_WARNED = new AtomicBoolean(false);
    /** Executor-side tail probe timeout — longer than driver-side (250ms) to allow for
     *  cold-start consumer setup (new environment, address resolution, etc.). */
    static final long EXECUTOR_TAIL_PROBE_WAIT_MS = 5_000L;
    private static final long CLOSED_CHECK_INTERVAL_MS = 100L;
    private static final long CONSUMER_INIT_RETRY_WINDOW_MS = 10_000L;
    private static final long MIN_TAIL_PROBE_CACHE_WINDOW_MS = 25L;
    private static final long MAX_TAIL_PROBE_CACHE_WINDOW_MS = 250L;
    private static final long MIN_STATS_TAIL_CACHE_WINDOW_MS = 25L;
    private static final long MAX_STATS_TAIL_CACHE_WINDOW_MS = 250L;
    final String stream;
    final long startOffset;
    volatile long endOffset;
    final ConnectorOptions options;
    final boolean useConfiguredStartingOffset;
    final String messageSizeTrackerScope;
    final LongAccumulator messageSizeBytesAccumulator;
    final LongAccumulator messageSizeRecordsAccumulator;
    final boolean lateBindEndOffset;
    final MessageToRowConverter converter;
    final MessagePostFilter postFilter;

    final BlockingQueue<QueuedMessage> queue;
    final AtomicReference<Throwable> consumerError = new AtomicReference<>();
    final AtomicBoolean consumerClosed = new AtomicBoolean(false);
    final AtomicBoolean closeCalled = new AtomicBoolean(false);
    final AtomicBoolean singleActiveConsumerStateKnown = new AtomicBoolean(false);
    final AtomicBoolean singleActiveConsumerActive = new AtomicBoolean(true);

    boolean pooledEnvironment = false;
    volatile Environment environment;
    volatile Consumer consumer;
    volatile InternalRow currentRow;
    volatile long lastEmittedOffset = -1;
    // Broker chunk delivery can make this temporarily lower than startOffset;
    // reconnect resume clamps observed-only progress back to startOffset.
    // AtomicLong because the consumer callback thread (post-filter drop path) and
    // the Spark task thread both advance this value; non-atomic read-test-write
    // can lose updates and let a stale value satisfy hasReachedPlannedEnd().
    AtomicLong lastObservedOffset = new AtomicLong(-1);
    boolean filteredTailReached = false;
    volatile boolean finished = false;
    long lastTailProbeNanos = -1L;
    long lastTailProbeOffset = -1L;
    long lastStatsTailNanos = -1L;
    long lastStatsTailOffset = -1L;

    enum TerminationDecision {
        CONTINUE,
        COMPLETE,
        THROW
    }

    // Task-level metric counters
    volatile long recordsRead = 0;
    volatile long payloadBytesRead = 0;
    volatile long estimatedWireBytesRead = 0;
    long pollWaitMs = 0;
    long offsetOutOfRange = 0;
    long dataLoss = 0;

    /**
     * A message queued by the consumer callback for pull-based reading.
     * Includes the {@link MessageHandler.Context} so that {@code processed()}
     * can be called on the pull side, tying credit grants to consumption rate.
     *
     * <p>{@code skipMarker} entries carry a dropped offset (post-filter reject or
     * out-of-range) so the pull loop can advance {@code lastObservedOffset} in
     * arrival order. They MUST NOT be treated as a Spark row and MUST NOT count
     * toward byte/record budgets. For skip markers, {@code message} and
     * {@code context} are {@code null}.
     */
    record QueuedMessage(Message message, long offset, long chunkTimestampMillis,
                         MessageHandler.Context context, boolean skipMarker) {

        QueuedMessage(Message message, long offset, long chunkTimestampMillis,
                      MessageHandler.Context context) {
            this(message, offset, chunkTimestampMillis, context, false);
        }

        static QueuedMessage skip(long offset) {
            return new QueuedMessage(null, offset, 0L, null, true);
        }
    }

    BaseRabbitMQPartitionReader(RabbitMQInputPartition partition, ConnectorOptions options) {
        this.stream = partition.getStream();
        this.startOffset = partition.getStartOffset();
        this.endOffset = partition.getEndOffset();
        this.options = options;
        this.useConfiguredStartingOffset = partition.isUseConfiguredStartingOffset();
        this.messageSizeTrackerScope = partition.getMessageSizeTrackerScope();
        this.messageSizeBytesAccumulator = partition.getMessageSizeBytesAccumulator();
        this.messageSizeRecordsAccumulator = partition.getMessageSizeRecordsAccumulator();
        this.lateBindEndOffset = partition.isLateBindEndOffset();
        this.converter = new MessageToRowConverter(options.getMetadataFields());
        this.postFilter = createPostFilter(options);
        this.queue = new ArrayBlockingQueue<>(options.getQueueCapacity());
    }

    @Override
    public boolean next() throws IOException {
        if (finished || closeCalled.get()) {
            finished = true;
            return false;
        }

        // Fast-path: if we already observed or emitted the final in-range offset, stop immediately.
        // lastObservedOffset is needed when post-filter drops tail records.
        if (hasReachedPlannedEnd()) {
            finished = true;
            return false;
        }

        // Lazy initialization: create consumer on first call
        if (consumer == null) {
            try {
                initConsumerWithRetry();
            } catch (Exception e) {
                if (!options.isFailOnDataLoss() && isMissingStreamException(e)) {
                    LOG.warn("Unable to initialize consumer for stream '{}' because stream/partition " +
                                    "is missing; completing split because failOnDataLoss=false",
                            stream);
                    dataLoss++;
                    finished = true;
                    return false;
                }
                throw new IOException("Failed to initialize consumer for stream '" + stream + "'", e);
            }
        }

        long totalWaitMs = 0;
        long pollTimeoutMs = options.getPollTimeoutMs();
        long maxWaitMs = options.getMaxWaitMs();
        long pollSliceMs = Math.max(1L, Math.min(pollTimeoutMs, CLOSED_CHECK_INTERVAL_MS));
        long waitStartNanos = System.nanoTime();

        while (true) {
            if (finished || closeCalled.get()) {
                finished = true;
                return false;
            }

            if (hasReachedPlannedEnd()) {
                finished = true;
                return false;
            }

            // Check for consumer errors
            Throwable error = consumerError.get();
            if (error != null) {
                throw new IOException("Consumer error on stream '" + stream + "'", error);
            }
            if (isSingleActiveConsumerKnownInactive() && queue.isEmpty()) {
                throw terminationFailure(false, false, true, 0L);
            }

            QueuedMessage qm;
            try {
                long pollStart = System.nanoTime();
                qm = queue.poll(pollSliceMs, TimeUnit.MILLISECONDS);
                pollWaitMs += TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - pollStart);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                LOG.debug("Interrupted while reading from stream '{}'; finishing split", stream);
                finished = true;
                return false;
            }

            if (qm == null) {
                if (consumerClosed.get()) {
                    boolean dataLossProven = isPlannedRangeNoLongerReachableDueToDataLoss();
                    TerminationDecision decision = decideTermination(
                            false, dataLossProven, false, false);
                    switch (decision) {
                        case COMPLETE:
                            LOG.warn("Consumer for stream '{}' closed and planned range [{}, {}) is no longer " +
                                            "reachable; completing split because failOnDataLoss=false",
                                    stream, startOffset, endOffset);
                            dataLoss++;
                            finished = true;
                            return false;
                        case THROW:
                            throw terminationFailure(false, dataLossProven, false, 0L);
                        case CONTINUE:
                            break;
                    }
                    throw new IOException(
                            "Consumer for stream '" + stream + "' closed before reaching target end offset "
                                    + endOffset + ". Last emitted offset: " + lastEmittedOffset);
                }
                if (canCompleteEmptyPlannedRange() && hasStreamTailReachedPlannedEnd()) {
                    finished = true;
                    return false;
                }
                if (isSingleActiveConsumerKnownInactive()) {
                    throw terminationFailure(false, false, true, 0L);
                }
                totalWaitMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - waitStartNanos);
                if (totalWaitMs >= maxWaitMs) {
                    boolean sacInactive = isSingleActiveConsumerKnownInactive();
                    boolean plannedEndReached = canCompleteEmptyPlannedRange()
                            && hasStreamTailReachedPlannedEnd();
                    boolean dataLossProven = isPlannedRangeNoLongerReachableDueToDataLoss();
                    boolean rangeEndedBeforeTarget = !options.isFailOnDataLoss()
                            && plannedRangeEndedBeforeTarget();
                    TerminationDecision decision = rangeEndedBeforeTarget
                            ? TerminationDecision.COMPLETE
                            : decideTermination(true, dataLossProven, plannedEndReached, sacInactive);
                    switch (decision) {
                        case COMPLETE:
                            if (dataLossProven || rangeEndedBeforeTarget) {
                                LOG.warn("Reached maxWaitMs={} on stream '{}' and planned range [{}, {}) is no longer " +
                                                "reachable after data loss/recreation; completing split because failOnDataLoss=false",
                                        maxWaitMs, stream, startOffset, endOffset);
                                dataLoss++;
                            } else {
                                LOG.warn("Reached maxWaitMs={} while reading filtered/timestamp stream '{}'; " +
                                                "tail indicates planned end reached at lastObservedOffset={} for endOffset={}",
                                        maxWaitMs, stream, lastObservedOffset.get(), endOffset);
                            }
                            finished = true;
                            return false;
                        case THROW:
                            throw terminationFailure(true, dataLossProven, sacInactive, totalWaitMs);
                        case CONTINUE:
                            break;
                    }
                    throw new IOException(
                            "Timed out waiting for messages from stream '" + stream +
                                    "'. Last emitted offset: " + lastEmittedOffset +
                                    ", target end offset: " + endOffset +
                                    ". Waited " + totalWaitMs + "ms (maxWaitMs=" + maxWaitMs + ")");
                }
                LOG.debug("Poll timeout on stream '{}', waited {}ms so far", stream, totalWaitMs);
                continue;
            }


            if (qm.skipMarker()) {
                // Order-preserving post-filter drop: advance observed and continue.
                // Does not count as a Spark row, does not consume credit
                // (already granted on the callback side via context.processed()).
                advanceObserved(qm.offset());
                continue;
            }

            if (consumerClosed.get()
                    && shouldDetectOffsetGapsAfterConsumerClosure()
                    && qm.offset() >= startOffset) {
                long expectedNextOffset = expectedNextObservedOffset();
                if (qm.offset() > expectedNextOffset) {
                    String message = "Detected offset gap after consumer closure on stream '"
                            + stream + "': expected offset " + expectedNextOffset
                            + " but observed " + qm.offset() + " for planned range ["
                            + startOffset + ", " + endOffset + ")";
                    if (options.isFailOnDataLoss()) {
                        throw new IOException(message);
                    }
                    LOG.warn("{}; completing split because failOnDataLoss=false", message);
                    dataLoss++;
                    finished = true;
                    return false;
                }
            }

            // Stop at end offset (exclusive) without granting additional credit.
            if (isAtOrBeyondPlannedEnd(qm.offset())) {
                finished = true;
                return false;
            }

            // Notify flow strategy that this message has been consumed (pull-side).
            // This ties credit grants to consumption rate rather than enqueue rate,
            // providing natural backpressure when the pull side is slow.
            qm.context().processed();

            advanceObserved(qm.offset());

            // Skip messages before start offset (can happen with timestamp-based starting)
            if (qm.offset() < startOffset) {
                continue;
            }

            // De-dup on reconnection: skip already-emitted offsets
            if (qm.offset() <= lastEmittedOffset) {
                continue;
            }

            if (shouldSkipByTimestamp(qm.chunkTimestampMillis())) {
                continue;
            }

            currentRow = converter.convert(
                    qm.message(), stream, qm.offset(), qm.chunkTimestampMillis());
            lastEmittedOffset = qm.offset();
            recordsRead++;
            payloadBytesRead += MessageSizeEstimator.payloadBytes(qm.message());
            estimatedWireBytesRead += MessageSizeEstimator.estimatedWireBytes(qm.message());
            return true;
        }
    }

    @Override
    public InternalRow get() {
        return currentRow;
    }

    @Override
    public void close() throws IOException {
        if (!closeCalled.compareAndSet(false, true)) {
            return;
        }
        finished = true;
        queue.clear(); // Immediately unblock Netty callback
        // Report actual message sizes for running average estimation.
        // Prefer Spark accumulators (driver-visible across executors); fall back to JVM-local tracker.
        if (messageSizeBytesAccumulator != null && messageSizeRecordsAccumulator != null) {
            messageSizeBytesAccumulator.add(payloadBytesRead);
            messageSizeRecordsAccumulator.add(recordsRead);
        } else {
            MessageSizeTracker.record(messageSizeTrackerScope, payloadBytesRead, recordsRead);
        }
        try {
            if (consumer != null) {
                consumer.close();
                consumer = null;
            }
        } catch (Exception e) {
            LOG.warn("Error closing consumer for stream '{}'", stream, e);
        }
        if (pooledEnvironment) {
            EnvironmentPool.getInstance().release(options);
            environment = null;
        } else {
            try {
                if (environment != null) {
                    environment.close();
                    environment = null;
                }
            } catch (Exception e) {
                LOG.warn("Error closing environment for stream '{}'", stream, e);
            }
        }
    }

    @Override
    public CustomTaskMetric[] currentMetricsValues() {
        return new CustomTaskMetric[]{
                RabbitMQSourceMetrics.taskMetric(RabbitMQSourceMetrics.RECORDS_READ, recordsRead),
                RabbitMQSourceMetrics.taskMetric(
                        RabbitMQSourceMetrics.PAYLOAD_BYTES_READ, payloadBytesRead),
                RabbitMQSourceMetrics.taskMetric(
                        RabbitMQSourceMetrics.ESTIMATED_WIRE_BYTES_READ, estimatedWireBytesRead),
                RabbitMQSourceMetrics.taskMetric(RabbitMQSourceMetrics.POLL_WAIT_MS, pollWaitMs),
                RabbitMQSourceMetrics.taskMetric(
                        RabbitMQSourceMetrics.OFFSET_OUT_OF_RANGE, offsetOutOfRange),
                RabbitMQSourceMetrics.taskMetric(RabbitMQSourceMetrics.DATA_LOSS, dataLoss),
        };
    }

    void initConsumer() {
        environment = EnvironmentPool.getInstance().acquire(options);
        pooledEnvironment = true;

        // Detect offset-out-of-range (retention truncation)
        try {
            StreamStats stats = environment.queryStreamStats(stream);
            long firstAvailable = stats.firstOffset();
            if (startOffset < firstAvailable) {
                handleStartOffsetOutOfRange(firstAvailable);
            }
        } catch (IllegalStateException e) {
            throw e;
        } catch (Exception e) {
            // Non-fatal: cannot check, proceed without metric
            LOG.debug("Cannot check offset range for stream '{}': {}", stream, e.getMessage());
        }

        // Late-bind endOffset for batch reads with endingOffsets=latest.
        // The driver passes Long.MAX_VALUE as a sentinel; resolve the actual
        // tail on the executor at read time (Kafka-style late binding).
        if (lateBindEndOffset && endOffset == Long.MAX_VALUE) {
            long resolved = probeTailFromExecutor(environment, stream);
            if (resolved <= startOffset) {
                LOG.info("Late-bound endOffset for stream '{}' resolved to {} " +
                        "(at or before startOffset {}); nothing to read",
                        stream, resolved, startOffset);
                finished = true;
                return;
            }
            endOffset = resolved;
            LOG.info("Late-bound endOffset for stream '{}': {}", stream, endOffset);
        }

        OffsetSpecification offsetSpec = resolveOffsetSpec();
        int effectiveInitialCredits = resolveEffectiveInitialCredits();
        LOG.debug("Initializing consumer for stream '{}' with planned range [{}, {}), "
                        + "offsetSpec={}, useConfiguredStartingOffset={}, initialCredits={}",
                stream, startOffset, endOffset, offsetSpec, useConfiguredStartingOffset,
                effectiveInitialCredits);

        ConsumerBuilder builder = environment.consumerBuilder()
                .stream(stream)
                .offset(offsetSpec)
                .noTrackingStrategy()
                .messageHandler(this::enqueueFromCallback)
                .flow()
                .initialCredits(effectiveInitialCredits)
                .strategy(ConsumerFlowStrategy.creditWhenHalfMessagesProcessed(
                        effectiveInitialCredits))
                .builder();
        builder.subscriptionListener(context -> context.offsetSpecification(
                resolveSubscriptionOffsetSpec(context.offsetSpecification(), offsetSpec)));
        if (options.isSingleActiveConsumer()) {
            warnIfSpeculationIncompatibleWithSac();
            builder.name(resolveSingleActiveConsumerName())
                    .singleActiveConsumer()
                    // SAC with noTrackingStrategy needs an explicit update listener
                    // to provide the activation offset.
                    .consumerUpdateListener(context -> {
                        singleActiveConsumerStateKnown.set(true);
                        singleActiveConsumerActive.set(context.isActive());
                        return context.isActive()
                                ? resolveSingleActiveConsumerActivationOffset(offsetSpec)
                                : OffsetSpecification.none();
                    });
        }

        // State listener for RECOVERING/CLOSED transitions
        builder.listeners(context -> {
            Resource.State from = context.previousState();
            Resource.State to = context.currentState();
            if (to == Resource.State.RECOVERING) {
                LOG.warn("Consumer for stream '{}' is recovering ({}->{})",
                        stream, from, to);
            } else if (to == Resource.State.CLOSED) {
                LOG.warn("Consumer for stream '{}' has closed ({}->{})",
                        stream, from, to);
                consumerClosed.set(true);
            } else {
                LOG.debug("Consumer for stream '{}' state change: {}->{}",
                        stream, from, to);
            }
        });

        // Configure filtering if specified
        if (options.getFilterValues() != null && !options.getFilterValues().isEmpty()) {
            if (postFilter == null) {
                LOG.warn("Broker-side filter configured on stream '{}' without deterministic " +
                                "client post-filter. Bloom-filter false positives may be emitted. " +
                                "Configure '{}' or '{}' to enable deterministic filtering.",
                        stream,
                        ConnectorOptions.FILTER_POST_FILTER_CLASS,
                        ConnectorOptions.FILTER_VALUE_PATH);
            }
            builder.filter()
                    .values(options.getFilterValues().toArray(new String[0]))
                    .matchUnfiltered(options.isFilterMatchUnfiltered())
                    // RabbitMQ stream client requires both filter values and post-filter logic.
                    .postFilter(msg -> true)
                    .builder();
        }

        try {
            consumer = builder.build();
        } catch (NullPointerException e) {
            // Workaround for rabbitmq-stream-java-client bug where Client.subscribe returns null
            // when the connection is closed or stream is deleted concurrently, causing an NPE in
            // ConsumersCoordinator$ClientSubscriptionsManager.add.
            boolean isSubscribeResponseBug = e.getMessage() != null && e.getMessage().contains("subscribeResponse");
            if (!isSubscribeResponseBug) {
                for (StackTraceElement element : e.getStackTrace()) {
                    if (element.getClassName().contains("ConsumersCoordinator$ClientSubscriptionsManager")
                            && element.getMethodName().equals("add")) {
                        isSubscribeResponseBug = true;
                        break;
                    }
                }
            }
            if (isSubscribeResponseBug) {
                throw new StreamDoesNotExistException(stream);
            }
            throw e;
        }

        LOG.info("Opened consumer for stream '{}' with offsets [{}, {})",
                stream, startOffset, endOffset);
    }

    void initConsumerWithRetry() throws Exception {
        long deadlineNanos = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(
                Math.min(options.getMaxWaitMs(), CONSUMER_INIT_RETRY_WINDOW_MS));
        int attempt = 0;
        while (true) {
            resetConsumerInitState();
            try {
                initConsumer();
                return;
            } catch (Exception e) {
                attempt++;
                if (!isTransientConsumerInitFailure(e)
                        || System.nanoTime() >= deadlineNanos
                        || closeCalled.get()) {
                    throw e;
                }
                LOG.warn("Transient failure initializing consumer for stream '{}' on attempt {}; "
                                + "retrying after {}ms: {}",
                        stream, attempt, consumerInitRetryDelayMs(), e.getMessage());
                cleanupAfterFailedConsumerInit();
                sleepBeforeConsumerInitRetry();
            }
        }
    }

    long consumerInitRetryDelayMs() {
        return Math.min(Math.max(options.getPollTimeoutMs(), 100L), 1_000L);
    }

    void sleepBeforeConsumerInitRetry() throws InterruptedException {
        Thread.sleep(consumerInitRetryDelayMs());
    }

    void resetConsumerInitState() {
        consumerClosed.set(false);
        consumerError.set(null);
    }

    void cleanupAfterFailedConsumerInit() {
        try {
            if (consumer != null) {
                consumer.close();
            }
        } catch (Exception closeError) {
            LOG.debug("Error closing partially initialized consumer for stream '{}'", stream, closeError);
        } finally {
            consumer = null;
        }

        if (pooledEnvironment) {
            try {
                EnvironmentPool.getInstance().release(options);
            } catch (RuntimeException releaseError) {
                LOG.debug("Error releasing pooled environment after init failure for stream '{}'",
                        stream, releaseError);
            }
            pooledEnvironment = false;
            environment = null;
            return;
        }

        try {
            if (environment != null) {
                environment.close();
            }
        } catch (Exception closeError) {
            LOG.debug("Error closing environment after init failure for stream '{}'", stream, closeError);
        } finally {
            environment = null;
        }
    }

    boolean isTransientConsumerInitFailure(Throwable failure) {
        Throwable current = failure;
        while (current != null) {
            String message = current.getMessage();
            if (message != null) {
                String lowerMessage = message.toLowerCase(java.util.Locale.ROOT);
                if (lowerMessage.contains("connection is closed")
                        || lowerMessage.contains("locator not available")
                        || lowerMessage.contains("connection reset")
                        || lowerMessage.contains("connection refused")
                        || lowerMessage.contains("broken pipe")) {
                    return true;
                }
            }
            if (current instanceof java.net.ConnectException
                    || current instanceof java.net.SocketException
                    || current instanceof java.util.concurrent.TimeoutException) {
                return true;
            }
            String simpleName = current.getClass().getSimpleName();
            if ("LocatorNotAvailableException".equals(simpleName)
                    || "ConnectionStreamException".equals(simpleName)
                    || "TimeoutStreamException".equals(simpleName)) {
                return true;
            }
            current = current.getCause();
        }
        return false;
    }

    int resolveEffectiveInitialCredits() {
        return computeEffectiveInitialCredits(
                options.getInitialCredits(), options.getQueueCapacity(), startOffset, endOffset);
    }

    /**
     * Resolve the stream tail offset on the executor side for late-bound batch reads.
     * Tries {@code committedOffset()} first (RabbitMQ 4.3+), then falls back to
     * {@code max(committedChunkId() + 1, probeLastMessage + 1)}.
     *
     * <p>{@code committedChunkId() + 1} is a known underestimate (first offset of
     * the last committed chunk + 1), but it serves as a safe lower bound when the
     * probe consumer times out (e.g. slow consumer startup with observation collectors).
     *
     * @return exclusive end offset (last offset + 1), or 0 if the stream appears empty
     */
    static long probeTailFromExecutor(Environment env, String stream) {
        long statsTail = 0;
        long committedChunkFallback = 0;
        try {
            StreamStats stats = env.queryStreamStats(stream);
            try {
                stats.firstOffset();
            } catch (NoOffsetException e) {
                return 0L; // Stream is genuinely empty, bypass probe
            }
            statsTail = BaseRabbitMQMicroBatchStream.resolveTailOffset(stats);
            // Keep committedChunkId()+1 as a last-resort fallback for batch reads.
            // resolveTailOffset intentionally omits +1 to avoid overshoot in streaming,
            // but for batch late-bind we need at least a positive value to attempt reading.
            if (statsTail == 0) {
                try {
                    committedChunkFallback = stats.committedChunkId() + 1;
                } catch (Exception ignored) {
                    // No committed chunk available
                }
            }
        } catch (Exception e) {
            LOG.debug("Cannot query stats for late-bind on stream '{}': {}",
                    stream, e.getMessage());
        }
        // Always probe to get the actual tail — stats alone underestimate.
        // Use the standard first-wait so the extra-drain logic in probeLastMessageOffsetInclusive
        // gets a proper budget (TAIL_PROBE_MAX_TOTAL_WAIT_MS - firstWait). If the first attempt
        // returns -1 (consumer startup too slow), retry with progressively longer timeouts.
        try {
            long[] retryWaitsMs = {
                    BaseRabbitMQMicroBatchStream.TAIL_PROBE_WAIT_MS,   // 250ms — standard
                    1_000L,                                             // 1s    — warm-up retry
                    EXECUTOR_TAIL_PROBE_WAIT_MS                         // 5s    — final attempt
            };
            for (long waitMs : retryWaitsMs) {
                long probed = BaseRabbitMQMicroBatchStream.probeLastMessageOffsetInclusive(
                        env, stream, waitMs) + 1;
                if (probed > 0) {
                    return Math.max(statsTail, probed);
                }
            }
            // All retries failed — use stats, falling back to committedChunkId()+1
            // if resolveTailOffset returned 0 (committedChunkId=0 means data starts
            // at offset 0, not that the stream is empty).
            return Math.max(statsTail, committedChunkFallback);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted while probing tail offset for stream '"
                    + stream + "'", e);
        }
    }

    static int computeEffectiveInitialCredits(
            int configuredInitialCredits,
            int queueCapacity,
            long startOffset,
            long endOffset) {
        int configured = Math.max(1, configuredInitialCredits);
        int capacity = Math.max(1, queueCapacity);
        return Math.min(configured, capacity);
    }

    void enqueueFromCallback(MessageHandler.Context context, Message message) {
        if (isAtOrBeyondPlannedEnd(context.offset())) {
            context.processed();
            return;
        }
        if (shouldDropByPostFilter(message)) {
            // Enqueue an order-preserving skip marker so the pull side can
            // observe the dropped offset in arrival order. Returning credit
            // here (callback thread) keeps backflow correct: the marker itself
            // does not consume credit on the pull side.
            try {
                if (!queue.offer(QueuedMessage.skip(context.offset()),
                        options.getCallbackEnqueueTimeoutMs(), TimeUnit.MILLISECONDS)) {
                    consumerError.compareAndSet(null,
                            new IOException("Queue full: timed out enqueuing skip marker " +
                                    "at offset " + context.offset() +
                                    " on stream '" + stream + "'"));
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                consumerError.compareAndSet(null,
                        new IOException("Interrupted while enqueuing skip marker at offset "
                                + context.offset() + " on stream '" + stream + "'", e));
            }
            context.processed();
            return;
        }
        try {
            // Enqueue with a bounded timeout. context.processed() is NOT called
            // here — it is deferred to the pull side (next()) so that credits
            // are granted based on consumption rate and provide backpressure.
            // This can block the client delivery callback thread briefly.
            if (!queue.offer(new QueuedMessage(
                    message, context.offset(), context.timestamp(), context),
                    options.getCallbackEnqueueTimeoutMs(), TimeUnit.MILLISECONDS)) {
                consumerError.compareAndSet(null,
                        new IOException("Queue full: timed out enqueuing message " +
                                "at offset " + context.offset() +
                                " on stream '" + stream + "'"));
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            consumerError.compareAndSet(null,
                    new IOException("Interrupted while enqueuing message at offset "
                            + context.offset() + " on stream '" + stream + "'", e));
        }
    }

    boolean shouldDropByPostFilter(Message message) {
        if (!isBrokerFilterConfigured() || postFilter == null) {
            return false;
        }
        boolean accepted = postFilter.accept(toMessageView(message));
        if (!accepted && options.isFilterWarningOnMismatch()) {
            LOG.warn("Post-filter dropped message on stream '{}' in enqueue callback", stream);
        }
        return !accepted;
    }

    void handleStartOffsetOutOfRange(long firstAvailable) {
        offsetOutOfRange++;
        String message = "Start offset " + startOffset + " is before first available "
                + firstAvailable + " in stream '" + stream
                + "' (retention truncation detected)";
        if (options.isFailOnDataLoss()) {
            throw new IllegalStateException(
                    message + ". Set failOnDataLoss=false to skip lost data.");
        }
        LOG.warn("{}; continuing because failOnDataLoss=false", message);
    }

    OffsetSpecification resolveOffsetSpec() {
        // Spark already plans split ranges from resolved numeric offsets. Seeking by
        // timestamp here forces the executor to replay historical backlog just to skip
        // to startOffset, which can starve bounded micro-batches in SAC/SST live streams.
        return OffsetSpecification.offset(startOffset);
    }

    String resolveSingleActiveConsumerName() {
        String consumerName = options.getConsumerName();
        if (consumerName == null || consumerName.isEmpty()) {
            return consumerName;
        }
        if (options.isSuperStreamMode()) {
            String resolved = consumerName + "-" + stream;
            if (resolved.length() > ConnectorOptions.MAX_BROKER_REFERENCE_LENGTH) {
                throw new IllegalArgumentException(
                        "Derived single-active-consumer name must be shorter than 256 characters, got: "
                                + resolved.length() + " (" + consumerName + "-<stream>)");
            }
            return resolved;
        }
        return consumerName;
    }

    OffsetSpecification resolveSubscriptionOffsetSpec(
            OffsetSpecification subscriptionOffsetSpec,
            OffsetSpecification configuredOffsetSpec) {
        if (lastEmittedOffset >= 0) {
            return OffsetSpecification.offset(lastEmittedOffset + 1);
        }
        long observed = lastObservedOffset.get();
        if (observed >= 0) {
            return OffsetSpecification.offset(Math.max(startOffset, observed + 1));
        }
        return subscriptionOffsetSpec != null ? subscriptionOffsetSpec : configuredOffsetSpec;
    }

    OffsetSpecification resolveSingleActiveConsumerActivationOffset(
            OffsetSpecification configuredOffsetSpec) {
        return resolveSubscriptionOffsetSpec(null, configuredOffsetSpec);
    }

    private void warnIfSpeculationIncompatibleWithSac() {
        try {
            SparkEnv sparkEnv = SparkEnv.get();
            if (sparkEnv == null || sparkEnv.conf() == null) {
                return;
            }
            if (sparkEnv.conf().getBoolean("spark.speculation", false)
                    && SAC_SPECULATION_WARNED.compareAndSet(false, true)) {
                LOG.warn("Single-active-consumer is enabled for stream '{}' while "
                                + "'spark.speculation=true'. SAC is incompatible with concurrent "
                                + "task attempts within a SAC group: a reactivation during "
                                + "speculation or task retry may re-emit records that a peer "
                                + "consumer already delivered, increasing duplicates above what "
                                + "at-least-once would normally produce. Disable speculation when "
                                + "reading via SAC. See SPEC_V1.md (Speculative execution). "
                                + "(This warning is logged at most once per executor JVM.)",
                        stream);
            }
        } catch (Exception t) {
            LOG.debug("Unable to inspect spark.speculation for SAC compatibility check: {}",
                    t.toString());
        }
    }

    boolean shouldSkipByTimestamp(long chunkTimestampMillis) {
        return useConfiguredStartingOffset
                && options.getStartingOffsets() == StartingOffsetsMode.TIMESTAMP
                && chunkTimestampMillis < resolveStartingTimestampForStream();
    }

    boolean isTimestampConfiguredStartingOffset() {
        return useConfiguredStartingOffset
                && options.getStartingOffsets() == StartingOffsetsMode.TIMESTAMP;
    }

    boolean canCompleteEmptyPlannedRange() {
        return isBrokerFilterConfigured() || isTimestampConfiguredStartingOffset();
    }

    boolean isSingleActiveConsumerKnownInactive() {
        return options.isSingleActiveConsumer()
                && singleActiveConsumerStateKnown.get()
                && !singleActiveConsumerActive.get();
    }

    boolean hasFinitePlannedEnd() {
        return endOffset != Long.MAX_VALUE;
    }

    boolean hasReachedPlannedEnd() {
        if (!hasFinitePlannedEnd()) {
            return false;
        }
        if (endOffset <= startOffset) {
            return true;
        }
        long lastPlannedOffset = endOffset - 1;
        long observed = lastObservedOffset.get();
        return (lastEmittedOffset >= 0 && lastEmittedOffset >= lastPlannedOffset)
                || (observed >= 0 && observed >= lastPlannedOffset);
    }

    /** Monotonically advance lastObservedOffset to {@code offset} if higher. */
    long advanceObserved(long offset) {
        return lastObservedOffset.accumulateAndGet(offset, Math::max);
    }

    boolean isAtOrBeyondPlannedEnd(long offset) {
        return hasFinitePlannedEnd() && offset >= endOffset;
    }

    boolean isTailBeforePlannedEnd(long tail) {
        if (!hasFinitePlannedEnd()) {
            return true;
        }
        if (endOffset <= startOffset) {
            return false;
        }
        return tail < endOffset - 1;
    }

    long pollIntervalMs(long remainingMs, long pollTimeoutMs) {
        long pollMs = Math.max(1L, Math.min(remainingMs, pollTimeoutMs));
        if (canCompleteEmptyPlannedRange()) {
            return Math.min(pollMs, CLOSED_CHECK_INTERVAL_MS);
        }
        return pollMs;
    }

    TerminationDecision decideTermination(
            boolean timeoutIsFailure,
            boolean dataLossProven,
            boolean plannedEndReached,
            boolean sacInactive) {
        if (dataLossProven) {
            return options.isFailOnDataLoss()
                    ? TerminationDecision.THROW
                    : TerminationDecision.COMPLETE;
        }
        if (plannedEndReached) {
            return TerminationDecision.COMPLETE;
        }
        if (sacInactive || timeoutIsFailure) {
            return TerminationDecision.THROW;
        }
        return TerminationDecision.CONTINUE;
    }

    IOException terminationFailure(
            boolean timeoutIsFailure,
            boolean dataLossProven,
            boolean sacInactive,
            long waitedMs) {
        if (sacInactive) {
            return new IOException(
                    "Single active consumer for stream '" + stream
                            + "' is currently inactive; retrying the task may attach to the active consumer");
        }
        if (dataLossProven && options.isFailOnDataLoss()) {
            return new IOException(
                    "Planned range [" + startOffset + ", " + endOffset + ") for stream '"
                            + stream + "' is no longer reachable due to data loss/recreation");
        }
        if (timeoutIsFailure) {
            return new IOException(
                    "Timed out waiting for messages from stream '" + stream +
                            "'. Last emitted offset: " + lastEmittedOffset +
                            ", target end offset: " + endOffset +
                            ". Waited " + waitedMs + "ms (maxWaitMs=" + options.getMaxWaitMs() + ")");
        }
        return new IOException(
                "Reader for stream '" + stream + "' cannot complete planned range ["
                        + startOffset + ", " + endOffset + ")");
    }

    boolean shouldDetectOffsetGapsAfterConsumerClosure() {
        return !isBrokerFilterConfigured() && !isTimestampConfiguredStartingOffset();
    }

    long expectedNextObservedOffset() {
        long observed = lastObservedOffset.get();
        if (observed < startOffset) {
            return startOffset;
        }
        return observed + 1;
    }

    long resolveStartingTimestampForStream() {
        Map<String, Long> perStreamTimestamps = options.getStartingOffsetsByTimestamp();
        if (perStreamTimestamps != null) {
            Long streamTimestamp = perStreamTimestamps.get(stream);
            if (streamTimestamp != null) {
                return streamTimestamp;
            }
        }
        Long defaultTimestamp = options.getStartingTimestamp();
        if (defaultTimestamp != null) {
            return defaultTimestamp;
        }
        throw new IllegalStateException(
                "No starting timestamp configured for stream '" + stream + "' while "
                        + ConnectorOptions.STARTING_OFFSETS + "=timestamp. Configure "
                        + ConnectorOptions.STARTING_TIMESTAMP + " or provide "
                        + ConnectorOptions.STARTING_OFFSETS_BY_TIMESTAMP + " for this stream.");
    }

    boolean isBrokerFilterConfigured() {
        return options.getFilterValues() != null && !options.getFilterValues().isEmpty();
    }

    boolean hasStreamTailReachedPlannedEnd() {
        if (!hasFinitePlannedEnd()) {
            return false;
        }
        if (filteredTailReached) {
            return true;
        }
        try {
            long probedTail = probeLastMessageOffset();
            long tail = probedTail >= 0L
                    ? probedTail
                    : queryStreamTailOffsetFromStatsWithCache();
            if (tail < 0) {
                return endOffset <= startOffset;
            }
            if (!isTailBeforePlannedEnd(tail)) {
                filteredTailReached = true;
                return true;
            }
            return false;
        } catch (Exception e) {
            LOG.debug("Unable to probe stream tail for filtered termination on '{}': {}",
                    stream, e.getMessage());
            return false;
        }
    }

    boolean isPlannedRangeNoLongerReachableDueToDataLoss() {
        try {
            Environment env = environment;
            if (env == null) {
                return false;
            }
            StreamStats stats = env.queryStreamStats(stream);
            long first = stats.firstOffset();
            boolean startWasTruncatedBeforeAnyInRangeProgress =
                    first > startOffset && lastObservedOffset.get() < startOffset;
            if (startWasTruncatedBeforeAnyInRangeProgress) {
                return true;
            }
            long tail = probeLastMessageOffset();
            return hasFinitePlannedEnd() && tail >= 0 && tail < startOffset;
        } catch (Exception e) {
            if (isMissingStreamException(e)) {
                return true;
            }
            LOG.debug("Unable to validate data-loss reachability for stream '{}': {}",
                    stream, e.getMessage());
            return false;
        }
    }

    boolean plannedRangeEndedBeforeTarget() {
        if (!hasFinitePlannedEnd() || endOffset <= startOffset) {
            return false;
        }
        long lastObservedInRange = Math.min(lastObservedOffset.get(), endOffset - 1);
        if (lastObservedInRange < startOffset) {
            return false;
        }
        long tail = probeLastMessageOffset();
        return tail >= 0 && tail <= lastObservedInRange && isTailBeforePlannedEnd(tail);
    }

    boolean isMissingStreamException(Throwable throwable) {
        Throwable current = throwable;
        while (current != null) {
            if (current instanceof StreamDoesNotExistException) {
                return true;
            }
            String message = current.getMessage();
            if (message != null && (message.contains("STREAM_DOES_NOT_EXIST")
                    || message.contains("does not exist")
                    || message.contains("has no partition streams"))) {
                return true;
            }
            current = current.getCause();
        }
        return false;
    }

    static long resolveTailOffsetInclusive(StreamStats stats) {
        return RabbitMQMicroBatchStream.resolveTailOffset(stats) - 1;
    }

    long probeLastMessageOffset() {
        long nowNanos = System.nanoTime();
        if (lastTailProbeNanos > 0) {
            long elapsedMs = TimeUnit.NANOSECONDS.toMillis(nowNanos - lastTailProbeNanos);
            if (elapsedMs < tailProbeCacheWindowMs()) {
                return lastTailProbeOffset;
            }
        }

        Environment env = environment;
        if (env == null) {
            lastTailProbeOffset = -1L;
            lastTailProbeNanos = nowNanos;
            return -1L;
        }

        try {
            long probeTimeoutMs = Math.max(1L, Math.min(
                    BaseRabbitMQMicroBatchStream.TAIL_PROBE_WAIT_MS, options.getMaxWaitMs()));
            long resolved = BaseRabbitMQMicroBatchStream.probeLastMessageOffsetInclusive(
                    env, stream, probeTimeoutMs);
            lastTailProbeOffset = resolved;
            lastTailProbeNanos = System.nanoTime();
            return resolved;
        } catch (Exception e) {
            LOG.debug("Unable to probe last-message tail offset for stream '{}': {}",
                    stream, e.getMessage());
            lastTailProbeOffset = -1L;
            lastTailProbeNanos = System.nanoTime();
            return -1;
        }
    }

    long tailProbeCacheWindowMs() {
        long pollTimeoutMs = Math.max(1L, options.getPollTimeoutMs());
        return Math.max(MIN_TAIL_PROBE_CACHE_WINDOW_MS,
                Math.min(MAX_TAIL_PROBE_CACHE_WINDOW_MS, pollTimeoutMs));
    }

    long tailStatsCacheWindowMs() {
        long pollTimeoutMs = Math.max(1L, options.getPollTimeoutMs());
        return Math.max(MIN_STATS_TAIL_CACHE_WINDOW_MS,
                Math.min(MAX_STATS_TAIL_CACHE_WINDOW_MS, pollTimeoutMs));
    }

    long queryStreamTailOffsetFromStatsWithCache() {
        long nowNanos = System.nanoTime();
        if (lastStatsTailNanos > 0) {
            long elapsedMs = TimeUnit.NANOSECONDS.toMillis(nowNanos - lastStatsTailNanos);
            if (elapsedMs < tailStatsCacheWindowMs()) {
                return lastStatsTailOffset;
            }
        }

        Environment env = environment;
        if (env == null) {
            lastStatsTailOffset = -1L;
            lastStatsTailNanos = nowNanos;
            return -1L;
        }

        StreamStats stats = env.queryStreamStats(stream);
        long resolvedTail = resolveTailOffsetInclusive(stats);
        lastStatsTailOffset = resolvedTail;
        lastStatsTailNanos = System.nanoTime();
        return resolvedTail;
    }

    static MessagePostFilter createPostFilter(ConnectorOptions options) {
        String className = options.getFilterPostFilterClass();
        if (className != null && !className.isEmpty()) {
            ConnectorPostFilter filter = ExtensionLoader.load(
                    className, ConnectorPostFilter.class,
                    ConnectorOptions.FILTER_POST_FILTER_CLASS);
            return filter::accept;
        }
        if (options.getFilterValues() == null || options.getFilterValues().isEmpty()) {
            return null;
        }
        String filterValuePath = options.getFilterValuePath();
        if (filterValuePath == null || filterValuePath.isEmpty()) {
            return null;
        }
        return new FilterValuesPostFilter(
                filterValuePath,
                options.getFilterValues(),
                options.isFilterMatchUnfiltered());
    }

    static ConnectorMessageView toMessageView(Message message) {
        return MessageViewCoercion.toMessageView(message);
    }

    static Map<String, String> coerceMapToStrings(Map<String, Object> source) {
        return MessageViewCoercion.coerceMapToStrings(source);
    }

    static Map<String, String> coercePropertiesToStrings(Properties properties) {
        return MessageViewCoercion.coercePropertiesToStrings(properties);
    }

    static String coerceIdToString(Object id) {
        return MessageViewCoercion.coerceIdToString(id);
    }

    static void putIfNotNull(Map<String, String> target, String key, String value) {
        MessageViewCoercion.putIfNotNull(target, key, value);
    }

    static final class FilterValuesPostFilter implements MessagePostFilter {
        private final String path;
        private final Set<String> allowedValues;
        private final boolean matchUnfiltered;

        FilterValuesPostFilter(String path, Iterable<String> filterValues,
                                       boolean matchUnfiltered) {
            this.path = path;
            Set<String> values = new HashSet<>();
            for (String value : filterValues) {
                if (value != null) {
                    values.add(value);
                }
            }
            this.allowedValues = Set.copyOf(values);
            this.matchUnfiltered = matchUnfiltered;
        }

        @Override
        public boolean accept(ConnectorMessageView message) {
            String value = message.valueAtPath(path);
            if (value == null) {
                return matchUnfiltered;
            }
            return allowedValues.contains(value);
        }
    }

    @FunctionalInterface
    interface MessagePostFilter {
        boolean accept(ConnectorMessageView message);
    }
}
