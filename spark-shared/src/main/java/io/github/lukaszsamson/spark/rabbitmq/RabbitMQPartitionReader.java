package io.github.lukaszsamson.spark.rabbitmq;

import com.rabbitmq.stream.*;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.metric.CustomTaskMetric;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Base64;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Push-to-pull bridge that reads messages from a RabbitMQ stream consumer
 * and presents them as Spark {@link InternalRow}s.
 *
 * <p>The RabbitMQ stream client is push-based (messages arrive via
 * {@link MessageHandler}). Spark readers are pull-based ({@link #next()}/{@link #get()}).
 * This reader bridges the two by pushing messages into a bounded
 * {@link BlockingQueue} and pulling from it on demand.
 */
final class RabbitMQPartitionReader implements PartitionReader<InternalRow> {

    private static final Logger LOG = LoggerFactory.getLogger(RabbitMQPartitionReader.class);

    private final String stream;
    private final long startOffset;
    private final long endOffset;
    private final ConnectorOptions options;
    private final boolean useConfiguredStartingOffset;
    private final MessageToRowConverter converter;
    private final MessagePostFilter postFilter;

    private final BlockingQueue<QueuedMessage> queue;
    private final AtomicReference<Throwable> consumerError = new AtomicReference<>();
    private final AtomicBoolean consumerClosed = new AtomicBoolean(false);
    private final AtomicBoolean closeCalled = new AtomicBoolean(false);

    private boolean pooledEnvironment = false;
    private Environment environment;
    private Consumer consumer;
    private InternalRow currentRow;
    private long lastEmittedOffset = -1;
    private long lastObservedOffset = -1;
    private boolean filteredTailReached = false;
    private boolean finished = false;

    // Task-level metric counters
    private long recordsRead = 0;
    private long payloadBytesRead = 0;
    private long estimatedWireBytesRead = 0;
    private long pollWaitMs = 0;
    private long offsetOutOfRange = 0;
    private long dataLoss = 0;

    /**
     * A message queued by the consumer callback for pull-based reading.
     * Includes the {@link MessageHandler.Context} so that {@code processed()}
     * can be called on the pull side, tying credit grants to consumption rate.
     */
    record QueuedMessage(Message message, long offset, long chunkTimestampMillis,
                         MessageHandler.Context context) {}

    RabbitMQPartitionReader(RabbitMQInputPartition partition, ConnectorOptions options) {
        this.stream = partition.getStream();
        this.startOffset = partition.getStartOffset();
        this.endOffset = partition.getEndOffset();
        this.options = options;
        this.useConfiguredStartingOffset = partition.isUseConfiguredStartingOffset();
        this.converter = new MessageToRowConverter(options.getMetadataFields());
        this.postFilter = createPostFilter(options);
        this.queue = new LinkedBlockingQueue<>(options.getQueueCapacity());
    }

    @Override
    public boolean next() throws IOException {
        if (finished) {
            return false;
        }

        // Fast-path: if we already observed or emitted the final in-range offset, stop immediately.
        // lastObservedOffset is needed when post-filter drops tail records.
        if ((lastEmittedOffset >= 0 && lastEmittedOffset >= endOffset - 1)
                || (lastObservedOffset >= 0 && lastObservedOffset >= endOffset - 1)) {
            finished = true;
            return false;
        }

        // Lazy initialization: create consumer on first call
        if (consumer == null) {
            try {
                initConsumer();
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

        while (true) {
            if ((lastEmittedOffset >= 0 && lastEmittedOffset >= endOffset - 1)
                    || (lastObservedOffset >= 0 && lastObservedOffset >= endOffset - 1)) {
                finished = true;
                return false;
            }

            // Check for consumer errors
            Throwable error = consumerError.get();
            if (error != null) {
                throw new IOException("Consumer error on stream '" + stream + "'", error);
            }

            QueuedMessage qm;
            try {
                long pollStart = System.nanoTime();
                qm = queue.poll(pollTimeoutMs, TimeUnit.MILLISECONDS);
                pollWaitMs += TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - pollStart);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("Interrupted while reading from stream '" + stream + "'", e);
            }

            if (qm == null) {
                if (consumerClosed.get()) {
                    if (!options.isFailOnDataLoss() && isPlannedRangeNoLongerReachableDueToDataLoss()) {
                        LOG.warn("Consumer for stream '{}' closed and planned range [{}, {}) is no longer " +
                                        "reachable; completing split because failOnDataLoss=false",
                                stream, startOffset, endOffset);
                        dataLoss++;
                        finished = true;
                        return false;
                    }
                    throw new IOException(
                            "Consumer for stream '" + stream + "' closed before reaching target end offset "
                                    + endOffset + ". Last emitted offset: " + lastEmittedOffset);
                }
                // With broker-side filtering, not all offsets in [start, end) are delivered.
                // Timestamp-seek split can also legitimately have no in-range rows when the
                // resolved broker seek position is already past this split's end.
                // If the stream tail has already reached this split's end, we can terminate.
                boolean brokerFilterConfigured = isBrokerFilterConfigured();
                boolean timestampStart = isTimestampConfiguredStartingOffset();
                boolean canTerminateOnEmpty = brokerFilterConfigured || timestampStart;
                if (canTerminateOnEmpty && hasStreamTailReachedPlannedEnd()) {
                    finished = true;
                    return false;
                }
                totalWaitMs += pollTimeoutMs;
                if (totalWaitMs >= maxWaitMs) {
                    if (timestampStart) {
                        LOG.warn("Reached maxWaitMs={} while reading filtered stream '{}'; " +
                                        "terminating split early at lastObservedOffset={} for planned endOffset={}",
                                maxWaitMs, stream, lastObservedOffset, endOffset);
                        finished = true;
                        return false;
                    }
                    if (brokerFilterConfigured && hasStreamTailReachedPlannedEnd()) {
                        LOG.warn("Reached maxWaitMs={} while reading filtered stream '{}'; " +
                                        "tail indicates planned end reached at lastObservedOffset={} for endOffset={}",
                                maxWaitMs, stream, lastObservedOffset, endOffset);
                        finished = true;
                        return false;
                    }
                    if (!options.isFailOnDataLoss() && isPlannedRangeNoLongerReachableDueToDataLoss()) {
                        LOG.warn("Reached maxWaitMs={} on stream '{}' and planned range [{}, {}) is no longer " +
                                        "reachable after data loss/recreation; completing split because failOnDataLoss=false",
                                maxWaitMs, stream, startOffset, endOffset);
                        dataLoss++;
                        finished = true;
                        return false;
                    }
                    if (brokerFilterConfigured && lastObservedOffset >= startOffset) {
                        LOG.warn("Reached maxWaitMs={} while reading filtered stream '{}'; " +
                                        "observed in-range offsets and no additional matching records arrived " +
                                        "(lastObservedOffset={}, planned endOffset={}). Completing split.",
                                maxWaitMs, stream, lastObservedOffset, endOffset);
                        finished = true;
                        return false;
                    }
                    if (!options.isFailOnDataLoss()
                            && endOffset > 0
                            && lastObservedOffset >= endOffset - 2) {
                        LOG.warn("Reached maxWaitMs={} on stream '{}' near planned end; " +
                                        "completing split because failOnDataLoss=false " +
                                        "(lastObservedOffset={}, planned endOffset={})",
                                maxWaitMs, stream, lastObservedOffset, endOffset);
                        finished = true;
                        return false;
                    }
                    if (!options.isFailOnDataLoss()
                            && lastObservedOffset < startOffset
                            && isStreamTailBelowPlannedEnd()) {
                        LOG.warn("Reached maxWaitMs={} on stream '{}' with no in-range progress and tail below " +
                                        "planned end; completing split because failOnDataLoss=false " +
                                        "(startOffset={}, planned endOffset={})",
                                maxWaitMs, stream, startOffset, endOffset);
                        finished = true;
                        return false;
                    }
                    if (!options.isFailOnDataLoss() && lastObservedOffset < startOffset) {
                        LOG.warn("Reached maxWaitMs={} on stream '{}' without in-range progress; " +
                                        "completing split because failOnDataLoss=false " +
                                        "(startOffset={}, planned endOffset={})",
                                maxWaitMs, stream, startOffset, endOffset);
                        finished = true;
                        return false;
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

            // Reset wait timer on message receipt
            totalWaitMs = 0;

            // Notify flow strategy that this message has been consumed (pull-side).
            // This ties credit grants to consumption rate rather than enqueue rate,
            // providing natural backpressure when the pull side is slow.
            qm.context().processed();

            // Skip messages before start offset (can happen with timestamp-based starting)
            if (qm.offset() < startOffset) {
                continue;
            }

            // Stop at end offset (exclusive)
            if (qm.offset() >= endOffset) {
                finished = true;
                return false;
            }

            // De-dup on reconnection: skip already-emitted offsets
            if (qm.offset() <= lastEmittedOffset) {
                continue;
            }

            if (qm.offset() > lastObservedOffset) {
                lastObservedOffset = qm.offset();
            }

            if (shouldSkipByTimestamp(qm.chunkTimestampMillis())) {
                continue;
            }

            if (postFilter != null && !postFilter.accept(toMessageView(qm.message()))) {
                if (options.isFilterWarningOnMismatch()) {
                    LOG.warn("Post-filter dropped message at offset {} on stream '{}'",
                            qm.offset(), stream);
                }
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
        // Report actual message sizes for running average estimation
        MessageSizeTracker.record(payloadBytesRead, recordsRead);
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

    private void initConsumer() {
        environment = EnvironmentPool.getInstance().acquire(options);
        pooledEnvironment = true;

        // Detect offset-out-of-range (retention truncation)
        try {
            StreamStats stats = environment.queryStreamStats(stream);
            long firstAvailable = stats.firstOffset();
            if (startOffset < firstAvailable) {
                offsetOutOfRange++;
                LOG.warn("Start offset {} is before first available {} in stream '{}' " +
                        "(retention truncation detected)", startOffset, firstAvailable, stream);
            }
        } catch (Exception e) {
            // Non-fatal: cannot check, proceed without metric
            LOG.debug("Cannot check offset range for stream '{}': {}", stream, e.getMessage());
        }

        OffsetSpecification offsetSpec = resolveOffsetSpec();

        ConsumerBuilder builder = environment.consumerBuilder()
                .stream(stream)
                .offset(offsetSpec)
                .noTrackingStrategy()
                .messageHandler((context, message) -> {
                    // Fast exit if we've already reached end offset
                    if (context.offset() >= endOffset) {
                        return;
                    }
                    try {
                        // Enqueue with a short timeout. context.processed() is NOT called
                        // here — it is deferred to the pull side (next()) so that credits
                        // are granted based on consumption rate, providing natural
                        // backpressure and avoiding blocking the Netty I/O thread.
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
                    }
                })
                .flow()
                .strategy(ConsumerFlowStrategy.creditWhenHalfMessagesProcessed(
                        options.getInitialCredits()))
                .builder();
        builder.subscriptionListener(context -> context.offsetSpecification(
                resolveSubscriptionOffsetSpec(context.offsetSpecification(), offsetSpec)));
        if (options.isSingleActiveConsumer()) {
            builder.name(resolveSingleActiveConsumerName())
                    .singleActiveConsumer()
                    // SAC with noTrackingStrategy needs an explicit update listener
                    // to provide the activation offset.
                    .consumerUpdateListener(context ->
                            context.isActive() ? offsetSpec : OffsetSpecification.none());
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
            MessagePostFilter brokerPostFilter = postFilter;
            if (brokerPostFilter == null) {
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
                    // Keep this consistent with the connector-level post-filter used in next().
                    .postFilter(msg -> brokerPostFilter == null
                            || brokerPostFilter.accept(toMessageView(msg)))
                    .builder();
        }

        consumer = builder.build();

        LOG.info("Opened consumer for stream '{}' with offsets [{}, {})",
                stream, startOffset, endOffset);
    }

    private OffsetSpecification resolveOffsetSpec() {
        if (useConfiguredStartingOffset && options.getStartingOffsets() == StartingOffsetsMode.TIMESTAMP) {
            return OffsetSpecification.timestamp(options.getStartingTimestamp());
        }
        return OffsetSpecification.offset(startOffset);
    }

    private String resolveSingleActiveConsumerName() {
        String consumerName = options.getConsumerName();
        if (consumerName == null || consumerName.isEmpty()) {
            return consumerName;
        }
        if (options.isSuperStreamMode()) {
            return consumerName + "-" + stream;
        }
        return consumerName;
    }

    private OffsetSpecification resolveSubscriptionOffsetSpec(
            OffsetSpecification subscriptionOffsetSpec,
            OffsetSpecification configuredOffsetSpec) {
        if (lastEmittedOffset >= 0) {
            return OffsetSpecification.offset(lastEmittedOffset + 1);
        }
        if (lastObservedOffset >= 0) {
            return OffsetSpecification.offset(lastObservedOffset + 1);
        }
        return subscriptionOffsetSpec != null ? subscriptionOffsetSpec : configuredOffsetSpec;
    }

    private boolean shouldSkipByTimestamp(long chunkTimestampMillis) {
        return useConfiguredStartingOffset
                && options.getStartingOffsets() == StartingOffsetsMode.TIMESTAMP
                && chunkTimestampMillis < options.getStartingTimestamp();
    }

    private boolean isTimestampConfiguredStartingOffset() {
        return useConfiguredStartingOffset
                && options.getStartingOffsets() == StartingOffsetsMode.TIMESTAMP;
    }

    private boolean isBrokerFilterConfigured() {
        return options.getFilterValues() != null && !options.getFilterValues().isEmpty();
    }

    private boolean hasStreamTailReachedPlannedEnd() {
        if (filteredTailReached) {
            return true;
        }
        try {
            StreamStats stats = environment.queryStreamStats(stream);
            long statsTail = resolveTailOffsetInclusive(stats);
            long probedTail = probeLastMessageOffset();
            long tail = Math.max(statsTail, probedTail);
            if (tail < 0) {
                return endOffset <= 0;
            }
            if (tail >= endOffset - 1) {
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

    private boolean isPlannedRangeNoLongerReachableDueToDataLoss() {
        try {
            StreamStats stats = environment.queryStreamStats(stream);
            long first = stats.firstOffset();
            long tail = resolveTailOffsetInclusive(stats);
            boolean streamWasResetOrTruncated =
                    tail < startOffset || first > startOffset || (lastObservedOffset >= 0 && tail < lastObservedOffset);
            return tail < endOffset - 1 && streamWasResetOrTruncated;
        } catch (Exception e) {
            if (isMissingStreamException(e)) {
                return true;
            }
            LOG.debug("Unable to validate data-loss reachability for stream '{}': {}",
                    stream, e.getMessage());
            return false;
        }
    }

    private boolean isStreamTailBelowPlannedEnd() {
        try {
            StreamStats stats = environment.queryStreamStats(stream);
            long statsTail = resolveTailOffsetInclusive(stats);
            long probedTail = probeLastMessageOffset();
            long tail = Math.max(statsTail, probedTail);
            return tail < endOffset - 1;
        } catch (Exception e) {
            LOG.debug("Unable to validate stream tail for reachability on '{}': {}",
                    stream, e.getMessage());
            return false;
        }
    }

    private boolean isMissingStreamException(Throwable throwable) {
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

    private static long resolveTailOffsetInclusive(StreamStats stats) {
        return RabbitMQMicroBatchStream.resolveTailOffset(stats) - 1;
    }

    private long probeLastMessageOffset() {
        ArrayBlockingQueue<Long> observedOffsets = new ArrayBlockingQueue<>(1);
        Consumer probe = null;
        try {
            probe = environment.consumerBuilder()
                    .stream(stream)
                    .offset(OffsetSpecification.last())
                    .noTrackingStrategy()
                    .messageHandler((context, message) -> observedOffsets.offer(context.offset()))
                    .build();
            Long observed = observedOffsets.poll(5, TimeUnit.SECONDS);
            return observed == null ? -1 : observed;
        } catch (Exception e) {
            LOG.debug("Unable to probe last-message tail offset for stream '{}': {}",
                    stream, e.getMessage());
            return -1;
        } finally {
            if (probe != null) {
                try {
                    probe.close();
                } catch (Exception e) {
                    LOG.debug("Error closing probe consumer for stream '{}': {}",
                            stream, e.getMessage());
                }
            }
        }
    }

    private static MessagePostFilter createPostFilter(ConnectorOptions options) {
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

    private static ConnectorMessageView toMessageView(Message message) {
        return new ConnectorMessageView(
                message.getBodyAsBinary(),
                coerceMapToStrings(message.getApplicationProperties()),
                coerceMapToStrings(message.getMessageAnnotations()),
                coercePropertiesToStrings(message.getProperties()));
    }

    private static Map<String, String> coerceMapToStrings(Map<String, Object> source) {
        if (source == null || source.isEmpty()) {
            return Map.of();
        }
        Map<String, String> out = new LinkedHashMap<>(source.size());
        for (Map.Entry<String, Object> entry : source.entrySet()) {
            out.put(entry.getKey(), entry.getValue() != null ? entry.getValue().toString() : null);
        }
        return out;
    }

    private static Map<String, String> coercePropertiesToStrings(Properties properties) {
        if (properties == null) {
            return Map.of();
        }
        Map<String, String> out = new LinkedHashMap<>();
        putIfNotNull(out, "message_id", coerceIdToString(properties.getMessageId()));
        if (properties.getUserId() != null) {
            out.put("user_id", Base64.getEncoder().encodeToString(properties.getUserId()));
        }
        putIfNotNull(out, "to", properties.getTo());
        putIfNotNull(out, "subject", properties.getSubject());
        putIfNotNull(out, "reply_to", properties.getReplyTo());
        putIfNotNull(out, "correlation_id", coerceIdToString(properties.getCorrelationId()));
        putIfNotNull(out, "content_type", properties.getContentType());
        putIfNotNull(out, "content_encoding", properties.getContentEncoding());
        if (properties.getAbsoluteExpiryTime() > 0) {
            out.put("absolute_expiry_time", Long.toString(properties.getAbsoluteExpiryTime()));
        }
        if (properties.getCreationTime() > 0) {
            out.put("creation_time", Long.toString(properties.getCreationTime()));
        }
        putIfNotNull(out, "group_id", properties.getGroupId());
        if (properties.getGroupSequence() > 0) {
            out.put("group_sequence", Long.toString(properties.getGroupSequence()));
        }
        putIfNotNull(out, "reply_to_group_id", properties.getReplyToGroupId());
        return out;
    }

    private static String coerceIdToString(Object id) {
        if (id == null) {
            return null;
        }
        if (id instanceof byte[] bytes) {
            return Base64.getEncoder().encodeToString(bytes);
        }
        return id.toString();
    }

    private static void putIfNotNull(Map<String, String> target, String key, String value) {
        if (value != null) {
            target.put(key, value);
        }
    }

    private static final class FilterValuesPostFilter implements MessagePostFilter {
        private final String path;
        private final Set<String> allowedValues;
        private final boolean matchUnfiltered;

        private FilterValuesPostFilter(String path, Iterable<String> filterValues,
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
    private interface MessagePostFilter {
        boolean accept(ConnectorMessageView message);
    }
}
