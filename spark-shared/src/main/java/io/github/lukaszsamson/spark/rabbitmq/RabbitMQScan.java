package io.github.lukaszsamson.spark.rabbitmq;

import com.rabbitmq.stream.ConsumerFlowStrategy;
import com.rabbitmq.stream.Environment;
import com.rabbitmq.stream.Message;
import com.rabbitmq.stream.NoOffsetException;
import com.rabbitmq.stream.Properties;
import com.rabbitmq.stream.StreamDoesNotExistException;
import com.rabbitmq.stream.StreamStats;
import org.apache.spark.sql.connector.metric.CustomMetric;
import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.streaming.MicroBatchStream;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Logical representation of a RabbitMQ stream scan.
 *
 * <p>{@link #toBatch()} resolves starting and ending offsets once and returns
 * a fixed {@link RabbitMQBatch}. {@link #toMicroBatchStream(String)} returns
 * a {@link RabbitMQMicroBatchStream} for Structured Streaming queries.
 */
final class RabbitMQScan implements Scan {

    private static final Logger LOG = LoggerFactory.getLogger(RabbitMQScan.class);
    private static final long TAIL_PROBE_TIMEOUT_MS = 1_500L;
    private static final long MIN_TIMESTAMP_PROBE_TIMEOUT_MS = 250L;
    /** Floor for the prove-absence pre-check budget — short enough to be cheap on dense
     *  streams (where it returns inconclusive), long enough to attach on a remote broker. */
    private static final long MIN_PROVE_ABSENCE_BUDGET_MS = 500L;
    /** Ceiling for the prove-absence pre-check budget — the regular timestamp probe still
     *  gets the full {@code pollTimeoutMs} after this returns inconclusive. */
    private static final long MAX_PROVE_ABSENCE_BUDGET_MS = 2_000L;
    private static final long RESOLVER_DRAIN_TIMEOUT_MS = 5_000L;
    /** Max concurrency for broker-side helper executors. */
    private static final int MAX_BROKER_HELPER_PARALLELISM = 20;
    private static final int TAIL_PROBE_EXECUTOR_PARALLELISM = Math.max(
            1,
            Math.min(Runtime.getRuntime().availableProcessors(),
                    MAX_BROKER_HELPER_PARALLELISM));
    private static final int RANGE_RESOLVER_EXECUTOR_PARALLELISM = Math.max(
            1,
            Math.min(Runtime.getRuntime().availableProcessors(),
                    MAX_BROKER_HELPER_PARALLELISM));
    private static final AtomicInteger TAIL_PROBE_THREAD_COUNTER = new AtomicInteger(0);
    private static final AtomicInteger RANGE_RESOLVER_THREAD_COUNTER = new AtomicInteger(0);

    // Lazily initialize shared pools so idle scans do not allocate worker threads.
    private static final class SharedExecutors {
        private static final ExecutorService TAIL_PROBE_TIMEOUT_EXECUTOR =
                createTailProbeExecutor();
        private static final ExecutorService RANGE_RESOLVER_EXECUTOR =
                createRangeResolverExecutor();

        static {
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                TAIL_PROBE_TIMEOUT_EXECUTOR.shutdownNow();
                RANGE_RESOLVER_EXECUTOR.shutdownNow();
            }, "rabbitmq-scan-executors-shutdown"));
        }
    }

    private static ExecutorService tailProbeTimeoutExecutor() {
        return SharedExecutors.TAIL_PROBE_TIMEOUT_EXECUTOR;
    }

    private static ExecutorService rangeResolverExecutor() {
        return SharedExecutors.RANGE_RESOLVER_EXECUTOR;
    }

    private static ExecutorService createTailProbeExecutor() {
        return Executors.newFixedThreadPool(TAIL_PROBE_EXECUTOR_PARALLELISM, r -> {
            Thread t = new Thread(
                    r,
                    "rabbitmq-scan-tail-probe-" + TAIL_PROBE_THREAD_COUNTER.incrementAndGet());
            t.setDaemon(true);
            return t;
        });
    }

    private static ExecutorService createRangeResolverExecutor() {
        return Executors.newFixedThreadPool(RANGE_RESOLVER_EXECUTOR_PARALLELISM, r -> {
            Thread t = new Thread(
                    r,
                    "rabbitmq-scan-range-resolver-"
                            + RANGE_RESOLVER_THREAD_COUNTER.incrementAndGet());
            t.setDaemon(true);
            return t;
        });
    }

    private final ConnectorOptions options;
    private final StructType schema;

    RabbitMQScan(ConnectorOptions options, StructType schema) {
        this.options = options;
        this.schema = schema;
    }

    @Override
    public StructType readSchema() {
        return schema;
    }

    @Override
    public Scan.ColumnarSupportMode columnarSupportMode() {
        return Scan.ColumnarSupportMode.UNSUPPORTED;
    }

    @Override
    public String description() {
        String target = options.isStreamMode()
                ? "stream=" + options.getStream()
                : "superstream=" + options.getSuperStream();
        return "RabbitMQScan[" + target + "]";
    }

    @Override
    public Batch toBatch() {
        List<String> streams = discoverStreams();
        Map<String, long[]> offsetRanges = resolveOffsetRanges(streams);
        return new RabbitMQBatch(options, schema, offsetRanges);
    }

    @Override
    public MicroBatchStream toMicroBatchStream(String checkpointLocation) {
        if (options.getEndingOffsets() == EndingOffsetsMode.OFFSET) {
            throw new IllegalArgumentException(
                    "endingOffsets=offset is not supported for streaming queries. " +
                            "Use endingOffsets only with batch reads.");
        }
        if (options.getEndingOffsets() == EndingOffsetsMode.TIMESTAMP) {
            throw new IllegalArgumentException(
                    "endingOffsets=timestamp is not supported for streaming queries. " +
                            "Use endingOffsets only with batch reads.");
        }
        return new RabbitMQMicroBatchStream(options, schema, checkpointLocation);
    }

    @Override
    public CustomMetric[] supportedCustomMetrics() {
        return RabbitMQSourceMetrics.SUPPORTED_METRICS;
    }

    /**
     * Discover the list of streams to read from.
     * For single-stream mode, returns the configured stream name.
     * For superstream mode, discovers partition streams from the broker.
     */
    private List<String> discoverStreams() {
        if (options.isStreamMode()) {
            return List.of(options.getStream());
        }

        List<String> partitions = SuperStreamPartitionDiscovery.discoverPartitions(
                options, options.getSuperStream());
        return resolveSuperStreamPartitions(partitions);
    }

    List<String> resolveSuperStreamPartitionsForTests(List<String> partitions) {
        return resolveSuperStreamPartitions(partitions);
    }

    private List<String> resolveSuperStreamPartitions(List<String> partitions) {
        if (partitions.isEmpty()) {
            if (options.isFailOnDataLoss()) {
                throw new IllegalStateException(
                        "Superstream '" + options.getSuperStream() +
                                "' has no partition streams");
            }
            LOG.warn("Superstream '{}' currently has no partition streams; returning empty topology "
                            + "because failOnDataLoss=false",
                    options.getSuperStream());
            return List.of();
        }
        LOG.info("Discovered {} partition streams for superstream '{}'",
                partitions.size(), options.getSuperStream());
        return partitions;
    }

    /**
     * Resolve starting and ending offsets for each stream.
     *
     * @return map of stream name to [startOffset, endOffset) pairs;
     *         streams with zero messages are excluded
     */
    private Map<String, long[]> resolveOffsetRanges(List<String> streams) {
        EnvironmentPool pool = EnvironmentPool.getInstance();
        Environment env = pool.acquire(options);
        try {
            Map<String, long[]> ranges = resolveOffsetRanges(env, streams);
            if (ranges.isEmpty()) {
                LOG.info("All streams are empty; batch will produce zero rows");
            }
            return ranges;
        } finally {
            pool.release(options);
        }
    }

    private Map<String, long[]> resolveOffsetRanges(Environment env, List<String> streams) {
        Map<String, long[]> ranges = new LinkedHashMap<>();
        int parallelism = Math.min(streams.size(), MAX_BROKER_HELPER_PARALLELISM);
        if (parallelism <= 1) {
            for (String stream : streams) {
                long[] range = resolveStreamOffsetRange(env, stream);
                if (range != null) {
                    ranges.put(stream, range);
                }
            }
            return ranges;
        }

        // Each task counts down its latch from a `finally` block, so the latch reaching
        // zero is a reliable signal that the task body has truly exited (success, error,
        // or interrupt) and is no longer using the Environment. `Future.cancel(true)`
        // alone flips a cancelled future to done immediately and `Future.get` then
        // returns CancellationException without verifying the underlying thread has
        // stopped — hence the latch.
        Map<String, Future<long[]>> futures = new LinkedHashMap<>();
        Map<String, CountDownLatch> taskExited = new LinkedHashMap<>();
        for (String stream : streams) {
            CountDownLatch latch = new CountDownLatch(1);
            taskExited.put(stream, latch);
            futures.put(stream, rangeResolverExecutor().submit(() -> {
                try {
                    return resolveStreamOffsetRange(env, stream);
                } finally {
                    latch.countDown();
                }
            }));
        }
        try {
            for (Map.Entry<String, Future<long[]>> entry : futures.entrySet()) {
                long[] range = awaitResolvedRange(entry.getKey(), entry.getValue());
                if (range != null) {
                    ranges.put(entry.getKey(), range);
                }
            }
            return ranges;
        } catch (RuntimeException | Error fail) {
            // A resolver failed; cancel all peers and wait for their bodies to exit
            // (latch countdown) before letting the outer `finally` release the
            // Environment, so an in-flight task cannot keep using a closed Environment.
            cancelAndAwaitResolverTermination(futures, taskExited);
            throw fail;
        }
    }

    private static void cancelAndAwaitResolverTermination(
            Map<String, Future<long[]>> futures,
            Map<String, CountDownLatch> taskExited) {
        for (Future<long[]> future : futures.values()) {
            future.cancel(true);
        }
        long deadlineNanos = System.nanoTime()
                + TimeUnit.MILLISECONDS.toNanos(RESOLVER_DRAIN_TIMEOUT_MS);
        for (Map.Entry<String, CountDownLatch> entry : taskExited.entrySet()) {
            long remainingNanos = deadlineNanos - System.nanoTime();
            if (remainingNanos <= 0) {
                LOG.debug("Range resolver for stream '{}' did not terminate within {} ms after cancel; "
                                + "abandoning to avoid blocking planning",
                        entry.getKey(), RESOLVER_DRAIN_TIMEOUT_MS);
                continue;
            }
            try {
                if (!entry.getValue().await(remainingNanos, TimeUnit.NANOSECONDS)) {
                    LOG.debug("Range resolver for stream '{}' did not exit within drain budget",
                            entry.getKey());
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }

    private long[] awaitResolvedRange(String stream, Future<long[]> future) {
        try {
            return future.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(
                    "Interrupted while resolving offset range for stream '" + stream + "'", e);
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof RuntimeException runtimeException) {
                throw runtimeException;
            }
            if (cause instanceof Error error) {
                throw error;
            }
            throw new IllegalStateException(
                    "Failed to resolve offset range for stream '" + stream + "'", cause);
        }
    }

    /**
     * Resolve the offset range for a single stream.
     *
     * @return [startOffset, endOffset) or null if the stream is empty
     */
    private long[] resolveStreamOffsetRange(Environment env, String stream) {
        StreamStats stats;
        try {
            stats = env.queryStreamStats(stream);
        } catch (StreamDoesNotExistException e) {
            // Single stream mode: always fail fast (configuration error)
            if (options.isStreamMode()) {
                throw new IllegalStateException(
                        "Stream '" + stream + "' does not exist. " +
                                "Verify the stream name is correct.", e);
            }
            // Superstream partition streams: respect failOnDataLoss
            if (options.isFailOnDataLoss()) {
                throw new IllegalStateException(
                        "Partition stream '" + stream + "' does not exist. " +
                                "Set failOnDataLoss=false to skip missing partition streams.", e);
            }
            LOG.warn("Partition stream '{}' does not exist, skipping (failOnDataLoss=false)", stream);
            return null;
        } catch (Exception e) {
            // failOnDataLoss controls retention/topology data loss handling.
            // Operational failures (auth/TLS/connectivity/protocol) must fail fast.
            throw new IllegalStateException(
                    "Failed to query stream stats for '" + stream + "'", e);
        }

        long firstAvailable;
        try {
            firstAvailable = stats.firstOffset();
        } catch (NoOffsetException e) {
            // Stream is empty
            LOG.debug("Stream '{}' is empty (no first offset)", stream);
            return null;
        }

        long startOffset = resolveStartOffset(env, stream, firstAvailable, stats);
        long endOffset = resolveEndOffset(env, stream, stats);

        // Validate explicit offset ranges (Kafka-compatible behavior).
        if (options.getStartingOffsets() == StartingOffsetsMode.OFFSET
                && options.getEndingOffsets() == EndingOffsetsMode.OFFSET
                && startOffset > endOffset) {
            throw new IllegalArgumentException(
                    "Requested startingOffset " + startOffset
                            + " is greater than endingOffset " + endOffset
                            + " for stream '" + stream + "'.");
        }

        // Handle data loss: start offset before first available
        if (startOffset < firstAvailable) {
            if (options.isFailOnDataLoss()) {
                throw new IllegalStateException(
                        "Requested start offset " + startOffset +
                                " is before the first available offset " + firstAvailable +
                                " in stream '" + stream + "'. Data may have been lost." +
                                " Set failOnDataLoss=false to skip lost data.");
            }
            LOG.warn("Start offset {} is before first available {} in stream '{}', advancing",
                    startOffset, firstAvailable, stream);
            startOffset = firstAvailable;
        }

        if (startOffset >= endOffset) {
            LOG.debug("Stream '{}' has no data in range [{}, {})", stream, startOffset, endOffset);
            return null;
        }

        return new long[]{startOffset, endOffset};
    }

    private long resolveStartOffset(Environment env, String stream, long firstAvailable, StreamStats stats) {
        // Check for per-stream timestamp override
        if (options.getStartingOffsets() == StartingOffsetsMode.TIMESTAMP) {
            Map<String, Long> perStreamTs = options.getStartingOffsetsByTimestamp();
            if (perStreamTs != null && perStreamTs.containsKey(stream)) {
                return resolveTimestampStartingOffset(
                        env, stream, firstAvailable, stats, perStreamTs.get(stream));
            }
        }
        return switch (options.getStartingOffsets()) {
            case EARLIEST -> firstAvailable;
            case LATEST -> resolveLatestOffset(env, stream, stats);
            case OFFSET -> options.getStartingOffset();
            case TIMESTAMP -> resolveTimestampStartingOffset(
                    env, stream, firstAvailable, stats, options.getStartingTimestamp());
        };
    }

    private long resolveTimestampStartingOffset(
            Environment env, String stream, long firstAvailable, StreamStats stats,
            long timestamp) {
        // Same retry-with-rebuild rationale as resolveTimestampEndingOffset (BUG-4-3):
        // a slow consumer attach on a remote broker can drain the entire pollTimeoutMs
        // budget before the probe even gets a chance to observe a message.
        long totalBudgetMs = timestampProbeTimeoutMs();
        long[] attemptBudgetsMs = splitProbeBudget(totalBudgetMs);
        Throwable lastError = null;
        for (int attempt = 0; attempt < attemptBudgetsMs.length; attempt++) {
            long attemptBudgetMs = attemptBudgetsMs[attempt];
            BlockingQueue<Long> observedOffsets = new LinkedBlockingQueue<>();
            com.rabbitmq.stream.Consumer probe = null;
            try {
                probe = env.consumerBuilder()
                        .stream(stream)
                        .offset(com.rabbitmq.stream.OffsetSpecification.timestamp(timestamp))
                        .noTrackingStrategy()
                        .messageHandler((context, message) -> observedOffsets.offer(context.offset()))
                        .flow()
                        .initialCredits(1)
                        .strategy(ConsumerFlowStrategy.creditOnChunkArrival(1))
                        .builder()
                        .build();

                Long observed = observedOffsets.poll(attemptBudgetMs, TimeUnit.MILLISECONDS);
                if (observed != null) {
                    return Math.max(firstAvailable, observed);
                }
                LOG.debug("Timestamp start probe attempt {} of {} timed out after {} ms for stream '{}'",
                        attempt + 1, attemptBudgetsMs.length, attemptBudgetMs, stream);
            } catch (NoOffsetException e) {
                return handleTimestampStartNoMatch(env, stream, firstAvailable, stats, timestamp);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(
                        "Interrupted resolving timestamp start offset for stream '" + stream + "'", e);
            } catch (IllegalStateException e) {
                throw e;
            } catch (Exception e) {
                LOG.debug("Timestamp start probe attempt {} of {} failed for stream '{}': {}",
                        attempt + 1, attemptBudgetsMs.length, stream, e.getMessage());
                lastError = e;
            } finally {
                if (probe != null) {
                    try {
                        probe.close();
                    } catch (Exception e) {
                        LOG.debug("Error closing timestamp-start probe consumer for stream '{}'", stream, e);
                    }
                }
            }
        }
        TimestampResolutionTimeoutException timeout = new TimestampResolutionTimeoutException(
                "Timed out resolving starting timestamp " + timestamp
                        + " for stream '" + stream + "' after " + totalBudgetMs
                        + " ms across " + attemptBudgetsMs.length + " probe attempts. Increase '"
                        + ConnectorOptions.POLL_TIMEOUT_MS + "' to extend the probe budget.");
        if (lastError != null) {
            timeout.addSuppressed(lastError);
        }
        throw timeout;
    }

    /**
     * Split the timestamp-probe budget across multiple consumer-rebuild attempts so a
     * single slow attach (remote broker behind a load balancer) does not consume the
     * full budget before any message is observed. Returns the per-attempt budgets in
     * milliseconds; each attempt rebuilds the probe consumer.
     *
     * <p>For very small total budgets we fall back to a single shot — splitting a
     * 250 ms budget into pieces would not give any attempt enough time to attach.
     */
    static long[] splitProbeBudget(long totalBudgetMs) {
        if (totalBudgetMs < 1_000L) {
            return new long[]{Math.max(1L, totalBudgetMs)};
        }
        // Two attempts at 50% each strikes a balance: the second attempt lets a transient
        // slow attach be retried without halving small budgets so much that neither attempt
        // can observe a message. The static positive-test cases use 1000 ms which now
        // becomes 500 ms + 500 ms — still ample for an in-process broker mock to deliver.
        long share = Math.max(500L, totalBudgetMs / 2L);
        long second = totalBudgetMs - share;
        if (second <= 0L) {
            return new long[]{totalBudgetMs};
        }
        return new long[]{share, second};
    }

    private long handleTimestampStartNoMatch(
            Environment env, String stream, long firstAvailable, StreamStats stats, long timestamp) {
        if (options.getStartingOffsetsByTimestampStrategy()
                == StartingOffsetsByTimestampStrategy.LATEST) {
            long tailExclusive = resolveLatestOffset(env, stream, stats);
            return Math.max(firstAvailable, tailExclusive);
        }
        throw new IllegalStateException(
                "No offset matched the requested starting timestamp " + timestamp
                        + " for stream '" + stream + "'. "
                        + "Set '" + ConnectorOptions.STARTING_OFFSETS_BY_TIMESTAMP_STRATEGY
                        + "=latest' to fall back to tail.");
    }

    private long resolveEndOffset(Environment env, String stream, StreamStats stats) {
        if (options.getEndingOffsets() == EndingOffsetsMode.TIMESTAMP) {
            // Check for per-stream ending timestamp override
            Map<String, Long> perStreamTs = options.getEndingOffsetsByTimestamp();
            if (perStreamTs != null && perStreamTs.containsKey(stream)) {
                return resolveTimestampEndingOffset(env, stream, perStreamTs.get(stream));
            }
            return resolveTimestampEndingOffset(env, stream, options.getEndingTimestamp());
        }

        if (options.getEndingOffsets() == EndingOffsetsMode.OFFSET) {
            return options.getEndingOffset();
        }

        // LATEST for batch reads: bind eagerly so every task observes the same end.
        // Late binding to MAX_VALUE caused per-task tail probes that diverged on slow-task
        // startup, producing a non-snapshot read. Real-time streaming still uses late
        // binding via a separate code path.
        return resolveLatestOffset(env, stream, stats);
    }

    private long resolveLatestOffset(Environment env, String stream, StreamStats stats) {
        // endingOffsets=latest: use the most recent tail we can observe.
        // Stream stats can lag behind newly published messages, so keep the probe and
        // take max(statsTail, probedTail). Bound probe latency to avoid planning stalls.
        long statsTail = resolveTailOffset(stats);
        long probedTail = probeTailOffsetFromLastMessageWithTimeout(
                env, stream, TAIL_PROBE_TIMEOUT_MS);
        return Math.max(statsTail, probedTail);
    }

    /**
     * Resolve an ending offset by timestamp using a probe consumer.
     * Returns the offset of the first message whose timestamp is &gt;= the requested
     * timestamp, used as an exclusive upper bound.
     *
     * <p>Per-message AMQP {@code creation_time} (when the publisher set it) takes
     * precedence — that gives a message-precise boundary even within a chunk that
     * straddles the bound, which is the {@code GPT-9} case we are fixing. When
     * {@code creation_time} is unset on a message, we fall back to the chunk-level
     * {@link com.rabbitmq.stream.MessageHandler.Context#timestamp() context
     * timestamp}, in which case the matching chunk is excluded as a whole — that
     * matches the original well-tested behavior on the IT path with broker-assigned
     * chunk timestamps.
     *
     * <p>If the probe times out before observing any message at/after the timestamp,
     * {@link TimestampResolutionTimeoutException} is thrown. The planner cannot prove
     * all data is before the cutoff, so silently returning tail would over-include records.
     *
     * <p>Before launching the timestamp probe we run a short {@link #proveAllBeforeCutoff}
     * pre-check: on a sparse stream where the last chunk has timestamp &lt; cutoff,
     * we already know all currently-available messages are before the cutoff, and the
     * tail (= last_offset + 1) is the deterministic exclusive end. This closes the
     * BUG-4-3 / BUG-4-5 regression for sparse streams (e.g. {@code ls-test}) where
     * {@code OffsetSpecification.timestamp(cutoff)} attaches at the tail and waits
     * indefinitely for a chunk that never arrives. The triage (GPT3-2) explicitly
     * suggested this "prove absence" path as an alternative to the throw-on-timeout
     * fix landed in commit {@code 8835fe4}.
     */
    private long resolveTimestampEndingOffset(Environment env, String stream, long timestamp) {
        // Pre-check: prove-absence by inspecting the broker tail. If the last
        // currently-available message has a timestamp strictly before the cutoff,
        // tail is provably the exclusive end and we can skip the timestamp probe
        // entirely. Bounded by a small fixed share of the budget so a slow attach
        // does not delay the regular probe path on streams where this check fails.
        long totalBudgetMs = timestampProbeTimeoutMs();
        long absenceProbeBudgetMs = Math.min(
                totalBudgetMs,
                Math.min(MAX_PROVE_ABSENCE_BUDGET_MS,
                         Math.max(MIN_PROVE_ABSENCE_BUDGET_MS, totalBudgetMs / 8L)));
        long provenTail = proveAllBeforeCutoff(env, stream, timestamp, absenceProbeBudgetMs);
        if (provenTail >= 0L) {
            if (provenTail == 0L) {
                LOG.debug("Stream '{}' is empty; returning 0 as proven ending offset", stream);
            } else {
                LOG.debug("Stream '{}' last chunk timestamp < cutoff {}; returning proven tail {}",
                        stream, timestamp, provenTail);
            }
            return provenTail;
        }

        // Slow consumer attach on remote brokers behind a load balancer can eat the entire
        // pollTimeoutMs budget on a single shot, leaving no time for the probe to actually
        // observe a message — the BUG-4-3 / BUG-4-5 symptom. Retry with rebuild so a slow
        // first attach doesn't doom the whole resolution. The total budget is still bounded
        // by pollTimeoutMs; each attempt gets a fresh build() and a share of the budget.
        long[] attemptBudgetsMs = splitProbeBudget(totalBudgetMs);
        Throwable lastError = null;
        for (int attempt = 0; attempt < attemptBudgetsMs.length; attempt++) {
            long attemptBudgetMs = attemptBudgetsMs[attempt];
            AtomicLong firstOffsetAtOrAfter = new AtomicLong(-1L);
            CountDownLatch boundaryFound = new CountDownLatch(1);
            com.rabbitmq.stream.Consumer probe = null;
            try {
                probe = env.consumerBuilder()
                        .stream(stream)
                        .offset(com.rabbitmq.stream.OffsetSpecification.timestamp(timestamp))
                        .noTrackingStrategy()
                        .messageHandler((context, message) -> {
                            if (messageOrChunkTimestamp(context, message) >= timestamp
                                    && firstOffsetAtOrAfter.compareAndSet(-1L, context.offset())) {
                                boundaryFound.countDown();
                            }
                        })
                        .flow()
                        .initialCredits(1)
                        .strategy(ConsumerFlowStrategy.creditOnChunkArrival(1))
                        .builder()
                        .build();

                if (boundaryFound.await(attemptBudgetMs, TimeUnit.MILLISECONDS)) {
                    return firstOffsetAtOrAfter.get();
                }
                LOG.debug("Timestamp end probe attempt {} of {} timed out after {} ms for stream '{}'",
                        attempt + 1, attemptBudgetsMs.length, attemptBudgetMs, stream);
            } catch (NoOffsetException e) {
                // Stream is empty — broker-confirmed no-match.
                return 0;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(
                        "Interrupted resolving ending timestamp for stream '" + stream + "'", e);
            } catch (Exception e) {
                LOG.debug("Timestamp end probe attempt {} of {} failed for stream '{}': {}",
                        attempt + 1, attemptBudgetsMs.length, stream, e.getMessage());
                lastError = e;
            } finally {
                closeProbeQuietly(probe, stream);
            }
        }
        // All attempts exhausted without observing a message at-or-after the cutoff. Per
        // the GPT3-2 triage decision, we cannot prove all data is before the cutoff, so
        // failing here is correct (silent fall-back to tail risks over-including).
        TimestampResolutionTimeoutException timeout = new TimestampResolutionTimeoutException(
                "Timed out resolving ending timestamp " + timestamp
                        + " for stream '" + stream + "' after " + totalBudgetMs
                        + " ms across " + attemptBudgetsMs.length + " probe attempts. Increase '"
                        + ConnectorOptions.POLL_TIMEOUT_MS + "' to extend the probe budget.");
        if (lastError != null && lastError != timeout) {
            timeout.addSuppressed(lastError);
        }
        throw timeout;
    }

    /**
     * Try to prove every currently-available message is strictly before {@code cutoff}
     * by inspecting the broker tail. Used as a pre-check by
     * {@link #resolveTimestampEndingOffset(Environment, String, long)} so a sparse
     * stream (no chunk has {@code chunk_ts >= cutoff}) does not waste the full
     * {@code pollTimeoutMs} on a probe that can never observe a message at-or-after
     * the cutoff.
     *
     * <p>Returns:
     * <ul>
     *   <li>{@code last_offset + 1} (≥ 0) when the broker tail is provably before the
     *       cutoff. Safe to use as the exclusive end.</li>
     *   <li>{@code 0} when the stream is broker-confirmed empty
     *       ({@link NoOffsetException}).</li>
     *   <li>{@code -1L} when the proof is inconclusive — fall through to the regular
     *       timestamp probe (last message has timestamp ≥ cutoff, probe error, or
     *       observation budget exhausted before any chunk arrived).</li>
     * </ul>
     *
     * <p>The probe attaches at {@link com.rabbitmq.stream.OffsetSpecification#last()}
     * which delivers exactly the last chunk, then drains the chunk under a brief idle
     * grace so we capture the highest-offset message in the chunk.
     *
     * <p><b>Timestamp field used:</b> only the chunk-level
     * {@link com.rabbitmq.stream.MessageHandler.Context#timestamp() context.timestamp()}
     * (server-assigned write time), never per-message AMQP {@code creation_time}.
     * {@code OffsetSpecification.timestamp(cutoff)} attaches at the first chunk whose
     * server-write time ≥ cutoff; if no such chunk exists the regular probe would wait
     * indefinitely. Using {@code creation_time} here would be incorrect: a publisher
     * can backdate {@code creation_time}, so the last message could have a
     * {@code creation_time} before the cutoff even though the chunk that contains it
     * has a server-write time ≥ cutoff — incorrectly treating that chunk as proved
     * absent.
     */
    private static long proveAllBeforeCutoff(
            Environment env, String stream, long cutoff, long budgetMs) {
        AtomicLong lastObservedOffset = new AtomicLong(-1L);
        AtomicLong lastObservedTimestamp = new AtomicLong(Long.MIN_VALUE);
        AtomicLong lastUpdateNanos = new AtomicLong(Long.MIN_VALUE);
        java.util.concurrent.ArrayBlockingQueue<Boolean> firstObserved =
                new java.util.concurrent.ArrayBlockingQueue<>(1);
        com.rabbitmq.stream.Consumer probe = null;
        try {
            probe = env.consumerBuilder()
                    .stream(stream)
                    .offset(com.rabbitmq.stream.OffsetSpecification.last())
                    .noTrackingStrategy()
                    .messageHandler((context, message) -> {
                        long off = context.offset();
                        // Use chunk-level timestamp only — see Javadoc above.
                        long ts = context.timestamp();
                        // Always advance to the LATEST observed; chunk delivery is in
                        // ascending offset order so the final values reflect the last
                        // message in the chunk.
                        lastObservedOffset.set(off);
                        lastObservedTimestamp.set(ts);
                        lastUpdateNanos.set(System.nanoTime());
                        firstObserved.offer(Boolean.TRUE);
                    })
                    .flow()
                    .initialCredits(1)
                    // No context.processed() in the handler; creditOnChunkArrival keeps
                    // chunk delivery moving without depending on processed().
                    .strategy(ConsumerFlowStrategy.creditOnChunkArrival(1))
                    .builder()
                    .build();

            Boolean first = firstObserved.poll(budgetMs, TimeUnit.MILLISECONDS);
            if (first == null) {
                // No chunk delivered within the budget — broker may be slow to attach,
                // OR the stream is genuinely empty but the broker hasn't surfaced
                // NoOffsetException synchronously. Inconclusive: fall through.
                return -1L;
            }

            // Idle-stabilization drain: wait until no new message arrives for
            // IDLE_GRACE_NS, bounded by drainDeadlineMs. A fixed Thread.sleep is not
            // reliable because message callbacks are async; on a large last chunk the
            // first observed message may not be the last one in the chunk. Polling on
            // lastUpdateNanos ensures we only declare "stable" once the callback thread
            // has actually gone quiet.
            final long IDLE_GRACE_NS = 20_000_000L; // 20 ms
            long drainBudgetMs = Math.min(150L, Math.max(40L, budgetMs / 8L));
            long drainDeadlineMs = System.currentTimeMillis() + drainBudgetMs;
            while (System.currentTimeMillis() < drainDeadlineMs) {
                long nsSinceLastUpdate = System.nanoTime() - lastUpdateNanos.get();
                if (nsSinceLastUpdate >= IDLE_GRACE_NS) {
                    break;
                }
                Thread.sleep(5L);
            }

            long lastTs = lastObservedTimestamp.get();
            long lastOff = lastObservedOffset.get();
            if (lastTs == Long.MIN_VALUE || lastOff < 0L) {
                return -1L;
            }
            if (lastTs < cutoff) {
                return lastOff + 1L;
            }
            return -1L;
        } catch (NoOffsetException e) {
            // Broker-confirmed empty stream — exclusive end is 0.
            return 0L;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return -1L;
        } catch (Exception e) {
            LOG.debug("Prove-absence probe failed for stream '{}': {}",
                    stream, e.getMessage());
            return -1L;
        } finally {
            closeProbeQuietly(probe, stream);
        }
    }

    /**
     * Per-message AMQP {@code creation_time} when the publisher set it, otherwise the
     * chunk-level context timestamp. {@code Properties#getCreationTime()} returns a
     * negative value when the property is unset (see {@code MessageToRowConverter}).
     */
    private static long messageOrChunkTimestamp(
            com.rabbitmq.stream.MessageHandler.Context context,
            Message message) {
        if (message != null) {
            Properties props = message.getProperties();
            if (props != null) {
                long creationTime = props.getCreationTime();
                if (creationTime >= 0L) {
                    return creationTime;
                }
            }
        }
        return context.timestamp();
    }

    private static void closeProbeQuietly(com.rabbitmq.stream.Consumer probe, String stream) {
        if (probe == null) {
            return;
        }
        try {
            probe.close();
        } catch (Exception ex) {
            LOG.debug("Error closing timestamp-end probe consumer for stream '{}'",
                    stream, ex);
        }
    }

    private long timestampProbeTimeoutMs() {
        return Math.max(MIN_TIMESTAMP_PROBE_TIMEOUT_MS, options.getPollTimeoutMs());
    }

    private long probeTailOffsetFromLastMessageWithTimeout(
            Environment env, String stream, long timeoutMs) {
        Future<Long> future = tailProbeTimeoutExecutor().submit(
                () -> probeTailOffsetFromLastMessage(env, stream));
        try {
            return future.get(timeoutMs, TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            future.cancel(true);
            LOG.debug("Tail probe timed out after {} ms for stream '{}'", timeoutMs, stream);
            return 0;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            future.cancel(true);
            return 0;
        } catch (ExecutionException e) {
            LOG.debug("Tail probe failed for stream '{}': {}", stream, e.getMessage());
            return 0;
        }
    }

    long[] resolveStreamOffsetRangeForTests(Environment env, String stream) {
        return resolveStreamOffsetRange(env, stream);
    }

    Map<String, long[]> resolveOffsetRangesForTests(Environment env, List<String> streams) {
        return resolveOffsetRanges(env, streams);
    }

    long resolveStartOffsetForTests(Environment env, String stream, long firstAvailable, StreamStats stats) {
        return resolveStartOffset(env, stream, firstAvailable, stats);
    }

    long resolveEndOffsetForTests(Environment env, String stream, StreamStats stats) {
        return resolveEndOffset(env, stream, stats);
    }

    static long resolveTailOffset(StreamStats stats) {
        return RabbitMQMicroBatchStream.resolveTailOffset(stats);
    }

    private long probeTailOffsetFromLastMessage(Environment env, String stream) {
        try {
            long maxSeen = BaseRabbitMQMicroBatchStream.probeLastMessageOffsetInclusive(
                    env, stream, BaseRabbitMQMicroBatchStream.TAIL_PROBE_WAIT_MS);
            if (maxSeen < 0) {
                return 0;
            }
            return maxSeen + 1;
        } catch (NoOffsetException e) {
            return 0;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return 0;
        } catch (Exception e) {
            LOG.debug("Unable to probe last message tail offset for stream '{}': {}",
                    stream, e.getMessage());
            return 0;
        }
    }
}
