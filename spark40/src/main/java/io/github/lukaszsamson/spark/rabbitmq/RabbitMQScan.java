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

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
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
    private static final long RESOLVER_DRAIN_TIMEOUT_MS = 5_000L;
    private static final int TAIL_PROBE_EXECUTOR_PARALLELISM = Math.max(
            1,
            Math.min(Runtime.getRuntime().availableProcessors(),
                    StoredOffsetLookup.MAX_CONCURRENT_LOOKUPS));
    private static final int RANGE_RESOLVER_EXECUTOR_PARALLELISM = Math.max(
            1,
            Math.min(Runtime.getRuntime().availableProcessors(),
                    StoredOffsetLookup.MAX_CONCURRENT_LOOKUPS));
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
        int parallelism = Math.min(streams.size(), StoredOffsetLookup.MAX_CONCURRENT_LOOKUPS);
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
        BlockingQueue<Long> observedOffsets = new LinkedBlockingQueue<>();
        com.rabbitmq.stream.Consumer probe = null;
        try {
            probe = env.consumerBuilder()
                    .stream(stream)
                    .offset(com.rabbitmq.stream.OffsetSpecification.timestamp(
                            timestamp))
                    .noTrackingStrategy()
                    .messageHandler((context, message) -> observedOffsets.offer(context.offset()))
                    .flow()
                    .initialCredits(1)
                    .strategy(ConsumerFlowStrategy.creditOnChunkArrival(1))
                    .builder()
                    .build();

            Long observed = observedOffsets.poll(timestampProbeTimeoutMs(), TimeUnit.MILLISECONDS);
            if (observed != null) {
                return Math.max(firstAvailable, observed);
            }
            // Probe budget exhausted without a result. Falling back here would silently
            // skip or over-include records, so fail planning so the operator can extend
            // the budget.
            throw new TimestampResolutionTimeoutException(
                    "Timed out resolving starting timestamp " + timestamp
                            + " for stream '" + stream + "' after " + timestampProbeTimeoutMs()
                            + " ms. Increase '" + ConnectorOptions.POLL_TIMEOUT_MS
                            + "' to extend the probe budget.");
        } catch (NoOffsetException e) {
            return handleTimestampStartNoMatch(env, stream, firstAvailable, stats, timestamp);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(
                    "Interrupted resolving timestamp start offset for stream '" + stream + "'", e);
        } catch (IllegalStateException e) {
            throw e;
        } catch (Exception e) {
            LOG.warn("Failed to resolve timestamp start offset for stream '{}': {}",
                    stream, e.getMessage());
            throw new IllegalStateException(
                    "Failed to resolve timestamp start offset for stream '" + stream + "'", e);
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

    private long timestampProbeTimeoutMs() {
        return Math.max(MIN_TIMESTAMP_PROBE_TIMEOUT_MS, options.getPollTimeoutMs());
    }

    private long resolveEndOffset(Environment env, String stream, StreamStats stats) {
        Map<String, Long> perStreamTs = options.getEndingOffsetsByTimestamp();
        if (perStreamTs != null && perStreamTs.containsKey(stream)) {
            return resolveTimestampEndingOffset(env, stream, perStreamTs.get(stream));
        }

        if (options.getEndingOffsets() == EndingOffsetsMode.OFFSET) {
            return options.getEndingOffset();
        }

        if (options.getEndingOffsets() == EndingOffsetsMode.TIMESTAMP) {
            return resolveTimestampEndingOffset(env, stream, options.getEndingTimestamp());
        }

        // LATEST for batch reads: bind eagerly so every task observes the same end.
        // Late binding to MAX_VALUE caused per-task tail probes that diverged on slow-task
        // startup, producing a non-snapshot read.
        return resolveLatestOffset(env, stream, stats);
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
     * <p>If only earlier messages are observed (none at/after the timestamp), all
     * available data is before the cutoff and the stream tail is returned.
     */
    private long resolveTimestampEndingOffset(Environment env, String stream, long timestamp) {
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

            if (boundaryFound.await(timestampProbeTimeoutMs(), TimeUnit.MILLISECONDS)) {
                return firstOffsetAtOrAfter.get();
            }
            // Probe budget exhausted without observing a message at or after the
            // requested timestamp. We cannot prove all data is before the cutoff,
            // so falling back to tail risks over-including records published after
            // the bound. Fail planning instead.
            throw new TimestampResolutionTimeoutException(
                    "Timed out resolving ending timestamp " + timestamp
                            + " for stream '" + stream + "' after " + timestampProbeTimeoutMs()
                            + " ms. Increase '" + ConnectorOptions.POLL_TIMEOUT_MS
                            + "' to extend the probe budget.");
        } catch (NoOffsetException e) {
            return 0;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(
                    "Interrupted resolving ending timestamp for stream '" + stream + "'", e);
        } catch (TimestampResolutionTimeoutException e) {
            throw e;
        } catch (Exception e) {
            LOG.warn("Failed to resolve timestamp end offset for stream '{}': {}",
                    stream, e.getMessage());
            throw new IllegalStateException(
                    "Failed to resolve timestamp end offset for stream '" + stream + "'", e);
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

    private long resolveLatestOffset(Environment env, String stream, StreamStats stats) {
        // endingOffsets=latest: combine stats-derived tail with a direct last-message probe
        // to avoid underestimating on older broker/client combinations.
        long statsTail = resolveTailOffset(stats);
        long probedTail = probeTailOffsetFromLastMessageWithTimeout(
                env, stream, TAIL_PROBE_TIMEOUT_MS);
        return Math.max(statsTail, probedTail);
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
        BlockingQueue<Long> observedOffsets = new LinkedBlockingQueue<>();
        com.rabbitmq.stream.Consumer probe = null;
        try {
            probe = env.consumerBuilder()
                    .stream(stream)
                    .offset(com.rabbitmq.stream.OffsetSpecification.last())
                    .noTrackingStrategy()
                    .messageHandler((context, message) -> observedOffsets.offer(context.offset()))
                    .flow()
                    .initialCredits(1)
                    .strategy(ConsumerFlowStrategy.creditOnChunkArrival(1))
                    .builder()
                    .build();

            Long first = observedOffsets.poll(250, TimeUnit.MILLISECONDS);
            if (first == null) {
                return 0;
            }

            long maxSeen = first;
            while (true) {
                Long next = observedOffsets.poll(40, TimeUnit.MILLISECONDS);
                if (next == null) {
                    break;
                }
                if (next > maxSeen) {
                    maxSeen = next;
                }
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
        } finally {
            if (probe != null) {
                try {
                    probe.close();
                } catch (Exception e) {
                    LOG.debug("Error closing tail probe consumer for stream '{}'", stream, e);
                }
            }
        }
    }
}
