package io.github.lukaszsamson.spark.rabbitmq;

import com.rabbitmq.stream.ConsumerFlowStrategy;
import com.rabbitmq.stream.Environment;
import com.rabbitmq.stream.NoOffsetException;
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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

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
    // Shared pool to avoid per-stream short-lived executor churn during tail probing.
    private static final ExecutorService TAIL_PROBE_TIMEOUT_EXECUTOR =
            Executors.newFixedThreadPool(TAIL_PROBE_EXECUTOR_PARALLELISM, r -> {
                Thread t = new Thread(
                        r,
                        "rabbitmq-scan-tail-probe-" + TAIL_PROBE_THREAD_COUNTER.incrementAndGet());
                t.setDaemon(true);
                return t;
            });
    // Shared pool to avoid per-call range resolver thread churn.
    private static final ExecutorService RANGE_RESOLVER_EXECUTOR =
            Executors.newFixedThreadPool(RANGE_RESOLVER_EXECUTOR_PARALLELISM, r -> {
                Thread t = new Thread(
                        r,
                        "rabbitmq-scan-range-resolver-"
                                + RANGE_RESOLVER_THREAD_COUNTER.incrementAndGet());
                t.setDaemon(true);
                return t;
            });
    static {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            TAIL_PROBE_TIMEOUT_EXECUTOR.shutdownNow();
            RANGE_RESOLVER_EXECUTOR.shutdownNow();
        }, "rabbitmq-scan-executors-shutdown"));
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

        Map<String, Future<long[]>> futures = new LinkedHashMap<>();
        for (String stream : streams) {
            futures.put(stream, RANGE_RESOLVER_EXECUTOR.submit(() -> resolveStreamOffsetRange(env, stream)));
        }
        for (Map.Entry<String, Future<long[]>> entry : futures.entrySet()) {
            long[] range = awaitResolvedRange(entry.getKey(), entry.getValue());
            if (range != null) {
                ranges.put(entry.getKey(), range);
            }
        }
        return ranges;
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
            if (options.isFailOnDataLoss()) {
                throw new IllegalStateException(
                        "Failed to query stream stats for '" + stream + "'", e);
            }
            LOG.warn("Failed to query stream stats for '{}', skipping: {}", stream, e.getMessage());
            return null;
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
        Map<String, Long> perStreamTs = options.getStartingOffsetsByTimestamp();
        if (perStreamTs != null && perStreamTs.containsKey(stream)) {
            return resolveTimestampStartingOffset(
                    env, stream, firstAvailable, stats, perStreamTs.get(stream));
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
                    .offset(com.rabbitmq.stream.OffsetSpecification.timestamp(timestamp))
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
            // No messages at/after timestamp — fail fast so the user gets a clear error
            // instead of silently empty results.
            throw new RuntimeException(
                    "No messages found at or after timestamp " + timestamp
                            + " in stream '" + stream + "'");
        } catch (NoOffsetException e) {
            // Broker explicitly reports no matching offset for the requested timestamp.
            throw new RuntimeException(
                    "No offset matched from request for timestamp " + timestamp
                            + " in stream '" + stream + "'", e);
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

    private long resolveEndOffset(Environment env, String stream, StreamStats stats) {
        // Check for per-stream ending timestamp override
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

        // LATEST: defer resolution to executor (Kafka-style late binding).
        // Each executor resolves the tail at read time for the freshest possible value.
        //
        // Exception: when startingOffsets=timestamp, the start may resolve to the stream
        // tail (no messages match). With late binding the range [tail, MAX_VALUE) looks
        // non-empty, preventing empty-range detection at plan time. Resolve eagerly so
        // that startOffset >= endOffset correctly identifies the empty range.
        if (options.getStartingOffsets() == StartingOffsetsMode.TIMESTAMP) {
            return resolveLatestOffset(env, stream, stats);
        }
        return Long.MAX_VALUE;
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
     * Finds the first offset at or after the given timestamp, then returns it as an
     * exclusive end offset. If no messages exist at/after the timestamp, returns the
     * stream tail (all available data is before the timestamp).
     */
    private long resolveTimestampEndingOffset(Environment env, String stream, long timestamp) {
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

            Long observed = observedOffsets.poll(timestampProbeTimeoutMs(), TimeUnit.MILLISECONDS);
            if (observed != null) {
                // The broker returns the first chunk at/after the timestamp.
                // Use the observed offset as the exclusive end (messages before this
                // timestamp are included). Note: chunk-granularity boundary may include
                // a few messages slightly past the target timestamp.
                return observed;
            }
            // No messages at/after timestamp: all data is before the cutoff.
            // Use stream tail as ending offset (include everything).
            StreamStats stats = env.queryStreamStats(stream);
            return resolveTailOffset(stats);
        } catch (NoOffsetException e) {
            // Stream is empty
            return 0;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(
                    "Interrupted resolving ending timestamp for stream '" + stream + "'", e);
        } catch (Exception e) {
            LOG.warn("Failed to resolve timestamp end offset for stream '{}': {}",
                    stream, e.getMessage());
            throw new IllegalStateException(
                    "Failed to resolve timestamp end offset for stream '" + stream + "'", e);
        } finally {
            if (probe != null) {
                try {
                    probe.close();
                } catch (Exception ex) {
                    LOG.debug("Error closing timestamp-end probe consumer for stream '{}'",
                            stream, ex);
                }
            }
        }
    }

    private long timestampProbeTimeoutMs() {
        return Math.max(MIN_TIMESTAMP_PROBE_TIMEOUT_MS, options.getPollTimeoutMs());
    }

    private long probeTailOffsetFromLastMessageWithTimeout(
            Environment env, String stream, long timeoutMs) {
        Future<Long> future = TAIL_PROBE_TIMEOUT_EXECUTOR.submit(
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
