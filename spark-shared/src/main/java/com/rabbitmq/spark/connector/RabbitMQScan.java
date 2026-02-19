package com.rabbitmq.spark.connector;

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
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

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

        // Superstream: discover partition streams using low-level Client
        {
            List<String> partitions = SuperStreamPartitionDiscovery.discoverPartitions(
                    options, options.getSuperStream());
            if (partitions.isEmpty()) {
                throw new IllegalStateException(
                        "Superstream '" + options.getSuperStream() +
                                "' has no partition streams");
            }
            LOG.info("Discovered {} partition streams for superstream '{}'",
                    partitions.size(), options.getSuperStream());
            return partitions;
        } // end superstream discovery
    }

    /**
     * Resolve starting and ending offsets for each stream.
     *
     * @return map of stream name to [startOffset, endOffset) pairs;
     *         streams with zero messages are excluded
     */
    private Map<String, long[]> resolveOffsetRanges(List<String> streams) {
        Map<String, long[]> ranges = new LinkedHashMap<>();

        try (Environment env = EnvironmentBuilderHelper.buildEnvironment(options)) {
            for (String stream : streams) {
                long[] range = resolveStreamOffsetRange(env, stream);
                if (range != null) {
                    ranges.put(stream, range);
                }
            }
        }

        if (ranges.isEmpty()) {
            LOG.info("All streams are empty; batch will produce zero rows");
        }

        return ranges;
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
        return switch (options.getStartingOffsets()) {
            case EARLIEST -> firstAvailable;
            case LATEST -> resolveEndOffset(env, stream, stats);
            case OFFSET -> options.getStartingOffset();
            case TIMESTAMP -> resolveTimestampStartingOffset(env, stream, firstAvailable, stats);
        };
    }

    private long resolveTimestampStartingOffset(
            Environment env, String stream, long firstAvailable, StreamStats stats) {
        BlockingQueue<Long> observedOffsets = new LinkedBlockingQueue<>();
        com.rabbitmq.stream.Consumer probe = null;
        try {
            probe = env.consumerBuilder()
                    .stream(stream)
                    .offset(com.rabbitmq.stream.OffsetSpecification.timestamp(
                            options.getStartingTimestamp()))
                    .noTrackingStrategy()
                    .messageHandler((context, message) -> observedOffsets.offer(context.offset()))
                    .build();

            Long observed = observedOffsets.poll(250, TimeUnit.MILLISECONDS);
            if (observed != null) {
                return Math.max(firstAvailable, observed);
            }
            throw new IllegalStateException(
                    "No offset matched from request for timestamp "
                            + options.getStartingTimestamp()
                            + " in stream '" + stream + "'");
        } catch (NoOffsetException e) {
            throw new IllegalStateException(
                    "No offset matched from request for timestamp "
                            + options.getStartingTimestamp()
                            + " in stream '" + stream + "'", e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(
                    "No offset matched from request for timestamp "
                            + options.getStartingTimestamp()
                            + " in stream '" + stream + "'", e);
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
        if (options.getEndingOffsets() == EndingOffsetsMode.OFFSET) {
            return options.getEndingOffset();
        }

        // endingOffsets=latest: use the most recent tail we can observe.
        // Stream stats can lag behind newly published messages, so keep the probe and
        // take max(statsTail, probedTail). Bound probe latency to avoid planning stalls.
        long statsTail = resolveTailOffset(stats);
        long probedTail = probeTailOffsetFromLastMessageWithTimeout(
                env, stream, TAIL_PROBE_TIMEOUT_MS);
        return Math.max(statsTail, probedTail);
    }

    private long probeTailOffsetFromLastMessageWithTimeout(
            Environment env, String stream, long timeoutMs) {
        ExecutorService executor = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "rabbitmq-scan-tail-probe");
            t.setDaemon(true);
            return t;
        });
        Future<Long> future = executor.submit(() -> probeTailOffsetFromLastMessage(env, stream));
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
        } finally {
            executor.shutdownNow();
        }
    }

    long[] resolveStreamOffsetRangeForTests(Environment env, String stream) {
        return resolveStreamOffsetRange(env, stream);
    }

    long resolveStartOffsetForTests(Environment env, String stream, long firstAvailable, StreamStats stats) {
        return resolveStartOffset(env, stream, firstAvailable, stats);
    }

    long resolveEndOffsetForTests(Environment env, String stream, StreamStats stats) {
        return resolveEndOffset(env, stream, stats);
    }

    static long resolveTailOffset(StreamStats stats) {
        try {
            Method committedOffset = stats.getClass().getMethod("committedOffset");
            Object value = committedOffset.invoke(stats);
            if (value instanceof Number n) {
                return n.longValue() + 1;
            }
        } catch (NoSuchMethodException e) {
            // Older client artifact; fall back to committedChunkId below.
        } catch (InvocationTargetException e) {
            if (!(e.getTargetException() instanceof NoOffsetException)) {
                LOG.debug("committedOffset() lookup failed, falling back to committedChunkId(): {}",
                        e.getTargetException().toString());
            }
        } catch (IllegalAccessException e) {
            LOG.debug("Unable to access committedOffset() via reflection, falling back: {}",
                    e.toString());
        }

        try {
            return stats.committedChunkId() + 1;
        } catch (NoOffsetException e) {
            return 0;
        }
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
