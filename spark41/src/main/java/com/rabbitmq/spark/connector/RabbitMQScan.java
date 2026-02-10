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

/**
 * Logical representation of a RabbitMQ stream scan.
 *
 * <p>{@link #toBatch()} resolves starting and ending offsets once and returns
 * a fixed {@link RabbitMQBatch}. {@link #toMicroBatchStream(String)} returns
 * a {@link RabbitMQMicroBatchStream} for Structured Streaming queries.
 */
final class RabbitMQScan implements Scan {

    private static final Logger LOG = LoggerFactory.getLogger(RabbitMQScan.class);

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

        long startOffset = resolveStartOffset(firstAvailable, stats);
        long endOffset = resolveEndOffset(stats);

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

    private long resolveStartOffset(long firstAvailable, StreamStats stats) {
        return switch (options.getStartingOffsets()) {
            case EARLIEST -> firstAvailable;
            case LATEST -> resolveEndOffset(stats);
            case OFFSET -> options.getStartingOffset();
            case TIMESTAMP -> {
                // For timestamp mode, start from the beginning and let the broker seek.
                // The consumer uses OffsetSpecification.timestamp() for efficient seeking.
                // For partition planning purposes, use firstAvailable as a lower bound.
                yield firstAvailable;
            }
        };
    }

    private long resolveEndOffset(StreamStats stats) {
        if (options.getEndingOffsets() == EndingOffsetsMode.OFFSET) {
            return options.getEndingOffset();
        }

        // endingOffsets=latest: resolve from broker stats.
        // committedChunkId() is the first offset of the last committed chunk (approximate).
        // TODO: migrate to stats.committedOffset() (precise tail, RabbitMQ 4.3+) when
        //  stream-client 1.5+ is released — it is present in the client source but not
        //  yet in the 1.4.0 Maven artifact.
        try {
            return stats.committedChunkId() + 1;
        } catch (NoOffsetException e) {
            return 0;
        }
    }
}
