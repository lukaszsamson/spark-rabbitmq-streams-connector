package com.rabbitmq.spark.connector;

import com.rabbitmq.stream.Environment;
import com.rabbitmq.stream.NoOffsetException;
import com.rabbitmq.stream.StreamStats;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.connector.read.streaming.MicroBatchStream;
import org.apache.spark.sql.connector.read.streaming.Offset;
import org.apache.spark.sql.connector.read.streaming.ReadLimit;
import org.apache.spark.sql.connector.read.streaming.SupportsAdmissionControl;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Core micro-batch streaming source for RabbitMQ Streams.
 *
 * <p>Implements the {@link MicroBatchStream} lifecycle:
 * <ul>
 *   <li>{@link #initialOffset()} – resolve starting offsets (broker lookup → fallback)</li>
 *   <li>{@link #latestOffset(Offset, ReadLimit)} – query broker for tail offsets</li>
 *   <li>{@link #planInputPartitions(Offset, Offset)} – create partitions for offset range</li>
 *   <li>{@link #commit(Offset)} – optional broker offset storage</li>
 *   <li>{@link #stop()} – release resources</li>
 * </ul>
 *
 * <p>Also implements {@link SupportsAdmissionControl} with basic pass-through
 * behavior (all limits treated as {@code ReadAllAvailable} for M4).
 * Milestone 5 will add proper limit distribution.
 */
final class RabbitMQMicroBatchStream implements MicroBatchStream, SupportsAdmissionControl {

    private static final Logger LOG = LoggerFactory.getLogger(RabbitMQMicroBatchStream.class);

    private final ConnectorOptions options;
    private final StructType schema;
    private final String checkpointLocation;

    /** Discovered streams (lazily initialized). */
    private List<String> streams;

    /** Driver-side Environment for broker queries (lazily initialized). */
    private Environment environment;

    /** Cached latest offset for reportLatestOffset(). */
    private volatile RabbitMQStreamOffset cachedLatestOffset;

    RabbitMQMicroBatchStream(ConnectorOptions options, StructType schema,
                              String checkpointLocation) {
        this.options = options;
        this.schema = schema;
        this.checkpointLocation = checkpointLocation;
    }

    // ---- SparkDataStream lifecycle ----

    @Override
    public Offset initialOffset() {
        List<String> streams = discoverStreams();
        String consumerName = options.getConsumerName();

        // 1. Try broker stored offsets if consumerName is set
        if (consumerName != null && !consumerName.isEmpty()) {
            try {
                Map<String, Long> stored = StoredOffsetLookup.lookup(
                        getEnvironment(), consumerName, streams);
                if (!stored.isEmpty()) {
                    LOG.info("Recovered stored offsets from broker for consumer '{}': {}",
                            consumerName, stored);
                    // Fill in any missing streams with starting offset resolution
                    Map<String, Long> merged = new LinkedHashMap<>(stored);
                    for (String stream : streams) {
                        merged.putIfAbsent(stream, resolveStartingOffset(stream));
                    }
                    return new RabbitMQStreamOffset(merged);
                }
                LOG.info("No stored offsets found for consumer '{}', falling back to startingOffsets",
                        consumerName);
            } catch (Exception e) {
                LOG.warn("Failed to look up stored offsets for consumer '{}': {}. " +
                                "Falling back to startingOffsets.",
                        consumerName, e.getMessage());
            }
        }

        // 2. Fall back to startingOffsets
        Map<String, Long> offsets = new LinkedHashMap<>();
        for (String stream : streams) {
            offsets.put(stream, resolveStartingOffset(stream));
        }
        LOG.info("Initial offsets from startingOffsets={}: {}", options.getStartingOffsets(), offsets);
        return new RabbitMQStreamOffset(offsets);
    }

    @Override
    public Offset deserializeOffset(String json) {
        return RabbitMQStreamOffset.fromJson(json);
    }

    @Override
    public void commit(Offset end) {
        RabbitMQStreamOffset endOffset = (RabbitMQStreamOffset) end;

        if (!options.isServerSideOffsetTracking(true)) {
            LOG.debug("Server-side offset tracking disabled, skipping broker commit");
            return;
        }

        String consumerName = options.getConsumerName();
        if (consumerName == null || consumerName.isEmpty()) {
            LOG.debug("No consumerName set, skipping broker offset commit");
            return;
        }

        Environment env = getEnvironment();
        for (Map.Entry<String, Long> entry : endOffset.getStreamOffsets().entrySet()) {
            String stream = entry.getKey();
            long nextOffset = entry.getValue();
            if (nextOffset <= 0) {
                continue;
            }
            long lastProcessed = nextOffset - 1;
            try {
                env.storeOffset(consumerName, stream, lastProcessed);
                LOG.debug("Stored offset {} for consumer '{}' on stream '{}'",
                        lastProcessed, consumerName, stream);
            } catch (Exception e) {
                // Best-effort: Spark checkpoint is the source of truth
                LOG.warn("Failed to store offset {} for consumer '{}' on stream '{}': {}",
                        lastProcessed, consumerName, stream, e.getMessage());
            }
        }
    }

    @Override
    public void stop() {
        if (environment != null) {
            try {
                environment.close();
            } catch (Exception e) {
                LOG.warn("Error closing environment", e);
            }
            environment = null;
        }
    }

    // ---- MicroBatchStream ----

    @Override
    public Offset latestOffset() {
        return latestOffset(null, ReadLimit.allAvailable());
    }

    @Override
    public InputPartition[] planInputPartitions(Offset start, Offset end) {
        RabbitMQStreamOffset startOffset = (RabbitMQStreamOffset) start;
        RabbitMQStreamOffset endOffset = (RabbitMQStreamOffset) end;

        List<InputPartition> partitions = new ArrayList<>();
        for (Map.Entry<String, Long> entry : endOffset.getStreamOffsets().entrySet()) {
            String stream = entry.getKey();
            long endOff = entry.getValue();
            long startOff = startOffset.getStreamOffsets().getOrDefault(stream, 0L);

            if (endOff > startOff) {
                partitions.add(new RabbitMQInputPartition(stream, startOff, endOff, options));
            }
        }

        LOG.info("Planned {} input partitions for micro-batch [{} → {}]",
                partitions.size(), start, end);
        return partitions.toArray(new InputPartition[0]);
    }

    @Override
    public PartitionReaderFactory createReaderFactory() {
        return new RabbitMQPartitionReaderFactory(options, schema);
    }

    // ---- SupportsAdmissionControl ----

    @Override
    public ReadLimit getDefaultReadLimit() {
        // TODO: M5 — return proper limits based on maxRecordsPerTrigger / maxBytesPerTrigger
        return ReadLimit.allAvailable();
    }

    @Override
    public Offset latestOffset(Offset startOffset, ReadLimit limit) {
        Map<String, Long> tailOffsets = queryTailOffsets();

        if (startOffset != null) {
            RabbitMQStreamOffset start = (RabbitMQStreamOffset) startOffset;
            // Check if any stream has new data
            boolean hasNewData = false;
            for (Map.Entry<String, Long> entry : tailOffsets.entrySet()) {
                long startOff = start.getStreamOffsets().getOrDefault(entry.getKey(), 0L);
                if (entry.getValue() > startOff) {
                    hasNewData = true;
                    break;
                }
            }
            if (!hasNewData) {
                LOG.debug("No new data available, returning start offset");
                return startOffset;
            }

            // TODO: M5 — apply ReadLimit budget distribution across streams
        }

        RabbitMQStreamOffset latest = new RabbitMQStreamOffset(tailOffsets);
        cachedLatestOffset = latest;
        return latest;
    }

    @Override
    public Offset reportLatestOffset() {
        return cachedLatestOffset;
    }

    // ---- Internal helpers ----

    private List<String> discoverStreams() {
        if (streams != null) {
            return streams;
        }

        if (options.isStreamMode()) {
            streams = List.of(options.getStream());
        } else {
            streams = SuperStreamPartitionDiscovery.discoverPartitions(
                    options, options.getSuperStream());
            if (streams.isEmpty()) {
                throw new IllegalStateException(
                        "Superstream '" + options.getSuperStream() +
                                "' has no partition streams");
            }
            LOG.info("Discovered {} partition streams for superstream '{}'",
                    streams.size(), options.getSuperStream());
        }
        return streams;
    }

    private Environment getEnvironment() {
        if (environment == null) {
            environment = EnvironmentBuilderHelper.buildEnvironment(options);
        }
        return environment;
    }

    /**
     * Query tail offsets for all streams from the broker.
     *
     * @return map of stream → end offset (exclusive, committedChunkId + 1)
     */
    private Map<String, Long> queryTailOffsets() {
        List<String> streams = discoverStreams();
        Environment env = getEnvironment();
        Map<String, Long> tailOffsets = new LinkedHashMap<>();

        for (String stream : streams) {
            long tail = queryStreamTailOffset(env, stream);
            tailOffsets.put(stream, tail);
        }
        return tailOffsets;
    }

    /**
     * Query the tail offset for a single stream.
     *
     * @return end offset (exclusive) — committedChunkId + 1 or 0 if empty
     */
    private long queryStreamTailOffset(Environment env, String stream) {
        StreamStats stats;
        try {
            stats = env.queryStreamStats(stream);
        } catch (Exception e) {
            if (options.isFailOnDataLoss()) {
                throw new IllegalStateException(
                        "Failed to query stream stats for '" + stream + "'", e);
            }
            LOG.warn("Failed to query stream stats for '{}': {}", stream, e.getMessage());
            return 0;
        }

        // TODO: migrate to stats.committedOffset() (precise tail, RabbitMQ 4.3+) when
        //  stream-client 1.5+ is released — it is present in the client source but not
        //  yet in the 1.4.0 Maven artifact.
        try {
            return stats.committedChunkId() + 1;
        } catch (NoOffsetException e) {
            return 0;
        }
    }

    /**
     * Resolve the starting offset for a single stream based on the configured
     * {@code startingOffsets} mode.
     */
    private long resolveStartingOffset(String stream) {
        return switch (options.getStartingOffsets()) {
            case EARLIEST -> resolveFirstAvailable(stream);
            case LATEST -> queryStreamTailOffset(getEnvironment(), stream);
            case OFFSET -> options.getStartingOffset();
            case TIMESTAMP -> {
                // For timestamp mode, use firstAvailable as offset-planning lower bound.
                // The consumer uses OffsetSpecification.timestamp() for efficient seeking.
                yield resolveFirstAvailable(stream);
            }
        };
    }

    /**
     * Resolve the first available offset for a stream.
     *
     * @return first offset, or 0 if the stream is empty
     */
    private long resolveFirstAvailable(String stream) {
        try {
            StreamStats stats = getEnvironment().queryStreamStats(stream);
            return stats.firstOffset();
        } catch (NoOffsetException e) {
            return 0;
        } catch (Exception e) {
            LOG.warn("Failed to query first offset for stream '{}': {}", stream, e.getMessage());
            return 0;
        }
    }
}
