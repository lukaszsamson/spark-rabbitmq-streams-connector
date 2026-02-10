package com.rabbitmq.spark.connector;

import com.rabbitmq.stream.Environment;
import com.rabbitmq.stream.NoOffsetException;
import com.rabbitmq.stream.StreamStats;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.connector.read.streaming.*;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Core micro-batch streaming source for RabbitMQ Streams.
 *
 * <p>Implements the full streaming source lifecycle including:
 * <ul>
 *   <li>{@link MicroBatchStream} – core offset-based micro-batch reads</li>
 *   <li>{@link SupportsAdmissionControl} – rate limiting via
 *       {@code maxRecordsPerTrigger} and {@code maxBytesPerTrigger}</li>
 *   <li>{@link SupportsTriggerAvailableNow} – snapshot-based bounded processing</li>
 *   <li>{@link ReportsSourceMetrics} – lag metrics for query progress</li>
 * </ul>
 */
final class RabbitMQMicroBatchStream
        implements MicroBatchStream, SupportsAdmissionControl,
                   SupportsTriggerAvailableNow, ReportsSourceMetrics {

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

    /**
     * Snapshot of tail offsets captured by {@link #prepareForTriggerAvailableNow()}.
     * When non-null, {@link #latestOffset(Offset, ReadLimit)} will not exceed these.
     */
    private volatile Map<String, Long> availableNowSnapshot;

    /** Running average message size in bytes, for byte-based admission control. */
    private int estimatedMessageSize;

    RabbitMQMicroBatchStream(ConnectorOptions options, StructType schema,
                              String checkpointLocation) {
        this.options = options;
        this.schema = schema;
        this.checkpointLocation = checkpointLocation;
        this.estimatedMessageSize = options.getEstimatedMessageSizeBytes();
    }

    // ---- SparkDataStream lifecycle ----

    @Override
    public Offset initialOffset() {
        List<String> streams = discoverStreams();
        String consumerName = options.getConsumerName();

        // 1. Try broker stored offsets if consumerName is set
        if (consumerName != null && !consumerName.isEmpty()) {
            try {
                StoredOffsetLookup.LookupResult result =
                        StoredOffsetLookup.lookupWithDetails(
                                getEnvironment(), consumerName, streams);

                Map<String, Long> stored = result.getOffsets();

                if (result.hasFailures()) {
                    // Non-fatal failures on some streams
                    LOG.warn("Stored offset lookup had non-fatal failures on streams: {}",
                            result.getFailedStreams());
                }

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
            } catch (IllegalStateException e) {
                // Fatal error (auth, connection, stream does not exist) — fail fast
                throw e;
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

            if (endOff <= startOff) {
                continue;
            }

            // Check for retention truncation: if requested start is below first available
            startOff = validateStartOffset(stream, startOff, endOff);
            if (startOff < 0) {
                // Stream should be skipped (deleted or empty)
                continue;
            }
            if (endOff <= startOff) {
                continue;
            }

            partitions.add(new RabbitMQInputPartition(stream, startOff, endOff, options));
        }

        LOG.info("Planned {} input partitions for micro-batch [{} → {}]",
                partitions.size(), start, end);
        return partitions.toArray(new InputPartition[0]);
    }

    /**
     * Validate a partition's start offset against the broker's first available offset.
     * Handles retention truncation and stream deletion per failOnDataLoss setting.
     *
     * @return the validated start offset, or -1 if the partition should be skipped
     */
    private long validateStartOffset(String stream, long startOff, long endOff) {
        Environment env;
        try {
            env = getEnvironment();
        } catch (Exception e) {
            // Cannot connect to broker for validation — proceed without check
            LOG.debug("Cannot validate offsets for stream '{}': {}", stream, e.getMessage());
            return startOff;
        }

        try {
            StreamStats stats = env.queryStreamStats(stream);
            long firstAvailable = stats.firstOffset();
            if (startOff < firstAvailable) {
                if (options.isFailOnDataLoss()) {
                    throw new IllegalStateException(
                            "Requested start offset " + startOff +
                                    " is before the first available offset " + firstAvailable +
                                    " in stream '" + stream + "'. Data may have been lost due to " +
                                    "retention policy. Set failOnDataLoss=false to advance.");
                }
                LOG.warn("Start offset {} is before first available {} in stream '{}', " +
                                "advancing to first available (data loss detected)",
                        startOff, firstAvailable, stream);
                return firstAvailable;
            }
            return startOff;
        } catch (NoOffsetException e) {
            // Stream is empty
            return -1;
        } catch (com.rabbitmq.stream.StreamDoesNotExistException e) {
            if (options.isFailOnDataLoss()) {
                throw new IllegalStateException(
                        "Stream '" + stream + "' no longer exists. " +
                                "Set failOnDataLoss=false to skip.", e);
            }
            LOG.warn("Stream '{}' no longer exists, skipping partition (failOnDataLoss=false)",
                    stream);
            return -1;
        } catch (IllegalStateException e) {
            throw e;
        } catch (Exception e) {
            // Non-fatal: cannot validate, proceed without check
            LOG.warn("Failed to validate stream '{}' offsets: {}", stream, e.getMessage());
            return startOff;
        }
    }

    @Override
    public PartitionReaderFactory createReaderFactory() {
        return new RabbitMQPartitionReaderFactory(options, schema);
    }

    // ---- SupportsAdmissionControl ----

    @Override
    public ReadLimit getDefaultReadLimit() {
        Long maxRows = options.getMaxRecordsPerTrigger();
        Long maxBytes = options.getMaxBytesPerTrigger();

        if (maxRows != null && maxBytes != null) {
            return ReadLimit.compositeLimit(new ReadLimit[]{
                    ReadLimit.maxRows(maxRows),
                    ReadLimit.maxBytes(maxBytes)
            });
        }
        if (maxRows != null) {
            return ReadLimit.maxRows(maxRows);
        }
        if (maxBytes != null) {
            return ReadLimit.maxBytes(maxBytes);
        }
        return ReadLimit.allAvailable();
    }

    @Override
    public Offset latestOffset(Offset startOffset, ReadLimit limit) {
        Map<String, Long> tailOffsets = queryTailOffsets();

        // Enforce Trigger.AvailableNow snapshot ceiling
        if (availableNowSnapshot != null) {
            tailOffsets = ReadLimitBudget.mostRestrictive(tailOffsets, availableNowSnapshot);
        }

        if (startOffset == null) {
            RabbitMQStreamOffset latest = new RabbitMQStreamOffset(tailOffsets);
            cachedLatestOffset = latest;
            return latest;
        }

        RabbitMQStreamOffset start = (RabbitMQStreamOffset) startOffset;
        Map<String, Long> startMap = start.getStreamOffsets();

        // Check if any stream has new data
        boolean hasNewData = false;
        for (Map.Entry<String, Long> entry : tailOffsets.entrySet()) {
            long startOff = startMap.getOrDefault(entry.getKey(), 0L);
            if (entry.getValue() > startOff) {
                hasNewData = true;
                break;
            }
        }
        if (!hasNewData) {
            LOG.debug("No new data available, returning start offset");
            return startOffset;
        }

        // Apply read limit budget
        Map<String, Long> endOffsets = applyReadLimit(startMap, tailOffsets, limit);

        RabbitMQStreamOffset latest = new RabbitMQStreamOffset(endOffsets);
        cachedLatestOffset = latest;
        return latest;
    }

    @Override
    public Offset reportLatestOffset() {
        return cachedLatestOffset;
    }

    // ---- SupportsTriggerAvailableNow ----

    @Override
    public void prepareForTriggerAvailableNow() {
        Map<String, Long> snapshot = queryTailOffsets();
        this.availableNowSnapshot = snapshot;
        LOG.info("Trigger.AvailableNow: snapshot tail offsets = {}", snapshot);
    }

    // ---- ReportsSourceMetrics ----

    @Override
    public Map<String, String> metrics(Optional<Offset> latestConsumedOffset) {
        Map<String, String> metrics = new LinkedHashMap<>();
        RabbitMQStreamOffset tail = cachedLatestOffset;

        if (tail == null || latestConsumedOffset.isEmpty()) {
            metrics.put("minOffsetsBehindLatest", "0");
            metrics.put("maxOffsetsBehindLatest", "0");
            metrics.put("avgOffsetsBehindLatest", "0.0");
            return metrics;
        }

        RabbitMQStreamOffset consumed = (RabbitMQStreamOffset) latestConsumedOffset.get();
        Map<String, Long> tailMap = tail.getStreamOffsets();
        Map<String, Long> consumedMap = consumed.getStreamOffsets();

        long minLag = Long.MAX_VALUE;
        long maxLag = 0;
        long totalLag = 0;
        int count = 0;

        for (Map.Entry<String, Long> entry : tailMap.entrySet()) {
            String stream = entry.getKey();
            long tailOff = entry.getValue();
            long consumedOff = consumedMap.getOrDefault(stream, 0L);
            long lag = Math.max(0, tailOff - consumedOff);
            minLag = Math.min(minLag, lag);
            maxLag = Math.max(maxLag, lag);
            totalLag += lag;
            count++;
        }

        if (count == 0) {
            minLag = 0;
        }

        double avgLag = count > 0 ? (double) totalLag / count : 0.0;

        metrics.put("minOffsetsBehindLatest", String.valueOf(minLag));
        metrics.put("maxOffsetsBehindLatest", String.valueOf(maxLag));
        metrics.put("avgOffsetsBehindLatest", String.format("%.1f", avgLag));
        return metrics;
    }

    // ---- Read limit dispatch ----

    private Map<String, Long> applyReadLimit(
            Map<String, Long> startOffsets,
            Map<String, Long> tailOffsets,
            ReadLimit limit) {

        if (limit instanceof ReadAllAvailable) {
            return tailOffsets;
        }

        if (limit instanceof ReadMaxRows maxRows) {
            return ReadLimitBudget.distributeRecordBudget(
                    startOffsets, tailOffsets, maxRows.maxRows());
        }

        if (limit instanceof ReadMaxBytes maxBytes) {
            return ReadLimitBudget.distributeByteBudget(
                    startOffsets, tailOffsets, maxBytes.maxBytes(), estimatedMessageSize);
        }

        if (limit instanceof CompositeReadLimit composite) {
            Map<String, Long> result = tailOffsets;
            for (ReadLimit component : composite.getReadLimits()) {
                Map<String, Long> componentEnd = applyReadLimit(
                        startOffsets, tailOffsets, component);
                result = ReadLimitBudget.mostRestrictive(result, componentEnd);
            }
            return result;
        }

        // Unknown limit type: treat as ReadAllAvailable
        LOG.debug("Unknown ReadLimit type {}, treating as ReadAllAvailable",
                limit.getClass().getName());
        return tailOffsets;
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
        } catch (com.rabbitmq.stream.StreamDoesNotExistException e) {
            // Stream deleted (topology change or manual deletion)
            if (options.isFailOnDataLoss()) {
                throw new IllegalStateException(
                        "Stream '" + stream + "' does not exist. " +
                                "It may have been deleted. Set failOnDataLoss=false to skip.", e);
            }
            LOG.warn("Stream '{}' does not exist, skipping (failOnDataLoss=false)", stream);
            return 0;
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
        } catch (com.rabbitmq.stream.StreamDoesNotExistException e) {
            if (options.isFailOnDataLoss()) {
                throw new IllegalStateException(
                        "Stream '" + stream + "' does not exist. Set failOnDataLoss=false to skip.", e);
            }
            LOG.warn("Stream '{}' does not exist, using offset 0 (failOnDataLoss=false)", stream);
            return 0;
        } catch (Exception e) {
            LOG.warn("Failed to query first offset for stream '{}': {}", stream, e.getMessage());
            return 0;
        }
    }
}
