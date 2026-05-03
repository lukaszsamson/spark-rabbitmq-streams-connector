package io.github.lukaszsamson.spark.rabbitmq;

import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.streaming.*;
import org.apache.spark.sql.types.StructType;

import java.util.*;

/**
 * Spark 4.1 micro-batch stream that extends the shared base with
 * {@link SupportsRealTimeMode} support and direct use of Spark 4.1 APIs
 * ({@link ReadMinRows}, {@link ReadMaxBytes}).
 *
 * <p>All core streaming lifecycle logic, offset management, admission control,
 * metrics, and broker interaction are inherited from {@link BaseRabbitMQMicroBatchStream}.
 * This subclass only adds the real-time mode contract methods and overrides
 * read-limit dispatch to use Spark 4.1 typed APIs instead of reflection.
 */
final class RabbitMQMicroBatchStream extends BaseRabbitMQMicroBatchStream
        implements SupportsRealTimeMode {

    private static final long REAL_TIME_TAIL_REFRESH_INTERVAL_MS = 1_000L;

    /** Whether real-time mode has been activated via {@link #prepareForRealTimeMode()}. */
    private volatile boolean realTimeMode = false;
    private final Object realTimeTailRefreshLock = new Object();
    private volatile long lastRealTimeTailRefreshMillis = 0L;

    RabbitMQMicroBatchStream(ConnectorOptions options, StructType schema,
                              String checkpointLocation) {
        super(options, schema, checkpointLocation);
    }

    // ---- SupportsRealTimeMode ----

    @Override
    public void prepareForRealTimeMode() {
        Integer minPartitions = options.getMinPartitions();
        if (minPartitions != null) {
            throw new IllegalArgumentException(
                    "minPartitions is not supported in real-time mode");
        }
        if (options.getMaxRecordsPerPartition() != null) {
            throw new IllegalArgumentException(
                    "maxRecordsPerPartition is not supported in real-time mode");
        }
        if (options.getMaxRecordsPerTrigger() != null || options.getMaxBytesPerTrigger() != null) {
            throw new IllegalArgumentException(
                    "maxRecordsPerTrigger and maxBytesPerTrigger are not supported in real-time mode");
        }
        if (options.getMinOffsetsPerTrigger() != null) {
            throw new IllegalArgumentException(
                    "minOffsetsPerTrigger is not compatible with real-time mode");
        }
        if (options.getMaxWaitMs() != ConnectorOptions.DEFAULT_MAX_WAIT_MS) {
            throw new IllegalArgumentException(
                    "maxWaitMs is not supported in real-time mode; use pollTimeoutMs and " +
                            "Spark real-time timeout controls instead");
        }
        this.realTimeMode = true;
        LOG.info("Real-time mode activated");
    }

    @Override
    public InputPartition[] planInputPartitions(Offset start) {
        RabbitMQStreamOffset startOffset = (RabbitMQStreamOffset) start;
        List<String> streams = discoverStreams();

        Map<String, Long> checkpointFallbackOffsets = null;
        boolean checkpointFallbackLoaded = false;

        List<InputPartition> partitions = new ArrayList<>();
        for (String stream : streams) {
            Long knownStart = startOffset.getStreamOffsets().get(stream);
            long startOff;
            boolean usedMissingStartFallback;
            if (knownStart != null) {
                startOff = knownStart;
                usedMissingStartFallback = false;
            } else {
                if (!checkpointFallbackLoaded) {
                    checkpointFallbackOffsets = loadCommittedOffsetsFromCheckpoint();
                    checkpointFallbackLoaded = true;
                }
                startOff = resolveMissingStartOffset(
                        stream,
                        "planInputPartitions(Offset)",
                        checkpointFallbackOffsets,
                        true);
                usedMissingStartFallback = true;
            }

            // Validate against retention truncation
            startOff = validateStartOffset(stream, startOff, Long.MAX_VALUE);
            if (startOff < 0) {
                continue;
            }

            // For timestamp-start mode, only honor the configured-seek flag when this
            // partition's start came from the start map (i.e., the original timestamp
            // anchor). resolveMissingStartOffset records its fallback in initialOffsets
            // as a side effect, which would otherwise trick useConfiguredStartingOffset
            // into re-applying timestamp filtering to records that were resolved via
            // checkpoint or first-available — silently skipping data on newly discovered
            // superstream partitions.
            boolean configuredSeek = !usedMissingStartFallback
                    && useConfiguredStartingOffset(stream, startOff);

            partitions.add(new RabbitMQInputPartition(
                    stream, startOff, Long.MAX_VALUE, options,
                    configuredSeek,
                    RabbitMQInputPartition.locationForStream(stream),
                    messageSizeTrackerScope,
                    messageSizeBytesAccumulator,
                    messageSizeRecordsAccumulator,
                    false));
        }

        LOG.info("Real-time mode: planned {} input partitions from start {}",
                partitions.size(), start);
        return partitions.toArray(new InputPartition[0]);
    }

    @Override
    public Offset mergeOffsets(PartitionOffset[] offsets) {
        Map<String, Long> merged = new LinkedHashMap<>();
        for (PartitionOffset po : offsets) {
            RabbitMQPartitionOffset rpo = (RabbitMQPartitionOffset) po;
            merged.merge(rpo.getStream(), rpo.getNextOffset(), Math::max);
        }
        RabbitMQStreamOffset result = new RabbitMQStreamOffset(merged);
        // Track consumed progress separately from cachedLatestOffset (source-latest reporting),
        // so reportLatestOffset() / lag metrics keep reflecting the broker tail rather than
        // the consumer's position.
        cachedConsumedOffset = result;
        if (realTimeMode) {
            refreshTailOffsetsForRealTimeMetricsIfDue();
        }
        return result;
    }

    private void refreshTailOffsetsForRealTimeMetricsIfDue() {
        long now = System.currentTimeMillis();
        if (now - lastRealTimeTailRefreshMillis < REAL_TIME_TAIL_REFRESH_INTERVAL_MS) {
            return;
        }
        synchronized (realTimeTailRefreshLock) {
            now = System.currentTimeMillis();
            if (now - lastRealTimeTailRefreshMillis < REAL_TIME_TAIL_REFRESH_INTERVAL_MS) {
                return;
            }
            try {
                // Stats-only path: avoids spinning up a tail-probe consumer on every metric
                // refresh. Real-time triggers fire frequently and a probe per refresh would
                // create excessive broker connections for a metric that tolerates lag.
                Map<String, Long> tailOffsets = queryTailOffsetsForMetrics();
                if (!tailOffsets.isEmpty()) {
                    cachedTailOffset = new RabbitMQStreamOffset(tailOffsets);
                }
            } catch (RuntimeException e) {
                // Lag metrics refresh should not fail real-time query execution.
                LOG.debug("Unable to refresh real-time tail offsets for lag metrics: {}",
                        e.toString());
            } finally {
                lastRealTimeTailRefreshMillis = now;
            }
        }
    }

    // In real-time mode, Spark's source.commit() is deferred to the start of the NEXT batch
    // (cleanUpLastExecutedMicroBatch). If the query is stopped after only 1 batch, commit() is
    // never called and lastCommittedEndOffsets stays null. The checkpoint fallback may also fail
    // if stop() races with the commit log write. As a safety net, allow cachedConsumedOffset
    // (set by mergeOffsets() after actual data delivery) to be used for stop-time persistence.
    @Override
    boolean shouldPersistCachedConsumedOffsetsOnStop() {
        return realTimeMode;
    }

    // ---- Spark 4.1 direct API overrides ----

    @Override
    ReadLimit buildUpperLimit(Long maxRows, Long maxBytes) {
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
        return null;
    }

    @Override
    Map<String, Long> applyReadLimit(
            Map<String, Long> startOffsets,
            Map<String, Long> tailOffsets,
            ReadLimit limit,
            long rotation) {

        if (limit instanceof ReadAllAvailable) {
            return tailOffsets;
        }

        if (limit instanceof ReadMaxRows maxRows) {
            return ReadLimitBudget.distributeRecordBudget(
                    startOffsets, tailOffsets, maxRows.maxRows(), rotation);
        }

        if (limit instanceof ReadMinRows minRows) {
            return handleReadMinRowsCore(
                    minRows.minRows(), minRows.maxTriggerDelayMs(),
                    startOffsets, tailOffsets);
        }

        if (limit instanceof ReadMaxBytes maxBytes) {
            return ReadLimitBudget.distributeByteBudget(
                    startOffsets, tailOffsets, maxBytes.maxBytes(),
                    currentEstimatedMessageSize(), rotation);
        }

        if (limit instanceof CompositeReadLimit composite) {
            Map<String, Long> result = tailOffsets;
            for (ReadLimit component : composite.getReadLimits()) {
                Map<String, Long> componentEnd = applyReadLimit(
                        startOffsets, tailOffsets, component, rotation);
                result = ReadLimitBudget.mostRestrictive(result, componentEnd);
            }
            return result;
        }

        // Unknown limit type: treat as ReadAllAvailable
        LOG.debug("Unknown ReadLimit type {}, treating as ReadAllAvailable",
                limit.getClass().getName());
        return tailOffsets;
    }

}
