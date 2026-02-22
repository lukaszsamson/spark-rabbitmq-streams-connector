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

    /** Whether real-time mode has been activated via {@link #prepareForRealTimeMode()}. */
    private volatile boolean realTimeMode = false;

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
        if (options.getMaxRecordsPerTrigger() != null || options.getMaxBytesPerTrigger() != null) {
            throw new IllegalArgumentException(
                    "maxRecordsPerTrigger and maxBytesPerTrigger are not supported in real-time mode");
        }
        if (options.getMinOffsetsPerTrigger() != null) {
            throw new IllegalArgumentException(
                    "minOffsetsPerTrigger is not compatible with real-time mode");
        }
        this.realTimeMode = true;
        LOG.info("Real-time mode activated");
    }

    @Override
    public InputPartition[] planInputPartitions(Offset start) {
        RabbitMQStreamOffset startOffset = (RabbitMQStreamOffset) start;
        List<String> streams = discoverStreams();

        List<InputPartition> partitions = new ArrayList<>();
        for (String stream : streams) {
            Long knownStart = startOffset.getStreamOffsets().get(stream);
            long startOff = knownStart != null ? knownStart : resolveStartingOffset(stream);

            // Validate against retention truncation
            startOff = validateStartOffset(stream, startOff, Long.MAX_VALUE);
            if (startOff < 0) {
                continue;
            }

            partitions.add(new RabbitMQInputPartition(
                    stream, startOff, Long.MAX_VALUE, options,
                    useConfiguredStartingOffset(stream, startOff),
                    RabbitMQInputPartition.locationForStream(stream)));
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
        cachedLatestOffset = result;
        return result;
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
            ReadLimit limit) {

        if (limit instanceof ReadAllAvailable) {
            return tailOffsets;
        }

        if (limit instanceof ReadMaxRows maxRows) {
            return ReadLimitBudget.distributeRecordBudget(
                    startOffsets, tailOffsets, maxRows.maxRows());
        }

        if (limit instanceof ReadMinRows minRows) {
            return handleReadMinRowsCore(
                    minRows.minRows(), minRows.maxTriggerDelayMs(),
                    startOffsets, tailOffsets);
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
}
