package io.github.lukaszsamson.spark.rabbitmq;

import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Physical representation of a batch read from RabbitMQ streams.
 *
 * <p>Plans input partitions based on resolved offset ranges and optional
 * {@code minPartitions} splitting.
 */
final class RabbitMQBatch implements Batch {

    private static final Logger LOG = LoggerFactory.getLogger(RabbitMQBatch.class);

    private final ConnectorOptions options;
    private final StructType schema;
    private final Map<String, long[]> offsetRanges; // stream -> [startOffset, endOffset)

    RabbitMQBatch(ConnectorOptions options, StructType schema,
                  Map<String, long[]> offsetRanges) {
        this.options = options;
        this.schema = schema;
        this.offsetRanges = offsetRanges;
    }

    @Override
    public InputPartition[] planInputPartitions() {
        if (offsetRanges.isEmpty()) {
            return new InputPartition[0];
        }

        Long maxRecordsPerPartition = options.getMaxRecordsPerPartition();
        Integer minPartitions = options.getMinPartitions();

        // Phase 1: apply maxRecordsPerPartition — cap each stream's partition size
        int totalPartitions = 0;
        Map<String, Integer> splitsPerStream = new LinkedHashMap<>();
        if (maxRecordsPerPartition != null) {
            for (Map.Entry<String, long[]> entry : offsetRanges.entrySet()) {
                long end = entry.getValue()[1];
                // Late-bound partitions (endOffset=MAX_VALUE) cannot be split — range is unknown
                long span = (end == Long.MAX_VALUE) ? 0L
                        : Math.max(0L, end - entry.getValue()[0]);
                int parts = span > 0 ? (int) Math.min(Integer.MAX_VALUE,
                        (span + maxRecordsPerPartition - 1) / maxRecordsPerPartition) : 0;
                parts = Math.max(1, parts);
                splitsPerStream.put(entry.getKey(), parts);
                totalPartitions += parts;
            }
        } else {
            for (String stream : offsetRanges.keySet()) {
                splitsPerStream.put(stream, 1);
            }
            totalPartitions = offsetRanges.size();
        }

        // Phase 2: if minPartitions requires more splits, distribute the extra evenly
        if (minPartitions != null && minPartitions > totalPartitions) {
            return planWithSplitting(minPartitions, splitsPerStream);
        }

        // If maxRecordsPerPartition caused splitting, use it
        if (maxRecordsPerPartition != null && totalPartitions > offsetRanges.size()) {
            return planWithSplitMap(splitsPerStream);
        }

        // Default: one partition per stream
        return planWithoutSplitting();
    }

    @Override
    public PartitionReaderFactory createReaderFactory() {
        return new RabbitMQPartitionReaderFactory(options, schema);
    }

    private InputPartition[] planWithSplitMap(Map<String, Integer> splitsPerStream) {
        validateSingleActiveConsumerSplitCompatibility(splitsPerStream);
        List<InputPartition> partitions = new ArrayList<>();
        for (Map.Entry<String, long[]> entry : offsetRanges.entrySet()) {
            String stream = entry.getKey();
            long start = entry.getValue()[0];
            long end = entry.getValue()[1];
            int numSplits = splitsPerStream.getOrDefault(stream, 1);
            splitStream(partitions, stream, start, end, numSplits);
        }
        LOG.info("Planned {} input partitions (with maxRecordsPerPartition={})",
                partitions.size(), options.getMaxRecordsPerPartition());
        return partitions.toArray(new InputPartition[0]);
    }

    private InputPartition[] planWithoutSplitting() {
        List<InputPartition> partitions = new ArrayList<>();
        for (Map.Entry<String, long[]> entry : offsetRanges.entrySet()) {
            String stream = entry.getKey();
            long start = entry.getValue()[0];
            long end = entry.getValue()[1];
            boolean useConfiguredStartingOffset = options.getStartingOffsets() == StartingOffsetsMode.TIMESTAMP;
            partitions.add(new RabbitMQInputPartition(stream, start, end, options,
                    useConfiguredStartingOffset, RabbitMQInputPartition.locationForStream(stream)));
        }
        LOG.info("Planned {} input partitions (one per stream)", partitions.size());
        return partitions.toArray(new InputPartition[0]);
    }

    /**
     * Split streams into more partitions when minPartitions > stream count.
     * Split counts are distributed deterministically while preserving any
     * existing per-stream minimum split counts (e.g. from maxRecordsPerPartition).
     */
    private InputPartition[] planWithSplitting(int minPartitions,
                                               Map<String, Integer> minimumSplitsPerStream) {
        long totalOffsetSpan = 0;
        for (long[] range : offsetRanges.values()) {
            long end = range[1];
            totalOffsetSpan += (end == Long.MAX_VALUE) ? 0L : Math.max(0L, end - range[0]);
        }

        if (totalOffsetSpan == 0) {
            // All ranges are late-bound (Long.MAX_VALUE) or truly empty.
            // Cannot split without known ranges — fall back to one partition per stream.
            return planWithoutSplitting();
        }

        List<InputPartition> partitions = new ArrayList<>();

        // Calculate deterministic split counts per stream.
        Map<String, Integer> splitsPerStream = allocateSplits(minPartitions, minimumSplitsPerStream);
        validateSingleActiveConsumerSplitCompatibility(splitsPerStream);

        for (Map.Entry<String, long[]> entry : offsetRanges.entrySet()) {
            String stream = entry.getKey();
            long start = entry.getValue()[0];
            long end = entry.getValue()[1];
            int numSplits = splitsPerStream.getOrDefault(stream, 1);

            splitStream(partitions, stream, start, end, numSplits);
        }

        LOG.info("Planned {} input partitions (with minPartitions={})",
                partitions.size(), minPartitions);
        return partitions.toArray(new InputPartition[0]);
    }

    /**
     * Allocate splits per stream with deterministic remainder assignment by stream order.
     * Existing per-stream split floors are preserved.
     */
    private Map<String, Integer> allocateSplits(int minPartitions,
                                                Map<String, Integer> minimumSplitsPerStream) {
        Map<String, Integer> splits = new LinkedHashMap<>();
        int streamCount = offsetRanges.size();
        if (streamCount == 0) {
            return splits;
        }

        int allocatedPartitions = 0;
        int streamIndex = 0;
        for (String stream : offsetRanges.keySet()) {
            int splitCount = Math.max(1, minimumSplitsPerStream.getOrDefault(stream, 1));
            splits.put(stream, splitCount);
            allocatedPartitions += splitCount;
        }

        if (allocatedPartitions >= minPartitions) {
            return splits;
        }

        int extraPartitions = minPartitions - allocatedPartitions;
        int baseExtra = extraPartitions / streamCount;
        int remainder = extraPartitions % streamCount;
        for (String stream : offsetRanges.keySet()) {
            int additional = baseExtra + (streamIndex < remainder ? 1 : 0);
            if (additional > 0) {
                splits.put(stream, splits.get(stream) + additional);
            }
            streamIndex++;
        }

        return splits;
    }

    /**
     * Split a stream's offset range into numSplits partitions.
     */
    private void splitStream(List<InputPartition> partitions, String stream,
                             long start, long end, int numSplits) {
        long offsetSpan = (end == Long.MAX_VALUE) ? 0L : end - start;
        String[] location = RabbitMQInputPartition.locationForStream(stream);
        if (numSplits <= 1 || offsetSpan <= 1) {
            boolean useConfiguredStartingOffset = options.getStartingOffsets() == StartingOffsetsMode.TIMESTAMP;
            partitions.add(new RabbitMQInputPartition(stream, start, end, options,
                    useConfiguredStartingOffset, location));
            return;
        }

        long chunkSize = offsetSpan / numSplits;
        long remainder = offsetSpan % numSplits;

        long currentStart = start;
        for (int i = 0; i < numSplits; i++) {
            long splitSize = chunkSize + (i < remainder ? 1 : 0);
            if (splitSize == 0) break;
            long splitEnd = currentStart + splitSize;
            boolean useConfiguredStartingOffset = options.getStartingOffsets() == StartingOffsetsMode.TIMESTAMP
                    && currentStart == start;
            partitions.add(new RabbitMQInputPartition(stream, currentStart, splitEnd, options,
                    useConfiguredStartingOffset, location));
            currentStart = splitEnd;
        }
    }

    private void validateSingleActiveConsumerSplitCompatibility(Map<String, Integer> splitsPerStream) {
        if (!options.isSingleActiveConsumer()) {
            return;
        }
        for (Map.Entry<String, long[]> entry : offsetRanges.entrySet()) {
            String stream = entry.getKey();
            long offsetSpan = Math.max(0L, entry.getValue()[1] - entry.getValue()[0]);
            int requestedSplits = Math.max(1, splitsPerStream.getOrDefault(stream, 1));
            int effectiveSplits = (offsetSpan <= 1L) ? 1 : requestedSplits;
            if (effectiveSplits > 1) {
                throw new IllegalArgumentException(
                        "'" + ConnectorOptions.SINGLE_ACTIVE_CONSUMER + "=true' is incompatible "
                                + "with split planning for stream '" + stream + "' ("
                                + effectiveSplits + " planned partitions). Disable single "
                                + "active consumer or remove split settings ('"
                                + ConnectorOptions.MIN_PARTITIONS + "'/'"
                                + ConnectorOptions.MAX_RECORDS_PER_PARTITION + "').");
            }
        }
    }
}
