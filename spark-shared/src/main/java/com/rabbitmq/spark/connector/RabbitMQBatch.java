package com.rabbitmq.spark.connector;

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
                long span = Math.max(0L, entry.getValue()[1] - entry.getValue()[0]);
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
            return planWithSplitting(minPartitions);
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
     * Split counts are distributed evenly across streams.
     */
    private InputPartition[] planWithSplitting(int minPartitions) {
        long totalOffsetSpan = 0;
        for (long[] range : offsetRanges.values()) {
            totalOffsetSpan += Math.max(0L, range[1] - range[0]);
        }

        if (totalOffsetSpan == 0) {
            return new InputPartition[0];
        }

        List<InputPartition> partitions = new ArrayList<>();

        // Calculate deterministic split counts per stream.
        Map<String, Integer> splitsPerStream = allocateSplits(minPartitions, totalOffsetSpan);

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
     * Allocate splits per stream evenly, with deterministic remainder assignment by stream order.
     */
    private Map<String, Integer> allocateSplits(int minPartitions, long totalOffsetSpan) {
        Map<String, Integer> splits = new LinkedHashMap<>();
        if (totalOffsetSpan <= 0) {
            return splits;
        }

        int streamCount = offsetRanges.size();
        if (streamCount == 0) {
            return splits;
        }
        int baseSplits = minPartitions / streamCount;
        int remainder = minPartitions % streamCount;
        int streamIndex = 0;
        for (String stream : offsetRanges.keySet()) {
            int splitCount = Math.max(1, baseSplits + (streamIndex < remainder ? 1 : 0));
            splits.put(stream, splitCount);
            streamIndex++;
        }

        return splits;
    }

    /**
     * Split a stream's offset range into numSplits partitions.
     */
    private void splitStream(List<InputPartition> partitions, String stream,
                             long start, long end, int numSplits) {
        long offsetSpan = end - start;
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
}
