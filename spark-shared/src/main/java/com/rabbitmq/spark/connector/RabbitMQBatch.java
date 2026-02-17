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

        Integer minPartitions = options.getMinPartitions();
        if (minPartitions != null && minPartitions > offsetRanges.size()) {
            return planWithSplitting(minPartitions);
        }

        // Default: one partition per stream
        return planWithoutSplitting();
    }

    @Override
    public PartitionReaderFactory createReaderFactory() {
        return new RabbitMQPartitionReaderFactory(options, schema);
    }

    private InputPartition[] planWithoutSplitting() {
        List<InputPartition> partitions = new ArrayList<>();
        for (Map.Entry<String, long[]> entry : offsetRanges.entrySet()) {
            String stream = entry.getKey();
            long start = entry.getValue()[0];
            long end = entry.getValue()[1];
            boolean useConfiguredStartingOffset = options.getStartingOffsets() == StartingOffsetsMode.TIMESTAMP;
            partitions.add(new RabbitMQInputPartition(stream, start, end, options, useConfiguredStartingOffset));
        }
        LOG.info("Planned {} input partitions (one per stream)", partitions.size());
        return partitions.toArray(new InputPartition[0]);
    }

    /**
     * Split streams into more partitions when minPartitions > stream count.
     * Each stream gets splits proportional to its share of total pending messages.
     */
    private InputPartition[] planWithSplitting(int minPartitions) {
        long totalMessages = 0;
        for (long[] range : offsetRanges.values()) {
            totalMessages += range[1] - range[0];
        }

        if (totalMessages == 0) {
            return new InputPartition[0];
        }

        List<InputPartition> partitions = new ArrayList<>();

        // Calculate splits per stream proportional to message count
        Map<String, Integer> splitsPerStream = allocateSplits(minPartitions, totalMessages);

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
     * Allocate splits per stream proportional to pending data.
     * Uses floor allocation with remainder distributed to streams with largest fractional shares.
     */
    private Map<String, Integer> allocateSplits(int minPartitions, long totalMessages) {
        Map<String, Integer> splits = new LinkedHashMap<>();
        Map<String, Double> fractional = new LinkedHashMap<>();
        int allocated = 0;

        for (Map.Entry<String, long[]> entry : offsetRanges.entrySet()) {
            String stream = entry.getKey();
            long messages = entry.getValue()[1] - entry.getValue()[0];
            double share = (double) messages / totalMessages * minPartitions;
            int floor = Math.max(1, (int) share);
            splits.put(stream, floor);
            fractional.put(stream, share - floor);
            allocated += floor;
        }

        // Distribute remainder by largest fractional share
        int remaining = minPartitions - allocated;
        if (remaining > 0) {
            fractional.entrySet().stream()
                    .sorted(Map.Entry.<String, Double>comparingByValue().reversed())
                    .limit(remaining)
                    .forEach(e -> splits.merge(e.getKey(), 1, Integer::sum));
        }

        return splits;
    }

    /**
     * Split a stream's offset range into numSplits partitions.
     */
    private void splitStream(List<InputPartition> partitions, String stream,
                             long start, long end, int numSplits) {
        long totalMessages = end - start;
        if (numSplits <= 1 || totalMessages <= 1) {
            boolean useConfiguredStartingOffset = options.getStartingOffsets() == StartingOffsetsMode.TIMESTAMP;
            partitions.add(new RabbitMQInputPartition(stream, start, end, options, useConfiguredStartingOffset));
            return;
        }

        long chunkSize = totalMessages / numSplits;
        long remainder = totalMessages % numSplits;

        long currentStart = start;
        for (int i = 0; i < numSplits; i++) {
            long splitSize = chunkSize + (i < remainder ? 1 : 0);
            if (splitSize == 0) break;
            long splitEnd = currentStart + splitSize;
            boolean useConfiguredStartingOffset = options.getStartingOffsets() == StartingOffsetsMode.TIMESTAMP
                    && currentStart == start;
            partitions.add(new RabbitMQInputPartition(stream, currentStart, splitEnd, options,
                    useConfiguredStartingOffset));
            currentStart = splitEnd;
        }
    }
}
