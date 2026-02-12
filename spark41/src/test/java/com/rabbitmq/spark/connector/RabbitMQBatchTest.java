package com.rabbitmq.spark.connector;

import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class RabbitMQBatchTest {

    @Test
    void batchTimestampMarksFirstPartitionForConfiguredSeek() {
        Map<String, String> opts = new LinkedHashMap<>();
        opts.put("endpoints", "localhost:5552");
        opts.put("stream", "test-stream");
        opts.put("startingOffsets", "timestamp");
        opts.put("startingTimestamp", "1234");
        opts.put("minPartitions", "2");
        ConnectorOptions options = new ConnectorOptions(opts);

        Map<String, long[]> ranges = new LinkedHashMap<>();
        ranges.put("test-stream", new long[]{10L, 20L});
        RabbitMQBatch batch = new RabbitMQBatch(options, new StructType(), ranges);

        InputPartition[] planned = batch.planInputPartitions();
        assertThat(planned).hasSize(2);

        RabbitMQInputPartition first = (RabbitMQInputPartition) planned[0];
        RabbitMQInputPartition second = (RabbitMQInputPartition) planned[1];
        assertThat(first.isUseConfiguredStartingOffset()).isTrue();
        assertThat(second.isUseConfiguredStartingOffset()).isFalse();
    }

    @Test
    void batchNonTimestampDoesNotMarkConfiguredSeek() {
        Map<String, String> opts = new LinkedHashMap<>();
        opts.put("endpoints", "localhost:5552");
        opts.put("stream", "test-stream");
        opts.put("startingOffsets", "earliest");
        ConnectorOptions options = new ConnectorOptions(opts);

        Map<String, long[]> ranges = new LinkedHashMap<>();
        ranges.put("test-stream", new long[]{0L, 5L});
        RabbitMQBatch batch = new RabbitMQBatch(options, new StructType(), ranges);

        InputPartition[] planned = batch.planInputPartitions();
        assertThat(planned).hasSize(1);
        RabbitMQInputPartition only = (RabbitMQInputPartition) planned[0];
        assertThat(only.isUseConfiguredStartingOffset()).isFalse();
    }
}
