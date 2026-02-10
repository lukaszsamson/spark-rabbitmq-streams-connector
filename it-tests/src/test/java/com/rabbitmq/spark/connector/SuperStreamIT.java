package com.rabbitmq.spark.connector;

import com.rabbitmq.stream.Message;
import com.rabbitmq.stream.Producer;
import org.apache.spark.SparkException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Integration tests for superstream support.
 */
class SuperStreamIT extends AbstractRabbitMQIT {

    private String superStream;
    private static final int PARTITION_COUNT = 3;

    @BeforeEach
    void setUp() {
        superStream = "test-super-" + uniqueStreamName().substring(12);
        createSuperStream(superStream, PARTITION_COUNT);
    }

    @AfterEach
    void tearDown() {
        deleteSuperStream(superStream);
    }

    @Test
    void batchReadFromSuperStream() throws Exception {
        // Publish messages to each partition directly
        for (int p = 0; p < PARTITION_COUNT; p++) {
            String partitionStream = superStream + "-" + p;
            publishMessages(partitionStream, 10, "p" + p + "-msg-");
        }

        // Wait for messages to be committed
        Thread.sleep(2000);

        Dataset<Row> df = spark.read()
                .format("rabbitmq_streams")
                .option("endpoints", streamEndpoint())
                .option("superstream", superStream)
                .option("startingOffsets", "earliest")
                .option("metadataFields", "")
                .option("addressResolverClass",
                        "com.rabbitmq.spark.connector.TestAddressResolver")
                .load();

        List<Row> rows = df.collectAsList();
        assertThat(rows).hasSize(30); // 10 per partition * 3 partitions

        // Verify messages came from different partition streams
        List<String> streams = rows.stream()
                .map(row -> row.getAs("stream").toString())
                .distinct()
                .sorted()
                .toList();

        assertThat(streams).hasSize(PARTITION_COUNT);
        for (int p = 0; p < PARTITION_COUNT; p++) {
            assertThat(streams).contains(superStream + "-" + p);
        }
    }

    @Test
    void batchWriteToSuperStream() throws Exception {
        // Write with routing keys via application_properties
        StructType schema = new StructType()
                .add("value", DataTypes.BinaryType, false)
                .add("application_properties",
                        DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType),
                        true);

        List<Row> data = new ArrayList<>();
        for (int i = 0; i < 30; i++) {
            data.add(RowFactory.create(
                    ("ss-write-" + i).getBytes(),
                    java.util.Map.of("routing_key", String.valueOf(i % PARTITION_COUNT))));
        }

        Dataset<Row> df = spark.createDataFrame(data, schema);

        df.write()
                .format("rabbitmq_streams")
                .mode("append")
                .option("endpoints", streamEndpoint())
                .option("superstream", superStream)
                .option("routingStrategy", "hash")
                .option("addressResolverClass",
                        "com.rabbitmq.spark.connector.TestAddressResolver")
                .save();

        // Verify messages were distributed across partitions
        int totalMessages = 0;
        for (int p = 0; p < PARTITION_COUNT; p++) {
            String partitionStream = superStream + "-" + p;
            List<Message> messages = consumeMessages(partitionStream, 30);
            totalMessages += messages.size();
        }

        assertThat(totalMessages).isEqualTo(30);
    }

    @Test
    void batchWriteToSuperStreamWithRoutingKeyColumn() throws Exception {
        // Write with routing_key column (not via application_properties)
        StructType schema = new StructType()
                .add("value", DataTypes.BinaryType, false)
                .add("routing_key", DataTypes.StringType, true);

        List<Row> data = new ArrayList<>();
        for (int i = 0; i < 30; i++) {
            data.add(RowFactory.create(
                    ("rk-col-write-" + i).getBytes(),
                    String.valueOf(i % PARTITION_COUNT)));
        }

        Dataset<Row> df = spark.createDataFrame(data, schema);

        df.write()
                .format("rabbitmq_streams")
                .mode("append")
                .option("endpoints", streamEndpoint())
                .option("superstream", superStream)
                .option("routingStrategy", "hash")
                .option("addressResolverClass",
                        "com.rabbitmq.spark.connector.TestAddressResolver")
                .save();

        // Verify all messages were distributed across partitions
        int totalMessages = 0;
        for (int p = 0; p < PARTITION_COUNT; p++) {
            String partitionStream = superStream + "-" + p;
            List<Message> messages = consumeMessages(partitionStream, 30);
            totalMessages += messages.size();
            // Verify routing_key was stored in application properties
            for (Message msg : messages) {
                assertThat(msg.getApplicationProperties())
                        .containsKey("routing_key");
            }
        }

        assertThat(totalMessages).isEqualTo(30);
    }

    @Test
    void batchWriteToSuperStreamFailsWithoutRoutingKey() {
        // Write without routing_key — should fail fast
        StructType schema = new StructType()
                .add("value", DataTypes.BinaryType, false);

        List<Row> data = new ArrayList<>();
        data.add(RowFactory.create("no-rk".getBytes()));

        Dataset<Row> df = spark.createDataFrame(data, schema);

        assertThatThrownBy(() ->
                df.write()
                        .format("rabbitmq_streams")
                        .mode("append")
                        .option("endpoints", streamEndpoint())
                        .option("superstream", superStream)
                        .option("routingStrategy", "hash")
                        .option("addressResolverClass",
                                "com.rabbitmq.spark.connector.TestAddressResolver")
                        .save())
                .isInstanceOf(SparkException.class)
                .hasMessageContaining("Routing key is required");
    }

    @Test
    void streamingWriteToSuperStream() throws Exception {
        StructType schema = new StructType()
                .add("value", DataTypes.BinaryType, false)
                .add("routing_key", DataTypes.StringType, true);

        List<Row> data = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
            data.add(RowFactory.create(
                    ("ss-streaming-" + i).getBytes(),
                    String.valueOf(i % PARTITION_COUNT)));
        }

        // Write data as parquet to stream from
        Path inputDir = Files.createTempDirectory("spark-input-ss-").resolve("data");
        Path checkpointDir = Files.createTempDirectory("spark-checkpoint-ss-");
        spark.createDataFrame(data, schema).write().parquet(inputDir.toString());

        StreamingQuery query = spark.readStream()
                .schema(schema)
                .parquet(inputDir.toString())
                .writeStream()
                .format("rabbitmq_streams")
                .option("endpoints", streamEndpoint())
                .option("superstream", superStream)
                .option("routingStrategy", "hash")
                .option("checkpointLocation", checkpointDir.toString())
                .option("addressResolverClass",
                        "com.rabbitmq.spark.connector.TestAddressResolver")
                .trigger(Trigger.AvailableNow())
                .start();

        query.awaitTermination(60_000);

        // Verify messages were distributed
        int totalMessages = 0;
        for (int p = 0; p < PARTITION_COUNT; p++) {
            totalMessages += consumeMessages(superStream + "-" + p, 20).size();
        }
        assertThat(totalMessages).isEqualTo(20);
    }

    @Test
    void failOnDataLossSkipsDeletedPartitionStream() throws Exception {
        // Publish to all partitions
        for (int p = 0; p < PARTITION_COUNT; p++) {
            publishMessages(superStream + "-" + p, 10, "part" + p + "-");
        }

        Thread.sleep(2000);

        // Delete one partition stream
        deleteStream(superStream + "-1");

        // With failOnDataLoss=false, the deleted partition should be skipped
        Dataset<Row> df = spark.read()
                .format("rabbitmq_streams")
                .option("endpoints", streamEndpoint())
                .option("superstream", superStream)
                .option("startingOffsets", "earliest")
                .option("failOnDataLoss", "false")
                .option("metadataFields", "")
                .option("addressResolverClass",
                        "com.rabbitmq.spark.connector.TestAddressResolver")
                .load();

        List<Row> rows = df.collectAsList();
        // Should have messages from 2 remaining partitions only
        assertThat(rows).hasSize(20);

        List<String> streams = rows.stream()
                .map(row -> row.getAs("stream").toString())
                .distinct()
                .sorted()
                .toList();
        assertThat(streams).hasSize(2);
        assertThat(streams).doesNotContain(superStream + "-1");
    }

    @Test
    void batchReadSuperStreamPreservesPartitionStreamNames() throws Exception {
        // Publish to partition 0 only
        publishMessages(superStream + "-0", 5);

        Thread.sleep(2000);

        Dataset<Row> df = spark.read()
                .format("rabbitmq_streams")
                .option("endpoints", streamEndpoint())
                .option("superstream", superStream)
                .option("startingOffsets", "earliest")
                .option("metadataFields", "")
                .option("addressResolverClass",
                        "com.rabbitmq.spark.connector.TestAddressResolver")
                .load();

        List<Row> rows = df.collectAsList();
        assertThat(rows).hasSize(5);

        // All messages should report the partition stream name, not the superstream name
        for (Row row : rows) {
            assertThat(row.getAs("stream").toString())
                    .isEqualTo(superStream + "-0");
        }
    }
}
