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
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

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

    // ---- IT-OFFSET-001 superstream: startingOffsets=latest ----

    @Test
    void streamingStartingOffsetsLatestSuperStream() throws Exception {
        // Publish historical messages to each partition
        for (int p = 0; p < PARTITION_COUNT; p++) {
            publishMessages(superStream + "-" + p, 20, "historical-p" + p + "-");
        }
        Thread.sleep(2000);

        Path checkpointDir = Files.createTempDirectory("spark-checkpoint-ss-latest-");
        Path outputDir = Files.createTempDirectory("spark-output-ss-latest-");

        // Start with latest — should get no historical data
        StreamingQuery query1 = spark.readStream()
                .format("rabbitmq_streams")
                .option("endpoints", streamEndpoint())
                .option("superstream", superStream)
                .option("startingOffsets", "latest")
                .option("metadataFields", "")
                .option("addressResolverClass",
                        "com.rabbitmq.spark.connector.TestAddressResolver")
                .load()
                .writeStream()
                .format("parquet")
                .option("path", outputDir.toString())
                .option("checkpointLocation", checkpointDir.toString())
                .trigger(Trigger.AvailableNow())
                .start();

        query1.awaitTermination(60_000);

        // No historical messages should be present.
        long phase1Count = countOutputRows(outputDir);
        assertThat(phase1Count).isEqualTo(0L);

        // Publish new messages
        for (int p = 0; p < PARTITION_COUNT; p++) {
            publishMessages(superStream + "-" + p, 10, "new-p" + p + "-");
        }
        Thread.sleep(2000);

        // Resume — should pick up only the new 30 messages
        StreamingQuery query2 = spark.readStream()
                .format("rabbitmq_streams")
                .option("endpoints", streamEndpoint())
                .option("superstream", superStream)
                .option("startingOffsets", "latest")
                .option("metadataFields", "")
                .option("addressResolverClass",
                        "com.rabbitmq.spark.connector.TestAddressResolver")
                .load()
                .writeStream()
                .format("parquet")
                .option("path", outputDir.toString())
                .option("checkpointLocation", checkpointDir.toString())
                .trigger(Trigger.AvailableNow())
                .start();

        query2.awaitTermination(60_000);

        long count = spark.read().schema(MINIMAL_OUTPUT_SCHEMA)
                .parquet(outputDir.toString()).count();
        assertThat(count).isEqualTo(30);

        // Verify all messages are from the new batch
        List<String> values = spark.read().schema(MINIMAL_OUTPUT_SCHEMA)
                .parquet(outputDir.toString())
                .collectAsList().stream()
                .map(row -> new String((byte[]) row.getAs("value")))
                .toList();
        assertThat(values).allMatch(v -> v.startsWith("new-"));
    }

    // ---- IT-OFFSET-006: mixed per-stream offsets with one empty partition ----

    @Test
    void batchReadWithOneEmptyPartition() throws Exception {
        // Publish messages to partitions 0 and 2 only, leave partition 1 empty
        publishMessages(superStream + "-0", 15, "p0-");
        publishMessages(superStream + "-2", 15, "p2-");

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
        // Should have 30 messages (15 from p0 + 15 from p2), no error from empty p1
        assertThat(rows).hasSize(30);

        // Verify messages from the correct partitions
        Map<String, Long> countsByStream = rows.stream()
                .collect(Collectors.groupingBy(
                        row -> row.getAs("stream").toString(),
                        Collectors.counting()));

        assertThat(countsByStream).containsEntry(superStream + "-0", 15L);
        assertThat(countsByStream).containsEntry(superStream + "-2", 15L);
        assertThat(countsByStream).doesNotContainKey(superStream + "-1");
    }

    // ---- IT-AVNOW-002: publish during run excluded by snapshot (superstream) ----

    @Test
    void triggerAvailableNowExcludesPostSnapshotSuperStream() throws Exception {
        // Pre-publish messages to each partition
        for (int p = 0; p < PARTITION_COUNT; p++) {
            publishMessages(superStream + "-" + p, 10, "pre-p" + p + "-");
        }
        Thread.sleep(2000);

        Path checkpointDir = Files.createTempDirectory("spark-checkpoint-ss-avnow-");
        Path outputDir = Files.createTempDirectory("spark-output-ss-avnow-");

        // Publish more messages asynchronously during processing
        CompletableFuture<Void> asyncPublish = CompletableFuture.runAsync(() -> {
            try {
                Thread.sleep(3000);
                for (int p = 0; p < PARTITION_COUNT; p++) {
                    publishMessages(superStream + "-" + p, 10, "post-p" + p + "-");
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });

        StreamingQuery query = spark.readStream()
                .format("rabbitmq_streams")
                .option("endpoints", streamEndpoint())
                .option("superstream", superStream)
                .option("startingOffsets", "earliest")
                .option("maxRecordsPerTrigger", "5")
                .option("metadataFields", "")
                .option("addressResolverClass",
                        "com.rabbitmq.spark.connector.TestAddressResolver")
                .load()
                .writeStream()
                .format("parquet")
                .option("path", outputDir.toString())
                .option("checkpointLocation", checkpointDir.toString())
                .trigger(Trigger.AvailableNow())
                .start();

        query.awaitTermination(120_000);
        asyncPublish.join();

        // Should have exactly the pre-snapshot messages (30), not the post-snapshot ones
        long count = spark.read().schema(MINIMAL_OUTPUT_SCHEMA)
                .parquet(outputDir.toString()).count();
        assertThat(count).isEqualTo(30);

        // Verify no post-snapshot messages leaked in
        List<String> values = spark.read().schema(MINIMAL_OUTPUT_SCHEMA)
                .parquet(outputDir.toString())
                .collectAsList().stream()
                .map(row -> new String((byte[]) row.getAs("value")))
                .toList();
        assertThat(values).allMatch(v -> v.startsWith("pre-"));
    }

    // ---- IT-AVNOW-003: snapshot with one partition unavailable ----

    @Test
    void triggerAvailableNowFailsWithDeletedPartitionFailOnDataLossTrue() throws Exception {
        // Publish to all partitions
        for (int p = 0; p < PARTITION_COUNT; p++) {
            publishMessages(superStream + "-" + p, 120, "avnow-p" + p + "-");
        }
        Thread.sleep(2000);

        // Delete one partition after the query starts so the partition is part of the planned end.
        CompletableFuture.runAsync(() -> {
            try {
                Thread.sleep(1200);
                deleteStream(superStream + "-1");
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });

        Path checkpointDir = Files.createTempDirectory("spark-checkpoint-ss-avnow-fail-");
        Path outputDir = Files.createTempDirectory("spark-output-ss-avnow-fail-");

        // With failOnDataLoss=true, should fail
        StreamingQuery query = spark.readStream()
                .format("rabbitmq_streams")
                .option("endpoints", streamEndpoint())
                .option("superstream", superStream)
                .option("startingOffsets", "earliest")
                .option("maxRecordsPerTrigger", "15")
                .option("failOnDataLoss", "true")
                .option("metadataFields", "")
                .option("addressResolverClass",
                        "com.rabbitmq.spark.connector.TestAddressResolver")
                .load()
                .writeStream()
                .format("parquet")
                .option("path", outputDir.toString())
                .option("checkpointLocation", checkpointDir.toString())
                .trigger(Trigger.AvailableNow())
                .start();

        assertThatThrownBy(() -> query.awaitTermination(120_000))
                .satisfies(ex -> assertThat(ex.getMessage())
                        .containsAnyOf("does not exist", "Failed to look up stored offset"));
    }

    @Test
    void triggerAvailableNowSkipsDeletedPartitionFailOnDataLossFalse() throws Exception {
        // Publish to all partitions
        for (int p = 0; p < PARTITION_COUNT; p++) {
            publishMessages(superStream + "-" + p, 10, "avnow-p" + p + "-");
        }
        Thread.sleep(2000);

        // Delete partition 1
        deleteStream(superStream + "-1");

        Path checkpointDir = Files.createTempDirectory("spark-checkpoint-ss-avnow-skip-");
        Path outputDir = Files.createTempDirectory("spark-output-ss-avnow-skip-");

        // With failOnDataLoss=false, should skip deleted partition
        StreamingQuery query = spark.readStream()
                .format("rabbitmq_streams")
                .option("endpoints", streamEndpoint())
                .option("superstream", superStream)
                .option("startingOffsets", "earliest")
                .option("failOnDataLoss", "false")
                .option("metadataFields", "")
                .option("addressResolverClass",
                        "com.rabbitmq.spark.connector.TestAddressResolver")
                .load()
                .writeStream()
                .format("parquet")
                .option("path", outputDir.toString())
                .option("checkpointLocation", checkpointDir.toString())
                .trigger(Trigger.AvailableNow())
                .start();

        query.awaitTermination(120_000);

        // Should have messages from surviving partitions only (20 from p0 + p2)
        long count = spark.read().schema(MINIMAL_OUTPUT_SCHEMA)
                .parquet(outputDir.toString()).count();
        assertThat(count).isEqualTo(20);

        List<String> streams = spark.read().schema(MINIMAL_OUTPUT_SCHEMA)
                .parquet(outputDir.toString())
                .collectAsList().stream()
                .map(row -> row.getAs("stream").toString())
                .distinct()
                .toList();
        assertThat(streams).doesNotContain(superStream + "-1");
    }

    // ---- IT-DATALOSS-006: failOnDataLoss=false no duplicates ----

    @Test
    void failOnDataLossFalseNoDuplicatesSuperStream() throws Exception {
        // Publish to all partitions
        for (int p = 0; p < PARTITION_COUNT; p++) {
            publishMessages(superStream + "-" + p, 20, "nodup-p" + p + "-");
        }
        Thread.sleep(2000);

        // Delete one partition to force failOnDataLoss path
        deleteStream(superStream + "-1");

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
        assertThat(rows).hasSize(40); // 20 from p0 + 20 from p2

        // Verify no duplicate offsets per partition stream
        Map<String, List<Long>> offsetsByStream = rows.stream()
                .collect(Collectors.groupingBy(
                        row -> row.getAs("stream").toString(),
                        Collectors.mapping(
                                row -> (Long) row.getAs("offset"),
                                Collectors.toList())));

        for (Map.Entry<String, List<Long>> entry : offsetsByStream.entrySet()) {
            List<Long> offsets = entry.getValue();
            Set<Long> uniqueOffsets = Set.copyOf(offsets);
            assertThat(uniqueOffsets)
                    .as("No duplicate offsets in stream " + entry.getKey())
                    .hasSize(offsets.size());
        }
    }

    // ---- IT-SPLIT-002: minPartitions on superstream when min > partition count ----

    @Test
    void batchReadMinPartitionsGreaterThanPartitionCount() throws Exception {
        // Publish 100 messages per partition (300 total)
        for (int p = 0; p < PARTITION_COUNT; p++) {
            publishMessages(superStream + "-" + p, 100, "split-p" + p + "-");
        }
        Thread.sleep(2000);

        // minPartitions=6 > partitionCount=3 should split individual partitions
        Dataset<Row> df = spark.read()
                .format("rabbitmq_streams")
                .option("endpoints", streamEndpoint())
                .option("superstream", superStream)
                .option("startingOffsets", "earliest")
                .option("minPartitions", "6")
                .option("metadataFields", "")
                .option("addressResolverClass",
                        "com.rabbitmq.spark.connector.TestAddressResolver")
                .load();

        List<Row> rows = df.collectAsList();
        assertThat(rows).hasSize(300);

        // Verify no gaps per partition stream
        Map<String, Set<Long>> offsetsByStream = rows.stream()
                .collect(Collectors.groupingBy(
                        row -> row.getAs("stream").toString(),
                        Collectors.mapping(
                                row -> (Long) row.getAs("offset"),
                                Collectors.toSet())));

        assertThat(offsetsByStream).hasSize(PARTITION_COUNT);

        for (Map.Entry<String, Set<Long>> entry : offsetsByStream.entrySet()) {
            Set<Long> offsets = entry.getValue();
            assertThat(offsets)
                    .as("All 100 offsets present for " + entry.getKey())
                    .hasSize(100);

            long min = offsets.stream().mapToLong(Long::longValue).min().orElse(-1);
            long max = offsets.stream().mapToLong(Long::longValue).max().orElse(-1);
            assertThat(max - min).isEqualTo(99);
        }
    }

    private long countOutputRows(Path outputDir) {
        try (var paths = Files.walk(outputDir)) {
            boolean hasParquet = paths.anyMatch(path ->
                    Files.isRegularFile(path) && path.getFileName().toString().endsWith(".parquet"));
            if (!hasParquet) {
                return 0L;
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to inspect output directory " + outputDir, e);
        }
        return spark.read().schema(MINIMAL_OUTPUT_SCHEMA).parquet(outputDir.toString()).count();
    }
}
