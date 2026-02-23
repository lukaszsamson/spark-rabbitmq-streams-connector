package io.github.lukaszsamson.spark.rabbitmq;

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
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;
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
                        "io.github.lukaszsamson.spark.rabbitmq.TestAddressResolver")
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
    void batchReadFromSuperStreamWithSingleActiveConsumerOption() throws Exception {
        // Keep this case deterministic: verify SAC wiring on a superstream without
        // introducing parallel partition-subscribe timing variability in CI.
        deleteSuperStream(superStream);
        createSuperStream(superStream, 1);

        // Publish messages to each partition directly
        for (int p = 0; p < 1; p++) {
            String partitionStream = superStream + "-" + p;
            publishMessages(partitionStream, 10, "sac-p" + p + "-msg-");
        }

        Thread.sleep(2000);

        Dataset<Row> df = spark.read()
                .format("rabbitmq_streams")
                .option("endpoints", streamEndpoint())
                .option("superstream", superStream)
                .option("startingOffsets", "earliest")
                .option("singleActiveConsumer", "true")
                .option("consumerName", "ss-sac-" + System.currentTimeMillis())
                .option("metadataFields", "")
                .option("addressResolverClass",
                        "io.github.lukaszsamson.spark.rabbitmq.TestAddressResolver")
                .load();

        List<Row> rows = df.collectAsList();
        assertThat(rows).hasSize(10);

        Set<String> streams = rows.stream()
                .map(row -> row.getAs("stream").toString())
                .collect(Collectors.toSet());
        for (int p = 0; p < 1; p++) {
            assertThat(streams).contains(superStream + "-" + p);
        }
    }

    @Test
    void batchReadFromSuperStreamWithFiltering() {
        // Publish filtered messages to each partition stream
        for (int p = 0; p < PARTITION_COUNT; p++) {
            String partitionStream = superStream + "-" + p;
            publishMessagesWithFilterValue(partitionStream, 5, "alpha-p" + p + "-", "alpha");
            publishMessagesWithFilterValue(partitionStream, 5, "beta-p" + p + "-", "beta");
        }

        Dataset<Row> df = spark.read()
                .format("rabbitmq_streams")
                .option("endpoints", streamEndpoint())
                .option("superstream", superStream)
                .option("startingOffsets", "earliest")
                .option("filterValues", "alpha")
                .option("filterMatchUnfiltered", "false")
                .option("pollTimeoutMs", "500")
                .option("maxWaitMs", "10000")
                .option("metadataFields", "")
                .option("addressResolverClass",
                        "io.github.lukaszsamson.spark.rabbitmq.TestAddressResolver")
                .load();

        List<Row> rows = df.collectAsList();
        List<String> values = rows.stream()
                .map(row -> new String((byte[]) row.getAs("value"), StandardCharsets.UTF_8))
                .toList();

        assertThat(values).hasSize(5 * PARTITION_COUNT);
        assertThat(values).allMatch(v -> v.startsWith("alpha-"));

        Set<String> streams = rows.stream()
                .map(row -> row.getAs("stream").toString())
                .collect(Collectors.toSet());
        for (int p = 0; p < PARTITION_COUNT; p++) {
            assertThat(streams).contains(superStream + "-" + p);
        }
    }

    @Test
    void batchReadNonExistentSuperStreamFailsFast() {
        String missing = "missing-super-" + System.currentTimeMillis();

        Dataset<Row> df = spark.read()
                .format("rabbitmq_streams")
                .option("endpoints", streamEndpoint())
                .option("superstream", missing)
                .option("startingOffsets", "earliest")
                .option("metadataFields", "")
                .option("addressResolverClass",
                        "io.github.lukaszsamson.spark.rabbitmq.TestAddressResolver")
                .load();

        assertThatThrownBy(df::collectAsList)
                .satisfies(ex -> {
                    assertThat(ex).isInstanceOfAny(IllegalStateException.class, SparkException.class);

                    boolean foundPartitionMissingCause = false;
                    Throwable cursor = ex;
                    while (cursor != null) {
                        String message = cursor.getMessage();
                        if (message != null) {
                            String normalized = message.toLowerCase(Locale.ROOT);
                            if (normalized.contains("superstream") && normalized.contains("partition")) {
                                foundPartitionMissingCause = true;
                                break;
                            }
                        }
                        cursor = cursor.getCause();
                    }

                    assertThat(foundPartitionMissingCause)
                            .as("exception chain should mention missing superstream partitions")
                            .isTrue();
                });
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
                        "io.github.lukaszsamson.spark.rabbitmq.TestAddressResolver")
                .save();

        long totalMessages = spark.read()
                .format("rabbitmq_streams")
                .option("endpoints", streamEndpoint())
                .option("superstream", superStream)
                .option("startingOffsets", "earliest")
                .option("metadataFields", "")
                .option("addressResolverClass",
                        "io.github.lukaszsamson.spark.rabbitmq.TestAddressResolver")
                .load()
                .count();

        assertThat(totalMessages).isEqualTo(30L);
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
                        "io.github.lukaszsamson.spark.rabbitmq.TestAddressResolver")
                .save();

        List<Row> rows = spark.read()
                .format("rabbitmq_streams")
                .option("endpoints", streamEndpoint())
                .option("superstream", superStream)
                .option("startingOffsets", "earliest")
                .option("addressResolverClass",
                        "io.github.lukaszsamson.spark.rabbitmq.TestAddressResolver")
                .load()
                .collectAsList();

        assertThat(rows).hasSize(30);
        for (Row row : rows) {
            scala.collection.Map<String, String> appProps = row.getAs("application_properties");
            assertThat(appProps).isNotNull();
            assertThat(appProps.contains("routing_key")).isTrue();
        }
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
                                "io.github.lukaszsamson.spark.rabbitmq.TestAddressResolver")
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
                        "io.github.lukaszsamson.spark.rabbitmq.TestAddressResolver")
                .trigger(Trigger.AvailableNow())
                .start();

        query.awaitTermination(60_000);

        long totalMessages = spark.read()
                .format("rabbitmq_streams")
                .option("endpoints", streamEndpoint())
                .option("superstream", superStream)
                .option("startingOffsets", "earliest")
                .option("metadataFields", "")
                .option("addressResolverClass",
                        "io.github.lukaszsamson.spark.rabbitmq.TestAddressResolver")
                .load()
                .count();
        assertThat(totalMessages).isEqualTo(20L);
    }

    @Test
    void streamingWriteToSuperStreamAppendMode() throws Exception {
        StructType schema = new StructType()
                .add("value", DataTypes.BinaryType, false)
                .add("routing_key", DataTypes.StringType, true);

        List<Row> data = new ArrayList<>();
        for (int i = 0; i < 24; i++) {
            data.add(RowFactory.create(
                    ("ss-streaming-append-" + i).getBytes(),
                    String.valueOf(i % PARTITION_COUNT)));
        }

        Path inputDir = Files.createTempDirectory("spark-input-ss-append-").resolve("data");
        Path checkpointDir = Files.createTempDirectory("spark-checkpoint-ss-append-");
        spark.createDataFrame(data, schema).write().parquet(inputDir.toString());

        StreamingQuery query = spark.readStream()
                .schema(schema)
                .parquet(inputDir.toString())
                .writeStream()
                .format("rabbitmq_streams")
                .outputMode("append")
                .option("endpoints", streamEndpoint())
                .option("superstream", superStream)
                .option("routingStrategy", "hash")
                .option("checkpointLocation", checkpointDir.toString())
                .option("addressResolverClass",
                        "io.github.lukaszsamson.spark.rabbitmq.TestAddressResolver")
                .trigger(Trigger.AvailableNow())
                .start();

        query.awaitTermination(60_000);

        long totalMessages = spark.read()
                .format("rabbitmq_streams")
                .option("endpoints", streamEndpoint())
                .option("superstream", superStream)
                .option("startingOffsets", "earliest")
                .option("metadataFields", "")
                .option("addressResolverClass",
                        "io.github.lukaszsamson.spark.rabbitmq.TestAddressResolver")
                .load()
                .count();
        assertThat(totalMessages).isEqualTo(24L);
    }

    @Test
    void streamingWriteToSuperStreamUpdateMode() throws Exception {
        StructType schema = new StructType()
                .add("value", DataTypes.BinaryType, false)
                .add("routing_key", DataTypes.StringType, true);

        List<Row> data = new ArrayList<>();
        for (int i = 0; i < 24; i++) {
            data.add(RowFactory.create(
                    ("ss-streaming-update-" + i).getBytes(),
                    String.valueOf(i % PARTITION_COUNT)));
        }

        Path inputDir = Files.createTempDirectory("spark-input-ss-update-").resolve("data");
        Path checkpointDir = Files.createTempDirectory("spark-checkpoint-ss-update-");
        spark.createDataFrame(data, schema).write().parquet(inputDir.toString());

        StreamingQuery query = spark.readStream()
                .schema(schema)
                .parquet(inputDir.toString())
                .writeStream()
                .format("rabbitmq_streams")
                .outputMode("update")
                .option("endpoints", streamEndpoint())
                .option("superstream", superStream)
                .option("routingStrategy", "hash")
                .option("checkpointLocation", checkpointDir.toString())
                .option("addressResolverClass",
                        "io.github.lukaszsamson.spark.rabbitmq.TestAddressResolver")
                .trigger(Trigger.AvailableNow())
                .start();

        query.awaitTermination(60_000);

        long totalMessages = spark.read()
                .format("rabbitmq_streams")
                .option("endpoints", streamEndpoint())
                .option("superstream", superStream)
                .option("startingOffsets", "earliest")
                .option("metadataFields", "")
                .option("addressResolverClass",
                        "io.github.lukaszsamson.spark.rabbitmq.TestAddressResolver")
                .load()
                .count();
        assertThat(totalMessages).isEqualTo(24L);
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
                        "io.github.lukaszsamson.spark.rabbitmq.TestAddressResolver")
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
                        "io.github.lukaszsamson.spark.rabbitmq.TestAddressResolver")
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
                        "io.github.lukaszsamson.spark.rabbitmq.TestAddressResolver")
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
                        "io.github.lukaszsamson.spark.rabbitmq.TestAddressResolver")
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
                        "io.github.lukaszsamson.spark.rabbitmq.TestAddressResolver")
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
                        "io.github.lukaszsamson.spark.rabbitmq.TestAddressResolver")
                .load()
                .writeStream()
                .format("parquet")
                .option("path", outputDir.toString())
                .option("checkpointLocation", checkpointDir.toString())
                .trigger(Trigger.AvailableNow())
                .start();

        awaitQueryTerminationWithin(query, 120_000, "streamingSuperStreamDropPartitionNoDataLossWithNewMessages");
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
        AtomicBoolean deleted = new AtomicBoolean(false);

        Path checkpointDir = Files.createTempDirectory("spark-checkpoint-ss-avnow-fail-");

        // With failOnDataLoss=true, delete a partition after first batch to deterministically
        // make a planned partition unavailable during subsequent AvailableNow batches.
        StreamingQuery query = spark.readStream()
                .format("rabbitmq_streams")
                .option("endpoints", streamEndpoint())
                .option("superstream", superStream)
                .option("startingOffsets", "earliest")
                .option("maxRecordsPerTrigger", "15")
                .option("failOnDataLoss", "true")
                .option("metadataFields", "")
                .option("addressResolverClass",
                        "io.github.lukaszsamson.spark.rabbitmq.TestAddressResolver")
                .load()
                .writeStream()
                .foreachBatch((batch, batchId) -> {
                    if (batchId == 0L && deleted.compareAndSet(false, true)) {
                        deleteStream(superStream + "-1");
                    }
                })
                .option("checkpointLocation", checkpointDir.toString())
                .trigger(Trigger.AvailableNow())
                .start();

        assertThatThrownBy(() -> query.awaitTermination(120_000))
                .satisfies(ex -> assertThat(ex.getMessage())
                        .containsAnyOf("does not exist", "no longer exists"));
        assertThat(deleted.get()).isTrue();
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
                        "io.github.lukaszsamson.spark.rabbitmq.TestAddressResolver")
                .load()
                .writeStream()
                .format("parquet")
                .option("path", outputDir.toString())
                .option("checkpointLocation", checkpointDir.toString())
                .trigger(Trigger.AvailableNow())
                .start();

        awaitQueryTerminationWithin(query, 120_000, "streamingSuperStreamDropPartitionFailOnDataLossTrue");

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

    @Test
    void triggerAvailableNowRefreshesTopologyAfterPartitionDeletion() throws Exception {
        String p0 = superStream + "-0";
        String p1 = superStream + "-1";
        String p2 = superStream + "-2";

        publishMessages(p0, 120, "dyn-p0-");
        publishMessages(p1, 120, "dyn-p1-");
        publishMessages(p2, 120, "dyn-p2-");
        Thread.sleep(2000);

        Path checkpointDir = Files.createTempDirectory("spark-checkpoint-ss-dyn-");
        Path outputDir = Files.createTempDirectory("spark-output-ss-dyn-");
        AtomicBoolean deleted = new AtomicBoolean(false);

        StreamingQuery query = spark.readStream()
                .format("rabbitmq_streams")
                .option("endpoints", streamEndpoint())
                .option("superstream", superStream)
                .option("startingOffsets", "earliest")
                .option("failOnDataLoss", "false")
                .option("maxRecordsPerTrigger", "15")
                .option("metadataFields", "")
                .option("addressResolverClass",
                        "io.github.lukaszsamson.spark.rabbitmq.TestAddressResolver")
                .load()
                .writeStream()
                .foreachBatch((batch, batchId) -> {
                    batch.write()
                            .mode("append")
                            .format("parquet")
                            .save(outputDir.toString());
                    if (batchId == 0L && deleted.compareAndSet(false, true)) {
                        deleteStream(p2);
                    }
                })
                .option("checkpointLocation", checkpointDir.toString())
                .trigger(Trigger.AvailableNow())
                .start();

        awaitQueryTerminationWithin(query, 120_000, "streamingSuperStreamDropPartitionDataLossFalse");
        assertThat(deleted.get()).isTrue();

        List<Row> rows = spark.read().schema(MINIMAL_OUTPUT_SCHEMA)
                .parquet(outputDir.toString())
                .collectAsList();

        Map<String, Long> countsByStream = rows.stream()
                .collect(Collectors.groupingBy(
                        row -> row.getAs("stream").toString(),
                        Collectors.counting()));

        assertThat(countsByStream).containsEntry(p0, 120L);
        assertThat(countsByStream).containsEntry(p1, 120L);
        assertThat(countsByStream.getOrDefault(p2, 0L)).isLessThan(120L);
        assertThat(rows.size()).isGreaterThan(240);
        assertThat(rows.size()).isLessThan(360);
    }

    // ---- IT-DATALOSS-003: retention truncation with failOnDataLoss=false (superstream) ----

    @Test
    void streamingSuperStreamFailOnDataLossFalseForTruncation() throws Exception {
        deleteSuperStream(superStream);
        createSuperStreamWithRetention(superStream, PARTITION_COUNT, 1000, 500);

        int batches = 8;
        int perBatch = 20;
        int expectedTotal = batches * perBatch * PARTITION_COUNT;

        for (int p = 0; p < PARTITION_COUNT; p++) {
            String partition = superStream + "-" + p;
            for (int batch = 0; batch < batches; batch++) {
                publishMessages(partition, perBatch, "trunc-p" + p + "-b" + batch + "-");
                Thread.sleep(200);
            }
        }

        List<String> partitions = new ArrayList<>();
        for (int p = 0; p < PARTITION_COUNT; p++) {
            partitions.add(superStream + "-" + p);
        }
        Map<String, Long> firstOffsets = waitForTruncationAll(partitions, 15_000);
        long truncationDeadline = System.currentTimeMillis() + 30_000;
        int extraBatch = 0;
        while (firstOffsets.values().stream().anyMatch(offset -> offset <= 0)
                && System.currentTimeMillis() < truncationDeadline) {
            for (Map.Entry<String, Long> entry : firstOffsets.entrySet()) {
                if (entry.getValue() <= 0) {
                    publishMessages(entry.getKey(), perBatch, "trunc-extra-" + extraBatch + "-");
                }
            }
            Thread.sleep(250);
            firstOffsets = waitForTruncationAll(partitions, 5_000);
            extraBatch++;
        }
        assertThat(firstOffsets)
                .as("all superstream partitions must observe retention truncation before assertion phase")
                .allSatisfy((partition, firstOffset) -> assertThat(firstOffset)
                        .as("partition %s firstOffset", partition)
                        .isGreaterThan(0L));

        Path checkpointDir = Files.createTempDirectory("spark-checkpoint-ss-trunc-");
        Path outputDir = Files.createTempDirectory("spark-output-ss-trunc-");

        StreamingQuery query = spark.readStream()
                .format("rabbitmq_streams")
                .option("endpoints", streamEndpoint())
                .option("superstream", superStream)
                .option("startingOffsets", "offset")
                .option("startingOffset", "0")
                .option("failOnDataLoss", "false")
                .option("metadataFields", "")
                .option("addressResolverClass",
                        "io.github.lukaszsamson.spark.rabbitmq.TestAddressResolver")
                .load()
                .writeStream()
                .format("parquet")
                .option("path", outputDir.toString())
                .option("checkpointLocation", checkpointDir.toString())
                .trigger(Trigger.AvailableNow())
                .start();

        awaitQueryTerminationWithin(query, 120_000, "streamingSuperStreamDropPartitionDataLossTrue");

        List<Row> rows = spark.read().schema(MINIMAL_OUTPUT_SCHEMA)
                .parquet(outputDir.toString())
                .collectAsList();

        assertThat(rows).isNotEmpty();
        assertThat(rows.size()).isLessThan(expectedTotal);

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

            long minOffset = offsets.stream().mapToLong(Long::longValue).min().orElse(-1);
            long expectedMin = firstOffsets.getOrDefault(entry.getKey(), -1L);
            assertThat(minOffset).isGreaterThanOrEqualTo(expectedMin);
        }
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
                        "io.github.lukaszsamson.spark.rabbitmq.TestAddressResolver")
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
                        "io.github.lukaszsamson.spark.rabbitmq.TestAddressResolver")
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

    // ---- IT-RL-003: skewed superstream with bounded budget ----

    @Test
    void streamingSkewedSuperStreamBoundedBudget() throws Exception {
        // Publish skewed data: 100 to p0, 10 to p1, 1 to p2
        publishMessages(superStream + "-0", 100, "p0-");
        publishMessages(superStream + "-1", 10, "p1-");
        publishMessages(superStream + "-2", 1, "p2-");
        Thread.sleep(2000);

        Path checkpointDir = Files.createTempDirectory("spark-checkpoint-ss-skew-");
        Path outputDir = Files.createTempDirectory("spark-output-ss-skew-");

        // maxRecordsPerTrigger=20 → forces multiple batches for the skewed distribution
        StreamingQuery query = spark.readStream()
                .format("rabbitmq_streams")
                .option("endpoints", streamEndpoint())
                .option("superstream", superStream)
                .option("startingOffsets", "earliest")
                .option("maxRecordsPerTrigger", "20")
                .option("metadataFields", "")
                .option("addressResolverClass",
                        "io.github.lukaszsamson.spark.rabbitmq.TestAddressResolver")
                .load()
                .writeStream()
                .format("parquet")
                .option("path", outputDir.toString())
                .option("checkpointLocation", checkpointDir.toString())
                .trigger(Trigger.AvailableNow())
                .start();

        awaitQueryTerminationWithin(query, 120_000, "streamingSuperStreamDropPartitionContinueWithRemaining");

        // All 111 messages should be consumed
        Dataset<Row> result = spark.read().schema(MINIMAL_OUTPUT_SCHEMA)
                .parquet(outputDir.toString());
        assertThat(result.count()).isEqualTo(111);

        // Verify per-partition counts
        Map<String, Long> countsByStream = result.collectAsList().stream()
                .collect(Collectors.groupingBy(
                        row -> row.getAs("stream").toString(),
                        Collectors.counting()));

        assertThat(countsByStream).containsEntry(superStream + "-0", 100L);
        assertThat(countsByStream).containsEntry(superStream + "-1", 10L);
        assertThat(countsByStream).containsEntry(superStream + "-2", 1L);
    }

    // ---- IT-SINK-007: custom routing strategy ----

    @Test
    void batchWriteCustomRoutingStrategy() throws Exception {
        // Write 30 messages with custom routing that routes everything to partition 0
        StructType schema = new StructType()
                .add("value", DataTypes.BinaryType, false)
                .add("routing_key", DataTypes.StringType, true);

        List<Row> data = new ArrayList<>();
        for (int i = 0; i < 30; i++) {
            data.add(RowFactory.create(
                    ("custom-routed-" + i).getBytes(),
                    String.valueOf(i)));
        }

        Dataset<Row> df = spark.createDataFrame(data, schema);

        df.write()
                .format("rabbitmq_streams")
                .mode("append")
                .option("endpoints", streamEndpoint())
                .option("superstream", superStream)
                .option("routingStrategy", "custom")
                .option("partitionerClass",
                        "io.github.lukaszsamson.spark.rabbitmq.TestRoutingStrategy")
                .option("addressResolverClass",
                        "io.github.lukaszsamson.spark.rabbitmq.TestAddressResolver")
                .save();

        // TestRoutingStrategy routes all messages to partition 0
        // Read back via connector to verify all messages are in partition 0
        Dataset<Row> readDf = spark.read()
                .format("rabbitmq_streams")
                .option("endpoints", streamEndpoint())
                .option("superstream", superStream)
                .option("startingOffsets", "earliest")
                .option("metadataFields", "")
                .option("addressResolverClass",
                        "io.github.lukaszsamson.spark.rabbitmq.TestAddressResolver")
                .load();

        List<Row> rows = readDf.collectAsList();
        assertThat(rows).hasSize(30);

        // All messages should be in partition 0
        Map<String, Long> countsByStream = rows.stream()
                .collect(Collectors.groupingBy(
                        row -> row.getAs("stream").toString(),
                        Collectors.counting()));

        assertThat(countsByStream).containsEntry(superStream + "-0", 30L);
        assertThat(countsByStream).doesNotContainKey(superStream + "-1");
        assertThat(countsByStream).doesNotContainKey(superStream + "-2");
    }

    @Test
    void batchWriteCustomRoutingStrategyWithMetadataRouteLookup() throws Exception {
        String metadataSuper = uniqueStreamName();
        deleteSuperStream(metadataSuper);
        createSuperStreamWithBindingKeys(metadataSuper, "amer", "emea", "apac");

        StructType schema = new StructType()
                .add("value", DataTypes.BinaryType, false)
                .add("routing_key", DataTypes.StringType, false);

        List<Row> data = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            data.add(RowFactory.create(("amer-meta-" + i).getBytes(), "amer"));
            data.add(RowFactory.create(("emea-meta-" + i).getBytes(), "emea"));
            data.add(RowFactory.create(("apac-meta-" + i).getBytes(), "apac"));
        }

        spark.createDataFrame(data, schema)
                .write()
                .format("rabbitmq_streams")
                .mode("append")
                .option("endpoints", streamEndpoint())
                .option("superstream", metadataSuper)
                .option("routingStrategy", "custom")
                .option("partitionerClass",
                        "io.github.lukaszsamson.spark.rabbitmq.TestMetadataRouteRoutingStrategy")
                .option("addressResolverClass",
                        "io.github.lukaszsamson.spark.rabbitmq.TestAddressResolver")
                .save();

        Dataset<Row> readDf = spark.read()
                .format("rabbitmq_streams")
                .option("endpoints", streamEndpoint())
                .option("superstream", metadataSuper)
                .option("startingOffsets", "earliest")
                .option("metadataFields", "")
                .option("addressResolverClass",
                        "io.github.lukaszsamson.spark.rabbitmq.TestAddressResolver")
                .load();

        List<Row> rows = readDf.collectAsList();
        assertThat(rows).hasSize(30);

        Map<String, List<String>> byStream = rows.stream()
                .collect(Collectors.groupingBy(
                        row -> row.getAs("stream").toString(),
                        Collectors.mapping(row -> new String((byte[]) row.getAs("value")),
                                Collectors.toList())));

        assertThat(byStream.get(metadataSuper + "-amer")).allMatch(v -> v.startsWith("amer-meta-"));
        assertThat(byStream.get(metadataSuper + "-emea")).allMatch(v -> v.startsWith("emea-meta-"));
        assertThat(byStream.get(metadataSuper + "-apac")).allMatch(v -> v.startsWith("apac-meta-"));

        deleteSuperStream(metadataSuper);
    }

    @Test
    void batchWriteHashRoutingWithCustomHashFunction() throws Exception {
        StructType schema = new StructType()
                .add("value", DataTypes.BinaryType, false)
                .add("routing_key", DataTypes.StringType, true);

        List<Row> data = new ArrayList<>();
        for (int i = 0; i < 30; i++) {
            data.add(RowFactory.create(("custom-hash-" + i).getBytes(), "rk-" + i));
        }

        spark.createDataFrame(data, schema)
                .write()
                .format("rabbitmq_streams")
                .mode("append")
                .option("endpoints", streamEndpoint())
                .option("superstream", superStream)
                .option("routingStrategy", "hash")
                .option("hashFunctionClass", "io.github.lukaszsamson.spark.rabbitmq.TestHashFunction")
                .option("addressResolverClass",
                        "io.github.lukaszsamson.spark.rabbitmq.TestAddressResolver")
                .save();

        Dataset<Row> readDf = spark.read()
                .format("rabbitmq_streams")
                .option("endpoints", streamEndpoint())
                .option("superstream", superStream)
                .option("startingOffsets", "earliest")
                .option("metadataFields", "")
                .option("addressResolverClass",
                        "io.github.lukaszsamson.spark.rabbitmq.TestAddressResolver")
                .load();

        List<Row> rows = readDf.collectAsList();
        assertThat(rows).hasSize(30);
        assertThat(rows.stream()
                .map(row -> row.getAs("stream").toString())
                .distinct()
                .count()).isEqualTo(1L);
    }

    // ---- S4: key-based routing with binding-key partitions ----

    @Test
    void batchWriteKeyRoutingWithBindingKeys() throws Exception {
        String keySuper = uniqueStreamName();
        deleteSuperStream(keySuper);
        createSuperStreamWithBindingKeys(keySuper, "amer", "emea", "apac");

        StructType schema = new StructType()
                .add("value", DataTypes.BinaryType, false)
                .add("routing_key", DataTypes.StringType, false);

        List<Row> data = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            data.add(RowFactory.create(("amer-" + i).getBytes(), "amer"));
            data.add(RowFactory.create(("emea-" + i).getBytes(), "emea"));
            data.add(RowFactory.create(("apac-" + i).getBytes(), "apac"));
        }

        spark.createDataFrame(data, schema)
                .write()
                .format("rabbitmq_streams")
                .mode("append")
                .option("endpoints", streamEndpoint())
                .option("superstream", keySuper)
                .option("routingStrategy", "key")
                .option("addressResolverClass",
                        "io.github.lukaszsamson.spark.rabbitmq.TestAddressResolver")
                .save();

        Dataset<Row> df = spark.read()
                .format("rabbitmq_streams")
                .option("endpoints", streamEndpoint())
                .option("superstream", keySuper)
                .option("startingOffsets", "earliest")
                .option("metadataFields", "")
                .option("addressResolverClass",
                        "io.github.lukaszsamson.spark.rabbitmq.TestAddressResolver")
                .load();

        List<Row> rows = df.collectAsList();
        assertThat(rows).hasSize(30);

        List<String> streams = rows.stream()
                .map(row -> row.getAs("stream").toString())
                .distinct()
                .sorted()
                .toList();
        assertThat(streams).containsExactly(keySuper + "-amer", keySuper + "-apac", keySuper + "-emea");

        Map<String, List<String>> byStream = rows.stream()
                .collect(Collectors.groupingBy(
                        row -> row.getAs("stream").toString(),
                        Collectors.mapping(row -> new String((byte[]) row.getAs("value")),
                                Collectors.toList())));

        assertThat(byStream.get(keySuper + "-amer")).allMatch(v -> v.startsWith("amer-"));
        assertThat(byStream.get(keySuper + "-emea")).allMatch(v -> v.startsWith("emea-"));
        assertThat(byStream.get(keySuper + "-apac")).allMatch(v -> v.startsWith("apac-"));

        deleteSuperStream(keySuper);
    }

    // ---- S4: deduplication with super stream producer ----

    @Test
    void batchWriteSuperStreamDedupAfterRetry() throws Exception {
        String dedupSuper = uniqueStreamName();
        deleteSuperStream(dedupSuper);
        createSuperStream(dedupSuper, PARTITION_COUNT);

        StructType schema = new StructType()
                .add("value", DataTypes.BinaryType, false)
                .add("routing_key", DataTypes.StringType, true);

        List<Row> data = new ArrayList<>();
        for (int i = 0; i < 8; i++) {
            data.add(RowFactory.create(("dup-" + i).getBytes(), "0"));
        }

        Dataset<Row> df = spark.createDataFrame(data, schema);

        stopRabbitMqApp();
        try {
            assertThatThrownBy(() -> df.write()
                    .format("rabbitmq_streams")
                    .mode("append")
                    .option("endpoints", streamEndpoint())
                    .option("superstream", dedupSuper)
                    .option("producerName", "ss-dedup")
                    .option("publisherConfirmTimeoutMs", "500")
                    .option("enqueueTimeoutMs", "200")
                    .option("addressResolverClass",
                            "io.github.lukaszsamson.spark.rabbitmq.TestAddressResolver")
                    .save())
                    .satisfies(t -> {
                        String msg = t.getMessage() == null ? "" : t.getMessage();
                        assertThat(msg).containsAnyOf(
                                "Timed out waiting for publisher confirms",
                                "Locator not available",
                                "Connection is closed");
                    });
        } finally {
            startRabbitMqApp();
        }

        df.write()
                .format("rabbitmq_streams")
                .mode("append")
                .option("endpoints", streamEndpoint())
                .option("superstream", dedupSuper)
                .option("producerName", "ss-dedup")
                .option("addressResolverClass",
                        "io.github.lukaszsamson.spark.rabbitmq.TestAddressResolver")
                .save();

        Dataset<Row> readDf = spark.read()
                .format("rabbitmq_streams")
                .option("endpoints", streamEndpoint())
                .option("superstream", dedupSuper)
                .option("startingOffsets", "earliest")
                .option("metadataFields", "")
                .option("addressResolverClass",
                        "io.github.lukaszsamson.spark.rabbitmq.TestAddressResolver")
                .load();

        List<String> values = readDf.collectAsList().stream()
                .map(row -> new String((byte[]) row.getAs("value"), StandardCharsets.UTF_8))
                .toList();
        assertThat(new java.util.HashSet<>(values)).hasSize(8);
        deleteSuperStream(dedupSuper);
    }

    // ---- S4: large partition count (tracking consumer limits) ----

    @Test
    void batchReadLargeSuperStreamPartitionCount() throws Exception {
        String largeSuper = uniqueStreamName();
        deleteSuperStream(largeSuper);
        int partitionCount = 40;
        createSuperStream(largeSuper, partitionCount);

        for (int p = 0; p < partitionCount; p++) {
            publishMessages(largeSuper + "-" + p, 2, "p" + p + "-");
        }

        Dataset<Row> df = spark.read()
                .format("rabbitmq_streams")
                .option("endpoints", streamEndpoint())
                .option("superstream", largeSuper)
                .option("startingOffsets", "earliest")
                .option("maxTrackingConsumersByConnection", "5")
                .option("metadataFields", "")
                .option("addressResolverClass",
                        "io.github.lukaszsamson.spark.rabbitmq.TestAddressResolver")
                .load();

        List<Row> rows = df.collectAsList();
        assertThat(rows).hasSize(partitionCount * 2);

        Set<String> streams = rows.stream()
                .map(row -> row.getAs("stream").toString())
                .collect(Collectors.toSet());
        assertThat(streams).hasSize(partitionCount);

        deleteSuperStream(largeSuper);
    }

    // ---- IT-RL-005: AvailableNow batch count matches rate limit (superstream) ----

    @Test
    void triggerAvailableNowBatchCountMatchesMaxRecordsPerTriggerSuperStream() throws Exception {
        for (int p = 0; p < PARTITION_COUNT; p++) {
            publishMessages(superStream + "-" + p, 9, "rate-p" + p + "-");
        }
        Thread.sleep(2000);

        AtomicInteger batchCount = new AtomicInteger(0);
        Path checkpointDir = Files.createTempDirectory("spark-checkpoint-ss-rl-");

        StreamingQuery query = spark.readStream()
                .format("rabbitmq_streams")
                .option("endpoints", streamEndpoint())
                .option("superstream", superStream)
                .option("startingOffsets", "earliest")
                .option("maxRecordsPerTrigger", "10")
                .option("metadataFields", "")
                .option("addressResolverClass",
                        "io.github.lukaszsamson.spark.rabbitmq.TestAddressResolver")
                .load()
                .writeStream()
                .foreachBatch((org.apache.spark.api.java.function.VoidFunction2<Dataset<Row>, Long>)
                        (batch, batchId) -> batchCount.incrementAndGet())
                .option("checkpointLocation", checkpointDir.toString())
                .trigger(Trigger.AvailableNow())
                .start();

        awaitQueryTerminationWithin(query, 120_000, "streamingSuperStreamDropAndRecreateDifferentPartition");

        assertThat(batchCount.get()).isEqualTo(3);
    }

    // ---- IT-ORDER-002: per-partition offsets monotonic ----

    @Test
    void superstreamPerPartitionOffsetsMonotonic() throws Exception {
        // Publish 50 messages to each of 3 partitions (150 total)
        for (int p = 0; p < PARTITION_COUNT; p++) {
            publishMessages(superStream + "-" + p, 50, "order-p" + p + "-");
        }
        Thread.sleep(2000);

        Dataset<Row> df = spark.read()
                .format("rabbitmq_streams")
                .option("endpoints", streamEndpoint())
                .option("superstream", superStream)
                .option("startingOffsets", "earliest")
                .option("metadataFields", "")
                .option("addressResolverClass",
                        "io.github.lukaszsamson.spark.rabbitmq.TestAddressResolver")
                .load();

        List<Row> rows = df.collectAsList();
        assertThat(rows).hasSize(150);

        // Group by stream column, verify per-partition offsets
        Map<String, List<Long>> offsetsByStream = rows.stream()
                .collect(Collectors.groupingBy(
                        row -> row.getAs("stream").toString(),
                        Collectors.mapping(
                                row -> (Long) row.getAs("offset"),
                                Collectors.toList())));

        assertThat(offsetsByStream).hasSize(PARTITION_COUNT);

        for (Map.Entry<String, List<Long>> entry : offsetsByStream.entrySet()) {
            List<Long> offsets = entry.getValue().stream().sorted().toList();
            assertThat(offsets)
                    .as("Partition " + entry.getKey() + " should have 50 offsets")
                    .hasSize(50);

            // Verify strictly increasing and contiguous [0, 49]
            for (int i = 0; i < offsets.size(); i++) {
                assertThat(offsets.get(i))
                        .as("Offset at index " + i + " in partition " + entry.getKey())
                        .isEqualTo((long) i);
            }
        }
    }

    // ---- IT-ORDER-003: add superstream partition while query is running ----

    @Test
    void streamingSuperStreamAddsPartitionDuringRun() throws Exception {
        for (int p = 0; p < PARTITION_COUNT; p++) {
            publishMessages(superStream + "-" + p, 10, "pre-p" + p + "-");
        }
        Thread.sleep(2000);

        Path checkpointDir = Files.createTempDirectory("spark-checkpoint-ss-add-");
        Path outputDir = Files.createTempDirectory("spark-output-ss-add-");
        AtomicBoolean added = new AtomicBoolean(false);
        java.util.concurrent.atomic.AtomicReference<String> addFailure = new java.util.concurrent.atomic.AtomicReference<>();

        StreamingQuery query = spark.readStream()
                .format("rabbitmq_streams")
                .option("endpoints", streamEndpoint())
                .option("superstream", superStream)
                .option("startingOffsets", "earliest")
                .option("maxRecordsPerTrigger", "5")
                .option("metadataFields", "")
                .option("addressResolverClass",
                        "io.github.lukaszsamson.spark.rabbitmq.TestAddressResolver")
                .load()
                .writeStream()
                .foreachBatch((batch, batchId) -> {
                    batch.write()
                            .mode("append")
                            .format("parquet")
                            .save(outputDir.toString());
                    if (batchId == 0L && added.compareAndSet(false, true)) {
                        try {
                            addSuperStreamPartitions(superStream, 1);
                            publishMessages(superStream + "-3", 10, "new-p3-");
                        } catch (RuntimeException e) {
                            addFailure.compareAndSet(null, e.getMessage());
                        }
                    }
                })
                .option("checkpointLocation", checkpointDir.toString())
                .trigger(Trigger.AvailableNow())
                .start();

        awaitQueryTerminationWithin(query, 120_000, "streamingSuperStreamDropAndRecreateWithOffsetTracking");
        if (addFailure.get() != null) {
            assertThat(addFailure.get())
                    .as("partition-add capability missing in this environment")
                    .contains("reference_already_exists");
            return;
        }
        assertThat(added.get()).isTrue();

        List<Row> rows = spark.read().schema(MINIMAL_OUTPUT_SCHEMA)
                .parquet(outputDir.toString())
                .collectAsList();

        Map<String, Long> countsByStream = rows.stream()
                .collect(Collectors.groupingBy(
                        row -> row.getAs("stream").toString(),
                        Collectors.counting()));

        assertThat(countsByStream).containsEntry(superStream + "-3", 10L);
    }

    // ---- IT-ORDER-004: remove then recreate partition stream with same name ----

    @Test
    void streamingSuperStreamRemoveAndRecreatePartition() throws Exception {
        publishMessages(superStream + "-0", 20, "p0-");
        publishMessages(superStream + "-1", 20, "p1-");
        publishMessages(superStream + "-2", 20, "p2-");
        Thread.sleep(2000);

        Path checkpointDir = Files.createTempDirectory("spark-checkpoint-ss-recreate-");
        Path outputDir = Files.createTempDirectory("spark-output-ss-recreate-");
        AtomicBoolean recreated = new AtomicBoolean(false);

        StreamingQuery query = spark.readStream()
                .format("rabbitmq_streams")
                .option("endpoints", streamEndpoint())
                .option("superstream", superStream)
                .option("startingOffsets", "earliest")
                .option("failOnDataLoss", "false")
                .option("maxRecordsPerTrigger", "10")
                .option("pollTimeoutMs", "200")
                .option("maxWaitMs", "2000")
                .option("metadataFields", "")
                .option("addressResolverClass",
                        "io.github.lukaszsamson.spark.rabbitmq.TestAddressResolver")
                .load()
                .writeStream()
                .foreachBatch((batch, batchId) -> {
                    batch.write()
                            .mode("append")
                            .format("parquet")
                            .save(outputDir.toString());
                    if (batchId == 0L && recreated.compareAndSet(false, true)) {
                        deleteStream(superStream + "-2");
                        Thread.sleep(500);
                        createStream(superStream + "-2");
                        publishMessages(superStream + "-2", 12, "p2-re-");
                    }
                })
                .option("checkpointLocation", checkpointDir.toString())
                .trigger(Trigger.AvailableNow())
                .start();

        awaitQueryTerminationWithin(query, 120_000, "streamingSuperStreamRecreatePartitionDataLossFalse");
        assertThat(recreated.get()).isTrue();

        List<Row> rows = spark.read().schema(MINIMAL_OUTPUT_SCHEMA)
                .parquet(outputDir.toString())
                .collectAsList();

        List<String> values = rows.stream()
                .map(row -> new String((byte[]) row.getAs("value")))
                .toList();

        assertThat(values).isNotEmpty();
        assertThat(values).anyMatch(v -> v.startsWith("p0-") || v.startsWith("p1-"));
    }

    // ---- IT-DATALOSS-005: deleted/recreated superstream partition between batches ----

    @Test
    void streamingSuperStreamRecreatePartitionNoDuplicates() throws Exception {
        for (int p = 0; p < PARTITION_COUNT; p++) {
            publishMessages(superStream + "-" + p, 15, "pre-p" + p + "-");
        }
        Thread.sleep(2000);

        Path checkpointDir = Files.createTempDirectory("spark-checkpoint-ss-loss-");
        AtomicBoolean recreated = new AtomicBoolean(false);
        List<String> values = Collections.synchronizedList(new ArrayList<>());

        StreamingQuery query = spark.readStream()
                .format("rabbitmq_streams")
                .option("endpoints", streamEndpoint())
                .option("superstream", superStream)
                .option("startingOffsets", "earliest")
                .option("failOnDataLoss", "false")
                .option("maxRecordsPerTrigger", "8")
                .option("metadataFields", "")
                .option("addressResolverClass",
                        "io.github.lukaszsamson.spark.rabbitmq.TestAddressResolver")
                .load()
                .writeStream()
                .foreachBatch((batch, batchId) -> {
                    List<Row> rows = batch.select("value").collectAsList();
                    for (Row row : rows) {
                        values.add(new String((byte[]) row.getAs("value"), StandardCharsets.UTF_8));
                    }
                })
                .option("checkpointLocation", checkpointDir.toString())
                .trigger(Trigger.ProcessingTime(200))
                .start();

        CompletableFuture<Void> recreateTask = CompletableFuture.runAsync(() -> {
            try {
                Thread.sleep(800);
                deleteStream(superStream + "-1");
                Thread.sleep(500);
                createStream(superStream + "-1");
                publishMessages(superStream + "-1", 8, "re-p1-");
                recreated.set(true);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });

        try {
            waitUntil(
                    recreated::get,
                    30_000,
                    "superstream partition recreation to happen");
            waitUntil(
                    () -> values.size() >= 24,
                    60_000,
                    "sufficient records to be consumed after recreation");
            recreateTask.join();
        } finally {
            query.stop();
            query.awaitTermination(10_000);
        }
        assertThat(recreated.get()).isTrue();
        assertThat(values).isNotEmpty();
        assertThat(Set.copyOf(values)).hasSize(values.size());
    }

    // ---- IT-STRESS-002: churn test with failOnDataLoss=false ----

    @Test
    void superStreamPartitionChurnDoesNotExplodeDuplicates() throws Exception {
        for (int p = 0; p < PARTITION_COUNT; p++) {
            publishMessages(superStream + "-" + p, 12, "churn-pre-p" + p + "-");
        }
        Thread.sleep(2000);

        Path checkpointDir = Files.createTempDirectory("spark-checkpoint-ss-churn-");
        AtomicInteger churned = new AtomicInteger(0);
        List<Row> captured = Collections.synchronizedList(new ArrayList<>());

        StreamingQuery query = spark.readStream()
                .format("rabbitmq_streams")
                .option("endpoints", streamEndpoint())
                .option("superstream", superStream)
                .option("startingOffsets", "earliest")
                .option("failOnDataLoss", "false")
                .option("maxRecordsPerTrigger", "8")
                .option("metadataFields", "")
                .option("addressResolverClass",
                        "io.github.lukaszsamson.spark.rabbitmq.TestAddressResolver")
                .load()
                .writeStream()
                .foreachBatch((batch, batchId) -> {
                    captured.addAll(batch.select("stream", "offset").collectAsList());
                })
                .option("checkpointLocation", checkpointDir.toString())
                .trigger(Trigger.ProcessingTime(200))
                .start();

        CompletableFuture<Void> churnTask = CompletableFuture.runAsync(() -> {
            try {
                for (int i = 0; i < 3; i++) {
                    String target = superStream + "-" + (i % PARTITION_COUNT);
                    deleteStream(target);
                    Thread.sleep(300);
                    createStream(target);
                    publishMessages(target, 6, "churn-new-" + i + "-");
                    churned.incrementAndGet();
                    Thread.sleep(500);
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });

        try {
            waitUntil(
                    () -> churned.get() == 3,
                    60_000,
                    "three churn cycles to complete");
            waitUntil(
                    () -> captured.size() >= 20,
                    60_000,
                    "rows to be captured for churn scenario");
            churnTask.join();
        } finally {
            query.stop();
            query.awaitTermination(10_000);
        }
        assertThat(churned.get()).isEqualTo(3);
        assertThat(captured).isNotEmpty();

        Map<String, List<Long>> offsetsByStream = captured.stream()
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

    private void awaitQueryTerminationWithin(StreamingQuery query, long timeoutMs, String context)
            throws Exception {
        long effectiveTimeoutMs = timeoutMs;
        long deadlineNs = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(effectiveTimeoutMs);
        while (query.isActive() && System.nanoTime() < deadlineNs) {
            Thread.sleep(250);
        }
        if (query.isActive()) {
            String progress = query.lastProgress() == null
                    ? "none"
                    : query.lastProgress().prettyJson();
            Thread stopper = new Thread(() -> {
                try {
                    query.stop();
                } catch (Exception ignored) {
                    // Best-effort stop for diagnostics path.
                }
            }, "superstream-it-stop-" + context);
            stopper.setDaemon(true);
            stopper.start();
            stopper.join(10_000);
            throw new AssertionError(
                    "Timed out waiting for streaming query termination in " + context
                            + " after " + effectiveTimeoutMs + "ms. Last progress: " + progress);
        }
        if (query.exception().isDefined()) {
            throw new AssertionError(
                    "Streaming query failed in " + context + ": "
                            + query.exception().get().getMessage(),
                    query.exception().get());
        }
    }

    private void waitUntil(BooleanSupplier condition, long timeoutMs, String description)
            throws Exception {
        long deadlineNs = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMs);
        while (!condition.getAsBoolean() && System.nanoTime() < deadlineNs) {
            Thread.sleep(200);
        }
        if (!condition.getAsBoolean()) {
            throw new AssertionError("Timed out after " + timeoutMs + "ms waiting for " + description);
        }
    }

}
