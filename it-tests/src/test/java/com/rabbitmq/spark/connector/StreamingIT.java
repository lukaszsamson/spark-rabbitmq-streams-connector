package com.rabbitmq.spark.connector;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.rabbitmq.stream.NoOffsetException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Integration tests for Structured Streaming micro-batch source and sink.
 */
class StreamingIT extends AbstractRabbitMQIT {

    private String sourceStream;
    private String sinkStream;
    private Path checkpointDir;

    @BeforeEach
    void setUp() throws Exception {
        sourceStream = uniqueStreamName();
        sinkStream = uniqueStreamName();
        createStream(sourceStream);
        createStream(sinkStream);
        checkpointDir = Files.createTempDirectory("spark-checkpoint-");
    }

    @AfterEach
    void tearDown() {
        deleteStream(sourceStream);
        deleteStream(sinkStream);
    }

    @Test
    void streamingReadWithTriggerAvailableNow() throws Exception {
        // Pre-publish messages
        publishMessages(sourceStream, 100);

        // Wait for messages to be committed
        Thread.sleep(2000);

        // Read with Trigger.AvailableNow — processes all available data then stops
        Path outputDir = Files.createTempDirectory("spark-output-");

        StructType outputSchema = new StructType()
                .add("value", DataTypes.BinaryType)
                .add("stream", DataTypes.StringType)
                .add("offset", DataTypes.LongType)
                .add("chunk_timestamp", DataTypes.LongType);

        StreamingQuery query = spark.readStream()
                .format("rabbitmq_streams")
                .option("endpoints", streamEndpoint())
                .option("stream", sourceStream)
                .option("startingOffsets", "earliest")
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

        // AvailableNow will process all data and terminate
        query.awaitTermination(120_000);

        // Read the output
        Dataset<Row> result = spark.read().schema(outputSchema).parquet(outputDir.toString());
        long count = result.count();
        assertThat(count).isEqualTo(100);
    }

    @Test
    void streamingReadWithMaxRecordsPerTrigger() throws Exception {
        publishMessages(sourceStream, 50);

        // Wait for messages to be committed
        Thread.sleep(2000);

        Path outputDir = Files.createTempDirectory("spark-output-");

        // Use maxRecordsPerTrigger to limit batch size
        StreamingQuery query = spark.readStream()
                .format("rabbitmq_streams")
                .option("endpoints", streamEndpoint())
                .option("stream", sourceStream)
                .option("startingOffsets", "earliest")
                .option("maxRecordsPerTrigger", "10")
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

        StructType outputSchema = new StructType()
                .add("value", DataTypes.BinaryType)
                .add("stream", DataTypes.StringType)
                .add("offset", DataTypes.LongType)
                .add("chunk_timestamp", DataTypes.LongType);

        Dataset<Row> result = spark.read().schema(outputSchema).parquet(outputDir.toString());
        assertThat(result.count()).isEqualTo(50);
    }

    @Test
    void streamingWriteThenRead() throws Exception {
        // Write via streaming sink
        StructType schema = new StructType()
                .add("value", DataTypes.BinaryType, false);

        List<Row> data = new ArrayList<>();
        for (int i = 0; i < 25; i++) {
            data.add(RowFactory.create(("streaming-write-" + i).getBytes()));
        }

        // Use a memory source as a streaming source, writing to RabbitMQ sink
        Dataset<Row> staticDf = spark.createDataFrame(data, schema);

        Path inputParent = Files.createTempDirectory("spark-input-");
        Path inputDir = inputParent.resolve("data");
        Path sinkCheckpointDir = Files.createTempDirectory("spark-sink-checkpoint-");

        // Write the data as parquet files that we'll stream from
        staticDf.write().parquet(inputDir.toString());

        // Stream from file source to RabbitMQ sink
        StreamingQuery query = spark.readStream()
                .schema(schema)
                .parquet(inputDir.toString())
                .writeStream()
                .format("rabbitmq_streams")
                .option("endpoints", streamEndpoint())
                .option("stream", sinkStream)
                .option("checkpointLocation", sinkCheckpointDir.toString())
                .option("addressResolverClass",
                        "com.rabbitmq.spark.connector.TestAddressResolver")
                .trigger(Trigger.AvailableNow())
                .start();

        query.awaitTermination(60_000);

        // Verify messages arrived at the broker
        var messages = consumeMessages(sinkStream, 25);
        assertThat(messages).hasSize(25);

        List<String> bodies = messages.stream()
                .map(msg -> new String(msg.getBodyAsBinary()))
                .sorted()
                .toList();

        assertThat(bodies).contains("streaming-write-0", "streaming-write-24");
    }

    @Test
    void streamingReadResumesFromCheckpoint() throws Exception {
        // Phase 1: publish 30 messages and read them
        publishMessages(sourceStream, 30);
        Thread.sleep(2000);

        Path outputDir = Files.createTempDirectory("spark-output-");

        StreamingQuery query1 = spark.readStream()
                .format("rabbitmq_streams")
                .option("endpoints", streamEndpoint())
                .option("stream", sourceStream)
                .option("startingOffsets", "earliest")
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

        StructType outputSchema = new StructType()
                .add("value", DataTypes.BinaryType)
                .add("stream", DataTypes.StringType)
                .add("offset", DataTypes.LongType)
                .add("chunk_timestamp", DataTypes.LongType);

        long count1 = spark.read().schema(outputSchema).parquet(outputDir.toString()).count();
        assertThat(count1).isEqualTo(30);

        // Phase 2: publish 20 more messages
        publishMessages(sourceStream, 20, "phase2-");
        Thread.sleep(2000);

        // Resume from checkpoint — should only read the new 20 messages
        StreamingQuery query2 = spark.readStream()
                .format("rabbitmq_streams")
                .option("endpoints", streamEndpoint())
                .option("stream", sourceStream)
                .option("startingOffsets", "earliest")
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

        long count2 = spark.read().schema(outputSchema).parquet(outputDir.toString()).count();
        assertThat(count2).isEqualTo(50); // 30 + 20
    }

    @Test
    void streamingWithServerSideOffsetTracking() throws Exception {
        String consumerName = "it-consumer-" + System.currentTimeMillis();
        publishMessages(sourceStream, 50);
        Thread.sleep(2000);

        Path outputDir = Files.createTempDirectory("spark-output-");

        // Run with server-side offset tracking enabled
        StreamingQuery query = spark.readStream()
                .format("rabbitmq_streams")
                .option("endpoints", streamEndpoint())
                .option("stream", sourceStream)
                .option("startingOffsets", "earliest")
                .option("consumerName", consumerName)
                .option("serverSideOffsetTracking", "true")
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

        StructType outputSchema = new StructType()
                .add("value", DataTypes.BinaryType)
                .add("stream", DataTypes.StringType)
                .add("offset", DataTypes.LongType)
                .add("chunk_timestamp", DataTypes.LongType);

        long count = spark.read().schema(outputSchema).parquet(outputDir.toString()).count();
        assertThat(count).isEqualTo(50);

        // Verify that broker stored the offset
        long storedOffset = queryStoredOffset(consumerName, sourceStream);
        assertThat(storedOffset).isGreaterThanOrEqualTo(0);
    }

    @Test
    void streamingWithServerSideOffsetTrackingDisabled() throws Exception {
        String consumerName = "it-consumer-disabled-" + System.currentTimeMillis();
        publishMessages(sourceStream, 30);
        Thread.sleep(2000);

        Path outputDir = Files.createTempDirectory("spark-output-");

        // Run with server-side offset tracking explicitly disabled
        StreamingQuery query = spark.readStream()
                .format("rabbitmq_streams")
                .option("endpoints", streamEndpoint())
                .option("stream", sourceStream)
                .option("startingOffsets", "earliest")
                .option("consumerName", consumerName)
                .option("serverSideOffsetTracking", "false")
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

        StructType outputSchema = new StructType()
                .add("value", DataTypes.BinaryType)
                .add("stream", DataTypes.StringType)
                .add("offset", DataTypes.LongType)
                .add("chunk_timestamp", DataTypes.LongType);

        long count = spark.read().schema(outputSchema).parquet(outputDir.toString()).count();
        assertThat(count).isEqualTo(30);

        // Verify that NO broker offset was stored
        assertThat(hasStoredOffset(consumerName, sourceStream)).isFalse();
    }

    @Test
    void streamingRecoveryFromBrokerStoredOffsets() throws Exception {
        String consumerName = "it-recovery-" + System.currentTimeMillis();
        publishMessages(sourceStream, 40);
        Thread.sleep(2000);

        // Phase 1: Process all messages with server-side offset tracking
        Path outputDir1 = Files.createTempDirectory("spark-output-phase1-");

        StreamingQuery query1 = spark.readStream()
                .format("rabbitmq_streams")
                .option("endpoints", streamEndpoint())
                .option("stream", sourceStream)
                .option("startingOffsets", "earliest")
                .option("consumerName", consumerName)
                .option("serverSideOffsetTracking", "true")
                .option("metadataFields", "")
                .option("addressResolverClass",
                        "com.rabbitmq.spark.connector.TestAddressResolver")
                .load()
                .writeStream()
                .format("parquet")
                .option("path", outputDir1.toString())
                .option("checkpointLocation", checkpointDir.toString())
                .trigger(Trigger.AvailableNow())
                .start();

        query1.awaitTermination(120_000);

        StructType outputSchema = new StructType()
                .add("value", DataTypes.BinaryType)
                .add("stream", DataTypes.StringType)
                .add("offset", DataTypes.LongType)
                .add("chunk_timestamp", DataTypes.LongType);

        assertThat(spark.read().schema(outputSchema)
                .parquet(outputDir1.toString()).count()).isEqualTo(40);

        // Phase 2: Publish 20 more messages
        publishMessages(sourceStream, 20, "phase2-");
        Thread.sleep(2000);

        // Phase 3: Start a NEW query (different checkpoint!) with same consumerName
        // It should recover from broker-stored offsets and only read the new messages
        Path newCheckpointDir = Files.createTempDirectory("spark-checkpoint-recovery-");
        Path outputDir2 = Files.createTempDirectory("spark-output-phase2-");

        StreamingQuery query2 = spark.readStream()
                .format("rabbitmq_streams")
                .option("endpoints", streamEndpoint())
                .option("stream", sourceStream)
                .option("startingOffsets", "earliest")
                .option("consumerName", consumerName)
                .option("serverSideOffsetTracking", "true")
                .option("metadataFields", "")
                .option("addressResolverClass",
                        "com.rabbitmq.spark.connector.TestAddressResolver")
                .load()
                .writeStream()
                .format("parquet")
                .option("path", outputDir2.toString())
                .option("checkpointLocation", newCheckpointDir.toString())
                .trigger(Trigger.AvailableNow())
                .start();

        query2.awaitTermination(120_000);

        // Should only read the 20 new messages (not re-read the 40 from phase 1)
        long count2 = spark.read().schema(outputSchema)
                .parquet(outputDir2.toString()).count();
        assertThat(count2).isEqualTo(20);
    }

    @Test
    void triggerAvailableNowRespectsSnapshotCeiling() throws Exception {
        // Phase 1: publish initial messages
        publishMessages(sourceStream, 60);
        Thread.sleep(2000);

        Path outputDir = Files.createTempDirectory("spark-output-avnow-");

        // Start query with Trigger.AvailableNow — this takes a snapshot of tail offsets
        // and processes only data up to that snapshot
        StreamingQuery query = spark.readStream()
                .format("rabbitmq_streams")
                .option("endpoints", streamEndpoint())
                .option("stream", sourceStream)
                .option("startingOffsets", "earliest")
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

        // AvailableNow should process all 60 messages and terminate
        query.awaitTermination(120_000);

        StructType outputSchema = new StructType()
                .add("value", DataTypes.BinaryType)
                .add("stream", DataTypes.StringType)
                .add("offset", DataTypes.LongType)
                .add("chunk_timestamp", DataTypes.LongType);

        long count = spark.read().schema(outputSchema).parquet(outputDir.toString()).count();
        assertThat(count).isEqualTo(60);

        // Verify the query terminated (AvailableNow must terminate after draining)
        assertThat(query.isActive()).isFalse();
    }

    @Test
    void triggerAvailableNowTerminatesOnEmptyStream() throws Exception {
        // Empty stream — AvailableNow should take snapshot (empty) and terminate immediately
        Path outputDir = Files.createTempDirectory("spark-output-avnow-empty-");

        StreamingQuery query = spark.readStream()
                .format("rabbitmq_streams")
                .option("endpoints", streamEndpoint())
                .option("stream", sourceStream)
                .option("startingOffsets", "earliest")
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

        query.awaitTermination(60_000);

        assertThat(query.isActive()).isFalse();
    }

    // ---- IT-OFFSET-001: startingOffsets=latest skips historical data ----

    @Test
    void streamingStartingOffsetsLatestSkipsHistorical() throws Exception {
        // Pre-publish historical messages that should NOT be read
        publishMessages(sourceStream, 50, "historical-");
        Thread.sleep(2000);

        Path outputDir = Files.createTempDirectory("spark-output-latest-");

        StreamingQuery query = spark.readStream()
                .format("rabbitmq_streams")
                .option("endpoints", streamEndpoint())
                .option("stream", sourceStream)
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

        query.awaitTermination(60_000);
        assertThat(readOutputCount(outputDir)).isEqualTo(0L);
    }

    @Test
    void streamingStartingOffsetsLatestThenNewData() throws Exception {
        // Pre-publish historical messages that should be skipped
        publishMessages(sourceStream, 50, "historical-");
        Thread.sleep(2000);

        Path outputDir = Files.createTempDirectory("spark-output-latest-new-");

        // Phase 1: start with latest, establish checkpoint at tail
        StreamingQuery query1 = spark.readStream()
                .format("rabbitmq_streams")
                .option("endpoints", streamEndpoint())
                .option("stream", sourceStream)
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

        // Phase 2: publish new messages, then resume — should only get new ones
        publishMessages(sourceStream, 30, "new-");
        Thread.sleep(2000);

        StreamingQuery query2 = spark.readStream()
                .format("rabbitmq_streams")
                .option("endpoints", streamEndpoint())
                .option("stream", sourceStream)
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

        List<Row> outputRows = readOutputRows(outputDir);
        assertThat(outputRows).hasSize(30);

        List<String> values = outputRows.stream()
                .map(row -> new String((byte[]) row.getAs("value")))
                .toList();
        assertThat(values).allMatch(v -> v.startsWith("new-"));
    }

    // ---- IT-AVNOW-001: publish during run excluded by snapshot ----

    @Test
    void triggerAvailableNowExcludesPostSnapshotData() throws Exception {
        // Pre-publish messages that form the snapshot ceiling
        publishMessages(sourceStream, 50, "pre-");
        Thread.sleep(2000);

        Path outputDir = Files.createTempDirectory("spark-output-avnow-ceiling-");

        // Publish more messages asynchronously during query execution
        // Use a delay to ensure query has started and snapshotted
        CompletableFuture<Void> asyncPublish = publishMessagesAsync(
                sourceStream, 50, "post-", 3000);

        // Start query with rate limiting to slow processing
        StreamingQuery query = spark.readStream()
                .format("rabbitmq_streams")
                .option("endpoints", streamEndpoint())
                .option("stream", sourceStream)
                .option("startingOffsets", "earliest")
                .option("maxRecordsPerTrigger", "10")
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

        // Should have exactly the pre-snapshot messages (50), not the post-snapshot ones
        long count = spark.read().schema(MINIMAL_OUTPUT_SCHEMA)
                .parquet(outputDir.toString()).count();
        assertThat(count).isEqualTo(50);

        // Verify no post-snapshot messages leaked in
        List<String> values = spark.read().schema(MINIMAL_OUTPUT_SCHEMA)
                .parquet(outputDir.toString())
                .collectAsList().stream()
                .map(row -> new String((byte[]) row.getAs("value")))
                .toList();
        assertThat(values).allMatch(v -> v.startsWith("pre-"));
    }

    // ---- IT-AVNOW-004: AvailableNow terminates with multiple batches ----

    @Test
    void triggerAvailableNowCompletesWithMultipleBatches() throws Exception {
        publishMessages(sourceStream, 100);
        Thread.sleep(2000);

        Path outputDir = Files.createTempDirectory("spark-output-avnow-multi-");

        // Force multiple micro-batches with rate limiting
        StreamingQuery query = spark.readStream()
                .format("rabbitmq_streams")
                .option("endpoints", streamEndpoint())
                .option("stream", sourceStream)
                .option("startingOffsets", "earliest")
                .option("maxRecordsPerTrigger", "20")
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

        // Must terminate within timeout (no infinite loop)
        boolean terminated = query.awaitTermination(120_000);
        assertThat(terminated).isTrue();
        assertThat(query.isActive()).isFalse();

        // All 100 messages should be in the output (no gaps)
        long count = spark.read().schema(MINIMAL_OUTPUT_SCHEMA)
                .parquet(outputDir.toString()).count();
        assertThat(count).isEqualTo(100);
    }

    // ---- IT-DATALOSS-001: retention truncation with failOnDataLoss=true ----

    @Test
    void streamingFailOnDataLossTrueForTruncation() throws Exception {
        // Create a stream with very small retention to force truncation
        String truncStream = uniqueStreamName();
        deleteStream(truncStream); // ensure clean state
        // Use small segment size (500 bytes) and max length (1000 bytes)
        createStreamWithRetention(truncStream, 1000, 500);

        try {
            // Publish enough data to trigger truncation
            // Each message ~20 bytes, 500 messages = ~10KB >> 1KB limit
            for (int batch = 0; batch < 10; batch++) {
                publishMessages(truncStream, 50, "trunc-batch" + batch + "-");
                Thread.sleep(500); // allow segments to flush
            }

            // Wait for truncation to occur
            long firstOffset = waitForTruncation(truncStream, 30_000);
            Assumptions.assumeTrue(firstOffset > 0,
                    "Retention truncation did not occur in time");

            // Now try to read from offset 0 with failOnDataLoss=true
            Dataset<Row> df = spark.read()
                    .format("rabbitmq_streams")
                    .option("endpoints", streamEndpoint())
                    .option("stream", truncStream)
                    .option("startingOffsets", "offset")
                    .option("startingOffset", "0")
                    .option("failOnDataLoss", "true")
                    .option("metadataFields", "")
                    .option("addressResolverClass",
                            "com.rabbitmq.spark.connector.TestAddressResolver")
                    .load();

            assertThatThrownBy(df::collectAsList)
                    .isInstanceOf(Exception.class);
        } finally {
            deleteStream(truncStream);
        }
    }

    // ---- IT-DATALOSS-002: retention truncation with failOnDataLoss=false ----

    @Test
    void streamingFailOnDataLossFalseForTruncation() throws Exception {
        // Create a stream with very small retention to force truncation
        String truncStream = uniqueStreamName();
        deleteStream(truncStream);
        createStreamWithRetention(truncStream, 1000, 500);

        try {
            // Publish enough data to trigger truncation
            for (int batch = 0; batch < 10; batch++) {
                publishMessages(truncStream, 50, "trunc-batch" + batch + "-");
                Thread.sleep(500);
            }

            long firstOffset = waitForTruncation(truncStream, 30_000);
            Assumptions.assumeTrue(firstOffset > 0,
                    "Retention truncation did not occur in time");

            // Read from offset 0 with failOnDataLoss=false — should advance to first available
            Dataset<Row> df = spark.read()
                    .format("rabbitmq_streams")
                    .option("endpoints", streamEndpoint())
                    .option("stream", truncStream)
                    .option("startingOffsets", "offset")
                    .option("startingOffset", "0")
                    .option("failOnDataLoss", "false")
                    .option("metadataFields", "")
                    .option("addressResolverClass",
                            "com.rabbitmq.spark.connector.TestAddressResolver")
                    .load();

            List<Row> rows = df.collectAsList();
            // Should have some rows (whatever survived retention)
            assertThat(rows).isNotEmpty();

            // Verify no duplicates
            Set<Long> offsets = rows.stream()
                    .map(row -> (Long) row.getAs("offset"))
                    .collect(Collectors.toSet());
            assertThat(offsets).hasSize(rows.size());

            // Verify first offset is >= firstOffset (truncation point)
            long minReadOffset = offsets.stream()
                    .mapToLong(Long::longValue).min().orElse(-1);
            assertThat(minReadOffset).isGreaterThanOrEqualTo(firstOffset);
        } finally {
            deleteStream(truncStream);
        }
    }

    // ---- IT-DATALOSS-004: deleted stream during running micro-batch ----

    @Test
    void streamingDeletedStreamDuringMicroBatch() throws Exception {
        publishMessages(sourceStream, 100);
        Thread.sleep(2000);

        Path outputDir = Files.createTempDirectory("spark-output-deleted-");

        // Delete stream after a delay while query is processing
        CompletableFuture.runAsync(() -> {
            try {
                Thread.sleep(3000);
                deleteStream(sourceStream);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });

        // Start query with rate limiting to give time for deletion
        StreamingQuery query = spark.readStream()
                .format("rabbitmq_streams")
                .option("endpoints", streamEndpoint())
                .option("stream", sourceStream)
                .option("startingOffsets", "earliest")
                .option("maxRecordsPerTrigger", "10")
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
                .hasMessageContaining("no longer exists");
    }

    // ---- IT-ALO-001/003: checkpoint resume with no offset gaps ----

    @Test
    void streamingResumeFromCheckpointNoGaps() throws Exception {
        // Phase 1: publish and read 100 messages
        publishMessages(sourceStream, 100);
        Thread.sleep(2000);

        Path outputDir = Files.createTempDirectory("spark-output-alo-");

        StreamingQuery query1 = spark.readStream()
                .format("rabbitmq_streams")
                .option("endpoints", streamEndpoint())
                .option("stream", sourceStream)
                .option("startingOffsets", "earliest")
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

        query1.awaitTermination(120_000);

        Dataset<Row> phase1Result = spark.read().schema(MINIMAL_OUTPUT_SCHEMA)
                .parquet(outputDir.toString());
        assertThat(phase1Result.count()).isEqualTo(100);

        // Get the max offset from phase 1
        long phase1MaxOffset = phase1Result.collectAsList().stream()
                .mapToLong(row -> row.getAs("offset"))
                .max().orElse(-1);

        // Phase 2: publish 50 more and resume from checkpoint
        publishMessages(sourceStream, 50, "phase2-");
        Thread.sleep(2000);

        StreamingQuery query2 = spark.readStream()
                .format("rabbitmq_streams")
                .option("endpoints", streamEndpoint())
                .option("stream", sourceStream)
                .option("startingOffsets", "earliest")
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

        query2.awaitTermination(120_000);

        // Total output: 100 + 50 = 150
        Dataset<Row> allResult = spark.read().schema(MINIMAL_OUTPUT_SCHEMA)
                .parquet(outputDir.toString());
        assertThat(allResult.count()).isEqualTo(150);

        // Verify no duplicate offsets across both phases
        List<Long> allOffsets = allResult.collectAsList().stream()
                .map(row -> (Long) row.getAs("offset"))
                .sorted()
                .toList();

        Set<Long> uniqueOffsets = allOffsets.stream().collect(Collectors.toSet());
        assertThat(uniqueOffsets).hasSize(150);

        // Verify offsets are strictly increasing (no duplicates, no going backwards)
        for (int i = 1; i < allOffsets.size(); i++) {
            assertThat(allOffsets.get(i)).isGreaterThan(allOffsets.get(i - 1));
        }

        // Phase 2 started after phase 1 ended (no overlap)
        long phase2MinOffset = allOffsets.stream()
                .filter(o -> o > phase1MaxOffset)
                .mapToLong(Long::longValue).min().orElse(-1);
        assertThat(phase2MinOffset).isGreaterThan(phase1MaxOffset);
    }

    // ---- IT-ALO-004: broker-offset recovery with exact resume point ----

    @Test
    void streamingRecoveryFromBrokerOffsetsExactResume() throws Exception {
        String consumerName = "it-exact-recovery-" + System.currentTimeMillis();
        publishMessages(sourceStream, 60);
        Thread.sleep(2000);

        // Phase 1: Read all with server-side tracking
        Path outputDir1 = Files.createTempDirectory("spark-output-exact1-");

        StreamingQuery query1 = spark.readStream()
                .format("rabbitmq_streams")
                .option("endpoints", streamEndpoint())
                .option("stream", sourceStream)
                .option("startingOffsets", "earliest")
                .option("consumerName", consumerName)
                .option("serverSideOffsetTracking", "true")
                .option("metadataFields", "")
                .option("addressResolverClass",
                        "com.rabbitmq.spark.connector.TestAddressResolver")
                .load()
                .writeStream()
                .format("parquet")
                .option("path", outputDir1.toString())
                .option("checkpointLocation", checkpointDir.toString())
                .trigger(Trigger.AvailableNow())
                .start();

        query1.awaitTermination(120_000);

        long phase1Count = spark.read().schema(MINIMAL_OUTPUT_SCHEMA)
                .parquet(outputDir1.toString()).count();
        assertThat(phase1Count).isEqualTo(60);

        // Get the last offset from phase 1
        long phase1MaxOffset = spark.read().schema(MINIMAL_OUTPUT_SCHEMA)
                .parquet(outputDir1.toString())
                .collectAsList().stream()
                .mapToLong(row -> row.getAs("offset"))
                .max().orElse(-1);

        // Broker stored offset should equal last processed offset exactly.
        long storedOffset = queryStoredOffset(consumerName, sourceStream);
        assertThat(storedOffset).isEqualTo(phase1MaxOffset);

        // Phase 2: Publish more, recover from broker offset (new checkpoint)
        publishMessages(sourceStream, 40, "phase2-");
        Thread.sleep(2000);

        Path newCheckpointDir = Files.createTempDirectory("spark-checkpoint-exact-");
        Path outputDir2 = Files.createTempDirectory("spark-output-exact2-");

        StreamingQuery query2 = spark.readStream()
                .format("rabbitmq_streams")
                .option("endpoints", streamEndpoint())
                .option("stream", sourceStream)
                .option("startingOffsets", "earliest")
                .option("consumerName", consumerName)
                .option("serverSideOffsetTracking", "true")
                .option("metadataFields", "")
                .option("addressResolverClass",
                        "com.rabbitmq.spark.connector.TestAddressResolver")
                .load()
                .writeStream()
                .format("parquet")
                .option("path", outputDir2.toString())
                .option("checkpointLocation", newCheckpointDir.toString())
                .trigger(Trigger.AvailableNow())
                .start();

        query2.awaitTermination(120_000);

        // Should have exactly 40 new messages
        Dataset<Row> phase2Result = spark.read().schema(MINIMAL_OUTPUT_SCHEMA)
                .parquet(outputDir2.toString());
        assertThat(phase2Result.count()).isEqualTo(40);

        // Phase 2 should start after the stored offset (no overlap with phase 1)
        long phase2MinOffset = phase2Result.collectAsList().stream()
                .mapToLong(row -> row.getAs("offset"))
                .min().orElse(-1);
        assertThat(phase2MinOffset).isGreaterThan(storedOffset);
    }

    // ---- IT-SPLIT-001: minPartitions split in streaming ----

    @Test
    void streamingMinPartitionsSplit() throws Exception {
        publishMessages(sourceStream, 200);
        Thread.sleep(2000);

        Path outputDir = Files.createTempDirectory("spark-output-split-");

        StreamingQuery query = spark.readStream()
                .format("rabbitmq_streams")
                .option("endpoints", streamEndpoint())
                .option("stream", sourceStream)
                .option("startingOffsets", "earliest")
                .option("minPartitions", "4")
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

        Dataset<Row> result = spark.read().schema(MINIMAL_OUTPUT_SCHEMA)
                .parquet(outputDir.toString());
        assertThat(result.count()).isEqualTo(200);

        // Verify no gaps or duplicates
        Set<Long> offsets = result.collectAsList().stream()
                .map(row -> (Long) row.getAs("offset"))
                .collect(Collectors.toSet());
        assertThat(offsets).hasSize(200);

        long minOffset = offsets.stream().mapToLong(Long::longValue).min().orElse(-1);
        long maxOffset = offsets.stream().mapToLong(Long::longValue).max().orElse(-1);
        Set<Long> expected = LongStream.rangeClosed(minOffset, maxOffset)
                .boxed().collect(Collectors.toSet());
        assertThat(offsets).isEqualTo(expected);
    }

    // ---- Helpers ----

    /**
     * Query the stored offset for a consumer on a stream.
     * Returns the stored offset or throws if none exists.
     */
    private long queryStoredOffset(String consumerName, String stream) {
        IllegalStateException lastStateError = null;
        for (int i = 0; i < 20; i++) {
            var consumer = testEnv.consumerBuilder()
                    .stream(stream)
                    .name(consumerName)
                    .manualTrackingStrategy()
                    .builder()
                    .offset(com.rabbitmq.stream.OffsetSpecification.next())
                    .messageHandler((ctx, msg) -> {})
                    .build();
            try {
                return consumer.storedOffset();
            } catch (IllegalStateException e) {
                lastStateError = e;
                if (e.getMessage() != null && e.getMessage().contains("for now, consumer state is")) {
                    try {
                        Thread.sleep(50);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        throw e;
                    }
                    continue;
                }
                throw e;
            } finally {
                consumer.close();
            }
        }
        throw lastStateError != null ? lastStateError :
                new IllegalStateException("Failed to query stored offset for consumer '" +
                        consumerName + "' on stream '" + stream + "'");
    }

    /**
     * Check if a stored offset exists for a consumer on a stream.
     */
    private boolean hasStoredOffset(String consumerName, String stream) {
        try {
            queryStoredOffset(consumerName, stream);
            return true;
        } catch (NoOffsetException e) {
            return false;
        }
    }

    private long readOutputCount(Path outputDir) {
        if (!hasParquetData(outputDir)) {
            return 0L;
        }
        return spark.read().schema(MINIMAL_OUTPUT_SCHEMA).parquet(outputDir.toString()).count();
    }

    private List<Row> readOutputRows(Path outputDir) {
        if (!hasParquetData(outputDir)) {
            return List.of();
        }
        return spark.read().schema(MINIMAL_OUTPUT_SCHEMA)
                .parquet(outputDir.toString())
                .collectAsList();
    }

    private boolean hasParquetData(Path outputDir) {
        try (var paths = Files.walk(outputDir)) {
            return paths.anyMatch(path ->
                    Files.isRegularFile(path) && path.getFileName().toString().endsWith(".parquet"));
        } catch (IOException e) {
            throw new RuntimeException("Failed to inspect output directory " + outputDir, e);
        }
    }
}
