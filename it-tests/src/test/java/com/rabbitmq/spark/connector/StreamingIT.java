package com.rabbitmq.spark.connector;

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

import com.rabbitmq.stream.NoOffsetException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

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
}
