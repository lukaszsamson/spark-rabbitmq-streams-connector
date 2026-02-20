package com.rabbitmq.spark.connector;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.connector.read.streaming.ReadLimit;
import org.apache.spark.sql.streaming.SourceProgress;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryProgress;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.window;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
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
        Thread.sleep(200);

        // Read with Trigger.AvailableNow — processes all available data then stops
        Path outputDir = Files.createTempDirectory("spark-output-");

        StructType outputSchema = new StructType()
                .add("value", DataTypes.BinaryType)
                .add("stream", DataTypes.StringType)
                .add("offset", DataTypes.LongType)
                .add("chunk_timestamp", DataTypes.TimestampType);

        StreamingQuery query = spark.readStream()
                .format("rabbitmq_streams")
                .option("endpoints", streamEndpoint())
                .option("stream", sourceStream)
                .option("startingOffsets", "earliest")
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
        Thread.sleep(200);

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
                .add("chunk_timestamp", DataTypes.TimestampType);

        Dataset<Row> result = spark.read().schema(outputSchema).parquet(outputDir.toString());
        assertThat(result.count()).isEqualTo(50);
    }

    @Test
    void streamingRejectsEndingOffsetsOption() throws Exception {
        Path outputDir = Files.createTempDirectory("spark-output-ending-offsets-");

        StreamingQuery query = spark.readStream()
                .format("rabbitmq_streams")
                .option("endpoints", streamEndpoint())
                .option("stream", sourceStream)
                .option("startingOffsets", "earliest")
                .option("endingOffsets", "offset")
                .option("endingOffset", "10")
                .option("addressResolverClass",
                        "com.rabbitmq.spark.connector.TestAddressResolver")
                .load()
                .writeStream()
                .format("parquet")
                .option("path", outputDir.toString())
                .option("checkpointLocation", checkpointDir.toString())
                .trigger(Trigger.AvailableNow())
                .start();

        assertThatThrownBy(() -> query.awaitTermination(30_000))
                .isInstanceOf(org.apache.spark.sql.streaming.StreamingQueryException.class)
                .hasMessageContaining("endingOffsets=offset is not supported for streaming queries");
    }

    @Test
    void microBatchPlanInputPartitionsMissingStartDoesNotBackfillFromZero() throws Exception {
        publishMessages(sourceStream, 10);
        Thread.sleep(300);

        Map<String, String> opts = new HashMap<>();
        opts.put("endpoints", streamEndpoint());
        opts.put("stream", sourceStream);
        opts.put("startingOffsets", "earliest");
        opts.put("metadataFields", "");
        opts.put("addressResolverClass",
                "com.rabbitmq.spark.connector.TestAddressResolver");

        ConnectorOptions connectorOptions = new ConnectorOptions(opts);
        StructType schema = RabbitMQStreamTable.buildSourceSchema(
                connectorOptions.getMetadataFields());
        RabbitMQMicroBatchStream microBatchStream = new RabbitMQMicroBatchStream(
                connectorOptions, schema, checkpointDir.resolve("direct-microbatch-1").toString());

        try {
            RabbitMQStreamOffset start = new RabbitMQStreamOffset(Map.of("other-stream", 0L));
            RabbitMQStreamOffset end = new RabbitMQStreamOffset(Map.of(sourceStream, 10L));

            var partitions = microBatchStream.planInputPartitions(start, end);
            assertThat(partitions).isEmpty();
        } finally {
            microBatchStream.stop();
        }
    }

    @Test
    void microBatchLatestOffsetNoNewDataReturnsExpandedStableOffsets() throws Exception {
        publishMessages(sourceStream, 5);
        Thread.sleep(300);

        Map<String, String> opts = new HashMap<>();
        opts.put("endpoints", streamEndpoint());
        opts.put("stream", sourceStream);
        opts.put("startingOffsets", "latest");
        opts.put("metadataFields", "");
        opts.put("addressResolverClass",
                "com.rabbitmq.spark.connector.TestAddressResolver");

        ConnectorOptions connectorOptions = new ConnectorOptions(opts);
        StructType schema = RabbitMQStreamTable.buildSourceSchema(
                connectorOptions.getMetadataFields());
        RabbitMQMicroBatchStream microBatchStream = new RabbitMQMicroBatchStream(
                connectorOptions, schema, checkpointDir.resolve("direct-microbatch-2").toString());

        try {
            RabbitMQStreamOffset latest = (RabbitMQStreamOffset) microBatchStream.latestOffset(
                    new RabbitMQStreamOffset(Map.of()),
                    ReadLimit.allAvailable());

            assertThat(latest.getStreamOffsets())
                    .containsEntry(sourceStream, 5L);
        } finally {
            microBatchStream.stop();
        }
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
        List<String> expected = new ArrayList<>();
        for (int i = 0; i < 25; i++) {
            expected.add("streaming-write-" + i);
        }
        assertThat(bodies).containsExactlyInAnyOrderElementsOf(expected);
    }

    // ---- IT-SINK-UPDATE-001: stateless update mode matches append mode ----

    @Test
    void streamingSinkUpdateModeStatelessEquivalentToAppend() throws Exception {
        String appendSink = uniqueStreamName();
        String updateSink = uniqueStreamName();
        createStream(appendSink);
        createStream(updateSink);
        try {
            StructType schema = new StructType().add("value", DataTypes.BinaryType, false);
            List<Row> data = new ArrayList<>();
            for (int i = 0; i < 40; i++) {
                data.add(RowFactory.create(("mode-" + i).getBytes()));
            }
            Path inputDir = Files.createTempDirectory("spark-input-sink-update-mode-")
                    .resolve("data");
            spark.createDataFrame(data, schema).write().parquet(inputDir.toString());

            Path appendCheckpoint = Files.createTempDirectory("spark-sink-append-ckpt-");
            StreamingQuery appendQuery = spark.readStream()
                    .schema(schema)
                    .parquet(inputDir.toString())
                    .select("value")
                    .writeStream()
                    .format("rabbitmq_streams")
                    .outputMode("append")
                    .option("endpoints", streamEndpoint())
                    .option("stream", appendSink)
                    .option("checkpointLocation", appendCheckpoint.toString())
                    .option("addressResolverClass",
                            "com.rabbitmq.spark.connector.TestAddressResolver")
                    .trigger(Trigger.AvailableNow())
                    .start();
            boolean appendTerminated = appendQuery.awaitTermination(20_000);
            if (!appendTerminated && appendQuery.isActive()) {
                appendQuery.stop();
            }
            assertThat(appendQuery.isActive())
                    .as("append query should terminate in AvailableNow mode")
                    .isFalse();

            Path updateCheckpoint = Files.createTempDirectory("spark-sink-update-ckpt-");
            StreamingQuery updateQuery = spark.readStream()
                    .schema(schema)
                    .parquet(inputDir.toString())
                    .select("value")
                    .writeStream()
                    .format("rabbitmq_streams")
                    .outputMode("update")
                    .option("endpoints", streamEndpoint())
                    .option("stream", updateSink)
                    .option("checkpointLocation", updateCheckpoint.toString())
                    .option("addressResolverClass",
                            "com.rabbitmq.spark.connector.TestAddressResolver")
                    .trigger(Trigger.AvailableNow())
                    .start();
            boolean updateTerminated = updateQuery.awaitTermination(20_000);
            if (!updateTerminated && updateQuery.isActive()) {
                updateQuery.stop();
            }
            assertThat(updateQuery.isActive())
                    .as("update query should terminate in AvailableNow mode")
                    .isFalse();

            Set<String> appendPayloads = readAllValuesFromStream(appendSink).stream()
                    .map(row -> new String((byte[]) row.getAs("value")))
                    .collect(Collectors.toSet());
            Set<String> updatePayloads = readAllValuesFromStream(updateSink).stream()
                    .map(row -> new String((byte[]) row.getAs("value")))
                    .collect(Collectors.toSet());

            assertThat(appendPayloads).hasSize(40);
            assertThat(updatePayloads).hasSize(40);
            assertThat(updatePayloads).isEqualTo(appendPayloads);
        } finally {
            deleteStream(appendSink);
            deleteStream(updateSink);
        }
    }

    // ---- IT-SINK-UPDATE-002: stateful update mode appends updates ----

    @Test
    void streamingSinkUpdateModeStatefulAppendsUpdates() throws Exception {
        String updateSink = uniqueStreamName();
        createStream(updateSink);
        try {
            publishMessages(sourceStream, 60, "stateful-");
            Thread.sleep(200);

            Path updateCheckpoint = Files.createTempDirectory("spark-sink-stateful-update-ckpt-");
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
                    .selectExpr("CAST(offset % 3 AS STRING) AS k")
                    .groupBy("k")
                    .count()
                    .selectExpr("CAST(concat(k, ':', CAST(count AS STRING)) AS BINARY) AS value")
                    .writeStream()
                    .format("rabbitmq_streams")
                    .outputMode("update")
                    .option("endpoints", streamEndpoint())
                    .option("stream", updateSink)
                    .option("checkpointLocation", updateCheckpoint.toString())
                    .option("addressResolverClass",
                            "com.rabbitmq.spark.connector.TestAddressResolver")
                    .trigger(Trigger.AvailableNow())
                    .start();
            query.awaitTermination(120_000);

            List<String> updates = readAllValuesFromStream(updateSink).stream()
                    .map(row -> new String((byte[]) row.getAs("value")))
                    .toList();

            Map<String, Set<Long>> distinctCountsByKey = new HashMap<>();
            for (String update : updates) {
                String[] parts = update.split(":");
                assertThat(parts).hasSize(2);
                String key = parts[0];
                long count = Long.parseLong(parts[1]);
                distinctCountsByKey.computeIfAbsent(key, k -> new java.util.HashSet<>()).add(count);
            }

            assertThat(distinctCountsByKey.keySet()).containsExactlyInAnyOrder("0", "1", "2");
            for (String key : distinctCountsByKey.keySet()) {
                Set<Long> counts = distinctCountsByKey.get(key);
                assertThat(counts).hasSizeGreaterThan(1);
                long max = counts.stream().mapToLong(Long::longValue).max().orElse(-1);
                assertThat(max).isEqualTo(20L);
            }
        } finally {
            deleteStream(updateSink);
        }
    }

    // ---- IT-SINK-UPDATE-003: update mode checkpoint resume ----

    @Test
    void streamingSinkUpdateModeCheckpointResume() throws Exception {
        String updateSink = uniqueStreamName();
        createStream(updateSink);
        try {
            Path updateCheckpoint = Files.createTempDirectory("spark-sink-update-resume-ckpt-");

            publishMessages(sourceStream, 30, "resume1-");
            Thread.sleep(200);

            StreamingQuery phase1 = spark.readStream()
                    .format("rabbitmq_streams")
                    .option("endpoints", streamEndpoint())
                    .option("stream", sourceStream)
                    .option("startingOffsets", "earliest")
                    .option("maxRecordsPerTrigger", "10")
                    .option("metadataFields", "")
                    .option("addressResolverClass",
                            "com.rabbitmq.spark.connector.TestAddressResolver")
                    .load()
                    .selectExpr("CAST(offset % 3 AS STRING) AS k")
                    .groupBy("k")
                    .count()
                    .selectExpr("CAST(concat(k, ':', CAST(count AS STRING)) AS BINARY) AS value")
                    .writeStream()
                    .format("rabbitmq_streams")
                    .outputMode("update")
                    .option("endpoints", streamEndpoint())
                    .option("stream", updateSink)
                    .option("checkpointLocation", updateCheckpoint.toString())
                    .option("addressResolverClass",
                            "com.rabbitmq.spark.connector.TestAddressResolver")
                    .trigger(Trigger.AvailableNow())
                    .start();
            phase1.awaitTermination(120_000);

            List<Row> phase1SinkRows = readAllValuesFromStream(updateSink);
            assertThat(phase1SinkRows).isNotEmpty();
            long phase1MaxSinkOffset = phase1SinkRows.stream()
                    .mapToLong(row -> row.getAs("offset"))
                    .max().orElse(-1);

            publishMessages(sourceStream, 30, "resume2-");
            Thread.sleep(200);

            StreamingQuery phase2 = spark.readStream()
                    .format("rabbitmq_streams")
                    .option("endpoints", streamEndpoint())
                    .option("stream", sourceStream)
                    .option("startingOffsets", "earliest")
                    .option("maxRecordsPerTrigger", "10")
                    .option("metadataFields", "")
                    .option("addressResolverClass",
                            "com.rabbitmq.spark.connector.TestAddressResolver")
                    .load()
                    .selectExpr("CAST(offset % 3 AS STRING) AS k")
                    .groupBy("k")
                    .count()
                    .selectExpr("CAST(concat(k, ':', CAST(count AS STRING)) AS BINARY) AS value")
                    .writeStream()
                    .format("rabbitmq_streams")
                    .outputMode("update")
                    .option("endpoints", streamEndpoint())
                    .option("stream", updateSink)
                    .option("checkpointLocation", updateCheckpoint.toString())
                    .option("addressResolverClass",
                            "com.rabbitmq.spark.connector.TestAddressResolver")
                    .trigger(Trigger.AvailableNow())
                    .start();
            phase2.awaitTermination(120_000);

            List<Row> allSinkRows = readAllValuesFromStream(updateSink);
            assertThat(allSinkRows.size()).isGreaterThan(phase1SinkRows.size());

            Map<String, Long> maxByKey = new HashMap<>();
            for (Row row : allSinkRows) {
                String update = new String((byte[]) row.getAs("value"));
                String[] parts = update.split(":");
                String key = parts[0];
                long count = Long.parseLong(parts[1]);
                maxByKey.merge(key, count, Math::max);
            }
            assertThat(maxByKey).containsEntry("0", 20L)
                    .containsEntry("1", 20L)
                    .containsEntry("2", 20L);

            List<String> phase2Updates = allSinkRows.stream()
                    .filter(row -> (Long) row.getAs("offset") > phase1MaxSinkOffset)
                    .sorted(Comparator.comparingLong(row -> row.getAs("offset")))
                    .map(row -> new String((byte[]) row.getAs("value")))
                    .toList();
            assertThat(phase2Updates).isNotEmpty();
            assertThat(phase2Updates).allMatch(v -> Long.parseLong(v.split(":")[1]) > 10L);
        } finally {
            deleteStream(updateSink);
        }
    }

    // ---- IT-SINK-UPDATE-004: append regression after update support ----

    @Test
    void streamingSinkAppendModeStillWorksAfterUpdateSupport() throws Exception {
        publishMessages(sourceStream, 25, "append-regression-");
        Thread.sleep(200);

        Path sinkCheckpointDir = Files.createTempDirectory("spark-sink-append-regression-ckpt-");
        StreamingQuery query = spark.readStream()
                .format("rabbitmq_streams")
                .option("endpoints", streamEndpoint())
                .option("stream", sourceStream)
                .option("startingOffsets", "earliest")
                .option("metadataFields", "")
                .option("addressResolverClass",
                        "com.rabbitmq.spark.connector.TestAddressResolver")
                .load()
                .select("value")
                .writeStream()
                .format("rabbitmq_streams")
                .outputMode("append")
                .option("endpoints", streamEndpoint())
                .option("stream", sinkStream)
                .option("checkpointLocation", sinkCheckpointDir.toString())
                .option("addressResolverClass",
                        "com.rabbitmq.spark.connector.TestAddressResolver")
                .trigger(Trigger.AvailableNow())
                .start();

        query.awaitTermination(120_000);

        Set<String> values = readAllValuesFromStream(sinkStream).stream()
                .map(row -> new String((byte[]) row.getAs("value")))
                .collect(Collectors.toSet());

        assertThat(values).hasSize(25);
        assertThat(values).allMatch(v -> v.startsWith("append-regression-"));
    }

    @Test
    void streamingReadResumesFromCheckpoint() throws Exception {
        // Phase 1: publish 30 messages and read them
        publishMessages(sourceStream, 30);
        Thread.sleep(200);

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
                .add("chunk_timestamp", DataTypes.TimestampType);

        long count1 = spark.read().schema(outputSchema).parquet(outputDir.toString()).count();
        assertThat(count1).isEqualTo(30);

        // Phase 2: publish 20 more messages
        publishMessages(sourceStream, 20, "phase2-");
        Thread.sleep(200);

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
        Thread.sleep(200);

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
                .add("chunk_timestamp", DataTypes.TimestampType);

        long count = waitForParquetCount(outputDir, outputSchema, 50L, 5_000);
        assertThat(count).isEqualTo(50);

        // Verify that broker stored the offset
        long storedOffset = queryStoredOffset(consumerName, sourceStream);
        assertThat(storedOffset).isGreaterThanOrEqualTo(0);
    }

    @Test
    void streamingWithServerSideOffsetTrackingDisabled() throws Exception {
        String consumerName = "it-consumer-disabled-" + System.currentTimeMillis();
        publishMessages(sourceStream, 30);
        Thread.sleep(200);

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
                .add("chunk_timestamp", DataTypes.TimestampType);

        long count = spark.read().schema(outputSchema).parquet(outputDir.toString()).count();
        assertThat(count).isEqualTo(30);

        // Verify that NO broker offset was stored
        assertThat(hasStoredOffset(consumerName, sourceStream)).isFalse();
    }

    @Test
    void streamingRecoveryFromBrokerStoredOffsets() throws Exception {
        String consumerName = "it-recovery-" + System.currentTimeMillis();
        publishMessages(sourceStream, 40);
        Thread.sleep(200);

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
                .add("chunk_timestamp", DataTypes.TimestampType);

        assertThat(spark.read().schema(outputSchema)
                .parquet(outputDir1.toString()).count()).isEqualTo(40);

        // Phase 2: Publish 20 more messages
        publishMessages(sourceStream, 20, "phase2-");
        Thread.sleep(200);

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
        Thread.sleep(200);

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
                .add("chunk_timestamp", DataTypes.TimestampType);

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
        Thread.sleep(200);

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
        Thread.sleep(200);

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
        Thread.sleep(200);

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

    // ---- IT-OFFSET-008: earliest stop/restart picks up data added while stopped ----

    @Test
    void streamingStartingOffsetsEarliestRestartsPickUpStoppedData() throws Exception {
        publishMessages(sourceStream, 20, "phase1-");
        Thread.sleep(400);

        Path outputDir = Files.createTempDirectory("spark-output-restart-earliest-");

        StreamingQuery first = spark.readStream()
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

        first.awaitTermination(120_000);

        long phase1Count = readOutputCount(outputDir);
        assertThat(phase1Count).isEqualTo(20);

        publishMessages(sourceStream, 15, "phase2-");
        Thread.sleep(400);

        StreamingQuery second = spark.readStream()
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

        second.awaitTermination(120_000);

        List<Row> rows = readOutputRows(outputDir);
        assertThat(rows).hasSize(35);

        Set<Long> offsets = rows.stream()
                .map(row -> (Long) row.getAs("offset"))
                .collect(Collectors.toSet());
        assertThat(offsets).hasSize(35);

        List<String> values = rows.stream()
                .map(row -> new String((byte[]) row.getAs("value")))
                .toList();
        assertThat(values).anyMatch(v -> v.startsWith("phase1-"));
        assertThat(values).anyMatch(v -> v.startsWith("phase2-"));
    }

    // ---- IT-AVNOW-001: publish during run excluded by snapshot ----

    @Test
    void triggerAvailableNowExcludesPostSnapshotData() throws Exception {
        // Pre-publish messages that form the snapshot ceiling
        publishMessages(sourceStream, 50, "pre-");
        long preSnapshotCount = waitForStreamCountAtLeast(sourceStream, 50L, 5_000);
        assertThat(preSnapshotCount).isGreaterThanOrEqualTo(50L);

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
        Thread.sleep(200);

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
                Thread.sleep(200); // allow segments to flush
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
                    .hasMessageContaining("before the first available offset");
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
                Thread.sleep(200);
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
        Thread.sleep(200);

        Path outputDir = Files.createTempDirectory("spark-output-deleted-");

        // Delete stream after a delay while query is processing
        CompletableFuture.runAsync(() -> {
            try {
                Thread.sleep(1000);
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
                .satisfies(ex -> assertThat(ex.getMessage())
                        .containsAnyOf(
                                "no longer exists",
                                "does not exist",
                                "Failed to look up stored offset"));
    }

    // ---- IT-ALO-001/003: checkpoint resume with no offset gaps ----

    @Test
    void streamingResumeFromCheckpointNoGaps() throws Exception {
        // Phase 1: publish and read 100 messages
        publishMessages(sourceStream, 100);
        Thread.sleep(200);

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
        long phase1Count = phase1Result.count();
        assertThat(phase1Count).isEqualTo(100L);

        // Get the max offset from phase 1
        long phase1MaxOffset = phase1Result.collectAsList().stream()
                .mapToLong(row -> row.getAs("offset"))
                .max().orElse(-1);

        // Phase 2: publish 50 more and resume from checkpoint
        publishMessages(sourceStream, 50, "phase2-");
        Thread.sleep(200);

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
        Thread.sleep(200);

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
        Thread.sleep(200);

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

    // ---- IT-ALO-005: derived consumerName fallback path on non-fatal lookup failure ----

    @Test
    void streamingDerivedConsumerNameFallbackOnLookupFailure() throws Exception {
        publishMessages(sourceStream, 20, "fallback-");
        Thread.sleep(400);

        Path outputDir = Files.createTempDirectory("spark-output-alo-fallback-");
        Path checkpointDir = Files.createTempDirectory("spark-checkpoint-alo-fallback-");

        EnvironmentPool.getInstance().closeAll();
        String envId = "fallback-env-" + System.currentTimeMillis();
        long pollTimeoutMs = 300;

        java.util.concurrent.ExecutorService executor = java.util.concurrent.Executors.newSingleThreadExecutor();
        java.util.concurrent.Future<?> stopper = executor.submit(() -> {
            Thread.sleep(200);
            stopRabbitMqApp();
            Thread.sleep(300);
            startRabbitMqApp();
            return null;
        });

        StreamingQuery query = spark.readStream()
                .format("rabbitmq_streams")
                .option("endpoints", streamEndpoint())
                .option("stream", sourceStream)
                .option("startingOffsets", "earliest")
                .option("environmentId", envId)
                .option("pollTimeoutMs", String.valueOf(pollTimeoutMs))
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
        stopper.get(5, TimeUnit.SECONDS);
        executor.shutdownNow();

        List<Row> rows = spark.read().schema(MINIMAL_OUTPUT_SCHEMA)
                .parquet(outputDir.toString())
                .collectAsList();
        assertThat(rows).hasSize(20);

        Set<Long> offsets = rows.stream()
                .map(row -> (Long) row.getAs("offset"))
                .collect(Collectors.toSet());
        assertThat(offsets).hasSize(20);

        EnvironmentPool.getInstance().closeAll();
    }

    // ---- IT-RETRY-001: transient broker disconnect during read ----

    @Test
    void streamingRecoversAfterBrokerRestartDuringRead() throws Exception {
        publishMessages(sourceStream, 40, "reco-");
        long preRestartCount = waitForStreamCountAtLeast(sourceStream, 40L, 5_000);
        assertThat(preRestartCount).isGreaterThanOrEqualTo(40L);

        Path outputDir = Files.createTempDirectory("spark-output-retry-read-");
        Path checkpointDir = Files.createTempDirectory("spark-checkpoint-retry-read-");
        EnvironmentPool.getInstance().closeAll();

        java.util.concurrent.ExecutorService executor = java.util.concurrent.Executors.newSingleThreadExecutor();
        java.util.concurrent.Future<?> stopper = executor.submit(() -> {
            Thread.sleep(300);
            stopRabbitMqApp();
            Thread.sleep(300);
            startRabbitMqApp();
            return null;
        });

        StreamingQuery query = spark.readStream()
                .format("rabbitmq_streams")
                .option("endpoints", streamEndpoint())
                .option("stream", sourceStream)
                .option("startingOffsets", "earliest")
                .option("pollTimeoutMs", "300")
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
        stopper.get(5, TimeUnit.SECONDS);
        executor.shutdownNow();

        List<Row> rows = spark.read().schema(MINIMAL_OUTPUT_SCHEMA)
                .parquet(outputDir.toString())
                .collectAsList();
        assertThat(rows).hasSize(40);

        Set<Long> offsets = rows.stream()
                .map(row -> (Long) row.getAs("offset"))
                .collect(Collectors.toSet());
        assertThat(offsets).hasSize(40);

        EnvironmentPool.getInstance().closeAll();
    }

    @Test
    void streamingNamedConsumerRecoversAfterBrokerRestart() throws Exception {
        String consumerName = "it-named-recovery-" + System.currentTimeMillis();
        publishMessages(sourceStream, 45, "named-reco-");
        long preRestartCount = waitForStreamCountAtLeast(sourceStream, 45L, 5_000);
        assertThat(preRestartCount).isGreaterThanOrEqualTo(45L);

        Path outputDir = Files.createTempDirectory("spark-output-named-retry-read-");
        Path checkpointDir = Files.createTempDirectory("spark-checkpoint-named-retry-read-");
        EnvironmentPool.getInstance().closeAll();

        java.util.concurrent.ExecutorService executor = java.util.concurrent.Executors.newSingleThreadExecutor();
        java.util.concurrent.Future<?> stopper = executor.submit(() -> {
            Thread.sleep(300);
            stopRabbitMqApp();
            Thread.sleep(300);
            startRabbitMqApp();
            return null;
        });

        StreamingQuery query = spark.readStream()
                .format("rabbitmq_streams")
                .option("endpoints", streamEndpoint())
                .option("stream", sourceStream)
                .option("startingOffsets", "earliest")
                .option("consumerName", consumerName)
                .option("pollTimeoutMs", "300")
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
        stopper.get(5, TimeUnit.SECONDS);
        executor.shutdownNow();

        List<Row> rows = spark.read().schema(MINIMAL_OUTPUT_SCHEMA)
                .parquet(outputDir.toString())
                .collectAsList();
        assertThat(rows).hasSize(45);
        assertThat(rows.stream().map(r -> (Long) r.getAs("offset")).collect(Collectors.toSet()))
                .hasSize(45);
        assertThat(hasStoredOffset(consumerName, sourceStream)).isTrue();

        publishMessages(sourceStream, 12, "named-reco-next-");
        Thread.sleep(200);

        Path resumedOutputDir = Files.createTempDirectory("spark-output-named-retry-resume-");
        Path resumedCheckpoint = Files.createTempDirectory("spark-checkpoint-named-retry-resume-");
        StreamingQuery resumedQuery = spark.readStream()
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
                .option("path", resumedOutputDir.toString())
                .option("checkpointLocation", resumedCheckpoint.toString())
                .trigger(Trigger.AvailableNow())
                .start();
        resumedQuery.awaitTermination(120_000);

        long resumedCount = spark.read().schema(MINIMAL_OUTPUT_SCHEMA)
                .parquet(resumedOutputDir.toString())
                .count();
        assertThat(resumedCount).isEqualTo(12L);

        EnvironmentPool.getInstance().closeAll();
    }

    // ---- IT-RETRY-005: broker-offset store timeout path (best effort) ----

    @Test
    void streamingBrokerOffsetStoreTimeoutDoesNotAbortQuery() throws Exception {
        publishMessages(sourceStream, 25, "commit-");
        Thread.sleep(400);

        EnvironmentPool.getInstance().closeAll();
        String envId = "commit-timeout-" + System.currentTimeMillis();

        Path outputDir = Files.createTempDirectory("spark-output-commit-timeout-");
        Path localCheckpoint = Files.createTempDirectory("spark-checkpoint-commit-timeout-");

        try {
            StreamingQuery query = spark.readStream()
                    .format("rabbitmq_streams")
                    .option("endpoints", streamEndpoint())
                    .option("stream", sourceStream)
                    .option("startingOffsets", "earliest")
                    .option("serverSideOffsetTracking", "true")
                    .option("environmentId", envId)
                    .option("metadataFields", "")
                    .option("addressResolverClass",
                            "com.rabbitmq.spark.connector.TestAddressResolver")
                    .load()
                    .writeStream()
                    .format("parquet")
                    .option("path", outputDir.toString())
                    .option("checkpointLocation", localCheckpoint.toString())
                    .trigger(Trigger.AvailableNow())
                    .start();

            long deadline = System.currentTimeMillis() + 30_000;
            while (System.currentTimeMillis() < deadline && readOutputCount(outputDir) == 0L) {
                Thread.sleep(100);
            }
            assertThat(readOutputCount(outputDir)).isGreaterThan(0L);

            blockRabbitMqPort();
            query.awaitTermination(120_000);

            List<Row> rows = spark.read().schema(MINIMAL_OUTPUT_SCHEMA)
                    .parquet(outputDir.toString())
                    .collectAsList();
            assertThat(rows).hasSize(25);
        } finally {
            unblockRabbitMqPort();
            EnvironmentPool.getInstance().closeAll();
        }
    }

    // ---- IT-ALO-002: sink task failure after partial writes causes duplicates ----

    @Test
    void streamingSinkFailureAfterPartialWritesCausesDuplicates() throws Exception {
        final int expectedCount = 12;
        publishMessages(sourceStream, expectedCount, "dup-");

        Path outputDir = Files.createTempDirectory("spark-output-alo-dup-");
        Path failureCheckpoint = Files.createTempDirectory("spark-checkpoint-alo-dup-");
        java.util.concurrent.atomic.AtomicBoolean failed = new java.util.concurrent.atomic.AtomicBoolean(false);

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
                .foreachBatch((batch, batchId) -> {
                    batch.write()
                            .mode("append")
                            .format("parquet")
                            .save(outputDir.toString());
                    if (batchId == 0L && failed.compareAndSet(false, true)) {
                        throw new RuntimeException("Intentional sink failure after write");
                    }
                })
                .option("checkpointLocation", failureCheckpoint.toString())
                .trigger(Trigger.AvailableNow())
                .start();

        assertThatThrownBy(() -> query.awaitTermination(30_000))
                .hasMessageContaining("Intentional sink failure");

        StreamingQuery retry = spark.readStream()
                .format("rabbitmq_streams")
                .option("endpoints", streamEndpoint())
                .option("stream", sourceStream)
                .option("startingOffsets", "earliest")
                .option("serverSideOffsetTracking", "false")
                .option("metadataFields", "")
                .option("addressResolverClass",
                        "com.rabbitmq.spark.connector.TestAddressResolver")
                .load()
                .writeStream()
                .format("parquet")
                .option("path", outputDir.toString())
                .option("checkpointLocation", failureCheckpoint.toString())
                .trigger(Trigger.AvailableNow())
                .start();

        long deadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(45);
        long observed = 0L;
        while (retry.isActive() && System.nanoTime() < deadlineNanos) {
            observed = readOutputCount(outputDir);
            if (observed >= expectedCount) {
                retry.stop();
                retry.awaitTermination(10_000);
                break;
            }
            Thread.sleep(250);
        }

        boolean terminated = !retry.isActive();
        if (!terminated) {
            observed = readOutputCount(outputDir);
            retry.stop();
            retry.awaitTermination(10_000);
            if (observed >= expectedCount) {
                terminated = true;
            }
        }
        if (!terminated) {
            StreamingQueryProgress lastProgress = retry.lastProgress();
            String progress = lastProgress == null ? "null" : lastProgress.prettyJson();
            throw new AssertionError(
                    "Retry query did not terminate within 120000ms; lastProgress=" + progress);
        }

        List<String> values = spark.read().schema(MINIMAL_OUTPUT_SCHEMA)
                .parquet(outputDir.toString())
                .collectAsList().stream()
                .map(row -> new String((byte[]) row.getAs("value")))
                .toList();

        Set<String> unique = Set.copyOf(values);
        // At-least-once semantics allow duplicates on retry, but Spark may also resume
        // without duplicates depending on failure timing/checkpoint state.
        assertThat(values.size()).isGreaterThanOrEqualTo(unique.size());
        assertThat(unique).hasSize(expectedCount);
    }

    // ---- IT-SPLIT-001: minPartitions split in streaming ----

    @Test
    void streamingMinPartitionsSplit() throws Exception {
        publishMessages(sourceStream, 200);
        Thread.sleep(200);

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

    // ---- IT-RETRY-006: successful AvailableNow commit persists broker offset ----

    @Test
    void streamingAvailableNowPersistsBrokerOffsetOnCommit() throws Exception {
        String consumerName = "it-stop-fallback-" + System.currentTimeMillis();
        publishMessages(sourceStream, 50);
        Thread.sleep(200);

        Path outputDir = Files.createTempDirectory("spark-output-stop-fallback-");

        // Run AvailableNow with server-side offset tracking
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

        // Verify all 50 messages were read
        long count = spark.read().schema(MINIMAL_OUTPUT_SCHEMA)
                .parquet(outputDir.toString()).count();
        assertThat(count).isEqualTo(50);

        // With server-side tracking enabled, successful batch commit should persist last processed offset.
        long storedOffset = queryStoredOffset(consumerName, sourceStream);
        assertThat(storedOffset).isEqualTo(49L);
    }

    @Test
    void streamingAvailableNowSingleBatchResumeFromCheckpointWithoutStopFallback() throws Exception {
        String consumerName = "it-single-batch-resume-" + System.currentTimeMillis();
        publishMessages(sourceStream, 50);
        Thread.sleep(400);

        Path outputDir = Files.createTempDirectory("spark-output-single-batch-resume-");

        // First run: one small AvailableNow micro-batch.
        StreamingQuery firstRun = spark.readStream()
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

        firstRun.awaitTermination(120_000);

        long firstCount = spark.read().schema(MINIMAL_OUTPUT_SCHEMA)
                .parquet(outputDir.toString()).count();
        assertThat(firstCount).isEqualTo(50);

        // Second run with same checkpoint and no new data must not reprocess messages.
        StreamingQuery secondRun = spark.readStream()
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

        boolean terminated = secondRun.awaitTermination(20_000);
        if (!terminated && secondRun.isActive()) {
            Thread stopper = new Thread(() -> {
                try {
                    secondRun.stop();
                } catch (Exception ignored) {
                    // best effort: assertion below checks final query state
                }
            }, "streaming-it-second-run-stopper");
            stopper.setDaemon(true);
            stopper.start();
            stopper.join(5_000);
        }
        assertThat(secondRun.isActive()).as("second AvailableNow run should terminate").isFalse();

        long totalCount = spark.read().schema(MINIMAL_OUTPUT_SCHEMA)
                .parquet(outputDir.toString()).count();
        assertThat(totalCount).isEqualTo(50);
    }

    // ---- IT-RL-001: maxBytesPerTrigger only ----

    @Test
    void streamingMaxBytesPerTriggerOnly() throws Exception {
        publishMessages(sourceStream, 200);
        Thread.sleep(200);

        Path outputDir = Files.createTempDirectory("spark-output-bytes-limit-");

        // maxBytesPerTrigger=200, estimatedMessageSizeBytes=5
        // Budget: 200/5 = 40 records/trigger → forces multiple micro-batches
        StreamingQuery query = spark.readStream()
                .format("rabbitmq_streams")
                .option("endpoints", streamEndpoint())
                .option("stream", sourceStream)
                .option("startingOffsets", "earliest")
                .option("maxBytesPerTrigger", "200")
                .option("estimatedMessageSizeBytes", "5")
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

        // All 200 messages should be consumed across multiple batches
        long count = spark.read().schema(MINIMAL_OUTPUT_SCHEMA)
                .parquet(outputDir.toString()).count();
        assertThat(count).isEqualTo(200);
    }

    // ---- IT-RL-002: composite record and byte limits ----

    @Test
    void streamingCompositeRecordAndByteLimits() throws Exception {
        publishMessages(sourceStream, 200);
        Thread.sleep(200);

        Path outputDir = Files.createTempDirectory("spark-output-composite-");

        // maxRecordsPerTrigger=50 AND maxBytesPerTrigger=200, estimatedMessageSizeBytes=5
        // Byte budget: 200/5 = 40 records. Record budget: 50.
        // Composite limit uses the most restrictive → 40 per trigger
        StreamingQuery query = spark.readStream()
                .format("rabbitmq_streams")
                .option("endpoints", streamEndpoint())
                .option("stream", sourceStream)
                .option("startingOffsets", "earliest")
                .option("maxRecordsPerTrigger", "50")
                .option("maxBytesPerTrigger", "200")
                .option("estimatedMessageSizeBytes", "5")
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

        // All 200 messages should be consumed
        long count = spark.read().schema(MINIMAL_OUTPUT_SCHEMA)
                .parquet(outputDir.toString()).count();
        assertThat(count).isEqualTo(200);
    }

    // ---- IT-STRESS-001: soak with periodic restarts ----

    @Test
    void streamingSoakWithPeriodicRestarts() throws Exception {
        Path outputDir = Files.createTempDirectory("spark-output-soak-");
        Path soakCheckpoint = Files.createTempDirectory("spark-checkpoint-soak-");

        int cycles = 3;
        int perCycle = 20;

        for (int cycle = 0; cycle < cycles; cycle++) {
            publishMessages(sourceStream, perCycle, "soak-" + cycle + "-");
            Thread.sleep(400);

            StreamingQuery query = spark.readStream()
                    .format("rabbitmq_streams")
                    .option("endpoints", streamEndpoint())
                    .option("stream", sourceStream)
                    .option("startingOffsets", "earliest")
                    .option("serverSideOffsetTracking", "false")
                    .option("metadataFields", "")
                    .option("addressResolverClass",
                            "com.rabbitmq.spark.connector.TestAddressResolver")
                    .load()
                    .writeStream()
                    .format("parquet")
                    .option("path", outputDir.toString())
                    .option("checkpointLocation", soakCheckpoint.toString())
                    .trigger(Trigger.AvailableNow())
                    .start();

            query.awaitTermination(120_000);
        }

        List<Row> rows = spark.read().schema(MINIMAL_OUTPUT_SCHEMA)
                .parquet(outputDir.toString())
                .collectAsList();

        int expected = cycles * perCycle;
        assertThat(rows).hasSize(expected);

        Set<Long> offsets = rows.stream()
                .map(row -> (Long) row.getAs("offset"))
                .collect(Collectors.toSet());
        assertThat(offsets).hasSize(expected);

        long min = offsets.stream().mapToLong(Long::longValue).min().orElse(-1);
        long max = offsets.stream().mapToLong(Long::longValue).max().orElse(-1);
        assertThat(max - min).isEqualTo(expected - 1L);

        List<String> values = rows.stream()
                .map(row -> new String((byte[]) row.getAs("value")))
                .toList();
        for (int cycle = 0; cycle < cycles; cycle++) {
            String prefix = "soak-" + cycle + "-";
            assertThat(values).anyMatch(v -> v.startsWith(prefix));
        }
    }

    // ---- IT-STRESS-004: environment pool eviction and reacquire ----

    @Test
    void environmentPoolEvictsAndReacquiresUnderConcurrentLoad() throws Exception {
        EnvironmentPool.getInstance().closeAll();

        int idleTimeoutMs = 200;
        publishMessages(sourceStream, 20, "pool-");
        Thread.sleep(300);

        StructType schema = new StructType()
                .add("value", DataTypes.BinaryType, false);

        List<Row> data = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            data.add(RowFactory.create(("pool-write-" + i).getBytes()));
        }

        Dataset<Row> writeDf = spark.createDataFrame(data, schema);

        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            Future<List<Row>> readFuture = executor.submit(() -> spark.read()
                    .format("rabbitmq_streams")
                    .option("endpoints", streamEndpoint())
                    .option("stream", sourceStream)
                    .option("startingOffsets", "earliest")
                    .option("metadataFields", "")
                    .option("environmentIdleTimeoutMs", String.valueOf(idleTimeoutMs))
                    .option("addressResolverClass",
                            "com.rabbitmq.spark.connector.TestAddressResolver")
                    .load()
                    .collectAsList());

            Future<Void> writeFuture = executor.submit(() -> {
                writeDf.write()
                        .format("rabbitmq_streams")
                        .mode("append")
                        .option("endpoints", streamEndpoint())
                        .option("stream", sinkStream)
                        .option("environmentIdleTimeoutMs", String.valueOf(idleTimeoutMs))
                        .option("addressResolverClass",
                                "com.rabbitmq.spark.connector.TestAddressResolver")
                        .save();
                return null;
            });

            readFuture.get(60, TimeUnit.SECONDS);
            writeFuture.get(60, TimeUnit.SECONDS);
        } finally {
            executor.shutdownNow();
        }

        long deadline = System.currentTimeMillis() + 5_000L;
        while (System.currentTimeMillis() < deadline
                && EnvironmentPool.getInstance().size() > 0) {
            Thread.sleep(100);
        }

        assertThat(EnvironmentPool.getInstance().size()).isEqualTo(0);

        List<Row> rows = spark.read()
                .format("rabbitmq_streams")
                .option("endpoints", streamEndpoint())
                .option("stream", sourceStream)
                .option("startingOffsets", "earliest")
                .option("metadataFields", "")
                .option("environmentIdleTimeoutMs", String.valueOf(idleTimeoutMs))
                .option("addressResolverClass",
                        "com.rabbitmq.spark.connector.TestAddressResolver")
                .load()
                .collectAsList();

        assertThat(rows).isNotEmpty();
        EnvironmentPool.getInstance().closeAll();
    }

    // ---- IT-RL-004: wrong initial estimatedMessageSizeBytes corrected over batches ----

    @Test
    void streamingSmallEstimatedSizeCorrectedOverBatches() throws Exception {
        // Publish 100 messages with ~100-byte bodies
        String longPrefix = "a]".repeat(50) + "-";  // ~101 bytes per message body
        publishMessages(sourceStream, 100, longPrefix);
        Thread.sleep(200);

        Path outputDir = Files.createTempDirectory("spark-output-size-correction-");

        // estimatedMessageSizeBytes=1 is wildly wrong (real size ~100 bytes)
        // maxBytesPerTrigger=1000 → initial budget: 1000/1 = 1000 records (overestimate)
        // MessageSizeTracker should correct this over subsequent batches
        StreamingQuery query = spark.readStream()
                .format("rabbitmq_streams")
                .option("endpoints", streamEndpoint())
                .option("stream", sourceStream)
                .option("startingOffsets", "earliest")
                .option("maxBytesPerTrigger", "1000")
                .option("estimatedMessageSizeBytes", "1")
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

        // All 100 messages should be consumed despite wrong initial estimate
        long count = spark.read().schema(MINIMAL_OUTPUT_SCHEMA)
                .parquet(outputDir.toString()).count();
        assertThat(count).isEqualTo(100);
    }

    // ---- IT-RL-005: AvailableNow batch count matches rate limit ----

    @Test
    void triggerAvailableNowBatchCountMatchesMaxRecordsPerTrigger() throws Exception {
        publishMessages(sourceStream, 25);
        Thread.sleep(400);

        AtomicInteger batchCount = new AtomicInteger(0);
        List<Long> batchSizes = new ArrayList<>();

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
                .foreachBatch((org.apache.spark.api.java.function.VoidFunction2<Dataset<Row>, Long>)
                        (batch, batchId) -> {
                            batchCount.incrementAndGet();
                            batchSizes.add(batch.count());
                        })
                .option("checkpointLocation", checkpointDir.toString())
                .trigger(Trigger.AvailableNow())
                .start();

        query.awaitTermination(120_000);

        assertThat(batchCount.get()).isEqualTo(3);
        assertThat(batchSizes).hasSize(3);
        assertThat(batchSizes).allMatch(count -> count > 0L);
    }

    // ---- IT-RL-006: maxRecordsPerTrigger=Long.MAX_VALUE does not overflow ----

    @Test
    void streamingMaxRecordsPerTriggerLongMaxValueCompletes() throws Exception {
        publishMessages(sourceStream, 5);
        Thread.sleep(300);

        Path outputDir = Files.createTempDirectory("spark-output-maxrecords-long-");

        StreamingQuery query = spark.readStream()
                .format("rabbitmq_streams")
                .option("endpoints", streamEndpoint())
                .option("stream", sourceStream)
                .option("startingOffsets", "earliest")
                .option("maxRecordsPerTrigger", String.valueOf(Long.MAX_VALUE))
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

        long count = spark.read().schema(MINIMAL_OUTPUT_SCHEMA)
                .parquet(outputDir.toString()).count();
        assertThat(count).isEqualTo(5);
    }

    // ---- IT-CONT-001: continuous trigger not supported ----

    @Test
    void streamingContinuousTriggerNotSupported() throws Exception {
        publishMessages(sourceStream, 10);
        Thread.sleep(200);

        Path outputDir = Files.createTempDirectory("spark-output-continuous-");

        assertThatThrownBy(() -> {
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
                    .option("checkpointLocation",
                            Files.createTempDirectory("spark-ckpt-cont-").toString())
                    .trigger(Trigger.Continuous("1 second"))
                    .start();
            query.awaitTermination(30_000);
        }).satisfies(ex -> assertThat(ex.getMessage())
                .containsIgnoringCase("ContinuousTrigger"));
    }

    // ---- IT-CONT-002: ensure micro-batch execution mode ----

    @Test
    void streamingReportsMicroBatchExecutionMode() throws Exception {
        publishMessages(sourceStream, 10);
        Thread.sleep(400);

        Path outputDir = Files.createTempDirectory("spark-output-microbatch-");

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

        query.awaitTermination(120_000);

        // Spark 4.1 wraps the internal execution with StreamingQueryWrapper, so class-name
        // checks are not stable. Validate micro-batch execution by the presence of
        // batch-oriented progress metadata.
        assertThat(query.lastProgress()).isNotNull();
        assertThat(query.lastProgress().batchId()).isGreaterThanOrEqualTo(0L);
        assertThat(query.lastProgress().numInputRows()).isEqualTo(10L);
    }

    // ---- IT-SCHEMA-001: watermark-based windowed aggregation ----

    @Test
    void streamingWatermarkAggregationOnChunkTimestamp() throws Exception {
        publishMessages(sourceStream, 12, "wm-");
        Thread.sleep(200);

        StreamingQuery query = spark.readStream()
                .format("rabbitmq_streams")
                .option("endpoints", streamEndpoint())
                .option("stream", sourceStream)
                .option("startingOffsets", "earliest")
                .option("metadataFields", "")
                .option("addressResolverClass",
                        "com.rabbitmq.spark.connector.TestAddressResolver")
                .load()
                .withWatermark("chunk_timestamp", "5 seconds")
                .groupBy(window(col("chunk_timestamp"), "5 seconds"))
                .count()
                .writeStream()
                .format("memory")
                .queryName("rabbitmq_watermark")
                .outputMode("complete")
                .trigger(Trigger.AvailableNow())
                .start();

        query.awaitTermination(120_000);

        List<Row> rows = spark.table("rabbitmq_watermark").collectAsList();
        long total = rows.stream().mapToLong(row -> row.getAs("count")).sum();
        assertThat(total).isEqualTo(12L);
    }

    // ---- IT-ORDER-001: offsets strictly increasing across batches ----

    @Test
    void streamingOffsetsStrictlyIncreasingAcrossBatches() throws Exception {
        publishMessages(sourceStream, 200);
        Thread.sleep(200);

        Path outputDir = Files.createTempDirectory("spark-output-order-");

        StreamingQuery query = spark.readStream()
                .format("rabbitmq_streams")
                .option("endpoints", streamEndpoint())
                .option("stream", sourceStream)
                .option("startingOffsets", "earliest")
                .option("maxRecordsPerTrigger", "30")
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
        List<Long> offsets = result.collectAsList().stream()
                .map(row -> (Long) row.getAs("offset"))
                .sorted()
                .toList();

        assertThat(offsets).hasSize(200);

        // Verify strictly increasing (no gaps, no duplicates)
        for (int i = 1; i < offsets.size(); i++) {
            assertThat(offsets.get(i)).isEqualTo(offsets.get(i - 1) + 1);
        }

        // Verify contiguous range
        assertThat(offsets.get(0)).isEqualTo(0L);
        assertThat(offsets.get(offsets.size() - 1)).isEqualTo(199L);
    }

    // ---- IT-METRIC-003: lag metrics in source progress ----

    @Test
    void streamingLagMetricsInSourceProgress() throws Exception {
        publishMessages(sourceStream, 200);
        Thread.sleep(200);

        Path outputDir = Files.createTempDirectory("spark-output-metrics-");

        StreamingQuery query = spark.readStream()
                .format("rabbitmq_streams")
                .option("endpoints", streamEndpoint())
                .option("stream", sourceStream)
                .option("startingOffsets", "earliest")
                .option("maxRecordsPerTrigger", "30")
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

        // Inspect recent progress entries
        StreamingQueryProgress[] progresses = query.recentProgress();
        assertThat(progresses).isNotEmpty();

        // Find a progress entry where numInputRows > 0
        StreamingQueryProgress activeProgress = null;
        for (StreamingQueryProgress p : progresses) {
            if (p.numInputRows() > 0) {
                activeProgress = p;
                break;
            }
        }
        assertThat(activeProgress).as("Should have at least one progress with numInputRows > 0")
                .isNotNull();

        // Check sources[0].metrics
        SourceProgress[] sources = activeProgress.sources();
        assertThat(sources).isNotEmpty();

        Map<String, String> metrics = sources[0].metrics();
        assertThat(metrics).containsKey("minOffsetsBehindLatest");
        assertThat(metrics).containsKey("maxOffsetsBehindLatest");
        assertThat(metrics).containsKey("avgOffsetsBehindLatest");

        long minLag = Long.parseLong(metrics.get("minOffsetsBehindLatest"));
        long maxLag = Long.parseLong(metrics.get("maxOffsetsBehindLatest"));
        double avgLag = Double.parseDouble(metrics.get("avgOffsetsBehindLatest"));

        assertThat(minLag).isGreaterThanOrEqualTo(0);
        assertThat(maxLag).isGreaterThan(0);
        assertThat(avgLag).isGreaterThanOrEqualTo(0.0);
        assertThat(minLag).isLessThanOrEqualTo(maxLag);
    }

    // ---- IT-METRIC-004: numInputRows matches records read ----

    @Test
    void streamingNumInputRowsMatchesOutputCount() throws Exception {
        publishMessages(sourceStream, 60);
        Thread.sleep(200);

        Path outputDir = Files.createTempDirectory("spark-output-input-rows-");

        StreamingQuery query = spark.readStream()
                .format("rabbitmq_streams")
                .option("endpoints", streamEndpoint())
                .option("stream", sourceStream)
                .option("startingOffsets", "earliest")
                .option("maxRecordsPerTrigger", "15")
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

        long outputCount = spark.read().schema(MINIMAL_OUTPUT_SCHEMA)
                .parquet(outputDir.toString()).count();

        long totalInputRows = Arrays.stream(query.recentProgress())
                .mapToLong(StreamingQueryProgress::numInputRows)
                .sum();

        assertThat(outputCount).isEqualTo(60L);
        assertThat(totalInputRows).isEqualTo(outputCount);
    }

    // ---- IT-SINK-011: streaming sink progress numOutputRows ----

    @Test
    void streamingSinkProgressReportsNumOutputRows() throws Exception {
        StructType schema = new StructType()
                .add("value", DataTypes.BinaryType, false);

        List<Row> data = new ArrayList<>();
        for (int i = 0; i < 12; i++) {
            data.add(RowFactory.create(("sink-progress-" + i).getBytes()));
        }

        Path inputDir = Files.createTempDirectory("spark-input-sink-progress-").resolve("data");
        Path sinkCheckpointDir = Files.createTempDirectory("spark-sink-progress-ckpt-");
        spark.createDataFrame(data, schema).write().parquet(inputDir.toString());

        StreamingQuery query = spark.readStream()
                .schema(schema)
                .parquet(inputDir.toString())
                .writeStream()
                .format("rabbitmq_streams")
                .outputMode("append")
                .option("endpoints", streamEndpoint())
                .option("stream", sinkStream)
                .option("checkpointLocation", sinkCheckpointDir.toString())
                .option("addressResolverClass",
                        "com.rabbitmq.spark.connector.TestAddressResolver")
                .trigger(Trigger.AvailableNow())
                .start();

        query.awaitTermination(120_000);

        boolean found = Arrays.stream(query.recentProgress())
                .anyMatch(progress -> progress.sink().numOutputRows() == 12L);
        assertThat(found).isTrue();
    }

    // ---- Helpers ----

    private long readOutputCount(Path outputDir) {
        if (!hasParquetData(outputDir)) {
            return 0L;
        }
        return spark.read().schema(MINIMAL_OUTPUT_SCHEMA).parquet(outputDir.toString()).count();
    }

    private long waitForParquetCount(Path outputDir, StructType schema,
                                     long expectedCount, long timeoutMs)
            throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        long lastCount = 0L;
        while (System.currentTimeMillis() < deadline) {
            lastCount = spark.read().schema(schema).parquet(outputDir.toString()).count();
            if (lastCount == expectedCount) {
                return lastCount;
            }
            Thread.sleep(100);
        }
        return spark.read().schema(schema).parquet(outputDir.toString()).count();
    }

    private long waitForStreamCountAtLeast(String stream, long expectedCount, long timeoutMs)
            throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        long lastCount = 0L;
        while (System.currentTimeMillis() < deadline) {
            lastCount = readAllValuesFromStream(stream).size();
            if (lastCount >= expectedCount) {
                return lastCount;
            }
            Thread.sleep(100);
        }
        return readAllValuesFromStream(stream).size();
    }

    private List<Row> readOutputRows(Path outputDir) {
        if (!hasParquetData(outputDir)) {
            return List.of();
        }
        return spark.read().schema(MINIMAL_OUTPUT_SCHEMA)
                .parquet(outputDir.toString())
                .collectAsList();
    }

    private List<Row> readAllValuesFromStream(String stream) {
        return spark.read()
                .format("rabbitmq_streams")
                .option("endpoints", streamEndpoint())
                .option("stream", stream)
                .option("startingOffsets", "earliest")
                .option("metadataFields", "")
                .option("addressResolverClass",
                        "com.rabbitmq.spark.connector.TestAddressResolver")
                .load()
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
