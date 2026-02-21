package io.github.lukaszsamson.spark.rabbitmq;

import com.rabbitmq.stream.Message;
import org.apache.spark.SparkException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Integration tests for batch writes via the {@code rabbitmq_streams} DataSource.
 */
class BatchWriteIT extends AbstractRabbitMQIT {

    private String stream;

    @BeforeEach
    void setUp() {
        stream = uniqueStreamName();
        createStream(stream);
    }

    @AfterEach
    void tearDown() {
        deleteStream(stream);
    }

    @Test
    void batchWriteAndReadBack() {
        // Create a DataFrame with value column
        StructType schema = new StructType()
                .add("value", DataTypes.BinaryType, false);

        List<Row> data = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
            data.add(RowFactory.create(("write-test-" + i).getBytes()));
        }

        Dataset<Row> df = spark.createDataFrame(data, schema);

        // Write via connector
        df.write()
                .format("rabbitmq_streams")
                .mode("append")
                .option("endpoints", streamEndpoint())
                .option("stream", stream)
                .option("addressResolverClass",
                        "io.github.lukaszsamson.spark.rabbitmq.TestAddressResolver")
                .save();

        // Read back from broker and verify
        List<Message> messages = consumeMessages(stream, 20);
        assertThat(messages).hasSize(20);

        List<String> bodies = messages.stream()
                .map(msg -> new String(msg.getBodyAsBinary()))
                .sorted()
                .toList();

        List<String> expected = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
            expected.add("write-test-" + i);
        }
        assertThat(bodies).containsExactlyInAnyOrderElementsOf(expected);
    }

    // ---- IT-SINK-002: publisherConfirmTimeoutMs timeout ----

    @Test
    void batchWritePublisherConfirmTimeoutFailsFast() {
        StructType schema = new StructType()
                .add("value", DataTypes.BinaryType, false);

        List<Row> data = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            data.add(RowFactory.create(("timeout-" + i).getBytes()));
        }

        Dataset<Row> df = spark.createDataFrame(data, schema);

        stopRabbitMqApp();

        try {
            assertThatThrownBy(() -> df.write()
                    .format("rabbitmq_streams")
                    .mode("append")
                    .option("endpoints", streamEndpoint())
                    .option("stream", stream)
                    .option("publisherConfirmTimeoutMs", "1500")
                    .option("enqueueTimeoutMs", "500")
                    .option("maxInFlight", "1")
                    .option("addressResolverClass",
                            "io.github.lukaszsamson.spark.rabbitmq.TestAddressResolver")
                    .save())
                    .satisfies(ex -> assertThat(ex.toString())
                            .containsAnyOf("Timed out waiting for publisher confirms",
                                    "Error when establishing stream connection",
                                    "Locator not available",
                                    "Connection is closed"));
        } finally {
            startRabbitMqApp();
        }
    }

    // ---- IT-RETRY-002: transient broker disconnect during write ----

    @Test
    void batchWriteRecoversAfterBrokerRestart() throws Exception {
        StructType schema = new StructType()
                .add("value", DataTypes.BinaryType, false);

        List<Row> data = new ArrayList<>();
        for (int i = 0; i < 30; i++) {
            data.add(RowFactory.create(("retry-" + i).getBytes()));
        }

        Dataset<Row> df = spark.createDataFrame(data, schema);

        java.util.concurrent.ExecutorService executor = java.util.concurrent.Executors.newSingleThreadExecutor();
        java.util.concurrent.Future<?> stopper = executor.submit(() -> {
            Thread.sleep(200);
            stopRabbitMqApp();
            Thread.sleep(800);
            startRabbitMqApp();
            return null;
        });

        try {
            df.write()
                    .format("rabbitmq_streams")
                    .mode("append")
                    .option("endpoints", streamEndpoint())
                    .option("stream", stream)
                    .option("retryOnRecovery", "true")
                    .option("publisherConfirmTimeoutMs", "5000")
                    .option("enqueueTimeoutMs", "500")
                    .option("maxInFlight", "5")
                    .option("addressResolverClass",
                            "io.github.lukaszsamson.spark.rabbitmq.TestAddressResolver")
                    .save();
        } finally {
            stopper.get(5, java.util.concurrent.TimeUnit.SECONDS);
            executor.shutdownNow();
        }

        List<Message> messages = consumeMessages(stream, 30);
        assertThat(messages).hasSize(30);
    }

    @Test
    void batchWriteWithApplicationProperties() {
        // Create DataFrame with value and application_properties
        StructType schema = new StructType()
                .add("value", DataTypes.BinaryType, false)
                .add("application_properties",
                        DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType),
                        true);

        List<Row> data = new ArrayList<>();
        data.add(RowFactory.create(
                "msg-with-props".getBytes(),
                java.util.Map.of("color", "blue", "priority", "high")));

        Dataset<Row> df = spark.createDataFrame(data, schema);

        df.write()
                .format("rabbitmq_streams")
                .mode("append")
                .option("endpoints", streamEndpoint())
                .option("stream", stream)
                .option("addressResolverClass",
                        "io.github.lukaszsamson.spark.rabbitmq.TestAddressResolver")
                .save();

        // Read back and verify application properties
        List<Message> messages = consumeMessages(stream, 1);
        assertThat(messages).hasSize(1);

        Message msg = messages.get(0);
        assertThat(new String(msg.getBodyAsBinary())).isEqualTo("msg-with-props");
        assertThat(msg.getApplicationProperties())
                .containsEntry("color", "blue")
                .containsEntry("priority", "high");
    }

    @Test
    void batchWriteFilterValueColumnRoundTripWithConsumerFilter() {
        StructType schema = new StructType()
                .add("value", DataTypes.BinaryType, false)
                .add("application_properties",
                        DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType),
                        true);

        List<Row> data = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
            data.add(RowFactory.create(("alpha-" + i).getBytes(), Map.of("region", "alpha")));
            data.add(RowFactory.create(("beta-" + i).getBytes(), Map.of("region", "beta")));
        }

        Dataset<Row> writeDf = spark.createDataFrame(data, schema);
        writeDf.write()
                .format("rabbitmq_streams")
                .mode("append")
                .option("endpoints", streamEndpoint())
                .option("stream", stream)
                .option("filterValuePath", "application_properties.region")
                .option("addressResolverClass",
                        "io.github.lukaszsamson.spark.rabbitmq.TestAddressResolver")
                .save();

        Dataset<Row> readDf = spark.read()
                .format("rabbitmq_streams")
                .option("endpoints", streamEndpoint())
                .option("stream", stream)
                .option("startingOffsets", "earliest")
                .option("filterValues", "alpha")
                .option("filterValuePath", "application_properties.region")
                .option("filterMatchUnfiltered", "false")
                .option("pollTimeoutMs", "500")
                .option("maxWaitMs", "10000")
                .option("metadataFields", "")
                .option("addressResolverClass",
                        "io.github.lukaszsamson.spark.rabbitmq.TestAddressResolver")
                .load();

        List<String> values = readDf.collectAsList().stream()
                .map(row -> new String((byte[]) row.getAs("value")))
                .sorted()
                .toList();

        assertThat(values).hasSize(20);
        assertThat(values).allMatch(v -> v.startsWith("alpha-"));
    }

    @Test
    void batchWriteFilterValueColumnRoundTripMatchUnfiltered() {
        StructType schema = new StructType()
                .add("value", DataTypes.BinaryType, false)
                .add("application_properties",
                        DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType),
                        true);

        List<Row> data = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            data.add(RowFactory.create(("alpha-" + i).getBytes(), Map.of("region", "alpha")));
            data.add(RowFactory.create(("unfiltered-" + i).getBytes(), Map.of("category", "misc")));
        }

        Dataset<Row> writeDf = spark.createDataFrame(data, schema);
        writeDf.write()
                .format("rabbitmq_streams")
                .mode("append")
                .option("endpoints", streamEndpoint())
                .option("stream", stream)
                .option("filterValuePath", "application_properties.region")
                .option("addressResolverClass",
                        "io.github.lukaszsamson.spark.rabbitmq.TestAddressResolver")
                .save();

        Dataset<Row> readDf = spark.read()
                .format("rabbitmq_streams")
                .option("endpoints", streamEndpoint())
                .option("stream", stream)
                .option("startingOffsets", "earliest")
                .option("filterValues", "alpha")
                .option("filterValuePath", "application_properties.region")
                .option("filterMatchUnfiltered", "true")
                .option("pollTimeoutMs", "500")
                .option("maxWaitMs", "10000")
                .option("metadataFields", "")
                .option("addressResolverClass",
                        "io.github.lukaszsamson.spark.rabbitmq.TestAddressResolver")
                .load();

        List<String> values = readDf.collectAsList().stream()
                .map(row -> new String((byte[]) row.getAs("value")))
                .toList();

        long alphaCount = values.stream().filter(v -> v.startsWith("alpha-")).count();
        long unfilteredCount = values.stream().filter(v -> v.startsWith("unfiltered-")).count();

        assertThat(alphaCount).isEqualTo(10);
        assertThat(unfilteredCount).isEqualTo(10);
    }

    @Test
    void batchWriteFilterValueColumnFromPropertiesSubjectRoundTrip() {
        StructType propsSchema = new StructType()
                .add("subject", DataTypes.StringType, true);
        StructType schema = new StructType()
                .add("value", DataTypes.BinaryType, false)
                .add("properties", propsSchema, true);

        List<Row> data = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
            data.add(RowFactory.create(("alpha-" + i).getBytes(), RowFactory.create("alpha")));
            data.add(RowFactory.create(("beta-" + i).getBytes(), RowFactory.create("beta")));
        }

        spark.createDataFrame(data, schema)
                .coalesce(1)
                .write()
                .format("rabbitmq_streams")
                .mode("append")
                .option("endpoints", streamEndpoint())
                .option("stream", stream)
                .option("filterValuePath", "properties.subject")
                .option("addressResolverClass",
                        "io.github.lukaszsamson.spark.rabbitmq.TestAddressResolver")
                .save();

        Dataset<Row> readDf = spark.read()
                .format("rabbitmq_streams")
                .option("endpoints", streamEndpoint())
                .option("stream", stream)
                .option("startingOffsets", "earliest")
                .option("filterValues", "alpha")
                .option("filterMatchUnfiltered", "false")
                .option("filterPostFilterClass",
                        "io.github.lukaszsamson.spark.rabbitmq.TestAlphaPrefixPostFilter")
                .option("pollTimeoutMs", "500")
                .option("maxWaitMs", "10000")
                .option("metadataFields", "")
                .option("addressResolverClass",
                        "io.github.lukaszsamson.spark.rabbitmq.TestAddressResolver")
                .load();

        List<String> values = readDf.collectAsList().stream()
                .map(row -> new String((byte[]) row.getAs("value")))
                .sorted()
                .toList();

        assertThat(values).hasSize(20);
        assertThat(values).allMatch(v -> v.startsWith("alpha-"));
    }

    @Test
    void batchWriteObservationCollectorClassInvoked() {
        StructType schema = new StructType()
                .add("value", DataTypes.BinaryType, false);

        List<Row> data = new ArrayList<>();
        for (int i = 0; i < 12; i++) {
            data.add(RowFactory.create(("obs-write-" + i).getBytes()));
        }

        TestObservationCollectorFactory.reset();

        spark.createDataFrame(data, schema)
                .write()
                .format("rabbitmq_streams")
                .mode("append")
                .option("endpoints", streamEndpoint())
                .option("stream", stream)
                .option("observationCollectorClass",
                        "io.github.lukaszsamson.spark.rabbitmq.TestObservationCollectorFactory")
                .option("addressResolverClass",
                        "io.github.lukaszsamson.spark.rabbitmq.TestAddressResolver")
                .save();

        assertThat(TestObservationCollectorFactory.prePublishCount()).isEqualTo(12);
        assertThat(TestObservationCollectorFactory.publishedCount()).isEqualTo(12);
    }

    @Test
    void batchWriteThenBatchRead() {
        // Write via connector
        StructType schema = new StructType()
                .add("value", DataTypes.BinaryType, false);

        List<Row> data = new ArrayList<>();
        for (int i = 0; i < 30; i++) {
            data.add(RowFactory.create(("roundtrip-" + i).getBytes()));
        }

        spark.createDataFrame(data, schema)
                .write()
                .format("rabbitmq_streams")
                .mode("append")
                .option("endpoints", streamEndpoint())
                .option("stream", stream)
                .option("addressResolverClass",
                        "io.github.lukaszsamson.spark.rabbitmq.TestAddressResolver")
                .save();

        // Verify messages arrived via broker client
        var messages = consumeMessages(stream, 30);
        assertThat(messages).hasSize(30);

        List<String> values = messages.stream()
                .map(msg -> new String(msg.getBodyAsBinary()))
                .sorted()
                .toList();

        List<String> expected = new ArrayList<>();
        for (int i = 0; i < 30; i++) {
            expected.add("roundtrip-" + i);
        }
        assertThat(values).containsExactlyInAnyOrderElementsOf(expected);
    }

    @Test
    void batchWriteComplexMetadataRoundTrip() {
        StructType propsSchema = new StructType()
                .add("message_id", DataTypes.StringType, true)
                .add("subject", DataTypes.StringType, true)
                .add("correlation_id", DataTypes.StringType, true)
                .add("content_type", DataTypes.StringType, true)
                .add("creation_time", DataTypes.TimestampType, true);
        StructType schema = new StructType()
                .add("value", DataTypes.BinaryType, false)
                .add("properties", propsSchema, true)
                .add("application_properties",
                        DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType), true)
                .add("message_annotations",
                        DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType), true)
                .add("creation_time", DataTypes.TimestampType, true);

        java.sql.Timestamp createdAt = new java.sql.Timestamp(System.currentTimeMillis());
        Row props = RowFactory.create(
                "msg-1",
                "subject-1",
                "corr-1",
                "text/plain",
                createdAt);

        Map<String, String> appProps = Map.of("region", "emea", "priority", "high");
        Map<String, String> annotations = Map.of("traceId", "abc-123", "span", "root");

        List<Row> data = List.of(RowFactory.create(
                "complex-0".getBytes(),
                props,
                appProps,
                annotations,
                createdAt));

        spark.createDataFrame(data, schema)
                .write()
                .format("rabbitmq_streams")
                .mode("append")
                .option("endpoints", streamEndpoint())
                .option("stream", stream)
                .option("addressResolverClass",
                        "io.github.lukaszsamson.spark.rabbitmq.TestAddressResolver")
                .save();

        Dataset<Row> df = spark.read()
                .format("rabbitmq_streams")
                .option("endpoints", streamEndpoint())
                .option("stream", stream)
                .option("startingOffsets", "earliest")
                .option("addressResolverClass",
                        "io.github.lukaszsamson.spark.rabbitmq.TestAddressResolver")
                .load();

        List<Row> rows = df.collectAsList();
        assertThat(rows).hasSize(1);

        Row row = rows.get(0);
        assertThat(new String((byte[]) row.getAs("value"))).isEqualTo("complex-0");
        scala.collection.Map<String, String> appPropsOut = row.getAs("application_properties");
        assertThat(scala.collection.JavaConverters.mapAsJavaMapConverter(appPropsOut).asJava())
                .containsEntry("region", "emea")
                .containsEntry("priority", "high");

        scala.collection.Map<String, String> annotationsOut = row.getAs("message_annotations");
        assertThat(scala.collection.JavaConverters.mapAsJavaMapConverter(annotationsOut).asJava())
                .containsEntry("traceId", "abc-123")
                .containsEntry("span", "root");

        Row propsOut = row.getAs("properties");
        assertThat(propsOut).isNotNull();
        assertThat(propsOut.getAs("message_id").toString()).isEqualTo("msg-1");
        assertThat(propsOut.getAs("subject").toString()).isEqualTo("subject-1");
        assertThat(propsOut.getAs("correlation_id").toString()).isEqualTo("corr-1");
        assertThat(propsOut.getAs("content_type").toString()).isEqualTo("text/plain");
        assertThat(((java.sql.Timestamp) propsOut.getAs("creation_time"))).isEqualTo(createdAt);

        assertThat(((java.sql.Timestamp) row.getAs("creation_time"))).isEqualTo(createdAt);
    }

    @Test
    void batchWriteNullMetadataRoundTrip() {
        StructType propsSchema = new StructType()
                .add("message_id", DataTypes.StringType, true)
                .add("subject", DataTypes.StringType, true)
                .add("correlation_id", DataTypes.StringType, true)
                .add("content_type", DataTypes.StringType, true)
                .add("creation_time", DataTypes.TimestampType, true);
        StructType schema = new StructType()
                .add("value", DataTypes.BinaryType, false)
                .add("properties", propsSchema, true)
                .add("application_properties",
                        DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType), true)
                .add("message_annotations",
                        DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType), true)
                .add("creation_time", DataTypes.TimestampType, true);

        List<Row> data = List.of(RowFactory.create(
                "null-meta".getBytes(), null, null, null, null));

        spark.createDataFrame(data, schema)
                .write()
                .format("rabbitmq_streams")
                .mode("append")
                .option("endpoints", streamEndpoint())
                .option("stream", stream)
                .option("addressResolverClass",
                        "io.github.lukaszsamson.spark.rabbitmq.TestAddressResolver")
                .save();

        Dataset<Row> df = spark.read()
                .format("rabbitmq_streams")
                .option("endpoints", streamEndpoint())
                .option("stream", stream)
                .option("startingOffsets", "earliest")
                .option("addressResolverClass",
                        "io.github.lukaszsamson.spark.rabbitmq.TestAddressResolver")
                .load();

        Row row = df.collectAsList().get(0);
        assertThat(new String((byte[]) row.getAs("value"))).isEqualTo("null-meta");
        assertThat((Object) row.getAs("properties")).isNull();
        assertThat((Object) row.getAs("application_properties")).isNull();
        assertThat((Object) row.getAs("message_annotations")).isNull();
    }

    @Test
    void batchWriteEmptyBodyRoundTrip() {
        StructType schema = new StructType()
                .add("value", DataTypes.BinaryType, false);
        Dataset<Row> df = spark.createDataFrame(
                List.of(RowFactory.create(new byte[0])),
                schema);

        df.write()
                .format("rabbitmq_streams")
                .mode("append")
                .option("endpoints", streamEndpoint())
                .option("stream", stream)
                .option("addressResolverClass",
                        "io.github.lukaszsamson.spark.rabbitmq.TestAddressResolver")
                .save();

        List<Message> messages = consumeMessages(stream, 1);
        assertThat(messages).hasSize(1);
        assertThat(messages.get(0).getBodyAsBinary()).hasSize(0);
    }

    // ---- IT-SINK-001: write to non-existent stream fails ----

    @Test
    void batchWriteToNonExistentStreamFails() {
        StructType schema = new StructType()
                .add("value", DataTypes.BinaryType, false);

        List<Row> data = List.of(RowFactory.create("test".getBytes()));
        Dataset<Row> df = spark.createDataFrame(data, schema);

        assertThatThrownBy(() ->
                df.write()
                        .format("rabbitmq_streams")
                        .mode("append")
                        .option("endpoints", streamEndpoint())
                        .option("stream", "non-existent-stream-" + System.currentTimeMillis())
                        .option("addressResolverClass",
                                "io.github.lukaszsamson.spark.rabbitmq.TestAddressResolver")
                        .save()
        ).isInstanceOf(SparkException.class)
                .satisfies(ex -> assertThat(ex.toString())
                        .containsAnyOf("does not exist", "STREAM_NOT_AVAILABLE"));
    }

    @Test
    void batchWriteWithWrongPasswordFailsFast() {
        StructType schema = new StructType()
                .add("value", DataTypes.BinaryType, false);

        List<Row> data = List.of(RowFactory.create("bad-pass".getBytes()));
        Dataset<Row> df = spark.createDataFrame(data, schema);

        assertThatThrownBy(() ->
                df.write()
                        .format("rabbitmq_streams")
                        .mode("append")
                        .option("endpoints", streamEndpoint())
                        .option("stream", stream)
                        .option("username", "guest")
                        .option("password", "wrong-password")
                        .option("addressResolverClass",
                                "io.github.lukaszsamson.spark.rabbitmq.TestAddressResolver")
                        .save()
        ).satisfies(ex -> assertThat(ex.toString())
                .containsIgnoringCase("authentication"));
    }

    // ---- IT-SINK-003: low maxInFlight doesn't deadlock ----

    @Test
    void batchWriteLowMaxInFlight() {
        StructType schema = new StructType()
                .add("value", DataTypes.BinaryType, false);

        List<Row> data = new ArrayList<>();
        for (int i = 0; i < 500; i++) {
            data.add(RowFactory.create(("inflight-" + i).getBytes()));
        }

        Dataset<Row> df = spark.createDataFrame(data, schema);

        df.write()
                .format("rabbitmq_streams")
                .mode("append")
                .option("endpoints", streamEndpoint())
                .option("stream", stream)
                .option("maxInFlight", "5")
                .option("addressResolverClass",
                        "io.github.lukaszsamson.spark.rabbitmq.TestAddressResolver")
                .save();

        // Verify all 500 messages arrived
        List<Message> messages = consumeMessages(stream, 500);
        assertThat(messages).hasSize(500);
    }

    @Test
    void batchWriteLargePayloadLowMaxInFlight() {
        StructType schema = new StructType()
                .add("value", DataTypes.BinaryType, false);

        List<Row> data = new ArrayList<>();
        for (int i = 0; i < 120; i++) {
            byte[] payload = new byte[15 * 1024];
            Arrays.fill(payload, (byte) ('a' + (i % 26)));
            data.add(RowFactory.create(payload));
        }

        Dataset<Row> df = spark.createDataFrame(data, schema);

        df.write()
                .format("rabbitmq_streams")
                .mode("append")
                .option("endpoints", streamEndpoint())
                .option("stream", stream)
                .option("maxInFlight", "1")
                .option("addressResolverClass",
                        "io.github.lukaszsamson.spark.rabbitmq.TestAddressResolver")
                .save();

        List<Message> messages = consumeMessages(stream, 120);
        assertThat(messages).hasSize(120);
        assertThat(messages.stream().mapToInt(m -> m.getBodyAsBinary().length))
                .allMatch(size -> size == 15 * 1024);
    }

    // ---- IT-SINK-004: batch settings (dynamicBatch, batchPublishingDelayMs, enqueueTimeoutMs) ----

    @Test
    void batchWriteWithDynamicBatchSettings() {
        StructType schema = new StructType()
                .add("value", DataTypes.BinaryType, false);

        List<Row> data = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            data.add(RowFactory.create(("batch-" + i).getBytes()));
        }

        Dataset<Row> df = spark.createDataFrame(data, schema);

        df.write()
                .format("rabbitmq_streams")
                .mode("append")
                .option("endpoints", streamEndpoint())
                .option("stream", stream)
                .option("dynamicBatch", "true")
                .option("batchPublishingDelayMs", "0")
                .option("enqueueTimeoutMs", "10000")
                .option("addressResolverClass",
                        "io.github.lukaszsamson.spark.rabbitmq.TestAddressResolver")
                .save();

        List<Message> messages = consumeMessages(stream, 100);
        assertThat(messages).hasSize(100);
    }

    // ---- IT-SINK-005: sub-entry compression round-trip ----

    @Test
    void batchWriteCompressionSubEntryRoundTrip() {
        StructType schema = new StructType()
                .add("value", DataTypes.BinaryType, false);

        List<Row> data = new ArrayList<>();
        for (int i = 0; i < 50; i++) {
            data.add(RowFactory.create(("compressed-" + i).getBytes()));
        }

        Dataset<Row> df = spark.createDataFrame(data, schema);

        // Write with sub-entry batching and gzip compression
        df.write()
                .format("rabbitmq_streams")
                .mode("append")
                .option("endpoints", streamEndpoint())
                .option("stream", stream)
                .option("subEntrySize", "10")
                .option("compression", "gzip")
                .option("addressResolverClass",
                        "io.github.lukaszsamson.spark.rabbitmq.TestAddressResolver")
                .save();

        // Read back via connector to verify content integrity
        Dataset<Row> readDf = spark.read()
                .format("rabbitmq_streams")
                .option("endpoints", streamEndpoint())
                .option("stream", stream)
                .option("startingOffsets", "earliest")
                .option("metadataFields", "")
                .option("addressResolverClass",
                        "io.github.lukaszsamson.spark.rabbitmq.TestAddressResolver")
                .load();

        List<Row> rows = readDf.collectAsList();
        assertThat(rows).hasSize(50);

        List<String> values = rows.stream()
                .map(row -> new String((byte[]) row.getAs("value")))
                .sorted()
                .toList();
        List<String> expected = new ArrayList<>();
        for (int i = 0; i < 50; i++) {
            expected.add("compressed-" + i);
        }
        assertThat(values).containsExactlyInAnyOrderElementsOf(expected);
    }

    // ---- IT-SINK-006: producerName deduplication ----

    @Test
    void batchWriteWithProducerNameDedup() {
        StructType schema = new StructType()
                .add("value", DataTypes.BinaryType, false);

        // First write with producerName
        List<Row> data1 = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
            data1.add(RowFactory.create(("first-" + i).getBytes()));
        }
        spark.createDataFrame(data1, schema)
                .write()
                .format("rabbitmq_streams")
                .mode("append")
                .option("endpoints", streamEndpoint())
                .option("stream", stream)
                .option("producerName", "dedup-test")
                .option("addressResolverClass",
                        "io.github.lukaszsamson.spark.rabbitmq.TestAddressResolver")
                .save();

        // Second write with same producerName
        List<Row> data2 = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
            data2.add(RowFactory.create(("second-" + i).getBytes()));
        }
        spark.createDataFrame(data2, schema)
                .write()
                .format("rabbitmq_streams")
                .mode("append")
                .option("endpoints", streamEndpoint())
                .option("stream", stream)
                .option("producerName", "dedup-test")
                .option("addressResolverClass",
                        "io.github.lukaszsamson.spark.rabbitmq.TestAddressResolver")
                .save();

        // Both writes should succeed — publishing IDs increment across invocations
        List<Message> messages = consumeMessages(stream, 40);
        assertThat(messages).hasSize(40);

        List<String> values = messages.stream()
                .map(msg -> new String(msg.getBodyAsBinary()))
                .sorted()
                .toList();
        List<String> expected = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
            expected.add("first-" + i);
            expected.add("second-" + i);
        }
        assertThat(values).containsExactlyInAnyOrderElementsOf(expected);
    }

    // ---- P1: dedup IT coverage for retries/restarts ----

    @Test
    void batchWriteDedupAfterPartialFailureSuppressesDuplicates() throws Exception {
        final int expectedCount = 6;
        StructType schema = new StructType()
                .add("value", DataTypes.BinaryType, false);

        List<Row> data = new ArrayList<>();
        for (int i = 0; i < expectedCount; i++) {
            data.add(RowFactory.create(("dedup-" + i).getBytes()));
        }

        Dataset<Row> df = spark.createDataFrame(data, schema);

        stopRabbitMqApp();
        try {
            assertThatThrownBy(() -> df.write()
                    .format("rabbitmq_streams")
                    .mode("append")
                    .option("endpoints", streamEndpoint())
                    .option("stream", stream)
                    .option("producerName", "dedup-retry")
                    .option("publisherConfirmTimeoutMs", "800")
                    .option("enqueueTimeoutMs", "500")
                    .option("addressResolverClass",
                            "io.github.lukaszsamson.spark.rabbitmq.TestAddressResolver")
                    .save())
                    .satisfies(ex -> assertThat(ex.toString())
                            .containsAnyOf("Timed out waiting for publisher confirms",
                                    "Error when establishing stream connection",
                                    "Locator not available",
                                    "Connection is closed"));
        } finally {
            startRabbitMqApp();
        }

        df.write()
                .format("rabbitmq_streams")
                .mode("append")
                .option("endpoints", streamEndpoint())
                .option("stream", stream)
                .option("producerName", "dedup-retry")
                .option("addressResolverClass",
                        "io.github.lukaszsamson.spark.rabbitmq.TestAddressResolver")
                .save();

        List<Message> messages = consumeMessages(stream, expectedCount);
        List<String> values = messages.stream()
                .map(msg -> new String(msg.getBodyAsBinary()))
                .toList();
        assertThat(values).hasSize(expectedCount);
        assertThat(values).containsExactlyInAnyOrder(
                "dedup-0", "dedup-1", "dedup-2", "dedup-3", "dedup-4", "dedup-5");
    }

    // ---- IT-SINK-008: ignoreUnknownColumns ----

    @Test
    void batchWriteIgnoreUnknownColumns() {
        // DataFrame with value + an extra unrecognized column
        StructType schema = new StructType()
                .add("value", DataTypes.BinaryType, false)
                .add("extra_column", DataTypes.StringType, true);

        List<Row> data = List.of(
                RowFactory.create("msg-with-extra".getBytes(), "extra-val"));
        Dataset<Row> df = spark.createDataFrame(data, schema);

        // With ignoreUnknownColumns=false (default), should fail
        assertThatThrownBy(() ->
                df.write()
                        .format("rabbitmq_streams")
                        .mode("append")
                        .option("endpoints", streamEndpoint())
                        .option("stream", stream)
                        .option("ignoreUnknownColumns", "false")
                        .option("addressResolverClass",
                                "io.github.lukaszsamson.spark.rabbitmq.TestAddressResolver")
                        .save()
        ).hasMessageContaining("unrecognized columns");

        // With ignoreUnknownColumns=true, should succeed
        df.write()
                .format("rabbitmq_streams")
                .mode("append")
                .option("endpoints", streamEndpoint())
                .option("stream", stream)
                .option("ignoreUnknownColumns", "true")
                .option("addressResolverClass",
                        "io.github.lukaszsamson.spark.rabbitmq.TestAddressResolver")
                .save();

        // Verify message was written
        List<Message> messages = consumeMessages(stream, 1);
        assertThat(messages).hasSize(1);
        assertThat(new String(messages.get(0).getBodyAsBinary())).isEqualTo("msg-with-extra");
    }

    // ---- IT-OPT-006-sink: invalid sink option combinations ----

    @Test
    void batchWriteInvalidOptionCombinations() {
        StructType schema = new StructType()
                .add("value", DataTypes.BinaryType, false);
        List<Row> data = List.of(RowFactory.create("test".getBytes()));
        Dataset<Row> df = spark.createDataFrame(data, schema);

        // 1. compression=snappy without subEntrySize
        assertThatThrownBy(() ->
                df.write()
                        .format("rabbitmq_streams")
                        .mode("append")
                        .option("endpoints", streamEndpoint())
                        .option("stream", stream)
                        .option("compression", "snappy")
                        .option("addressResolverClass",
                                "io.github.lukaszsamson.spark.rabbitmq.TestAddressResolver")
                        .save()
        ).satisfies(ex -> {
            assertThat(ex.getMessage()).containsIgnoringCase("compression");
            assertThat(ex.getMessage()).containsIgnoringCase("subEntrySize");
        });

        // 2. routingStrategy=custom without partitionerClass
        assertThatThrownBy(() ->
                df.write()
                        .format("rabbitmq_streams")
                        .mode("append")
                        .option("endpoints", streamEndpoint())
                        .option("stream", stream)
                        .option("routingStrategy", "custom")
                        .option("addressResolverClass",
                                "io.github.lukaszsamson.spark.rabbitmq.TestAddressResolver")
                        .save()
        ).satisfies(ex -> {
            assertThat(ex.getMessage()).containsIgnoringCase("partitionerClass");
            assertThat(ex.getMessage()).containsIgnoringCase("required");
        });
    }

    // ---- IT-SINK-009: sink schema type validation ----

    @Test
    void batchWriteSchemaTypeValidationFails() {
        StructType schema = new StructType()
                .add("value", DataTypes.StringType, false)
                .add("stream", DataTypes.IntegerType, true)
                .add("routing_key", DataTypes.IntegerType, true);

        List<Row> data = List.of(RowFactory.create("bad-value", 1, 2));
        Dataset<Row> df = spark.createDataFrame(data, schema);

        assertThatThrownBy(() -> df.write()
                .format("rabbitmq_streams")
                .mode("append")
                .option("endpoints", streamEndpoint())
                .option("stream", stream)
                .option("addressResolverClass",
                        "io.github.lukaszsamson.spark.rabbitmq.TestAddressResolver")
                .save())
                .hasMessageContaining("value")
                .hasMessageContaining("binary");
    }

    @Test
    void batchWriteSchemaTypeValidationFailsForMetadataColumns() {
        StructType wrongAppPropsSchema = new StructType()
                .add("value", DataTypes.BinaryType, false)
                .add("application_properties",
                        DataTypes.createMapType(DataTypes.StringType, DataTypes.IntegerType), true);
        Dataset<Row> wrongAppPropsDf = spark.createDataFrame(
                List.of(RowFactory.create("bad-map".getBytes(), Map.of("k", 1))),
                wrongAppPropsSchema);

        assertThatThrownBy(() -> wrongAppPropsDf.write()
                .format("rabbitmq_streams")
                .mode("append")
                .option("endpoints", streamEndpoint())
                .option("stream", stream)
                .option("addressResolverClass",
                        "io.github.lukaszsamson.spark.rabbitmq.TestAddressResolver")
                .save())
                .satisfies(ex -> assertThat(ex.getMessage())
                        .containsIgnoringCase("application_properties"));

        StructType wrongPropsSchema = new StructType()
                .add("value", DataTypes.BinaryType, false)
                .add("properties", new StructType()
                        .add("message_id", DataTypes.IntegerType, true), true);
        Dataset<Row> wrongPropsDf = spark.createDataFrame(
                List.of(RowFactory.create("bad-props".getBytes(), RowFactory.create(1))),
                wrongPropsSchema);

        assertThatThrownBy(() -> wrongPropsDf.write()
                .format("rabbitmq_streams")
                .mode("append")
                .option("endpoints", streamEndpoint())
                .option("stream", stream)
                .option("addressResolverClass",
                        "io.github.lukaszsamson.spark.rabbitmq.TestAddressResolver")
                .save())
                .satisfies(ex -> assertThat(ex.getMessage())
                        .containsIgnoringCase("properties"));
    }

    // ---- IT-SINK-010: unsupported save modes ----

    @Test
    void batchWriteUnsupportedSaveModesFail() {
        StructType schema = new StructType()
                .add("value", DataTypes.BinaryType, false);
        List<Row> data = List.of(RowFactory.create("mode-test".getBytes()));
        Dataset<Row> df = spark.createDataFrame(data, schema);

        assertThatThrownBy(() -> df.write()
                .format("rabbitmq_streams")
                .mode("overwrite")
                .option("endpoints", streamEndpoint())
                .option("stream", stream)
                .option("addressResolverClass",
                        "io.github.lukaszsamson.spark.rabbitmq.TestAddressResolver")
                .save())
                .satisfies(ex -> assertThat(ex.getMessage())
                        .containsIgnoringCase("overwrite"));

        assertThatThrownBy(() -> df.write()
                .format("rabbitmq_streams")
                .mode("ignore")
                .option("endpoints", streamEndpoint())
                .option("stream", stream)
                .option("addressResolverClass",
                        "io.github.lukaszsamson.spark.rabbitmq.TestAddressResolver")
                .save())
                .satisfies(ex -> assertThat(ex.getMessage())
                        .containsIgnoringCase("ignore"));
    }

    // ---- IT-METRIC-002: sink custom metrics reported ----

    @Test
    void batchWriteReportsSinkCustomMetrics() {
        StructType schema = new StructType()
                .add("value", DataTypes.BinaryType, false);

        List<Row> data = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            data.add(RowFactory.create(("metric-write-" + i).getBytes()));
        }

        Dataset<Row> df = spark.createDataFrame(data, schema);
        df.write()
                .format("rabbitmq_streams")
                .mode("append")
                .option("endpoints", streamEndpoint())
                .option("stream", stream)
                .option("addressResolverClass",
                        "io.github.lukaszsamson.spark.rabbitmq.TestAddressResolver")
                .save();

        scala.collection.Iterator<org.apache.spark.sql.execution.metric.SQLMetric> iterator =
                df.queryExecution().executedPlan().metrics().valuesIterator();
        List<String> metrics = new ArrayList<>();
        while (iterator.hasNext()) {
            scala.Option<String> name = iterator.next().name();
            if (name != null && name.isDefined()) {
                metrics.add(name.get());
            }
        }

        assertThat(metrics)
                .contains("number of output rows");
    }

    @Test
    void batchWriteToNonExistentSuperStreamFails() {
        StructType schema = new StructType()
                .add("value", DataTypes.BinaryType, false)
                .add("routing_key", DataTypes.StringType, true);

        List<Row> data = List.of(RowFactory.create("ss-miss".getBytes(), "rk"));
        Dataset<Row> df = spark.createDataFrame(data, schema);

        assertThatThrownBy(() ->
                df.write()
                        .format("rabbitmq_streams")
                        .mode("append")
                        .option("endpoints", streamEndpoint())
                        .option("superstream", "missing-superstream-" + System.currentTimeMillis())
                        .option("routingStrategy", "hash")
                        .option("addressResolverClass",
                                "io.github.lukaszsamson.spark.rabbitmq.TestAddressResolver")
                        .save()
        ).satisfies(ex -> {
            assertThat(ex).isInstanceOfAny(SparkException.class, IllegalArgumentException.class);

            boolean foundMissingPartitionCause = false;
            Throwable cursor = ex;
            while (cursor != null) {
                String message = cursor.getMessage();
                if (message != null) {
                    String normalized = message.toLowerCase(Locale.ROOT);
                    if (normalized.contains("super stream")
                            && normalized.contains("partition")) {
                        foundMissingPartitionCause = true;
                        break;
                    }
                }
                cursor = cursor.getCause();
            }

            assertThat(foundMissingPartitionCause)
                    .as("exception chain should mention missing super stream partitions")
                    .isTrue();
        });
    }

    // ---- IT-STRESS-003: large message round-trip ----

    @Test
    void batchWriteAndReadLargeMessage() {
        // Create a 500KB payload with markers
        byte[] largePayload = new byte[500 * 1024];
        Arrays.fill(largePayload, (byte) 'X');
        byte[] startMarker = "START-MARKER".getBytes();
        byte[] endMarker = "END-MARKER".getBytes();
        System.arraycopy(startMarker, 0, largePayload, 0, startMarker.length);
        System.arraycopy(endMarker, 0, largePayload,
                largePayload.length - endMarker.length, endMarker.length);

        StructType schema = new StructType()
                .add("value", DataTypes.BinaryType, false);
        List<Row> data = List.of(RowFactory.create(largePayload));
        Dataset<Row> df = spark.createDataFrame(data, schema);

        // Write via connector
        df.write()
                .format("rabbitmq_streams")
                .mode("append")
                .option("endpoints", streamEndpoint())
                .option("stream", stream)
                .option("addressResolverClass",
                        "io.github.lukaszsamson.spark.rabbitmq.TestAddressResolver")
                .save();

        // Read back via connector
        Dataset<Row> readDf = spark.read()
                .format("rabbitmq_streams")
                .option("endpoints", streamEndpoint())
                .option("stream", stream)
                .option("startingOffsets", "earliest")
                .option("metadataFields", "")
                .option("addressResolverClass",
                        "io.github.lukaszsamson.spark.rabbitmq.TestAddressResolver")
                .load();

        List<Row> rows = readDf.collectAsList();
        assertThat(rows).hasSize(1);

        byte[] readBack = rows.get(0).getAs("value");
        assertThat(readBack).isEqualTo(largePayload);
    }

    // ---- IT-OPT-006-sink-nostream: no stream or superstream fails ----

    @Test
    void batchWriteNoStreamOrSuperStreamFails() {
        StructType schema = new StructType()
                .add("value", DataTypes.BinaryType, false);
        List<Row> data = List.of(RowFactory.create("test".getBytes()));
        Dataset<Row> df = spark.createDataFrame(data, schema);

        assertThatThrownBy(() ->
                df.write()
                        .format("rabbitmq_streams")
                        .mode("append")
                        .option("endpoints", streamEndpoint())
                        .option("addressResolverClass",
                                "io.github.lukaszsamson.spark.rabbitmq.TestAddressResolver")
                        .save()
        ).hasMessageContaining("neither");
    }
}
