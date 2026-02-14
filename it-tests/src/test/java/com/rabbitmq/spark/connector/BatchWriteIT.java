package com.rabbitmq.spark.connector;

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
                        "com.rabbitmq.spark.connector.TestAddressResolver")
                .save();

        // Read back from broker and verify
        List<Message> messages = consumeMessages(stream, 20);
        assertThat(messages).hasSize(20);

        List<String> bodies = messages.stream()
                .map(msg -> new String(msg.getBodyAsBinary()))
                .sorted()
                .toList();

        assertThat(bodies).contains("write-test-0", "write-test-1", "write-test-19");
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
                        "com.rabbitmq.spark.connector.TestAddressResolver")
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
                .option("filterValueColumn", "region")
                .option("addressResolverClass",
                        "com.rabbitmq.spark.connector.TestAddressResolver")
                .save();

        Dataset<Row> readDf = spark.read()
                .format("rabbitmq_streams")
                .option("endpoints", streamEndpoint())
                .option("stream", stream)
                .option("startingOffsets", "earliest")
                .option("filterValues", "alpha")
                .option("filterValueColumn", "region")
                .option("filterMatchUnfiltered", "false")
                .option("pollTimeoutMs", "500")
                .option("maxWaitMs", "10000")
                .option("metadataFields", "")
                .option("addressResolverClass",
                        "com.rabbitmq.spark.connector.TestAddressResolver")
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
                .option("filterValueColumn", "region")
                .option("addressResolverClass",
                        "com.rabbitmq.spark.connector.TestAddressResolver")
                .save();

        Dataset<Row> readDf = spark.read()
                .format("rabbitmq_streams")
                .option("endpoints", streamEndpoint())
                .option("stream", stream)
                .option("startingOffsets", "earliest")
                .option("filterValues", "alpha")
                .option("filterValueColumn", "region")
                .option("filterMatchUnfiltered", "true")
                .option("pollTimeoutMs", "500")
                .option("maxWaitMs", "10000")
                .option("metadataFields", "")
                .option("addressResolverClass",
                        "com.rabbitmq.spark.connector.TestAddressResolver")
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
                .option("filterValueColumn", "properties.subject")
                .option("addressResolverClass",
                        "com.rabbitmq.spark.connector.TestAddressResolver")
                .save();

        Dataset<Row> readDf = spark.read()
                .format("rabbitmq_streams")
                .option("endpoints", streamEndpoint())
                .option("stream", stream)
                .option("startingOffsets", "earliest")
                .option("filterValues", "alpha")
                .option("filterMatchUnfiltered", "false")
                .option("filterPostFilterClass",
                        "com.rabbitmq.spark.connector.TestAlphaPrefixPostFilter")
                .option("pollTimeoutMs", "500")
                .option("maxWaitMs", "10000")
                .option("metadataFields", "")
                .option("addressResolverClass",
                        "com.rabbitmq.spark.connector.TestAddressResolver")
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
                        "com.rabbitmq.spark.connector.TestObservationCollectorFactory")
                .option("addressResolverClass",
                        "com.rabbitmq.spark.connector.TestAddressResolver")
                .save();

        assertThat(TestObservationCollectorFactory.prePublishCount()).isGreaterThanOrEqualTo(12);
        assertThat(TestObservationCollectorFactory.publishedCount()).isGreaterThanOrEqualTo(12);
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
                        "com.rabbitmq.spark.connector.TestAddressResolver")
                .save();

        // Verify messages arrived via broker client
        var messages = consumeMessages(stream, 30);
        assertThat(messages).hasSize(30);

        List<String> values = messages.stream()
                .map(msg -> new String(msg.getBodyAsBinary()))
                .sorted()
                .toList();

        assertThat(values).contains("roundtrip-0", "roundtrip-15", "roundtrip-29");
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
                                "com.rabbitmq.spark.connector.TestAddressResolver")
                        .save()
        ).isInstanceOf(SparkException.class);
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
                                "com.rabbitmq.spark.connector.TestAddressResolver")
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
                        "com.rabbitmq.spark.connector.TestAddressResolver")
                .save();

        // Verify all 500 messages arrived
        List<Message> messages = consumeMessages(stream, 500);
        assertThat(messages).hasSize(500);
    }

    // ---- IT-SINK-004: batch settings (batchSize, batchPublishingDelayMs, enqueueTimeoutMs) ----

    @Test
    void batchWriteWithBatchSettings() {
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
                .option("batchSize", "10")
                .option("batchPublishingDelayMs", "0")
                .option("enqueueTimeoutMs", "10000")
                .option("addressResolverClass",
                        "com.rabbitmq.spark.connector.TestAddressResolver")
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
                        "com.rabbitmq.spark.connector.TestAddressResolver")
                .save();

        // Read back via connector to verify content integrity
        Dataset<Row> readDf = spark.read()
                .format("rabbitmq_streams")
                .option("endpoints", streamEndpoint())
                .option("stream", stream)
                .option("startingOffsets", "earliest")
                .option("metadataFields", "")
                .option("addressResolverClass",
                        "com.rabbitmq.spark.connector.TestAddressResolver")
                .load();

        List<Row> rows = readDf.collectAsList();
        assertThat(rows).hasSize(50);

        List<String> values = rows.stream()
                .map(row -> new String((byte[]) row.getAs("value")))
                .sorted()
                .toList();
        assertThat(values).contains("compressed-0", "compressed-1", "compressed-49");
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
                        "com.rabbitmq.spark.connector.TestAddressResolver")
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
                        "com.rabbitmq.spark.connector.TestAddressResolver")
                .save();

        // Both writes should succeed — publishing IDs increment across invocations
        List<Message> messages = consumeMessages(stream, 40);
        assertThat(messages).hasSize(40);

        List<String> values = messages.stream()
                .map(msg -> new String(msg.getBodyAsBinary()))
                .sorted()
                .toList();
        assertThat(values).contains("first-0", "first-19", "second-0", "second-19");
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
                                "com.rabbitmq.spark.connector.TestAddressResolver")
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
                        "com.rabbitmq.spark.connector.TestAddressResolver")
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
                                "com.rabbitmq.spark.connector.TestAddressResolver")
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
                                "com.rabbitmq.spark.connector.TestAddressResolver")
                        .save()
        ).satisfies(ex -> {
            assertThat(ex.getMessage()).containsIgnoringCase("partitionerClass");
            assertThat(ex.getMessage()).containsIgnoringCase("required");
        });
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
                                "com.rabbitmq.spark.connector.TestAddressResolver")
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
                        "com.rabbitmq.spark.connector.TestAddressResolver")
                .save();

        // Read back via connector
        Dataset<Row> readDf = spark.read()
                .format("rabbitmq_streams")
                .option("endpoints", streamEndpoint())
                .option("stream", stream)
                .option("startingOffsets", "earliest")
                .option("metadataFields", "")
                .option("addressResolverClass",
                        "com.rabbitmq.spark.connector.TestAddressResolver")
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
                                "com.rabbitmq.spark.connector.TestAddressResolver")
                        .save()
        ).hasMessageContaining("neither");
    }
}
