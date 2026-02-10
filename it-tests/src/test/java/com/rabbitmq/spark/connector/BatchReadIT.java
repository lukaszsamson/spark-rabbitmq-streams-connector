package com.rabbitmq.spark.connector;

import org.apache.spark.SparkException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Integration tests for batch reads via the {@code rabbitmq_streams} DataSource.
 */
class BatchReadIT extends AbstractRabbitMQIT {

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
    void batchReadAllMessages() {
        publishMessages(stream, 50);

        Dataset<Row> df = spark.read()
                .format("rabbitmq_streams")
                .option("endpoints", streamEndpoint())
                .option("stream", stream)
                .option("startingOffsets", "earliest")
                .option("metadataFields", "")
                .option("addressResolverClass",
                        "com.rabbitmq.spark.connector.TestAddressResolver")
                .load();

        // Verify fixed columns are present
        assertThat(df.schema().fieldNames())
                .contains("value", "stream", "offset", "chunk_timestamp");

        List<Row> rows = df.collectAsList();
        assertThat(rows).hasSize(50);

        // Verify stream column value
        for (Row row : rows) {
            assertThat(row.getAs("stream").toString()).isEqualTo(stream);
        }
    }

    @Test
    void batchReadEmptyStream() {
        // Empty stream should produce zero rows
        Dataset<Row> df = spark.read()
                .format("rabbitmq_streams")
                .option("endpoints", streamEndpoint())
                .option("stream", stream)
                .option("startingOffsets", "earliest")
                .option("addressResolverClass",
                        "com.rabbitmq.spark.connector.TestAddressResolver")
                .load();

        List<Row> rows = df.collectAsList();
        assertThat(rows).isEmpty();
    }

    @Test
    void batchReadWithOffsetRange() {
        publishMessages(stream, 100);

        // Read only offsets [10, 20)
        Dataset<Row> df = spark.read()
                .format("rabbitmq_streams")
                .option("endpoints", streamEndpoint())
                .option("stream", stream)
                .option("startingOffsets", "offset")
                .option("startingOffset", "10")
                .option("endingOffsets", "offset")
                .option("endingOffset", "20")
                .option("metadataFields", "")
                .option("addressResolverClass",
                        "com.rabbitmq.spark.connector.TestAddressResolver")
                .load();

        List<Row> rows = df.collectAsList();
        assertThat(rows).hasSize(10);

        // Verify offset range
        for (Row row : rows) {
            long offset = row.getAs("offset");
            assertThat(offset).isGreaterThanOrEqualTo(10).isLessThan(20);
        }
    }

    @Test
    void batchReadPreservesMessageContent() {
        publishMessages(stream, 10, "hello-");

        Dataset<Row> df = spark.read()
                .format("rabbitmq_streams")
                .option("endpoints", streamEndpoint())
                .option("stream", stream)
                .option("startingOffsets", "earliest")
                .option("metadataFields", "")
                .option("addressResolverClass",
                        "com.rabbitmq.spark.connector.TestAddressResolver")
                .load();

        List<Row> rows = df.collectAsList();
        assertThat(rows).hasSize(10);

        // Check message body contents
        List<String> values = rows.stream()
                .map(row -> new String((byte[]) row.getAs("value")))
                .sorted()
                .toList();

        assertThat(values).contains("hello-0", "hello-1", "hello-9");
    }

    @Test
    void batchReadWithMetadataFields() {
        publishMessages(stream, 5);

        // Default includes all metadata fields
        Dataset<Row> dfFull = spark.read()
                .format("rabbitmq_streams")
                .option("endpoints", streamEndpoint())
                .option("stream", stream)
                .option("startingOffsets", "earliest")
                .option("addressResolverClass",
                        "com.rabbitmq.spark.connector.TestAddressResolver")
                .load();

        assertThat(dfFull.schema().fieldNames())
                .contains("properties", "application_properties",
                        "message_annotations", "creation_time", "routing_key");

        // With no metadata fields
        Dataset<Row> dfMinimal = spark.read()
                .format("rabbitmq_streams")
                .option("endpoints", streamEndpoint())
                .option("stream", stream)
                .option("startingOffsets", "earliest")
                .option("metadataFields", "")
                .option("addressResolverClass",
                        "com.rabbitmq.spark.connector.TestAddressResolver")
                .load();

        assertThat(dfMinimal.schema().fieldNames())
                .containsExactly("value", "stream", "offset", "chunk_timestamp");
    }

    @Test
    void batchReadOffsetsAreMonotonic() {
        publishMessages(stream, 100);

        Dataset<Row> df = spark.read()
                .format("rabbitmq_streams")
                .option("endpoints", streamEndpoint())
                .option("stream", stream)
                .option("startingOffsets", "earliest")
                .option("metadataFields", "")
                .option("addressResolverClass",
                        "com.rabbitmq.spark.connector.TestAddressResolver")
                .load();

        List<Long> offsets = df.collectAsList().stream()
                .map(row -> (Long) row.getAs("offset"))
                .sorted()
                .toList();

        assertThat(offsets).hasSize(100);
        // Verify monotonic: offsets[i+1] > offsets[i]
        for (int i = 1; i < offsets.size(); i++) {
            assertThat(offsets.get(i)).isGreaterThan(offsets.get(i - 1));
        }
    }

    @Test
    void batchReadWithMinPartitions() {
        publishMessages(stream, 200);

        Dataset<Row> df = spark.read()
                .format("rabbitmq_streams")
                .option("endpoints", streamEndpoint())
                .option("stream", stream)
                .option("startingOffsets", "earliest")
                .option("minPartitions", "4")
                .option("metadataFields", "")
                .option("addressResolverClass",
                        "com.rabbitmq.spark.connector.TestAddressResolver")
                .load();

        List<Row> rows = df.collectAsList();
        assertThat(rows).hasSize(200);

        // Verify all offsets are present and monotonic
        List<Long> offsets = rows.stream()
                .map(row -> (Long) row.getAs("offset"))
                .sorted()
                .toList();
        for (int i = 1; i < offsets.size(); i++) {
            assertThat(offsets.get(i)).isGreaterThan(offsets.get(i - 1));
        }
    }

    @Test
    void batchReadFailOnDataLossForDeletedStream() {
        // failOnDataLoss=true (default) should fail when stream is deleted
        publishMessages(stream, 10);
        deleteStream(stream);

        Dataset<Row> df = spark.read()
                .format("rabbitmq_streams")
                .option("endpoints", streamEndpoint())
                .option("stream", stream)
                .option("startingOffsets", "earliest")
                .option("failOnDataLoss", "true")
                .option("addressResolverClass",
                        "com.rabbitmq.spark.connector.TestAddressResolver")
                .load();

        assertThatThrownBy(df::collectAsList)
                .hasMessageContaining("does not exist");
    }

    @Test
    void environmentPoolReusesAcrossMultipleBatchReads() {
        // Multiple sequential batch reads should work correctly,
        // verifying the pool properly acquires and releases environments
        publishMessages(stream, 30);

        for (int i = 0; i < 3; i++) {
            Dataset<Row> df = spark.read()
                    .format("rabbitmq_streams")
                    .option("endpoints", streamEndpoint())
                    .option("stream", stream)
                    .option("startingOffsets", "earliest")
                    .option("metadataFields", "")
                    .option("addressResolverClass",
                            "com.rabbitmq.spark.connector.TestAddressResolver")
                    .load();

            List<Row> rows = df.collectAsList();
            assertThat(rows).hasSize(30);
        }
    }

    @Test
    void batchReadNonExistentStreamFailsFast() {
        // Reading from a non-existent stream should fail with a descriptive error
        // regardless of failOnDataLoss setting (single stream mode = configuration error)
        Dataset<Row> df = spark.read()
                .format("rabbitmq_streams")
                .option("endpoints", streamEndpoint())
                .option("stream", "non-existent-stream-" + System.currentTimeMillis())
                .option("startingOffsets", "earliest")
                .option("addressResolverClass",
                        "com.rabbitmq.spark.connector.TestAddressResolver")
                .load();

        assertThatThrownBy(df::collectAsList)
                .hasMessageContaining("does not exist");
    }
}
