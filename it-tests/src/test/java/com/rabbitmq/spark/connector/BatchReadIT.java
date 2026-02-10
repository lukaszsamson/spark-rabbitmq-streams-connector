package com.rabbitmq.spark.connector;

import org.apache.spark.SparkException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

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
    void batchReadOffsetsAreUniqueAndGapFree() {
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

        Set<Long> offsets = df.collectAsList().stream()
                .map(row -> (Long) row.getAs("offset"))
                .collect(Collectors.toSet());

        assertThat(offsets).hasSize(100);
        long minOffset = offsets.stream().mapToLong(Long::longValue).min().orElse(-1);
        long maxOffset = offsets.stream().mapToLong(Long::longValue).max().orElse(-1);
        assertThat(minOffset).isEqualTo(0L);
        assertThat(maxOffset).isEqualTo(99L);
        assertThat(offsets).isEqualTo(LongStream.rangeClosed(0, 99).boxed().collect(Collectors.toSet()));
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

    // ---- IT-OFFSET-002: startingOffsets=offset boundary conditions ----

    @Test
    void batchReadStartingOffsetAtZero() {
        publishMessages(stream, 50);

        // offset=0 should read all messages (0 is the first offset in RabbitMQ streams)
        Dataset<Row> df = spark.read()
                .format("rabbitmq_streams")
                .option("endpoints", streamEndpoint())
                .option("stream", stream)
                .option("startingOffsets", "offset")
                .option("startingOffset", "0")
                .option("metadataFields", "")
                .option("addressResolverClass",
                        "com.rabbitmq.spark.connector.TestAddressResolver")
                .load();

        List<Row> rows = df.collectAsList();
        assertThat(rows).hasSize(50);

        long minOffset = rows.stream()
                .mapToLong(row -> row.getAs("offset"))
                .min().orElse(-1);
        assertThat(minOffset).isEqualTo(0);
    }

    @Test
    void batchReadStartingOffsetAtOne() {
        publishMessages(stream, 50);

        // offset=1 should skip the first message
        Dataset<Row> df = spark.read()
                .format("rabbitmq_streams")
                .option("endpoints", streamEndpoint())
                .option("stream", stream)
                .option("startingOffsets", "offset")
                .option("startingOffset", "1")
                .option("metadataFields", "")
                .option("addressResolverClass",
                        "com.rabbitmq.spark.connector.TestAddressResolver")
                .load();

        List<Row> rows = df.collectAsList();
        assertThat(rows).hasSize(49);

        long minOffset = rows.stream()
                .mapToLong(row -> row.getAs("offset"))
                .min().orElse(-1);
        assertThat(minOffset).isEqualTo(1);
    }

    @Test
    void batchReadStartingOffsetAtTailMinusOne() {
        publishMessages(stream, 50);

        // offset=49 (tail-1) should read only the last message
        Dataset<Row> df = spark.read()
                .format("rabbitmq_streams")
                .option("endpoints", streamEndpoint())
                .option("stream", stream)
                .option("startingOffsets", "offset")
                .option("startingOffset", "49")
                .option("metadataFields", "")
                .option("addressResolverClass",
                        "com.rabbitmq.spark.connector.TestAddressResolver")
                .load();

        List<Row> rows = df.collectAsList();
        assertThat(rows).hasSize(1);

        long offset = rows.get(0).getAs("offset");
        assertThat(offset).isEqualTo(49);
    }

    @Test
    void batchReadStartingOffsetAtTail() {
        publishMessages(stream, 50);

        // offset=50 (past tail) should return zero rows
        Dataset<Row> df = spark.read()
                .format("rabbitmq_streams")
                .option("endpoints", streamEndpoint())
                .option("stream", stream)
                .option("startingOffsets", "offset")
                .option("startingOffset", "50")
                .option("metadataFields", "")
                .option("addressResolverClass",
                        "com.rabbitmq.spark.connector.TestAddressResolver")
                .load();

        List<Row> rows = df.collectAsList();
        assertThat(rows).isEmpty();
    }

    // ---- IT-OFFSET-003: startingOffsets=timestamp ----

    @Test
    void batchReadWithTimestamp() throws Exception {
        // Publish first batch
        publishMessages(stream, 30, "batch1-");

        // Wait to create a clear timestamp boundary between batches
        Thread.sleep(3000);
        long timestampBetweenBatches = System.currentTimeMillis();
        Thread.sleep(1000);

        // Publish second batch
        publishMessages(stream, 30, "batch2-");

        // Read with timestamp — should get at least the second batch
        Dataset<Row> df = spark.read()
                .format("rabbitmq_streams")
                .option("endpoints", streamEndpoint())
                .option("stream", stream)
                .option("startingOffsets", "timestamp")
                .option("startingTimestamp", String.valueOf(timestampBetweenBatches))
                .option("metadataFields", "")
                .option("addressResolverClass",
                        "com.rabbitmq.spark.connector.TestAddressResolver")
                .load();

        List<Row> rows = df.collectAsList();

        // Timestamp resolution is at chunk level, so we may get some messages
        // from before the timestamp. But we should get all 30 from the second batch.
        assertThat(rows.size()).isGreaterThanOrEqualTo(30);
        assertThat(rows.size()).isLessThanOrEqualTo(60);

        // Verify at least one message from the second batch is present
        List<String> values = rows.stream()
                .map(row -> new String((byte[]) row.getAs("value")))
                .toList();
        assertThat(values).anyMatch(v -> v.startsWith("batch2-"));
    }

    // ---- IT-OFFSET-004: endingOffsets=latest fixed snapshot ----

    @Test
    void batchReadEndingOffsetsLatestReflectsTailAtReadTime() {
        // Publish initial messages
        publishMessages(stream, 50, "initial-");

        // First read with endingOffsets=latest should capture current tail (50)
        Dataset<Row> df1 = spark.read()
                .format("rabbitmq_streams")
                .option("endpoints", streamEndpoint())
                .option("stream", stream)
                .option("startingOffsets", "earliest")
                .option("endingOffsets", "latest")
                .option("metadataFields", "")
                .option("addressResolverClass",
                        "com.rabbitmq.spark.connector.TestAddressResolver")
                .load();

        List<Row> rows1 = df1.collectAsList();
        assertThat(rows1).hasSize(50);

        // Publish more messages
        publishMessages(stream, 50, "additional-");

        // Second read is a fresh DataFrame with endingOffsets=latest, so it should see all 100
        Dataset<Row> df2 = spark.read()
                .format("rabbitmq_streams")
                .option("endpoints", streamEndpoint())
                .option("stream", stream)
                .option("startingOffsets", "earliest")
                .option("endingOffsets", "latest")
                .option("metadataFields", "")
                .option("addressResolverClass",
                        "com.rabbitmq.spark.connector.TestAddressResolver")
                .load();

        List<Row> rows2 = df2.collectAsList();
        assertThat(rows2).hasSize(100);
    }

    // ---- IT-OFFSET-005: startingOffset > endingOffset validation ----

    @Test
    void batchReadStartingOffsetGreaterThanEndingOffsetFails() {
        publishMessages(stream, 10);

        Dataset<Row> df = spark.read()
                .format("rabbitmq_streams")
                .option("endpoints", streamEndpoint())
                .option("stream", stream)
                .option("startingOffsets", "offset")
                .option("startingOffset", "20")
                .option("endingOffsets", "offset")
                .option("endingOffset", "10")
                .option("metadataFields", "")
                .option("addressResolverClass",
                        "com.rabbitmq.spark.connector.TestAddressResolver")
                .load();

        assertThatThrownBy(df::collectAsList)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("greater than endingOffset");
    }

    // ---- IT-SPLIT-003: minPartitions uneven distribution ----

    @Test
    void batchReadMinPartitionsUnevenDistribution() {
        // Publish a non-round number of messages to test remainder distribution
        publishMessages(stream, 103);

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
        assertThat(rows).hasSize(103);

        // Verify no gaps or duplicates in offsets
        Set<Long> offsets = rows.stream()
                .map(row -> (Long) row.getAs("offset"))
                .collect(Collectors.toSet());
        assertThat(offsets).hasSize(103);

        long minOffset = offsets.stream().mapToLong(Long::longValue).min().orElse(-1);
        long maxOffset = offsets.stream().mapToLong(Long::longValue).max().orElse(-1);

        // Verify contiguous range
        Set<Long> expected = LongStream.rangeClosed(minOffset, maxOffset)
                .boxed().collect(Collectors.toSet());
        assertThat(offsets).isEqualTo(expected);
    }

    // ---- IT-SPLIT-004: split boundary at chunk edge ----

    @Test
    void batchReadMinPartitionsSplitBoundaryAtChunkEdge() {
        // Publish exactly 100 messages and split into 4 partitions
        publishMessages(stream, 100);

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
        assertThat(rows).hasSize(100);

        // Verify complete offset coverage with no off-by-one
        Set<Long> offsets = rows.stream()
                .map(row -> (Long) row.getAs("offset"))
                .collect(Collectors.toSet());
        assertThat(offsets).hasSize(100);

        long minOffset = offsets.stream().mapToLong(Long::longValue).min().orElse(-1);
        long maxOffset = offsets.stream().mapToLong(Long::longValue).max().orElse(-1);
        assertThat(maxOffset - minOffset).isEqualTo(99);
    }
}
