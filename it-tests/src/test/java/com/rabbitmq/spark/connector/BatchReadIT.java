package com.rabbitmq.spark.connector;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
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
    void batchReadFromStreamWithSingleActiveConsumerOption() {
        publishMessages(stream, 25, "sac-stream-");

        Dataset<Row> df = spark.read()
                .format("rabbitmq_streams")
                .option("endpoints", streamEndpoint())
                .option("stream", stream)
                .option("startingOffsets", "earliest")
                .option("singleActiveConsumer", "true")
                .option("consumerName", "stream-sac-" + System.currentTimeMillis())
                .option("metadataFields", "")
                .option("addressResolverClass",
                        "com.rabbitmq.spark.connector.TestAddressResolver")
                .load();

        List<Row> rows = df.collectAsList();
        assertThat(rows).hasSize(25);

        List<Long> offsets = rows.stream()
                .map(row -> (Long) row.getAs("offset"))
                .sorted()
                .toList();
        assertThat(offsets).isEqualTo(LongStream.range(0, 25).boxed().toList());
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
    void batchReadObservationCollectorClassInvoked() {
        publishMessages(stream, 15, "obs-read-");
        TestObservationCollectorFactory.reset();

        Dataset<Row> df = spark.read()
                .format("rabbitmq_streams")
                .option("endpoints", streamEndpoint())
                .option("stream", stream)
                .option("startingOffsets", "earliest")
                .option("metadataFields", "")
                .option("observationCollectorClass",
                        "com.rabbitmq.spark.connector.TestObservationCollectorFactory")
                .option("addressResolverClass",
                        "com.rabbitmq.spark.connector.TestAddressResolver")
                .load();

        List<Row> rows = df.collectAsList();
        assertThat(rows).hasSize(15);
        assertThat(TestObservationCollectorFactory.subscribeCount()).isGreaterThanOrEqualTo(1);
        assertThat(TestObservationCollectorFactory.handleCount()).isGreaterThanOrEqualTo(15);
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

    // ---- IT-RETRY-003: short pollTimeoutMs still succeeds ----

    @Test
    void batchReadShortPollTimeoutSucceeds() {
        publishMessages(stream, 50);

        // A very short pollTimeoutMs should not cause premature failure
        // as long as maxWaitMs gives enough total time
        Dataset<Row> df = spark.read()
                .format("rabbitmq_streams")
                .option("endpoints", streamEndpoint())
                .option("stream", stream)
                .option("startingOffsets", "earliest")
                .option("pollTimeoutMs", "200")
                .option("maxWaitMs", "60000")
                .option("metadataFields", "")
                .option("addressResolverClass",
                        "com.rabbitmq.spark.connector.TestAddressResolver")
                .load();

        List<Row> rows = df.collectAsList();
        assertThat(rows).hasSize(50);
    }

    // ---- IT-RETRY-004: exceeding maxWaitMs throws diagnostic error ----

    @Test
    void batchReadExceedMaxWaitMsThrowsDiagnostic() {
        // Publish only 10 messages but request offsets [0, 100)
        // Reader will time out waiting for offsets 10-99 that never arrive
        publishMessages(stream, 10);

        Dataset<Row> df = spark.read()
                .format("rabbitmq_streams")
                .option("endpoints", streamEndpoint())
                .option("stream", stream)
                .option("startingOffsets", "offset")
                .option("startingOffset", "0")
                .option("endingOffsets", "offset")
                .option("endingOffset", "100")
                .option("maxWaitMs", "3000")
                .option("pollTimeoutMs", "500")
                .option("metadataFields", "")
                .option("addressResolverClass",
                        "com.rabbitmq.spark.connector.TestAddressResolver")
                .load();

        assertThatThrownBy(df::collectAsList)
                .hasMessageContaining("Timed out waiting for messages")
                .hasMessageContaining("target end offset");
    }

    @Test
    void batchReadFailsFastWhenStreamClosesEvenWithLargeMaxWait() throws Exception {
        publishMessages(stream, 10);

        Dataset<Row> df = spark.read()
                .format("rabbitmq_streams")
                .option("endpoints", streamEndpoint())
                .option("stream", stream)
                .option("startingOffsets", "offset")
                .option("startingOffset", "0")
                .option("endingOffsets", "offset")
                .option("endingOffset", "100")
                .option("maxWaitMs", "60000")
                .option("pollTimeoutMs", "200")
                .option("metadataFields", "")
                .option("addressResolverClass",
                        "com.rabbitmq.spark.connector.TestAddressResolver")
                .load();

        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            long startMs = System.currentTimeMillis();
            Future<List<Row>> future = executor.submit(df::collectAsList);
            Thread.sleep(1000L);
            deleteStream(stream);

            assertThatThrownBy(() -> future.get(20, TimeUnit.SECONDS))
                    .isInstanceOfAny(ExecutionException.class, java.util.concurrent.TimeoutException.class)
                    .satisfies(ex -> {
                        if (ex instanceof ExecutionException ee) {
                            String message = ee.getCause() != null
                                    ? ee.getCause().getMessage()
                                    : ee.getMessage();
                            assertThat(message).isNotNull();
                            assertThat(
                                    message.contains("closed before reaching target end offset")
                                            || message.contains("does not exist")
                                            || message.contains("Timed out waiting for messages"))
                                    .isTrue();
                        }
                    });

            long elapsedMs = System.currentTimeMillis() - startMs;
            assertThat(elapsedMs).isLessThan(20000L);
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void batchReadAfterStreamDeletionFailsToCloseReader() {
        publishMessages(stream, 20);

        Dataset<Row> df = spark.read()
                .format("rabbitmq_streams")
                .option("endpoints", streamEndpoint())
                .option("stream", stream)
                .option("startingOffsets", "earliest")
                .option("metadataFields", "")
                .option("addressResolverClass",
                        "com.rabbitmq.spark.connector.TestAddressResolver")
                .load();

        deleteStream(stream);

        assertThatThrownBy(df::collectAsList)
                .hasMessageContaining("does not exist");
    }

    // ---- IT-FILTER-001: broker-side filter with filterValues ----

    @Test
    void batchReadFilterValuesPreFilter() {
        // Publish 50 messages with filterValue="alpha" and 50 with filterValue="beta"
        publishMessagesWithFilterValue(stream, 50, "alpha-", "alpha");
        publishMessagesWithFilterValue(stream, 50, "beta-", "beta");

        // Read with filterValues=alpha — broker Bloom filter pre-filters at chunk level
        Dataset<Row> df = spark.read()
                .format("rabbitmq_streams")
                .option("endpoints", streamEndpoint())
                .option("stream", stream)
                .option("startingOffsets", "earliest")
                .option("filterValues", "alpha")
                .option("filterMatchUnfiltered", "false")
                .option("pollTimeoutMs", "500")
                .option("maxWaitMs", "10000")
                .option("metadataFields", "")
                .option("addressResolverClass",
                        "com.rabbitmq.spark.connector.TestAddressResolver")
                .load();

        List<Row> rows = df.collectAsList();
        // Bloom filter may have false positives but no false negatives:
        // all 50 "alpha-" messages must be present
        assertThat(rows.size()).isGreaterThanOrEqualTo(50);

        List<String> values = rows.stream()
                .map(row -> new String((byte[]) row.getAs("value")))
                .toList();
        long alphaCount = values.stream().filter(v -> v.startsWith("alpha-")).count();
        assertThat(alphaCount).isEqualTo(50);
    }

    @Test
    void batchReadFilterValuesMultiple() {
        publishMessagesWithFilterValue(stream, 20, "alpha-", "alpha");
        publishMessagesWithFilterValue(stream, 20, "beta-", "beta");
        publishMessagesWithFilterValue(stream, 20, "gamma-", "gamma");

        Dataset<Row> df = spark.read()
                .format("rabbitmq_streams")
                .option("endpoints", streamEndpoint())
                .option("stream", stream)
                .option("startingOffsets", "earliest")
                .option("filterValues", "alpha,beta")
                .option("filterValueColumn", "filter")
                .option("filterMatchUnfiltered", "false")
                .option("pollTimeoutMs", "500")
                .option("maxWaitMs", "10000")
                .option("metadataFields", "")
                .option("addressResolverClass",
                        "com.rabbitmq.spark.connector.TestAddressResolver")
                .load();

        List<String> values = df.collectAsList().stream()
                .map(row -> new String((byte[]) row.getAs("value")))
                .toList();

        assertThat(values).hasSize(40);
        assertThat(values).allMatch(v -> v.startsWith("alpha-") || v.startsWith("beta-"));
    }

    // ---- IT-FILTER-002: filterMatchUnfiltered includes unfiltered messages ----

    @Test
    void batchReadFilterMatchUnfiltered() {
        // Publish 30 messages WITH filterValue="alpha"
        publishMessagesWithFilterValue(stream, 30, "filtered-", "alpha");
        // Publish 30 messages WITHOUT any filter value (regular publish)
        publishMessages(stream, 30, "unfiltered-");

        // With filterMatchUnfiltered=true, should get both filtered and unfiltered
        Dataset<Row> dfAll = spark.read()
                .format("rabbitmq_streams")
                .option("endpoints", streamEndpoint())
                .option("stream", stream)
                .option("startingOffsets", "earliest")
                .option("filterValues", "alpha")
                .option("filterMatchUnfiltered", "true")
                .option("pollTimeoutMs", "500")
                .option("maxWaitMs", "10000")
                .option("metadataFields", "")
                .option("addressResolverClass",
                        "com.rabbitmq.spark.connector.TestAddressResolver")
                .load();

        List<Row> rowsAll = dfAll.collectAsList();
        // Should include all 60 messages (30 filtered + 30 unfiltered)
        assertThat(rowsAll.size()).isGreaterThanOrEqualTo(60);

        List<String> allValues = rowsAll.stream()
                .map(row -> new String((byte[]) row.getAs("value")))
                .toList();
        long filteredCount = allValues.stream().filter(v -> v.startsWith("filtered-")).count();
        long unfilteredCount = allValues.stream().filter(v -> v.startsWith("unfiltered-")).count();
        assertThat(filteredCount).isEqualTo(30);
        assertThat(unfilteredCount).isEqualTo(30);

        // With filterMatchUnfiltered=false, should get only filtered messages
        Dataset<Row> dfFiltered = spark.read()
                .format("rabbitmq_streams")
                .option("endpoints", streamEndpoint())
                .option("stream", stream)
                .option("startingOffsets", "earliest")
                .option("filterValues", "alpha")
                .option("filterMatchUnfiltered", "false")
                .option("pollTimeoutMs", "500")
                .option("maxWaitMs", "10000")
                .option("metadataFields", "")
                .option("addressResolverClass",
                        "com.rabbitmq.spark.connector.TestAddressResolver")
                .load();

        List<Row> rowsFiltered = dfFiltered.collectAsList();
        // At minimum, all 30 "filtered-" messages must be present (Bloom false positives possible)
        assertThat(rowsFiltered.size()).isGreaterThanOrEqualTo(30);

        List<String> filteredValues = rowsFiltered.stream()
                .map(row -> new String((byte[]) row.getAs("value")))
                .toList();
        long alphaCount = filteredValues.stream().filter(v -> v.startsWith("filtered-")).count();
        assertThat(alphaCount).isEqualTo(30);
    }

    // ---- IT-FILTER-003: custom post-filter class ----

    @Test
    void batchReadCustomPostFilter() {
        // Publish 25 "keep-" messages and 25 "drop-" messages
        publishMessages(stream, 25, "keep-");
        publishMessages(stream, 25, "drop-");

        // Read with TestPostFilter which accepts only messages starting with "keep-"
        Dataset<Row> df = spark.read()
                .format("rabbitmq_streams")
                .option("endpoints", streamEndpoint())
                .option("stream", stream)
                .option("startingOffsets", "earliest")
                .option("filterPostFilterClass",
                        "com.rabbitmq.spark.connector.TestPostFilter")
                .option("pollTimeoutMs", "500")
                .option("maxWaitMs", "10000")
                .option("metadataFields", "")
                .option("addressResolverClass",
                        "com.rabbitmq.spark.connector.TestAddressResolver")
                .load();

        List<Row> rows = df.collectAsList();
        assertThat(rows).hasSize(25);

        List<String> values = rows.stream()
                .map(row -> new String((byte[]) row.getAs("value")))
                .toList();
        assertThat(values).allMatch(v -> v.startsWith("keep-"));
    }

    // ---- IT-OPT-002: uris option instead of endpoints ----

    @Test
    void batchReadWithUrisInsteadOfEndpoints() {
        publishMessages(stream, 20);

        String uri = "rabbitmq-stream://guest:guest@" + RABBIT.getHost() + ":"
                + RABBIT.getMappedPort(STREAM_PORT);

        Dataset<Row> df = spark.read()
                .format("rabbitmq_streams")
                .option("uris", uri)
                .option("stream", stream)
                .option("startingOffsets", "earliest")
                .option("metadataFields", "")
                .option("addressResolverClass",
                        "com.rabbitmq.spark.connector.TestAddressResolver")
                .load();

        List<Row> rows = df.collectAsList();
        assertThat(rows).hasSize(20);
    }

    // ---- IT-OPT-004a: wrong password fails fast ----

    @Test
    void batchReadWithWrongPasswordFailsFast() {
        publishMessages(stream, 5);

        Dataset<Row> df = spark.read()
                .format("rabbitmq_streams")
                .option("endpoints", streamEndpoint())
                .option("stream", stream)
                .option("username", "guest")
                .option("password", "wrong-password")
                .option("startingOffsets", "earliest")
                .option("addressResolverClass",
                        "com.rabbitmq.spark.connector.TestAddressResolver")
                .load();

        assertThatThrownBy(df::collectAsList)
                .satisfies(ex -> assertThat(ex.toString())
                        .containsIgnoringCase("authentication"));
    }

    // ---- IT-OPT-004b: wrong vhost fails fast ----

    @Test
    void batchReadWithWrongVhostFailsFast() {
        publishMessages(stream, 5);

        Dataset<Row> df = spark.read()
                .format("rabbitmq_streams")
                .option("endpoints", streamEndpoint())
                .option("stream", stream)
                .option("vhost", "/nonexistent-vhost")
                .option("startingOffsets", "earliest")
                .option("addressResolverClass",
                        "com.rabbitmq.spark.connector.TestAddressResolver")
                .load();

        assertThatThrownBy(df::collectAsList)
                .satisfies(ex -> assertThat(ex.toString())
                        .containsAnyOf("VIRTUAL_HOST", "virtual host", "access refused"));
    }

    // ---- IT-OPT-005: metadata field subsets ----

    @Test
    void batchReadMetadataFieldSubsets() {
        publishMessages(stream, 5);

        String[] fixedCols = {"value", "stream", "offset", "chunk_timestamp"};

        // properties only
        Dataset<Row> df1 = spark.read()
                .format("rabbitmq_streams")
                .option("endpoints", streamEndpoint())
                .option("stream", stream)
                .option("startingOffsets", "earliest")
                .option("metadataFields", "properties")
                .option("addressResolverClass",
                        "com.rabbitmq.spark.connector.TestAddressResolver")
                .load();
        assertThat(df1.schema().fieldNames()).containsExactly(
                "value", "stream", "offset", "chunk_timestamp", "properties");
        assertThat(df1.collectAsList()).hasSize(5);

        // application_properties only
        Dataset<Row> df2 = spark.read()
                .format("rabbitmq_streams")
                .option("endpoints", streamEndpoint())
                .option("stream", stream)
                .option("startingOffsets", "earliest")
                .option("metadataFields", "application_properties")
                .option("addressResolverClass",
                        "com.rabbitmq.spark.connector.TestAddressResolver")
                .load();
        assertThat(df2.schema().fieldNames()).containsExactly(
                "value", "stream", "offset", "chunk_timestamp", "application_properties");
        assertThat(df2.collectAsList()).hasSize(5);

        // creation_time only
        Dataset<Row> df3 = spark.read()
                .format("rabbitmq_streams")
                .option("endpoints", streamEndpoint())
                .option("stream", stream)
                .option("startingOffsets", "earliest")
                .option("metadataFields", "creation_time")
                .option("addressResolverClass",
                        "com.rabbitmq.spark.connector.TestAddressResolver")
                .load();
        assertThat(df3.schema().fieldNames()).containsExactly(
                "value", "stream", "offset", "chunk_timestamp", "creation_time");
        assertThat(df3.collectAsList()).hasSize(5);

        // routing_key,message_annotations
        Dataset<Row> df4 = spark.read()
                .format("rabbitmq_streams")
                .option("endpoints", streamEndpoint())
                .option("stream", stream)
                .option("startingOffsets", "earliest")
                .option("metadataFields", "routing_key,message_annotations")
                .option("addressResolverClass",
                        "com.rabbitmq.spark.connector.TestAddressResolver")
                .load();
        List<String> fieldNames = Arrays.asList(df4.schema().fieldNames());
        assertThat(fieldNames).containsAll(Arrays.asList(fixedCols));
        assertThat(fieldNames).contains("message_annotations", "routing_key");
        assertThat(fieldNames).hasSize(6); // 4 fixed + 2 metadata
        assertThat(df4.collectAsList()).hasSize(5);
    }

    // ---- IT-OPT-006-source: invalid option combinations ----

    @Test
    void batchReadInvalidOptionCombinations() {
        publishMessages(stream, 5);

        // 1. Both stream and superstream set
        assertThatThrownBy(() ->
                spark.read()
                        .format("rabbitmq_streams")
                        .option("endpoints", streamEndpoint())
                        .option("stream", stream)
                        .option("superstream", "some-super")
                        .option("startingOffsets", "earliest")
                        .option("addressResolverClass",
                                "com.rabbitmq.spark.connector.TestAddressResolver")
                        .load()
                        .collectAsList()
        ).hasMessageContaining("both");

        // 2. Neither endpoints nor uris set
        assertThatThrownBy(() ->
                spark.read()
                        .format("rabbitmq_streams")
                        .option("stream", stream)
                        .option("startingOffsets", "earliest")
                        .option("addressResolverClass",
                                "com.rabbitmq.spark.connector.TestAddressResolver")
                        .load()
                        .collectAsList()
        ).satisfies(ex -> {
            assertThat(ex.getMessage()).contains("endpoints");
            assertThat(ex.getMessage()).contains("uris");
        });

        // 3. startingOffsets=offset without startingOffset
        assertThatThrownBy(() ->
                spark.read()
                        .format("rabbitmq_streams")
                        .option("endpoints", streamEndpoint())
                        .option("stream", stream)
                        .option("startingOffsets", "offset")
                        .option("addressResolverClass",
                                "com.rabbitmq.spark.connector.TestAddressResolver")
                        .load()
                        .collectAsList()
        ).satisfies(ex -> {
            assertThat(ex.getMessage()).contains("startingOffset");
            assertThat(ex.getMessage()).contains("required");
        });
    }
}
