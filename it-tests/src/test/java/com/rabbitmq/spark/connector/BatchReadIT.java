package com.rabbitmq.spark.connector;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.sql.execution.SparkPlan;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.MapType;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
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
        List<String> expected = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            expected.add("hello-" + i);
        }
        assertThat(values).containsExactlyInAnyOrderElementsOf(expected);
    }

    @Test
    void batchReadExposesExpectedSchemaColumnTypes() {
        publishMessages(stream, 1, "schema-");

        Dataset<Row> df = spark.read()
                .format("rabbitmq_streams")
                .option("endpoints", streamEndpoint())
                .option("stream", stream)
                .option("startingOffsets", "earliest")
                .option("addressResolverClass",
                        "com.rabbitmq.spark.connector.TestAddressResolver")
                .load();

        StructType schema = df.schema();
        assertThat(schema.apply("value").dataType()).isEqualTo(DataTypes.BinaryType);
        assertThat(schema.apply("stream").dataType()).isEqualTo(DataTypes.StringType);
        assertThat(schema.apply("offset").dataType()).isEqualTo(DataTypes.LongType);
        assertThat(schema.apply("chunk_timestamp").dataType()).isEqualTo(DataTypes.TimestampType);
        assertThat(schema.apply("properties").dataType()).isInstanceOf(StructType.class);
        assertThat(schema.apply("application_properties").dataType()).isInstanceOf(MapType.class);
        assertThat(schema.apply("message_annotations").dataType()).isInstanceOf(MapType.class);
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
                    .isInstanceOf(ExecutionException.class)
                    .satisfies(ex -> {
                        if (ex instanceof ExecutionException ee) {
                            String message = ee.getCause() != null
                                    ? ee.getCause().getMessage()
                                    : ee.getMessage();
                            assertThat(message).isNotNull();
                            assertThat(
                                    message.contains("closed before reaching target end offset")
                                            || message.contains("does not exist"))
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
                .option("filterValuePath", "application_properties.filter")
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
                .option("filterValuePath", "application_properties.filter")
                .option("filterMatchUnfiltered", "true")
                .option("pollTimeoutMs", "200")
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
                .option("filterValuePath", "application_properties.filter")
                .option("filterMatchUnfiltered", "false")
                .option("pollTimeoutMs", "200")
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

    // ---- IT-OPT-003: TLS modes (trustAll) ----

    @Test
    void batchReadWithTlsTrustAll() {
        publishMessages(stream, 12);

        Throwable lastError = null;
        for (int attempt = 1; attempt <= 3; attempt++) {
            try {
                Dataset<Row> df = spark.read()
                        .format("rabbitmq_streams")
                        .option("endpoints", streamTlsEndpoint())
                        .option("stream", stream)
                        .option("tls", "true")
                        .option("tls.trustAll", "true")
                        .option("startingOffsets", "earliest")
                        .option("metadataFields", "")
                        .option("addressResolverClass",
                                "com.rabbitmq.spark.connector.TestAddressResolver")
                        .load();
                List<Row> rows = df.collectAsList();
                assertThat(rows).hasSize(12);
                return;
            } catch (Throwable t) {
                lastError = t;
                if (!isTransientTlsHandshakeFailure(t)) {
                    throw t;
                }
                if (attempt == 3) {
                    throw new AssertionError(
                            "TLS trustAll read did not stabilize after retries; this indicates TLS/bootstrap "
                                    + "flakiness in the test environment. Last error: " + t.getMessage(),
                            t);
                }
                try {
                    Thread.sleep(300L * attempt);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
                }
            }
        }
        throw new IllegalStateException("Unexpected TLS read retry flow", lastError);
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

    // ---- IT-OFFSET-007: no matched timestamp resolves to error ----

    @Test
    void batchReadTimestampNoMatchFailsFast() {
        publishMessages(stream, 10, "ts-");

        long futureTimestamp = System.currentTimeMillis() + 24 * 60 * 60 * 1000L;

        Dataset<Row> df = spark.read()
                .format("rabbitmq_streams")
                .option("endpoints", streamEndpoint())
                .option("stream", stream)
                .option("startingOffsets", "timestamp")
                .option("startingTimestamp", String.valueOf(futureTimestamp))
                .option("metadataFields", "")
                .option("addressResolverClass",
                        "com.rabbitmq.spark.connector.TestAddressResolver")
                .load();

        assertThatThrownBy(df::collectAsList)
                .satisfies(ex -> assertThat(ex.toString())
                        .contains("No offset matched from request"));
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

    // ---- IT-SCHEMA-003: self-join generates correct metrics ----

    @Test
    void batchReadSelfJoinCountsMatch() {
        publishMessages(stream, 8, "join-");

        Dataset<Row> df = spark.read()
                .format("rabbitmq_streams")
                .option("endpoints", streamEndpoint())
                .option("stream", stream)
                .option("startingOffsets", "earliest")
                .option("metadataFields", "")
                .option("addressResolverClass",
                        "com.rabbitmq.spark.connector.TestAddressResolver")
                .load()
                .selectExpr("CAST(value AS STRING) AS value");

        Dataset<Row> left = df.withColumn("key", df.col("value"));
        Dataset<Row> right = df.withColumn("key", df.col("value"));
        long joinedCount = left.join(right, "key").count();

        assertThat(joinedCount).isEqualTo(8L);
    }

    // ---- IT-METRIC-001: source custom metrics reported ----

    @Test
    void batchReadReportsSourceCustomMetrics() {
        publishMessages(stream, 15, "metric-");

        Dataset<Row> df = spark.read()
                .format("rabbitmq_streams")
                .option("endpoints", streamEndpoint())
                .option("stream", stream)
                .option("startingOffsets", "earliest")
                .option("metadataFields", "")
                .option("addressResolverClass",
                        "com.rabbitmq.spark.connector.TestAddressResolver")
                .load();

        df.collectAsList();

        Set<String> metrics = collectMetricNames(df.queryExecution().executedPlan());
        assertThat(metrics)
                .contains("Total records read from RabbitMQ streams")
                .contains("Total bytes read from RabbitMQ streams")
                .contains("Total read latency in milliseconds (time spent waiting for messages)");
    }

    // ---- IT-OFFSET-009: checkpoint forward compatibility (Spark 3.5 -> 4.x) ----

    @Test
    void batchReadCheckpointForwardCompatibilitySpark35() throws Exception {
        var fixtureMetadata = getClass().getClassLoader()
                .getResource("fixtures/spark35-checkpoint-v1/metadata");
        assertThat(fixtureMetadata)
                .as("spark35 checkpoint fixture must exist in test resources")
                .isNotNull();
        Path fixtureDir = Path.of(fixtureMetadata.toURI()).getParent();
        Path checkpointDir = Files.createTempDirectory("spark35-fixture-ckpt-");
        copyDirectory(fixtureDir, checkpointDir);
        rewriteFixtureOffsetForStream(checkpointDir.resolve("offsets").resolve("0"), stream);

        publishMessages(stream, 25, "ckpt35-");
        Thread.sleep(500);
        AtomicLong processedRows = new AtomicLong(0);

        StreamingQuery query = spark.readStream()
                .format("rabbitmq_streams")
                .option("endpoints", streamEndpoint())
                .option("stream", stream)
                .option("startingOffsets", "earliest")
                .option("metadataFields", "")
                .option("addressResolverClass",
                        "com.rabbitmq.spark.connector.TestAddressResolver")
                .load()
                .writeStream()
                .foreachBatch((VoidFunction2<Dataset<Row>, Long>) (batch, id) -> {
                    processedRows.addAndGet(batch.count());
                })
                .option("checkpointLocation", checkpointDir.toString())
                .trigger(Trigger.AvailableNow())
                .start();

        query.awaitTermination(120_000);
        assertThat(query.exception().isEmpty()).isTrue();
        assertThat(processedRows.get()).isGreaterThan(0L);
    }

    private static void rewriteFixtureOffsetForStream(Path offsetsFile, String stream) throws IOException {
        List<String> lines = Files.readAllLines(offsetsFile);
        assertThat(lines).hasSizeGreaterThanOrEqualTo(3);
        lines.set(2, "{\"" + stream + "\":0}");
        Files.write(offsetsFile, lines);
    }

    private static void copyDirectory(Path source, Path target) throws IOException {
        try (var paths = Files.walk(source)) {
            for (Path sourcePath : paths.toList()) {
                Path destination = target.resolve(source.relativize(sourcePath).toString());
                if (Files.isDirectory(sourcePath)) {
                    Files.createDirectories(destination);
                } else {
                    Files.copy(sourcePath, destination, StandardCopyOption.REPLACE_EXISTING);
                }
            }
        }
    }

    // ---- IT-OFFSET-010: mixed timestamp starting + numeric ending offset ----

    @Test
    void batchReadStartingTimestampWithNumericEndingOffset() throws Exception {
        publishMessages(stream, 20, "pre-");
        Thread.sleep(1500);
        long boundaryTimestamp = System.currentTimeMillis();
        Thread.sleep(1000);
        publishMessages(stream, 30, "post-");

        Dataset<Row> df = spark.read()
                .format("rabbitmq_streams")
                .option("endpoints", streamEndpoint())
                .option("stream", stream)
                .option("startingOffsets", "timestamp")
                .option("startingTimestamp", String.valueOf(boundaryTimestamp))
                .option("endingOffsets", "offset")
                .option("endingOffset", "35")
                .option("metadataFields", "")
                .option("addressResolverClass",
                        "com.rabbitmq.spark.connector.TestAddressResolver")
                .load();

        List<Row> rows = df.collectAsList();
        assertThat(rows).isNotEmpty();

        long maxOffset = rows.stream()
                .mapToLong(row -> row.getAs("offset"))
                .max().orElse(-1);
        assertThat(maxOffset).isLessThan(35L);

        List<String> values = rows.stream()
                .map(row -> new String((byte[]) row.getAs("value")))
                .toList();
        assertThat(values).anyMatch(v -> v.startsWith("post-"));
    }

    // ---- IT-DATALOSS-007: failOnDataLoss=false no duplicates in batch ----

    @Test
    void batchReadFailOnDataLossFalseNoDuplicates() throws Exception {
        String truncStream = uniqueStreamName();
        deleteStream(truncStream);
        createStreamWithRetention(truncStream, 1000, 500);

        try {
            for (int batch = 0; batch < 10; batch++) {
                publishMessages(truncStream, 50, "trunc-batch" + batch + "-");
                Thread.sleep(500);
            }

            long firstOffset = waitForTruncation(truncStream, 30_000);
            Assumptions.assumeTrue(firstOffset > 0,
                    "Retention truncation did not occur in time");

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
            assertThat(rows).isNotEmpty();

            Set<Long> offsets = rows.stream()
                    .map(row -> (Long) row.getAs("offset"))
                    .collect(Collectors.toSet());
            assertThat(offsets).hasSize(rows.size());

            long minReadOffset = offsets.stream()
                    .mapToLong(Long::longValue).min().orElse(-1);
            assertThat(minReadOffset).isGreaterThanOrEqualTo(firstOffset);
        } finally {
            deleteStream(truncStream);
        }
    }

    @Test
    void batchReadStartingOffsetsEarliestLateBindsAfterTruncation() throws Exception {
        String truncStream = uniqueStreamName();
        deleteStream(truncStream);
        createStreamWithRetention(truncStream, 1000, 500);

        try {
            publishMessages(truncStream, 20, "early-");

            Dataset<Row> df = spark.read()
                    .format("rabbitmq_streams")
                    .option("endpoints", streamEndpoint())
                    .option("stream", truncStream)
                    .option("startingOffsets", "earliest")
                    .option("failOnDataLoss", "false")
                    .option("metadataFields", "")
                    .option("addressResolverClass",
                            "com.rabbitmq.spark.connector.TestAddressResolver")
                    .load();

            for (int batch = 0; batch < 8; batch++) {
                publishMessages(truncStream, 50, "late-" + batch + "-");
                Thread.sleep(400);
            }

            long firstOffset = waitForTruncation(truncStream, 30_000);
            Assumptions.assumeTrue(firstOffset > 0,
                    "Retention truncation did not occur in time");

            List<Row> rows = df.collectAsList();
            assertThat(rows).isNotEmpty();
            long minOffset = rows.stream()
                    .mapToLong(row -> row.getAs("offset"))
                    .min()
                    .orElse(-1L);
            assertThat(minOffset).isGreaterThanOrEqualTo(firstOffset);
        } finally {
            deleteStream(truncStream);
        }
    }

    // ---- IT-OPT-001: case-insensitive options ----

    @Test
    void batchReadAcceptsCaseInsensitiveOptions() {
        publishMessages(stream, 10);

        Dataset<Row> df = spark.read()
                .format("rabbitmq_streams")
                .option("EnDpOiNtS", streamEndpoint())
                .option("StReAm", stream)
                .option("StArTiNgOfFsEtS", "earliest")
                .option("MeTaDaTaFiElDs", "")
                .option("AdDrEsSReSoLvErClAsS",
                        "com.rabbitmq.spark.connector.TestAddressResolver")
                .load();

        assertThat(df.collectAsList()).hasSize(10);
    }

    @Test
    void batchReadRejectsPerStreamJsonStartingOffset() {
        publishMessages(stream, 10);

        assertThatThrownBy(() ->
                spark.read()
                        .format("rabbitmq_streams")
                        .option("endpoints", streamEndpoint())
                        .option("stream", stream)
                        .option("startingOffsets", "offset")
                        .option("startingOffset", "{\"" + stream + "\":5}")
                        .option("metadataFields", "")
                        .option("addressResolverClass",
                                "com.rabbitmq.spark.connector.TestAddressResolver")
                        .load()
                        .collectAsList())
                .hasMessageContaining("'startingOffset' must be a valid long");
    }

    // ---- IT-SCHEMA-002: DataFrame reuse in batch query ----

    @Test
    void batchReadDataFrameReuseUnion() {
        publishMessages(stream, 12, "reuse-");

        Dataset<Row> df = spark.read()
                .format("rabbitmq_streams")
                .option("endpoints", streamEndpoint())
                .option("stream", stream)
                .option("startingOffsets", "earliest")
                .option("metadataFields", "")
                .option("addressResolverClass",
                        "com.rabbitmq.spark.connector.TestAddressResolver")
                .load();

        Dataset<Row> union = df.union(df);
        assertThat(union.count()).isEqualTo(24L);
    }

    // ---- IT-FILTER-004: filterWarningOnMismatch log behavior ----

    @Test
    void batchReadFilterWarningOnMismatchLogs() {
        publishMessagesWithFilterValue(stream, 10, "drop-", "alpha");

        LoggerContext context = (LoggerContext) LogManager.getContext(false);
        Configuration configuration = context.getConfiguration();
        LoggerConfig loggerConfig = configuration.getLoggerConfig(
                "com.rabbitmq.spark.connector.RabbitMQPartitionReader");
        MemoryAppender appender = new MemoryAppender("filterMismatchAppender");
        appender.start();
        loggerConfig.addAppender(appender, Level.WARN, null);
        context.updateLoggers();

        try {
            Dataset<Row> df = spark.read()
                    .format("rabbitmq_streams")
                    .option("endpoints", streamEndpoint())
                    .option("stream", stream)
                    .option("startingOffsets", "earliest")
                    .option("filterPostFilterClass",
                            "com.rabbitmq.spark.connector.TestPostFilter")
                    .option("filterWarningOnMismatch", "true")
                    .option("pollTimeoutMs", "200")
                    .option("maxWaitMs", "10000")
                    .option("metadataFields", "")
                    .option("addressResolverClass",
                            "com.rabbitmq.spark.connector.TestAddressResolver")
                    .load();

            List<Row> rows = df.collectAsList();
            assertThat(rows).isEmpty();

            List<LogEvent> warnings = appender.getEvents().stream()
                    .filter(event -> event.getMessage().getFormattedMessage()
                            .contains("Post-filter dropped message"))
                    .toList();
            assertThat(warnings).isNotEmpty();

            appender.clear();

            Dataset<Row> dfNoWarn = spark.read()
                    .format("rabbitmq_streams")
                    .option("endpoints", streamEndpoint())
                    .option("stream", stream)
                    .option("startingOffsets", "earliest")
                    .option("filterPostFilterClass",
                            "com.rabbitmq.spark.connector.TestPostFilter")
                    .option("filterWarningOnMismatch", "false")
                    .option("pollTimeoutMs", "200")
                    .option("maxWaitMs", "10000")
                    .option("metadataFields", "")
                    .option("addressResolverClass",
                            "com.rabbitmq.spark.connector.TestAddressResolver")
                    .load();

            assertThat(dfNoWarn.collectAsList()).isEmpty();

            List<LogEvent> warningsDisabled = appender.getEvents().stream()
                    .filter(event -> event.getMessage().getFormattedMessage()
                            .contains("Post-filter dropped message"))
                    .toList();
            assertThat(warningsDisabled).isEmpty();
        } finally {
            loggerConfig.removeAppender("filterMismatchAppender");
            appender.stop();
            context.updateLoggers();
        }
    }

    private static Set<String> collectMetricNames(SparkPlan root) {
        LinkedHashSet<String> metricNames = new LinkedHashSet<>();
        scala.collection.Iterator<SparkPlan> nodeIterator = root.collectLeaves().iterator();
        while (nodeIterator.hasNext()) {
            SparkPlan node = nodeIterator.next();
            scala.collection.Iterator<org.apache.spark.sql.execution.metric.SQLMetric> metricIterator =
                    node.metrics().valuesIterator();
            while (metricIterator.hasNext()) {
                scala.Option<String> name = metricIterator.next().name();
                if (name != null && name.isDefined()) {
                    metricNames.add(name.get());
                }
            }
        }
        return metricNames;
    }

    private static boolean isTransientTlsHandshakeFailure(Throwable throwable) {
        Throwable cursor = throwable;
        while (cursor != null) {
            String message = cursor.getMessage();
            if (message != null) {
                String lower = message.toLowerCase(java.util.Locale.ROOT);
                if (lower.contains("handshake")
                        || lower.contains("closedchannel")
                        || lower.contains("failed to initialize consumer")
                        || lower.contains("error when establishing stream connection")) {
                    return true;
                }
            }
            cursor = cursor.getCause();
        }
        return false;
    }

    private static final class MemoryAppender extends AbstractAppender {
        private final List<LogEvent> events = new CopyOnWriteArrayList<>();

        private MemoryAppender(String name) {
            super(name, null, null, false, null);
        }

        @Override
        public void append(LogEvent event) {
            events.add(event.toImmutable());
        }

        private List<LogEvent> getEvents() {
            return events;
        }

        private void clear() {
            events.clear();
        }
    }
}
