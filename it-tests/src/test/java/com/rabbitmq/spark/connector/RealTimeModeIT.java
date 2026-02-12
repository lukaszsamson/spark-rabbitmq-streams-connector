package com.rabbitmq.spark.connector;

import org.apache.spark.SparkIllegalArgumentException;
import org.apache.spark.sql.ForeachWriter;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Integration tests for SupportsRealTimeMode (Spark 4.1+).
 *
 * <p>These tests use {@link Trigger#RealTime(String)} with a short batch duration
 * and a {@code foreach} sink that collects rows into a static list.
 * Tests are skipped on Spark versions that do not support real-time mode.
 */
class RealTimeModeIT extends AbstractRabbitMQIT {

    /** Static collector — safe in local mode where all threads share the same JVM. */
    static final CopyOnWriteArrayList<Row> COLLECTED_ROWS = new CopyOnWriteArrayList<>();

    private String sourceStream;
    private Path checkpointDir;

    private static final String MIN_BATCH_DURATION_KEY =
            "spark.sql.streaming.realTimeMode.minBatchDuration";

    @BeforeEach
    void setUp() throws Exception {
        Assumptions.assumeTrue(isRealTimeTriggerAvailable(),
                "Trigger.RealTime is not available in this Spark version");

        // Lower the minimum batch duration so we can use short triggers in tests
        spark.conf().set(MIN_BATCH_DURATION_KEY, "1s");

        sourceStream = uniqueStreamName();
        createStream(sourceStream);
        checkpointDir = Files.createTempDirectory("spark-rt-checkpoint-");
        COLLECTED_ROWS.clear();
    }

    @AfterEach
    void tearDown() {
        if (sourceStream != null) {
            deleteStream(sourceStream);
        }
        COLLECTED_ROWS.clear();

        // Restore default minBatchDuration
        spark.conf().unset(MIN_BATCH_DURATION_KEY);
    }

    @Test
    void realTimeModeReadsPrePublishedMessages() throws Exception {
        publishMessages(sourceStream, 50);
        Thread.sleep(2000);

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
                .outputMode("update")
                .foreach(new RowCollector())
                .option("checkpointLocation", checkpointDir.toString())
                .trigger(createRealTimeTrigger("2 seconds"))
                .start();

        try {
            awaitAtLeastRows(50, 30_000);
        } finally {
            if (query.isActive()) {
                query.stop();
            }
        }

        Set<String> payloads = payloadsWithPrefix("msg-");
        assertThat(payloads).hasSize(50);
    }

    @Test
    void realTimeModeReadsLiveMessages() throws Exception {
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
                .outputMode("update")
                .foreach(new RowCollector())
                .option("checkpointLocation", checkpointDir.toString())
                .trigger(createRealTimeTrigger("2 seconds"))
                .start();

        try {
            // Wait for query to start, then publish messages
            Thread.sleep(3000);
            publishMessages(sourceStream, 30);

            // Wait for messages to be processed
            awaitAtLeastRows(30, 30_000);
        } finally {
            query.stop();
        }

        Set<String> payloads = payloadsWithPrefix("msg-");
        assertThat(payloads).hasSize(30);
    }

    @Test
    void realTimeModeEmptyStream() throws Exception {
        // No messages published — query should start and stop cleanly
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
                .outputMode("update")
                .foreach(new RowCollector())
                .option("checkpointLocation", checkpointDir.toString())
                .trigger(createRealTimeTrigger("2 seconds"))
                .start();

        try {
            // Let the query run briefly with an empty stream
            Thread.sleep(5000);
        } finally {
            query.stop();
        }

        // No errors and no data
        assertThat(COLLECTED_ROWS).isEmpty();
    }

    @Test
    void realTimeModeServerSideOffsetTracking() throws Exception {
        String consumerName = "rt-consumer-" + System.currentTimeMillis();
        publishMessages(sourceStream, 40);
        Thread.sleep(2000);

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
                .outputMode("update")
                .foreach(new RowCollector())
                .option("checkpointLocation", checkpointDir.toString())
                .trigger(createRealTimeTrigger("2 seconds"))
                .start();

        try {
            long deadline = System.currentTimeMillis() + 30_000;
            while (COLLECTED_ROWS.size() < 40 && System.currentTimeMillis() < deadline) {
                Thread.sleep(500);
            }
        } finally {
            query.stop();
        }

        assertThat(COLLECTED_ROWS.size()).isGreaterThanOrEqualTo(40);

        // Verify broker stored the offset
        long storedOffset = queryStoredOffset(consumerName, sourceStream);
        assertThat(storedOffset).isGreaterThanOrEqualTo(0);
    }

    @Test
    void realTimeModeCheckpointResume() throws Exception {
        // Phase 1: Publish and consume initial messages
        publishMessages(sourceStream, 30, "phase1-");
        Thread.sleep(2000);

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
                .outputMode("update")
                .foreach(new RowCollector())
                .option("checkpointLocation", checkpointDir.toString())
                .trigger(createRealTimeTrigger("2 seconds"))
                .start();

        try {
            long deadline = System.currentTimeMillis() + 30_000;
            while (COLLECTED_ROWS.size() < 30 && System.currentTimeMillis() < deadline) {
                Thread.sleep(500);
            }
        } finally {
            query1.stop();
        }

        int phase1Count = COLLECTED_ROWS.size();
        assertThat(payloadsWithPrefix("phase1-")).hasSize(30);
        assertThat(phase1Count).isEqualTo(30);

        // Phase 2: Publish more, resume from checkpoint
        publishMessages(sourceStream, 20, "phase2-");
        Thread.sleep(2000);
        COLLECTED_ROWS.clear();

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
                .outputMode("update")
                .foreach(new RowCollector())
                .option("checkpointLocation", checkpointDir.toString())
                .trigger(createRealTimeTrigger("2 seconds"))
                .start();

        try {
            long deadline = System.currentTimeMillis() + 30_000;
            while (COLLECTED_ROWS.size() < 20 && System.currentTimeMillis() < deadline) {
                Thread.sleep(500);
            }
        } finally {
            query2.stop();
        }

        // Phase 2 should only have the new messages (no duplicates from phase 1)
        assertThat(payloadsWithPrefix("phase2-")).hasSize(20);
        assertThat(payloadsWithPrefix("phase1-")).isEmpty();
    }

    @Test
    void realTimeModeWithSuperstream() throws Exception {
        String superStream = "rt-super-" + System.currentTimeMillis();
        createSuperStream(superStream, 3);
        try {
            // Publish to each partition via the superstream routing
            try (var producer = testEnv.producerBuilder()
                    .superStream(superStream)
                    .routing(msg -> msg.getProperties().getMessageId().toString())
                    .producerBuilder()
                    .build()) {
                java.util.concurrent.CountDownLatch latch =
                        new java.util.concurrent.CountDownLatch(30);
                for (int i = 0; i < 30; i++) {
                    byte[] body = ("ss-msg-" + i).getBytes(java.nio.charset.StandardCharsets.UTF_8);
                    producer.send(
                            producer.messageBuilder()
                                    .properties().messageId("key-" + i).messageBuilder()
                                    .addData(body)
                                    .build(),
                            status -> latch.countDown());
                }
                assertThat(latch.await(30, TimeUnit.SECONDS)).isTrue();
            }
            Thread.sleep(2000);

            Path ssCheckpointDir = Files.createTempDirectory("spark-rt-ss-checkpoint-");

            StreamingQuery query = spark.readStream()
                    .format("rabbitmq_streams")
                    .option("endpoints", streamEndpoint())
                    .option("superstream", superStream)
                    .option("startingOffsets", "earliest")
                    .option("metadataFields", "")
                    .option("addressResolverClass",
                            "com.rabbitmq.spark.connector.TestAddressResolver")
                    .load()
                    .writeStream()
                    .outputMode("update")
                    .foreach(new RowCollector())
                    .option("checkpointLocation", ssCheckpointDir.toString())
                    .trigger(createRealTimeTrigger("2 seconds"))
                    .start();

            try {
                long deadline = System.currentTimeMillis() + 30_000;
                while (COLLECTED_ROWS.size() < 30 && System.currentTimeMillis() < deadline) {
                    Thread.sleep(500);
                }
            } finally {
                query.stop();
            }

            assertThat(payloadsWithPrefix("ss-msg-")).hasSize(30);
        } finally {
            deleteSuperStream(superStream);
        }
    }

    @Test
    void realTimeModeStartingOffsetsLatestSkipsHistorical() throws Exception {
        publishMessages(sourceStream, 20, "old-");
        Thread.sleep(2000);

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
                .outputMode("update")
                .foreach(new RowCollector())
                .option("checkpointLocation", checkpointDir.toString())
                .trigger(createRealTimeTrigger("2 seconds"))
                .start();

        try {
            Thread.sleep(3000);
            publishMessages(sourceStream, 15, "new-");
            awaitAtLeastRows(15, 30_000);
        } finally {
            query.stop();
        }

        assertThat(payloadsWithPrefix("new-")).hasSize(15);
        assertThat(payloadsWithPrefix("old-")).isEmpty();
    }

    @Test
    void realTimeModeStartingOffsetsTimestampSkipsOlderMessages() throws Exception {
        publishMessages(sourceStream, 10, "before-");
        Thread.sleep(1200);
        long boundaryTimestampMs = System.currentTimeMillis();
        Thread.sleep(1200);
        publishMessages(sourceStream, 12, "after-");

        StreamingQuery query = spark.readStream()
                .format("rabbitmq_streams")
                .option("endpoints", streamEndpoint())
                .option("stream", sourceStream)
                .option("startingOffsets", "timestamp")
                .option("startingTimestamp", String.valueOf(boundaryTimestampMs))
                .option("metadataFields", "")
                .option("addressResolverClass",
                        "com.rabbitmq.spark.connector.TestAddressResolver")
                .load()
                .writeStream()
                .outputMode("update")
                .foreach(new RowCollector())
                .option("checkpointLocation", checkpointDir.toString())
                .trigger(createRealTimeTrigger("2 seconds"))
                .start();

        try {
            awaitAtLeastRows(12, 30_000);
        } finally {
            query.stop();
        }

        assertThat(payloadsWithPrefix("after-")).hasSize(12);
        assertThat(payloadsWithPrefix("before-")).isEmpty();
    }

    @Test
    void realTimeModeConnectorSinkRequiresAllowlistOverride() throws Exception {
        String sinkStream = uniqueStreamName();
        createStream(sinkStream);
        try {
            publishMessages(sourceStream, 1, "sink-src-");
            Thread.sleep(2000);
            assertThatThrownBy(() -> spark.readStream()
                    .format("rabbitmq_streams")
                    .option("endpoints", streamEndpoint())
                    .option("stream", sourceStream)
                    .option("startingOffsets", "earliest")
                    .option("metadataFields", "")
                    .option("addressResolverClass",
                            "com.rabbitmq.spark.connector.TestAddressResolver")
                    .load()
                    .writeStream()
                    .format("rabbitmq_streams")
                    .outputMode("update")
                    .option("endpoints", streamEndpoint())
                    .option("stream", sinkStream)
                    .option("checkpointLocation",
                            Files.createTempDirectory("spark-rt-sink-checkpoint-").toString())
                    .option("addressResolverClass",
                            "com.rabbitmq.spark.connector.TestAddressResolver")
                    .trigger(createRealTimeTrigger("2 seconds"))
                    .start())
                    .isInstanceOf(SparkIllegalArgumentException.class)
                    .hasMessageContaining("sink allowlist");
        } finally {
            deleteStream(sinkStream);
        }
    }

    // ---- Helpers ----

    /**
     * Check if Trigger.RealTime is available in the current Spark version.
     */
    private static boolean isRealTimeTriggerAvailable() {
        try {
            Trigger.class.getMethod("RealTime", String.class);
            return true;
        } catch (NoSuchMethodException e) {
            return false;
        }
    }

    /**
     * Create a Trigger.RealTime via reflection so the class compiles against
     * all Spark versions.
     */
    private static Trigger createRealTimeTrigger(String duration) {
        try {
            Method m = Trigger.class.getMethod("RealTime", String.class);
            return (Trigger) m.invoke(null, duration);
        } catch (Exception e) {
            throw new RuntimeException("Failed to create RealTime trigger", e);
        }
    }

    /**
     * ForeachWriter that collects rows into the static COLLECTED_ROWS list.
     * Must be serializable — uses static reference to the collector.
     */
    public static class RowCollector extends ForeachWriter<Row> {
        @Override
        public boolean open(long partitionId, long epochId) {
            return true;
        }

        @Override
        public void process(Row row) {
            COLLECTED_ROWS.add(row);
        }

        @Override
        public void close(Throwable errorOrNull) {
        }
    }

    private static void awaitAtLeastRows(int expectedMinRows, long timeoutMs) throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (COLLECTED_ROWS.size() < expectedMinRows && System.currentTimeMillis() < deadline) {
            Thread.sleep(200);
        }
    }

    private static Set<String> payloadsWithPrefix(String prefix) {
        Set<String> payloads = new HashSet<>();
        for (Row row : COLLECTED_ROWS) {
            byte[] value = row.getAs("value");
            String payload = new String(value, StandardCharsets.UTF_8);
            if (payload.startsWith(prefix)) {
                payloads.add(payload);
            }
        }
        return payloads;
    }
}
