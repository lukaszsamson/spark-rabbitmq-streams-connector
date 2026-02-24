package io.github.lukaszsamson.spark.rabbitmq;

import com.rabbitmq.stream.Address;
import com.rabbitmq.stream.Environment;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Reproduction tests for active bugs tracked in BUGS.md.
 */
class BugsReproIT extends AbstractRabbitMQIT {

    private String stream;

    @BeforeEach
    void setUp() {
        stream = uniqueStreamName();
        createStream(stream);
    }

    @AfterEach
    void tearDown() {
        if (stream != null) {
            deleteStream(stream);
        }
    }

    @Test
    void freshConsumerLatestWithServerTrackingStoresValidOffsetAndRestarts() throws Exception {
        deleteStream(stream);
        createStreamWithRetention(stream, 24_000, 2_000);

        for (int i = 0; i < 30; i++) {
            publishMessages(stream, 80, "warm-" + i + "-");
            Thread.sleep(120);
        }

        long firstAvailable = waitForTruncation(stream, 30_000);
        Assumptions.assumeTrue(firstAvailable > 10,
                "Retention truncation did not produce a large first offset");

        String consumerName = "bug-repro-" + UUID.randomUUID().toString().substring(0, 8);

        Path out1 = Files.createTempDirectory("spark-bug1-out1-");
        Path cp1 = Files.createTempDirectory("spark-bug1-cp1-");

        StreamingQuery q1 = spark.readStream()
                .format("rabbitmq_streams")
                .option("endpoints", streamEndpoint())
                .option("stream", stream)
                .option("startingOffsets", "latest")
                .option("consumerName", consumerName)
                .option("serverSideOffsetTracking", "true")
                .option("maxRecordsPerTrigger", "100")
                .option("metadataFields", "")
                .option("addressResolverClass",
                        "io.github.lukaszsamson.spark.rabbitmq.TestAddressResolver")
                .load()
                .writeStream()
                .format("parquet")
                .option("path", out1.toString())
                .option("checkpointLocation", cp1.toString())
                .trigger(Trigger.ProcessingTime("2 seconds"))
                .start();

        publishMessagesAsync(stream, 200, "live-1-", 800L);
        long phase1Count = waitForParquetCountAtLeast(out1, 10L, 20_000L);

        var phase1Error = q1.exception();
        q1.stop();

        assertThat(phase1Error.isDefined())
                .as("first run should not fail")
                .isFalse();
        assertThat(phase1Count)
                .as("first run should consume live messages")
                .isGreaterThan(0L);

        long storedOffset = queryStoredOffset(consumerName, stream);
        assertThat(storedOffset)
                .as("stored offset must not jump to sentinel values below retention floor")
                .isGreaterThanOrEqualTo(firstAvailable);

        Path out2 = Files.createTempDirectory("spark-bug1-out2-");
        Path cp2 = Files.createTempDirectory("spark-bug1-cp2-");

        StreamingQuery q2 = spark.readStream()
                .format("rabbitmq_streams")
                .option("endpoints", streamEndpoint())
                .option("stream", stream)
                .option("startingOffsets", "latest")
                .option("consumerName", consumerName)
                .option("serverSideOffsetTracking", "true")
                .option("failOnDataLoss", "true")
                .option("maxRecordsPerTrigger", "100")
                .option("metadataFields", "")
                .option("addressResolverClass",
                        "io.github.lukaszsamson.spark.rabbitmq.TestAddressResolver")
                .load()
                .writeStream()
                .format("parquet")
                .option("path", out2.toString())
                .option("checkpointLocation", cp2.toString())
                .trigger(Trigger.ProcessingTime("2 seconds"))
                .start();

        publishMessagesAsync(stream, 200, "live-2-", 800L);
        long phase2Count = waitForParquetCountAtLeast(out2, 10L, 20_000L);

        var phase2Error = q2.exception();
        q2.stop();

        assertThat(phase2Error.isDefined())
                .as("restart with same consumer must not fail with stale retained offset")
                .isFalse();
        assertThat(phase2Count)
                .as("restart should continue consuming live data")
                .isGreaterThan(0L);
    }

    @Test
    void failOnDataLossFalseWithStaleStoredOffsetStillDeliversData() throws Exception {
        deleteStream(stream);
        createStreamWithRetention(stream, 24_000, 2_000);

        for (int i = 0; i < 25; i++) {
            publishMessages(stream, 80, "warm-" + i + "-");
            Thread.sleep(120);
        }

        long firstAvailable = waitForTruncation(stream, 30_000);
        Assumptions.assumeTrue(firstAvailable > 10,
                "Retention truncation did not produce a large first offset");

        String consumerName = "bug-stale-" + UUID.randomUUID().toString().substring(0, 8);
        storeOffset(consumerName, stream, 1L);

        Path outputDir = Files.createTempDirectory("spark-bug2-out-");
        Path checkpointDir = Files.createTempDirectory("spark-bug2-cp-");

        StreamingQuery query = spark.readStream()
                .format("rabbitmq_streams")
                .option("endpoints", streamEndpoint())
                .option("stream", stream)
                .option("startingOffsets", "latest")
                .option("consumerName", consumerName)
                .option("serverSideOffsetTracking", "true")
                .option("failOnDataLoss", "false")
                .option("maxRecordsPerTrigger", "100")
                .option("metadataFields", "")
                .option("addressResolverClass",
                        "io.github.lukaszsamson.spark.rabbitmq.TestAddressResolver")
                .load()
                .writeStream()
                .format("parquet")
                .option("path", outputDir.toString())
                .option("checkpointLocation", checkpointDir.toString())
                .trigger(Trigger.ProcessingTime("2 seconds"))
                .start();

        publishMessagesAsync(stream, 220, "live-fdl-", 1_000L);
        long count = waitForParquetCountAtLeast(outputDir, 10L, 25_000L);

        var error = query.exception();
        query.stop();

        assertThat(error.isDefined())
                .as("failOnDataLoss=false should not fail on stale stored offsets")
                .isFalse();
        assertThat(count)
                .as("query should advance to first available and consume new data")
                .isGreaterThan(0L);
    }

    @Test
    void shortMaxWaitOnActiveStreamStillProducesRows() throws Exception {
        Path outputDir = Files.createTempDirectory("spark-bug3-out-");
        Path checkpointDir = Files.createTempDirectory("spark-bug3-cp-");

        StreamingQuery query = spark.readStream()
                .format("rabbitmq_streams")
                .option("endpoints", streamEndpoint())
                .option("stream", stream)
                .option("startingOffsets", "latest")
                .option("maxRecordsPerTrigger", "100")
                .option("maxWaitMs", "10000")
                .option("metadataFields", "")
                .option("addressResolverClass",
                        "io.github.lukaszsamson.spark.rabbitmq.TestAddressResolver")
                .load()
                .writeStream()
                .format("parquet")
                .option("path", outputDir.toString())
                .option("checkpointLocation", checkpointDir.toString())
                .trigger(Trigger.ProcessingTime("2 seconds"))
                .start();

        publishMessagesAsync(stream, 900, "live-timeout-", 800L);

        long count = waitForParquetCountAtLeast(outputDir, 10L, 30_000L);
        var error = query.exception();
        query.stop();

        assertThat(error.isDefined())
                .as("short maxWait on active stream should not terminate query with error")
                .isFalse();
        assertThat(count)
                .as("active stream should produce rows with maxWaitMs=10000")
                .isGreaterThan(0L);
    }

    @Test
    void availableNowWithCheckpointTerminatesAndSecondRunDoesNotDuplicate() throws Exception {
        for (int i = 0; i < 8; i++) {
            publishMessages(stream, 50, "avnow-" + i + "-");
            Thread.sleep(100);
        }

        long startTs = System.currentTimeMillis() - 45_000L;
        Path outputDir = Files.createTempDirectory("spark-bug7-out-");
        Path checkpointDir = Files.createTempDirectory("spark-bug7-cp-");

        var baseWriter = spark.readStream()
                .format("rabbitmq_streams")
                .option("endpoints", streamEndpoint())
                .option("stream", stream)
                .option("startingOffsets", "timestamp")
                .option("startingTimestamp", String.valueOf(startTs))
                .option("maxRecordsPerTrigger", "500")
                .option("metadataFields", "")
                .option("addressResolverClass",
                        "io.github.lukaszsamson.spark.rabbitmq.TestAddressResolver")
                .load()
                .selectExpr("CAST(offset AS STRING) AS offset", "CAST(value AS STRING) AS value");

        StreamingQuery firstRun = baseWriter.writeStream()
                .format("csv")
                .option("header", "true")
                .option("path", outputDir.toString())
                .option("checkpointLocation", checkpointDir.toString())
                .trigger(Trigger.AvailableNow())
                .start();

        boolean firstTerminated = firstRun.awaitTermination(120_000L);
        assertThat(firstTerminated).as("first availableNow run should terminate").isTrue();
        assertThat(firstRun.isActive()).isFalse();

        long firstCount = spark.read().option("header", "true")
                .csv(outputDir.toString())
                .count();
        assertThat(firstCount).isGreaterThan(0L);

        StreamingQuery secondRun = baseWriter.writeStream()
                .format("csv")
                .option("header", "true")
                .option("path", outputDir.toString())
                .option("checkpointLocation", checkpointDir.toString())
                .trigger(Trigger.AvailableNow())
                .start();

        boolean secondTerminated = secondRun.awaitTermination(120_000L);
        assertThat(secondTerminated).as("second availableNow run should terminate").isTrue();
        assertThat(secondRun.isActive()).isFalse();

        long secondCount = spark.read().option("header", "true")
                .csv(outputDir.toString())
                .count();
        assertThat(secondCount).as("second run must not duplicate output").isEqualTo(firstCount);
    }

    private long waitForParquetCountAtLeast(Path outputDir, long minimum, long timeoutMs)
            throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        long lastCount = 0L;
        while (System.currentTimeMillis() < deadline) {
            lastCount = readParquetCount(outputDir);
            if (lastCount >= minimum) {
                return lastCount;
            }
            Thread.sleep(200L);
        }
        return readParquetCount(outputDir);
    }

    private long readParquetCount(Path outputDir) {
        if (!hasParquetData(outputDir)) {
            return 0L;
        }
        return spark.read().schema(MINIMAL_OUTPUT_SCHEMA).parquet(outputDir.toString()).count();
    }

    private boolean hasParquetData(Path outputDir) {
        try (var paths = Files.walk(outputDir)) {
            return paths.anyMatch(path -> Files.isRegularFile(path)
                    && path.getFileName().toString().endsWith(".parquet"));
        } catch (IOException e) {
            throw new RuntimeException("Failed to inspect output directory " + outputDir, e);
        }
    }

    private void storeOffset(String consumerName, String streamName, long offset) {
        String host = RABBIT.getHost();
        int port = RABBIT.getMappedPort(STREAM_PORT);
        try (Environment env = Environment.builder()
                .uri("rabbitmq-stream://guest:guest@" + host + ":" + port)
                .addressResolver(addr -> new Address(host, port))
                .build()) {
            env.storeOffset(consumerName, streamName, offset);
        }
    }
}
