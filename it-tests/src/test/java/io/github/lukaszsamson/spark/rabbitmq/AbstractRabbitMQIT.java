package io.github.lukaszsamson.spark.rabbitmq;

import com.rabbitmq.stream.Address;
import com.rabbitmq.stream.Consumer;
import com.rabbitmq.stream.Environment;
import com.rabbitmq.stream.Message;
import com.rabbitmq.stream.OffsetSpecification;
import com.rabbitmq.stream.Producer;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.RabbitMQContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import com.rabbitmq.stream.NoOffsetException;

import java.nio.charset.StandardCharsets;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Base class for RabbitMQ Streams integration tests.
 *
 * <p>Provides a shared Testcontainer running RabbitMQ with the stream
 * plugin enabled, a shared Spark session in local mode, and helper
 * methods for stream management and message publishing/consuming.
 */
@Testcontainers(disabledWithoutDocker = true)
abstract class AbstractRabbitMQIT {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractRabbitMQIT.class);

    static final int STREAM_PORT = 5552;
    static final int STREAM_TLS_PORT = 5551;
    static final String ERLANG_COOKIE = "SPARKLINGRABBITINTEGRATIONCOOKIE1234567890";
    private static final int ENV_BUILD_ATTEMPTS = 8;
    private static final long ENV_BUILD_RETRY_DELAY_MS = 1500;

    /**
     * Shared RabbitMQ container with stream plugin enabled.
     * Static @Container: started once per test class, shared across test methods.
     */
    @Container
    static final RabbitMQContainer RABBIT = new RabbitMQContainer(
            DockerImageName.parse("rabbitmq:4.0-management"))
            .withTmpFs(Map.of("/var/lib/rabbitmq", "rw,noexec,nosuid,size=512m"))
            .withCreateContainerCmdModifier(cmd -> cmd.withEntrypoint(
                    "bash",
                    "-lc",
                    "set -euo pipefail; "
                            + "echo '" + ERLANG_COOKIE + "' > /var/lib/rabbitmq/.erlang.cookie; "
                            + "chmod 600 /var/lib/rabbitmq/.erlang.cookie; "
                            + "chown rabbitmq:rabbitmq /var/lib/rabbitmq/.erlang.cookie; "
                            + "exec /usr/local/bin/docker-entrypoint.sh rabbitmq-server"))
            .withPluginsEnabled("rabbitmq_stream")
            .withCopyFileToContainer(
                    MountableFile.forClasspathResource("rabbitmq/rabbitmq.conf"),
                    "/etc/rabbitmq/rabbitmq.conf")
            .withCopyFileToContainer(
                    MountableFile.forClasspathResource("tls/rabbitmq-key.pem"),
                    "/etc/rabbitmq/certs/rabbitmq-key.pem")
            .withCopyFileToContainer(
                    MountableFile.forClasspathResource("tls/rabbitmq-cert.pem"),
                    "/etc/rabbitmq/certs/rabbitmq-cert.pem")
            .withCopyFileToContainer(
                    MountableFile.forClasspathResource("tls/ca-cert.pem"),
                    "/etc/rabbitmq/certs/ca-cert.pem")
            .withEnv("RABBITMQ_CONFIG_FILE", "/etc/rabbitmq/rabbitmq")
            .withEnv("RABBITMQ_SERVER_ADDITIONAL_ERL_ARGS",
                    "-rabbitmq_stream advertised_tls_port " + STREAM_TLS_PORT)
            .withExposedPorts(STREAM_PORT, STREAM_TLS_PORT);

    /** Shared Spark session — local mode, reused across all IT classes. */
    static SparkSession spark;

    /** Unshaded RabbitMQ Environment for test setup/verification. */
    static Environment testEnv;

    @BeforeAll
    static void setupShared() {
        if (!RABBIT.isRunning()) {
            RABBIT.start();
        }

        waitForRabbitMqStartup();

        // Set system properties for the TestAddressResolver used by the connector
        System.setProperty("rabbitmq.test.host", RABBIT.getHost());
        System.setProperty("rabbitmq.test.port",
                String.valueOf(RABBIT.getMappedPort(STREAM_PORT)));
        System.setProperty("rabbitmq.test.tls.port",
                String.valueOf(RABBIT.getMappedPort(STREAM_TLS_PORT)));

        if (spark == null) {
            spark = SparkSession.builder()
                    .master("local[4]")
                    .appName("rabbitmq-streams-it")
                    .config("spark.ui.enabled", "false")
                    .config("spark.sql.shuffle.partitions", "4")
                    .config("spark.driver.host", "127.0.0.1")
                    .config("spark.driver.bindAddress", "127.0.0.1")
                    .config("spark.local.ip", "127.0.0.1")
                    .config("spark.sql.codegen.wholeStage", "false")
                    .getOrCreate();
        }

        ensureTestEnvironmentReady();
    }

    @AfterAll
    static void teardownShared() {
        if (testEnv != null) {
            try {
                testEnv.close();
            } catch (Exception e) {
                LOG.debug("Error closing test environment", e);
            }
            testEnv = null;
        }
    }

    private static void waitForRabbitMqStartup() {
        String host = RABBIT.getHost();
        int port = RABBIT.getMappedPort(STREAM_PORT);
        RuntimeException lastFailure = null;
        for (int attempt = 1; attempt <= ENV_BUILD_ATTEMPTS; attempt++) {
            try {
                var await = RABBIT.execInContainer("rabbitmqctl", "await_startup");
                if (await.getExitCode() != 0) {
                    // If app is stopped, try to bring it up for this class.
                    RABBIT.execInContainer("rabbitmqctl", "start_app");
                    await = RABBIT.execInContainer("rabbitmqctl", "await_startup");
                }
                if (await.getExitCode() != 0) {
                    throw new RuntimeException("rabbitmqctl await_startup failed: " + await.getStderr());
                }

                try (Socket socket = new Socket()) {
                    socket.connect(new InetSocketAddress(host, port), 2000);
                }
                return;
            } catch (Exception e) {
                lastFailure = new RuntimeException(
                        "RabbitMQ stream endpoint not ready on attempt " + attempt + "/" + ENV_BUILD_ATTEMPTS, e);
                LOG.warn(lastFailure.getMessage());
                sleepQuietly(ENV_BUILD_RETRY_DELAY_MS);
            }
        }
        throw new RuntimeException("RabbitMQ did not become ready for stream tests", lastFailure);
    }

    private static void ensureTestEnvironmentReady() {
        if (testEnv != null) {
            return;
        }
        String host = RABBIT.getHost();
        int port = RABBIT.getMappedPort(STREAM_PORT);
        RuntimeException lastFailure = null;
        for (int attempt = 1; attempt <= ENV_BUILD_ATTEMPTS; attempt++) {
            try {
                testEnv = Environment.builder()
                        .uri("rabbitmq-stream://guest:guest@" + host + ":" + port)
                        .addressResolver(addr -> new Address(host, port))
                        .build();
                return;
            } catch (Exception e) {
                lastFailure = new RuntimeException(
                        "Failed to create RabbitMQ stream Environment on attempt "
                                + attempt + "/" + ENV_BUILD_ATTEMPTS, e);
                LOG.warn(lastFailure.getMessage());
                closeTestEnvironmentQuietly();
                sleepQuietly(ENV_BUILD_RETRY_DELAY_MS);
            }
        }
        throw new RuntimeException("Unable to create RabbitMQ stream Environment", lastFailure);
    }

    private static Environment getHealthyTestEnvironment() {
        ensureTestEnvironmentReady();
        try {
            // Touch the environment; closed instances throw IllegalStateException here.
            testEnv.streamCreator();
            return testEnv;
        } catch (IllegalStateException closed) {
            LOG.warn("Test environment is closed; recreating it");
            closeTestEnvironmentQuietly();
            ensureTestEnvironmentReady();
            return testEnv;
        }
    }

    private static void closeTestEnvironmentQuietly() {
        if (testEnv != null) {
            try {
                testEnv.close();
            } catch (Exception e) {
                LOG.debug("Error closing test environment", e);
            } finally {
                testEnv = null;
            }
        }
    }

    private static void sleepQuietly(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted while waiting for RabbitMQ readiness", ie);
        }
    }

    /** Minimal output schema (fixed columns only, no metadata). */
    static final StructType MINIMAL_OUTPUT_SCHEMA = new StructType()
            .add("value", DataTypes.BinaryType)
            .add("stream", DataTypes.StringType)
            .add("offset", DataTypes.LongType)
            .add("chunk_timestamp", DataTypes.TimestampType);

    // ---- Stream management ----

    /** The mapped stream endpoint for connector options. */
    String streamEndpoint() {
        return RABBIT.getHost() + ":" + RABBIT.getMappedPort(STREAM_PORT);
    }

    /** The mapped TLS stream endpoint for connector options. */
    String streamTlsEndpoint() {
        return RABBIT.getHost() + ":" + RABBIT.getMappedPort(STREAM_TLS_PORT);
    }

    /** Generate a unique stream name for a test. */
    String uniqueStreamName() {
        return "test-stream-" + UUID.randomUUID().toString().substring(0, 8);
    }

    /** Create a stream on the broker. */
    void createStream(String name) {
        getHealthyTestEnvironment().streamCreator().stream(name).create();
        LOG.info("Created stream '{}'", name);
    }

    /** Delete a stream from the broker (best-effort). */
    void deleteStream(String name) {
        try {
            getHealthyTestEnvironment().deleteStream(name);
            LOG.info("Deleted stream '{}'", name);
        } catch (Exception e) {
            LOG.debug("Failed to delete stream '{}': {}", name, e.getMessage());
        }
    }

    /** Create a superstream with the given number of partitions via CLI. */
    void createSuperStream(String name, int partitions) {
        try {
            var result = RABBIT.execInContainer(
                    "rabbitmq-streams", "add_super_stream", name,
                    "--partitions", String.valueOf(partitions));
            if (result.getExitCode() != 0) {
                throw new RuntimeException(
                        "Failed to create superstream '" + name + "': " + result.getStderr());
            }
            LOG.info("Created superstream '{}' with {} partitions", name, partitions);
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException("Failed to create superstream '" + name + "'", e);
        }
    }

    /** Create a superstream with explicit retention settings for partition streams. */
    void createSuperStreamWithRetention(String name, int partitions,
                                        long maxLengthBytes, long maxSegmentSizeBytes) {
        try {
            var result = RABBIT.execInContainer(
                    "rabbitmq-streams", "add_super_stream", name,
                    "--partitions", String.valueOf(partitions),
                    "--max-length-bytes", String.valueOf(maxLengthBytes),
                    "--stream-max-segment-size-bytes", String.valueOf(maxSegmentSizeBytes));
            if (result.getExitCode() != 0) {
                throw new RuntimeException(
                        "Failed to create retained superstream '" + name + "': " + result.getStderr());
            }
            LOG.info("Created retained superstream '{}' with {} partitions (maxLengthBytes={}, segmentSize={})",
                    name, partitions, maxLengthBytes, maxSegmentSizeBytes);
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException("Failed to create retained superstream '" + name + "'", e);
        }
    }

    /** Create a superstream with explicit binding keys via CLI. */
    void createSuperStreamWithBindingKeys(String name, String... keys) {
        List<String> args = new ArrayList<>();
        args.add("rabbitmq-streams");
        args.add("add_super_stream");
        args.add(name);
        args.add("--binding-keys");
        args.add(String.join(",", keys));
        try {
            var result = RABBIT.execInContainer(args.toArray(new String[0]));
            if (result.getExitCode() != 0) {
                throw new RuntimeException(
                        "Failed to create superstream with binding keys '" + name
                                + "': " + result.getStderr());
            }
            LOG.info("Created superstream '{}' with binding keys {}", name, String.join(",", keys));
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException("Failed to create superstream with binding keys '" + name + "'", e);
        }
    }

    /** Add partitions to an existing superstream via CLI. */
    void addSuperStreamPartitions(String name, int partitionsToAdd) {
        try {
            var result = RABBIT.execInContainer(
                    "rabbitmq-streams", "add_super_stream", name,
                    "--partitions", String.valueOf(partitionsToAdd));
            if (result.getExitCode() != 0) {
                throw new RuntimeException(
                        "Failed to add partitions to superstream '" + name + "': " + result.getStderr());
            }
            LOG.info("Added {} partitions to superstream '{}'", partitionsToAdd, name);
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException("Failed to add partitions to superstream '" + name + "'", e);
        }
    }

    /** Delete a superstream via CLI (best-effort). */
    void deleteSuperStream(String name) {
        try {
            RABBIT.execInContainer(
                    "rabbitmq-streams", "delete_super_stream", name);
            LOG.info("Deleted superstream '{}'", name);
        } catch (Exception e) {
            LOG.debug("Failed to delete superstream '{}': {}", name, e.getMessage());
        }
    }

    // ---- Broker lifecycle helpers ----

    void stopRabbitMqApp() {
        try {
            var result = RABBIT.execInContainer("rabbitmqctl", "stop_app");
            if (result.getExitCode() != 0) {
                throw new RuntimeException("Failed to stop RabbitMQ app: " + result.getStderr());
            }
            LOG.info("RabbitMQ app stopped");
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException("Failed to stop RabbitMQ app", e);
        }
    }

    void startRabbitMqApp() {
        try {
            var result = RABBIT.execInContainer("rabbitmqctl", "start_app");
            if (result.getExitCode() != 0) {
                throw new RuntimeException("Failed to start RabbitMQ app: " + result.getStderr());
            }
            var await = RABBIT.execInContainer("rabbitmqctl", "await_startup");
            if (await.getExitCode() != 0) {
                throw new RuntimeException("RabbitMQ did not start: " + await.getStderr());
            }
            waitForRabbitMqStartup();
            LOG.info("RabbitMQ app started");
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException("Failed to start RabbitMQ app", e);
        }
    }

    void blockRabbitMqPort() {
        try {
            RABBIT.execInContainer("sh", "-c",
                    "iptables -I INPUT -p tcp --dport " + STREAM_PORT + " -j DROP");
            LOG.info("Blocked RabbitMQ stream port {}", STREAM_PORT);
        } catch (Exception e) {
            throw new RuntimeException("Failed to block RabbitMQ port", e);
        }
    }

    void unblockRabbitMqPort() {
        try {
            RABBIT.execInContainer("sh", "-c",
                    "iptables -D INPUT -p tcp --dport " + STREAM_PORT + " -j DROP");
            LOG.info("Unblocked RabbitMQ stream port {}", STREAM_PORT);
        } catch (Exception e) {
            LOG.debug("Failed to unblock RabbitMQ port: {}", e.getMessage());
        }
    }

    // ---- Publishing helpers ----

    /** Publish N messages to a stream with body "msg-{i}". */
    void publishMessages(String stream, int count) {
        publishMessages(stream, count, "msg-");
    }

    /** Publish N messages to a stream with body "{prefix}{i}". */
    void publishMessages(String stream, int count, String prefix) {
        try (Producer producer = getHealthyTestEnvironment().producerBuilder().stream(stream).build()) {
            CountDownLatch latch = new CountDownLatch(count);
            for (int i = 0; i < count; i++) {
                byte[] body = (prefix + i).getBytes(StandardCharsets.UTF_8);
                producer.send(
                        producer.messageBuilder().addData(body).build(),
                        status -> latch.countDown());
            }
            if (!latch.await(30, TimeUnit.SECONDS)) {
                throw new RuntimeException(
                        "Timed out publishing " + count + " messages to '" + stream + "'");
            }
            LOG.info("Published {} messages to stream '{}'", count, stream);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    // ---- Consuming helpers ----

    /** Consume up to {@code count} messages from a stream, starting at first. */
    List<Message> consumeMessages(String stream, int count) {
        return consumeMessages(stream, count, OffsetSpecification.first());
    }

    /** Consume up to {@code count} messages from a stream. */
    List<Message> consumeMessages(String stream, int count,
                                   OffsetSpecification offsetSpec) {
        CopyOnWriteArrayList<Message> messages = new CopyOnWriteArrayList<>();
        CountDownLatch latch = new CountDownLatch(count);

        Consumer consumer = getHealthyTestEnvironment().consumerBuilder()
                .stream(stream)
                .offset(offsetSpec)
                .messageHandler((ctx, msg) -> {
                    messages.add(msg);
                    latch.countDown();
                })
                .build();

        try {
            boolean complete = latch.await(30, TimeUnit.SECONDS);
            if (!complete) {
                LOG.warn("Timed out waiting to consume {} messages from stream '{}'; consumed {}",
                        count, stream, messages.size());
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            consumer.close();
        }

        return new ArrayList<>(messages);
    }

    // ---- Retention and truncation helpers ----

    /**
     * Create a stream with small retention limits to allow testing truncation.
     * Uses small segment and max-length to force early truncation.
     */
    void createStreamWithRetention(String name, long maxLengthBytes,
                                    long maxSegmentSizeBytes) {
        getHealthyTestEnvironment().streamCreator()
                .stream(name)
                .maxLengthBytes(
                        com.rabbitmq.stream.ByteCapacity.B(maxLengthBytes))
                .maxSegmentSizeBytes(
                        com.rabbitmq.stream.ByteCapacity.B(maxSegmentSizeBytes))
                .create();
        LOG.info("Created stream '{}' with maxLengthBytes={}, maxSegmentSizeBytes={}",
                name, maxLengthBytes, maxSegmentSizeBytes);
    }

    /**
     * Wait until a stream's first offset is greater than 0 (truncation occurred).
     * Returns the first available offset, or -1 if truncation didn't happen within timeout.
     */
    long waitForTruncation(String stream, long maxWaitMs) {
        long deadline = System.currentTimeMillis() + maxWaitMs;
        while (System.currentTimeMillis() < deadline) {
            try {
                long firstOffset = getHealthyTestEnvironment().queryStreamStats(stream).firstOffset();
                if (firstOffset > 0) {
                    LOG.info("Stream '{}' truncated, firstOffset={}", stream, firstOffset);
                    return firstOffset;
                }
            } catch (Exception e) {
                LOG.debug("Error querying stats for '{}': {}", stream, e.getMessage());
            }
            try {
                Thread.sleep(200);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        LOG.warn("Stream '{}' was not truncated within {}ms", stream, maxWaitMs);
        return -1;
    }

    /**
     * Wait until all streams are truncated (first offset > 0) within one shared timeout window.
     * Returns first offsets for each stream (-1 for streams that did not truncate in time).
     */
    Map<String, Long> waitForTruncationAll(List<String> streams, long maxWaitMs) {
        java.util.LinkedHashMap<String, Long> firstOffsets = new java.util.LinkedHashMap<>();
        for (String stream : streams) {
            firstOffsets.put(stream, -1L);
        }

        long deadline = System.currentTimeMillis() + maxWaitMs;
        while (System.currentTimeMillis() < deadline) {
            boolean allTruncated = true;
            for (String stream : streams) {
                if (firstOffsets.get(stream) > 0) {
                    continue;
                }
                try {
                    long firstOffset = getHealthyTestEnvironment().queryStreamStats(stream).firstOffset();
                    if (firstOffset > 0) {
                        LOG.info("Stream '{}' truncated, firstOffset={}", stream, firstOffset);
                        firstOffsets.put(stream, firstOffset);
                    } else {
                        allTruncated = false;
                    }
                } catch (Exception e) {
                    allTruncated = false;
                    LOG.debug("Error querying stats for '{}': {}", stream, e.getMessage());
                }
            }
            if (allTruncated) {
                return firstOffsets;
            }
            try {
                Thread.sleep(200);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }

        for (Map.Entry<String, Long> entry : firstOffsets.entrySet()) {
            if (entry.getValue() <= 0) {
                LOG.warn("Stream '{}' was not truncated within {}ms", entry.getKey(), maxWaitMs);
            }
        }
        return firstOffsets;
    }

    /**
     * Publish messages asynchronously after a delay (for concurrent publish tests).
     * Returns a future that completes when all messages are published.
     */
    CompletableFuture<Void> publishMessagesAsync(String stream, int count,
                                                  String prefix, long delayMs) {
        return CompletableFuture.runAsync(() -> {
            try {
                Thread.sleep(delayMs);
                publishMessages(stream, count, prefix);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
    }

    // ---- Filtering helpers ----

    /**
     * Publish N messages with a broker-side filter value.
     * The producer uses a {@code filterValue()} extractor that reads from
     * the application property "filter". Each message's body is "{prefix}{i}".
     */
    // ---- Stored offset helpers ----

    /**
     * Query the stored offset for a consumer on a stream.
     * Returns the stored offset or throws if none exists.
     */
    long queryStoredOffset(String consumerName, String stream) {
        IllegalStateException lastStateError = null;
        for (int i = 0; i < 20; i++) {
            var consumer = getHealthyTestEnvironment().consumerBuilder()
                    .stream(stream)
                    .name(consumerName)
                    .manualTrackingStrategy()
                    .builder()
                    .offset(OffsetSpecification.next())
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
    boolean hasStoredOffset(String consumerName, String stream) {
        try {
            queryStoredOffset(consumerName, stream);
            return true;
        } catch (NoOffsetException e) {
            return false;
        }
    }

    // ---- Filtering helpers ----

    void publishMessagesWithFilterValue(String stream, int count,
                                         String prefix, String filterValue) {
        try (Producer producer = getHealthyTestEnvironment().producerBuilder()
                .stream(stream)
                .filterValue(msg -> {
                    var props = msg.getApplicationProperties();
                    return props != null ? (String) props.get("filter") : null;
                })
                .build()) {
            CountDownLatch latch = new CountDownLatch(count);
            for (int i = 0; i < count; i++) {
                byte[] body = (prefix + i).getBytes(StandardCharsets.UTF_8);
                producer.send(
                        producer.messageBuilder()
                                .addData(body)
                                .applicationProperties()
                                .entry("filter", filterValue)
                                .messageBuilder()
                                .build(),
                        status -> latch.countDown());
            }
            if (!latch.await(30, TimeUnit.SECONDS)) {
                throw new RuntimeException(
                        "Timed out publishing " + count + " filtered messages to '" + stream + "'");
            }
            LOG.info("Published {} messages with filterValue='{}' to stream '{}'",
                    count, filterValue, stream);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }
}
