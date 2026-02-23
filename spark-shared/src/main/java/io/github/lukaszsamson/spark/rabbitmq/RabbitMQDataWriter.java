package io.github.lukaszsamson.spark.rabbitmq;

import com.rabbitmq.stream.*;
import com.rabbitmq.stream.compression.Compression;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.metric.CustomTaskMetric;
import org.apache.spark.sql.connector.write.DataWriter;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Writes Spark rows to a RabbitMQ stream using the native Streams producer.
 *
 * <p>Lifecycle: {@code write()} for each record → {@code commit()} or
 * {@code abort()} → {@code close()}.
 *
 * <p>The producer is lazily initialized on the first {@link #write(InternalRow)}
 * call. Publisher confirms are tracked; if any send fails, subsequent writes
 * will throw immediately. {@link #commit()} waits for all outstanding confirms
 * with a configurable timeout.
 */
final class RabbitMQDataWriter implements DataWriter<InternalRow> {

    private static final Logger LOG = LoggerFactory.getLogger(RabbitMQDataWriter.class);
    private static final long DEFAULT_PUBLISHER_CONFIRM_TIMEOUT_MS = 30_000L;
    private static final long CLIENT_MIN_CONFIRM_TIMEOUT_MS = 1_000L;

    private final ConnectorOptions options;
    private final int partitionId;
    private final long taskId;
    private final long epochId;

    private final RowToMessageConverter converter;

    private boolean pooledEnvironment = false;
    private Environment environment;
    private Producer producer;

    // Confirm tracking
    private final AtomicReference<Throwable> sendError = new AtomicReference<>();
    private final AtomicLong outstandingConfirms = new AtomicLong(0);
    private final AtomicLong confirmationFailureCount = new AtomicLong(0);
    private final AtomicLong lastConfirmationFailureCode = new AtomicLong(-1L);
    private final Object confirmMonitor = new Object();
    private final AtomicBoolean closeCalled = new AtomicBoolean(false);

    // Deduplication: monotonic publishing ID
    private long nextPublishingId = -1;

    // Metrics
    private long recordsWritten = 0;
    private long payloadBytesWritten = 0;
    private long estimatedWireBytesWritten = 0;
    private final AtomicLong writeLatencyMs = new AtomicLong(0);
    private final AtomicLong publishConfirms = new AtomicLong(0);
    private final AtomicLong publishErrors = new AtomicLong(0);

    RabbitMQDataWriter(ConnectorOptions options, StructType inputSchema,
                       int partitionId, long taskId, long epochId) {
        this.options = options;
        this.partitionId = partitionId;
        this.taskId = taskId;
        this.epochId = epochId;
        this.converter = new RowToMessageConverter(inputSchema);
    }

    @Override
    public void write(InternalRow record) throws IOException {
        try {
            // Check for prior send errors
            Throwable error = sendError.get();
            if (error != null) {
                throw new IOException(
                        "Previous send failed on partition " + partitionId + confirmationFailureSummary(),
                        error);
            }

            // Lazy initialization
            if (producer == null) {
                initProducer();
            }

            // Validate stream column in single-stream mode
            if (options.isStreamMode()) {
                String rowStream = converter.getStream(record);
                if (rowStream != null && !rowStream.equals(options.getStream())) {
                    throw new IOException(
                            "Row targets stream '" + rowStream + "' but connector is configured for " +
                                    "stream '" + options.getStream() + "'. In stream mode, all rows " +
                                    "must target the configured stream.");
                }
            }

            // Build message, with explicit per-row publishing_id override when present.
            MessageBuilder builder = producer.messageBuilder();
            Long explicitPublishingId = converter.getPublishingId(record);
            if (explicitPublishingId != null) {
                if (explicitPublishingId < 0) {
                    throw new IOException(
                            "Column 'publishing_id' must be >= 0, got: " + explicitPublishingId);
                }
                builder.publishingId(explicitPublishingId);
                if (nextPublishingId >= 0 && explicitPublishingId >= nextPublishingId) {
                    nextPublishingId = explicitPublishingId + 1;
                }
            } else if (nextPublishingId >= 0) {
                builder.publishingId(nextPublishingId++);
            }
            Message message = converter.convert(record, builder);

            // Validate routing key for superstream hash/key strategies
            if (options.isSuperStreamMode()
                    && options.getRoutingStrategy() != RoutingStrategyType.CUSTOM) {
                String rk = extractRoutingKey(message);
                if (rk == null || rk.isEmpty()) {
                    throw new IOException(missingRoutingKeyErrorMessage());
                }
            }

            // Track outstanding confirms
            outstandingConfirms.incrementAndGet();
            long sendStartNanos = System.nanoTime();

            // Send with confirmation handler
            try {
                producer.send(message, confirmationStatus -> {
                    long elapsedMs = TimeUnit.NANOSECONDS.toMillis(
                            System.nanoTime() - sendStartNanos);
                    writeLatencyMs.addAndGet(Math.max(0L, elapsedMs));
                    if (!confirmationStatus.isConfirmed()) {
                        confirmationFailureCount.incrementAndGet();
                        lastConfirmationFailureCode.set(confirmationStatus.getCode());
                        publishErrors.incrementAndGet();
                        sendError.compareAndSet(null,
                                new IOException("Message confirmation failed with code " +
                                        confirmationStatus.getCode() +
                                        " on partition " + partitionId));
                    } else {
                        publishConfirms.incrementAndGet();
                    }
                    synchronized (confirmMonitor) {
                        long remaining = outstandingConfirms.decrementAndGet();
                        if (remaining == 0 || sendError.get() != null) {
                            confirmMonitor.notifyAll();
                        }
                    }
                });
            } catch (Exception e) {
                outstandingConfirms.decrementAndGet();
                publishErrors.incrementAndGet();
                throw new IOException("Failed to send message on partition " + partitionId, e);
            }

            // Track metrics
            recordsWritten++;
            payloadBytesWritten += MessageSizeEstimator.payloadBytes(message);
            estimatedWireBytesWritten += MessageSizeEstimator.estimatedWireBytes(message);
        } catch (IOException e) {
            cleanupAfterWriteFailure(e);
            throw e;
        } catch (RuntimeException e) {
            cleanupAfterWriteFailure(e);
            throw e;
        }
    }

    @Override
    public WriterCommitMessage commit() throws IOException {
        // Wait for all outstanding confirms
        if (outstandingConfirms.get() > 0) {
            long timeoutMs = effectivePublisherConfirmTimeoutMs();
            synchronized (confirmMonitor) {
                if (timeoutMs == 0) {
                    while (outstandingConfirms.get() > 0) {
                        Throwable error = sendError.get();
                        if (error != null) {
                            throw new IOException(
                                    "One or more messages failed confirmation on partition "
                                            + partitionId + confirmationFailureSummary(), error);
                        }
                        try {
                            confirmMonitor.wait();
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            throw new IOException("Interrupted waiting for publisher confirms", e);
                        }
                    }
                } else {
                    long deadlineNanos = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMs);
                    while (outstandingConfirms.get() > 0) {
                        Throwable error = sendError.get();
                        if (error != null) {
                            throw new IOException(
                                    "One or more messages failed confirmation on partition "
                                            + partitionId + confirmationFailureSummary(), error);
                        }
                        long remainingNanos = deadlineNanos - System.nanoTime();
                        if (remainingNanos <= 0) {
                            throw new IOException(
                                    "Timed out waiting for publisher confirms on partition " +
                                            partitionId + ". Outstanding: " + outstandingConfirms.get() +
                                            ", timeout: " + timeoutMs + "ms");
                        }
                        try {
                            TimeUnit.NANOSECONDS.timedWait(confirmMonitor, remainingNanos);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            throw new IOException("Interrupted waiting for publisher confirms", e);
                        }
                    }
                }
            }
        }

        // Check for send errors
        Throwable error = sendError.get();
        if (error != null) {
            throw new IOException(
                    "One or more messages failed confirmation on partition " + partitionId +
                            confirmationFailureSummary(), error);
        }

        LOG.info("Committed partition {} (task {}): {} records, {} bytes",
                partitionId, taskId, recordsWritten, payloadBytesWritten);

        return new RabbitMQWriterCommitMessage(
                partitionId, taskId, recordsWritten, payloadBytesWritten);
    }

    @Override
    public void abort() throws IOException {
        LOG.warn("Aborting writer for partition {} (task {}): {} records written before abort",
                partitionId, taskId, recordsWritten);
        // Close resources; unconfirmed messages may be lost
        close();
    }

    @Override
    public void close() throws IOException {
        if (!closeCalled.compareAndSet(false, true)) {
            return;
        }
        try {
            if (producer != null) {
                producer.close();
                producer = null;
            }
        } catch (Exception e) {
            LOG.warn("Error closing producer for partition {}", partitionId, e);
        }
        if (pooledEnvironment) {
            EnvironmentPool.getInstance().release(options);
            pooledEnvironment = false;
            environment = null;
        } else {
            try {
                if (environment != null) {
                    environment.close();
                    environment = null;
                }
            } catch (Exception e) {
                LOG.warn("Error closing environment for partition {}", partitionId, e);
            }
        }
    }

    @Override
    public CustomTaskMetric[] currentMetricsValues() {
        return new CustomTaskMetric[]{
                RabbitMQSinkMetrics.taskMetric(RabbitMQSinkMetrics.RECORDS_WRITTEN, recordsWritten),
                RabbitMQSinkMetrics.taskMetric(
                        RabbitMQSinkMetrics.PAYLOAD_BYTES_WRITTEN, payloadBytesWritten),
                RabbitMQSinkMetrics.taskMetric(
                        RabbitMQSinkMetrics.ESTIMATED_WIRE_BYTES_WRITTEN, estimatedWireBytesWritten),
                RabbitMQSinkMetrics.taskMetric(RabbitMQSinkMetrics.WRITE_LATENCY_MS,
                        writeLatencyMs.get()),
                RabbitMQSinkMetrics.taskMetric(RabbitMQSinkMetrics.PUBLISH_CONFIRMS,
                        publishConfirms.get()),
                RabbitMQSinkMetrics.taskMetric(RabbitMQSinkMetrics.PUBLISH_ERRORS,
                        publishErrors.get()),
        };
    }

    // ---- Producer initialization ----

    private void initProducer() throws IOException {
        environment = EnvironmentPool.getInstance().acquire(options);
        pooledEnvironment = true;
        String derivedName = null;
        try {
            ProducerBuilder builder;
            if (options.isSuperStreamMode()) {
                builder = buildSuperStreamProducer();
            } else {
                builder = environment.producerBuilder().stream(options.getStream());
            }

            // Producer name for deduplication
            derivedName = deriveProducerName();
            if (derivedName != null) {
                builder.name(derivedName);
            }

            // Sub-entry batching and compression
            if (options.getSubEntrySize() != null && options.getSubEntrySize() > 1) {
                builder.subEntrySize(options.getSubEntrySize());
                builder.compression(toStreamCompression(options.getCompression()));
            }

            // Batch settings
            if (options.getBatchPublishingDelayMs() != null) {
                builder.batchPublishingDelay(
                        Duration.ofMillis(options.getBatchPublishingDelayMs()));
            }

            // Flow control
            if (options.getMaxInFlight() != null) {
                builder.maxUnconfirmedMessages(options.getMaxInFlight());
            }
            if (options.getPublisherConfirmTimeoutMs() != null) {
                long configuredMs = options.getPublisherConfirmTimeoutMs();
                long effectiveMs = effectivePublisherConfirmTimeoutMs();
                if (configuredMs > 0 && configuredMs < CLIENT_MIN_CONFIRM_TIMEOUT_MS) {
                    LOG.warn("publisherConfirmTimeoutMs={}ms is below client minimum; using {}ms for producer confirm timeout",
                            configuredMs, effectiveMs);
                }
                builder.confirmTimeout(Duration.ofMillis(effectiveMs));
            }
            builder.enqueueTimeout(Duration.ofMillis(options.getEnqueueTimeoutMs()));
            if (options.getRetryOnRecovery() != null) {
                builder.retryOnRecovery(options.getRetryOnRecovery());
            }
            if (options.getDynamicBatch() != null) {
                builder.dynamicBatch(options.getDynamicBatch());
            }

            // State listener for RECOVERING/CLOSED transitions
            builder.listeners(context -> {
                Resource.State from = context.previousState();
                Resource.State to = context.currentState();
                if (to == Resource.State.RECOVERING) {
                    LOG.warn("Producer for partition {} is recovering ({}->{})",
                            partitionId, from, to);
                } else if (to == Resource.State.CLOSED) {
                    LOG.warn("Producer for partition {} has closed ({}->{})",
                            partitionId, from, to);
                    sendError.compareAndSet(null,
                            new IOException("Producer closed unexpectedly on partition " + partitionId));
                    synchronized (confirmMonitor) {
                        confirmMonitor.notifyAll();
                    }
                } else {
                    LOG.debug("Producer for partition {} state change: {}->{}",
                            partitionId, from, to);
                }
            });

            // Filter value extraction
            ConnectorFilterValueExtractor filterValueExtractor = createFilterValueExtractor(options);
            if (filterValueExtractor != null) {
                builder.filterValue(message -> filterValueExtractor.extract(toMessageView(message)));
            }

            producer = builder.build();

            // Initialize dedup publishing ID
            if (derivedName != null) {
                nextPublishingId = epochId >= 0 ? 0L : producer.getLastPublishingId() + 1;
                LOG.info("Dedup enabled for partition {} with producer '{}', starting publishingId={}",
                        partitionId, derivedName, nextPublishingId);
            }

            LOG.info("Initialized producer for partition {} (task {}, epoch {})",
                    partitionId, taskId, epochId);
        } catch (Exception e) {
            cleanupAfterInitFailure();
            throw new IOException("Failed to initialize producer for partition " + partitionId +
                    " (task " + taskId + ", epoch " + epochId + ")", e);
        }
    }

    private void cleanupAfterWriteFailure(Throwable originalFailure) {
        try {
            close();
        } catch (IOException closeError) {
            originalFailure.addSuppressed(closeError);
        }
    }

    private void cleanupAfterInitFailure() {
        try {
            if (producer != null) {
                producer.close();
            }
        } catch (Exception closeError) {
            LOG.warn("Error closing producer after init failure for partition {}", partitionId,
                    closeError);
        } finally {
            producer = null;
        }

        if (pooledEnvironment) {
            EnvironmentPool.getInstance().release(options);
            pooledEnvironment = false;
            environment = null;
        } else if (environment != null) {
            try {
                environment.close();
            } catch (Exception closeError) {
                LOG.warn("Error closing environment after init failure for partition {}",
                        partitionId, closeError);
            } finally {
                environment = null;
            }
        }
        nextPublishingId = -1L;
    }

    private long effectivePublisherConfirmTimeoutMs() {
        Long configuredMs = options.getPublisherConfirmTimeoutMs();
        if (configuredMs == null) {
            return DEFAULT_PUBLISHER_CONFIRM_TIMEOUT_MS;
        }
        if (configuredMs == 0) {
            return 0;
        }
        return Math.max(configuredMs, CLIENT_MIN_CONFIRM_TIMEOUT_MS);
    }

    private ProducerBuilder buildSuperStreamProducer() {
        ProducerBuilder builder = environment.producerBuilder()
                .superStream(options.getSuperStream());

        // Set up routing
        ProducerBuilder.RoutingConfiguration routing = builder.routing(message -> {
            String routingKey = extractRoutingKey(message);
            if (routingKey != null && !routingKey.isEmpty()) {
                return routingKey;
            }
            if (options.getRoutingStrategy() == RoutingStrategyType.CUSTOM) {
                return "";
            }
            throw new IllegalStateException(missingRoutingKeyErrorMessage());
        });

        switch (options.getRoutingStrategy()) {
            case HASH -> {
                String hashFunctionClass = options.getHashFunctionClass();
                if (hashFunctionClass != null && !hashFunctionClass.isEmpty()) {
                    ConnectorHashFunction hashFunction = ExtensionLoader.load(
                            hashFunctionClass,
                            ConnectorHashFunction.class,
                            ConnectorOptions.HASH_FUNCTION_CLASS);
                    routing.hash(hashFunction::hash).producerBuilder();
                } else {
                    routing.hash().producerBuilder();
                }
            }
            case KEY -> routing.key().producerBuilder();
            case CUSTOM -> {
                ConnectorRoutingStrategy customStrategy = ExtensionLoader.load(
                        options.getPartitionerClass(),
                        ConnectorRoutingStrategy.class,
                        ConnectorOptions.PARTITIONER_CLASS);
                routing.strategy((message, metadata) -> {
                    ConnectorMessageView messageView = toMessageView(message);
                    return customStrategy.route(
                            messageView,
                            new ConnectorRoutingMetadataView(metadata));
                }).producerBuilder();
            }
        }

        return builder;
    }

    /**
     * Derive the producer name for deduplication.
     *
     * <p>Uses an epoch-scoped producer identity for streaming retries and a
     * task-scoped identity for batch writes.
     *
     * @return the derived name, or null if dedup is not enabled
     */
    private String deriveProducerName() {
        String baseName = options.getProducerName();
        if (baseName == null || baseName.isEmpty()) {
            return null;
        }
        if (epochId >= 0) {
            return baseName + "-p" + partitionId + "-e" + epochId;
        }
        // Batch writes may run concurrent task attempts for one partition; include taskId
        // to avoid producer-name collisions (RabbitMQ permits only one live producer/name).
        return baseName + "-p" + partitionId + "-t" + taskId;
    }

    /**
     * Extract routing key from a built message using the same lookup order
     * as the superstream routing function: application_properties["routing_key"],
     * then properties.subject.
     */
    private static String extractRoutingKey(Message message) {
        var appProps = message.getApplicationProperties();
        if (appProps != null) {
            Object rk = appProps.get("routing_key");
            if (rk != null) {
                return rk.toString();
            }
        }
        if (message.getProperties() != null && message.getProperties().getSubject() != null) {
            return message.getProperties().getSubject();
        }
        return null;
    }

    private String confirmationFailureSummary() {
        long failureCount = confirmationFailureCount.get();
        if (failureCount <= 0) {
            return "";
        }
        long lastCode = lastConfirmationFailureCode.get();
        if (lastCode >= 0) {
            return " (confirmation failures=" + failureCount + ", lastCode=" + lastCode + ")";
        }
        return " (confirmation failures=" + failureCount + ")";
    }

    private String missingRoutingKeyErrorMessage() {
        return "Routing key is required for superstream with " +
                options.getRoutingStrategy() + " routing strategy. " +
                "Provide a 'routing_key' column, set 'routing_key' in " +
                "'application_properties', or set 'subject' in 'properties'.";
    }

    private record ConnectorRoutingMetadataView(
            com.rabbitmq.stream.RoutingStrategy.Metadata delegate)
            implements ConnectorRoutingStrategy.Metadata {
        @Override
        public java.util.List<String> partitions() {
            return delegate.partitions();
        }

        @Override
        public java.util.List<String> route(String routingKey) {
            return delegate.route(routingKey);
        }
    }

    private static ConnectorFilterValueExtractor createFilterValueExtractor(
            ConnectorOptions options) {
        String className = options.getFilterValueExtractorClass();
        if (className != null && !className.isEmpty()) {
            return ExtensionLoader.load(className, ConnectorFilterValueExtractor.class,
                    ConnectorOptions.FILTER_VALUE_EXTRACTOR_CLASS);
        }
        String path = options.getFilterValuePath();
        if (path != null && !path.isEmpty()) {
            return message -> message.valueAtPath(path);
        }
        return null;
    }

    private static ConnectorMessageView toMessageView(Message message) {
        return MessageViewCoercion.toMessageView(message);
    }

    private static Map<String, String> coerceMapToStrings(Map<String, Object> source) {
        return MessageViewCoercion.coerceMapToStrings(source);
    }

    private static Map<String, String> coercePropertiesToStrings(Properties properties) {
        return MessageViewCoercion.coercePropertiesToStrings(properties);
    }

    private static String coerceIdToString(Object id) {
        return MessageViewCoercion.coerceIdToString(id);
    }

    private static void putIfNotNull(Map<String, String> target, String key, String value) {
        MessageViewCoercion.putIfNotNull(target, key, value);
    }

    private static Compression toStreamCompression(CompressionType type) {
        return switch (type) {
            case NONE -> Compression.NONE;
            case GZIP -> Compression.GZIP;
            case SNAPPY -> Compression.SNAPPY;
            case LZ4 -> Compression.LZ4;
            case ZSTD -> Compression.ZSTD;
        };
    }
}
