package com.rabbitmq.spark.connector;

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
import java.util.concurrent.TimeUnit;
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
    private final Object confirmMonitor = new Object();

    // Deduplication: monotonic publishing ID
    private long nextPublishingId = -1;

    // Metrics
    private long recordsWritten = 0;
    private long bytesWritten = 0;
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
        // Check for prior send errors
        Throwable error = sendError.get();
        if (error != null) {
            throw new IOException("Previous send failed on partition " + partitionId, error);
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

        // Build message, with publishingId if deduplication is enabled
        MessageBuilder builder = producer.messageBuilder();
        if (nextPublishingId >= 0) {
            builder.publishingId(nextPublishingId++);
        }
        Message message = converter.convert(record, builder);

        // Validate routing key for superstream hash/key strategies
        if (options.isSuperStreamMode()
                && options.getRoutingStrategy() != RoutingStrategyType.CUSTOM) {
            String rk = extractRoutingKey(message);
            if (rk == null || rk.isEmpty()) {
                throw new IOException(
                        "Routing key is required for superstream with " +
                                options.getRoutingStrategy() + " routing strategy. " +
                                "Provide a 'routing_key' column, set 'routing_key' in " +
                                "'application_properties', or set 'subject' in 'properties'.");
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
                    publishErrors.incrementAndGet();
                    sendError.compareAndSet(null,
                            new IOException("Message confirmation failed with code " +
                                    confirmationStatus.getCode() +
                                    " on partition " + partitionId));
                } else {
                    publishConfirms.incrementAndGet();
                }
                synchronized (confirmMonitor) {
                    if (outstandingConfirms.decrementAndGet() == 0) {
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
        byte[] body = converter.getValue(record);
        if (body != null) {
            bytesWritten += body.length;
        }
    }

    @Override
    public WriterCommitMessage commit() throws IOException {
        // Wait for all outstanding confirms
        if (outstandingConfirms.get() > 0) {
            long timeoutMs = options.getPublisherConfirmTimeoutMs() != null
                    ? options.getPublisherConfirmTimeoutMs()
                    : 30_000L;
            long deadlineNanos = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMs);
            synchronized (confirmMonitor) {
                while (outstandingConfirms.get() > 0) {
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

        // Check for send errors
        Throwable error = sendError.get();
        if (error != null) {
            throw new IOException(
                    "One or more messages failed confirmation on partition " + partitionId, error);
        }

        LOG.info("Committed partition {} (task {}): {} records, {} bytes",
                partitionId, taskId, recordsWritten, bytesWritten);

        return new RabbitMQWriterCommitMessage(
                partitionId, taskId, recordsWritten, bytesWritten);
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
                RabbitMQSinkMetrics.taskMetric(RabbitMQSinkMetrics.BYTES_WRITTEN, bytesWritten),
                RabbitMQSinkMetrics.taskMetric(RabbitMQSinkMetrics.WRITE_LATENCY_MS,
                        writeLatencyMs.get()),
                RabbitMQSinkMetrics.taskMetric(RabbitMQSinkMetrics.PUBLISH_CONFIRMS,
                        publishConfirms.get()),
                RabbitMQSinkMetrics.taskMetric(RabbitMQSinkMetrics.PUBLISH_ERRORS,
                        publishErrors.get()),
        };
    }

    // ---- Producer initialization ----

    private void initProducer() {
        environment = EnvironmentPool.getInstance().acquire(options);
        pooledEnvironment = true;

        ProducerBuilder builder;
        if (options.isSuperStreamMode()) {
            builder = buildSuperStreamProducer();
        } else {
            builder = environment.producerBuilder().stream(options.getStream());
        }

        // Producer name for deduplication
        String derivedName = deriveProducerName();
        if (derivedName != null) {
            builder.name(derivedName);
        }

        // Sub-entry batching and compression
        if (options.getSubEntrySize() != null && options.getSubEntrySize() > 1) {
            builder.subEntrySize(options.getSubEntrySize());
            builder.compression(toStreamCompression(options.getCompression()));
        }

        // Batch settings
        if (options.getBatchSize() != null) {
            builder.batchSize(options.getBatchSize());
        }
        if (options.getBatchPublishingDelayMs() != null) {
            builder.batchPublishingDelay(
                    Duration.ofMillis(options.getBatchPublishingDelayMs()));
        }

        // Flow control
        if (options.getMaxInFlight() != null) {
            builder.maxUnconfirmedMessages(options.getMaxInFlight());
        }
        if (options.getPublisherConfirmTimeoutMs() != null) {
            builder.confirmTimeout(
                    Duration.ofMillis(options.getPublisherConfirmTimeoutMs()));
        }
        builder.enqueueTimeout(Duration.ofMillis(options.getEnqueueTimeoutMs()));

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
            } else {
                LOG.debug("Producer for partition {} state change: {}->{}",
                        partitionId, from, to);
            }
        });

        // Filter value extraction
        if (options.getFilterValueColumn() != null && !options.getFilterValueColumn().isEmpty()) {
            String filterCol = options.getFilterValueColumn();
            builder.filterValue(message -> {
                // The filter value is extracted from the message's application properties
                var appProps = message.getApplicationProperties();
                if (appProps != null) {
                    Object val = appProps.get(filterCol);
                    return val != null ? val.toString() : null;
                }
                return null;
            });
        }

        producer = builder.build();

        // Initialize dedup publishing ID
        if (derivedName != null) {
            nextPublishingId = producer.getLastPublishingId() + 1;
            LOG.info("Dedup enabled for partition {} with producer '{}', starting publishingId={}",
                    partitionId, derivedName, nextPublishingId);
        }

        LOG.info("Initialized producer for partition {} (task {}, epoch {})",
                partitionId, taskId, epochId);
    }

    private ProducerBuilder buildSuperStreamProducer() {
        ProducerBuilder builder = environment.producerBuilder()
                .superStream(options.getSuperStream());

        // Set up routing
        ProducerBuilder.RoutingConfiguration routing = builder.routing(message -> {
            // Extract routing key from application properties
            var appProps = message.getApplicationProperties();
            if (appProps != null) {
                // Check for routing_key in application properties
                Object rk = appProps.get("routing_key");
                if (rk != null) {
                    return rk.toString();
                }
            }
            // Fall back to message subject as routing key
            if (message.getProperties() != null && message.getProperties().getSubject() != null) {
                return message.getProperties().getSubject();
            }
            return "";
        });

        switch (options.getRoutingStrategy()) {
            case HASH -> routing.hash().producerBuilder();
            case KEY -> routing.key().producerBuilder();
            case CUSTOM -> {
                ConnectorRoutingStrategy customStrategy = ExtensionLoader.load(
                        options.getPartitionerClass(),
                        ConnectorRoutingStrategy.class,
                        ConnectorOptions.PARTITIONER_CLASS);
                routing.strategy((message, metadata) -> {
                    String routingKey = null;
                    var appProps = message.getApplicationProperties();
                    if (appProps != null) {
                        Object rk = appProps.get("routing_key");
                        if (rk != null) {
                            routingKey = rk.toString();
                        }
                    }
                    return customStrategy.route(routingKey, metadata.partitions());
                }).producerBuilder();
            }
        }

        return builder;
    }

    /**
     * Derive the producer name for deduplication.
     *
     * <p>Uses a stable logical name scoped to the Spark partition so retries
     * and re-executions can resume dedup state with the same producer identity.
     *
     * @return the derived name, or null if dedup is not enabled
     */
    private String deriveProducerName() {
        String baseName = options.getProducerName();
        if (baseName == null || baseName.isEmpty()) {
            return null;
        }
        return baseName + "-p" + partitionId;
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
