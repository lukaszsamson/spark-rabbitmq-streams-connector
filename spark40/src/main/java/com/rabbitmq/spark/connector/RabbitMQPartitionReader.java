package com.rabbitmq.spark.connector;

import com.rabbitmq.stream.*;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.metric.CustomTaskMetric;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Push-to-pull bridge that reads messages from a RabbitMQ stream consumer
 * and presents them as Spark {@link InternalRow}s.
 *
 * <p>The RabbitMQ stream client is push-based (messages arrive via
 * {@link MessageHandler}). Spark readers are pull-based ({@link #next()}/{@link #get()}).
 * This reader bridges the two by pushing messages into a bounded
 * {@link BlockingQueue} and pulling from it on demand.
 */
final class RabbitMQPartitionReader implements PartitionReader<InternalRow> {

    private static final Logger LOG = LoggerFactory.getLogger(RabbitMQPartitionReader.class);

    private final String stream;
    private final long startOffset;
    private final long endOffset;
    private final ConnectorOptions options;
    private final MessageToRowConverter converter;

    private final BlockingQueue<QueuedMessage> queue;
    private final AtomicReference<Throwable> consumerError = new AtomicReference<>();
    private final AtomicBoolean consumerClosed = new AtomicBoolean(false);

    private boolean pooledEnvironment = false;
    private Environment environment;
    private Consumer consumer;
    private InternalRow currentRow;
    private long lastEmittedOffset = -1;
    private boolean finished = false;

    // Task-level metric counters
    private long recordsRead = 0;
    private long bytesRead = 0;

    /**
     * A message queued by the consumer callback for pull-based reading.
     */
    record QueuedMessage(Message message, long offset, long chunkTimestampMillis) {}

    RabbitMQPartitionReader(RabbitMQInputPartition partition, ConnectorOptions options) {
        this.stream = partition.getStream();
        this.startOffset = partition.getStartOffset();
        this.endOffset = partition.getEndOffset();
        this.options = options;
        this.converter = new MessageToRowConverter(options.getMetadataFields());
        this.queue = new LinkedBlockingQueue<>(options.getQueueCapacity());
    }

    @Override
    public boolean next() throws IOException {
        if (finished) {
            return false;
        }

        // Lazy initialization: create consumer on first call
        if (consumer == null) {
            initConsumer();
        }

        long totalWaitMs = 0;
        long pollTimeoutMs = options.getPollTimeoutMs();
        long maxWaitMs = options.getMaxWaitMs();

        while (true) {
            // Check for consumer errors
            Throwable error = consumerError.get();
            if (error != null) {
                throw new IOException("Consumer error on stream '" + stream + "'", error);
            }

            QueuedMessage qm;
            try {
                qm = queue.poll(pollTimeoutMs, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("Interrupted while reading from stream '" + stream + "'", e);
            }

            if (qm == null) {
                totalWaitMs += pollTimeoutMs;
                if (totalWaitMs >= maxWaitMs) {
                    throw new IOException(
                            "Timed out waiting for messages from stream '" + stream +
                                    "'. Last emitted offset: " + lastEmittedOffset +
                                    ", target end offset: " + endOffset +
                                    ". Waited " + totalWaitMs + "ms (maxWaitMs=" + maxWaitMs + ")");
                }
                LOG.debug("Poll timeout on stream '{}', waited {}ms so far", stream, totalWaitMs);
                continue;
            }

            // Reset wait timer on message receipt
            totalWaitMs = 0;

            // Skip messages before start offset (can happen with timestamp-based starting)
            if (qm.offset() < startOffset) {
                continue;
            }

            // Stop at end offset (exclusive)
            if (qm.offset() >= endOffset) {
                finished = true;
                return false;
            }

            // De-dup on reconnection: skip already-emitted offsets
            if (qm.offset() <= lastEmittedOffset) {
                continue;
            }

            currentRow = converter.convert(
                    qm.message(), stream, qm.offset(), qm.chunkTimestampMillis());
            lastEmittedOffset = qm.offset();
            recordsRead++;
            byte[] body = qm.message().getBodyAsBinary();
            if (body != null) {
                bytesRead += body.length;
            }
            return true;
        }
    }

    @Override
    public InternalRow get() {
        return currentRow;
    }

    @Override
    public void close() throws IOException {
        finished = true;
        try {
            if (consumer != null) {
                consumer.close();
                consumer = null;
            }
        } catch (Exception e) {
            LOG.warn("Error closing consumer for stream '{}'", stream, e);
        }
        if (pooledEnvironment) {
            EnvironmentPool.getInstance().release(options);
            environment = null;
        } else {
            try {
                if (environment != null) {
                    environment.close();
                    environment = null;
                }
            } catch (Exception e) {
                LOG.warn("Error closing environment for stream '{}'", stream, e);
            }
        }
    }

    @Override
    public CustomTaskMetric[] currentMetricsValues() {
        return new CustomTaskMetric[]{
                RabbitMQSourceMetrics.taskMetric(RabbitMQSourceMetrics.RECORDS_READ, recordsRead),
                RabbitMQSourceMetrics.taskMetric(RabbitMQSourceMetrics.BYTES_READ, bytesRead),
        };
    }

    private void initConsumer() {
        environment = EnvironmentPool.getInstance().acquire(options);
        pooledEnvironment = true;

        OffsetSpecification offsetSpec = resolveOffsetSpec();

        ConsumerBuilder builder = environment.consumerBuilder()
                .stream(stream)
                .offset(offsetSpec)
                .noTrackingStrategy()
                .messageHandler((context, message) -> {
                    // Fast exit if we've already reached end offset
                    if (context.offset() >= endOffset) {
                        return;
                    }
                    try {
                        // Use offer with timeout to avoid indefinitely blocking the Netty thread
                        if (!queue.offer(new QueuedMessage(
                                message, context.offset(), context.timestamp()),
                                30, TimeUnit.SECONDS)) {
                            consumerError.compareAndSet(null,
                                    new IOException("Queue full: timed out enqueuing message " +
                                            "at offset " + context.offset() +
                                            " on stream '" + stream + "'"));
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                })
                .flow()
                .initialCredits(options.getInitialCredits())
                .strategy(ConsumerFlowStrategy.creditOnChunkArrival(
                        options.getInitialCredits()))
                .builder();

        // State listener for RECOVERING/CLOSED transitions
        builder.listeners(context -> {
            Resource.State from = context.previousState();
            Resource.State to = context.currentState();
            if (to == Resource.State.RECOVERING) {
                LOG.warn("Consumer for stream '{}' is recovering ({}->{})",
                        stream, from, to);
            } else if (to == Resource.State.CLOSED) {
                LOG.warn("Consumer for stream '{}' has closed ({}->{})",
                        stream, from, to);
                consumerClosed.set(true);
            } else {
                LOG.debug("Consumer for stream '{}' state change: {}->{}",
                        stream, from, to);
            }
        });

        // Configure filtering if specified
        if (options.getFilterValues() != null && !options.getFilterValues().isEmpty()) {
            builder.filter()
                    .values(options.getFilterValues().toArray(new String[0]))
                    .matchUnfiltered(options.isFilterMatchUnfiltered())
                    .builder();
        }

        consumer = builder.build();

        LOG.info("Opened consumer for stream '{}' with offsets [{}, {})",
                stream, startOffset, endOffset);
    }

    private OffsetSpecification resolveOffsetSpec() {
        return switch (options.getStartingOffsets()) {
            case EARLIEST -> OffsetSpecification.first();
            case LATEST -> OffsetSpecification.offset(startOffset);
            case OFFSET -> OffsetSpecification.offset(startOffset);
            case TIMESTAMP -> OffsetSpecification.timestamp(options.getStartingTimestamp());
        };
    }
}
