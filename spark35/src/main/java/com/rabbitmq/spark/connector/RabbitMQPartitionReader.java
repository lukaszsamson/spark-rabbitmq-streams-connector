package com.rabbitmq.spark.connector;

import com.rabbitmq.stream.*;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.metric.CustomTaskMetric;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
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
    private final boolean useConfiguredStartingOffset;
    private final MessageToRowConverter converter;
    private final ConnectorPostFilter postFilter;

    private final BlockingQueue<QueuedMessage> queue;
    private final AtomicReference<Throwable> consumerError = new AtomicReference<>();
    private final AtomicBoolean consumerClosed = new AtomicBoolean(false);

    private boolean pooledEnvironment = false;
    private Environment environment;
    private Consumer consumer;
    private InternalRow currentRow;
    private long lastEmittedOffset = -1;
    private long lastObservedOffset = -1;
    private boolean filteredTailReached = false;
    private boolean finished = false;

    // Task-level metric counters
    private long recordsRead = 0;
    private long bytesRead = 0;
    private long readLatencyMs = 0;

    /**
     * A message queued by the consumer callback for pull-based reading.
     * Includes the {@link MessageHandler.Context} so that {@code processed()}
     * can be called on the pull side, tying credit grants to consumption rate.
     */
    record QueuedMessage(Message message, long offset, long chunkTimestampMillis,
                         MessageHandler.Context context) {}

    RabbitMQPartitionReader(RabbitMQInputPartition partition, ConnectorOptions options) {
        this.stream = partition.getStream();
        this.startOffset = partition.getStartOffset();
        this.endOffset = partition.getEndOffset();
        this.options = options;
        this.useConfiguredStartingOffset = partition.isUseConfiguredStartingOffset();
        this.converter = new MessageToRowConverter(options.getMetadataFields());
        this.postFilter = createPostFilter(options);
        this.queue = new LinkedBlockingQueue<>(options.getQueueCapacity());
    }

    @Override
    public boolean next() throws IOException {
        if (finished) {
            return false;
        }

        // Fast-path: if we already observed or emitted the final in-range offset, stop immediately.
        // lastObservedOffset is needed when post-filter drops tail records.
        if ((lastEmittedOffset >= 0 && lastEmittedOffset >= endOffset - 1)
                || (lastObservedOffset >= 0 && lastObservedOffset >= endOffset - 1)) {
            finished = true;
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
            if ((lastEmittedOffset >= 0 && lastEmittedOffset >= endOffset - 1)
                    || (lastObservedOffset >= 0 && lastObservedOffset >= endOffset - 1)) {
                finished = true;
                return false;
            }

            // Check for consumer errors
            Throwable error = consumerError.get();
            if (error != null) {
                throw new IOException("Consumer error on stream '" + stream + "'", error);
            }

            QueuedMessage qm;
            try {
                long pollStart = System.nanoTime();
                qm = queue.poll(pollTimeoutMs, TimeUnit.MILLISECONDS);
                readLatencyMs += TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - pollStart);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("Interrupted while reading from stream '" + stream + "'", e);
            }

            if (qm == null) {
                // With broker-side filtering, not all offsets in [start, end) are delivered.
                // If the stream tail has already reached this split's end, we can terminate.
                if (isBrokerFilterConfigured() && hasStreamTailReachedPlannedEnd()) {
                    finished = true;
                    return false;
                }
                totalWaitMs += pollTimeoutMs;
                if (totalWaitMs >= maxWaitMs) {
                    if (isBrokerFilterConfigured()) {
                        LOG.warn("Reached maxWaitMs={} while reading filtered stream '{}'; " +
                                        "terminating split early at lastObservedOffset={} for planned endOffset={}",
                                maxWaitMs, stream, lastObservedOffset, endOffset);
                        finished = true;
                        return false;
                    }
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

            // Notify flow strategy that this message has been consumed (pull-side).
            // This ties credit grants to consumption rate rather than enqueue rate,
            // providing natural backpressure when the pull side is slow.
            qm.context().processed();

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

            if (qm.offset() > lastObservedOffset) {
                lastObservedOffset = qm.offset();
            }

            if (postFilter != null && !postFilter.accept(
                    qm.message().getBodyAsBinary(), coerceMapToStrings(qm.message().getApplicationProperties()))) {
                if (options.isFilterWarningOnMismatch()) {
                    LOG.warn("Post-filter dropped message at offset {} on stream '{}'",
                            qm.offset(), stream);
                }
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
        // Report actual message sizes for running average estimation
        MessageSizeTracker.record(bytesRead, recordsRead);
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
                RabbitMQSourceMetrics.taskMetric(RabbitMQSourceMetrics.READ_LATENCY_MS, readLatencyMs),
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
                        // Enqueue with a short timeout. context.processed() is NOT called
                        // here — it is deferred to the pull side (next()) so that credits
                        // are granted based on consumption rate, providing natural
                        // backpressure and avoiding blocking the Netty I/O thread.
                        if (!queue.offer(new QueuedMessage(
                                message, context.offset(), context.timestamp(), context),
                                5, TimeUnit.SECONDS)) {
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
                .strategy(ConsumerFlowStrategy.creditWhenHalfMessagesProcessed(
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
                    // RabbitMQ stream client requires both filter values and post-filter logic.
                    .postFilter(msg -> true)
                    .builder();
        }

        consumer = builder.build();

        LOG.info("Opened consumer for stream '{}' with offsets [{}, {})",
                stream, startOffset, endOffset);
    }

    private OffsetSpecification resolveOffsetSpec() {
        if (useConfiguredStartingOffset && options.getStartingOffsets() == StartingOffsetsMode.TIMESTAMP) {
            return OffsetSpecification.timestamp(options.getStartingTimestamp());
        }
        return OffsetSpecification.offset(startOffset);
    }

    private boolean isBrokerFilterConfigured() {
        return options.getFilterValues() != null && !options.getFilterValues().isEmpty();
    }

    private boolean hasStreamTailReachedPlannedEnd() {
        if (filteredTailReached) {
            return true;
        }
        try {
            StreamStats stats = environment.queryStreamStats(stream);
            long statsTail = stats.committedChunkId();
            long probedTail = probeLastMessageOffset();
            long tail = Math.max(statsTail, probedTail);
            if (tail < 0) {
                return endOffset <= 0;
            }
            if (tail >= endOffset - 1) {
                filteredTailReached = true;
                return true;
            }
            return false;
        } catch (Exception e) {
            LOG.debug("Unable to probe stream tail for filtered termination on '{}': {}",
                    stream, e.getMessage());
            return false;
        }
    }

    private long probeLastMessageOffset() {
        ArrayBlockingQueue<Long> observedOffsets = new ArrayBlockingQueue<>(1);
        Consumer probe = null;
        try {
            probe = environment.consumerBuilder()
                    .stream(stream)
                    .offset(OffsetSpecification.last())
                    .noTrackingStrategy()
                    .messageHandler((context, message) -> observedOffsets.offer(context.offset()))
                    .build();
            Long observed = observedOffsets.poll(5, TimeUnit.SECONDS);
            return observed == null ? -1 : observed;
        } catch (Exception e) {
            LOG.debug("Unable to probe last-message tail offset for stream '{}': {}",
                    stream, e.getMessage());
            return -1;
        } finally {
            if (probe != null) {
                try {
                    probe.close();
                } catch (Exception e) {
                    LOG.debug("Error closing probe consumer for stream '{}': {}",
                            stream, e.getMessage());
                }
            }
        }
    }

    private static ConnectorPostFilter createPostFilter(ConnectorOptions options) {
        String className = options.getFilterPostFilterClass();
        if (className == null || className.isEmpty()) {
            return null;
        }
        return ExtensionLoader.load(className, ConnectorPostFilter.class,
                ConnectorOptions.FILTER_POST_FILTER_CLASS);
    }

    private static Map<String, String> coerceMapToStrings(Map<String, Object> source) {
        if (source == null || source.isEmpty()) {
            return Map.of();
        }
        Map<String, String> out = new LinkedHashMap<>(source.size());
        for (Map.Entry<String, Object> entry : source.entrySet()) {
            out.put(entry.getKey(), entry.getValue() != null ? entry.getValue().toString() : null);
        }
        return out;
    }
}
