package io.github.lukaszsamson.spark.rabbitmq;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.streaming.PartitionOffset;
import org.apache.spark.sql.connector.read.streaming.SupportsRealTimeRead;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * Spark 4.1 partition reader that extends the shared base with
 * {@link SupportsRealTimeRead} support for real-time streaming mode.
 *
 * <p>All core push-to-pull bridge logic, consumer lifecycle management,
 * filtering, and metrics are inherited from {@link BaseRabbitMQPartitionReader}.
 * This subclass only adds the real-time read contract methods.
 */
final class RabbitMQPartitionReader extends BaseRabbitMQPartitionReader
        implements SupportsRealTimeRead<InternalRow> {

    RabbitMQPartitionReader(RabbitMQInputPartition partition, ConnectorOptions options) {
        super(partition, options);
    }

    // ---- SupportsRealTimeRead ----

    @Override
    public PartitionOffset getOffset() {
        long nextFromEmitted = lastEmittedOffset >= 0 ? lastEmittedOffset + 1 : startOffset;
        long nextFromObserved = lastObservedOffset >= 0 ? lastObservedOffset + 1 : startOffset;
        long nextOffset = Math.max(nextFromEmitted, nextFromObserved);
        return new RabbitMQPartitionOffset(stream, nextOffset);
    }

    @Override
    public RecordStatus nextWithTimeout(Long timeout) throws IOException {
        if (finished || closeCalled.get()) {
            finished = true;
            return RecordStatus.newStatusWithoutArrivalTime(false);
        }

        // Lazy initialization: create consumer on first call
        if (consumer == null) {
            try {
                initConsumer();
            } catch (Exception e) {
                if (!options.isFailOnDataLoss() && isMissingStreamException(e)) {
                    LOG.warn("Unable to initialize consumer for stream '{}' because stream/partition " +
                                    "is missing; completing split because failOnDataLoss=false",
                            stream);
                    finished = true;
                    return RecordStatus.newStatusWithoutArrivalTime(false);
                }
                throw new IOException("Failed to initialize consumer for stream '" + stream + "'", e);
            }
        }

        long deadlineNanos = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeout);
        long pollTimeoutMs = options.getPollTimeoutMs();

        while (true) {
            if (finished || closeCalled.get()) {
                finished = true;
                return RecordStatus.newStatusWithoutArrivalTime(false);
            }

            // Check for consumer errors
            Throwable error = consumerError.get();
            if (error != null) {
                throw new IOException("Consumer error on stream '" + stream + "'", error);
            }

            long remainingMs = TimeUnit.NANOSECONDS.toMillis(deadlineNanos - System.nanoTime());
            if (remainingMs <= 0) {
                return RecordStatus.newStatusWithoutArrivalTime(false);
            }

            QueuedMessage qm;
            try {
                long pollMs = Math.min(remainingMs, pollTimeoutMs);
                long pollStart = System.nanoTime();
                qm = queue.poll(pollMs, TimeUnit.MILLISECONDS);
                pollWaitMs += TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - pollStart);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                LOG.debug("Interrupted while reading from stream '{}'; finishing split", stream);
                finished = true;
                return RecordStatus.newStatusWithoutArrivalTime(false);
            }

            if (qm == null) {
                if (finished || closeCalled.get()) {
                    finished = true;
                    return RecordStatus.newStatusWithoutArrivalTime(false);
                }
                if (consumerClosed.get()) {
                    if (!options.isFailOnDataLoss() && isPlannedRangeNoLongerReachableDueToDataLoss()) {
                        LOG.warn("Consumer for stream '{}' closed and planned range [{}, {}) is no longer " +
                                        "reachable; completing split because failOnDataLoss=false",
                                stream, startOffset, endOffset);
                        dataLoss++;
                        finished = true;
                        return RecordStatus.newStatusWithoutArrivalTime(false);
                    }
                    throw new IOException(
                            "Consumer for stream '" + stream + "' closed while waiting for data");
                }
                continue;
            }

            // Credit flow: notify that this message has been consumed
            qm.context().processed();

            // Skip messages before start offset
            if (qm.offset() < startOffset) {
                continue;
            }

            // De-dup on reconnection: skip already-emitted offsets
            if (qm.offset() <= lastEmittedOffset) {
                continue;
            }

            if (qm.offset() > lastObservedOffset) {
                lastObservedOffset = qm.offset();
            }

            if (shouldSkipByTimestamp(qm.chunkTimestampMillis())) {
                continue;
            }

            currentRow = converter.convert(
                    qm.message(), stream, qm.offset(), qm.chunkTimestampMillis());
            lastEmittedOffset = qm.offset();
            recordsRead++;
            payloadBytesRead += MessageSizeEstimator.payloadBytes(qm.message());
            estimatedWireBytesRead += MessageSizeEstimator.estimatedWireBytes(qm.message());
            return RecordStatus.newStatusWithArrivalTimeMs(qm.chunkTimestampMillis());
        }
    }
}
