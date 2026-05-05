package io.github.lukaszsamson.spark.rabbitmq;

import org.apache.spark.sql.connector.write.BatchWrite;
import org.apache.spark.sql.connector.write.DataWriterFactory;
import org.apache.spark.sql.connector.write.PhysicalWriteInfo;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Batch write implementation for RabbitMQ Streams.
 *
 * <p>Creates a {@link RabbitMQDataWriterFactory} for each batch write job.
 * Commit is a no-op (persistence is handled by individual {@link RabbitMQDataWriter#commit()}).
 * Abort is also a no-op (per-writer cleanup happens in {@link RabbitMQDataWriter#abort()}).
 *
 * <p>Batch writes are at-least-once and may be partially visible if some tasks
 * commit and others fail.
 */
final class RabbitMQBatchWrite implements BatchWrite {

    private static final Logger LOG = LoggerFactory.getLogger(RabbitMQBatchWrite.class);

    private final RabbitMQDataWriterFactory writerFactory;

    RabbitMQBatchWrite(RabbitMQDataWriterFactory writerFactory) {
        this.writerFactory = writerFactory;
    }

    @Override
    public DataWriterFactory createBatchWriterFactory(PhysicalWriteInfo info) {
        LOG.info("Creating batch writer factory for {} partitions", info.numPartitions());
        return writerFactory;
    }

    @Override
    public boolean useCommitCoordinator() {
        return true;
    }

    @Override
    public void onDataWriterCommit(WriterCommitMessage message) {
        if (message instanceof RabbitMQWriterCommitMessage rmqMsg) {
            LOG.debug("Partition {} committed: {} records, {} bytes",
                    rmqMsg.getPartitionId(), rmqMsg.getRecordsWritten(),
                    rmqMsg.getBytesWritten());
        }
    }

    @Override
    public void commit(WriterCommitMessage[] messages) {
        long totalRecords = 0;
        long totalBytes = 0;
        long totalWireBytes = 0;
        long totalConfirms = 0;
        long totalErrors = 0;
        long totalLatencyMs = 0;
        long totalFailureCount = 0;
        for (WriterCommitMessage msg : messages) {
            if (msg instanceof RabbitMQWriterCommitMessage rmqMsg) {
                totalRecords += rmqMsg.getRecordsWritten();
                totalBytes += rmqMsg.getBytesWritten();
                totalWireBytes += rmqMsg.getEstimatedWireBytesWritten();
                totalConfirms += rmqMsg.getPublishConfirms();
                totalErrors += rmqMsg.getPublishErrors();
                totalLatencyMs += rmqMsg.getWriteLatencyMs();
                totalFailureCount += rmqMsg.getConfirmationFailureCount();
            }
        }
        LOG.info("Batch write committed: {} partitions, {} records, {} payload bytes, " +
                        "{} estimated wire bytes, {} confirms, {} errors, {} ms total latency, " +
                        "{} confirmation failures",
                messages.length, totalRecords, totalBytes, totalWireBytes,
                totalConfirms, totalErrors, totalLatencyMs, totalFailureCount);
    }

    @Override
    public void abort(WriterCommitMessage[] messages) {
        LOG.warn("Batch write aborted with {} partition messages", messages.length);
    }
}
