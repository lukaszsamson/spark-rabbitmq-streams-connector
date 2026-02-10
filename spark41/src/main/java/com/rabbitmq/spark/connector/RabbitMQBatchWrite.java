package com.rabbitmq.spark.connector;

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
        for (WriterCommitMessage msg : messages) {
            if (msg instanceof RabbitMQWriterCommitMessage rmqMsg) {
                totalRecords += rmqMsg.getRecordsWritten();
                totalBytes += rmqMsg.getBytesWritten();
            }
        }
        LOG.info("Batch write committed: {} partitions, {} total records, {} total bytes",
                messages.length, totalRecords, totalBytes);
    }

    @Override
    public void abort(WriterCommitMessage[] messages) {
        LOG.warn("Batch write aborted with {} partition messages", messages.length);
    }
}
