package com.rabbitmq.spark.connector;

import org.apache.spark.sql.connector.write.PhysicalWriteInfo;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
import org.apache.spark.sql.connector.write.streaming.StreamingDataWriterFactory;
import org.apache.spark.sql.connector.write.streaming.StreamingWrite;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Streaming write implementation for RabbitMQ Streams.
 *
 * <p>Creates a {@link RabbitMQDataWriterFactory} that produces epoch-aware
 * writers. {@link #commit(long, WriterCommitMessage[])} is idempotent;
 * repeated commits for the same epoch are safe.
 *
 * <p>At-least-once delivery: if a task fails before commit, Spark will
 * re-run the task for the same epoch, potentially producing duplicates.
 */
final class RabbitMQStreamingWrite implements StreamingWrite {

    private static final Logger LOG = LoggerFactory.getLogger(RabbitMQStreamingWrite.class);

    private final RabbitMQDataWriterFactory writerFactory;

    RabbitMQStreamingWrite(RabbitMQDataWriterFactory writerFactory) {
        this.writerFactory = writerFactory;
    }

    @Override
    public StreamingDataWriterFactory createStreamingWriterFactory(PhysicalWriteInfo info) {
        LOG.info("Creating streaming writer factory for {} partitions", info.numPartitions());
        return writerFactory;
    }

    @Override
    public boolean useCommitCoordinator() {
        return false;
    }

    @Override
    public void commit(long epochId, WriterCommitMessage[] messages) {
        long totalRecords = 0;
        long totalBytes = 0;
        for (WriterCommitMessage msg : messages) {
            if (msg instanceof RabbitMQWriterCommitMessage rmqMsg) {
                totalRecords += rmqMsg.getRecordsWritten();
                totalBytes += rmqMsg.getBytesWritten();
            }
        }
        LOG.info("Streaming write committed epoch {}: {} partitions, {} total records, {} total bytes",
                epochId, messages.length, totalRecords, totalBytes);
        for (int i = 0; i < messages.length; i++) {
            messages[i] = null;
        }
    }

    @Override
    public void abort(long epochId, WriterCommitMessage[] messages) {
        LOG.warn("Streaming write aborted for epoch {} with {} partition messages",
                epochId, messages.length);
    }
}
