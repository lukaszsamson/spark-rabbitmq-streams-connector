package io.github.lukaszsamson.spark.rabbitmq;

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
        LOG.info("Streaming write committed epoch {}: {} partitions, {} records, {} payload bytes, " +
                        "{} estimated wire bytes, {} confirms, {} errors, {} ms total latency, " +
                        "{} confirmation failures",
                epochId, messages.length, totalRecords, totalBytes, totalWireBytes,
                totalConfirms, totalErrors, totalLatencyMs, totalFailureCount);
    }

    @Override
    public void abort(long epochId, WriterCommitMessage[] messages) {
        LOG.warn("Streaming write aborted for epoch {} with {} partition messages",
                epochId, messages.length);
    }
}
