package io.github.lukaszsamson.spark.rabbitmq;

import org.apache.spark.sql.connector.write.WriterCommitMessage;

/**
 * Commit message returned by {@link RabbitMQDataWriter#commit()}.
 *
 * <p>Carries summary information about the records written by a single
 * partition writer, which is aggregated on the driver side.
 */
final class RabbitMQWriterCommitMessage implements WriterCommitMessage {
    private static final long serialVersionUID = 1L;

    private final int partitionId;
    private final long taskId;
    private final long recordsWritten;
    private final long bytesWritten;

    RabbitMQWriterCommitMessage(int partitionId, long taskId,
                                long recordsWritten, long bytesWritten) {
        this.partitionId = partitionId;
        this.taskId = taskId;
        this.recordsWritten = recordsWritten;
        this.bytesWritten = bytesWritten;
    }

    int getPartitionId() { return partitionId; }
    long getTaskId() { return taskId; }
    long getRecordsWritten() { return recordsWritten; }
    long getBytesWritten() { return bytesWritten; }

    @Override
    public String toString() {
        return "RabbitMQWriterCommitMessage{partitionId=" + partitionId +
                ", taskId=" + taskId +
                ", recordsWritten=" + recordsWritten +
                ", bytesWritten=" + bytesWritten + "}";
    }
}
