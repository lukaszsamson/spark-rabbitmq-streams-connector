package io.github.lukaszsamson.spark.rabbitmq;

import org.apache.spark.sql.connector.write.WriterCommitMessage;

/**
 * Commit message returned by {@link RabbitMQDataWriter#commit()}.
 *
 * <p>Carries summary information about the records written by a single
 * partition writer, which is aggregated on the driver side.
 */
final class RabbitMQWriterCommitMessage implements WriterCommitMessage {
    private static final long serialVersionUID = 2L;

    private final int partitionId;
    private final long taskId;
    private final long recordsWritten;
    private final long bytesWritten;
    private final long estimatedWireBytesWritten;
    private final long publishConfirms;
    private final long publishErrors;
    private final long writeLatencyMs;
    private final long confirmationFailureCount;
    private final long lastConfirmationFailureCode;

    RabbitMQWriterCommitMessage(int partitionId, long taskId,
                                long recordsWritten, long bytesWritten) {
        this(partitionId, taskId, recordsWritten, bytesWritten, 0L, 0L, 0L, 0L, 0L, -1L);
    }

    RabbitMQWriterCommitMessage(int partitionId, long taskId,
                                long recordsWritten, long bytesWritten,
                                long estimatedWireBytesWritten,
                                long publishConfirms, long publishErrors,
                                long writeLatencyMs,
                                long confirmationFailureCount,
                                long lastConfirmationFailureCode) {
        this.partitionId = partitionId;
        this.taskId = taskId;
        this.recordsWritten = recordsWritten;
        this.bytesWritten = bytesWritten;
        this.estimatedWireBytesWritten = estimatedWireBytesWritten;
        this.publishConfirms = publishConfirms;
        this.publishErrors = publishErrors;
        this.writeLatencyMs = writeLatencyMs;
        this.confirmationFailureCount = confirmationFailureCount;
        this.lastConfirmationFailureCode = lastConfirmationFailureCode;
    }

    int getPartitionId() { return partitionId; }
    long getTaskId() { return taskId; }
    long getRecordsWritten() { return recordsWritten; }
    long getBytesWritten() { return bytesWritten; }
    long getEstimatedWireBytesWritten() { return estimatedWireBytesWritten; }
    long getPublishConfirms() { return publishConfirms; }
    long getPublishErrors() { return publishErrors; }
    long getWriteLatencyMs() { return writeLatencyMs; }
    long getConfirmationFailureCount() { return confirmationFailureCount; }
    long getLastConfirmationFailureCode() { return lastConfirmationFailureCode; }

    @Override
    public String toString() {
        return "RabbitMQWriterCommitMessage{partitionId=" + partitionId +
                ", taskId=" + taskId +
                ", recordsWritten=" + recordsWritten +
                ", bytesWritten=" + bytesWritten +
                ", estimatedWireBytesWritten=" + estimatedWireBytesWritten +
                ", publishConfirms=" + publishConfirms +
                ", publishErrors=" + publishErrors +
                ", writeLatencyMs=" + writeLatencyMs +
                ", confirmationFailureCount=" + confirmationFailureCount +
                ", lastConfirmationFailureCode=" + lastConfirmationFailureCode + "}";
    }
}
