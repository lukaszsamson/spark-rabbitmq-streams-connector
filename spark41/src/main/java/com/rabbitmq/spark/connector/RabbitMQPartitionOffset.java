package com.rabbitmq.spark.connector;

import org.apache.spark.sql.connector.read.streaming.PartitionOffset;

import java.util.Objects;

/**
 * Per-partition offset for real-time mode streaming.
 *
 * <p>Carries the stream name and the next offset (exclusive) so that
 * {@link RabbitMQMicroBatchStream#mergeOffsets} can reconstruct a global
 * {@link RabbitMQStreamOffset} from per-reader progress.
 */
final class RabbitMQPartitionOffset implements PartitionOffset {

    private static final long serialVersionUID = 1L;

    private final String stream;
    private final long nextOffset;

    /**
     * @param stream     the RabbitMQ stream name
     * @param nextOffset exclusive next offset (last emitted + 1)
     */
    RabbitMQPartitionOffset(String stream, long nextOffset) {
        this.stream = Objects.requireNonNull(stream, "stream");
        this.nextOffset = nextOffset;
    }

    public String getStream() {
        return stream;
    }

    public long getNextOffset() {
        return nextOffset;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof RabbitMQPartitionOffset that)) return false;
        return nextOffset == that.nextOffset && stream.equals(that.stream);
    }

    @Override
    public int hashCode() {
        return Objects.hash(stream, nextOffset);
    }

    @Override
    public String toString() {
        return "RabbitMQPartitionOffset{stream='" + stream + "', nextOffset=" + nextOffset + "}";
    }
}
