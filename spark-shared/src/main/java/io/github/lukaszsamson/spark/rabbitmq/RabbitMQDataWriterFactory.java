package io.github.lukaszsamson.spark.rabbitmq;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.write.DataWriter;
import org.apache.spark.sql.connector.write.DataWriterFactory;
import org.apache.spark.sql.connector.write.streaming.StreamingDataWriterFactory;
import org.apache.spark.sql.types.StructType;

/**
 * Factory that creates {@link RabbitMQDataWriter} instances on executors.
 *
 * <p>Implements both {@link DataWriterFactory} (for batch writes) and
 * {@link StreamingDataWriterFactory} (for streaming writes). The factory
 * is serialized to executors; it carries only configuration, no live
 * RabbitMQ connections.
 */
final class RabbitMQDataWriterFactory
        implements DataWriterFactory, StreamingDataWriterFactory {
    private static final long serialVersionUID = 1L;

    private final ConnectorOptions options;
    private final StructType inputSchema;

    RabbitMQDataWriterFactory(ConnectorOptions options, StructType inputSchema) {
        this.options = options;
        this.inputSchema = inputSchema;
    }

    @Override
    public DataWriter<InternalRow> createWriter(int partitionId, long taskId) {
        return new RabbitMQDataWriter(options, inputSchema, partitionId, taskId, -1);
    }

    @Override
    public DataWriter<InternalRow> createWriter(int partitionId, long taskId, long epochId) {
        return new RabbitMQDataWriter(options, inputSchema, partitionId, taskId, epochId);
    }
}
