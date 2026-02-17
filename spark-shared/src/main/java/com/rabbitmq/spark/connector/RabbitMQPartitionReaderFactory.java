package com.rabbitmq.spark.connector;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.types.StructType;

/**
 * Factory for creating {@link RabbitMQPartitionReader} instances on executors.
 *
 * <p>Serialized to executors along with the connector options and schema.
 */
final class RabbitMQPartitionReaderFactory implements PartitionReaderFactory {
    private static final long serialVersionUID = 1L;

    private final ConnectorOptions options;
    private final StructType schema;

    RabbitMQPartitionReaderFactory(ConnectorOptions options, StructType schema) {
        this.options = options;
        this.schema = schema;
    }

    @Override
    public PartitionReader<InternalRow> createReader(InputPartition partition) {
        RabbitMQInputPartition rmqPartition = (RabbitMQInputPartition) partition;
        return new RabbitMQPartitionReader(rmqPartition, options);
    }
}
