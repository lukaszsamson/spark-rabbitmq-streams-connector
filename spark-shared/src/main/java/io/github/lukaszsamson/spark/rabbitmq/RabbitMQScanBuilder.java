package io.github.lukaszsamson.spark.rabbitmq;

import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.types.StructType;

/**
 * Builds a {@link RabbitMQScan} for reading from RabbitMQ streams.
 *
 * <p>Validates source options at construction time as required by the spec.
 */
final class RabbitMQScanBuilder implements ScanBuilder {

    private final ConnectorOptions options;
    private final StructType schema;

    RabbitMQScanBuilder(ConnectorOptions options, StructType schema) {
        this.options = options;
        this.schema = schema;
        options.validateForSource();
    }

    @Override
    public Scan build() {
        return new RabbitMQScan(options, schema);
    }
}
