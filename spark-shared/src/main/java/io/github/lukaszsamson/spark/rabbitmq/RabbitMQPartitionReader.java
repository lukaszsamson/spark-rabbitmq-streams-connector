package io.github.lukaszsamson.spark.rabbitmq;

/**
 * Concrete {@link BaseRabbitMQPartitionReader} used by Spark 3.5, 4.0, and as
 * a default when no version-specific subclass is present.
 *
 * <p>Spark 4.1 provides its own subclass that additionally implements
 * {@code SupportsRealTimeRead}; the shade plugin replaces this class with
 * the Spark-4.1-specific variant in the spark41 artifact.
 */
final class RabbitMQPartitionReader extends BaseRabbitMQPartitionReader {

    RabbitMQPartitionReader(RabbitMQInputPartition partition, ConnectorOptions options) {
        super(partition, options);
    }
}
