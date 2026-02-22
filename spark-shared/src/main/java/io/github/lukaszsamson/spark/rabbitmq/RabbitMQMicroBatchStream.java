package io.github.lukaszsamson.spark.rabbitmq;

import org.apache.spark.sql.types.StructType;

/**
 * Concrete {@link BaseRabbitMQMicroBatchStream} used by Spark 3.5, 4.0, and as
 * a default when no version-specific subclass is present.
 *
 * <p>Spark 4.1 provides its own subclass that additionally implements
 * {@code SupportsRealTimeMode}; the shade plugin replaces this class with
 * the Spark-4.1-specific variant in the spark41 artifact.
 */
final class RabbitMQMicroBatchStream extends BaseRabbitMQMicroBatchStream {

    RabbitMQMicroBatchStream(ConnectorOptions options, StructType schema,
                              String checkpointLocation) {
        super(options, schema, checkpointLocation);
    }
}
