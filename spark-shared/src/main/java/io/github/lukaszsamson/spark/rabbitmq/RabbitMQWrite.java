package io.github.lukaszsamson.spark.rabbitmq;

import org.apache.spark.SparkEnv;
import org.apache.spark.sql.connector.metric.CustomMetric;
import org.apache.spark.sql.connector.write.BatchWrite;
import org.apache.spark.sql.connector.write.Write;
import org.apache.spark.sql.connector.write.streaming.StreamingWrite;

/**
 * Logical representation of a RabbitMQ Streams write operation.
 *
 * <p>Supports both batch and streaming writes. Returns the same
 * {@link RabbitMQDataWriterFactory} in both modes; the factory adapts
 * to the Spark runtime via {@link RabbitMQBatchWrite} or
 * {@link RabbitMQStreamingWrite}.
 */
final class RabbitMQWrite implements Write {

    private final RabbitMQDataWriterFactory writerFactory;
    private final ConnectorOptions options;

    RabbitMQWrite(ConnectorOptions options, RabbitMQDataWriterFactory writerFactory) {
        this.options = options;
        this.writerFactory = writerFactory;
    }

    @Override
    public String description() {
        String target = options.isStreamMode()
                ? "stream=" + options.getStream()
                : "superstream=" + options.getSuperStream();
        return "RabbitMQWrite[" + target + "]";
    }

    @Override
    public BatchWrite toBatch() {
        return new RabbitMQBatchWrite(writerFactory);
    }

    @Override
    public StreamingWrite toStreaming() {
        validateStreamingDedupSpeculationCompatibility(options);
        return new RabbitMQStreamingWrite(writerFactory);
    }

    @Override
    public CustomMetric[] supportedCustomMetrics() {
        return RabbitMQSinkMetrics.SUPPORTED_METRICS;
    }

    static void validateStreamingDedupSpeculationCompatibility(ConnectorOptions options) {
        String producerName = options.getProducerName();
        if (producerName == null || producerName.isBlank()) {
            return;
        }
        SparkEnv sparkEnv = SparkEnv.get();
        if (sparkEnv == null || sparkEnv.conf() == null) {
            return;
        }
        if (!sparkEnv.conf().getBoolean("spark.speculation", false)) {
            return;
        }
        throw new IllegalStateException(
                "Streaming deduplication with 'producerName' is incompatible with " +
                        "'spark.speculation=true'. RabbitMQ Streams allows only one live producer " +
                        "per producer name. Disable speculation or unset 'producerName'.");
    }
}
