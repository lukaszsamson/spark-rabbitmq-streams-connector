package com.rabbitmq.spark.connector;

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
        return new RabbitMQStreamingWrite(writerFactory);
    }

    @Override
    public CustomMetric[] supportedCustomMetrics() {
        return RabbitMQSinkMetrics.SUPPORTED_METRICS;
    }
}
