package io.github.lukaszsamson.spark.rabbitmq;

import org.apache.spark.SparkEnv;
import org.apache.spark.sql.connector.metric.CustomMetric;
import org.apache.spark.sql.connector.write.BatchWrite;
import org.apache.spark.sql.connector.write.Write;
import org.apache.spark.sql.connector.write.streaming.StreamingWrite;
import org.apache.spark.sql.types.StructType;

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
    private final StructType inputSchema;

    RabbitMQWrite(ConnectorOptions options, StructType inputSchema,
                  RabbitMQDataWriterFactory writerFactory) {
        this.options = options;
        this.inputSchema = inputSchema;
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
        validateBatchSuperStreamDedupCompatibility(options, inputSchema);
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

    static void validateBatchSuperStreamDedupCompatibility(ConnectorOptions options,
                                                           StructType inputSchema) {
        if (!options.isSuperStreamMode()) {
            return;
        }
        String producerName = options.getProducerName();
        if (producerName == null || producerName.isBlank()) {
            return;
        }
        // Auto-dedup seeds nextPublishingId from a single partition's
        // getLastPublishingId(); on a superstream, that is the MIN across partitions
        // and lets the broker silently drop already-stored ids on other partitions.
        // If the schema carries an explicit 'publishing_id' column, the user owns
        // monotonicity and assumes the per-partition risk; otherwise fail fast.
        if (inputSchema != null && inputSchema.getFieldIndex("publishing_id").isDefined()) {
            return;
        }
        throw new IllegalStateException(
                "Auto-deduplication via 'producerName' is not supported for superstream batch " +
                        "writes: the publishing-id seed is taken from a single partition and " +
                        "cannot guarantee monotonicity across all routed partitions, which can " +
                        "cause silent broker-side message drops. Either unset 'producerName', " +
                        "switch to a single-stream sink, or supply an explicit 'publishing_id' " +
                        "column so monotonicity is per-row controlled.");
    }
}
