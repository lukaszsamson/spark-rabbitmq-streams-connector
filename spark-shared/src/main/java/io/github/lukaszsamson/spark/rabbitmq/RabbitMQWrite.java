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
    private final String queryId;

    RabbitMQWrite(ConnectorOptions options, StructType inputSchema,
                  RabbitMQDataWriterFactory writerFactory) {
        this(options, inputSchema, writerFactory, null);
    }

    RabbitMQWrite(ConnectorOptions options, StructType inputSchema,
                  RabbitMQDataWriterFactory writerFactory, String queryId) {
        this.options = options;
        this.inputSchema = inputSchema;
        this.writerFactory = writerFactory;
        this.queryId = queryId;
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
        validateStreamingDedupRequiresQueryId(options, queryId);
        return new RabbitMQStreamingWrite(writerFactory);
    }

    @Override
    public CustomMetric[] supportedCustomMetrics() {
        return RabbitMQSinkMetrics.SUPPORTED_METRICS;
    }

    static void validateStreamingDedupRequiresQueryId(ConnectorOptions options, String queryId) {
        String producerName = options.getProducerName();
        if (producerName == null || producerName.isBlank()) {
            return;
        }
        if (queryId == null || queryId.isBlank()) {
            throw new IllegalStateException(
                    "Streaming deduplication with 'producerName' requires a non-blank Spark " +
                            "queryId so the derived per-query producer name is unique across " +
                            "queries. Spark normally provides this via " +
                            "writeStream().queryName(...) or a checkpointed StreamingQuery; " +
                            "verify the query has a stable queryId.");
        }
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
        // RabbitMQ defines a named superstream producer's publishing IDs as one
        // logical sequence shared across partitions. getLastPublishingId()
        // intentionally returns the minimum partition cursor so applications can
        // replay the same logical sequence after restart and let advanced
        // partitions deduplicate already-stored IDs.
        //
        // Spark batch append is different: a new batch is not necessarily a replay
        // of the same logical sequence with the same routing. If we auto-generate
        // IDs from the superstream minimum for new rows, rows routed to advanced
        // partitions can be deduplicated as if they were replayed messages. Require
        // an explicit publishing_id column so the application owns the sequence and
        // routing/replay contract; otherwise fail fast.
        if (hasPublishingIdColumn(inputSchema)) {
            return;
        }
        throw illegalSuperStreamDedupState();
    }

    private static boolean hasPublishingIdColumn(StructType inputSchema) {
        if (inputSchema == null) {
            return false;
        }
        for (String name : inputSchema.fieldNames()) {
            if ("publishing_id".equalsIgnoreCase(name)) {
                return true;
            }
        }
        return false;
    }

    private static IllegalStateException illegalSuperStreamDedupState() {
        return new IllegalStateException(
                "Auto-deduplication via 'producerName' is not supported for superstream batch " +
                        "writes without an explicit 'publishing_id' column: RabbitMQ uses the " +
                        "minimum partition publishing-id cursor as a replay point for the same " +
                        "logical sequence, but a Spark batch append can contain new rows routed " +
                        "to already-advanced partitions and have them deduplicated. Either unset " +
                        "'producerName', switch to a single-stream sink, or supply an explicit " +
                        "'publishing_id' column so the application controls the sequence.");
    }
}
