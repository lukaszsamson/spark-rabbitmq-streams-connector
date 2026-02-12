package com.rabbitmq.spark.connector;

import org.apache.spark.sql.connector.write.Write;
import org.apache.spark.sql.connector.write.WriteBuilder;
import org.apache.spark.sql.internal.connector.SupportsStreamingUpdateAsAppend;
import org.apache.spark.sql.types.StructType;

/**
 * Builds a {@link RabbitMQWrite} after validating sink options and schema.
 *
 * <p>Validation runs eagerly at construction time: sink-specific options are
 * checked and the input schema is validated against the expected sink columns.
 */
final class RabbitMQWriteBuilder implements WriteBuilder, SupportsStreamingUpdateAsAppend {

    private final ConnectorOptions options;
    private final StructType inputSchema;
    private final String queryId;

    RabbitMQWriteBuilder(ConnectorOptions options, StructType inputSchema, String queryId) {
        this.options = options;
        this.inputSchema = inputSchema;
        this.queryId = queryId;

        // Validate sink options and schema eagerly
        options.validateForSink();
        SinkSchema.validate(inputSchema, options.isIgnoreUnknownColumns());
    }

    @Override
    public Write build() {
        RabbitMQDataWriterFactory factory = new RabbitMQDataWriterFactory(options, inputSchema);
        return new RabbitMQWrite(options, factory);
    }
}
