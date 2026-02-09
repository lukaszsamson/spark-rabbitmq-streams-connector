package com.rabbitmq.spark.connector;

import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableProvider;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.sources.DataSourceRegister;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

import java.util.Map;

/**
 * Spark DataSource V2 {@link TableProvider} for RabbitMQ Streams.
 *
 * <p>Registered as {@code rabbitmq_streams} via {@link DataSourceRegister}.
 */
public class RabbitMQStreamTableProvider implements TableProvider, DataSourceRegister {

    @Override
    public String shortName() {
        return "rabbitmq_streams";
    }

    @Override
    public StructType inferSchema(CaseInsensitiveStringMap options) {
        // TODO: Milestone 1 – return fixed source schema
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public Table getTable(StructType schema, Transform[] partitioning, Map<String, String> properties) {
        // TODO: Milestone 1 – parse options, validate, return Table
        throw new UnsupportedOperationException("Not yet implemented");
    }
}
