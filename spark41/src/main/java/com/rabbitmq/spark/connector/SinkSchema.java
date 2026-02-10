package com.rabbitmq.spark.connector;

import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Defines and validates the sink input schema for RabbitMQ Streams writes.
 */
public final class SinkSchema {

    /** Valid sink column definitions (name → expected Spark type). */
    private static final Map<String, DataType> VALID_COLUMNS;

    static {
        VALID_COLUMNS = new LinkedHashMap<>();
        VALID_COLUMNS.put("value", DataTypes.BinaryType);
        VALID_COLUMNS.put("stream", DataTypes.StringType);
        VALID_COLUMNS.put("routing_key", DataTypes.StringType);
        VALID_COLUMNS.put("properties", RabbitMQStreamTable.PROPERTIES_STRUCT);
        VALID_COLUMNS.put("application_properties",
                DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType));
        VALID_COLUMNS.put("message_annotations",
                DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType));
        VALID_COLUMNS.put("creation_time", DataTypes.TimestampType);
    }

    private SinkSchema() {}

    /**
     * Build the full sink schema (all optional columns included).
     */
    public static StructType fullSchema() {
        List<StructField> fields = new ArrayList<>();
        fields.add(new StructField("value", DataTypes.BinaryType, false, Metadata.empty()));
        fields.add(new StructField("stream", DataTypes.StringType, true, Metadata.empty()));
        fields.add(new StructField("routing_key", DataTypes.StringType, true, Metadata.empty()));
        fields.add(new StructField("properties", RabbitMQStreamTable.PROPERTIES_STRUCT,
                true, Metadata.empty()));
        fields.add(new StructField("application_properties",
                DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType),
                true, Metadata.empty()));
        fields.add(new StructField("message_annotations",
                DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType),
                true, Metadata.empty()));
        fields.add(new StructField("creation_time", DataTypes.TimestampType,
                true, Metadata.empty()));
        return new StructType(fields.toArray(new StructField[0]));
    }

    /**
     * Validate an input schema against the expected sink columns.
     *
     * @param inputSchema the DataFrame schema being written
     * @param ignoreUnknownColumns if {@code true}, extra columns are silently ignored;
     *                             if {@code false}, extra columns cause an error
     * @throws IllegalArgumentException if the schema is invalid
     */
    public static void validate(StructType inputSchema, boolean ignoreUnknownColumns) {
        // 'value' column is required
        if (!hasField(inputSchema, "value")) {
            throw new IllegalArgumentException(
                    "Sink schema must contain a 'value' column of BinaryType");
        }

        // Check types of known columns
        for (StructField field : inputSchema.fields()) {
            String name = field.name().toLowerCase();
            DataType expectedType = VALID_COLUMNS.get(name);
            if (expectedType != null) {
                validateColumnType(name, field.dataType(), expectedType);
            }
        }

        // Check for unknown columns
        if (!ignoreUnknownColumns) {
            List<String> unknownColumns = new ArrayList<>();
            for (StructField field : inputSchema.fields()) {
                if (!VALID_COLUMNS.containsKey(field.name().toLowerCase())) {
                    unknownColumns.add(field.name());
                }
            }
            if (!unknownColumns.isEmpty()) {
                String unknown = unknownColumns.stream()
                        .collect(Collectors.joining("', '", "'", "'"));
                String valid = VALID_COLUMNS.keySet().stream()
                        .collect(Collectors.joining(", "));
                throw new IllegalArgumentException(
                        "Sink schema contains unrecognized columns: " + unknown +
                                ". Valid columns are: " + valid +
                                ". Set 'ignoreUnknownColumns' to true to ignore extra columns.");
            }
        }
    }

    private static boolean hasField(StructType schema, String name) {
        for (StructField field : schema.fields()) {
            if (field.name().equalsIgnoreCase(name)) {
                return true;
            }
        }
        return false;
    }

    private static void validateColumnType(String name, DataType actual, DataType expected) {
        // For struct types, just check that it's a struct (allow schema evolution)
        if (expected instanceof StructType && actual instanceof StructType) {
            return;
        }
        // Use simpleString comparison to avoid Spark runtime initialization (SqlApiConf)
        if (!actual.simpleString().equals(expected.simpleString())) {
            throw new IllegalArgumentException(
                    "Column '" + name + "' has type " + actual.simpleString() +
                            " but expected " + expected.simpleString());
        }
    }
}
