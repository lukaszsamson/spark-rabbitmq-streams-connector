package io.github.lukaszsamson.spark.rabbitmq;

import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Locale;
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
        VALID_COLUMNS.put("publishing_id", DataTypes.LongType);
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
        fields.add(new StructField("publishing_id", DataTypes.LongType, true, Metadata.empty()));
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
            String name = field.name().toLowerCase(Locale.ROOT);
            DataType expectedType = VALID_COLUMNS.get(name);
            if (expectedType != null) {
                validateColumnType(name, field.dataType(), expectedType);
            }
        }

        // Check for unknown columns
        if (!ignoreUnknownColumns) {
            List<String> unknownColumns = new ArrayList<>();
            for (StructField field : inputSchema.fields()) {
                if (!VALID_COLUMNS.containsKey(field.name().toLowerCase(Locale.ROOT))) {
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
        // For struct types (properties), validate field types individually
        if (expected instanceof StructType expectedStruct) {
            if (actual instanceof StructType actualStruct) {
                validatePropertiesStruct(actualStruct, expectedStruct);
            } else {
                throw new IllegalArgumentException(
                        "Column '" + name + "' has type " + actual.simpleString() +
                                " but expected a struct");
            }
            return;
        }
        // Use simpleString comparison to avoid Spark runtime initialization (SqlApiConf)
        if (!actual.simpleString().equals(expected.simpleString())) {
            throw new IllegalArgumentException(
                    "Column '" + name + "' has type " + actual.simpleString() +
                            " but expected " + expected.simpleString());
        }
    }

    /**
     * Validate fields in a user-supplied properties struct against the expected struct.
     * Each field present in the actual struct must exist in the expected struct with a matching type.
     */
    private static void validatePropertiesStruct(StructType actual, StructType expected) {
        Map<String, DataType> expectedFields = new LinkedHashMap<>();
        for (StructField field : expected.fields()) {
            expectedFields.put(field.name().toLowerCase(Locale.ROOT), field.dataType());
        }
        for (StructField field : actual.fields()) {
            String fieldName = field.name().toLowerCase(Locale.ROOT);
            DataType expectedType = expectedFields.get(fieldName);
            if (expectedType == null) {
                throw new IllegalArgumentException(
                        "Properties struct contains unrecognized field '" + field.name() +
                                "'. Valid fields are: " + String.join(", ", expectedFields.keySet()));
            }
            if (!field.dataType().simpleString().equals(expectedType.simpleString())) {
                throw new IllegalArgumentException(
                        "Properties field '" + field.name() + "' has type " +
                                field.dataType().simpleString() + " but expected " +
                                expectedType.simpleString());
            }
        }
    }
}
