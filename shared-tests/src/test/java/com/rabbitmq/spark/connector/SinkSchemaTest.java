package com.rabbitmq.spark.connector;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.*;

/**
 * Unit tests for {@link SinkSchema} validation and column definitions
 * without requiring a broker.
 */
class SinkSchemaTest {

    @Test
    void fullSchemaContainsAllColumns() {
        StructType schema = SinkSchema.fullSchema();
        assertThat(schema.fieldNames()).containsExactly(
                "value", "publishing_id", "stream", "routing_key", "properties",
                "application_properties", "message_annotations", "creation_time");
    }

    @Test
    void fullSchemaValueIsNotNullable() {
        StructType schema = SinkSchema.fullSchema();
        assertThat(schema.apply("value").nullable()).isFalse();
    }

    @Test
    void fullSchemaOptionalColumnsAreNullable() {
        StructType schema = SinkSchema.fullSchema();
        for (String name : new String[]{"publishing_id", "stream", "routing_key", "properties",
                "application_properties", "message_annotations", "creation_time"}) {
            assertThat(schema.apply(name).nullable())
                    .as("Column '%s' should be nullable", name)
                    .isTrue();
        }
    }

    // ---- validate: valid schemas ----

    @Test
    void acceptsMinimalValueOnlySchema() {
        StructType schema = new StructType(new StructField[]{
                new StructField("value", DataTypes.BinaryType, false, Metadata.empty()),
        });
        assertThatNoException().isThrownBy(() -> SinkSchema.validate(schema, false));
    }

    @Test
    void acceptsFullSinkSchema() {
        assertThatNoException().isThrownBy(() -> SinkSchema.validate(SinkSchema.fullSchema(), false));
    }

    @Test
    void acceptsSubsetOfValidColumns() {
        StructType schema = new StructType(new StructField[]{
                new StructField("value", DataTypes.BinaryType, false, Metadata.empty()),
                new StructField("routing_key", DataTypes.StringType, true, Metadata.empty()),
        });
        assertThatNoException().isThrownBy(() -> SinkSchema.validate(schema, false));
    }

    // ---- validate: missing value ----

    @Test
    void rejectsMissingValueColumn() {
        StructType schema = new StructType(new StructField[]{
                new StructField("stream", DataTypes.StringType, true, Metadata.empty()),
        });
        assertThatThrownBy(() -> SinkSchema.validate(schema, false))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("value")
                .hasMessageContaining("BinaryType");
    }

    // ---- validate: unknown columns ----

    @Test
    void rejectsUnknownColumns() {
        StructType schema = new StructType(new StructField[]{
                new StructField("value", DataTypes.BinaryType, false, Metadata.empty()),
                new StructField("extra_col", DataTypes.StringType, true, Metadata.empty()),
        });
        assertThatThrownBy(() -> SinkSchema.validate(schema, false))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("extra_col")
                .hasMessageContaining("unrecognized")
                .hasMessageContaining("ignoreUnknownColumns");
    }

    @Test
    void rejectsMultipleUnknownColumns() {
        StructType schema = new StructType(new StructField[]{
                new StructField("value", DataTypes.BinaryType, false, Metadata.empty()),
                new StructField("foo", DataTypes.StringType, true, Metadata.empty()),
                new StructField("bar", DataTypes.IntegerType, true, Metadata.empty()),
        });
        assertThatThrownBy(() -> SinkSchema.validate(schema, false))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("foo")
                .hasMessageContaining("bar");
    }

    @Test
    void ignoresUnknownColumnsWhenConfigured() {
        StructType schema = new StructType(new StructField[]{
                new StructField("value", DataTypes.BinaryType, false, Metadata.empty()),
                new StructField("extra_col", DataTypes.StringType, true, Metadata.empty()),
        });
        assertThatNoException().isThrownBy(() -> SinkSchema.validate(schema, true));
    }

    // ---- validate: type mismatches ----

    @Test
    void rejectsWrongValueType() {
        StructType schema = new StructType(new StructField[]{
                new StructField("value", DataTypes.StringType, false, Metadata.empty()),
        });
        assertThatThrownBy(() -> SinkSchema.validate(schema, false))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("value")
                .hasMessageContaining("string")
                .hasMessageContaining("binary");
    }

    @Test
    void rejectsWrongStreamType() {
        StructType schema = new StructType(new StructField[]{
                new StructField("value", DataTypes.BinaryType, false, Metadata.empty()),
                new StructField("stream", DataTypes.IntegerType, true, Metadata.empty()),
        });
        assertThatThrownBy(() -> SinkSchema.validate(schema, false))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("stream")
                .hasMessageContaining("int");
    }

    @Test
    void rejectsWrongCreationTimeType() {
        StructType schema = new StructType(new StructField[]{
                new StructField("value", DataTypes.BinaryType, false, Metadata.empty()),
                new StructField("creation_time", DataTypes.LongType, true, Metadata.empty()),
        });
        assertThatThrownBy(() -> SinkSchema.validate(schema, false))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("creation_time")
                .hasMessageContaining("bigint");
    }

    @Test
    void rejectsWrongPublishingIdType() {
        StructType schema = new StructType(new StructField[]{
                new StructField("value", DataTypes.BinaryType, false, Metadata.empty()),
                new StructField("publishing_id", DataTypes.StringType, true, Metadata.empty()),
        });
        assertThatThrownBy(() -> SinkSchema.validate(schema, false))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("publishing_id")
                .hasMessageContaining("string")
                .hasMessageContaining("bigint");
    }

    @Test
    void acceptsPropertiesStructWithMatchingFields() {
        StructType schema = new StructType(new StructField[]{
                new StructField("value", DataTypes.BinaryType, false, Metadata.empty()),
                new StructField("properties", RabbitMQStreamTable.PROPERTIES_STRUCT,
                        true, Metadata.empty()),
        });
        assertThatNoException().isThrownBy(() -> SinkSchema.validate(schema, false));
    }

    @Test
    void acceptsPropertiesStructWithSubset() {
        // A user struct with fewer fields should still be accepted as a struct
        StructType subsetStruct = new StructType(new StructField[]{
                new StructField("message_id", DataTypes.StringType, true, Metadata.empty()),
                new StructField("content_type", DataTypes.StringType, true, Metadata.empty()),
        });
        StructType schema = new StructType(new StructField[]{
                new StructField("value", DataTypes.BinaryType, false, Metadata.empty()),
                new StructField("properties", subsetStruct, true, Metadata.empty()),
        });
        assertThatNoException().isThrownBy(() -> SinkSchema.validate(schema, false));
    }

    @Test
    void rejectsPropertiesStructWithUnknownField() {
        StructType badStruct = new StructType(new StructField[]{
                new StructField("message_id", DataTypes.StringType, true, Metadata.empty()),
                new StructField("bogus_field", DataTypes.StringType, true, Metadata.empty()),
        });
        StructType schema = new StructType(new StructField[]{
                new StructField("value", DataTypes.BinaryType, false, Metadata.empty()),
                new StructField("properties", badStruct, true, Metadata.empty()),
        });
        assertThatThrownBy(() -> SinkSchema.validate(schema, false))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("bogus_field")
                .hasMessageContaining("unrecognized");
    }

    @Test
    void rejectsPropertiesStructFieldWithWrongType() {
        StructType badStruct = new StructType(new StructField[]{
                new StructField("message_id", DataTypes.IntegerType, true, Metadata.empty()),
        });
        StructType schema = new StructType(new StructField[]{
                new StructField("value", DataTypes.BinaryType, false, Metadata.empty()),
                new StructField("properties", badStruct, true, Metadata.empty()),
        });
        assertThatThrownBy(() -> SinkSchema.validate(schema, false))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("message_id")
                .hasMessageContaining("int")
                .hasMessageContaining("string");
    }

    @Test
    void rejectsPropertiesNotAStruct() {
        StructType schema = new StructType(new StructField[]{
                new StructField("value", DataTypes.BinaryType, false, Metadata.empty()),
                new StructField("properties", DataTypes.StringType, true, Metadata.empty()),
        });
        assertThatThrownBy(() -> SinkSchema.validate(schema, false))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("properties")
                .hasMessageContaining("struct");
    }
}
