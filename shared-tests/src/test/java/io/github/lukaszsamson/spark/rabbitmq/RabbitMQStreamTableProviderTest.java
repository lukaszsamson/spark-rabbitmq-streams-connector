package io.github.lukaszsamson.spark.rabbitmq;

import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCapability;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.Write;
import org.apache.spark.sql.connector.write.WriteBuilder;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.*;

/**
 * Unit tests for {@link RabbitMQStreamTableProvider} schema inference and
 * capability exposure without requiring a broker.
 */
class RabbitMQStreamTableProviderTest {

    private final RabbitMQStreamTableProvider provider = new RabbitMQStreamTableProvider();

    private static Map<String, String> minimalOptions() {
        Map<String, String> opts = new HashMap<>();
        opts.put("stream", "test-stream");
        opts.put("endpoints", "localhost:5552");
        return opts;
    }

    @Nested
    class Registration {

        @Test
        void shortNameIsRabbitmqStreams() {
            assertThat(provider.shortName()).isEqualTo("rabbitmq_streams");
        }
    }

    @Nested
    class InferSchema {

        @Test
        void returnsSchemaWithAllMetadataFieldsByDefault() {
            var options = new CaseInsensitiveStringMap(minimalOptions());
            StructType schema = provider.inferSchema(options);

            // Fixed fields
            assertThat(schema.fieldIndex("value")).isGreaterThanOrEqualTo(0);
            assertThat(schema.fieldIndex("stream")).isGreaterThanOrEqualTo(0);
            assertThat(schema.fieldIndex("offset")).isGreaterThanOrEqualTo(0);
            assertThat(schema.fieldIndex("chunk_timestamp")).isGreaterThanOrEqualTo(0);

            // Default metadata fields
            assertThat(schema.fieldIndex("properties")).isGreaterThanOrEqualTo(0);
            assertThat(schema.fieldIndex("application_properties")).isGreaterThanOrEqualTo(0);
            assertThat(schema.fieldIndex("message_annotations")).isGreaterThanOrEqualTo(0);
            assertThat(schema.fieldIndex("creation_time")).isGreaterThanOrEqualTo(0);
            assertThat(schema.fieldIndex("routing_key")).isGreaterThanOrEqualTo(0);
        }

        @Test
        void respectsMetadataFieldsOption() {
            var map = minimalOptions();
            map.put("metadataFields", "properties");
            var options = new CaseInsensitiveStringMap(map);
            StructType schema = provider.inferSchema(options);

            // Fixed fields always present
            assertThat(schema.fieldIndex("value")).isGreaterThanOrEqualTo(0);
            assertThat(schema.fieldIndex("stream")).isGreaterThanOrEqualTo(0);

            // Only properties metadata
            assertThat(schema.fieldIndex("properties")).isGreaterThanOrEqualTo(0);

            // Others absent
            assertThatThrownBy(() -> schema.fieldIndex("application_properties"))
                    .isInstanceOf(IllegalArgumentException.class);
            assertThatThrownBy(() -> schema.fieldIndex("routing_key"))
                    .isInstanceOf(IllegalArgumentException.class);
        }

        @Test
        void emptyMetadataFieldsReturnsOnlyFixedColumns() {
            var map = minimalOptions();
            map.put("metadataFields", "");
            var options = new CaseInsensitiveStringMap(map);
            StructType schema = provider.inferSchema(options);

            assertThat(schema.fields()).hasSize(4); // value, stream, offset, chunk_timestamp
        }
    }

    @Nested
    class GetTable {

        @Test
        void returnsTableForValidStreamConfig() {
            Table table = provider.getTable(null, new Transform[]{}, minimalOptions());
            assertThat(table).isInstanceOf(RabbitMQStreamTable.class);
            assertThat(table.name()).isEqualTo("test-stream");
        }

        @Test
        void returnsTableForValidSuperStreamConfig() {
            var map = new HashMap<String, String>();
            map.put("superstream", "my-super-stream");
            map.put("endpoints", "localhost:5552");
            Table table = provider.getTable(null, new Transform[]{}, map);
            assertThat(table.name()).isEqualTo("my-super-stream");
        }

        @Test
        void tableHasCorrectCapabilities() {
            Table table = provider.getTable(null, new Transform[]{}, minimalOptions());
            assertThat(table.capabilities()).containsExactlyInAnyOrder(
                    TableCapability.BATCH_READ,
                    TableCapability.BATCH_WRITE,
                    TableCapability.MICRO_BATCH_READ,
                    TableCapability.STREAMING_WRITE,
                    TableCapability.ACCEPT_ANY_SCHEMA
            );
            assertThat(table.capabilities()).doesNotContain(TableCapability.CONTINUOUS_READ);
        }

        @Test
        void tableSchemaHasFixedColumns() {
            Table table = provider.getTable(null, new Transform[]{}, minimalOptions());
            StructType schema = table.schema();

            assertThat(schema.apply("value").dataType()).isEqualTo(DataTypes.BinaryType);
            assertThat(schema.apply("value").nullable()).isTrue();
            assertThat(schema.apply("stream").dataType()).isEqualTo(DataTypes.StringType);
            assertThat(schema.apply("stream").nullable()).isFalse();
            assertThat(schema.apply("offset").dataType()).isEqualTo(DataTypes.LongType);
            assertThat(schema.apply("offset").nullable()).isFalse();
            assertThat(schema.apply("chunk_timestamp").dataType()).isEqualTo(DataTypes.TimestampType);
            assertThat(schema.apply("chunk_timestamp").nullable()).isFalse();
        }

        @Test
        void tableExposesParsedOptions() {
            Table table = provider.getTable(null, new Transform[]{}, minimalOptions());
            RabbitMQStreamTable streamTable = (RabbitMQStreamTable) table;
            ConnectorOptions opts = streamTable.getOptions();

            assertThat(opts.getStream()).isEqualTo("test-stream");
            assertThat(opts.getEndpoints()).isEqualTo("localhost:5552");
        }

        @Test
        void tableSchemaPropertiesStruct() {
            Table table = provider.getTable(null, new Transform[]{}, minimalOptions());
            StructType schema = table.schema();
            StructType props = (StructType) schema.apply("properties").dataType();

            assertThat(props.fieldNames()).containsExactly(
                    "message_id", "user_id", "to", "subject", "reply_to",
                    "correlation_id", "content_type", "content_encoding",
                    "absolute_expiry_time", "creation_time", "group_id",
                    "group_sequence", "reply_to_group_id"
            );
        }

        @Test
        void newScanBuilderMergesTableOptionsWithPerOperationOverrides() {
            RabbitMQStreamTable table = (RabbitMQStreamTable) provider.getTable(
                    null, new Transform[]{}, minimalOptions());

            Map<String, String> scanOverrides = new HashMap<>();
            scanOverrides.put("stream", "scan-override");
            Scan scan = table.newScanBuilder(new CaseInsensitiveStringMap(scanOverrides)).build();

            assertThat(scan.description()).contains("stream=scan-override");
            assertThat(scan.readSchema().fieldNames()).contains("value", "stream", "offset", "chunk_timestamp");
        }

        @Test
        void newWriteBuilderMergesTableOptionsWithPerOperationOverrides() {
            RabbitMQStreamTable table = (RabbitMQStreamTable) provider.getTable(
                    null, new Transform[]{}, minimalOptions());

            Map<String, String> writeOverrides = new HashMap<>();
            writeOverrides.put("stream", "write-override");
            LogicalWriteInfo info = logicalWriteInfo(minimalSinkSchema(), "test-query", writeOverrides);

            WriteBuilder builder = table.newWriteBuilder(info);
            Write write = builder.build();
            assertThat(write.description()).contains("stream=write-override");
        }

        @Test
        void failsOnMissingStreamAndSuperstream() {
            var map = new HashMap<String, String>();
            map.put("endpoints", "localhost:5552");
            assertThatThrownBy(() ->
                    provider.getTable(null, new Transform[]{}, map))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("stream")
                    .hasMessageContaining("superstream");
        }

        @Test
        void failsOnBothStreamAndSuperstream() {
            var map = minimalOptions();
            map.put("superstream", "my-super-stream");
            assertThatThrownBy(() ->
                    provider.getTable(null, new Transform[]{}, map))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("both");
        }

        @Test
        void failsOnMissingEndpointsAndUris() {
            var map = new HashMap<String, String>();
            map.put("stream", "test-stream");
            assertThatThrownBy(() ->
                    provider.getTable(null, new Transform[]{}, map))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("endpoints")
                    .hasMessageContaining("uris");
        }

        @Test
        void caseInsensitiveOptionsWork() {
            var map = new HashMap<String, String>();
            map.put("Stream", "test-stream");
            map.put("Endpoints", "localhost:5552");
            var caseInsensitive = new CaseInsensitiveStringMap(map);
            // CaseInsensitiveStringMap implements Map<String, String>
            Table table = provider.getTable(null, new Transform[]{}, caseInsensitive);
            assertThat(table.name()).isEqualTo("test-stream");
        }

        @Test
        void acceptsLowercasedAddressResolverInPlainMap() {
            // Spark Connect batch can pass lowercased keys in a plain map.
            // ConnectorOptions must still resolve the custom resolver option.
            var map = new HashMap<String, String>();
            map.put("stream", "test-stream");
            map.put("endpoints", "localhost:5552");
            map.put("addressresolverclass",
                    "io.github.lukaszsamson.spark.rabbitmq.EnvironmentBuilderHelperTest$TestAddressResolver");

            Table table = provider.getTable(null, new Transform[]{}, map);
            RabbitMQStreamTable streamTable = (RabbitMQStreamTable) table;

            assertThat(streamTable.getOptions().getAddressResolverClass())
                    .isEqualTo(
                            "io.github.lukaszsamson.spark.rabbitmq.EnvironmentBuilderHelperTest$TestAddressResolver");
        }
    }

    private static StructType minimalSinkSchema() {
        return new StructType(new StructField[]{
                new StructField("value", DataTypes.BinaryType, false, Metadata.empty()),
        });
    }

    private static LogicalWriteInfo logicalWriteInfo(
            StructType schema, String queryId, Map<String, String> options) {
        return new LogicalWriteInfo() {
            @Override
            public CaseInsensitiveStringMap options() {
                return new CaseInsensitiveStringMap(options);
            }

            @Override
            public String queryId() {
                return queryId;
            }

            @Override
            public StructType schema() {
                return schema;
            }
        };
    }
}
