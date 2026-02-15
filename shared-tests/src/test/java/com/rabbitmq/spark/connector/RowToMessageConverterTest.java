package com.rabbitmq.spark.connector;

import com.rabbitmq.stream.Message;
import com.rabbitmq.stream.MessageBuilder;
import com.rabbitmq.stream.codec.QpidProtonCodec;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.catalyst.util.ArrayBasedMapData;
import org.apache.spark.sql.catalyst.util.GenericArrayData;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.*;

/**
 * Unit tests for {@link RowToMessageConverter} field mapping without requiring
 * a RabbitMQ broker. Integration of produced messages is covered by it-tests.
 */
class RowToMessageConverterTest {

    private static final QpidProtonCodec CODEC = new QpidProtonCodec();

    // ---- Schema helpers ----

    private static StructType valueOnlySchema() {
        return new StructType(new StructField[]{
                new StructField("value", DataTypes.BinaryType, false, Metadata.empty()),
        });
    }

    private static StructType schemaWithStream() {
        return new StructType(new StructField[]{
                new StructField("value", DataTypes.BinaryType, false, Metadata.empty()),
                new StructField("stream", DataTypes.StringType, true, Metadata.empty()),
        });
    }

    private static StructType schemaWithRoutingKey() {
        return new StructType(new StructField[]{
                new StructField("value", DataTypes.BinaryType, false, Metadata.empty()),
                new StructField("routing_key", DataTypes.StringType, true, Metadata.empty()),
        });
    }

    private static StructType schemaWithCreationTime() {
        return new StructType(new StructField[]{
                new StructField("value", DataTypes.BinaryType, false, Metadata.empty()),
                new StructField("creation_time", DataTypes.TimestampType, true, Metadata.empty()),
        });
    }

    private static StructType schemaWithProperties() {
        return new StructType(new StructField[]{
                new StructField("value", DataTypes.BinaryType, false, Metadata.empty()),
                new StructField("properties", RabbitMQStreamTable.PROPERTIES_STRUCT,
                        true, Metadata.empty()),
        });
    }

    private static StructType schemaWithPropertiesAndTopLevelCreationTime() {
        return new StructType(new StructField[]{
                new StructField("value", DataTypes.BinaryType, false, Metadata.empty()),
                new StructField("properties", RabbitMQStreamTable.PROPERTIES_STRUCT,
                        true, Metadata.empty()),
                new StructField("creation_time", DataTypes.TimestampType, true, Metadata.empty()),
        });
    }

    private static StructType schemaWithPropertiesFirst(StructType propertiesStruct) {
        return new StructType(new StructField[]{
                new StructField("properties", propertiesStruct, true, Metadata.empty()),
                new StructField("value", DataTypes.BinaryType, false, Metadata.empty()),
        });
    }

    private static StructType schemaWithRoutingKeyFirst() {
        return new StructType(new StructField[]{
                new StructField("routing_key", DataTypes.StringType, true, Metadata.empty()),
                new StructField("value", DataTypes.BinaryType, false, Metadata.empty()),
        });
    }

    private static StructType schemaWithStreamFirst() {
        return new StructType(new StructField[]{
                new StructField("stream", DataTypes.StringType, true, Metadata.empty()),
                new StructField("value", DataTypes.BinaryType, false, Metadata.empty()),
        });
    }

    private static StructType schemaWithCreationTimeFirst() {
        return new StructType(new StructField[]{
                new StructField("creation_time", DataTypes.TimestampType, true, Metadata.empty()),
                new StructField("value", DataTypes.BinaryType, false, Metadata.empty()),
        });
    }

    private static StructType schemaWithAppPropertiesFirst() {
        return new StructType(new StructField[]{
                new StructField("application_properties",
                        DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType),
                        true, Metadata.empty()),
                new StructField("value", DataTypes.BinaryType, false, Metadata.empty()),
        });
    }

    private static StructType schemaWithMsgAnnotationsFirst() {
        return new StructType(new StructField[]{
                new StructField("message_annotations",
                        DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType),
                        true, Metadata.empty()),
                new StructField("value", DataTypes.BinaryType, false, Metadata.empty()),
        });
    }

    private static StructType schemaWithAppProperties() {
        return new StructType(new StructField[]{
                new StructField("value", DataTypes.BinaryType, false, Metadata.empty()),
                new StructField("application_properties",
                        DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType),
                        true, Metadata.empty()),
        });
    }

    private static StructType schemaWithMsgAnnotations() {
        return new StructType(new StructField[]{
                new StructField("value", DataTypes.BinaryType, false, Metadata.empty()),
                new StructField("message_annotations",
                        DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType),
                        true, Metadata.empty()),
        });
    }

    // ========================================================================
    // Basic value conversion
    // ========================================================================

    @Nested
    class BasicConversion {

        @Test
        void convertsValueOnly() {
            var converter = new RowToMessageConverter(valueOnlySchema());
            InternalRow row = new GenericInternalRow(new Object[]{
                    "hello".getBytes()
            });

            Message msg = converter.convert(row, CODEC.messageBuilder());
            assertThat(msg.getBodyAsBinary()).isEqualTo("hello".getBytes());
        }

        @Test
        void emptyBody() {
            var converter = new RowToMessageConverter(valueOnlySchema());
            InternalRow row = new GenericInternalRow(new Object[]{new byte[0]});

            Message msg = converter.convert(row, CODEC.messageBuilder());
            assertThat(msg.getBodyAsBinary()).isEqualTo(new byte[0]);
        }
    }

    // ========================================================================
    // Stream and routing key extraction
    // ========================================================================

    @Nested
    class StreamAndRoutingKey {

        @Test
        void extractsStreamName() {
            var converter = new RowToMessageConverter(schemaWithStream());
            InternalRow row = new GenericInternalRow(new Object[]{
                    "body".getBytes(),
                    UTF8String.fromString("target-stream")
            });

            assertThat(converter.getStream(row)).isEqualTo("target-stream");
        }

        @Test
        void extractsStreamNameAtIndexZero() {
            var converter = new RowToMessageConverter(schemaWithStreamFirst());
            InternalRow row = new GenericInternalRow(new Object[]{
                    UTF8String.fromString("zero-stream"),
                    "body".getBytes()
            });

            assertThat(converter.getStream(row)).isEqualTo("zero-stream");
        }

        @Test
        void streamNullWhenAbsent() {
            var converter = new RowToMessageConverter(valueOnlySchema());
            InternalRow row = new GenericInternalRow(new Object[]{"body".getBytes()});

            assertThat(converter.getStream(row)).isNull();
        }

        @Test
        void streamNullWhenNull() {
            var converter = new RowToMessageConverter(schemaWithStream());
            InternalRow row = new GenericInternalRow(new Object[]{"body".getBytes(), null});

            assertThat(converter.getStream(row)).isNull();
        }

        @Test
        void extractsRoutingKey() {
            var converter = new RowToMessageConverter(schemaWithRoutingKey());
            InternalRow row = new GenericInternalRow(new Object[]{
                    "body".getBytes(),
                    UTF8String.fromString("my-key")
            });

            assertThat(converter.getRoutingKey(row)).isEqualTo("my-key");
        }

        @Test
        void extractsRoutingKeyAtIndexZero() {
            var converter = new RowToMessageConverter(schemaWithRoutingKeyFirst());
            InternalRow row = new GenericInternalRow(new Object[]{
                    UTF8String.fromString("rk-zero"),
                    "body".getBytes()
            });

            assertThat(converter.getRoutingKey(row)).isEqualTo("rk-zero");
            Message msg = converter.convert(row, CODEC.messageBuilder());
            assertThat(msg.getApplicationProperties()).containsEntry("routing_key", "rk-zero");
        }

        @Test
        void routingKeyNullWhenAbsent() {
            var converter = new RowToMessageConverter(valueOnlySchema());
            InternalRow row = new GenericInternalRow(new Object[]{"body".getBytes()});

            assertThat(converter.getRoutingKey(row)).isNull();
        }

        @Test
        void routingKeyStoredInApplicationProperties() {
            var converter = new RowToMessageConverter(schemaWithRoutingKey());
            InternalRow row = new GenericInternalRow(new Object[]{
                    "body".getBytes(),
                    UTF8String.fromString("my-route")
            });

            Message msg = converter.convert(row, CODEC.messageBuilder());
            assertThat(msg.getApplicationProperties()).isNotNull();
            assertThat(msg.getApplicationProperties().get("routing_key")).isEqualTo("my-route");
        }

        @Test
        void nullRoutingKeyNotStoredInApplicationProperties() {
            var converter = new RowToMessageConverter(schemaWithRoutingKey());
            InternalRow row = new GenericInternalRow(new Object[]{"body".getBytes(), null});

            Message msg = converter.convert(row, CODEC.messageBuilder());
            // routing_key is null, so no application properties entry
            var appProps = msg.getApplicationProperties();
            assertThat(appProps == null || !appProps.containsKey("routing_key")).isTrue();
        }

        @Test
        void extractsValueByNameWhenSchemaIsReordered() {
            StructType schema = new StructType(new StructField[]{
                    new StructField("stream", DataTypes.StringType, true, Metadata.empty()),
                    new StructField("value", DataTypes.BinaryType, false, Metadata.empty()),
            });
            var converter = new RowToMessageConverter(schema);
            InternalRow row = new GenericInternalRow(new Object[]{
                    UTF8String.fromString("s1"),
                    "body".getBytes()
            });

            assertThat(converter.getValue(row)).isEqualTo("body".getBytes());
        }
    }

    // ========================================================================
    // Creation time
    // ========================================================================

    @Nested
    class CreationTime {

        @Test
        void setsCreationTimeWithMicrosToMillisConversion() {
            var converter = new RowToMessageConverter(schemaWithCreationTime());
            long micros = 1700000000000L * 1000L; // micros
            InternalRow row = new GenericInternalRow(new Object[]{
                    "body".getBytes(), micros
            });

            Message msg = converter.convert(row, CODEC.messageBuilder());
            assertThat(msg.getProperties().getCreationTime()).isEqualTo(1700000000000L);
        }

        @Test
        void setsCreationTimeWhenFieldIsFirst() {
            var converter = new RowToMessageConverter(schemaWithCreationTimeFirst());
            long micros = 1700000200000L * 1000L;
            InternalRow row = new GenericInternalRow(new Object[]{
                    micros, "body".getBytes()
            });

            Message msg = converter.convert(row, CODEC.messageBuilder());
            assertThat(msg.getProperties().getCreationTime()).isEqualTo(1700000200000L);
        }

        @Test
        void nullCreationTimeSkipsProperty() {
            var converter = new RowToMessageConverter(schemaWithCreationTime());
            InternalRow row = new GenericInternalRow(new Object[]{"body".getBytes(), null});

            Message msg = converter.convert(row, CODEC.messageBuilder());
            assertThat(msg.getProperties()).isNull();
            assertThat(msg.getBodyAsBinary()).isEqualTo("body".getBytes());
        }

        @Test
        void topLevelCreationTimeOverridesStructCreationTimeWithoutDroppingOtherProperties() {
            var converter = new RowToMessageConverter(schemaWithPropertiesAndTopLevelCreationTime());
            long structCreationMicros = 1700000000000L * 1000L;
            long topLevelCreationMicros = 1700000500000L * 1000L;

            InternalRow propertiesRow = new GenericInternalRow(new Object[]{
                    UTF8String.fromString("msg-id"), // message_id
                    null,                            // user_id
                    null,                            // to
                    UTF8String.fromString("subject-a"), // subject
                    null,                            // reply_to
                    null,                            // correlation_id
                    null,                            // content_type
                    null,                            // content_encoding
                    null,                            // absolute_expiry_time
                    structCreationMicros,            // creation_time
                    null,                            // group_id
                    null,                            // group_sequence
                    null                             // reply_to_group_id
            });

            InternalRow row = new GenericInternalRow(new Object[]{
                    "body".getBytes(),
                    propertiesRow,
                    topLevelCreationMicros
            });

            Message msg = converter.convert(row, CODEC.messageBuilder());
            assertThat(msg.getProperties().getCreationTime()).isEqualTo(1700000500000L);
            assertThat(msg.getProperties().getMessageIdAsString()).isEqualTo("msg-id");
            assertThat(msg.getProperties().getSubject()).isEqualTo("subject-a");
        }
    }

    // ========================================================================
    // Properties struct
    // ========================================================================

    @Nested
    class PropertiesConversion {

        @Test
        void convertsPropertiesStruct() {
            var converter = new RowToMessageConverter(schemaWithProperties());
            long expiryMicros = 1700000100000L * 1000L;
            long creationMicros = 1700000000000L * 1000L;

            InternalRow propsRow = new GenericInternalRow(new Object[]{
                    UTF8String.fromString("msg-id"),    // message_id
                    "uid".getBytes(),                    // user_id
                    UTF8String.fromString("to-addr"),    // to
                    UTF8String.fromString("subj"),       // subject
                    UTF8String.fromString("reply-to"),   // reply_to
                    UTF8String.fromString("corr-id"),    // correlation_id
                    UTF8String.fromString("text/plain"), // content_type
                    UTF8String.fromString("gzip"),       // content_encoding
                    expiryMicros,                        // absolute_expiry_time
                    creationMicros,                       // creation_time
                    UTF8String.fromString("g1"),         // group_id
                    7L,                                   // group_sequence
                    UTF8String.fromString("rg1"),        // reply_to_group_id
            });

            InternalRow row = new GenericInternalRow(new Object[]{
                    "body".getBytes(), propsRow
            });

            Message msg = converter.convert(row, CODEC.messageBuilder());
            var p = msg.getProperties();
            assertThat(p.getMessageIdAsString()).isEqualTo("msg-id");
            assertThat(p.getUserId()).isEqualTo("uid".getBytes());
            assertThat(p.getTo()).isEqualTo("to-addr");
            assertThat(p.getSubject()).isEqualTo("subj");
            assertThat(p.getReplyTo()).isEqualTo("reply-to");
            assertThat(p.getCorrelationIdAsString()).isEqualTo("corr-id");
            assertThat(p.getContentType()).isEqualTo("text/plain");
            assertThat(p.getContentEncoding()).isEqualTo("gzip");
            assertThat(p.getAbsoluteExpiryTime()).isEqualTo(1700000100000L);
            assertThat(p.getCreationTime()).isEqualTo(1700000000000L);
            assertThat(p.getGroupId()).isEqualTo("g1");
            assertThat(p.getGroupSequence()).isEqualTo(7L);
            assertThat(p.getReplyToGroupId()).isEqualTo("rg1");
        }

        @Test
        void convertsPropertiesWhenStructIsFirst() {
            StructType schema = schemaWithPropertiesFirst(RabbitMQStreamTable.PROPERTIES_STRUCT);
            var converter = new RowToMessageConverter(schema);
            InternalRow propsRow = new GenericInternalRow(new Object[]{
                    UTF8String.fromString("id-zero"),
                    null, null, null, null, null, null, null, null, null, null, null, null
            });
            InternalRow row = new GenericInternalRow(new Object[]{propsRow, "body".getBytes()});

            Message msg = converter.convert(row, CODEC.messageBuilder());
            assertThat(msg.getProperties().getMessageIdAsString()).isEqualTo("id-zero");
        }

        @Test
        void propertiesFieldsAtIndexZeroAreApplied() {
            assertSinglePropertyApplied("message_id", UTF8String.fromString("m-1"),
                    msg -> msg.getProperties().getMessageIdAsString());
            assertSinglePropertyApplied("user_id", "uid".getBytes(),
                    msg -> msg.getProperties().getUserId());
            assertSinglePropertyApplied("to", UTF8String.fromString("to-addr"),
                    msg -> msg.getProperties().getTo());
            assertSinglePropertyApplied("subject", UTF8String.fromString("subj"),
                    msg -> msg.getProperties().getSubject());
            assertSinglePropertyApplied("reply_to", UTF8String.fromString("reply-to"),
                    msg -> msg.getProperties().getReplyTo());
            assertSinglePropertyApplied("correlation_id", UTF8String.fromString("corr"),
                    msg -> msg.getProperties().getCorrelationIdAsString());
            assertSinglePropertyApplied("content_type", UTF8String.fromString("text/plain"),
                    msg -> msg.getProperties().getContentType());
            assertSinglePropertyApplied("content_encoding", UTF8String.fromString("gzip"),
                    msg -> msg.getProperties().getContentEncoding());
            assertSinglePropertyApplied("absolute_expiry_time", 1700000300000L * 1000L,
                    msg -> msg.getProperties().getAbsoluteExpiryTime());
            assertSinglePropertyApplied("creation_time", 1700000400000L * 1000L,
                    msg -> msg.getProperties().getCreationTime());
            assertSinglePropertyApplied("group_id", UTF8String.fromString("g1"),
                    msg -> msg.getProperties().getGroupId());
            assertSinglePropertyApplied("group_sequence", 9L,
                    msg -> msg.getProperties().getGroupSequence());
            assertSinglePropertyApplied("reply_to_group_id", UTF8String.fromString("rg1"),
                    msg -> msg.getProperties().getReplyToGroupId());
        }

        @Test
        void nullPropertiesSkipsAll() {
            var converter = new RowToMessageConverter(schemaWithProperties());
            InternalRow row = new GenericInternalRow(new Object[]{"body".getBytes(), null});

            Message msg = converter.convert(row, CODEC.messageBuilder());
            // Message should have body but no properties
            assertThat(msg.getBodyAsBinary()).isEqualTo("body".getBytes());
            assertThat(msg.getProperties()).isNull();
            assertThat(msg.getApplicationProperties()).isNull();
            assertThat(msg.getMessageAnnotations()).isNull();
        }

        @Test
        void propertiesWithNullFields() {
            var converter = new RowToMessageConverter(schemaWithProperties());
            // All nulls except message_id
            InternalRow propsRow = new GenericInternalRow(new Object[]{
                    UTF8String.fromString("id-only"),
                    null, null, null, null, null, null, null, null, null, null, null, null
            });

            InternalRow row = new GenericInternalRow(new Object[]{"body".getBytes(), propsRow});
            Message msg = converter.convert(row, CODEC.messageBuilder());

            assertThat(msg.getProperties().getMessageIdAsString()).isEqualTo("id-only");
            assertThat(msg.getProperties().getTo()).isNull();
            assertThat(msg.getProperties().getContentType()).isNull();
        }

        @Test
        void convertsPropertiesSubsetStruct() {
            // Only message_id and content_type (not in default order)
            StructType subsetStruct = new StructType(new StructField[]{
                    new StructField("message_id", DataTypes.StringType, true, Metadata.empty()),
                    new StructField("content_type", DataTypes.StringType, true, Metadata.empty()),
            });
            StructType schema = new StructType(new StructField[]{
                    new StructField("value", DataTypes.BinaryType, false, Metadata.empty()),
                    new StructField("properties", subsetStruct, true, Metadata.empty()),
            });

            var converter = new RowToMessageConverter(schema);
            InternalRow propsRow = new GenericInternalRow(new Object[]{
                    UTF8String.fromString("msg-123"),
                    UTF8String.fromString("application/json"),
            });
            InternalRow row = new GenericInternalRow(new Object[]{"body".getBytes(), propsRow});

            Message msg = converter.convert(row, CODEC.messageBuilder());
            assertThat(msg.getProperties().getMessageIdAsString()).isEqualTo("msg-123");
            assertThat(msg.getProperties().getContentType()).isEqualTo("application/json");
            // Other fields should be unset
            assertThat(msg.getProperties().getTo()).isNull();
            assertThat(msg.getProperties().getSubject()).isNull();
        }

        @Test
        void convertsPropertiesReorderedStruct() {
            // Fields in reverse order: reply_to_group_id, content_type, message_id
            StructType reorderedStruct = new StructType(new StructField[]{
                    new StructField("reply_to_group_id", DataTypes.StringType, true, Metadata.empty()),
                    new StructField("content_type", DataTypes.StringType, true, Metadata.empty()),
                    new StructField("message_id", DataTypes.StringType, true, Metadata.empty()),
            });
            StructType schema = new StructType(new StructField[]{
                    new StructField("value", DataTypes.BinaryType, false, Metadata.empty()),
                    new StructField("properties", reorderedStruct, true, Metadata.empty()),
            });

            var converter = new RowToMessageConverter(schema);
            InternalRow propsRow = new GenericInternalRow(new Object[]{
                    UTF8String.fromString("rg-1"),       // reply_to_group_id at index 0
                    UTF8String.fromString("text/xml"),    // content_type at index 1
                    UTF8String.fromString("id-reord"),    // message_id at index 2
            });
            InternalRow row = new GenericInternalRow(new Object[]{"body".getBytes(), propsRow});

            Message msg = converter.convert(row, CODEC.messageBuilder());
            assertThat(msg.getProperties().getMessageIdAsString()).isEqualTo("id-reord");
            assertThat(msg.getProperties().getContentType()).isEqualTo("text/xml");
            assertThat(msg.getProperties().getReplyToGroupId()).isEqualTo("rg-1");
        }
    }

    // ========================================================================
    // Application properties and message annotations
    // ========================================================================

    @Nested
    class MapConversion {

        @Test
        void convertsApplicationProperties() {
            var converter = new RowToMessageConverter(schemaWithAppProperties());
            var mapData = createStringMap("k1", "v1", "k2", "v2");
            InternalRow row = new GenericInternalRow(new Object[]{
                    "body".getBytes(), mapData
            });

            Message msg = converter.convert(row, CODEC.messageBuilder());
            Map<String, Object> appProps = msg.getApplicationProperties();
            assertThat(appProps).hasSize(2);
            assertThat(appProps.get("k1")).isEqualTo("v1");
            assertThat(appProps.get("k2")).isEqualTo("v2");
        }

        @Test
        void convertsApplicationPropertiesAtIndexZero() {
            var converter = new RowToMessageConverter(schemaWithAppPropertiesFirst());
            var mapData = createStringMap("k1", "v1");
            InternalRow row = new GenericInternalRow(new Object[]{mapData, "body".getBytes()});

            Message msg = converter.convert(row, CODEC.messageBuilder());
            assertThat(msg.getApplicationProperties()).containsEntry("k1", "v1");
        }

        @Test
        void convertsMessageAnnotations() {
            var converter = new RowToMessageConverter(schemaWithMsgAnnotations());
            var mapData = createStringMap("ann1", "val1");
            InternalRow row = new GenericInternalRow(new Object[]{
                    "body".getBytes(), mapData
            });

            Message msg = converter.convert(row, CODEC.messageBuilder());
            Map<String, Object> annotations = msg.getMessageAnnotations();
            assertThat(annotations).hasSize(1);
            assertThat(annotations.get("ann1")).hasToString("val1");
        }

        @Test
        void convertsMessageAnnotationsAtIndexZero() {
            var converter = new RowToMessageConverter(schemaWithMsgAnnotationsFirst());
            var mapData = createStringMap("ann1", "val1");
            InternalRow row = new GenericInternalRow(new Object[]{mapData, "body".getBytes()});

            Message msg = converter.convert(row, CODEC.messageBuilder());
            assertThat(msg.getMessageAnnotations()).containsEntry("ann1", "val1");
        }

        @Test
        void nullMapSkips() {
            var converter = new RowToMessageConverter(schemaWithAppProperties());
            InternalRow row = new GenericInternalRow(new Object[]{"body".getBytes(), null});

            Message msg = converter.convert(row, CODEC.messageBuilder());
            // null or empty application properties
            var appProps = msg.getApplicationProperties();
            assertThat(appProps == null || appProps.isEmpty()).isTrue();
        }
    }

    // ========================================================================
    // Round-trip: Row → Message → Row
    // ========================================================================

    @Nested
    class RoundTrip {

        @Test
        void roundTripBasicMessage() {
            // Create a row with a value and convert to message
            var sinkConverter = new RowToMessageConverter(schemaWithCreationTime());
            long micros = 1700000000000L * 1000L;
            InternalRow sinkRow = new GenericInternalRow(new Object[]{
                    "round-trip".getBytes(), micros
            });
            Message msg = sinkConverter.convert(sinkRow, CODEC.messageBuilder());

            // Convert message back to row
            var sourceConverter = new MessageToRowConverter(
                    java.util.EnumSet.of(MetadataField.CREATION_TIME));
            InternalRow sourceRow = sourceConverter.convert(msg, "s", 42L, 9999L);

            assertThat(sourceRow.getBinary(0)).isEqualTo("round-trip".getBytes());
            assertThat(sourceRow.getLong(2)).isEqualTo(42L);
            // creation_time should survive the round trip
            assertThat(sourceRow.getLong(4)).isEqualTo(micros);
        }
    }

    // ---- Helpers ----

    private static ArrayBasedMapData createStringMap(String... kvPairs) {
        int size = kvPairs.length / 2;
        UTF8String[] keys = new UTF8String[size];
        UTF8String[] values = new UTF8String[size];
        for (int i = 0; i < size; i++) {
            keys[i] = UTF8String.fromString(kvPairs[i * 2]);
            values[i] = UTF8String.fromString(kvPairs[i * 2 + 1]);
        }
        return new ArrayBasedMapData(
                new GenericArrayData(keys),
                new GenericArrayData(values));
    }

    private static void assertSinglePropertyApplied(
            String fieldName,
            Object value,
            java.util.function.Function<Message, Object> extractor) {
        StructType propsStruct = new StructType(new StructField[]{
                new StructField(fieldName,
                        value instanceof byte[] ? DataTypes.BinaryType
                                : (value instanceof Long ? DataTypes.LongType : DataTypes.StringType),
                        true,
                        Metadata.empty())
        });
        StructType schema = schemaWithPropertiesFirst(propsStruct);
        var converter = new RowToMessageConverter(schema);
        InternalRow propsRow = new GenericInternalRow(new Object[]{value});
        InternalRow row = new GenericInternalRow(new Object[]{propsRow, "body".getBytes()});

        Message msg = converter.convert(row, CODEC.messageBuilder());
        Object extracted = extractor.apply(msg);
        if ("absolute_expiry_time".equals(fieldName) || "creation_time".equals(fieldName)) {
            long expectedMillis = (long) value / 1000L;
            assertThat(extracted).isEqualTo(expectedMillis);
        } else if (value instanceof UTF8String) {
            assertThat(extracted).isEqualTo(((UTF8String) value).toString());
        } else if (value instanceof byte[]) {
            assertThat(extracted).isEqualTo(value);
        } else {
            assertThat(extracted).isEqualTo(value);
        }
    }
}
