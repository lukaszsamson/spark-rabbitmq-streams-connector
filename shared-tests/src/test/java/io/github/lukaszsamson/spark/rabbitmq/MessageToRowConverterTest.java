package io.github.lukaszsamson.spark.rabbitmq;

import com.rabbitmq.stream.Message;
import com.rabbitmq.stream.Properties;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.util.ArrayBasedMapData;
import org.apache.spark.unsafe.types.UTF8String;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.*;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Unit tests for {@link MessageToRowConverter} mapping and metadata projection
 * without requiring a RabbitMQ broker. Broker integration is covered by
 * it-tests.
 */
class MessageToRowConverterTest {

    private static final long CHUNK_TS_MILLIS = 1700000000000L;
    private static final long CHUNK_TS_MICROS = 1700000000000L * 1000L;

    private Message mockMessage(byte[] body) {
        Message msg = mock(Message.class);
        when(msg.getBodyAsBinary()).thenReturn(body);
        return msg;
    }

    private Properties mockProperties() {
        Properties props = mock(Properties.class);
        when(props.getMessageId()).thenReturn("msg-123");
        when(props.getUserId()).thenReturn("user1".getBytes());
        when(props.getTo()).thenReturn("to-addr");
        when(props.getSubject()).thenReturn("subj");
        when(props.getReplyTo()).thenReturn("reply-addr");
        when(props.getCorrelationId()).thenReturn("corr-456");
        when(props.getContentType()).thenReturn("application/json");
        when(props.getContentEncoding()).thenReturn("utf-8");
        when(props.getAbsoluteExpiryTime()).thenReturn(1700000100000L);
        when(props.getCreationTime()).thenReturn(1700000000000L);
        when(props.getGroupId()).thenReturn("group1");
        when(props.getGroupSequence()).thenReturn(42L);
        when(props.getReplyToGroupId()).thenReturn("reply-group1");
        return props;
    }

    // ========================================================================
    // Fixed fields
    // ========================================================================

    @Nested
    class FixedFields {

        @Test
        void convertsFixedFieldsWithNoMetadata() {
            var converter = new MessageToRowConverter(EnumSet.noneOf(MetadataField.class));
            Message msg = mockMessage("hello".getBytes());
            InternalRow row = converter.convert(msg, "my-stream", 99L, CHUNK_TS_MILLIS);

            assertThat(row.numFields()).isEqualTo(4);
            assertThat(row.getBinary(0)).isEqualTo("hello".getBytes());
            assertThat(row.getUTF8String(1)).isEqualTo(UTF8String.fromString("my-stream"));
            assertThat(row.getLong(2)).isEqualTo(99L);
            assertThat(row.getLong(3)).isEqualTo(CHUNK_TS_MICROS);
        }

        @Test
        void timestampConvertedFromMillisToMicros() {
            var converter = new MessageToRowConverter(EnumSet.noneOf(MetadataField.class));
            Message msg = mockMessage(new byte[0]);
            InternalRow row = converter.convert(msg, "s", 0, 1234L);

            assertThat(row.getLong(3)).isEqualTo(1234L * 1000L);
        }
    }

    // ========================================================================
    // Properties struct
    // ========================================================================

    @Nested
    class PropertiesConversion {

        @Test
        void convertsAllPropertyFields() {
            var converter = new MessageToRowConverter(EnumSet.of(MetadataField.PROPERTIES));
            Message msg = mockMessage(new byte[0]);
            Properties props = mockProperties();
            when(msg.getProperties()).thenReturn(props);

            InternalRow row = converter.convert(msg, "s", 0, CHUNK_TS_MILLIS);
            InternalRow propsRow = row.getStruct(4, 13);

            // message_id
            assertThat(propsRow.getUTF8String(0)).isEqualTo(UTF8String.fromString("msg-123"));
            // user_id
            assertThat(propsRow.getBinary(1)).isEqualTo("user1".getBytes());
            // to
            assertThat(propsRow.getUTF8String(2)).isEqualTo(UTF8String.fromString("to-addr"));
            // subject
            assertThat(propsRow.getUTF8String(3)).isEqualTo(UTF8String.fromString("subj"));
            // reply_to
            assertThat(propsRow.getUTF8String(4)).isEqualTo(UTF8String.fromString("reply-addr"));
            // correlation_id
            assertThat(propsRow.getUTF8String(5)).isEqualTo(UTF8String.fromString("corr-456"));
            // content_type
            assertThat(propsRow.getUTF8String(6)).isEqualTo(UTF8String.fromString("application/json"));
            // content_encoding
            assertThat(propsRow.getUTF8String(7)).isEqualTo(UTF8String.fromString("utf-8"));
            // absolute_expiry_time (millis → micros)
            assertThat(propsRow.getLong(8)).isEqualTo(1700000100000L * 1000L);
            // creation_time (millis → micros)
            assertThat(propsRow.getLong(9)).isEqualTo(1700000000000L * 1000L);
            // group_id
            assertThat(propsRow.getUTF8String(10)).isEqualTo(UTF8String.fromString("group1"));
            // group_sequence
            assertThat(propsRow.getLong(11)).isEqualTo(42L);
            // reply_to_group_id
            assertThat(propsRow.getUTF8String(12)).isEqualTo(UTF8String.fromString("reply-group1"));
        }

        @Test
        void nullPropertiesProducesNullStruct() {
            var converter = new MessageToRowConverter(EnumSet.of(MetadataField.PROPERTIES));
            Message msg = mockMessage(new byte[0]);
            when(msg.getProperties()).thenReturn(null);

            InternalRow row = converter.convert(msg, "s", 0, CHUNK_TS_MILLIS);
            assertThat(row.isNullAt(4)).isTrue();
        }

        @Test
        void zeroTimestampsMapToNull() {
            var converter = new MessageToRowConverter(EnumSet.of(MetadataField.PROPERTIES));
            Properties props = mock(Properties.class);
            when(props.getCreationTime()).thenReturn(0L);
            when(props.getAbsoluteExpiryTime()).thenReturn(0L);

            InternalRow propsRow = MessageToRowConverter.convertProperties(props);
            assertThat(propsRow.isNullAt(8)).isTrue(); // absolute_expiry_time
            assertThat(propsRow.isNullAt(9)).isTrue(); // creation_time
        }

        @Test
        void groupSequenceSentinelMapsToNullWhileZeroRemainsValid() {
            Properties unsetProps = mock(Properties.class);
            when(unsetProps.getGroupSequence()).thenReturn(-1L);

            InternalRow unsetRow = MessageToRowConverter.convertProperties(unsetProps);
            assertThat(unsetRow.isNullAt(11)).isTrue();

            Properties zeroProps = mock(Properties.class);
            when(zeroProps.getGroupSequence()).thenReturn(0L);

            InternalRow zeroRow = MessageToRowConverter.convertProperties(zeroProps);
            assertThat(zeroRow.getLong(11)).isEqualTo(0L);
        }
    }

    // ========================================================================
    // ID coercion
    // ========================================================================

    @Nested
    class IdCoercion {

        @Test
        void coercesStringId() {
            assertThat(MessageToRowConverter.coerceIdToString("my-id"))
                    .isEqualTo(UTF8String.fromString("my-id"));
        }

        @Test
        void coercesUuidId() {
            UUID uuid = UUID.fromString("12345678-1234-1234-1234-123456789abc");
            assertThat(MessageToRowConverter.coerceIdToString(uuid))
                    .isEqualTo(UTF8String.fromString("12345678-1234-1234-1234-123456789abc"));
        }

        @Test
        void coercesBinaryId() {
            byte[] bytes = {0x01, 0x02, 0x03};
            UTF8String result = MessageToRowConverter.coerceIdToString(bytes);
            assertThat(result.toString()).isEqualTo(Base64.getEncoder().encodeToString(bytes));
        }

        @Test
        void coercesNumericId() {
            // UnsignedLong and similar types use toString()
            assertThat(MessageToRowConverter.coerceIdToString(12345L))
                    .isEqualTo(UTF8String.fromString("12345"));
        }

        @Test
        void nullIdReturnsNull() {
            assertThat(MessageToRowConverter.coerceIdToString(null)).isNull();
        }
    }

    // ========================================================================
    // Map conversion
    // ========================================================================

    @Nested
    class MapConversion {

        @Test
        void convertsApplicationProperties() {
            var converter = new MessageToRowConverter(
                    EnumSet.of(MetadataField.APPLICATION_PROPERTIES));
            Message msg = mockMessage(new byte[0]);
            Map<String, Object> appProps = new LinkedHashMap<>();
            appProps.put("key1", "value1");
            appProps.put("key2", 42);
            appProps.put("key3", true);
            when(msg.getApplicationProperties()).thenReturn(appProps);

            InternalRow row = converter.convert(msg, "s", 0, CHUNK_TS_MILLIS);
            ArrayBasedMapData mapData = (ArrayBasedMapData) row.getMap(4);

            assertThat(mapData.numElements()).isEqualTo(3);
            var keyArray = mapData.keyArray();
            var valArray = mapData.valueArray();
            Map<String, String> valuesByKey = new LinkedHashMap<>();
            for (int i = 0; i < mapData.numElements(); i++) {
                valuesByKey.put(keyArray.getUTF8String(i).toString(),
                        valArray.getUTF8String(i).toString());
            }
            assertThat(valuesByKey).containsEntry("key1", "value1");
            assertThat(valuesByKey).containsEntry("key2", "42");
            assertThat(valuesByKey).containsEntry("key3", "true");
        }

        @Test
        void preservesNullApplicationPropertyValue() {
            var converter = new MessageToRowConverter(
                    EnumSet.of(MetadataField.APPLICATION_PROPERTIES));
            Message msg = mockMessage(new byte[0]);
            Map<String, Object> appProps = new LinkedHashMap<>();
            appProps.put("key1", null);
            appProps.put("key2", "value2");
            when(msg.getApplicationProperties()).thenReturn(appProps);

            InternalRow row = converter.convert(msg, "s", 0, CHUNK_TS_MILLIS);
            ArrayBasedMapData mapData = (ArrayBasedMapData) row.getMap(4);

            assertThat(mapData.numElements()).isEqualTo(2);
            var keyArray = mapData.keyArray();
            var valArray = mapData.valueArray();
            Map<String, String> valuesByKey = new LinkedHashMap<>();
            for (int i = 0; i < mapData.numElements(); i++) {
                valuesByKey.put(keyArray.getUTF8String(i).toString(),
                        valArray.isNullAt(i) ? null : valArray.getUTF8String(i).toString());
            }
            assertThat(valuesByKey).containsEntry("key1", null);
            assertThat(valuesByKey).containsEntry("key2", "value2");
        }

        @Test
        void nullMapProducesNull() {
            ArrayBasedMapData result = MessageToRowConverter.convertStringMap(null);
            assertThat(result).isNull();
        }

        @Test
        void emptyMapProducesNull() {
            ArrayBasedMapData result = MessageToRowConverter.convertStringMap(Map.of());
            assertThat(result).isNull();
        }
    }

    // ========================================================================
    // Creation time and routing key
    // ========================================================================

    @Nested
    class CreationTimeAndRoutingKey {

        @Test
        void extractsCreationTimeFromProperties() {
            var converter = new MessageToRowConverter(EnumSet.of(MetadataField.CREATION_TIME));
            Message msg = mockMessage(new byte[0]);
            Properties props = mock(Properties.class);
            when(props.getCreationTime()).thenReturn(1700000000000L);
            when(msg.getProperties()).thenReturn(props);

            InternalRow row = converter.convert(msg, "s", 0, CHUNK_TS_MILLIS);
            assertThat(row.getLong(4)).isEqualTo(1700000000000L * 1000L);
        }

        @Test
        void creationTimeNullWhenNoProperties() {
            var converter = new MessageToRowConverter(EnumSet.of(MetadataField.CREATION_TIME));
            Message msg = mockMessage(new byte[0]);
            when(msg.getProperties()).thenReturn(null);

            InternalRow row = converter.convert(msg, "s", 0, CHUNK_TS_MILLIS);
            assertThat(row.isNullAt(4)).isTrue();
        }

        @Test
        void extractsRoutingKeyFromApplicationProperties() {
            var converter = new MessageToRowConverter(EnumSet.of(MetadataField.ROUTING_KEY));
            Message msg = mockMessage(new byte[0]);
            Map<String, Object> appProps = Map.of("routing_key", "region-1");
            when(msg.getApplicationProperties()).thenReturn(appProps);

            InternalRow row = converter.convert(msg, "s", 0, CHUNK_TS_MILLIS);
            assertThat(row.getUTF8String(4)).isEqualTo(UTF8String.fromString("region-1"));
        }

        @Test
        void routingKeyNullWhenMissing() {
            var converter = new MessageToRowConverter(EnumSet.of(MetadataField.ROUTING_KEY));
            Message msg = mockMessage(new byte[0]);
            when(msg.getApplicationProperties()).thenReturn(null);

            InternalRow row = converter.convert(msg, "s", 0, CHUNK_TS_MILLIS);
            assertThat(row.isNullAt(4)).isTrue();
        }

        @Test
        void routingKeyNullWhenApplicationPropertyValueIsNull() {
            var converter = new MessageToRowConverter(EnumSet.of(MetadataField.ROUTING_KEY));
            Message msg = mockMessage(new byte[0]);
            Map<String, Object> appProps = new LinkedHashMap<>();
            appProps.put("routing_key", null);
            when(msg.getApplicationProperties()).thenReturn(appProps);

            InternalRow row = converter.convert(msg, "s", 0, CHUNK_TS_MILLIS);
            assertThat(row.isNullAt(4)).isTrue();
        }
    }

    // ========================================================================
    // All metadata fields
    // ========================================================================

    @Nested
    class AllMetadataFields {

        @Test
        void convertsAllFieldsWithDefaultMetadata() {
            var converter = new MessageToRowConverter(MetadataField.ALL);
            Message msg = mockMessage("body".getBytes());
            Properties props = mockProperties();
            when(msg.getProperties()).thenReturn(props);
            Map<String, Object> appProps = new LinkedHashMap<>();
            appProps.put("routing_key", "rk1");
            appProps.put("extra", "val");
            when(msg.getApplicationProperties()).thenReturn(appProps);
            when(msg.getMessageAnnotations()).thenReturn(Map.of("ann1", "v1"));

            InternalRow row = converter.convert(msg, "stream1", 500L, CHUNK_TS_MILLIS);

            // Total fields: 4 fixed + 5 metadata = 9
            assertThat(row.numFields()).isEqualTo(9);
            // Fixed
            assertThat(row.getBinary(0)).isEqualTo("body".getBytes());
            assertThat(row.getUTF8String(1)).isEqualTo(UTF8String.fromString("stream1"));
            assertThat(row.getLong(2)).isEqualTo(500L);
            // Properties (index 4)
            assertThat(row.isNullAt(4)).isFalse();
            // App properties (index 5)
            assertThat(row.getMap(5).numElements()).isEqualTo(2);
            // Message annotations (index 6)
            assertThat(row.getMap(6).numElements()).isEqualTo(1);
            // Creation time (index 7)
            assertThat(row.getLong(7)).isEqualTo(1700000000000L * 1000L);
            // Routing key (index 8)
            assertThat(row.getUTF8String(8)).isEqualTo(UTF8String.fromString("rk1"));
        }

        @Test
        void convertPlacesFieldsAtCorrectArrayIndices() {
            var converter = new MessageToRowConverter(MetadataField.ALL);
            Message msg = mockMessage("body".getBytes());
            Properties props = mock(Properties.class);
            when(props.getMessageId()).thenReturn("msg-id");
            when(props.getUserId()).thenReturn("uid".getBytes());
            when(props.getTo()).thenReturn("to-addr");
            when(props.getSubject()).thenReturn("subject");
            when(props.getReplyTo()).thenReturn("reply");
            when(props.getCorrelationId()).thenReturn("corr");
            when(props.getContentType()).thenReturn("text/plain");
            when(props.getContentEncoding()).thenReturn("utf-8");
            when(props.getAbsoluteExpiryTime()).thenReturn(1700000200000L);
            when(props.getCreationTime()).thenReturn(1700000100000L);
            when(props.getGroupId()).thenReturn("group");
            when(props.getGroupSequence()).thenReturn(99L);
            when(props.getReplyToGroupId()).thenReturn("reply-group");
            when(msg.getProperties()).thenReturn(props);
            when(msg.getApplicationProperties()).thenReturn(Map.of("app", "val", "routing_key", "rk1"));
            when(msg.getMessageAnnotations()).thenReturn(Map.of("ann", "v1"));

            InternalRow row = converter.convert(msg, "stream-x", 42L, CHUNK_TS_MILLIS);

            assertThat(row.getBinary(0)).isEqualTo("body".getBytes());
            assertThat(row.getUTF8String(1)).isEqualTo(UTF8String.fromString("stream-x"));
            assertThat(row.getLong(2)).isEqualTo(42L);
            assertThat(row.getLong(3)).isEqualTo(CHUNK_TS_MICROS);

            InternalRow propsRow = row.getStruct(4, 13);
            assertThat(propsRow.getUTF8String(0)).isEqualTo(UTF8String.fromString("msg-id"));
            assertThat(propsRow.getBinary(1)).isEqualTo("uid".getBytes());
            assertThat(propsRow.getUTF8String(2)).isEqualTo(UTF8String.fromString("to-addr"));
            assertThat(propsRow.getUTF8String(3)).isEqualTo(UTF8String.fromString("subject"));
            assertThat(propsRow.getUTF8String(4)).isEqualTo(UTF8String.fromString("reply"));
            assertThat(propsRow.getUTF8String(5)).isEqualTo(UTF8String.fromString("corr"));
            assertThat(propsRow.getUTF8String(6)).isEqualTo(UTF8String.fromString("text/plain"));
            assertThat(propsRow.getUTF8String(7)).isEqualTo(UTF8String.fromString("utf-8"));
            assertThat(propsRow.getLong(8)).isEqualTo(1700000200000L * 1000L);
            assertThat(propsRow.getLong(9)).isEqualTo(1700000100000L * 1000L);
            assertThat(propsRow.getUTF8String(10)).isEqualTo(UTF8String.fromString("group"));
            assertThat(propsRow.getLong(11)).isEqualTo(99L);
            assertThat(propsRow.getUTF8String(12)).isEqualTo(UTF8String.fromString("reply-group"));

            ArrayBasedMapData appProps = (ArrayBasedMapData) row.getMap(5);
            assertThat(appProps.numElements()).isEqualTo(2);
            ArrayBasedMapData annotations = (ArrayBasedMapData) row.getMap(6);
            assertThat(annotations.numElements()).isEqualTo(1);
            assertThat(row.getLong(7)).isEqualTo(1700000100000L * 1000L);
            assertThat(row.getUTF8String(8)).isEqualTo(UTF8String.fromString("rk1"));
        }
    }

    // ========================================================================
    // Timestamp conversion helpers
    // ========================================================================

    @Nested
    class TimestampConversion {

        @Test
        void millisToMicros() {
            assertThat(MessageToRowConverter.millisToMicros(1000L)).isEqualTo(1_000_000L);
            assertThat(MessageToRowConverter.millisToMicros(0L)).isEqualTo(0L);
        }

        @Test
        void microsToMillis() {
            assertThat(MessageToRowConverter.microsToMillis(1_000_000L)).isEqualTo(1000L);
            assertThat(MessageToRowConverter.microsToMillis(999L)).isEqualTo(0L);
        }
    }
}
