package io.github.lukaszsamson.spark.rabbitmq;

import com.rabbitmq.stream.Message;
import com.rabbitmq.stream.Properties;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.catalyst.util.ArrayBasedMapData;
import org.apache.spark.sql.catalyst.util.GenericArrayData;
import org.apache.spark.unsafe.types.UTF8String;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/**
 * Converts a RabbitMQ {@link Message} plus stream context into a Spark {@link InternalRow}
 * matching the source schema.
 *
 * <p>Handles AMQP property mapping, timestamp conversion (millis → micros), and
 * type coercion for message_id/correlation_id.
 */
public final class MessageToRowConverter implements Serializable {
    private static final long serialVersionUID = 1L;

    private final boolean includeProperties;
    private final boolean includeApplicationProperties;
    private final boolean includeMessageAnnotations;
    private final boolean includeCreationTime;
    private final boolean includeRoutingKey;
    private final int numFields;

    public MessageToRowConverter(Set<MetadataField> metadataFields) {
        this.includeProperties = metadataFields.contains(MetadataField.PROPERTIES);
        this.includeApplicationProperties = metadataFields.contains(MetadataField.APPLICATION_PROPERTIES);
        this.includeMessageAnnotations = metadataFields.contains(MetadataField.MESSAGE_ANNOTATIONS);
        this.includeCreationTime = metadataFields.contains(MetadataField.CREATION_TIME);
        this.includeRoutingKey = metadataFields.contains(MetadataField.ROUTING_KEY);
        this.numFields = 4
                + (includeProperties ? 1 : 0)
                + (includeApplicationProperties ? 1 : 0)
                + (includeMessageAnnotations ? 1 : 0)
                + (includeCreationTime ? 1 : 0)
                + (includeRoutingKey ? 1 : 0);
    }

    /**
     * Convert a message and its context to a Spark row.
     *
     * @param message the RabbitMQ message
     * @param stream the stream name
     * @param offset the stream offset
     * @param chunkTimestampMillis the chunk timestamp in epoch milliseconds
     * @return the Spark internal row
     */
    public InternalRow convert(Message message, String stream, long offset,
                               long chunkTimestampMillis) {
        Object[] values = new Object[numFields];
        int idx = 0;

        // Fixed fields
        values[idx++] = safeBodyAsBinary(message);
        values[idx++] = UTF8String.fromString(stream);
        values[idx++] = offset;
        values[idx++] = millisToMicros(chunkTimestampMillis);

        // Optional metadata fields (order must match schema builder)
        if (includeProperties) {
            values[idx++] = convertProperties(message.getProperties());
        }
        if (includeApplicationProperties) {
            values[idx++] = convertStringMap(message.getApplicationProperties());
        }
        if (includeMessageAnnotations) {
            values[idx++] = convertStringMap(message.getMessageAnnotations());
        }
        if (includeCreationTime) {
            Properties props = message.getProperties();
            if (props != null && props.getCreationTime() >= 0) {
                values[idx++] = safeMillisToMicros(props.getCreationTime());
            } else {
                values[idx++] = null;
            }
        }
        if (includeRoutingKey) {
            Map<String, Object> appProps = message.getApplicationProperties();
            if (appProps != null && appProps.containsKey("routing_key")) {
                Object routingValue = appProps.get("routing_key");
                values[idx++] = routingValue != null
                        ? UTF8String.fromString(coerceValueToString(routingValue))
                        : null;
            } else {
                values[idx++] = null;
            }
        }

        return new GenericInternalRow(values);
    }

    static byte[] safeBodyAsBinary(Message message) {
        try {
            return message.getBodyAsBinary();
        } catch (IllegalStateException bodyTypeMismatch) {
            Object body;
            try {
                body = message.getBody();
            } catch (RuntimeException ignored) {
                return null;
            }
            if (body == null) {
                return null;
            }
            if (body instanceof byte[] bytes) {
                return bytes;
            }
            return String.valueOf(body).getBytes(StandardCharsets.UTF_8);
        }
    }

    // ---- Property struct conversion ----

    /**
     * Convert AMQP 1.0 Properties to a Spark struct row.
     * All 13 fields are mapped; null when the property is absent.
     */
    static InternalRow convertProperties(Properties props) {
        if (props == null) {
            return null;
        }
        Object[] values = new Object[13];
        values[0] = coerceIdToString(props.getMessageId());          // message_id
        values[1] = props.getUserId();                                // user_id (byte[])
        values[2] = utf8OrNull(props.getTo());                       // to
        values[3] = utf8OrNull(props.getSubject());                   // subject
        values[4] = utf8OrNull(props.getReplyTo());                   // reply_to
        values[5] = coerceIdToString(props.getCorrelationId());       // correlation_id
        values[6] = utf8OrNull(props.getContentType());               // content_type
        values[7] = utf8OrNull(props.getContentEncoding());           // content_encoding
        values[8] = timestampOrNull(props.getAbsoluteExpiryTime());   // absolute_expiry_time
        values[9] = timestampOrNull(props.getCreationTime());         // creation_time
        values[10] = utf8OrNull(props.getGroupId());                  // group_id
        values[11] = longOrNull(props.getGroupSequence());            // group_sequence
        values[12] = utf8OrNull(props.getReplyToGroupId());           // reply_to_group_id
        return new GenericInternalRow(values);
    }

    // ---- Map conversion ----

    /**
     * Convert a {@code Map<String, Object>} to Spark MapData with string keys and values.
     * Values are coerced to strings deterministically per {@link #coerceValueToString(Object)}
     * (base64 for byte[], UUID.toString(), default toString() for other types).
     */
    static ArrayBasedMapData convertStringMap(Map<String, Object> map) {
        if (map == null || map.isEmpty()) {
            return null;
        }
        UTF8String[] keys = new UTF8String[map.size()];
        UTF8String[] vals = new UTF8String[map.size()];
        int i = 0;
        for (Map.Entry<String, Object> entry : map.entrySet()) {
            keys[i] = UTF8String.fromString(entry.getKey());
            Object value = entry.getValue();
            vals[i] = value != null ? UTF8String.fromString(coerceValueToString(value)) : null;
            i++;
        }
        return new ArrayBasedMapData(new GenericArrayData(keys), new GenericArrayData(vals));
    }

    /**
     * Coerce an arbitrary application_properties / message_annotations value to a
     * deterministic string. Mirrors {@link #coerceIdToString} so that byte[] is
     * encoded as base64 and UUIDs use {@link UUID#toString()} rather than
     * {@code String.valueOf} which would emit JVM identity strings for byte[].
     */
    static String coerceValueToString(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof String s) {
            return s;
        }
        if (value instanceof byte[] bytes) {
            return java.util.Base64.getEncoder().encodeToString(bytes);
        }
        if (value instanceof UUID uuid) {
            return uuid.toString();
        }
        return value.toString();
    }

    // ---- Type coercion helpers ----

    /**
     * Coerce an AMQP message_id or correlation_id to a Spark UTF8String.
     * Supports: String, long (UnsignedLong), byte[] (Binary), UUID.
     */
    static UTF8String coerceIdToString(Object id) {
        if (id == null) {
            return null;
        }
        if (id instanceof String s) {
            return UTF8String.fromString(s);
        }
        if (id instanceof UUID uuid) {
            return UTF8String.fromString(uuid.toString());
        }
        if (id instanceof byte[] bytes) {
            return UTF8String.fromString(java.util.Base64.getEncoder().encodeToString(bytes));
        }
        // Number types (UnsignedLong wraps long)
        return UTF8String.fromString(id.toString());
    }

    /** Maximum input millis whose micros conversion still fits in a signed long. */
    static final long MAX_MILLIS_FOR_MICROS = Long.MAX_VALUE / 1000L;
    /** Minimum input millis whose micros conversion still fits in a signed long. */
    static final long MIN_MILLIS_FOR_MICROS = Long.MIN_VALUE / 1000L;

    /**
     * Convert millis to micros for Spark TimestampType, saturating on overflow.
     * Used for the required {@code chunk_timestamp} field where a non-null value
     * must always be produced; saturation only triggers for millis values
     * outside roughly +/- 292 million years from the epoch.
     */
    static long millisToMicros(long millis) {
        if (millis > MAX_MILLIS_FOR_MICROS) {
            return Long.MAX_VALUE;
        }
        if (millis < MIN_MILLIS_FOR_MICROS) {
            return Long.MIN_VALUE;
        }
        return millis * 1000L;
    }

    /**
     * Convert millis to micros for an optional Spark TimestampType column,
     * returning {@code null} when the value would overflow rather than
     * silently saturating.
     */
    static Long safeMillisToMicros(long millis) {
        if (millis > MAX_MILLIS_FOR_MICROS || millis < MIN_MILLIS_FOR_MICROS) {
            return null;
        }
        return millis * 1000L;
    }

    /** Convert micros to millis for RabbitMQ timestamps. */
    static long microsToMillis(long micros) {
        return micros / 1000L;
    }

    private static UTF8String utf8OrNull(String s) {
        return s != null ? UTF8String.fromString(s) : null;
    }

    private static Object timestampOrNull(long millis) {
        return millis >= 0 ? safeMillisToMicros(millis) : null;
    }

    private static Object longOrNull(long value) {
        return value >= 0 ? value : null;
    }
}
