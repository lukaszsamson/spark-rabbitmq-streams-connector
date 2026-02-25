package io.github.lukaszsamson.spark.rabbitmq;

import com.rabbitmq.stream.Message;
import com.rabbitmq.stream.Properties;

import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Shared conversion helpers for creating {@link ConnectorMessageView} instances.
 */
final class MessageViewCoercion {

    private MessageViewCoercion() {
    }

    static ConnectorMessageView toMessageView(Message message) {
        return new ConnectorMessageView(
                MessageToRowConverter.safeBodyAsBinary(message),
                coerceMapToStrings(message.getApplicationProperties()),
                coerceMapToStrings(message.getMessageAnnotations()),
                coercePropertiesToStrings(message.getProperties()));
    }

    static Map<String, String> coerceMapToStrings(Map<String, Object> source) {
        if (source == null || source.isEmpty()) {
            return Map.of();
        }
        Map<String, String> out = new LinkedHashMap<>(source.size());
        for (Map.Entry<String, Object> entry : source.entrySet()) {
            out.put(entry.getKey(), entry.getValue() != null ? entry.getValue().toString() : null);
        }
        return out;
    }

    static Map<String, String> coercePropertiesToStrings(Properties properties) {
        if (properties == null) {
            return Map.of();
        }
        Map<String, String> out = new LinkedHashMap<>();
        putIfNotNull(out, "message_id", coerceIdToString(properties.getMessageId()));
        if (properties.getUserId() != null) {
            out.put("user_id", Base64.getEncoder().encodeToString(properties.getUserId()));
        }
        putIfNotNull(out, "to", properties.getTo());
        putIfNotNull(out, "subject", properties.getSubject());
        putIfNotNull(out, "reply_to", properties.getReplyTo());
        putIfNotNull(out, "correlation_id", coerceIdToString(properties.getCorrelationId()));
        putIfNotNull(out, "content_type", properties.getContentType());
        putIfNotNull(out, "content_encoding", properties.getContentEncoding());
        if (properties.getAbsoluteExpiryTime() > 0) {
            out.put("absolute_expiry_time", Long.toString(properties.getAbsoluteExpiryTime()));
        }
        if (properties.getCreationTime() > 0) {
            out.put("creation_time", Long.toString(properties.getCreationTime()));
        }
        putIfNotNull(out, "group_id", properties.getGroupId());
        if (properties.getGroupSequence() >= 0) {
            out.put("group_sequence", Long.toString(properties.getGroupSequence()));
        }
        putIfNotNull(out, "reply_to_group_id", properties.getReplyToGroupId());
        return out;
    }

    static String coerceIdToString(Object id) {
        if (id == null) {
            return null;
        }
        if (id instanceof byte[] bytes) {
            return Base64.getEncoder().encodeToString(bytes);
        }
        return id.toString();
    }

    static void putIfNotNull(Map<String, String> target, String key, String value) {
        if (value != null) {
            target.put(key, value);
        }
    }
}
