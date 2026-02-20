package com.rabbitmq.spark.connector;

import com.rabbitmq.stream.Message;
import com.rabbitmq.stream.Properties;

import java.nio.charset.StandardCharsets;
import java.util.Map;

final class MessageSizeEstimator {

    private MessageSizeEstimator() {}

    static long payloadBytes(Message message) {
        byte[] body = message.getBodyAsBinary();
        return body == null ? 0L : body.length;
    }

    static long estimatedWireBytes(Message message) {
        long total = payloadBytes(message);
        total += estimateMapBytes(message.getApplicationProperties());
        total += estimateMapBytes(message.getMessageAnnotations());
        total += estimatePropertiesBytes(message.getProperties());
        return total;
    }

    private static long estimateMapBytes(Map<String, Object> values) {
        if (values == null || values.isEmpty()) {
            return 0L;
        }
        long total = 0L;
        for (Map.Entry<String, Object> entry : values.entrySet()) {
            total += utf8Size(entry.getKey());
            total += objectSize(entry.getValue());
        }
        return total;
    }

    private static long estimatePropertiesBytes(Properties properties) {
        if (properties == null) {
            return 0L;
        }
        long total = 0L;
        total += propertyBytes("message_id", properties.getMessageId());
        total += propertyBytes("user_id", properties.getUserId());
        total += propertyBytes("to", properties.getTo());
        total += propertyBytes("subject", properties.getSubject());
        total += propertyBytes("reply_to", properties.getReplyTo());
        total += propertyBytes("correlation_id", properties.getCorrelationId());
        total += propertyBytes("content_type", properties.getContentType());
        total += propertyBytes("content_encoding", properties.getContentEncoding());
        if (properties.getAbsoluteExpiryTime() > 0) {
            total += propertyBytes("absolute_expiry_time", properties.getAbsoluteExpiryTime());
        }
        if (properties.getCreationTime() > 0) {
            total += propertyBytes("creation_time", properties.getCreationTime());
        }
        total += propertyBytes("group_id", properties.getGroupId());
        if (properties.getGroupSequence() > 0) {
            total += propertyBytes("group_sequence", properties.getGroupSequence());
        }
        total += propertyBytes("reply_to_group_id", properties.getReplyToGroupId());
        return total;
    }

    private static long propertyBytes(String key, Object value) {
        if (value == null) {
            return 0L;
        }
        return utf8Size(key) + objectSize(value);
    }

    private static long objectSize(Object value) {
        if (value == null) {
            return 0L;
        }
        if (value instanceof byte[] bytes) {
            return bytes.length;
        }
        return utf8Size(value.toString());
    }

    private static int utf8Size(String value) {
        return value == null ? 0 : value.getBytes(StandardCharsets.UTF_8).length;
    }
}
