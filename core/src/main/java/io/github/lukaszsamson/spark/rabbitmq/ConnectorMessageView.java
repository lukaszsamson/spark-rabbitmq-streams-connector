package io.github.lukaszsamson.spark.rabbitmq;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Connector-friendly representation of a stream message for filtering hooks.
 *
 * <p>This type intentionally avoids exposing RabbitMQ client classes so user
 * extensions remain stable with connector shading.
 */
public final class ConnectorMessageView implements Serializable {
    private static final long serialVersionUID = 1L;

    private final byte[] body;
    private final Map<String, String> applicationProperties;
    private final Map<String, String> messageAnnotations;
    private final Map<String, String> properties;

    public ConnectorMessageView(byte[] body,
                                Map<String, String> applicationProperties,
                                Map<String, String> messageAnnotations,
                                Map<String, String> properties) {
        this.body = body == null ? null : Arrays.copyOf(body, body.length);
        this.applicationProperties = immutableCopyAllowingNullValues(applicationProperties);
        this.messageAnnotations = immutableCopyAllowingNullValues(messageAnnotations);
        this.properties = immutableCopyAllowingNullValues(properties);
    }

    private static Map<String, String> immutableCopyAllowingNullValues(Map<String, String> source) {
        if (source == null || source.isEmpty()) {
            return Map.of();
        }
        return Collections.unmodifiableMap(new LinkedHashMap<>(source));
    }

    public byte[] getBody() {
        return body == null ? null : Arrays.copyOf(body, body.length);
    }

    public Map<String, String> getApplicationProperties() {
        return applicationProperties;
    }

    public Map<String, String> getMessageAnnotations() {
        return messageAnnotations;
    }

    /**
     * AMQP properties with canonical snake_case keys (e.g. {@code subject},
     * {@code group_id}, {@code creation_time}).
     */
    public Map<String, String> getProperties() {
        return properties;
    }

    /**
     * Resolve a value from a path expression.
     *
     * <p>Supported roots: {@code application_properties}, {@code message_annotations},
     * {@code properties}.
     */
    public String valueAtPath(String path) {
        return ConnectorMessagePath.extract(this, path);
    }
}
