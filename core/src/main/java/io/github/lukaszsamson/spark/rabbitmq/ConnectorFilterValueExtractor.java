package io.github.lukaszsamson.spark.rabbitmq;

import java.io.Serializable;

/**
 * Extension point to derive producer-side filter values from messages.
 */
public interface ConnectorFilterValueExtractor extends Serializable {

    /**
     * Return the filter value for a message, or {@code null} for unfiltered messages.
     */
    String extract(ConnectorMessageView message);
}
