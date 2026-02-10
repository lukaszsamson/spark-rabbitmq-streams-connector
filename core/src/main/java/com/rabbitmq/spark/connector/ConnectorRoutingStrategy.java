package com.rabbitmq.spark.connector;

import java.io.Serializable;
import java.util.List;

/**
 * Custom routing strategy for superstream producers.
 *
 * <p>When {@code routingStrategy=custom}, this interface determines which
 * partition streams a message should be published to.
 *
 * <p>Implementations must have a public no-arg constructor, be
 * {@link Serializable} (they are shipped to executors), and produce
 * deterministic output for a given input.
 *
 * <p>This is a connector-defined interface that avoids exposing shaded
 * RabbitMQ client types to user code.
 */
public interface ConnectorRoutingStrategy extends Serializable {

    /**
     * Determine the target partition streams for a message.
     *
     * @param routingKey the routing key extracted from the message (may be null)
     * @param partitions the list of partition stream names in the superstream
     * @return the list of partition stream names to publish to (must be non-empty
     *         and a subset of {@code partitions})
     */
    List<String> route(String routingKey, List<String> partitions);
}
