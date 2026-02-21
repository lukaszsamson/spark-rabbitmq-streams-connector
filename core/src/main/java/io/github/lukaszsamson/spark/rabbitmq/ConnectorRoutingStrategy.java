package io.github.lukaszsamson.spark.rabbitmq;

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
     * Metadata about the superstream routing topology.
     */
    interface Metadata extends Serializable {
        /**
         * @return all partition stream names of the target superstream
         */
        List<String> partitions();

        /**
         * Resolve candidate partition streams for a routing key according to
         * broker superstream binding metadata.
         *
         * @param routingKey routing key to resolve
         * @return partition stream names matching the routing key
         */
        List<String> route(String routingKey);
    }

    /**
     * Determine the target partition streams for a message.
     *
     * @param message connector view of the message, including body,
     *                application properties, annotations, and properties
     * @param metadata routing metadata with partition list and key-based lookup
     * @return partition stream names to publish to
     */
    List<String> route(ConnectorMessageView message, Metadata metadata);
}
