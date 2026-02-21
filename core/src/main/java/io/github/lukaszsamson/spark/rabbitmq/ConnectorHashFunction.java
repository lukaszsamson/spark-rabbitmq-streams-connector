package io.github.lukaszsamson.spark.rabbitmq;

import java.io.Serializable;

/**
 * Custom hash function for superstream hash routing.
 *
 * <p>When {@code routingStrategy=hash}, this interface allows overriding the
 * default MurmurHash3 hash used by the RabbitMQ stream client.
 */
public interface ConnectorHashFunction extends Serializable {

    /**
     * Hash the routing key for partition selection.
     *
     * @param routingKey routing key extracted from the message
     * @return hash value used for partition modulo
     */
    int hash(String routingKey);
}
