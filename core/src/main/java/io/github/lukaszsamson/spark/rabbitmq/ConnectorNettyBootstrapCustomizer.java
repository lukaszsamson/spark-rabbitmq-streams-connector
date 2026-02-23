package io.github.lukaszsamson.spark.rabbitmq;

import java.io.Serializable;

/**
 * Customizes Netty bootstrap instances used by RabbitMQ stream connections.
 */
public interface ConnectorNettyBootstrapCustomizer extends Serializable {

    /**
     * Apply customization to a bootstrap instance.
     *
     * @param bootstrap bootstrap object compatible with {@code io.netty.bootstrap.Bootstrap}
     */
    void customize(Object bootstrap);
}
