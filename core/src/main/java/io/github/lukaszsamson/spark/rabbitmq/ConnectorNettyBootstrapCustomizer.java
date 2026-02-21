package io.github.lukaszsamson.spark.rabbitmq;

import io.netty.bootstrap.Bootstrap;

/**
 * Customizes Netty {@link Bootstrap} instances used by RabbitMQ stream connections.
 */
public interface ConnectorNettyBootstrapCustomizer {

    /**
     * Apply customization to a bootstrap instance.
     *
     * @param bootstrap bootstrap to customize
     */
    void customize(Bootstrap bootstrap);
}

