package io.github.lukaszsamson.spark.rabbitmq;

import io.netty.channel.Channel;

/**
 * Customizes Netty {@link Channel} instances used by RabbitMQ stream connections.
 */
public interface ConnectorNettyChannelCustomizer {

    /**
     * Apply customization to a channel.
     *
     * @param channel channel to customize
     */
    void customize(Channel channel);
}

