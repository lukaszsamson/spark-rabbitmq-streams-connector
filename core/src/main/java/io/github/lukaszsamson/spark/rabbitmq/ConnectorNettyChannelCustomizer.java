package io.github.lukaszsamson.spark.rabbitmq;

import java.io.Serializable;

/**
 * Customizes Netty channel instances used by RabbitMQ stream connections.
 */
public interface ConnectorNettyChannelCustomizer extends Serializable {

    /**
     * Apply customization to a channel.
     *
     * @param channel channel object compatible with {@code io.netty.channel.Channel}
     */
    void customize(Object channel);
}
