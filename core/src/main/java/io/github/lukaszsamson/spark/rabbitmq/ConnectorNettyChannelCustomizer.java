package io.github.lukaszsamson.spark.rabbitmq;

/**
 * Customizes Netty channel instances used by RabbitMQ stream connections.
 */
public interface ConnectorNettyChannelCustomizer {

    /**
     * Apply customization to a channel.
     *
     * @param channel channel object compatible with {@code io.netty.channel.Channel}
     */
    void customize(Object channel);
}
