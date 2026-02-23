package io.github.lukaszsamson.spark.rabbitmq;

/**
 * Creates a custom Netty event loop group for RabbitMQ stream connections.
 */
public interface ConnectorNettyEventLoopGroupFactory {

    /**
     * Create an event loop group instance.
     *
     * @param options connector options
     * @return event loop group object compatible with {@code io.netty.channel.EventLoopGroup}
     */
    Object create(ConnectorOptions options);
}
