package com.rabbitmq.spark.connector;

import io.netty.channel.EventLoopGroup;

/**
 * Creates a custom Netty {@link EventLoopGroup} for RabbitMQ stream connections.
 */
public interface ConnectorNettyEventLoopGroupFactory {

    /**
     * Create an event loop group instance.
     *
     * @param options connector options
     * @return event loop group to use
     */
    EventLoopGroup create(ConnectorOptions options);
}

