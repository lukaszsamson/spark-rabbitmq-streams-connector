package com.rabbitmq.spark.connector;

import io.netty.buffer.ByteBufAllocator;

/**
 * Creates a custom Netty {@link ByteBufAllocator} for RabbitMQ stream connections.
 */
public interface ConnectorNettyByteBufAllocatorFactory {

    /**
     * Create a byte buffer allocator instance.
     *
     * @param options connector options
     * @return allocator to use
     */
    ByteBufAllocator create(ConnectorOptions options);
}

