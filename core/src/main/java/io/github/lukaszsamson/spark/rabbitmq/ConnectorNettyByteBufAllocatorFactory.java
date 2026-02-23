package io.github.lukaszsamson.spark.rabbitmq;

/**
 * Creates a custom Netty byte buffer allocator for RabbitMQ stream connections.
 */
public interface ConnectorNettyByteBufAllocatorFactory {

    /**
     * Create a byte buffer allocator instance.
     *
     * @param options connector options
     * @return allocator object compatible with {@code io.netty.buffer.ByteBufAllocator}
     */
    Object create(ConnectorOptions options);
}
