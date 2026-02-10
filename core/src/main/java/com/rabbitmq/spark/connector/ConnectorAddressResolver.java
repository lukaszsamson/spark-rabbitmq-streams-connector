package com.rabbitmq.spark.connector;

/**
 * Address resolver for RabbitMQ stream connections.
 *
 * <p>Implementations must have a public no-arg constructor.
 * The resolver runs on the driver only and is not serialized to executors.
 *
 * <p>This is a connector-defined interface that avoids exposing shaded
 * RabbitMQ client types to user code.
 */
public interface ConnectorAddressResolver {

    /**
     * Resolve the given address to a (possibly different) address.
     *
     * @param address the address to resolve
     * @return the resolved address
     */
    Address resolve(Address address);

    /** An immutable host:port pair. */
    record Address(String host, int port) {
        @Override
        public String toString() {
            return host + ":" + port;
        }
    }
}
