package com.rabbitmq.spark.connector;

/**
 * Address resolver for integration tests with Testcontainers.
 *
 * <p>Routes all connections to the Docker-mapped host and port,
 * overriding the broker's advertised addresses which are
 * container-internal and unreachable from the host.
 *
 * <p>The target host/port are read from system properties
 * {@code rabbitmq.test.host} and {@code rabbitmq.test.port}.
 */
public class TestAddressResolver implements ConnectorAddressResolver {

    @Override
    public Address resolve(Address address) {
        String host = System.getProperty("rabbitmq.test.host", "localhost");
        int port = Integer.parseInt(
                System.getProperty("rabbitmq.test.port", "5552"));
        int tlsPort = Integer.parseInt(
                System.getProperty("rabbitmq.test.tls.port", String.valueOf(port)));
        if (address.port() == tlsPort) {
            port = tlsPort;
        }
        return new Address(host, port);
    }
}
