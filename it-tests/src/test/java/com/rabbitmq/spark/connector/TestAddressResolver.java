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
        int mappedStreamPort = Integer.parseInt(
                System.getProperty("rabbitmq.test.port", "5552"));
        int mappedTlsPort = Integer.parseInt(
                System.getProperty("rabbitmq.test.tls.port", String.valueOf(mappedStreamPort)));

        // RabbitMQ may advertise container-internal default ports (5552/5551),
        // while clients outside Docker must connect to mapped host ports.
        int requestedPort = address.port();
        int port = mappedStreamPort;
        if (requestedPort == 5551 || requestedPort == mappedTlsPort) {
            port = mappedTlsPort;
        } else if (requestedPort == 5552 || requestedPort == mappedStreamPort) {
            port = mappedStreamPort;
        }
        return new Address(host, port);
    }
}
