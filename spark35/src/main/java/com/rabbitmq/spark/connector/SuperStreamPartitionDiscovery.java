package com.rabbitmq.spark.connector;

import com.rabbitmq.stream.impl.Client;

import java.net.URI;
import java.util.Arrays;
import java.util.List;

/**
 * Discovers partition streams for a RabbitMQ superstream.
 *
 * <p>Uses the low-level {@link Client} API to query superstream partitions
 * from the broker. This is safe because the RabbitMQ client is shaded into
 * the connector jar.
 */
final class SuperStreamPartitionDiscovery {

    private SuperStreamPartitionDiscovery() {}

    /**
     * Discover partition streams using explicit connection parameters.
     *
     * @param options connector options with connection configuration
     * @param superStream the superstream name
     * @return ordered list of partition stream names
     */
    static List<String> discoverPartitions(ConnectorOptions options, String superStream) {
        Client.ClientParameters params = buildClientParams(options);
        try (Client client = new Client(params)) {
            return client.partitions(superStream);
        }
    }

    private static Client.ClientParameters buildClientParams(ConnectorOptions options) {
        Client.ClientParameters params = new Client.ClientParameters();

        applyHostAndPort(params, options);

        if (options.getUsername() != null) {
            params.username(options.getUsername());
        }
        if (options.getPassword() != null) {
            params.password(options.getPassword());
        }
        if (options.getVhost() != null) {
            params.virtualHost(options.getVhost());
        }

        return params;
    }

    private static void applyHostAndPort(Client.ClientParameters params, ConnectorOptions options) {
        if (options.getEndpoints() != null && !options.getEndpoints().isEmpty()) {
            String endpoint = Arrays.stream(options.getEndpoints().split(","))
                    .map(String::trim)
                    .filter(s -> !s.isEmpty())
                    .findFirst()
                    .orElse(null);
            if (endpoint != null) {
                String[] parts = endpoint.split(":");
                params.host(parts[0]);
                if (parts.length > 1) {
                    params.port(Integer.parseInt(parts[1].trim()));
                }
                return;
            }
        }

        if (options.getUris() != null && !options.getUris().isEmpty()) {
            String uriText = Arrays.stream(options.getUris().split(","))
                    .map(String::trim)
                    .filter(s -> !s.isEmpty())
                    .findFirst()
                    .orElse(null);
            if (uriText != null) {
                URI uri = URI.create(uriText);
                if (uri.getHost() != null) {
                    params.host(uri.getHost());
                }
                if (uri.getPort() > 0) {
                    params.port(uri.getPort());
                }
            }
        }
    }
}
