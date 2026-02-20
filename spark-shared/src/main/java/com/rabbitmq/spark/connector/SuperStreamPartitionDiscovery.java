package com.rabbitmq.spark.connector;

import com.rabbitmq.stream.impl.Client;

import java.net.URI;
import java.util.Arrays;
import java.util.List;

/**
 * Discovers partition streams for a RabbitMQ superstream.
 */
final class SuperStreamPartitionDiscovery {

    private static final int DEFAULT_STREAM_PORT = 5552;
    private static final int DEFAULT_STREAM_TLS_PORT = 5551;

    private SuperStreamPartitionDiscovery() {}

    /**
     * Discover partition streams using connector connection options.
     *
     * @param options connector options with connection configuration
     * @param superStream the superstream name
     * @return ordered list of partition stream names
     */
    static List<String> discoverPartitions(ConnectorOptions options, String superStream) {
        Client.ClientParameters params = buildClientParams(options);
        return discoverPartitions(superStream, stream -> {
            try (Client client = new Client(params)) {
                return client.partitions(stream);
            }
        });
    }

    static List<String> discoverPartitions(String superStream, PartitionsQuery query) {
        try {
            return query.partitions(superStream);
        } catch (Exception e) {
            throw new IllegalStateException(
                    "Unable to discover superstream partitions for '" + superStream + "'", e);
        }
    }

    private static Client.ClientParameters buildClientParams(ConnectorOptions options) {
        Client.ClientParameters params = new Client.ClientParameters();
        if (hasText(options.getUris())) {
            List<URI> uris = Arrays.stream(options.getUris().split(","))
                    .map(String::trim)
                    .filter(s -> !s.isEmpty())
                    .map(URI::create)
                    .toList();
            params = Client.maybeSetUpClientParametersFromUris(uris, params);
        } else {
            applyHostAndPort(params, options);
        }
        if (hasText(options.getUsername())) {
            params.username(options.getUsername());
        }
        if (hasText(options.getPassword())) {
            params.password(options.getPassword());
        }
        if (hasText(options.getVhost())) {
            params.virtualHost(options.getVhost());
        }
        return params;
    }

    private static void applyHostAndPort(Client.ClientParameters params, ConnectorOptions options) {
        String endpoint = Arrays.stream(options.getEndpoints().split(","))
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .findFirst()
                .orElse(null);
        if (endpoint == null) {
            return;
        }
        String[] parts = endpoint.split(":");
        String host = parts[0];
        int port = parts.length > 1
                ? Integer.parseInt(parts[1].trim())
                : (options.isTls() ? DEFAULT_STREAM_TLS_PORT : DEFAULT_STREAM_PORT);
        params.host(host).port(port);
    }

    private static boolean hasText(String value) {
        return value != null && !value.isBlank();
    }

    @FunctionalInterface
    interface PartitionsQuery {
        List<String> partitions(String superStream) throws Exception;
    }
}
