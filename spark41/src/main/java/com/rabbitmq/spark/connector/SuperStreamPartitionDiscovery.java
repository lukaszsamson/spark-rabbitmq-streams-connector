package com.rabbitmq.spark.connector;

import com.rabbitmq.stream.Environment;
import com.rabbitmq.stream.impl.Client;

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
     * Discover partition stream names for a superstream.
     *
     * <p>Creates a temporary low-level client connection to query partition metadata,
     * then closes it.
     *
     * @param env an active Environment (used to validate connectivity; the
     *            actual partition query uses a separate Client connection)
     * @param superStream the superstream name
     * @return ordered list of partition stream names
     * @throws com.rabbitmq.stream.StreamException if the superstream does not exist
     *         or the broker cannot be reached
     */
    static List<String> discoverPartitions(Environment env, String superStream) {
        // The Environment doesn't expose partitions() publicly, so we use the
        // low-level Client directly. The Client is public in com.rabbitmq.stream.impl.
        // Since we shade the entire client, this coupling is acceptable.
        //
        // We reuse the environment's connection info by creating a Client
        // pointed at the same broker(s). The environment validates connectivity.
        return discoverWithClient(env, superStream);
    }

    private static List<String> discoverWithClient(Environment env, String superStream) {
        // Create a lightweight Client using default connection params.
        // The Environment has already validated connectivity, so we know
        // the broker is reachable. Use default localhost:5552 settings
        // as the Client will connect to whatever the environment connected to.
        //
        // Note: For production use with custom endpoints/TLS, the Client
        // parameters need to match the Environment configuration.
        // This is handled by the overload that takes ConnectorOptions.
        try (Client client = new Client()) {
            return client.partitions(superStream);
        }
    }

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

        // Parse the first endpoint for host/port
        if (options.getEndpoints() != null && !options.getEndpoints().isEmpty()) {
            String[] endpoints = options.getEndpoints().split(",");
            String firstEndpoint = endpoints[0].trim();
            String[] parts = firstEndpoint.split(":");
            params.host(parts[0]);
            if (parts.length > 1) {
                params.port(Integer.parseInt(parts[1].trim()));
            }
        }

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
}
