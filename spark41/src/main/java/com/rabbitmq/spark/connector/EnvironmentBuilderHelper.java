package com.rabbitmq.spark.connector;

import com.rabbitmq.stream.Address;
import com.rabbitmq.stream.Environment;
import com.rabbitmq.stream.EnvironmentBuilder;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Builds a RabbitMQ {@link Environment} from {@link ConnectorOptions}.
 *
 * <p>Handles endpoints/URIs, TLS, credentials, vhost, and address resolver configuration.
 */
final class EnvironmentBuilderHelper {

    private static final int DEFAULT_STREAM_PORT = 5552;
    private static final int DEFAULT_STREAM_TLS_PORT = 5551;

    private EnvironmentBuilderHelper() {}

    /**
     * Build a RabbitMQ Environment from connector options.
     *
     * @param options parsed connector options
     * @return a configured and connected Environment
     */
    static Environment buildEnvironment(ConnectorOptions options) {
        EnvironmentBuilder builder = Environment.builder();

        configureConnection(builder, options);
        configureCredentials(builder, options);
        configureTls(builder, options);
        configureAddressResolver(builder, options);

        return builder.build();
    }

    private static void configureConnection(EnvironmentBuilder builder, ConnectorOptions options) {
        if (options.getUris() != null && !options.getUris().isEmpty()) {
            List<String> uris = Arrays.stream(options.getUris().split(","))
                    .map(String::trim)
                    .filter(s -> !s.isEmpty())
                    .toList();
            builder.uris(uris);
        } else if (options.getEndpoints() != null && !options.getEndpoints().isEmpty()) {
            String scheme = options.isTls() ? "rabbitmq-stream+tls" : "rabbitmq-stream";
            List<String> uris = new ArrayList<>();
            for (String endpoint : options.getEndpoints().split(",")) {
                String trimmed = endpoint.trim();
                if (trimmed.isEmpty()) continue;
                String[] parts = trimmed.split(":");
                String host = parts[0];
                int port = parts.length > 1
                        ? Integer.parseInt(parts[1].trim())
                        : (options.isTls() ? DEFAULT_STREAM_TLS_PORT : DEFAULT_STREAM_PORT);
                uris.add(scheme + "://" + host + ":" + port);
            }
            builder.uris(uris);
        }
    }

    private static void configureCredentials(EnvironmentBuilder builder,
                                              ConnectorOptions options) {
        if (options.getUsername() != null) {
            builder.username(options.getUsername());
        }
        if (options.getPassword() != null) {
            builder.password(options.getPassword());
        }
        if (options.getVhost() != null) {
            builder.virtualHost(options.getVhost());
        }
    }

    private static void configureTls(EnvironmentBuilder builder, ConnectorOptions options) {
        if (!options.isTls()) {
            return;
        }
        if (options.isTlsTrustAll()) {
            builder.tls().trustEverything().environmentBuilder();
        } else {
            // Basic TLS with system defaults; JKS truststore/keystore support is TODO (M7)
            builder.tls().environmentBuilder();
        }
    }

    private static void configureAddressResolver(EnvironmentBuilder builder,
                                                  ConnectorOptions options) {
        String resolverClass = options.getAddressResolverClass();
        if (resolverClass == null || resolverClass.isEmpty()) {
            return;
        }
        ConnectorAddressResolver resolver = ExtensionLoader.load(
                resolverClass, ConnectorAddressResolver.class,
                ConnectorOptions.ADDRESS_RESOLVER_CLASS);
        builder.addressResolver(address -> {
            ConnectorAddressResolver.Address resolved = resolver.resolve(
                    new ConnectorAddressResolver.Address(address.host(), address.port()));
            return new Address(resolved.host(), resolved.port());
        });
    }
}
