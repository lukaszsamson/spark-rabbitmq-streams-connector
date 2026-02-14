package com.rabbitmq.spark.connector;

import com.rabbitmq.stream.Environment;
import com.rabbitmq.stream.impl.Client;

import java.util.List;
import java.util.function.Function;

/**
 * Discovers partition streams for a RabbitMQ superstream via the configured
 * {@link Environment} path.
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
        Environment environment = EnvironmentBuilderHelper.buildEnvironment(options);
        try {
            return discoverPartitions(environment, superStream);
        } finally {
            try {
                environment.close();
            } catch (Exception ignored) {
                // best-effort close
            }
        }
    }

    @SuppressWarnings("unchecked")
    static List<String> discoverPartitions(Environment environment, String superStream) {
        try {
            // Ensure locators are initialized before issuing superstream operations.
            var maybeInitialize = environment.getClass().getDeclaredMethod("maybeInitializeLocator");
            maybeInitialize.setAccessible(true);
            maybeInitialize.invoke(environment);

            var locatorOperation = environment.getClass().getDeclaredMethod(
                    "locatorOperation", Function.class);
            locatorOperation.setAccessible(true);
            return (List<String>) locatorOperation.invoke(
                    environment, (Function<Client, List<String>>) c -> c.partitions(superStream));
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException(
                    "Unable to discover superstream partitions via configured Environment", e);
        }
    }
}
