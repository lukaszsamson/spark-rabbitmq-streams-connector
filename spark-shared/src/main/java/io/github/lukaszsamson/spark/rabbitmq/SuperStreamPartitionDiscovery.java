package io.github.lukaszsamson.spark.rabbitmq;

import com.rabbitmq.stream.Environment;
import java.util.List;

/**
 * Discovers partition streams for a RabbitMQ superstream.
 */
final class SuperStreamPartitionDiscovery {

    private SuperStreamPartitionDiscovery() {}

    /**
     * Discover partition streams using connector connection options.
     *
     * @param options connector options with connection configuration
     * @param superStream the superstream name
     * @return ordered list of partition stream names
     */
    static List<String> discoverPartitions(ConnectorOptions options, String superStream) {
        Environment environment = EnvironmentPool.getInstance().acquire(options);
        try {
            return discoverPartitions(superStream,
                    stream -> discoverPartitionsViaEnvironment(environment, stream));
        } finally {
            EnvironmentPool.getInstance().release(options);
        }
    }

    static List<String> discoverPartitions(String superStream, PartitionsQuery query) {
        try {
            return query.partitions(superStream);
        } catch (Exception e) {
            throw new IllegalStateException(
                    "Unable to discover superstream partitions for '" + superStream + "'", e);
        }
    }

    private static List<String> discoverPartitionsViaEnvironment(Environment environment,
                                                                 String superStream) {
        try {
            var queryMethod = environment.getClass()
                    .getMethod("querySuperStreamPartitions", String.class);
            Object result = queryMethod.invoke(environment, superStream);
            return copyPartitions(result);
        } catch (NoSuchMethodException e) {
            throw new IllegalStateException(
                    "Superstream partition discovery requires RabbitMQ stream client support " +
                            "for Environment.querySuperStreamPartitions(String). " +
                            "Internal client API fallback is intentionally disabled.",
                    e);
        } catch (RuntimeException e) {
            throw e;
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException(
                    "Failed to query partitions for super stream '" + superStream + "'", e);
        }
    }

    @SuppressWarnings("unchecked")
    private static List<String> copyPartitions(Object value) {
        if (value == null) {
            return List.of();
        }
        List<?> partitions = (List<?>) value;
        for (Object partition : partitions) {
            if (!(partition instanceof String)) {
                throw new IllegalStateException(
                        "Unexpected partition metadata type: " +
                                (partition == null ? "null" : partition.getClass().getName()));
            }
        }
        return (List<String>) List.copyOf(partitions);
    }

    @FunctionalInterface
    interface PartitionsQuery {
        List<String> partitions(String superStream) throws Exception;
    }
}
