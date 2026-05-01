package io.github.lukaszsamson.spark.rabbitmq;

import com.rabbitmq.stream.Environment;
import java.lang.reflect.Method;
import java.util.List;
import java.util.function.Function;

/**
 * Discovers partition streams for a RabbitMQ superstream.
 *
 * <p>Compatibility: this implementation reflectively invokes
 * {@code Environment.locatorOperation(Function)} and the locator client's
 * {@code partitions(String)} method. These hooks have been part of the
 * official rabbitmq-stream-java-client since 0.x and remain present through
 * the 1.x line; the resolved {@link Method} handles are cached per runtime
 * class via {@link ClassValue} so cached entries do not pin classloaders
 * (Spark may load shaded/unshaded copies of the connector under separate
 * classloaders that should be reclaimable).
 */
final class SuperStreamPartitionDiscovery {

    /**
     * Per-class cache for the resolved {@code Environment#locatorOperation(Function)}
     * {@link Method} handle. Uses {@link ClassValue} so the cache entry is
     * weakly tied to the {@link Class}'s lifetime, preventing classloader
     * pinning in long-running Spark JVMs.
     */
    private static final ClassValue<Method> LOCATOR_OPERATION_CACHE = new ClassValue<>() {
        @Override
        protected Method computeValue(Class<?> environmentClass) {
            try {
                Method method = environmentClass.getDeclaredMethod(
                        "locatorOperation", Function.class);
                method.setAccessible(true);
                return method;
            } catch (NoSuchMethodException e) {
                throw new IllegalStateException(
                        "rabbitmq-stream-java-client " + environmentClass.getName() +
                                " does not expose locatorOperation(Function); " +
                                "client version is incompatible", e);
            }
        }
    };

    /**
     * Per-class cache for the resolved {@code partitions(String)} {@link Method}
     * handle on the locator client returned by {@code locatorOperation}. Uses
     * {@link ClassValue} for the same classloader-friendliness reasons as
     * {@link #LOCATOR_OPERATION_CACHE}.
     */
    private static final ClassValue<Method> PARTITIONS_METHOD_CACHE = new ClassValue<>() {
        @Override
        protected Method computeValue(Class<?> clientClass) {
            try {
                return clientClass.getMethod("partitions", String.class);
            } catch (NoSuchMethodException e) {
                throw new IllegalStateException(
                        "rabbitmq-stream-java-client locator " + clientClass.getName() +
                                " does not expose partitions(String); " +
                                "client version is incompatible", e);
            }
        }
    };

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
            Method locatorOperation = LOCATOR_OPERATION_CACHE.get(environment.getClass());

            Function<Object, Object> partitionsOperation = client -> {
                try {
                    Method partitionsMethod = PARTITIONS_METHOD_CACHE.get(client.getClass());
                    return partitionsMethod.invoke(client, superStream);
                } catch (ReflectiveOperationException e) {
                    throw new IllegalStateException(
                            "Failed to query partitions for super stream '" + superStream + "'",
                            e);
                }
            };

            Object result = locatorOperation.invoke(environment, partitionsOperation);
            return copyPartitions(result);
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
