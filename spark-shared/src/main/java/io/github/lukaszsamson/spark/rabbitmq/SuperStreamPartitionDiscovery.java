package io.github.lukaszsamson.spark.rabbitmq;

import com.rabbitmq.stream.Environment;
import java.lang.reflect.Method;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

/**
 * Discovers partition streams for a RabbitMQ superstream.
 *
 * <p>Compatibility: this implementation reflectively invokes
 * {@code Environment.locatorOperation(Function)} and the locator client's
 * {@code partitions(String)} method. These hooks have been part of the
 * official rabbitmq-stream-java-client since 0.x and remain present through
 * the 1.x line; the resolved {@link Method} handles are cached statically per
 * runtime class to avoid re-doing the lookup on every superstream discovery.
 */
final class SuperStreamPartitionDiscovery {

    /**
     * Cache of resolved {@code Environment#locatorOperation(Function)} {@link Method}
     * handles, keyed by the concrete environment runtime class. Populated on first
     * use; entries live for the lifetime of the JVM (one per runtime class).
     */
    private static final ConcurrentHashMap<Class<?>, Method> LOCATOR_OPERATION_CACHE =
            new ConcurrentHashMap<>();

    /**
     * Cache of resolved {@code Client#partitions(String)} {@link Method} handles,
     * keyed by the concrete client runtime class returned by the locator
     * operation. Populated lazily on first use.
     */
    private static final ConcurrentHashMap<Class<?>, Method> PARTITIONS_METHOD_CACHE =
            new ConcurrentHashMap<>();

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
            Method locatorOperation = resolveLocatorOperation(environment.getClass());

            Function<Object, Object> partitionsOperation = client -> {
                try {
                    Method partitionsMethod = resolvePartitionsMethod(client.getClass());
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

    private static Method resolveLocatorOperation(Class<?> environmentClass)
            throws ReflectiveOperationException {
        Method cached = LOCATOR_OPERATION_CACHE.get(environmentClass);
        if (cached != null) {
            return cached;
        }
        Method resolved = environmentClass.getDeclaredMethod("locatorOperation", Function.class);
        resolved.setAccessible(true);
        // putIfAbsent for benign-race-on-first-call semantics (Method.equals
        // distinguishes by declaring class + signature, so duplicates are fine).
        Method existing = LOCATOR_OPERATION_CACHE.putIfAbsent(environmentClass, resolved);
        return existing != null ? existing : resolved;
    }

    private static Method resolvePartitionsMethod(Class<?> clientClass)
            throws ReflectiveOperationException {
        Method cached = PARTITIONS_METHOD_CACHE.get(clientClass);
        if (cached != null) {
            return cached;
        }
        Method resolved = clientClass.getMethod("partitions", String.class);
        Method existing = PARTITIONS_METHOD_CACHE.putIfAbsent(clientClass, resolved);
        return existing != null ? existing : resolved;
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
