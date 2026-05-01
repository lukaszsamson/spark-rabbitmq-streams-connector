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
     *
     * <p>{@code computeValue} re-throws {@link NoSuchMethodException} as a
     * {@link CachedReflectionException} so callers can recover the original
     * checked exception and let the existing wrap site produce the
     * documented "Failed to query partitions ..." message.
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
                throw new CachedReflectionException(e);
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
                throw new CachedReflectionException(e);
            }
        }
    };

    private SuperStreamPartitionDiscovery() {}

    private static Method locatorOperation(Class<?> environmentClass)
            throws ReflectiveOperationException {
        try {
            return LOCATOR_OPERATION_CACHE.get(environmentClass);
        } catch (CachedReflectionException e) {
            throw e.cause;
        }
    }

    private static Method partitionsMethod(Class<?> clientClass)
            throws ReflectiveOperationException {
        try {
            return PARTITIONS_METHOD_CACHE.get(clientClass);
        } catch (CachedReflectionException e) {
            throw e.cause;
        }
    }

    /**
     * Lightweight unchecked wrapper used to thread a checked
     * {@link ReflectiveOperationException} through {@link ClassValue#computeValue}
     * (which forbids checked throws). Caught and unwrapped by the {@code locatorOperation}
     * / {@code partitionsMethod} accessors so the rest of the class continues
     * to handle the original checked exception type.
     */
    private static final class CachedReflectionException extends RuntimeException {
        private static final long serialVersionUID = 1L;
        final ReflectiveOperationException cause;

        CachedReflectionException(ReflectiveOperationException cause) {
            super(cause);
            this.cause = cause;
        }
    }

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
            Method locatorOperation = locatorOperation(environment.getClass());

            Function<Object, Object> partitionsOperation = client -> {
                try {
                    Method partitionsMethod = partitionsMethod(client.getClass());
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
