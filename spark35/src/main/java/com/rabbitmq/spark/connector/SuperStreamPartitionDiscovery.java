package com.rabbitmq.spark.connector;

import com.rabbitmq.stream.Environment;
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

    static List<String> discoverPartitions(Environment environment, String superStream) {
        ReflectiveOperationException lastFailure = null;

        // Preferred: direct locator().client().partitions(...) invocation.
        try {
            Object locator = invokeNoArg(environment, "locator");
            Object client = invokeNoArg(locator, "client");
            return invokePartitions(client, superStream);
        } catch (ReflectiveOperationException e) {
            lastFailure = e;
        }

        // Fallback: StreamEnvironment internals via locatorOperation(Function<Client, T>).
        try {
            tryInvokeNoArg(environment, "maybeInitializeLocator");
            Object result = invoke(
                    environment,
                    "locatorOperation",
                    new Class<?>[]{Function.class},
                    new Object[]{(Function<Object, List<String>>) c -> {
                        try {
                            return invokePartitions(c, superStream);
                        } catch (ReflectiveOperationException ex) {
                            throw new IllegalStateException(ex);
                        }
                    }});
            @SuppressWarnings("unchecked")
            List<String> partitions = (List<String>) result;
            return partitions;
        } catch (ReflectiveOperationException e) {
            if (lastFailure != null) {
                e.addSuppressed(lastFailure);
            }
            throw new IllegalStateException(
                    "Unable to discover superstream partitions via configured Environment", e);
        }
    }

    private static List<String> invokePartitions(Object client, String superStream)
            throws ReflectiveOperationException {
        Object result = invoke(
                client,
                "partitions",
                new Class<?>[]{String.class},
                new Object[]{superStream});
        @SuppressWarnings("unchecked")
        List<String> partitions = (List<String>) result;
        return partitions;
    }

    private static Object invokeNoArg(Object target, String methodName)
            throws ReflectiveOperationException {
        return invoke(target, methodName, new Class<?>[0], new Object[0]);
    }

    private static void tryInvokeNoArg(Object target, String methodName) {
        try {
            invokeNoArg(target, methodName);
        } catch (ReflectiveOperationException ignored) {
            // Optional initialization hook on internal implementations.
        }
    }

    private static Object invoke(Object target, String methodName,
                                 Class<?>[] paramTypes, Object[] args)
            throws ReflectiveOperationException {
        var method = findMethod(target.getClass(), methodName, paramTypes);
        method.setAccessible(true);
        return method.invoke(target, args);
    }

    private static java.lang.reflect.Method findMethod(Class<?> type, String methodName,
                                                       Class<?>[] paramTypes)
            throws NoSuchMethodException {
        Class<?> current = type;
        while (current != null) {
            try {
                return current.getDeclaredMethod(methodName, paramTypes);
            } catch (NoSuchMethodException ignored) {
                current = current.getSuperclass();
            }
        }
        throw new NoSuchMethodException(
                "Method '" + methodName + "' not found on " + type.getName());
    }
}
