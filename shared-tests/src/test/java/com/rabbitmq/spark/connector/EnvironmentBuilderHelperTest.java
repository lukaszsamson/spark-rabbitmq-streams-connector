package com.rabbitmq.spark.connector;

import com.rabbitmq.stream.impl.StreamEnvironmentBuilder;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for {@link EnvironmentBuilderHelper} option parsing and wiring
 * without requiring a live RabbitMQ broker. Broker connection behavior is
 * covered by integration tests.
 */
class EnvironmentBuilderHelperTest {

    @Test
    void endpointParsingUsesDefaultPorts() throws Exception {
        ConnectorOptions options = new ConnectorOptions(Map.of(
                "endpoints", "hostA,hostB:6000",
                "stream", "test-stream"
        ));

        StreamEnvironmentBuilder builder = new StreamEnvironmentBuilder();
        invokeConfigureConnection(builder, options);
        List<String> uris = getUris(builder);

        assertThat(uris).containsExactly(
                "rabbitmq-stream://hostA:5552",
                "rabbitmq-stream://hostB:6000"
        );
    }

    @Test
    void endpointParsingUsesTlsDefaultPortWhenTlsEnabled() throws Exception {
        ConnectorOptions options = new ConnectorOptions(Map.of(
                "endpoints", "hostA",
                "stream", "test-stream",
                "tls", "true",
                "tls.trustAll", "true"
        ));

        StreamEnvironmentBuilder builder = new StreamEnvironmentBuilder();
        invokeConfigureConnection(builder, options);
        List<String> uris = getUris(builder);

        assertThat(uris).containsExactly("rabbitmq-stream+tls://hostA:5551");
    }

    @Test
    void urisParsingUsesFirstTrimmedEntries() throws Exception {
        ConnectorOptions options = new ConnectorOptions(Map.of(
                "uris", " rabbitmq-stream://h1:5552 ,, rabbitmq-stream://h2:5553 ",
                "stream", "test-stream"
        ));

        StreamEnvironmentBuilder builder = new StreamEnvironmentBuilder();
        invokeConfigureConnection(builder, options);
        List<String> uris = getUris(builder);

        assertThat(uris).containsExactly(
                "rabbitmq-stream://h1:5552",
                "rabbitmq-stream://h2:5553"
        );
    }

    @Test
    void configureCredentialsSetsUsernamePasswordVhost() throws Exception {
        ConnectorOptions options = new ConnectorOptions(Map.of(
                "endpoints", "hostA",
                "stream", "test-stream",
                "username", "user1",
                "password", "pass1",
                "vhost", "/v1"
        ));

        StreamEnvironmentBuilder builder = new StreamEnvironmentBuilder();
        invokeConfigureCredentials(builder, options);

        Object clientParameters = getClientParameters(builder);
        Object credentialsProvider = getFieldValue(clientParameters, "credentialsProvider");
        assertThat(invokeStringGetter(credentialsProvider, "getUsername")).isEqualTo("user1");
        assertThat(invokeStringGetter(credentialsProvider, "getPassword")).isEqualTo("pass1");
        assertThat(getFieldValue(clientParameters, "virtualHost")).isEqualTo("/v1");
    }

    @Test
    void tlsTrustAllConfiguresSslWithoutJks() throws Exception {
        ConnectorOptions options = new ConnectorOptions(Map.of(
                "endpoints", "hostA",
                "stream", "test-stream",
                "tls", "true",
                "tls.trustAll", "true"
        ));

        StreamEnvironmentBuilder builder = new StreamEnvironmentBuilder();
        invokeConfigureTls(builder, options);

        Object tlsConfig = getTlsConfig(builder);
        assertThat(getTlsEnabled(tlsConfig)).isTrue();
        assertThat(getTlsSslContext(tlsConfig)).isNotNull();
    }

    @Test
    void truststoreLoadingFailureIsIllegalArgumentException() {
        ConnectorOptions options = new ConnectorOptions(Map.of(
                "endpoints", "hostA",
                "stream", "test-stream",
                "tls", "true",
                "tls.truststore", "missing.jks",
                "tls.truststorePassword", "secret"
        ));

        assertThatThrownBy(() -> invokeBuildSslContext(options))
                .hasCauseInstanceOf(IllegalArgumentException.class)
                .cause()
                .hasMessageContaining("Failed to initialize TLS JKS configuration");
    }

    @Test
    void addressResolverClassReceivesMappedHostPort() throws Exception {
        ConnectorOptions options = new ConnectorOptions(Map.of(
                "endpoints", "hostA:5552",
                "stream", "test-stream",
                "addressResolverClass", "com.rabbitmq.spark.connector.EnvironmentBuilderHelperTest$TestAddressResolver"
        ));

        StreamEnvironmentBuilder builder = new StreamEnvironmentBuilder();
        invokeConfigureAddressResolver(builder, options);
        invokeAddressResolver(builder, "hostA", 5552);

        assertThat(TestAddressResolver.lastResolved).isNotNull();
        assertThat(TestAddressResolver.lastResolved.host()).isEqualTo("resolved-host");
        assertThat(TestAddressResolver.lastResolved.port()).isEqualTo(6000);
    }

    @Test
    void observationCollectorClassConfiguresBuilderCollector() throws Exception {
        ConnectorOptions options = new ConnectorOptions(Map.of(
                "endpoints", "hostA:5552",
                "stream", "test-stream",
                "observationCollectorClass",
                "com.rabbitmq.spark.connector.EnvironmentBuilderHelperTest$TestObservationCollectorFactory"
        ));

        StreamEnvironmentBuilder builder = new StreamEnvironmentBuilder();
        invokeConfigureObservationCollector(builder, options);

        Object collector = getFieldValue(builder, "observationCollector");
        assertThat(collector).isSameAs(TestObservationCollectorFactory.COLLECTOR);
    }

    @Test
    void observationCollectorClassReturningNullFailsFast() {
        ConnectorOptions options = new ConnectorOptions(Map.of(
                "endpoints", "hostA:5552",
                "stream", "test-stream",
                "observationCollectorClass",
                "com.rabbitmq.spark.connector.EnvironmentBuilderHelperTest$NullObservationCollectorFactory"
        ));

        StreamEnvironmentBuilder builder = new StreamEnvironmentBuilder();
        assertThatThrownBy(() -> invokeConfigureObservationCollector(builder, options))
                .hasCauseInstanceOf(IllegalArgumentException.class)
                .cause()
                .hasMessageContaining("observationCollectorClass")
                .hasMessageContaining("returned null");
    }

    @Test
    void appliesHighPriorityEnvironmentTuningOptions() throws Exception {
        ConnectorOptions options = new ConnectorOptions(Map.of(
                "endpoints", "hostA:5552",
                "stream", "test-stream",
                "environmentId", "spark-rmq-prod",
                "rpcTimeoutMs", "15000",
                "requestedHeartbeatSeconds", "30",
                "forceReplicaForConsumers", "true",
                "forceLeaderForProducers", "false",
                "locatorConnectionCount", "3"
        ));

        StreamEnvironmentBuilder builder = new StreamEnvironmentBuilder();
        invokeConfigureConnection(builder, options);
        invokeConfigureCredentials(builder, options);
        invokeConfigureTls(builder, options);
        invokeConfigureAddressResolver(builder, options);
        invokeConfigureTuning(builder, options);

        assertThat(getFieldValue(builder, "id")).isEqualTo("spark-rmq-prod");
        assertThat(getFieldValue(builder, "forceReplicaForConsumers")).isEqualTo(true);
        assertThat(getFieldValue(builder, "forceLeaderForProducers")).isEqualTo(false);
        assertThat(getFieldValue(builder, "locatorConnectionCount")).isEqualTo(3);

        Object clientParameters = getClientParameters(builder);
        assertThat(getFieldValue(clientParameters, "rpcTimeout"))
                .hasToString("PT15S");
        assertThat(getFieldValue(clientParameters, "requestedHeartbeat"))
                .hasToString("PT30S");
    }

    private static List<String> getUris(StreamEnvironmentBuilder builder) throws Exception {
        Field urisField = StreamEnvironmentBuilder.class.getDeclaredField("uris");
        urisField.setAccessible(true);
        @SuppressWarnings("unchecked")
        List<java.net.URI> uris = (List<java.net.URI>) urisField.get(builder);
        return uris.stream().map(java.net.URI::toString).toList();
    }

    private static void invokeConfigureConnection(StreamEnvironmentBuilder builder,
                                                  ConnectorOptions options) throws Exception {
        Method method = EnvironmentBuilderHelper.class.getDeclaredMethod(
                "configureConnection",
                com.rabbitmq.stream.EnvironmentBuilder.class,
                ConnectorOptions.class);
        method.setAccessible(true);
        method.invoke(null, builder, options);
    }

    private static void invokeConfigureTls(StreamEnvironmentBuilder builder,
                                           ConnectorOptions options) throws Exception {
        Method method = EnvironmentBuilderHelper.class.getDeclaredMethod(
                "configureTls",
                com.rabbitmq.stream.EnvironmentBuilder.class,
                ConnectorOptions.class);
        method.setAccessible(true);
        method.invoke(null, builder, options);
    }

    private static void invokeConfigureCredentials(StreamEnvironmentBuilder builder,
                                                   ConnectorOptions options) throws Exception {
        Method method = EnvironmentBuilderHelper.class.getDeclaredMethod(
                "configureCredentials",
                com.rabbitmq.stream.EnvironmentBuilder.class,
                ConnectorOptions.class);
        method.setAccessible(true);
        method.invoke(null, builder, options);
    }

    private static void invokeConfigureAddressResolver(StreamEnvironmentBuilder builder,
                                                       ConnectorOptions options) throws Exception {
        Method method = EnvironmentBuilderHelper.class.getDeclaredMethod(
                "configureAddressResolver",
                com.rabbitmq.stream.EnvironmentBuilder.class,
                ConnectorOptions.class);
        method.setAccessible(true);
        method.invoke(null, builder, options);
    }

    private static void invokeConfigureTuning(StreamEnvironmentBuilder builder,
                                              ConnectorOptions options) throws Exception {
        Method method = EnvironmentBuilderHelper.class.getDeclaredMethod(
                "configureTuning",
                com.rabbitmq.stream.EnvironmentBuilder.class,
                ConnectorOptions.class);
        method.setAccessible(true);
        method.invoke(null, builder, options);
    }

    private static void invokeConfigureObservationCollector(StreamEnvironmentBuilder builder,
                                                            ConnectorOptions options) throws Exception {
        Method method = EnvironmentBuilderHelper.class.getDeclaredMethod(
                "configureObservationCollector",
                com.rabbitmq.stream.EnvironmentBuilder.class,
                ConnectorOptions.class);
        method.setAccessible(true);
        method.invoke(null, builder, options);
    }

    private static Object invokeBuildSslContext(ConnectorOptions options) throws Exception {
        Method method = EnvironmentBuilderHelper.class.getDeclaredMethod(
                "buildSslContext", ConnectorOptions.class);
        method.setAccessible(true);
        return method.invoke(null, options);
    }

    private static void invokeAddressResolver(StreamEnvironmentBuilder builder,
                                              String host, int port) throws Exception {
        Field resolverField = StreamEnvironmentBuilder.class.getDeclaredField("addressResolver");
        resolverField.setAccessible(true);
        com.rabbitmq.stream.AddressResolver resolver =
                (com.rabbitmq.stream.AddressResolver) resolverField.get(builder);
        resolver.resolve(new com.rabbitmq.stream.Address(host, port));
    }

    private static Object getTlsConfig(StreamEnvironmentBuilder builder) throws Exception {
        Field tlsField = StreamEnvironmentBuilder.class.getDeclaredField("tls");
        tlsField.setAccessible(true);
        return tlsField.get(builder);
    }

    private static Object getClientParameters(StreamEnvironmentBuilder builder) throws Exception {
        return getFieldValue(builder, "clientParameters");
    }

    private static Object getFieldValue(Object target, String fieldName) throws Exception {
        Class<?> current = target.getClass();
        while (current != null) {
            try {
                Field field = current.getDeclaredField(fieldName);
                field.setAccessible(true);
                return field.get(target);
            } catch (NoSuchFieldException ignored) {
                current = current.getSuperclass();
            }
        }
        throw new NoSuchFieldException(fieldName);
    }

    private static String invokeStringGetter(Object target, String methodName) throws Exception {
        Method method = target.getClass().getDeclaredMethod(methodName);
        method.setAccessible(true);
        return (String) method.invoke(target);
    }

    private static boolean getTlsEnabled(Object tlsConfig) throws Exception {
        Method method = tlsConfig.getClass().getDeclaredMethod("enabled");
        method.setAccessible(true);
        return (boolean) method.invoke(tlsConfig);
    }

    private static Object getTlsSslContext(Object tlsConfig) throws Exception {
        Method method = tlsConfig.getClass().getDeclaredMethod("sslContext");
        method.setAccessible(true);
        return method.invoke(tlsConfig);
    }

    public static final class TestAddressResolver implements ConnectorAddressResolver {
        static Address lastResolved;

        @Override
        public Address resolve(Address address) {
            lastResolved = new Address("resolved-host", 6000);
            return lastResolved;
        }
    }

    public static final class TestObservationCollectorFactory
            implements ConnectorObservationCollectorFactory {
        static final com.rabbitmq.stream.ObservationCollector<?> COLLECTOR =
                com.rabbitmq.stream.ObservationCollector.NO_OP;

        @Override
        public com.rabbitmq.stream.ObservationCollector<?> create(ConnectorOptions options) {
            return COLLECTOR;
        }
    }

    public static final class NullObservationCollectorFactory
            implements ConnectorObservationCollectorFactory {
        @Override
        public com.rabbitmq.stream.ObservationCollector<?> create(ConnectorOptions options) {
            return null;
        }
    }
}
