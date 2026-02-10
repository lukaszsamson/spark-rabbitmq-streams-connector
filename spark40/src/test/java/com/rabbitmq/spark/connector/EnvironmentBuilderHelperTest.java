package com.rabbitmq.spark.connector;

import com.rabbitmq.stream.impl.StreamEnvironmentBuilder;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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

    private static void invokeConfigureAddressResolver(StreamEnvironmentBuilder builder,
                                                       ConnectorOptions options) throws Exception {
        Method method = EnvironmentBuilderHelper.class.getDeclaredMethod(
                "configureAddressResolver",
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
}
