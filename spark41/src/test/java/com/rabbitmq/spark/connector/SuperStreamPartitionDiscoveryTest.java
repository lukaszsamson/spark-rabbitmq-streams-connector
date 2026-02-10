package com.rabbitmq.spark.connector;

import com.rabbitmq.stream.impl.Client;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SuperStreamPartitionDiscoveryTest {

    @Test
    void endpointParsingHostAndPort() throws Exception {
        ConnectorOptions options = new ConnectorOptions(Map.of(
                "endpoints", "hostA:6000",
                "superstream", "super"
        ));

        Client.ClientParameters params = buildParams(options);

        assertThat(getField(params, "host")).isEqualTo("hostA");
        assertThat(getField(params, "port")).isEqualTo(6000);
    }

    @Test
    void endpointHostOnlyUsesDefaultPort() throws Exception {
        ConnectorOptions options = new ConnectorOptions(Map.of(
                "endpoints", "hostOnly",
                "superstream", "super"
        ));

        Client.ClientParameters params = buildParams(options);

        assertThat(getField(params, "host")).isEqualTo("hostOnly");
        assertThat(getField(params, "port")).isEqualTo(Client.DEFAULT_PORT);
    }

    @Test
    void uriParsingHostAndPort() throws Exception {
        ConnectorOptions options = new ConnectorOptions(Map.of(
                "uris", "rabbitmq-stream://uri-host:6001",
                "superstream", "super"
        ));

        Client.ClientParameters params = buildParams(options);

        assertThat(getField(params, "host")).isEqualTo("uri-host");
        assertThat(getField(params, "port")).isEqualTo(6001);
    }

    @Test
    void endpointsTakePrecedenceOverUris() throws Exception {
        ConnectorOptions options = new ConnectorOptions(Map.of(
                "endpoints", "hostA:6000",
                "uris", "rabbitmq-stream://uri-host:6001",
                "superstream", "super"
        ));

        Client.ClientParameters params = buildParams(options);

        assertThat(getField(params, "host")).isEqualTo("hostA");
        assertThat(getField(params, "port")).isEqualTo(6000);
    }

    @Test
    void malformedEndpointThrows() throws Exception {
        ConnectorOptions options = new ConnectorOptions(Map.of(
                "endpoints", "hostA:not-a-port",
                "superstream", "super"
        ));

        assertThatThrownBy(() -> buildParams(options))
                .hasCauseInstanceOf(NumberFormatException.class);
    }

    @Test
    void credentialsAndVhostAreApplied() throws Exception {
        ConnectorOptions options = new ConnectorOptions(Map.of(
                "endpoints", "hostA:6000",
                "superstream", "super",
                "username", "user",
                "password", "secret",
                "vhost", "/v1"
        ));

        Client.ClientParameters params = buildParams(options);

        Object creds = getField(params, "credentialsProvider");
        Object username = invokeGetter(creds, "getUsername");
        Object password = invokeGetter(creds, "getPassword");
        Object vhost = getField(params, "virtualHost");

        assertThat(username).isEqualTo("user");
        assertThat(password).isEqualTo("secret");
        assertThat(vhost).isEqualTo("/v1");
    }

    private static Client.ClientParameters buildParams(ConnectorOptions options) throws Exception {
        Method method = SuperStreamPartitionDiscovery.class.getDeclaredMethod(
                "buildClientParams", ConnectorOptions.class);
        method.setAccessible(true);
        return (Client.ClientParameters) method.invoke(null, options);
    }

    private static Object getField(Object target, String name) throws Exception {
        var field = target.getClass().getDeclaredField(name);
        field.setAccessible(true);
        return field.get(target);
    }

    private static Object invokeGetter(Object target, String name) throws Exception {
        Method method = target.getClass().getMethod(name);
        return method.invoke(target);
    }
}
