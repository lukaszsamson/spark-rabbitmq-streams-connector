package com.rabbitmq.spark.connector;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for {@link EnvironmentPool} configuration and key management.
 *
 * <p>Note: Tests that exercise actual pooling with live environments
 * are in the integration test suite. These unit tests verify the pool
 * key derivation and configuration parsing.
 */
class EnvironmentPoolTest {

    // ======================================================================
    // EnvironmentKey tests
    // ======================================================================

    @Nested
    class EnvironmentKeyTests {

        @Test
        void sameOptionsProduceSameKey() {
            ConnectorOptions opts1 = opts("localhost:5552", "test-stream");
            ConnectorOptions opts2 = opts("localhost:5552", "test-stream");

            var key1 = EnvironmentPool.EnvironmentKey.from(opts1);
            var key2 = EnvironmentPool.EnvironmentKey.from(opts2);

            assertThat(key1).isEqualTo(key2);
            assertThat(key1.hashCode()).isEqualTo(key2.hashCode());
        }

        @Test
        void differentEndpointsProduceDifferentKeys() {
            ConnectorOptions opts1 = opts("host1:5552", "test-stream");
            ConnectorOptions opts2 = opts("host2:5552", "test-stream");

            var key1 = EnvironmentPool.EnvironmentKey.from(opts1);
            var key2 = EnvironmentPool.EnvironmentKey.from(opts2);

            assertThat(key1).isNotEqualTo(key2);
        }

        @Test
        void differentCredentialsProduceDifferentKeys() {
            Map<String, String> map1 = minimalMap("localhost:5552", "test-stream");
            map1.put("username", "user1");
            map1.put("password", "pass1");

            Map<String, String> map2 = minimalMap("localhost:5552", "test-stream");
            map2.put("username", "user2");
            map2.put("password", "pass2");

            var key1 = EnvironmentPool.EnvironmentKey.from(new ConnectorOptions(map1));
            var key2 = EnvironmentPool.EnvironmentKey.from(new ConnectorOptions(map2));

            assertThat(key1).isNotEqualTo(key2);
        }

        @Test
        void sameConnectionDifferentStreamSharesKey() {
            // Streams don't affect the Environment key — only connection params matter
            ConnectorOptions opts1 = opts("localhost:5552", "stream-a");
            ConnectorOptions opts2 = opts("localhost:5552", "stream-b");

            var key1 = EnvironmentPool.EnvironmentKey.from(opts1);
            var key2 = EnvironmentPool.EnvironmentKey.from(opts2);

            // Keys should be equal because connection params are the same
            assertThat(key1).isEqualTo(key2);
        }

        @Test
        void differentTlsSettingsProduceDifferentKeys() {
            Map<String, String> map1 = minimalMap("localhost:5552", "test-stream");
            map1.put("tls", "false");

            Map<String, String> map2 = minimalMap("localhost:5552", "test-stream");
            map2.put("tls", "true");
            map2.put("tls.trustAll", "true");

            var key1 = EnvironmentPool.EnvironmentKey.from(new ConnectorOptions(map1));
            var key2 = EnvironmentPool.EnvironmentKey.from(new ConnectorOptions(map2));

            assertThat(key1).isNotEqualTo(key2);
        }

        @Test
        void differentVhostProduceDifferentKeys() {
            Map<String, String> map1 = minimalMap("localhost:5552", "test-stream");
            map1.put("vhost", "vhost1");

            Map<String, String> map2 = minimalMap("localhost:5552", "test-stream");
            map2.put("vhost", "vhost2");

            var key1 = EnvironmentPool.EnvironmentKey.from(new ConnectorOptions(map1));
            var key2 = EnvironmentPool.EnvironmentKey.from(new ConnectorOptions(map2));

            assertThat(key1).isNotEqualTo(key2);
        }

        @Test
        void keyIncludesAddressResolverClass() {
            Map<String, String> map1 = minimalMap("localhost:5552", "test-stream");

            Map<String, String> map2 = minimalMap("localhost:5552", "test-stream");
            map2.put("addressResolverClass", "com.example.MyResolver");

            var key1 = EnvironmentPool.EnvironmentKey.from(new ConnectorOptions(map1));
            var key2 = EnvironmentPool.EnvironmentKey.from(new ConnectorOptions(map2));

            assertThat(key1).isNotEqualTo(key2);
        }

        @Test
        void uriBasedKeyDiffersFromEndpointBased() {
            Map<String, String> map1 = minimalMap("localhost:5552", "test-stream");

            Map<String, String> map2 = new LinkedHashMap<>();
            map2.put("stream", "test-stream");
            map2.put("uris", "rabbitmq-stream://localhost:5552");

            var key1 = EnvironmentPool.EnvironmentKey.from(new ConnectorOptions(map1));
            var key2 = EnvironmentPool.EnvironmentKey.from(new ConnectorOptions(map2));

            assertThat(key1).isNotEqualTo(key2);
        }
    }

    // ======================================================================
    // Pool configuration tests
    // ======================================================================

    @Nested
    class PoolConfigTests {

        @Test
        void defaultIdleTimeoutIs60Seconds() {
            ConnectorOptions opts = opts("localhost:5552", "test-stream");
            assertThat(opts.getEnvironmentIdleTimeoutMs()).isEqualTo(60_000L);
        }

        @Test
        void customIdleTimeout() {
            Map<String, String> map = minimalMap("localhost:5552", "test-stream");
            map.put("environmentIdleTimeoutMs", "120000");
            ConnectorOptions opts = new ConnectorOptions(map);
            assertThat(opts.getEnvironmentIdleTimeoutMs()).isEqualTo(120_000L);
        }

        @Test
        void singletonInstance() {
            EnvironmentPool pool1 = EnvironmentPool.getInstance();
            EnvironmentPool pool2 = EnvironmentPool.getInstance();
            assertThat(pool1).isSameAs(pool2);
        }
    }

    // ---- Helpers ----

    private static Map<String, String> minimalMap(String endpoints, String stream) {
        Map<String, String> map = new LinkedHashMap<>();
        map.put("endpoints", endpoints);
        map.put("stream", stream);
        return map;
    }

    private static ConnectorOptions opts(String endpoints, String stream) {
        return new ConnectorOptions(minimalMap(endpoints, stream));
    }
}
