package com.rabbitmq.spark.connector;

import com.rabbitmq.stream.ConsumerBuilder;
import com.rabbitmq.stream.Environment;
import com.rabbitmq.stream.ProducerBuilder;
import com.rabbitmq.stream.StreamCreator;
import com.rabbitmq.stream.StreamStats;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for {@link EnvironmentPool} configuration and key management.
 *
 * <p>Note: Tests that exercise actual pooling with live environments
 * are in the integration test suite. These unit tests verify the pool
 * key derivation and configuration parsing.
 */
class EnvironmentPoolTest {

    @AfterEach
    void tearDown() {
        EnvironmentPool.getInstance().closeAll();
    }

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

    // ======================================================================
    // Pool behavior tests
    // ======================================================================

    @Nested
    class PoolBehaviorTests {

        @Test
        void acquireReleaseReferenceCounting() throws Exception {
            EnvironmentPool pool = EnvironmentPool.getInstance();
            ConnectorOptions options = opts("localhost:5552", "test-stream");
            EnvironmentPool.EnvironmentKey key = EnvironmentPool.EnvironmentKey.from(options);
            CountingEnvironment env = new CountingEnvironment();
            Object entry = newEntry(env);
            putEntry(key, entry);

            Environment acquired = pool.acquire(options);
            assertThat(acquired).isSameAs(env);
            assertThat(getRefCount(entry)).isEqualTo(2);

            pool.release(options);
            assertThat(getRefCount(entry)).isEqualTo(1);
            assertThat((Object) getEvictionTask(entry)).isNull();
        }

        @Test
        void doubleReleaseDoesNotGoNegative() throws Exception {
            EnvironmentPool pool = EnvironmentPool.getInstance();
            ConnectorOptions options = opts("localhost:5552", "test-stream");
            EnvironmentPool.EnvironmentKey key = EnvironmentPool.EnvironmentKey.from(options);
            Object entry = newEntry(new CountingEnvironment());
            putEntry(key, entry);

            pool.release(options);
            assertThat(getRefCount(entry)).isEqualTo(0);
            ScheduledFuture<?> scheduled = getEvictionTask(entry);
            assertThat((Object) scheduled).isNotNull();

            pool.release(options);
            assertThat(getRefCount(entry)).isEqualTo(0);
            assertThat((Object) getEvictionTask(entry)).isSameAs(scheduled);
        }

        @Test
        void evictionScheduledAndCanceledOnReacquire() throws Exception {
            EnvironmentPool pool = EnvironmentPool.getInstance();
            Map<String, String> map = minimalMap("localhost:5552", "test-stream");
            map.put("environmentIdleTimeoutMs", "60000");
            ConnectorOptions options = new ConnectorOptions(map);
            EnvironmentPool.EnvironmentKey key = EnvironmentPool.EnvironmentKey.from(options);
            CountingEnvironment env = new CountingEnvironment();
            Object entry = newEntry(env);
            putEntry(key, entry);

            pool.release(options);
            ScheduledFuture<?> scheduled = getEvictionTask(entry);
            assertThat((Object) scheduled).isNotNull();

            setRefCount(entry, 1);
            Environment acquired = pool.acquire(options);
            assertThat(acquired).isSameAs(env);
            assertThat((Object) getEvictionTask(entry)).isNull();
            assertThat(scheduled.isCancelled()).isTrue();
        }

        @Test
        void closeFailureDoesNotLeakPoolEntry() throws Exception {
            EnvironmentPool pool = EnvironmentPool.getInstance();
            ConnectorOptions options = opts("localhost:5552", "test-stream");
            EnvironmentPool.EnvironmentKey key = EnvironmentPool.EnvironmentKey.from(options);
            Object entry = newEntry(new ThrowingCloseEnvironment());
            setRefCount(entry, 0);
            putEntry(key, entry);

            invokeEvict(pool, key, entry);
            assertThat(getPoolMap()).doesNotContainKey(key);
        }

        @Test
        void keyDoesNotNormalizeEquivalentEndpoints() {
            Map<String, String> map1 = minimalMap("localhost:5552", "test-stream");
            Map<String, String> map2 = minimalMap(" localhost:5552 ", "test-stream");

            var key1 = EnvironmentPool.EnvironmentKey.from(new ConnectorOptions(map1));
            var key2 = EnvironmentPool.EnvironmentKey.from(new ConnectorOptions(map2));

            assertThat(key1).isNotEqualTo(key2);
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

    @SuppressWarnings("unchecked")
    private static ConcurrentHashMap<EnvironmentPool.EnvironmentKey, Object> getPoolMap()
            throws Exception {
        Field poolField = EnvironmentPool.class.getDeclaredField("pool");
        poolField.setAccessible(true);
        return (ConcurrentHashMap<EnvironmentPool.EnvironmentKey, Object>) poolField.get(
                EnvironmentPool.getInstance());
    }

    private static void putEntry(EnvironmentPool.EnvironmentKey key, Object entry) throws Exception {
        getPoolMap().put(key, entry);
    }

    private static Object newEntry(Environment environment) throws Exception {
        Class<?> entryClass = Class.forName("com.rabbitmq.spark.connector.EnvironmentPool$PooledEntry");
        Constructor<?> ctor = entryClass.getDeclaredConstructor(Environment.class);
        ctor.setAccessible(true);
        return ctor.newInstance(environment);
    }

    private static int getRefCount(Object entry) throws Exception {
        Field refCountField = entry.getClass().getDeclaredField("refCount");
        refCountField.setAccessible(true);
        AtomicInteger refCount = (AtomicInteger) refCountField.get(entry);
        return refCount.get();
    }

    private static void setRefCount(Object entry, int value) throws Exception {
        Field refCountField = entry.getClass().getDeclaredField("refCount");
        refCountField.setAccessible(true);
        AtomicInteger refCount = (AtomicInteger) refCountField.get(entry);
        refCount.set(value);
    }

    private static ScheduledFuture<?> getEvictionTask(Object entry) throws Exception {
        Field evictionTaskField = entry.getClass().getDeclaredField("evictionTask");
        evictionTaskField.setAccessible(true);
        return (ScheduledFuture<?>) evictionTaskField.get(entry);
    }

    private static void invokeEvict(EnvironmentPool pool, EnvironmentPool.EnvironmentKey key,
                                    Object entry) throws Exception {
        Method evict = EnvironmentPool.class.getDeclaredMethod("evict",
                EnvironmentPool.EnvironmentKey.class, entry.getClass());
        evict.setAccessible(true);
        evict.invoke(pool, key, entry);
    }

    private static final class CountingEnvironment implements Environment {
        private final AtomicInteger closeCount = new AtomicInteger();

        @Override
        public StreamCreator streamCreator() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void deleteStream(String stream) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void deleteSuperStream(String superStream) {
            throw new UnsupportedOperationException();
        }

        @Override
        public StreamStats queryStreamStats(String stream) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void storeOffset(String reference, String stream, long offset) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean streamExists(String stream) {
            throw new UnsupportedOperationException();
        }

        @Override
        public ProducerBuilder producerBuilder() {
            throw new UnsupportedOperationException();
        }

        @Override
        public ConsumerBuilder consumerBuilder() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void close() {
            closeCount.incrementAndGet();
        }
    }

    private static final class ThrowingCloseEnvironment implements Environment {
        @Override
        public StreamCreator streamCreator() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void deleteStream(String stream) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void deleteSuperStream(String superStream) {
            throw new UnsupportedOperationException();
        }

        @Override
        public StreamStats queryStreamStats(String stream) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void storeOffset(String reference, String stream, long offset) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean streamExists(String stream) {
            throw new UnsupportedOperationException();
        }

        @Override
        public ProducerBuilder producerBuilder() {
            throw new UnsupportedOperationException();
        }

        @Override
        public ConsumerBuilder consumerBuilder() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void close() {
            throw new RuntimeException("close failed");
        }
    }
}
