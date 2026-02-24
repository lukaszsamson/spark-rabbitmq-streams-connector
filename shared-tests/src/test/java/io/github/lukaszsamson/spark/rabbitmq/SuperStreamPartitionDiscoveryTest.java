package io.github.lukaszsamson.spark.rabbitmq;

import com.rabbitmq.stream.ConsumerBuilder;
import com.rabbitmq.stream.Environment;
import com.rabbitmq.stream.ProducerBuilder;
import com.rabbitmq.stream.StreamCreator;
import com.rabbitmq.stream.StreamStats;
import org.junit.jupiter.api.Test;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SuperStreamPartitionDiscoveryTest {

    @Test
    void discoversPartitionsViaQuery() {
        List<String> partitions = SuperStreamPartitionDiscovery.discoverPartitions(
                "super", stream -> List.of(stream + "-0", stream + "-1"));

        assertThat(partitions).containsExactly("super-0", "super-1");
    }

    @Test
    void discoversPartitionsViaEnvironmentQueryMethodWhenAvailable() throws Exception {
        QueryMethodEnvironment environment =
                new QueryMethodEnvironment(List.of("super-0", "super-1"));

        List<String> partitions = invokeDiscoverViaEnvironment(environment, "super");

        assertThat(partitions).containsExactly("super-0", "super-1");
        assertThat(environment.queryCalls).isEqualTo(1);
    }

    @Test
    void failsFastWhenEnvironmentQueryMethodIsUnavailable() {
        assertThatThrownBy(() -> invokeDiscoverViaEnvironment(new LegacyEnvironment(), "super"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("requires RabbitMQ stream client support")
                .hasMessageContaining("querySuperStreamPartitions")
                .hasRootCauseInstanceOf(NoSuchMethodException.class);
    }

    @Test
    void wrapsQueryFailuresWithStateException() {
        assertThatThrownBy(() -> SuperStreamPartitionDiscovery.discoverPartitions(
                "super", stream -> {
                    throw new RuntimeException("boom");
                }))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Unable to discover superstream partitions")
                .hasRootCauseMessage("boom");
    }

    @SuppressWarnings("unchecked")
    private static List<String> invokeDiscoverViaEnvironment(Environment environment, String superStream)
            throws Exception {
        Method method = SuperStreamPartitionDiscovery.class.getDeclaredMethod(
                "discoverPartitionsViaEnvironment", Environment.class, String.class);
        method.setAccessible(true);
        try {
            return (List<String>) method.invoke(null, environment, superStream);
        } catch (InvocationTargetException e) {
            Throwable cause = e.getCause();
            if (cause instanceof Exception exception) {
                throw exception;
            }
            throw e;
        }
    }

    private abstract static class NoopEnvironment implements Environment {
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
        public void close() {}
    }

    private static final class QueryMethodEnvironment extends NoopEnvironment {
        private final List<String> partitions;
        private int queryCalls;

        private QueryMethodEnvironment(List<String> partitions) {
            this.partitions = partitions;
        }

        public List<String> querySuperStreamPartitions(String superStream) {
            queryCalls++;
            return partitions;
        }
    }

    private static final class LegacyEnvironment extends NoopEnvironment {
    }
}
