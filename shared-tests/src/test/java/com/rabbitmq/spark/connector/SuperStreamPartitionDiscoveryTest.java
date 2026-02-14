package com.rabbitmq.spark.connector;

import com.rabbitmq.stream.ConsumerBuilder;
import com.rabbitmq.stream.Environment;
import com.rabbitmq.stream.ProducerBuilder;
import com.rabbitmq.stream.StreamCreator;
import com.rabbitmq.stream.StreamStats;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for superstream partition discovery logic without requiring a broker.
 */
class SuperStreamPartitionDiscoveryTest {

    @Test
    void discoversPartitionsViaEnvironmentLocatorClient() {
        StubLocatorEnvironment env = new StubLocatorEnvironment(List.of("super-0", "super-1"));

        List<String> partitions = SuperStreamPartitionDiscovery.discoverPartitions(env, "super");

        assertThat(partitions).containsExactly("super-0", "super-1");
    }

    @Test
    void fallsBackToLocatorOperationWhenLocatorClientApiUnavailable() {
        StubEnvironment env = new StubEnvironment(List.of("super-0", "super-1"));

        List<String> partitions = SuperStreamPartitionDiscovery.discoverPartitions(env, "super");

        assertThat(partitions).containsExactly("super-0", "super-1");
        assertThat(env.maybeInitialized).isTrue();
    }

    @Test
    void wrapsReflectionFailuresWithStateException() {
        Environment env = new MinimalEnvironment();

        assertThatThrownBy(() -> SuperStreamPartitionDiscovery.discoverPartitions(env, "super"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Unable to discover superstream partitions");
    }

    private static final class StubEnvironment extends MinimalEnvironment {
        private final List<String> partitions;
        private boolean maybeInitialized;

        private StubEnvironment(List<String> partitions) {
            this.partitions = partitions;
        }

        @SuppressWarnings("unused")
        void maybeInitializeLocator() {
            this.maybeInitialized = true;
        }

        @SuppressWarnings("unused")
        Object locatorOperation(Function<com.rabbitmq.stream.impl.Client, List<String>> operation) {
            return this.partitions;
        }
    }

    private static final class StubLocatorEnvironment extends MinimalEnvironment {
        private final List<String> partitions;

        private StubLocatorEnvironment(List<String> partitions) {
            this.partitions = partitions;
        }

        @SuppressWarnings("unused")
        Object locator() {
            return new Object() {
                @SuppressWarnings("unused")
                Object client() {
                    return new Object() {
                        @SuppressWarnings("unused")
                        List<String> partitions(String superStream) {
                            return partitions;
                        }
                    };
                }
            };
        }
    }

    private static class MinimalEnvironment implements Environment {
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
            // no-op
        }
    }
}
