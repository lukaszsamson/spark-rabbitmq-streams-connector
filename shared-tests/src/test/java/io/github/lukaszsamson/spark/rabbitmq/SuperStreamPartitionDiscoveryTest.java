package io.github.lukaszsamson.spark.rabbitmq;

import org.junit.jupiter.api.Test;

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
    void wrapsQueryFailuresWithStateException() {
        assertThatThrownBy(() -> SuperStreamPartitionDiscovery.discoverPartitions(
                "super", stream -> {
                    throw new RuntimeException("boom");
                }))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Unable to discover superstream partitions")
                .hasRootCauseMessage("boom");
    }
}
