package io.github.lukaszsamson.spark.rabbitmq;

import org.apache.spark.sql.connector.metric.CustomMetric;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Constructor;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for source/sink metric descriptors and constructor visibility
 * without requiring a broker. Metrics emission with a live broker is covered
 * by integration tests.
 */
class RabbitMQMetricsTest {

    @Test
    void sourceMetricsInstantiateWithNoArgConstructors() throws Exception {
        for (CustomMetric metric : RabbitMQSourceMetrics.SUPPORTED_METRICS) {
            Constructor<?> ctor = metric.getClass().getDeclaredConstructor();
            assertThat(ctor.canAccess(null)).isTrue();
            Object instance = ctor.newInstance();
            assertThat(instance).isInstanceOf(CustomMetric.class);
        }
    }

    @Test
    void sinkMetricsInstantiateWithNoArgConstructors() throws Exception {
        for (CustomMetric metric : RabbitMQSinkMetrics.SUPPORTED_METRICS) {
            Constructor<?> ctor = metric.getClass().getDeclaredConstructor();
            assertThat(ctor.canAccess(null)).isTrue();
            Object instance = ctor.newInstance();
            assertThat(instance).isInstanceOf(CustomMetric.class);
        }
    }

    @Test
    void sourceMetricsNamesAreUnique() {
        Set<String> names = Arrays.stream(RabbitMQSourceMetrics.SUPPORTED_METRICS)
                .map(CustomMetric::name)
                .collect(Collectors.toSet());

        assertThat(names).hasSize(RabbitMQSourceMetrics.SUPPORTED_METRICS.length);
    }

    @Test
    void sinkMetricsNamesAreUnique() {
        Set<String> names = Arrays.stream(RabbitMQSinkMetrics.SUPPORTED_METRICS)
                .map(CustomMetric::name)
                .collect(Collectors.toSet());

        assertThat(names).hasSize(RabbitMQSinkMetrics.SUPPORTED_METRICS.length);
    }

    @Test
    void sourceTaskMetricReturnsNameAndValue() {
        var metric = RabbitMQSourceMetrics.taskMetric("recordsRead", 42L);
        assertThat(metric.name()).isEqualTo("recordsRead");
        assertThat(metric.value()).isEqualTo(42L);
    }
}
