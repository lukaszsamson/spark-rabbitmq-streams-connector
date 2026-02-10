package com.rabbitmq.spark.connector;

import org.apache.spark.sql.connector.metric.CustomMetric;
import org.apache.spark.sql.connector.metric.CustomSumMetric;
import org.apache.spark.sql.connector.metric.CustomTaskMetric;

/**
 * Custom metrics for the RabbitMQ Streams source.
 *
 * <p>Task-level metrics are collected by {@link RabbitMQPartitionReader} and
 * aggregated by Spark using the corresponding {@link CustomMetric} definitions.
 */
final class RabbitMQSourceMetrics {

    private RabbitMQSourceMetrics() {}

    // ---- Metric names (must match between CustomMetric and CustomTaskMetric) ----

    static final String RECORDS_READ = "recordsRead";
    static final String BYTES_READ = "bytesRead";
    static final String READ_LATENCY_MS = "readLatencyMs";

    // ---- CustomMetric definitions (registered on Scan) ----

    static final CustomMetric[] SUPPORTED_METRICS = {
            new RecordsReadMetric(),
            new BytesReadMetric(),
            new ReadLatencyMsMetric(),
    };

    static final class RecordsReadMetric extends CustomSumMetric {
        @Override
        public String name() {
            return RECORDS_READ;
        }

        @Override
        public String description() {
            return "Total records read from RabbitMQ streams";
        }
    }

    static final class BytesReadMetric extends CustomSumMetric {
        @Override
        public String name() {
            return BYTES_READ;
        }

        @Override
        public String description() {
            return "Total bytes read from RabbitMQ streams";
        }
    }

    static final class ReadLatencyMsMetric extends CustomSumMetric {
        @Override
        public String name() {
            return READ_LATENCY_MS;
        }

        @Override
        public String description() {
            return "Total read latency in milliseconds (time spent waiting for messages)";
        }
    }

    // ---- CustomTaskMetric factory ----

    static CustomTaskMetric taskMetric(String name, long value) {
        return new SimpleTaskMetric(name, value);
    }

    private record SimpleTaskMetric(String name, long value) implements CustomTaskMetric {
        @Override
        public String name() {
            return name;
        }

        @Override
        public long value() {
            return value;
        }
    }
}
