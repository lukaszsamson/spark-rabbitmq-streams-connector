package com.rabbitmq.spark.connector;

import org.apache.spark.sql.connector.metric.CustomMetric;
import org.apache.spark.sql.connector.metric.CustomSumMetric;
import org.apache.spark.sql.connector.metric.CustomTaskMetric;

/**
 * Custom metrics for the RabbitMQ Streams sink.
 *
 * <p>Task-level metrics are collected by {@link RabbitMQDataWriter} and
 * aggregated by Spark using the corresponding {@link CustomMetric} definitions.
 */
public final class RabbitMQSinkMetrics {

    private RabbitMQSinkMetrics() {}

    // ---- Metric names ----

    static final String RECORDS_WRITTEN = "recordsWritten";
    static final String BYTES_WRITTEN = "bytesWritten";

    // ---- CustomMetric definitions (registered on Write) ----

    static final CustomMetric[] SUPPORTED_METRICS = {
            new RecordsWrittenMetric(),
            new BytesWrittenMetric(),
    };

    public static final class RecordsWrittenMetric extends CustomSumMetric {
        public RecordsWrittenMetric() {}

        @Override
        public String name() {
            return RECORDS_WRITTEN;
        }

        @Override
        public String description() {
            return "Total records written to RabbitMQ streams";
        }
    }

    public static final class BytesWrittenMetric extends CustomSumMetric {
        public BytesWrittenMetric() {}

        @Override
        public String name() {
            return BYTES_WRITTEN;
        }

        @Override
        public String description() {
            return "Total bytes written to RabbitMQ streams";
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
