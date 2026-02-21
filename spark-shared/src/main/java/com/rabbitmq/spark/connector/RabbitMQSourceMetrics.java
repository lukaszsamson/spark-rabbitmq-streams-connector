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
public final class RabbitMQSourceMetrics {

    private RabbitMQSourceMetrics() {}

    // ---- Metric names (must match between CustomMetric and CustomTaskMetric) ----

    static final String RECORDS_READ = "recordsRead";
    static final String PAYLOAD_BYTES_READ = "payloadBytesRead";
    static final String ESTIMATED_WIRE_BYTES_READ = "estimatedWireBytesRead";
    static final String POLL_WAIT_MS = "pollWaitMs";
    static final String OFFSET_OUT_OF_RANGE = "offsetOutOfRange";
    static final String DATA_LOSS = "dataLoss";

    // ---- CustomMetric definitions (registered on Scan) ----

    static final CustomMetric[] SUPPORTED_METRICS = {
            new RecordsReadMetric(),
            new PayloadBytesReadMetric(),
            new EstimatedWireBytesReadMetric(),
            new PollWaitMsMetric(),
            new OffsetOutOfRangeMetric(),
            new DataLossMetric(),
    };

    public static final class RecordsReadMetric extends CustomSumMetric {
        public RecordsReadMetric() {}

        @Override
        public String name() {
            return RECORDS_READ;
        }

        @Override
        public String description() {
            return "Total records read from RabbitMQ streams";
        }
    }

    public static final class PayloadBytesReadMetric extends CustomSumMetric {
        public PayloadBytesReadMetric() {}

        @Override
        public String name() {
            return PAYLOAD_BYTES_READ;
        }

        @Override
        public String description() {
            return "Total payload bytes read from RabbitMQ streams";
        }
    }

    public static final class EstimatedWireBytesReadMetric extends CustomSumMetric {
        public EstimatedWireBytesReadMetric() {}

        @Override
        public String name() {
            return ESTIMATED_WIRE_BYTES_READ;
        }

        @Override
        public String description() {
            return "Estimated total wire bytes read (payload + metadata)";
        }
    }

    public static final class PollWaitMsMetric extends CustomSumMetric {
        public PollWaitMsMetric() {}

        @Override
        public String name() {
            return POLL_WAIT_MS;
        }

        @Override
        public String description() {
            return "Total queue poll wait time in milliseconds";
        }
    }

    public static final class OffsetOutOfRangeMetric extends CustomSumMetric {
        public OffsetOutOfRangeMetric() {}

        @Override
        public String name() {
            return OFFSET_OUT_OF_RANGE;
        }

        @Override
        public String description() {
            return "Number of offset-out-of-range events (retention truncation)";
        }
    }

    public static final class DataLossMetric extends CustomSumMetric {
        public DataLossMetric() {}

        @Override
        public String name() {
            return DATA_LOSS;
        }

        @Override
        public String description() {
            return "Number of detected data loss events";
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
