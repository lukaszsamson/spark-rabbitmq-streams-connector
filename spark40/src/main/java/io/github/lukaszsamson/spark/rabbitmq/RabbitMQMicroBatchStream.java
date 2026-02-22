package io.github.lukaszsamson.spark.rabbitmq;

import org.apache.spark.sql.connector.read.streaming.*;
import org.apache.spark.sql.types.StructType;

import java.util.Map;

/**
 * Spark 4.0 micro-batch stream that overrides read-limit handling to use
 * the Spark 4.0+ typed APIs ({@link ReadMaxBytes}) directly instead of
 * the reflection-based fallback in the base class.
 *
 * <p>All core streaming lifecycle logic is inherited from
 * {@link BaseRabbitMQMicroBatchStream}. This subclass only replaces the
 * {@code ReadMaxBytes} dispatch path with direct {@code instanceof} checks.
 */
final class RabbitMQMicroBatchStream extends BaseRabbitMQMicroBatchStream {

    RabbitMQMicroBatchStream(ConnectorOptions options, StructType schema,
                              String checkpointLocation) {
        super(options, schema, checkpointLocation);
    }

    // ---- Spark 4.0+ direct API overrides ----

    @Override
    ReadLimit buildUpperLimit(Long maxRows, Long maxBytes) {
        if (maxRows != null && maxBytes != null) {
            return ReadLimit.compositeLimit(new ReadLimit[]{
                    ReadLimit.maxRows(maxRows),
                    ReadLimit.maxBytes(maxBytes)
            });
        }
        if (maxRows != null) {
            return ReadLimit.maxRows(maxRows);
        }
        if (maxBytes != null) {
            return ReadLimit.maxBytes(maxBytes);
        }
        return null;
    }

    @Override
    Map<String, Long> applyReadLimit(
            Map<String, Long> startOffsets,
            Map<String, Long> tailOffsets,
            ReadLimit limit) {

        if (limit instanceof ReadAllAvailable) {
            return tailOffsets;
        }

        if (limit instanceof ReadMaxRows maxRows) {
            return ReadLimitBudget.distributeRecordBudget(
                    startOffsets, tailOffsets, maxRows.maxRows());
        }

        if (limit instanceof ReadMinRows minRows) {
            return handleReadMinRowsCore(
                    minRows.minRows(), minRows.maxTriggerDelayMs(),
                    startOffsets, tailOffsets);
        }

        if (limit instanceof ReadMaxBytes maxBytes) {
            return ReadLimitBudget.distributeByteBudget(
                    startOffsets, tailOffsets, maxBytes.maxBytes(), estimatedMessageSize);
        }

        if (limit instanceof CompositeReadLimit composite) {
            Map<String, Long> result = tailOffsets;
            for (ReadLimit component : composite.getReadLimits()) {
                Map<String, Long> componentEnd = applyReadLimit(
                        startOffsets, tailOffsets, component);
                result = ReadLimitBudget.mostRestrictive(result, componentEnd);
            }
            return result;
        }

        // Unknown limit type: treat as ReadAllAvailable
        LOG.debug("Unknown ReadLimit type {}, treating as ReadAllAvailable",
                limit.getClass().getName());
        return tailOffsets;
    }
}
