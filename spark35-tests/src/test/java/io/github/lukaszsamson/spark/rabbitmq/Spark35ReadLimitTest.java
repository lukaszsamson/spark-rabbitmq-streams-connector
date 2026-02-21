package io.github.lukaszsamson.spark.rabbitmq;

import org.apache.spark.sql.connector.read.streaming.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests specific to Spark 3.5 {@link RabbitMQMicroBatchStream#getDefaultReadLimit()} behavior,
 * where {@code ReadMaxBytes} is not available and byte limits are converted to row-based limits.
 */
class Spark35ReadLimitTest {

    @Test
    void maxBytesPerTriggerAloneReturnsMaxRowsBasedOnEstimatedSize() {
        Map<String, String> opts = new LinkedHashMap<>();
        opts.put("endpoints", "localhost:5552");
        opts.put("stream", "test-stream");
        opts.put("maxBytesPerTrigger", "1048576");
        RabbitMQMicroBatchStream stream = createStream(new ConnectorOptions(opts));

        ReadLimit limit = stream.getDefaultReadLimit();
        assertThat(limit).isInstanceOf(ReadMaxRows.class);
        // 1048576 / 1024 (default estimatedMessageSize) = 1024
        assertThat(((ReadMaxRows) limit).maxRows()).isEqualTo(1024L);
    }

    @Test
    void bothLimitsSetReturnsCompositeWithTwoMaxRows() {
        Map<String, String> opts = new LinkedHashMap<>();
        opts.put("endpoints", "localhost:5552");
        opts.put("stream", "test-stream");
        opts.put("maxRecordsPerTrigger", "500");
        opts.put("maxBytesPerTrigger", "1048576");
        RabbitMQMicroBatchStream stream = createStream(new ConnectorOptions(opts));

        ReadLimit limit = stream.getDefaultReadLimit();
        assertThat(limit).isInstanceOf(CompositeReadLimit.class);

        CompositeReadLimit composite = (CompositeReadLimit) limit;
        ReadLimit[] components = composite.getReadLimits();
        assertThat(components).hasSize(2);

        // Both components should be ReadMaxRows (no ReadMaxBytes in Spark 3.5)
        for (ReadLimit component : components) {
            assertThat(component).isInstanceOf(ReadMaxRows.class);
        }

        // First component: maxRows from maxRecordsPerTrigger
        assertThat(((ReadMaxRows) components[0]).maxRows()).isEqualTo(500L);
        // Second component: maxBytes converted to rows (1048576 / 1024 = 1024)
        assertThat(((ReadMaxRows) components[1]).maxRows()).isEqualTo(1024L);
    }

    @Test
    void compositeReadLimitAppliesMostRestrictivePerStreamRowBased() throws Exception {
        RabbitMQMicroBatchStream stream = createStream(minimalOptions());
        Map<String, Long> snapshot = new LinkedHashMap<>();
        snapshot.put("s1", 100L);
        snapshot.put("s2", 200L);
        setPrivateField(stream, "availableNowSnapshot", snapshot);
        setPrivateField(stream, "estimatedMessageSize", 2);

        Map<String, Long> startOffsets = new LinkedHashMap<>();
        startOffsets.put("s1", 0L);
        startOffsets.put("s2", 0L);
        RabbitMQStreamOffset start = new RabbitMQStreamOffset(startOffsets);

        // Both components are ReadMaxRows — simulates Spark 3.5 composite
        ReadLimit limit = ReadLimit.compositeLimit(new ReadLimit[]{
                ReadLimit.maxRows(100),
                ReadLimit.maxRows(50)
        });

        RabbitMQStreamOffset latest = (RabbitMQStreamOffset) stream.latestOffset(start, limit);
        // maxRows(50) is more restrictive than maxRows(100), so 50 total budget
        assertThat(latest.getStreamOffsets().get("s1") + latest.getStreamOffsets().get("s2"))
                .isLessThanOrEqualTo(50L);
    }

    @Test
    void maxBytesConversionClampsToAtLeastOneRow() {
        Map<String, String> opts = new LinkedHashMap<>();
        opts.put("endpoints", "localhost:5552");
        opts.put("stream", "test-stream");
        opts.put("maxBytesPerTrigger", "1");
        opts.put("estimatedMessageSizeBytes", "1024");
        RabbitMQMicroBatchStream stream = createStream(new ConnectorOptions(opts));

        ReadLimit limit = stream.getDefaultReadLimit();
        assertThat(limit).isInstanceOf(ReadMaxRows.class);
        assertThat(((ReadMaxRows) limit).maxRows()).isEqualTo(1L);
    }

    @Test
    void customEstimatedMessageSizeAffectsConversion() throws Exception {
        Map<String, String> opts = new LinkedHashMap<>();
        opts.put("endpoints", "localhost:5552");
        opts.put("stream", "test-stream");
        opts.put("maxBytesPerTrigger", "10000");
        opts.put("estimatedMessageSizeBytes", "100");
        RabbitMQMicroBatchStream stream = createStream(new ConnectorOptions(opts));

        ReadLimit limit = stream.getDefaultReadLimit();
        assertThat(limit).isInstanceOf(ReadMaxRows.class);
        // 10000 / 100 = 100 rows
        assertThat(((ReadMaxRows) limit).maxRows()).isEqualTo(100L);
    }

    private static ConnectorOptions minimalOptions() {
        Map<String, String> opts = new LinkedHashMap<>();
        opts.put("endpoints", "localhost:5552");
        opts.put("stream", "test-stream");
        return new ConnectorOptions(opts);
    }

    private static RabbitMQMicroBatchStream createStream(ConnectorOptions opts) {
        // Use a minimal schema to avoid triggering RabbitMQStreamTable static
        // initialization which depends on json4s (via Spark 3.5's Metadata class)
        StructType schema = new StructType().add("value", DataTypes.BinaryType);
        return new RabbitMQMicroBatchStream(opts, schema, "/tmp/checkpoint");
    }

    private static void setPrivateField(Object target, String fieldName, Object value)
            throws Exception {
        Field field = target.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(target, value);
    }
}
