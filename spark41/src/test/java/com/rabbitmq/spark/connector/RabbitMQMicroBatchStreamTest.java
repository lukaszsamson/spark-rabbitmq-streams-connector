package com.rabbitmq.spark.connector;

import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.streaming.MicroBatchStream;
import org.apache.spark.sql.connector.read.streaming.Offset;
import org.apache.spark.sql.connector.read.streaming.ReadLimit;
import org.apache.spark.sql.connector.read.streaming.SupportsAdmissionControl;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link RabbitMQMicroBatchStream}.
 *
 * <p>These tests exercise the MicroBatchStream contract and offset handling
 * without requiring a real RabbitMQ broker. Broker-dependent behaviors
 * (initialOffset with stored offsets, latestOffset, commit) are tested
 * in the it-tests module.
 */
class RabbitMQMicroBatchStreamTest {

    // ======================================================================
    // Scan integration
    // ======================================================================

    @Nested
    class ScanIntegration {

        @Test
        void toMicroBatchStreamReturnsMicroBatchStream() {
            ConnectorOptions opts = minimalOptions();
            var schema = RabbitMQStreamTable.buildSourceSchema(opts.getMetadataFields());
            var scan = new RabbitMQScan(opts, schema);

            MicroBatchStream stream = scan.toMicroBatchStream("/tmp/checkpoint");
            assertThat(stream).isInstanceOf(RabbitMQMicroBatchStream.class);
        }

        @Test
        void microBatchStreamImplementsAdmissionControl() {
            ConnectorOptions opts = minimalOptions();
            var schema = RabbitMQStreamTable.buildSourceSchema(opts.getMetadataFields());
            var scan = new RabbitMQScan(opts, schema);

            MicroBatchStream stream = scan.toMicroBatchStream("/tmp/checkpoint");
            assertThat(stream).isInstanceOf(SupportsAdmissionControl.class);
        }
    }

    // ======================================================================
    // Offset deserialization
    // ======================================================================

    @Nested
    class OffsetDeserialization {

        @Test
        void deserializeOffsetParsesJson() {
            RabbitMQMicroBatchStream stream = createStream(minimalOptions());

            Offset offset = stream.deserializeOffset("{\"test-stream\":42}");
            assertThat(offset).isInstanceOf(RabbitMQStreamOffset.class);

            RabbitMQStreamOffset rmqOffset = (RabbitMQStreamOffset) offset;
            assertThat(rmqOffset.getStreamOffsets()).containsEntry("test-stream", 42L);
        }

        @Test
        void deserializeOffsetRoundTrip() {
            RabbitMQMicroBatchStream stream = createStream(minimalOptions());

            RabbitMQStreamOffset original = new RabbitMQStreamOffset(Map.of("s", 100L));
            Offset deserialized = stream.deserializeOffset(original.json());
            assertThat(deserialized).isEqualTo(original);
        }

        @Test
        void deserializeMultiStreamOffset() {
            RabbitMQMicroBatchStream stream = createStream(minimalOptions());

            String json = "{\"stream-a\":100,\"stream-b\":200}";
            RabbitMQStreamOffset offset = (RabbitMQStreamOffset) stream.deserializeOffset(json);

            assertThat(offset.getStreamOffsets()).hasSize(2);
            assertThat(offset.getStreamOffsets()).containsEntry("stream-a", 100L);
            assertThat(offset.getStreamOffsets()).containsEntry("stream-b", 200L);
        }
    }

    // ======================================================================
    // planInputPartitions
    // ======================================================================

    @Nested
    class PlanInputPartitions {

        @Test
        void plansSinglePartition() {
            RabbitMQMicroBatchStream stream = createStream(minimalOptions());

            RabbitMQStreamOffset start = new RabbitMQStreamOffset(Map.of("test-stream", 0L));
            RabbitMQStreamOffset end = new RabbitMQStreamOffset(Map.of("test-stream", 100L));

            InputPartition[] partitions = stream.planInputPartitions(start, end);
            assertThat(partitions).hasSize(1);

            RabbitMQInputPartition p = (RabbitMQInputPartition) partitions[0];
            assertThat(p.getStream()).isEqualTo("test-stream");
            assertThat(p.getStartOffset()).isEqualTo(0L);
            assertThat(p.getEndOffset()).isEqualTo(100L);
        }

        @Test
        void plansMultiplePartitions() {
            RabbitMQMicroBatchStream stream = createStream(minimalOptions());

            Map<String, Long> startMap = new LinkedHashMap<>();
            startMap.put("s1", 10L);
            startMap.put("s2", 50L);
            Map<String, Long> endMap = new LinkedHashMap<>();
            endMap.put("s1", 100L);
            endMap.put("s2", 200L);

            InputPartition[] partitions = stream.planInputPartitions(
                    new RabbitMQStreamOffset(startMap),
                    new RabbitMQStreamOffset(endMap));

            assertThat(partitions).hasSize(2);
        }

        @Test
        void skipsEmptyRanges() {
            RabbitMQMicroBatchStream stream = createStream(minimalOptions());

            Map<String, Long> startMap = new LinkedHashMap<>();
            startMap.put("s1", 100L);
            startMap.put("s2", 50L);
            Map<String, Long> endMap = new LinkedHashMap<>();
            endMap.put("s1", 100L); // same as start → skip
            endMap.put("s2", 200L);

            InputPartition[] partitions = stream.planInputPartitions(
                    new RabbitMQStreamOffset(startMap),
                    new RabbitMQStreamOffset(endMap));

            assertThat(partitions).hasSize(1);
            RabbitMQInputPartition p = (RabbitMQInputPartition) partitions[0];
            assertThat(p.getStream()).isEqualTo("s2");
        }

        @Test
        void emptyEndOffsetProducesNoPartitions() {
            RabbitMQMicroBatchStream stream = createStream(minimalOptions());

            InputPartition[] partitions = stream.planInputPartitions(
                    new RabbitMQStreamOffset(Map.of()),
                    new RabbitMQStreamOffset(Map.of()));

            assertThat(partitions).isEmpty();
        }

        @Test
        void handlesNewStreamInEndNotInStart() {
            RabbitMQMicroBatchStream stream = createStream(minimalOptions());

            // Start has no entry for "new-stream" → defaults to 0
            RabbitMQStreamOffset start = new RabbitMQStreamOffset(Map.of("s1", 10L));
            Map<String, Long> endMap = new LinkedHashMap<>();
            endMap.put("s1", 100L);
            endMap.put("new-stream", 50L);

            InputPartition[] partitions = stream.planInputPartitions(
                    start, new RabbitMQStreamOffset(endMap));

            assertThat(partitions).hasSize(2);
        }
    }

    // ======================================================================
    // createReaderFactory
    // ======================================================================

    @Nested
    class ReaderFactory {

        @Test
        void returnsPartitionReaderFactory() {
            RabbitMQMicroBatchStream stream = createStream(minimalOptions());
            assertThat(stream.createReaderFactory())
                    .isInstanceOf(RabbitMQPartitionReaderFactory.class);
        }
    }

    // ======================================================================
    // Admission control defaults
    // ======================================================================

    @Nested
    class AdmissionControl {

        @Test
        void defaultReadLimitIsAllAvailable() {
            RabbitMQMicroBatchStream stream = createStream(minimalOptions());
            assertThat(stream.getDefaultReadLimit()).isNotNull();
        }

        @Test
        void reportLatestOffsetReturnsNullBeforeQuery() {
            RabbitMQMicroBatchStream stream = createStream(minimalOptions());
            assertThat(stream.reportLatestOffset()).isNull();
        }
    }

    // ======================================================================
    // stop()
    // ======================================================================

    @Nested
    class StopLifecycle {

        @Test
        void stopIsIdempotent() {
            RabbitMQMicroBatchStream stream = createStream(minimalOptions());
            // Should not throw when called multiple times without initialization
            stream.stop();
            stream.stop();
        }
    }

    // ---- Helpers ----

    private static ConnectorOptions minimalOptions() {
        Map<String, String> opts = new LinkedHashMap<>();
        opts.put("endpoints", "localhost:5552");
        opts.put("stream", "test-stream");
        return new ConnectorOptions(opts);
    }

    private static RabbitMQMicroBatchStream createStream(ConnectorOptions opts) {
        var schema = RabbitMQStreamTable.buildSourceSchema(opts.getMetadataFields());
        return new RabbitMQMicroBatchStream(opts, schema, "/tmp/checkpoint");
    }
}
