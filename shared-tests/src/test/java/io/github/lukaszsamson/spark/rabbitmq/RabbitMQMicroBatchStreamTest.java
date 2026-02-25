package io.github.lukaszsamson.spark.rabbitmq;

import org.apache.spark.sql.connector.metric.CustomMetric;
import org.apache.spark.sql.connector.metric.CustomSumMetric;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.streaming.*;
import org.apache.spark.util.LongAccumulator;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import com.rabbitmq.stream.ConsumerBuilder;
import com.rabbitmq.stream.Environment;
import com.rabbitmq.stream.NoOffsetException;
import com.rabbitmq.stream.ProducerBuilder;
import com.rabbitmq.stream.StreamCreator;
import com.rabbitmq.stream.StreamStats;
import com.rabbitmq.stream.codec.QpidProtonCodec;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ThreadPoolExecutor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link RabbitMQMicroBatchStream}.
 *
 * <p>These tests exercise the MicroBatchStream contract, offset handling,
 * admission control, trigger support, and metrics without requiring a real
 * RabbitMQ broker. Broker-dependent behaviors (initialOffset with stored
 * offsets, latestOffset, commit) are tested in the it-tests module.
 */
class RabbitMQMicroBatchStreamTest {

    private static final QpidProtonCodec CODEC = new QpidProtonCodec();

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
        void microBatchStreamSupportsAdmissionControlAndLimitsRespectRequested() throws Exception {
            ConnectorOptions opts = minimalOptions();
            var schema = RabbitMQStreamTable.buildSourceSchema(opts.getMetadataFields());
            var scan = new RabbitMQScan(opts, schema);

            MicroBatchStream stream = scan.toMicroBatchStream("/tmp/checkpoint");
            assertThat(stream).isInstanceOf(SupportsAdmissionControl.class);

            setPrivateField(stream, "availableNowSnapshot", Map.of("s1", 10L));
            setPrivateField(stream, "cachedTailOffset", new RabbitMQStreamOffset(Map.of("s1", 10L)));
            var last = new RabbitMQStreamOffset(Map.of("s1", 0L));
            Offset limited = ((SupportsAdmissionControl) stream)
                    .latestOffset(last, ReadLimit.maxRows(3));

            assertThat(((RabbitMQStreamOffset) limited).getStreamOffsets())
                    .containsEntry("s1", 3L);
        }

        @Test
        void microBatchStreamSupportsTriggerAvailableNowAndSnapshotsTail() throws Exception {
            ConnectorOptions opts = minimalOptions();
            var schema = RabbitMQStreamTable.buildSourceSchema(opts.getMetadataFields());
            var scan = new RabbitMQScan(opts, schema);

            MicroBatchStream stream = scan.toMicroBatchStream("/tmp/checkpoint");
            assertThat(stream).isInstanceOf(SupportsTriggerAvailableNow.class);

            setPrivateField(stream, "cachedTailOffset", new RabbitMQStreamOffset(Map.of("s1", 5L)));
            setPrivateField(stream, "environment", new CountingEnvironment());
            ((SupportsTriggerAvailableNow) stream).prepareForTriggerAvailableNow();

            setPrivateField(stream, "cachedTailOffset", new RabbitMQStreamOffset(Map.of("s1", 9L)));
            setPrivateField(stream, "availableNowSnapshot", Map.of("s1", 5L));
            Offset bounded = ((SupportsAdmissionControl) stream)
                    .latestOffset(new RabbitMQStreamOffset(Map.of("s1", 0L)),
                            ReadLimit.allAvailable());

            assertThat(((RabbitMQStreamOffset) bounded).getStreamOffsets())
                    .containsEntry("s1", 5L);
        }

        @Test
        void microBatchStreamSupportsReportsSourceMetricsAndNamesPresent() {
            ConnectorOptions opts = minimalOptions();
            var schema = RabbitMQStreamTable.buildSourceSchema(opts.getMetadataFields());
            var scan = new RabbitMQScan(opts, schema);

            MicroBatchStream stream = scan.toMicroBatchStream("/tmp/checkpoint");
            assertThat(stream).isInstanceOf(ReportsSourceMetrics.class);

            CustomMetric[] metrics = scan.supportedCustomMetrics();
            assertThat(metrics).hasSize(6);
            Set<String> names = new HashSet<>();
            for (CustomMetric m : metrics) {
                names.add(m.name());
                assertThat(m).isInstanceOf(CustomSumMetric.class);
            }
            assertThat(names).containsExactlyInAnyOrder(
                    "recordsRead", "payloadBytesRead",
                    "estimatedWireBytesRead", "pollWaitMs",
                    "offsetOutOfRange", "dataLoss");
        }

        @Test
        void scanSupportedCustomMetricsIncludesRecordsAndBytes() {
            ConnectorOptions opts = minimalOptions();
            var schema = RabbitMQStreamTable.buildSourceSchema(opts.getMetadataFields());
            var scan = new RabbitMQScan(opts, schema);

            CustomMetric[] metrics = scan.supportedCustomMetrics();
            assertThat(metrics).hasSize(6);

            Set<String> names = new HashSet<>();
            for (CustomMetric m : metrics) {
                names.add(m.name());
                assertThat(m).isInstanceOf(CustomSumMetric.class);
            }
            assertThat(names).containsExactlyInAnyOrder(
                    "recordsRead", "payloadBytesRead",
                    "estimatedWireBytesRead", "pollWaitMs",
                    "offsetOutOfRange", "dataLoss");
        }
    }

    @Nested
    class RealTimeModeValidation {

        @Test
        void prepareForRealTimeModeRejectsMinPartitions() throws Exception {
            Map<String, String> opts = new LinkedHashMap<>();
            opts.put("endpoints", "localhost:5552");
            opts.put("stream", "test-stream");
            opts.put("minPartitions", "2");
            RabbitMQMicroBatchStream stream = createStream(new ConnectorOptions(opts));

            Assumptions.assumeTrue(hasPrepareForRealTimeMode(stream));
            assertThatThrownBy(() -> invokePrepareForRealTimeMode(stream))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("minPartitions");
        }

        @Test
        void prepareForRealTimeModeRejectsReadLimits() throws Exception {
            Map<String, String> opts = new LinkedHashMap<>();
            opts.put("endpoints", "localhost:5552");
            opts.put("stream", "test-stream");
            opts.put("maxRecordsPerTrigger", "100");
            opts.put("maxBytesPerTrigger", "4096");
            RabbitMQMicroBatchStream stream = createStream(new ConnectorOptions(opts));

            Assumptions.assumeTrue(hasPrepareForRealTimeMode(stream));
            assertThatThrownBy(() -> invokePrepareForRealTimeMode(stream))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("real-time mode");
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
    // initialOffset
    // ======================================================================

    @Nested
    class InitialOffset {

        @Test
        void initialOffsetSkipsLookupWhenServerSideTrackingDisabled() {
            Map<String, String> opts = new LinkedHashMap<>();
            opts.put("endpoints", "localhost:5552");
            opts.put("stream", "test-stream");
            opts.put("startingOffsets", "offset");
            opts.put("startingOffset", "42");
            opts.put("serverSideOffsetTracking", "false");

            RabbitMQMicroBatchStream stream = createStream(new ConnectorOptions(opts));
            stream.environment = new FirstOffsetEnvironment(0L);

            RabbitMQStreamOffset offset = (RabbitMQStreamOffset) stream.initialOffset();
            assertThat(offset.getStreamOffsets()).containsEntry("test-stream", 42L);
        }

        @Test
        void initialOffsetEarliestStatsFailureFailsFast() throws Exception {
            Map<String, String> opts = new LinkedHashMap<>();
            opts.put("endpoints", "localhost:5552");
            opts.put("stream", "test-stream");
            opts.put("startingOffsets", "earliest");
            opts.put("serverSideOffsetTracking", "false");

            RabbitMQMicroBatchStream stream = createStream(new ConnectorOptions(opts));
            Environment env = mock(Environment.class);
            when(env.queryStreamStats("test-stream"))
                    .thenThrow(new RuntimeException("transient stats failure"));
            setPrivateField(stream, "environment", env);

            assertThatThrownBy(stream::initialOffset)
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("Failed to validate configured stream 'test-stream'");
        }

        @Test
        void initialOffsetEarliestNoOffsetStillUsesZero() throws Exception {
            Map<String, String> opts = new LinkedHashMap<>();
            opts.put("endpoints", "localhost:5552");
            opts.put("stream", "test-stream");
            opts.put("startingOffsets", "earliest");
            opts.put("serverSideOffsetTracking", "false");

            RabbitMQMicroBatchStream stream = createStream(new ConnectorOptions(opts));
            Environment env = mock(Environment.class);
            when(env.queryStreamStats("test-stream")).thenThrow(new NoOffsetException("empty stream"));
            setPrivateField(stream, "environment", env);

            RabbitMQStreamOffset offset = (RabbitMQStreamOffset) stream.initialOffset();
            assertThat(offset.getStreamOffsets()).containsEntry("test-stream", 0L);
        }

        @Test
        void initialOffsetMissingConfiguredStreamFailsFastEvenWhenFailOnDataLossFalse() throws Exception {
            Map<String, String> opts = new LinkedHashMap<>();
            opts.put("endpoints", "localhost:5552");
            opts.put("stream", "missing-stream");
            opts.put("failOnDataLoss", "false");
            opts.put("serverSideOffsetTracking", "false");

            RabbitMQMicroBatchStream stream = createStream(new ConnectorOptions(opts));
            setPrivateField(stream, "environment", new MissingStreamEnvironment());

            assertThatThrownBy(stream::initialOffset)
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("does not exist");
        }

        @Test
        void initialOffsetTimestampProbeFailureFailsFastInsteadOfFallingBackToEarliest()
                throws Exception {
            Map<String, String> opts = new LinkedHashMap<>();
            opts.put("endpoints", "localhost:5552");
            opts.put("stream", "test-stream");
            opts.put("startingOffsets", "timestamp");
            opts.put("startingTimestamp", "1700000000000");
            opts.put("serverSideOffsetTracking", "false");

            RabbitMQMicroBatchStream stream = createStream(new ConnectorOptions(opts));
            setPrivateField(stream, "environment",
                    new ThrowingConsumerBuilderEnvironment(new RuntimeException("probe failed")));

            assertThatThrownBy(stream::initialOffset)
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("Failed to resolve timestamp start offset");
        }

        @Test
        void initialOffsetExplicitConsumerNameNonFatalLookupFailureFailsFast() throws Exception {
            Map<String, String> opts = new LinkedHashMap<>();
            opts.put("endpoints", "localhost:5552");
            opts.put("stream", "test-stream");
            opts.put("consumerName", "explicit");

            RabbitMQMicroBatchStream stream = createStream(new ConnectorOptions(opts));
            Environment env = new ThrowingConsumerBuilderEnvironment(
                    new RuntimeException("tracking consumer limit reached"));
            setPrivateField(stream, "environment", env);

            assertThatThrownBy(stream::initialOffset)
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("consumerName is explicitly configured");
        }

        @Test
        void initialOffsetDerivedConsumerNameNonFatalLookupFallsBackToStartingOffsets() throws Exception {
            Map<String, String> opts = new LinkedHashMap<>();
            opts.put("endpoints", "localhost:5552");
            opts.put("stream", "test-stream");
            opts.put("startingOffsets", "offset");
            opts.put("startingOffset", "7");

            RabbitMQMicroBatchStream stream = createStream(new ConnectorOptions(opts));
            Environment env = new ThrowingConsumerBuilderEnvironment(
                    new RuntimeException("tracking consumer limit reached"));
            setPrivateField(stream, "environment", env);

            RabbitMQStreamOffset offset = (RabbitMQStreamOffset) stream.initialOffset();
            assertThat(offset.getStreamOffsets()).containsEntry("test-stream", 7L);
        }

        @Test
        void initialOffsetFatalLookupErrorFailsWithExplicitConsumerName() throws Exception {
            Map<String, String> opts = new LinkedHashMap<>();
            opts.put("endpoints", "localhost:5552");
            opts.put("stream", "test-stream");
            opts.put("consumerName", "explicit");

            RabbitMQMicroBatchStream stream = createStream(new ConnectorOptions(opts));
            Environment env = new ThrowingConsumerBuilderEnvironment(
                    new RuntimeException("authentication failed"));
            setPrivateField(stream, "environment", env);

            assertThatThrownBy(stream::initialOffset)
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("Failed to look up stored offset");
        }

        @Test
        void initialOffsetFatalLookupErrorFailsWithDerivedConsumerName() throws Exception {
            Map<String, String> opts = new LinkedHashMap<>();
            opts.put("endpoints", "localhost:5552");
            opts.put("stream", "test-stream");

            RabbitMQMicroBatchStream stream = createStream(new ConnectorOptions(opts));
            Environment env = new ThrowingConsumerBuilderEnvironment(
                    new RuntimeException("authentication failed"));
            setPrivateField(stream, "environment", env);

            assertThatThrownBy(stream::initialOffset)
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("Failed to look up stored offset");
        }

        @Test
        void initialOffsetDerivedConsumerNameUnknownLookupFailureFailsFast() throws Exception {
            Map<String, String> opts = new LinkedHashMap<>();
            opts.put("endpoints", "localhost:5552");
            opts.put("stream", "test-stream");
            opts.put("startingOffsets", "offset");
            opts.put("startingOffset", "7");

            RabbitMQMicroBatchStream stream = createStream(new ConnectorOptions(opts));
            Environment env = new ThrowingConsumerBuilderEnvironment(
                    new RuntimeException("dns resolution failed"));
            setPrivateField(stream, "environment", env);

            assertThatThrownBy(stream::initialOffset)
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("Failed to look up stored offset");
        }

        @Test
        void initialOffsetMergesPartialStoredOffsetsWithFallback() throws Exception {
            Map<String, String> opts = new LinkedHashMap<>();
            opts.put("endpoints", "localhost:5552");
            opts.put("stream", "test-stream");
            opts.put("startingOffsets", "offset");
            opts.put("startingOffset", "5");

            RabbitMQMicroBatchStream stream = createStream(new ConnectorOptions(opts));
            setPrivateField(stream, "streams", List.of("s1", "s2"));
            setPrivateField(stream, "environment", new StoredOffsetEnvironment(
                    Map.of("s1", 9L)));

            RabbitMQStreamOffset offset = (RabbitMQStreamOffset) stream.initialOffset();
            assertThat(offset.getStreamOffsets()).containsEntry("s1", 10L);
            assertThat(offset.getStreamOffsets()).containsEntry("s2", 5L);
        }

        @Test
        void initialOffsetLatestIgnoresRecoveredStoredOffsetBeforeFirstAvailable() throws Exception {
            Map<String, String> opts = new LinkedHashMap<>();
            opts.put("endpoints", "localhost:5552");
            opts.put("stream", "test-stream");
            opts.put("startingOffsets", "latest");
            opts.put("consumerName", "fresh-consumer");

            RabbitMQMicroBatchStream stream = createStream(new ConnectorOptions(opts));
            setPrivateField(stream, "environment", new StoredOffsetWithStatsEnvironment(
                    Map.of("test-stream", 0L),
                    Map.of("test-stream", 100L)));

            RabbitMQStreamOffset offset = (RabbitMQStreamOffset) stream.initialOffset();
            assertThat(offset.getStreamOffsets()).containsEntry("test-stream", 101L);
        }

        @Test
        void initialOffsetLatestKeepsRecoveredStoredOffsetAtOrAfterFirstAvailable() throws Exception {
            Map<String, String> opts = new LinkedHashMap<>();
            opts.put("endpoints", "localhost:5552");
            opts.put("stream", "test-stream");
            opts.put("startingOffsets", "latest");
            opts.put("consumerName", "existing-consumer");

            RabbitMQMicroBatchStream stream = createStream(new ConnectorOptions(opts));
            setPrivateField(stream, "environment", new StoredOffsetWithStatsEnvironment(
                    Map.of("test-stream", 120L),
                    Map.of("test-stream", 100L)));

            RabbitMQStreamOffset offset = (RabbitMQStreamOffset) stream.initialOffset();
            assertThat(offset.getStreamOffsets()).containsEntry("test-stream", 121L);
        }

        @Test
        void discoverStreamsSuperStreamEmptyPartitionsThrows() throws Exception {
            Map<String, String> opts = new LinkedHashMap<>();
            opts.put("endpoints", "localhost:5552");
            opts.put("superstream", "super");

            RabbitMQMicroBatchStream stream = createStream(new ConnectorOptions(opts));
            setPrivateField(stream, "streams", List.of());
            setPrivateField(stream, "environment", new CountingEnvironment());

            assertThatThrownBy(stream::initialOffset)
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("has no partition streams");
        }

        @Test
        void discoverStreamsDropsRemovedPartitionsWhenFailOnDataLossFalse() {
            Map<String, String> opts = new LinkedHashMap<>();
            opts.put("endpoints", "localhost:5552");
            opts.put("superstream", "super");
            opts.put("failOnDataLoss", "false");
            ConnectorOptions options = new ConnectorOptions(opts);
            var schema = RabbitMQStreamTable.buildSourceSchema(options.getMetadataFields());

            BaseRabbitMQMicroBatchStream stream = new SequencedDiscoveryMicroBatchStream(
                    options,
                    schema,
                    "/tmp/checkpoint",
                    java.util.List.of(
                            java.util.List.of("s1", "s2"),
                            java.util.List.of("s1")));

            assertThat(stream.discoverStreams()).containsExactly("s1", "s2");
            assertThat(stream.discoverStreams()).containsExactly("s1");
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
            Map<String, long[]> seen = new LinkedHashMap<>();
            for (InputPartition partition : partitions) {
                RabbitMQInputPartition rp = (RabbitMQInputPartition) partition;
                seen.put(rp.getStream(), new long[]{rp.getStartOffset(), rp.getEndOffset()});
            }
            assertThat(seen).containsEntry("s1", new long[]{10L, 100L});
            assertThat(seen).containsEntry("s2", new long[]{50L, 200L});
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
        void backfillsNewStreamWhenMissingFromStartOffsetUsingConfiguredStartingOffsetsInSuperStreamMode() throws Exception {
            Map<String, String> opts = new LinkedHashMap<>();
            opts.put("endpoints", "localhost:5552");
            opts.put("superstream", "super");
            RabbitMQMicroBatchStream stream = createStream(new ConnectorOptions(opts));
            setPrivateField(stream, "environment", new FirstOffsetEnvironment(0L));

            RabbitMQStreamOffset start = new RabbitMQStreamOffset(Map.of("s1", 10L));
            Map<String, Long> endMap = new LinkedHashMap<>();
            endMap.put("s1", 100L);
            endMap.put("new-stream", 50L);

            InputPartition[] partitions = stream.planInputPartitions(
                    start, new RabbitMQStreamOffset(endMap));

            assertThat(partitions).hasSize(2);
            Map<String, long[]> seen = new LinkedHashMap<>();
            for (InputPartition partition : partitions) {
                RabbitMQInputPartition rp = (RabbitMQInputPartition) partition;
                seen.put(rp.getStream(), new long[]{rp.getStartOffset(), rp.getEndOffset()});
            }
            assertThat(seen).containsEntry("s1", new long[]{10L, 100L});
            assertThat(seen).containsEntry("new-stream", new long[]{0L, 50L});
        }

        @Test
        void marksConfiguredStartingOffsetOnlyForInitialTimestampBatch() throws Exception {
            Map<String, String> opts = new LinkedHashMap<>();
            opts.put("endpoints", "localhost:5552");
            opts.put("stream", "test-stream");
            opts.put("startingOffsets", "timestamp");
            opts.put("startingTimestamp", "1700000000000");
            RabbitMQMicroBatchStream stream = createStream(new ConnectorOptions(opts));

            setPrivateField(stream, "initialOffsets", Map.of("test-stream", 10L));

            InputPartition[] first = stream.planInputPartitions(
                    new RabbitMQStreamOffset(Map.of("test-stream", 10L)),
                    new RabbitMQStreamOffset(Map.of("test-stream", 20L)));
            assertThat(first).hasSize(1);
            assertThat(((RabbitMQInputPartition) first[0]).isUseConfiguredStartingOffset()).isTrue();

            InputPartition[] next = stream.planInputPartitions(
                    new RabbitMQStreamOffset(Map.of("test-stream", 11L)),
                    new RabbitMQStreamOffset(Map.of("test-stream", 20L)));
            assertThat(next).hasSize(1);
            assertThat(((RabbitMQInputPartition) next[0]).isUseConfiguredStartingOffset()).isFalse();
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
        void defaultReadLimitIsAllAvailableWhenNoLimitsSet() {
            RabbitMQMicroBatchStream stream = createStream(minimalOptions());
            ReadLimit limit = stream.getDefaultReadLimit();
            assertThat(limit).isInstanceOf(ReadAllAvailable.class);
        }

        @Test
        void defaultReadLimitIsMaxRowsWhenOnlyMaxRecordsSet() {
            Map<String, String> opts = new LinkedHashMap<>();
            opts.put("endpoints", "localhost:5552");
            opts.put("stream", "test-stream");
            opts.put("maxRecordsPerTrigger", "1000");
            RabbitMQMicroBatchStream stream = createStream(new ConnectorOptions(opts));

            ReadLimit limit = stream.getDefaultReadLimit();
            assertThat(limit).isInstanceOf(ReadMaxRows.class);
            assertThat(((ReadMaxRows) limit).maxRows()).isEqualTo(1000L);
        }

        @Test
        @Tag("spark4x")
        void defaultReadLimitIsMaxBytesWhenOnlyMaxBytesSet() {
            Map<String, String> opts = new LinkedHashMap<>();
            opts.put("endpoints", "localhost:5552");
            opts.put("stream", "test-stream");
            opts.put("maxBytesPerTrigger", "1048576");
            RabbitMQMicroBatchStream stream = createStream(new ConnectorOptions(opts));

            ReadLimit limit = stream.getDefaultReadLimit();
            assertThat(limit).isInstanceOf(ReadMaxBytes.class);
            assertThat(((ReadMaxBytes) limit).maxBytes()).isEqualTo(1048576L);
        }

        @Test
        @Tag("spark4x")
        void defaultReadLimitIsCompositeWhenBothLimitsSet() {
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

            // Verify component types
            boolean hasMaxRows = false;
            boolean hasMaxBytes = false;
            for (ReadLimit component : components) {
                if (component instanceof ReadMaxRows) hasMaxRows = true;
                if (component instanceof ReadMaxBytes) hasMaxBytes = true;
            }
            assertThat(hasMaxRows).isTrue();
            assertThat(hasMaxBytes).isTrue();
        }

        @Test
        void reportLatestOffsetReturnsNullBeforeQuery() {
            RabbitMQMicroBatchStream stream = createStream(minimalOptions());
            assertThat(stream.reportLatestOffset()).isNull();
        }

        @Test
        void reportLatestOffsetPrefersTailOverLimitedEndOffset() throws Exception {
            RabbitMQMicroBatchStream stream = createStream(minimalOptions());
            RabbitMQStreamOffset limited = new RabbitMQStreamOffset(Map.of("test-stream", 10L));
            RabbitMQStreamOffset tail = new RabbitMQStreamOffset(Map.of("test-stream", 100L));
            setPrivateField(stream, "cachedLatestOffset", limited);
            setPrivateField(stream, "cachedTailOffset", tail);

            assertThat(stream.reportLatestOffset()).isEqualTo(tail);
        }

        @Test
        void reportLatestOffsetUsesLimitedWhenTailMissing() throws Exception {
            RabbitMQMicroBatchStream stream = createStream(minimalOptions());
            RabbitMQStreamOffset limited = new RabbitMQStreamOffset(Map.of("test-stream", 10L));
            setPrivateField(stream, "cachedLatestOffset", limited);
            setPrivateField(stream, "cachedTailOffset", null);

            assertThat(stream.reportLatestOffset()).isEqualTo(limited);
        }
    }

    // ======================================================================
    // latestOffset
    // ======================================================================

    @Nested
    class LatestOffset {

        @Test
        void latestOffsetReturnsStartWhenNoNewData() throws Exception {
            RabbitMQMicroBatchStream stream = createStream(minimalOptions());
            Map<String, Long> snapshot = Map.of("test-stream", 10L);
            setPrivateField(stream, "availableNowSnapshot", snapshot);

            RabbitMQStreamOffset start = new RabbitMQStreamOffset(snapshot);
            Offset latest = stream.latestOffset(start, ReadLimit.allAvailable());

            assertThat(latest).isEqualTo(start);
        }

        @Test
        void latestOffsetNullStartStillAppliesReadLimit() throws Exception {
            Map<String, String> opts = new LinkedHashMap<>();
            opts.put("endpoints", "localhost:5552");
            opts.put("superstream", "super");
            opts.put("startingOffsets", "offset");
            opts.put("startingOffset", "0");

            RabbitMQMicroBatchStream stream = createStream(new ConnectorOptions(opts));
            Map<String, Long> snapshot = new LinkedHashMap<>();
            snapshot.put("s1", 100L);
            snapshot.put("s2", 80L);
            setPrivateField(stream, "availableNowSnapshot", snapshot);

            RabbitMQStreamOffset latest = (RabbitMQStreamOffset) stream.latestOffset(
                    null, ReadLimit.maxRows(20));
            assertThat(latest.getStreamOffsets())
                    .containsEntry("s1", 10L)
                    .containsEntry("s2", 10L);
        }

        @Test
        void latestOffsetClampsTailBelowStartOffsets() throws Exception {
            RabbitMQMicroBatchStream stream = createStream(minimalOptions());
            Map<String, Long> snapshot = Map.of("s1", 5L, "s2", 100L);
            setPrivateField(stream, "availableNowSnapshot", snapshot);

            RabbitMQStreamOffset start = new RabbitMQStreamOffset(Map.of("s1", 10L, "s2", 50L));
            Offset latest = stream.latestOffset(start, ReadLimit.allAvailable());

            RabbitMQStreamOffset latestOffset = (RabbitMQStreamOffset) latest;
            assertThat(latestOffset.getStreamOffsets()).containsEntry("s1", 10L);
            assertThat(latestOffset.getStreamOffsets()).containsEntry("s2", 100L);
        }

        @Test
        void latestOffsetUnknownReadLimitDefaultsToAllAvailable() throws Exception {
            RabbitMQMicroBatchStream stream = createStream(minimalOptions());
            Map<String, Long> snapshot = Map.of("s1", 20L);
            setPrivateField(stream, "availableNowSnapshot", snapshot);

            RabbitMQStreamOffset start = new RabbitMQStreamOffset(Map.of("s1", 10L));
            ReadLimit unknownLimit = ReadLimit.minRows(5, 1000);

            RabbitMQStreamOffset latest = (RabbitMQStreamOffset) stream.latestOffset(start, unknownLimit);
            assertThat(latest.getStreamOffsets()).containsEntry("s1", 20L);
        }

        @Test
        @Tag("spark4x")
        void firstReadMinRowsEvaluationStartsDelayWindowAtInvocationTime() throws Exception {
            RabbitMQMicroBatchStream stream = createStream(minimalOptions());
            setPrivateField(stream, "availableNowSnapshot", Map.of("s1", 10L));

            RabbitMQStreamOffset start = new RabbitMQStreamOffset(Map.of("s1", 9L));
            Thread.sleep(120L);

            RabbitMQStreamOffset latest = (RabbitMQStreamOffset) stream.latestOffset(
                    start, ReadLimit.minRows(5, 50L));
            assertThat(latest).isEqualTo(start);
        }

        @Test
        void latestOffsetUsesConfiguredStartForStreamsMissingFromCheckpoint() throws Exception {
            Map<String, String> opts = new LinkedHashMap<>();
            opts.put("endpoints", "localhost:5552");
            opts.put("superstream", "super");
            opts.put("startingOffsets", "offset");
            opts.put("startingOffset", "5");

            RabbitMQMicroBatchStream stream = createStream(new ConnectorOptions(opts));
            setPrivateField(stream, "availableNowSnapshot", Map.of("s1", 10L, "s2", 4L));

            RabbitMQStreamOffset start = new RabbitMQStreamOffset(Map.of("s1", 10L));
            Offset latest = stream.latestOffset(start, ReadLimit.allAvailable());

            assertThat(latest).isNotSameAs(start);
            assertThat(((RabbitMQStreamOffset) latest).getStreamOffsets())
                    .containsEntry("s1", 10L)
                    .containsEntry("s2", 5L);
        }

        @Test
        void latestOffsetWithCheckpointMissingStreamsStaysStableInSingleStreamMode() throws Exception {
            Map<String, String> opts = new LinkedHashMap<>();
            opts.put("endpoints", "localhost:5552");
            opts.put("stream", "test-stream");
            opts.put("startingOffsets", "offset");
            opts.put("startingOffset", "100");
            opts.put("maxRecordsPerTrigger", "500");

            RabbitMQMicroBatchStream stream = createStream(new ConnectorOptions(opts));
            setPrivateField(stream, "availableNowSnapshot", Map.of("test-stream", 1000L));

            RabbitMQStreamOffset resumedStart = new RabbitMQStreamOffset(Map.of());
            RabbitMQStreamOffset latest = (RabbitMQStreamOffset) stream.latestOffset(
                    resumedStart, ReadLimit.maxRows(500));

            assertThat(latest.getStreamOffsets()).containsEntry("test-stream", 600L);
        }

        @Test
        @Tag("spark4x")
        void compositeReadLimitAppliesMostRestrictivePerStream() throws Exception {
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

            ReadLimit limit = ReadLimit.compositeLimit(new ReadLimit[]{
                    ReadLimit.maxRows(100),
                    ReadLimit.maxBytes(50_000)
            });

            RabbitMQStreamOffset latest = (RabbitMQStreamOffset) stream.latestOffset(start, limit);
            assertThat(latest.getStreamOffsets()).containsEntry("s1", 50L);
            assertThat(latest.getStreamOffsets()).containsEntry("s2", 50L);
        }

        @Test
        void resolveStartingOffsetTimestampUsesTimestampProbeOffset() throws Exception {
            Map<String, String> opts = new LinkedHashMap<>();
            opts.put("endpoints", "localhost:5552");
            opts.put("stream", "test-stream");
            opts.put("startingOffsets", "timestamp");
            opts.put("startingTimestamp", "1700000000000");

            RabbitMQMicroBatchStream stream = createStream(new ConnectorOptions(opts));
            setPrivateField(stream, "environment",
                    new TimestampStartEnvironment(10L, java.util.List.of(42L)));

            RabbitMQStreamOffset offset = (RabbitMQStreamOffset) stream.initialOffset();
            assertThat(offset.getStreamOffsets()).containsEntry("test-stream", 42L);
        }

        @Test
        void resolveStartingOffsetTimestampWaitsForProbeWithinConfiguredPollTimeout() throws Exception {
            Map<String, String> opts = new LinkedHashMap<>();
            opts.put("endpoints", "localhost:5552");
            opts.put("stream", "test-stream");
            opts.put("startingOffsets", "timestamp");
            opts.put("startingTimestamp", "1700000000000");
            opts.put("pollTimeoutMs", "1000");

            RabbitMQMicroBatchStream stream = createStream(new ConnectorOptions(opts));
            setPrivateField(stream, "environment",
                    new TimestampStartEnvironment(10L, java.util.List.of(42L), 400L));

            RabbitMQStreamOffset offset = (RabbitMQStreamOffset) stream.initialOffset();
            assertThat(offset.getStreamOffsets()).containsEntry("test-stream", 42L);
        }

        @Test
        void latestOffsetUsesStatsOnlyAcrossCalls() throws Exception {
            RabbitMQMicroBatchStream stream = createStream(minimalOptions());
            ProbeCountingEnvironment env = new ProbeCountingEnvironment(10L, java.util.List.of(14L));
            setPrivateField(stream, "environment", env);

            RabbitMQStreamOffset start = new RabbitMQStreamOffset(Map.of("test-stream", 0L));
            RabbitMQStreamOffset first =
                    (RabbitMQStreamOffset) stream.latestOffset(start, ReadLimit.allAvailable());
            RabbitMQStreamOffset second =
                    (RabbitMQStreamOffset) stream.latestOffset(start, ReadLimit.allAvailable());

            assertThat(first.getStreamOffsets()).containsEntry("test-stream", 11L);
            assertThat(second.getStreamOffsets()).containsEntry("test-stream", 11L);
            assertThat(env.queryStatsCalls).isEqualTo(2);
            assertThat(env.probeBuilderCalls).isEqualTo(0);
        }

        @Test
        void planInputPartitionsClearsLatestOffsetInvocationCache() throws Exception {
            RabbitMQMicroBatchStream stream = createStream(minimalOptions());
            ProbeCountingEnvironment env = new ProbeCountingEnvironment(10L, java.util.List.of(14L));
            setPrivateField(stream, "environment", env);

            RabbitMQStreamOffset start = new RabbitMQStreamOffset(Map.of("test-stream", 10L));
            RabbitMQStreamOffset end =
                    (RabbitMQStreamOffset) stream.latestOffset(start, ReadLimit.allAvailable());

            stream.planInputPartitions(start, end);
            int queryStatsBeforeSecondLatest = env.queryStatsCalls;
            stream.latestOffset(start, ReadLimit.allAvailable());

            assertThat(env.queryStatsCalls).isEqualTo(queryStatsBeforeSecondLatest + 1);
        }

        @Test
        void latestOffsetQueriesMultipleStreamsConcurrently() throws Exception {
            Assumptions.assumeTrue(Runtime.getRuntime().availableProcessors() > 1);

            Map<String, String> optsMap = new LinkedHashMap<>();
            optsMap.put("endpoints", "localhost:5552");
            optsMap.put("superstream", "super");
            ConnectorOptions opts = new ConnectorOptions(optsMap);
            var schema = RabbitMQStreamTable.buildSourceSchema(opts.getMetadataFields());
            BaseRabbitMQMicroBatchStream stream = new ForcedStreamsMicroBatchStream(
                    opts, schema, "/tmp/checkpoint", java.util.List.of("s1", "s2", "s3"));
            DelayedStatsProbeEnvironment env =
                    new DelayedStatsProbeEnvironment(120L, java.util.List.of(9L));
            setPrivateField(stream, "environment", env);

            try {
                RabbitMQStreamOffset latest = (RabbitMQStreamOffset) stream.latestOffset(
                        new RabbitMQStreamOffset(Map.of("s1", 0L, "s2", 0L, "s3", 0L)),
                        ReadLimit.allAvailable());

                assertThat(latest.getStreamOffsets())
                        .containsEntry("s1", 7L)
                        .containsEntry("s2", 7L)
                        .containsEntry("s3", 7L);
                assertThat(env.maxConcurrentStatsQueries()).isGreaterThan(1);
            } finally {
                stream.stop();
            }
        }
    }

    @Nested
    class TriggerAvailableNow {

        @Test
        void prepareForTriggerAvailableNowSnapshotsAndFreezesTail() throws Exception {
            Map<String, String> opts = new LinkedHashMap<>();
            opts.put("endpoints", "localhost:5552");
            opts.put("stream", "test-stream");
            opts.put("failOnDataLoss", "false");

            RabbitMQMicroBatchStream stream = createStream(new ConnectorOptions(opts));
            setPrivateField(stream, "environment", new CountingEnvironment());

            stream.prepareForTriggerAvailableNow();
            setPrivateField(stream, "availableNowSnapshot", Map.of("test-stream", 5L));

            RabbitMQStreamOffset start = new RabbitMQStreamOffset(Map.of("test-stream", 0L));
            RabbitMQStreamOffset latest =
                    (RabbitMQStreamOffset) stream.latestOffset(start, ReadLimit.allAvailable());
            assertThat(latest.getStreamOffsets()).containsEntry("test-stream", 5L);

            RabbitMQStreamOffset latestAgain =
                    (RabbitMQStreamOffset) stream.latestOffset(start, ReadLimit.allAvailable());
            assertThat(latestAgain.getStreamOffsets()).containsEntry("test-stream", 5L);
        }

        @Test
        void latestOffsetRespectsAvailableNowSnapshotUnderLargerTails() throws Exception {
            Map<String, String> opts = new LinkedHashMap<>();
            opts.put("endpoints", "localhost:5552");
            opts.put("stream", "test-stream");
            opts.put("failOnDataLoss", "false");

            RabbitMQMicroBatchStream stream = createStream(new ConnectorOptions(opts));
            setPrivateField(stream, "environment", new CountingEnvironment());

            stream.prepareForTriggerAvailableNow();
            setPrivateField(stream, "availableNowSnapshot", Map.of("test-stream", 5L));

            RabbitMQStreamOffset start = new RabbitMQStreamOffset(Map.of("test-stream", 0L));
            RabbitMQStreamOffset latest =
                    (RabbitMQStreamOffset) stream.latestOffset(start, ReadLimit.allAvailable());
            assertThat(latest.getStreamOffsets()).containsEntry("test-stream", 5L);
        }

        @Test
        void queryTailOffsetsForAvailableNowUsesStatsTail() throws Exception {
            RabbitMQMicroBatchStream stream = createStream(minimalOptions());
            setPrivateField(stream, "environment", new CountingEnvironment());
            setPrivateField(stream, "availableNowSnapshot", null);

            stream.prepareForTriggerAvailableNow();

            RabbitMQStreamOffset latest = (RabbitMQStreamOffset) stream.latestOffset(
                    new RabbitMQStreamOffset(Map.of("test-stream", 0L)),
                    ReadLimit.allAvailable());
            assertThat(latest.getStreamOffsets()).containsEntry("test-stream", 10L);
        }

        @Test
        void queryTailOffsetsForAvailableNowDoesNotUseProbeWhenStatsOvershoot() throws Exception {
            RabbitMQMicroBatchStream stream = createStream(minimalOptions());
            // stats tail => 401, direct last-message probe tail => 400
            setPrivateField(stream, "environment",
                    new ProbeCountingEnvironment(400L, java.util.List.of(399L)));
            setPrivateField(stream, "availableNowSnapshot", null);

            stream.prepareForTriggerAvailableNow();

            RabbitMQStreamOffset latest = (RabbitMQStreamOffset) stream.latestOffset(
                    new RabbitMQStreamOffset(Map.of("test-stream", 400L)),
                    ReadLimit.allAvailable());
            assertThat(latest.getStreamOffsets()).containsEntry("test-stream", 401L);
            assertThat(((ProbeCountingEnvironment) getPrivateField(stream, "environment")).probeBuilderCalls)
                    .isEqualTo(0);
        }

        @Test
        void prepareForTriggerAvailableNowUsesStatsWithoutTailProbe() throws Exception {
            RabbitMQMicroBatchStream stream = createStream(minimalOptions());
            ProbeCountingEnvironment env = new ProbeCountingEnvironment(10L, java.util.List.of(14L));
            setPrivateField(stream, "environment", env);

            RabbitMQStreamOffset start = new RabbitMQStreamOffset(Map.of("test-stream", 0L));
            RabbitMQStreamOffset first = (RabbitMQStreamOffset) stream.latestOffset(start, ReadLimit.allAvailable());
            assertThat(first.getStreamOffsets()).containsEntry("test-stream", 11L);

            stream.prepareForTriggerAvailableNow();

            assertThat(env.probeBuilderCalls).isEqualTo(0);
            assertThat(env.queryStatsCalls).isEqualTo(3);
        }

        @Test
        void probeTailOffsetFromLastMessageHandlesInterruptEmptyPollAndCloseFailure() throws Exception {
            RabbitMQMicroBatchStream stream = createStream(minimalOptions());

            Thread.currentThread().interrupt();
            long interrupted = invokeProbe(stream, new ProbingErrorEnvironment());
            Thread.interrupted();

            long empty = invokeProbe(stream, new ProbingErrorEnvironment());
            long closeFail = invokeProbe(stream, new ProbingErrorEnvironment());

            assertThat(interrupted).isEqualTo(0L);
            assertThat(empty).isEqualTo(0L);
            assertThat(closeFail).isEqualTo(0L);
            Thread.interrupted();
        }

        @Test
        void probeTailReturnsZeroWhenNoMessagesReceived() throws Exception {
            RabbitMQMicroBatchStream stream = createStream(minimalOptions());
            long result = invokeProbe(stream, new FixedOffsetProbeEnvironment(java.util.List.of()));
            assertThat(result).isEqualTo(0L);
        }

        @Test
        void probeTailTakesMaxWhenMultipleOffsetsReceived() throws Exception {
            RabbitMQMicroBatchStream stream = createStream(minimalOptions());
            long result = invokeProbe(stream,
                    new FixedOffsetProbeEnvironment(java.util.List.of(5L, 3L, 7L, 7L)));
            assertThat(result).isEqualTo(8L);
        }

        @Test
        void probeTailCapturesStaggeredOffsetsWithinIdleGraceWindow() throws Exception {
            RabbitMQMicroBatchStream stream = createStream(minimalOptions());
            long result = invokeProbe(stream,
                    new FixedOffsetProbeEnvironment(java.util.List.of(5L, 9L), 0L, 120L));
            assertThat(result).isEqualTo(10L);
        }

        @Test
        void probeTailPreservesInterruptFlag() throws Exception {
            RabbitMQMicroBatchStream stream = createStream(minimalOptions());
            Thread.currentThread().interrupt();
            try {
                long result = invokeProbe(stream, new FixedOffsetProbeEnvironment(java.util.List.of()));
                assertThat(result).isEqualTo(0L);
                assertThat(Thread.currentThread().isInterrupted()).isTrue();
            } finally {
                Thread.interrupted();
            }
        }

        @Test
        void probeTailReturnsZeroWhenConsumerBuilderFails() throws Exception {
            RabbitMQMicroBatchStream stream = createStream(minimalOptions());
            long result = invokeProbe(stream, new ThrowingConsumerBuilderEnvironment(
                    new RuntimeException("build failed")));
            assertThat(result).isEqualTo(0L);
        }
    }

    @Nested
    class CommitAndStopBehavior {

        @Test
        void stopDoesNotPersistCachedOffsetsWhenCommitNotCalled() throws Exception {
            Map<String, String> opts = new LinkedHashMap<>();
            opts.put("endpoints", "localhost:5552");
            opts.put("stream", "test-stream");

            RabbitMQMicroBatchStream stream = createStream(new ConnectorOptions(opts));
            OffsetTrackingEnvironment env = new OffsetTrackingEnvironment();
            setPrivateField(stream, "environment", env);
            setPrivateField(stream, "cachedLatestOffset",
                    new RabbitMQStreamOffset(Map.of("test-stream", 10L)));

            stream.stop();

            assertThat(env.recordedOffsets).isEmpty();
        }

        @Test
        void stopPersistsLastCommittedOffsetsNotCachedLatest() throws Exception {
            Map<String, String> opts = new LinkedHashMap<>();
            opts.put("endpoints", "localhost:5552");
            opts.put("stream", "test-stream");

            RabbitMQMicroBatchStream stream = createStream(new ConnectorOptions(opts));
            OffsetTrackingEnvironment env = new OffsetTrackingEnvironment();
            setPrivateField(stream, "environment", env);
            setPrivateField(stream, "cachedLatestOffset",
                    new RabbitMQStreamOffset(Map.of("test-stream", 20L)));
            setPrivateField(stream, "lastCommittedEndOffsets",
                    new LinkedHashMap<>(Map.of("test-stream", 10L)));

            stream.stop();

            assertThat(env.recordedOffsets).containsExactly(Map.entry("test-stream", 9L));
        }

        @Test
        void stopCheckpointFallbackUsesSourceIndexLineFromOffsetsLog() throws Exception {
            Map<String, String> opts = new LinkedHashMap<>();
            opts.put("endpoints", "localhost:5552");
            opts.put("stream", "test-stream");

            Path queryCheckpoint = Files.createTempDirectory("rmq-checkpoint-");
            Files.createDirectories(queryCheckpoint.resolve("commits"));
            Files.createDirectories(queryCheckpoint.resolve("offsets"));
            Files.createDirectories(queryCheckpoint.resolve("sources").resolve("1"));
            Files.writeString(queryCheckpoint.resolve("commits").resolve("0"), "", StandardCharsets.UTF_8);
            Files.write(
                    queryCheckpoint.resolve("offsets").resolve("0"),
                    List.of(
                            "v1",
                            "{\"batchWatermarkMs\":0,\"batchTimestampMs\":0,\"conf\":{}}",
                            "{\"other-stream\":7}",
                            "{\"test-stream\":11}"),
                    StandardCharsets.UTF_8);

            RabbitMQMicroBatchStream stream = createStream(
                    new ConnectorOptions(opts),
                    queryCheckpoint.resolve("sources").resolve("1").toString());
            OffsetTrackingEnvironment env = new OffsetTrackingEnvironment();
            setPrivateField(stream, "environment", env);
            setPrivateField(stream, "streams", List.of("test-stream"));

            stream.stop();

            assertThat(env.recordedOffsets).containsExactly(Map.entry("test-stream", 10L));
        }

        @Test
        void stopDoesNotPersistAvailableNowPlannedOffsetsWhenCommitNotCalled() throws Exception {
            Map<String, String> opts = new LinkedHashMap<>();
            opts.put("endpoints", "localhost:5552");
            opts.put("stream", "test-stream");

            RabbitMQMicroBatchStream stream = createStream(new ConnectorOptions(opts));
            OffsetTrackingEnvironment env = new OffsetTrackingEnvironment();
            setPrivateField(stream, "environment", env);
            setPrivateField(stream, "availableNowSnapshot", Map.of("test-stream", 10L));
            setPrivateField(stream, "cachedLatestOffset",
                    new RabbitMQStreamOffset(Map.of("test-stream", 10L)));

            stream.stop();

            assertThat(env.recordedOffsets).isEmpty();
        }

        @Test
        void persistBrokerOffsetsDedupesRepeatedEndOffsets() throws Exception {
            Map<String, String> opts = new LinkedHashMap<>();
            opts.put("endpoints", "localhost:5552");
            opts.put("stream", "test-stream");

            RabbitMQMicroBatchStream stream = createStream(new ConnectorOptions(opts));
            OffsetTrackingEnvironment env = new OffsetTrackingEnvironment();
            setPrivateField(stream, "environment", env);

            stream.commit(new RabbitMQStreamOffset(Map.of("test-stream", 10L)));
            stream.commit(new RabbitMQStreamOffset(Map.of("test-stream", 10L)));

            assertThat(env.recordedOffsets).containsExactly(Map.entry("test-stream", 9L));
        }

        @Test
        void commitUpdatesEstimatedMessageSizeFromSparkAccumulators() throws Exception {
            Map<String, String> opts = new LinkedHashMap<>();
            opts.put("endpoints", "localhost:5552");
            opts.put("stream", "test-stream");

            RabbitMQMicroBatchStream stream = createStream(new ConnectorOptions(opts));
            OffsetTrackingEnvironment env = new OffsetTrackingEnvironment();
            setPrivateField(stream, "environment", env);
            setPrivateField(stream, "estimatedMessageSize", 16);

            LongAccumulator bytesAccumulator = new LongAccumulator();
            LongAccumulator recordsAccumulator = new LongAccumulator();
            bytesAccumulator.add(1200L);
            recordsAccumulator.add(3L);
            setPrivateField(stream, "messageSizeBytesAccumulator", bytesAccumulator);
            setPrivateField(stream, "messageSizeRecordsAccumulator", recordsAccumulator);

            stream.commit(new RabbitMQStreamOffset(Map.of("test-stream", 10L)));

            assertThat(getPrivateField(stream, "estimatedMessageSize")).isEqualTo(400);
        }

        @Test
        void persistBrokerOffsetsIgnoresZeroEndOffsets() throws Exception {
            Map<String, String> opts = new LinkedHashMap<>();
            opts.put("endpoints", "localhost:5552");
            opts.put("stream", "test-stream");

            RabbitMQMicroBatchStream stream = createStream(new ConnectorOptions(opts));
            OffsetTrackingEnvironment env = new OffsetTrackingEnvironment();
            setPrivateField(stream, "environment", env);

            stream.commit(new RabbitMQStreamOffset(Map.of("test-stream", 0L)));

            assertThat(env.recordedOffsets).isEmpty();
        }

        @Test
        void persistBrokerOffsetsAvoidsSynchronousStreamStatsChecks() throws Exception {
            Map<String, String> opts = new LinkedHashMap<>();
            opts.put("endpoints", "localhost:5552");
            opts.put("stream", "test-stream");
            opts.put("failOnDataLoss", "false");

            RabbitMQMicroBatchStream stream = createStream(new ConnectorOptions(opts));
            QueryCountingOffsetTrackingEnvironment env = new QueryCountingOffsetTrackingEnvironment();
            setPrivateField(stream, "environment", env);

            stream.commit(new RabbitMQStreamOffset(Map.of("test-stream", 10L)));

            assertThat(env.recordedOffsets).containsExactly(Map.entry("test-stream", 9L));
            assertThat(env.queryStatsCalls).isZero();
        }

        @Test
        void persistBrokerOffsetsDoesNotCacheWhenCommitFutureHasExecutionFailure() throws Exception {
            Map<String, String> opts = new LinkedHashMap<>();
            opts.put("endpoints", "localhost:5552");
            opts.put("stream", "test-stream");

            RabbitMQMicroBatchStream stream = createStream(new ConnectorOptions(opts));
            ExecutionFailureOffsetTrackingEnvironment env =
                    new ExecutionFailureOffsetTrackingEnvironment("s2");
            setPrivateField(stream, "environment", env);

            RabbitMQStreamOffset end = new RabbitMQStreamOffset(Map.of("s1", 10L, "s2", 20L));
            stream.commit(end);
            stream.commit(end);

            long s1Stores = env.recordedOffsets.stream()
                    .filter(entry -> "s1".equals(entry.getKey()))
                    .count();
            assertThat(s1Stores).isEqualTo(2L);
        }

        @Test
        void persistBrokerOffsetsDoesNotCacheWhenStoreOffsetExceptionIsCaught() throws Exception {
            Map<String, String> opts = new LinkedHashMap<>();
            opts.put("endpoints", "localhost:5552");
            opts.put("stream", "test-stream");

            RabbitMQMicroBatchStream stream = createStream(new ConnectorOptions(opts));
            CaughtStoreFailureOffsetTrackingEnvironment env =
                    new CaughtStoreFailureOffsetTrackingEnvironment("s2");
            setPrivateField(stream, "environment", env);

            RabbitMQStreamOffset end = new RabbitMQStreamOffset(Map.of("s1", 10L, "s2", 20L));
            stream.commit(end);
            stream.commit(end);

            long s1Stores = env.recordedOffsets.stream()
                    .filter(entry -> "s1".equals(entry.getKey()))
                    .count();
            assertThat(s1Stores).isEqualTo(2L);
        }
    }

    @Nested
    class PlanInputPartitionsDataLoss {

        @Test
        void planInputPartitionsRetentionTruncationFailOnDataLossTrueThrows() throws Exception {
            Map<String, String> opts = new LinkedHashMap<>();
            opts.put("endpoints", "localhost:5552");
            opts.put("stream", "test-stream");
            opts.put("failOnDataLoss", "true");

            RabbitMQMicroBatchStream stream = createStream(new ConnectorOptions(opts));
            setPrivateField(stream, "environment", new FirstOffsetEnvironment(10L));

            RabbitMQStreamOffset start = new RabbitMQStreamOffset(Map.of("test-stream", 0L));
            RabbitMQStreamOffset end = new RabbitMQStreamOffset(Map.of("test-stream", 20L));

            assertThatThrownBy(() -> stream.planInputPartitions(start, end))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("before the first available offset");
        }

        @Test
        void planInputPartitionsRetentionTruncationFailOnDataLossFalseAdvancesStart() throws Exception {
            Map<String, String> opts = new LinkedHashMap<>();
            opts.put("endpoints", "localhost:5552");
            opts.put("stream", "test-stream");
            opts.put("failOnDataLoss", "false");

            RabbitMQMicroBatchStream stream = createStream(new ConnectorOptions(opts));
            setPrivateField(stream, "environment", new FirstOffsetEnvironment(10L));

            RabbitMQStreamOffset start = new RabbitMQStreamOffset(Map.of("test-stream", 0L));
            RabbitMQStreamOffset end = new RabbitMQStreamOffset(Map.of("test-stream", 20L));

            InputPartition[] partitions = stream.planInputPartitions(start, end);
            assertThat(partitions).hasSize(1);
            RabbitMQInputPartition partition = (RabbitMQInputPartition) partitions[0];
            assertThat(partition.getStartOffset()).isEqualTo(10L);
        }

        @Test
        void planInputPartitionsMissingStreamFailOnDataLossFalseSkips() throws Exception {
            Map<String, String> opts = new LinkedHashMap<>();
            opts.put("endpoints", "localhost:5552");
            opts.put("stream", "test-stream");
            opts.put("failOnDataLoss", "false");

            RabbitMQMicroBatchStream stream = createStream(new ConnectorOptions(opts));
            setPrivateField(stream, "environment", new MissingStreamEnvironment());

            RabbitMQStreamOffset start = new RabbitMQStreamOffset(Map.of("test-stream", 0L));
            RabbitMQStreamOffset end = new RabbitMQStreamOffset(Map.of("test-stream", 20L));

            InputPartition[] partitions = stream.planInputPartitions(start, end);
            assertThat(partitions).isEmpty();
        }

        @Test
        void planInputPartitionsMissingStreamFailOnDataLossTrueThrows() throws Exception {
            Map<String, String> opts = new LinkedHashMap<>();
            opts.put("endpoints", "localhost:5552");
            opts.put("stream", "test-stream");
            opts.put("failOnDataLoss", "true");

            RabbitMQMicroBatchStream stream = createStream(new ConnectorOptions(opts));
            setPrivateField(stream, "environment", new MissingStreamEnvironment());

            RabbitMQStreamOffset start = new RabbitMQStreamOffset(Map.of("test-stream", 0L));
            RabbitMQStreamOffset end = new RabbitMQStreamOffset(Map.of("test-stream", 20L));

            assertThatThrownBy(() -> stream.planInputPartitions(start, end))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("no longer exists");
        }
    }

    @Nested
    class SplitPlanning {

        @Test
        void splitPlanningWithSingleActiveConsumerThrows() throws Exception {
            Map<String, String> opts = new LinkedHashMap<>();
            opts.put("endpoints", "localhost:5552");
            opts.put("stream", "test-stream");
            opts.put("singleActiveConsumer", "true");
            opts.put("consumerName", "sac-reader");
            opts.put("minPartitions", "2");

            RabbitMQMicroBatchStream stream = createStream(new ConnectorOptions(opts));
            setPrivateField(stream, "environment", new FirstOffsetEnvironment(0L));

            RabbitMQStreamOffset start = new RabbitMQStreamOffset(Map.of("test-stream", 0L));
            RabbitMQStreamOffset end = new RabbitMQStreamOffset(Map.of("test-stream", 100L));

            assertThatThrownBy(() -> stream.planInputPartitions(start, end))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("singleActiveConsumer")
                    .hasMessageContaining("split");
        }

        @Test
        void minPartitionsSplitAllocationExactness() {
            Map<String, String> opts = new LinkedHashMap<>();
            opts.put("endpoints", "localhost:5552");
            opts.put("stream", "test-stream");
            opts.put("minPartitions", "4");

            RabbitMQMicroBatchStream stream = createStream(new ConnectorOptions(opts));

            Map<String, Long> start = new LinkedHashMap<>();
            start.put("s1", 0L);
            start.put("s2", 0L);
            Map<String, Long> end = new LinkedHashMap<>();
            end.put("s1", 100L);
            end.put("s2", 100L);

            InputPartition[] partitions = stream.planInputPartitions(
                    new RabbitMQStreamOffset(start), new RabbitMQStreamOffset(end));

            assertThat(partitions).hasSize(4);
            Map<String, long[]> merged = new LinkedHashMap<>();
            for (InputPartition partition : partitions) {
                RabbitMQInputPartition p = (RabbitMQInputPartition) partition;
                merged.merge(p.getStream(), new long[]{p.getStartOffset(), p.getEndOffset()},
                        (current, value) -> new long[]{current[0], value[1]});
            }
            assertThat(merged.get("s1")[0]).isEqualTo(0L);
            assertThat(merged.get("s1")[1]).isEqualTo(100L);
            assertThat(merged.get("s2")[0]).isEqualTo(0L);
            assertThat(merged.get("s2")[1]).isEqualTo(100L);
        }

        @Test
        void splitAllocationWhenRemainderTiesOccur() {
            Map<String, String> opts = new LinkedHashMap<>();
            opts.put("endpoints", "localhost:5552");
            opts.put("stream", "test-stream");
            opts.put("minPartitions", "3");

            RabbitMQMicroBatchStream stream = createStream(new ConnectorOptions(opts));

            Map<String, Long> start = new LinkedHashMap<>();
            start.put("s1", 0L);
            start.put("s2", 0L);
            Map<String, Long> end = new LinkedHashMap<>();
            end.put("s1", 100L);
            end.put("s2", 100L);

            InputPartition[] partitions = stream.planInputPartitions(
                    new RabbitMQStreamOffset(start), new RabbitMQStreamOffset(end));

            assertThat(partitions).hasSize(3);
            int s1Count = 0;
            int s2Count = 0;
            for (InputPartition partition : partitions) {
                RabbitMQInputPartition p = (RabbitMQInputPartition) partition;
                if (p.getStream().equals("s1")) {
                    s1Count++;
                } else if (p.getStream().equals("s2")) {
                    s2Count++;
                }
            }
            assertThat(s1Count + s2Count).isEqualTo(3);
            assertThat(Math.abs(s1Count - s2Count)).isLessThanOrEqualTo(1);
        }

        @Test
        void planWithSplittingDistributesSplitsEvenlyAcrossStreams() {
            Map<String, String> opts = new LinkedHashMap<>();
            opts.put("endpoints", "localhost:5552");
            opts.put("stream", "test-stream");
            opts.put("minPartitions", "6");

            RabbitMQMicroBatchStream stream = createStream(new ConnectorOptions(opts));

            Map<String, Long> start = new LinkedHashMap<>();
            start.put("s1", 0L);
            start.put("s2", 0L);
            Map<String, Long> end = new LinkedHashMap<>();
            end.put("s1", 100L);
            end.put("s2", 200L);

            InputPartition[] partitions = stream.planInputPartitions(
                    new RabbitMQStreamOffset(start), new RabbitMQStreamOffset(end));

            Map<String, java.util.List<long[]>> splits = new LinkedHashMap<>();
            for (InputPartition partition : partitions) {
                RabbitMQInputPartition p = (RabbitMQInputPartition) partition;
                splits.computeIfAbsent(p.getStream(), k -> new java.util.ArrayList<>())
                        .add(new long[]{p.getStartOffset(), p.getEndOffset()});
            }

            assertThat(splits.get("s1")).hasSize(3);
            assertThat(splits.get("s2")).hasSize(3);

            assertThat(formatRanges(splits.get("s1")))
                    .containsExactly("0-34", "34-67", "67-100");
            assertThat(formatRanges(splits.get("s2")))
                    .containsExactly("0-67", "67-134", "134-200");
        }

        @Test
        void minPartitionsWithMaxRecordsPreservesMaxRecordsBound() {
            Map<String, String> opts = new LinkedHashMap<>();
            opts.put("endpoints", "localhost:5552");
            opts.put("stream", "test-stream");
            opts.put("minPartitions", "12");
            opts.put("maxRecordsPerPartition", "100");

            RabbitMQMicroBatchStream stream = createStream(new ConnectorOptions(opts));

            Map<String, Long> start = new LinkedHashMap<>();
            start.put("s1", 0L);
            start.put("s2", 0L);
            Map<String, Long> end = new LinkedHashMap<>();
            end.put("s1", 1000L);
            end.put("s2", 10L);

            InputPartition[] partitions = stream.planInputPartitions(
                    new RabbitMQStreamOffset(start), new RabbitMQStreamOffset(end));

            assertThat(partitions).hasSize(12);
            long s1Splits = 0;
            long maxS1Span = 0;
            for (InputPartition partition : partitions) {
                RabbitMQInputPartition p = (RabbitMQInputPartition) partition;
                long span = p.getEndOffset() - p.getStartOffset();
                if ("s1".equals(p.getStream())) {
                    s1Splits++;
                    maxS1Span = Math.max(maxS1Span, span);
                }
            }
            assertThat(s1Splits).isGreaterThanOrEqualTo(10);
            assertThat(maxS1Span).isLessThanOrEqualTo(100L);
        }

        @Test
        void planWithSplittingHandlesExactDivision() {
            Map<String, String> opts = new LinkedHashMap<>();
            opts.put("endpoints", "localhost:5552");
            opts.put("stream", "test-stream");
            opts.put("minPartitions", "5");

            RabbitMQMicroBatchStream stream = createStream(new ConnectorOptions(opts));

            Map<String, Long> start = Map.of("s1", 0L);
            Map<String, Long> end = Map.of("s1", 10L);

            InputPartition[] partitions = stream.planInputPartitions(
                    new RabbitMQStreamOffset(start), new RabbitMQStreamOffset(end));

            assertThat(partitions).hasSize(5);
            long expectedStart = 0L;
            for (InputPartition partition : partitions) {
                RabbitMQInputPartition p = (RabbitMQInputPartition) partition;
                assertThat(p.getStream()).isEqualTo("s1");
                assertThat(p.getStartOffset()).isEqualTo(expectedStart);
                assertThat(p.getEndOffset()).isEqualTo(expectedStart + 2);
                expectedStart += 2;
            }
        }

        @Test
        void planWithSplittingHandlesSingleMessagePerSplit() {
            Map<String, String> opts = new LinkedHashMap<>();
            opts.put("endpoints", "localhost:5552");
            opts.put("stream", "test-stream");
            opts.put("minPartitions", "3");

            RabbitMQMicroBatchStream stream = createStream(new ConnectorOptions(opts));

            Map<String, Long> start = Map.of("s1", 0L);
            Map<String, Long> end = Map.of("s1", 3L);

            InputPartition[] partitions = stream.planInputPartitions(
                    new RabbitMQStreamOffset(start), new RabbitMQStreamOffset(end));

            assertThat(partitions).hasSize(3);
            long offset = 0L;
            for (InputPartition partition : partitions) {
                RabbitMQInputPartition p = (RabbitMQInputPartition) partition;
                assertThat(p.getStartOffset()).isEqualTo(offset);
                assertThat(p.getEndOffset()).isEqualTo(offset + 1);
                offset++;
            }
        }

        @Test
        void planWithSplittingRemainderIsDeterministicByStreamOrder() {
            Map<String, String> opts = new LinkedHashMap<>();
            opts.put("endpoints", "localhost:5552");
            opts.put("stream", "test-stream");
            opts.put("minPartitions", "4");

            RabbitMQMicroBatchStream stream = createStream(new ConnectorOptions(opts));

            Map<String, Long> start = new LinkedHashMap<>();
            start.put("s1", 0L);
            start.put("s2", 0L);
            start.put("s3", 0L);
            Map<String, Long> end = new LinkedHashMap<>();
            end.put("s1", 9L);
            end.put("s2", 8L);
            end.put("s3", 7L);

            InputPartition[] partitions = stream.planInputPartitions(
                    new RabbitMQStreamOffset(start), new RabbitMQStreamOffset(end));

            int s1Count = 0;
            int s2Count = 0;
            int s3Count = 0;
            for (InputPartition partition : partitions) {
                RabbitMQInputPartition p = (RabbitMQInputPartition) partition;
                if (p.getStream().equals("s1")) {
                    s1Count++;
                } else if (p.getStream().equals("s2")) {
                    s2Count++;
                } else if (p.getStream().equals("s3")) {
                    s3Count++;
                }
            }
            assertThat(s1Count).isEqualTo(2);
            assertThat(s2Count).isEqualTo(1);
            assertThat(s3Count).isEqualTo(1);
        }
    }

    private static java.util.List<String> formatRanges(java.util.List<long[]> ranges) {
        java.util.List<long[]> sorted = new java.util.ArrayList<>(ranges);
        sorted.sort(java.util.Comparator.comparingLong(range -> range[0]));
        java.util.List<String> formatted = new java.util.ArrayList<>();
        for (long[] range : sorted) {
            formatted.add(range[0] + "-" + range[1]);
        }
        return formatted;
    }

    // ======================================================================
    // Metrics (ReportsSourceMetrics)
    // ======================================================================

    @Nested
    class Metrics {

        @Test
        void metricsReturnsZerosWhenNoLatestOffset() {
            RabbitMQMicroBatchStream stream = createStream(minimalOptions());

            Map<String, String> metrics = stream.metrics(Optional.empty());

            assertThat(metrics).containsEntry("minOffsetsBehindLatest", "0");
            assertThat(metrics).containsEntry("maxOffsetsBehindLatest", "0");
            assertThat(metrics).containsEntry("avgOffsetsBehindLatest", "0.0");
        }

        @Test
        void metricsContainsExpectedKeys() {
            RabbitMQMicroBatchStream stream = createStream(minimalOptions());

            Map<String, String> metrics = stream.metrics(Optional.empty());

            assertThat(metrics).containsKeys(
                    "minOffsetsBehindLatest",
                    "maxOffsetsBehindLatest",
                    "avgOffsetsBehindLatest");
        }

        @Test
        void metricsUseTailOffsetsWhenAvailable() throws Exception {
            RabbitMQMicroBatchStream stream = createStream(minimalOptions());
            setPrivateField(stream, "cachedLatestOffset",
                    new RabbitMQStreamOffset(Map.of("test-stream", 10L)));
            setPrivateField(stream, "cachedTailOffset",
                    new RabbitMQStreamOffset(Map.of("test-stream", 20L)));

            Map<String, String> metrics = stream.metrics(Optional.of(
                    new RabbitMQStreamOffset(Map.of("test-stream", 5L))));

            assertThat(metrics).containsEntry("minOffsetsBehindLatest", "15");
            assertThat(metrics).containsEntry("maxOffsetsBehindLatest", "15");
            assertThat(metrics).containsEntry("avgOffsetsBehindLatest", "15.0");
        }

        @Test
        void metricsWithMultipleStreamsAndAsymmetricLag() throws Exception {
            RabbitMQMicroBatchStream stream = createStream(minimalOptions());
            setPrivateField(stream, "cachedTailOffset",
                    new RabbitMQStreamOffset(Map.of("s1", 100L, "s2", 50L)));

            Map<String, String> metrics = stream.metrics(Optional.of(
                    new RabbitMQStreamOffset(Map.of("s1", 90L, "s2", 10L))));

            assertThat(metrics).containsEntry("minOffsetsBehindLatest", "10");
            assertThat(metrics).containsEntry("maxOffsetsBehindLatest", "40");
            assertThat(metrics).containsEntry("avgOffsetsBehindLatest", "25.0");
        }

        @Test
        void metricsClampWhenConsumedOffsetAboveTail() throws Exception {
            RabbitMQMicroBatchStream stream = createStream(minimalOptions());
            setPrivateField(stream, "cachedTailOffset",
                    new RabbitMQStreamOffset(Map.of("s1", 50L)));

            Map<String, String> metrics = stream.metrics(Optional.of(
                    new RabbitMQStreamOffset(Map.of("s1", 70L))));

            assertThat(metrics).containsEntry("minOffsetsBehindLatest", "0");
            assertThat(metrics).containsEntry("maxOffsetsBehindLatest", "0");
            assertThat(metrics).containsEntry("avgOffsetsBehindLatest", "0.0");
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

        @Test
        void streamModeUsesSingleThreadBrokerExecutor() throws Exception {
            RabbitMQMicroBatchStream stream = createStream(minimalOptions());
            try {
                Object executor = getPrivateField(stream, "brokerCommitExecutor");
                assertThat(executor).isInstanceOf(ThreadPoolExecutor.class);
                assertThat(((ThreadPoolExecutor) executor).getCorePoolSize()).isEqualTo(1);
            } finally {
                stream.stop();
            }
        }

        @Test
        void tailQueryExecutorIsSeparateFromBrokerCommitExecutor() throws Exception {
            RabbitMQMicroBatchStream stream = createStream(minimalOptions());
            try {
                Object commitExecutor = getPrivateField(stream, "brokerCommitExecutor");
                Object tailExecutor = getPrivateField(stream, "tailQueryExecutor");
                assertThat(commitExecutor).isInstanceOf(ThreadPoolExecutor.class);
                assertThat(tailExecutor).isInstanceOf(ThreadPoolExecutor.class);
                assertThat(tailExecutor).isNotSameAs(commitExecutor);
                assertThat(((ThreadPoolExecutor) tailExecutor).getCorePoolSize()).isEqualTo(1);
            } finally {
                stream.stop();
            }
        }
    }

    @Nested
    class ConsumerNameDerivation {
        @Test
        void derivesStableConsumerNameFromCheckpointWhenMissing() throws Exception {
            ConnectorOptions opts = minimalOptions();
            String derived = (String) invokeStatic(
                    "deriveConsumerName",
                    new Class<?>[]{ConnectorOptions.class, String.class},
                    opts, "/tmp/checkpoint/path");
            assertThat(derived).startsWith("spark-rmq-");
        }

        @Test
        void derivedConsumerNameIncludesLegacyAndStrongHashSegments() throws Exception {
            ConnectorOptions opts = minimalOptions();
            String checkpointPath = "/tmp/checkpoint/path";
            String derived = (String) invokeStatic(
                    "deriveConsumerName",
                    new Class<?>[]{ConnectorOptions.class, String.class},
                    opts, checkpointPath);
            String legacy = (String) invokeStatic(
                    "deriveLegacyConsumerName",
                    new Class<?>[]{String.class},
                    checkpointPath);
            assertThat(derived).startsWith(legacy + "-");
        }

        @Test
        void usesConfiguredConsumerNameWhenProvided() throws Exception {
            Map<String, String> optsMap = new LinkedHashMap<>();
            optsMap.put("endpoints", "localhost:5552");
            optsMap.put("stream", "test-stream");
            optsMap.put("consumerName", "explicit-consumer");
            ConnectorOptions opts = new ConnectorOptions(optsMap);
            String derived = (String) invokeStatic(
                    "deriveConsumerName",
                    new Class<?>[]{ConnectorOptions.class, String.class},
                    opts, "/tmp/checkpoint/path");
            assertThat(derived).isEqualTo("explicit-consumer");
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
        return createStream(opts, "/tmp/checkpoint");
    }

    private static RabbitMQMicroBatchStream createStream(
            ConnectorOptions opts, String checkpointLocation) {
        var schema = RabbitMQStreamTable.buildSourceSchema(opts.getMetadataFields());
        return new RabbitMQMicroBatchStream(opts, schema, checkpointLocation);
    }

    private static void setPrivateField(Object target, String fieldName, Object value)
            throws Exception {
        Field field = findField(target.getClass(), fieldName);
        field.setAccessible(true);
        field.set(target, value);
    }

    private static Object getPrivateField(Object target, String fieldName)
            throws Exception {
        Field field = findField(target.getClass(), fieldName);
        field.setAccessible(true);
        return field.get(target);
    }

    private static Field findField(Class<?> clazz, String fieldName) throws NoSuchFieldException {
        for (Class<?> current = clazz; current != null; current = current.getSuperclass()) {
            try {
                return current.getDeclaredField(fieldName);
            } catch (NoSuchFieldException ignored) {
            }
        }
        throw new NoSuchFieldException(fieldName + " not found in " + clazz.getName() + " hierarchy");
    }

    private static Method findMethod(Class<?> clazz, String methodName, Class<?>... parameterTypes)
            throws NoSuchMethodException {
        for (Class<?> current = clazz; current != null; current = current.getSuperclass()) {
            try {
                return current.getDeclaredMethod(methodName, parameterTypes);
            } catch (NoSuchMethodException ignored) {
            }
        }
        throw new NoSuchMethodException(
                methodName + " not found in " + clazz.getName() + " hierarchy");
    }

    private static Object invokeStatic(String methodName, Class<?>[] parameterTypes, Object... args)
            throws Exception {
        Method method = findMethod(RabbitMQMicroBatchStream.class, methodName, parameterTypes);
        method.setAccessible(true);
        return method.invoke(null, args);
    }

    private static boolean hasPrepareForRealTimeMode(RabbitMQMicroBatchStream stream) {
        try {
            stream.getClass().getMethod("prepareForRealTimeMode");
            return true;
        } catch (NoSuchMethodException e) {
            return false;
        }
    }

    private static void invokePrepareForRealTimeMode(RabbitMQMicroBatchStream stream) throws Exception {
        Method method = stream.getClass().getMethod("prepareForRealTimeMode");
        try {
            method.invoke(stream);
        } catch (java.lang.reflect.InvocationTargetException e) {
            if (e.getCause() instanceof Exception exception) {
                throw exception;
            }
            throw e;
        }
    }

    private static long invokeProbe(RabbitMQMicroBatchStream stream,
                                    com.rabbitmq.stream.Environment env) throws Exception {
        Method method = findMethod(RabbitMQMicroBatchStream.class,
                "probeTailOffsetFromLastMessage",
                com.rabbitmq.stream.Environment.class, String.class);
        method.setAccessible(true);
        return (long) method.invoke(stream, env, "test-stream");
    }

    private static final class CountingEnvironment implements com.rabbitmq.stream.Environment {
        private int calls = 0;

        @Override
        public com.rabbitmq.stream.StreamCreator streamCreator() {
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
        public com.rabbitmq.stream.StreamStats queryStreamStats(String stream) {
            calls++;
            long offset = calls == 1 ? 4L : 9L;
            return new FixedStreamStats(offset);
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
        public com.rabbitmq.stream.ProducerBuilder producerBuilder() {
            throw new UnsupportedOperationException();
        }

        @Override
        public com.rabbitmq.stream.ConsumerBuilder consumerBuilder() {
            return new ProbingConsumerBuilder();
        }

        @Override
        public void close() {
        }
    }

    private static final class ProbeCountingEnvironment implements com.rabbitmq.stream.Environment {
        private final long committedOffset;
        private final java.util.List<Long> probeOffsets;
        private int queryStatsCalls = 0;
        private int probeBuilderCalls = 0;

        private ProbeCountingEnvironment(long committedOffset, java.util.List<Long> probeOffsets) {
            this.committedOffset = committedOffset;
            this.probeOffsets = probeOffsets;
        }

        @Override
        public com.rabbitmq.stream.StreamCreator streamCreator() {
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
        public com.rabbitmq.stream.StreamStats queryStreamStats(String stream) {
            queryStatsCalls++;
            return new FixedStreamStats(committedOffset);
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
        public com.rabbitmq.stream.ProducerBuilder producerBuilder() {
            throw new UnsupportedOperationException();
        }

        @Override
        public com.rabbitmq.stream.ConsumerBuilder consumerBuilder() {
            probeBuilderCalls++;
            return new FixedOffsetProbeConsumerBuilder(probeOffsets);
        }

        @Override
        public void close() {
        }
    }

    private static final class DelayedStatsProbeEnvironment implements com.rabbitmq.stream.Environment {
        private final long delayMs;
        private final java.util.List<Long> probeOffsets;
        private final java.util.concurrent.atomic.AtomicInteger inFlightStatsQueries =
                new java.util.concurrent.atomic.AtomicInteger();
        private final java.util.concurrent.atomic.AtomicInteger maxConcurrentStatsQueries =
                new java.util.concurrent.atomic.AtomicInteger();

        private DelayedStatsProbeEnvironment(long delayMs, java.util.List<Long> probeOffsets) {
            this.delayMs = delayMs;
            this.probeOffsets = probeOffsets;
        }

        int maxConcurrentStatsQueries() {
            return maxConcurrentStatsQueries.get();
        }

        @Override
        public com.rabbitmq.stream.StreamCreator streamCreator() {
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
        public com.rabbitmq.stream.StreamStats queryStreamStats(String stream) {
            int inFlight = inFlightStatsQueries.incrementAndGet();
            maxConcurrentStatsQueries.accumulateAndGet(inFlight, Math::max);
            try {
                Thread.sleep(delayMs);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } finally {
                inFlightStatsQueries.decrementAndGet();
            }
            return new FixedStreamStats(6L);
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
        public com.rabbitmq.stream.ProducerBuilder producerBuilder() {
            throw new UnsupportedOperationException();
        }

        @Override
        public com.rabbitmq.stream.ConsumerBuilder consumerBuilder() {
            return new FixedOffsetProbeConsumerBuilder(probeOffsets);
        }

        @Override
        public void close() {
        }
    }

    private static final class ForcedStreamsMicroBatchStream extends BaseRabbitMQMicroBatchStream {
        private final java.util.List<String> forcedStreams;

        private ForcedStreamsMicroBatchStream(
                ConnectorOptions options,
                org.apache.spark.sql.types.StructType schema,
                String checkpointLocation,
                java.util.List<String> forcedStreams) {
            super(options, schema, checkpointLocation);
            this.forcedStreams = forcedStreams;
        }

        @Override
        synchronized java.util.List<String> discoverStreams() {
            return forcedStreams;
        }
    }

    private static final class SequencedDiscoveryMicroBatchStream extends BaseRabbitMQMicroBatchStream {
        private final java.util.List<java.util.List<String>> discoveries;
        private int index = 0;

        private SequencedDiscoveryMicroBatchStream(
                ConnectorOptions options,
                org.apache.spark.sql.types.StructType schema,
                String checkpointLocation,
                java.util.List<java.util.List<String>> discoveries) {
            super(options, schema, checkpointLocation);
            this.discoveries = discoveries;
        }

        @Override
        java.util.List<String> discoverSuperStreamPartitions() {
            if (discoveries.isEmpty()) {
                return java.util.List.of();
            }
            int current = Math.min(index, discoveries.size() - 1);
            index++;
            return discoveries.get(current);
        }
    }

    private static final class FirstOffsetEnvironment implements com.rabbitmq.stream.Environment {
        private final long firstOffset;

        private FirstOffsetEnvironment(long firstOffset) {
            this.firstOffset = firstOffset;
        }

        @Override
        public com.rabbitmq.stream.StreamCreator streamCreator() {
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
        public com.rabbitmq.stream.StreamStats queryStreamStats(String stream) {
            return new FixedStreamStats(firstOffset);
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
        public com.rabbitmq.stream.ProducerBuilder producerBuilder() {
            throw new UnsupportedOperationException();
        }

        @Override
        public com.rabbitmq.stream.ConsumerBuilder consumerBuilder() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void close() {
        }
    }

    private static final class TimestampStartEnvironment implements com.rabbitmq.stream.Environment {
        private final long firstOffset;
        private final java.util.List<Long> probedOffsets;
        private final long delayMs;

        private TimestampStartEnvironment(long firstOffset, java.util.List<Long> probedOffsets) {
            this(firstOffset, probedOffsets, 0L);
        }

        private TimestampStartEnvironment(long firstOffset, java.util.List<Long> probedOffsets, long delayMs) {
            this.firstOffset = firstOffset;
            this.probedOffsets = probedOffsets;
            this.delayMs = delayMs;
        }

        @Override
        public com.rabbitmq.stream.StreamCreator streamCreator() {
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
        public com.rabbitmq.stream.StreamStats queryStreamStats(String stream) {
            return new FixedStreamStats(firstOffset);
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
        public com.rabbitmq.stream.ProducerBuilder producerBuilder() {
            throw new UnsupportedOperationException();
        }

        @Override
        public com.rabbitmq.stream.ConsumerBuilder consumerBuilder() {
            return new FixedOffsetProbeConsumerBuilder(probedOffsets, delayMs);
        }

        @Override
        public void close() {
        }
    }

    private static final class MissingStreamEnvironment implements com.rabbitmq.stream.Environment {
        @Override
        public com.rabbitmq.stream.StreamCreator streamCreator() {
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
        public com.rabbitmq.stream.StreamStats queryStreamStats(String stream) {
            throw new com.rabbitmq.stream.StreamDoesNotExistException(stream);
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
        public com.rabbitmq.stream.ProducerBuilder producerBuilder() {
            throw new UnsupportedOperationException();
        }

        @Override
        public com.rabbitmq.stream.ConsumerBuilder consumerBuilder() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void close() {
        }
    }

    private static final class FixedStreamStats implements com.rabbitmq.stream.StreamStats {
        private final long firstOffset;

        private FixedStreamStats(long firstOffset) {
            this.firstOffset = firstOffset;
        }

        @Override
        public long firstOffset() {
            return firstOffset;
        }

        @Override
        public long committedChunkId() {
            return firstOffset;
        }

        @Override
        public long committedOffset() {
            return firstOffset;
        }
    }


    private static final class ProbingConsumerBuilder implements com.rabbitmq.stream.ConsumerBuilder {
        private com.rabbitmq.stream.MessageHandler handler;

        @Override
        public com.rabbitmq.stream.ConsumerBuilder stream(String stream) {
            return this;
        }

        @Override
        public com.rabbitmq.stream.ConsumerBuilder superStream(String superStream) {
            return this;
        }

        @Override
        public com.rabbitmq.stream.ConsumerBuilder offset(
                com.rabbitmq.stream.OffsetSpecification offsetSpecification) {
            return this;
        }

        @Override
        public com.rabbitmq.stream.ConsumerBuilder messageHandler(
                com.rabbitmq.stream.MessageHandler messageHandler) {
            this.handler = messageHandler;
            return this;
        }

        @Override
        public com.rabbitmq.stream.ConsumerBuilder name(String name) {
            return this;
        }

        @Override
        public com.rabbitmq.stream.ConsumerBuilder singleActiveConsumer() {
            return this;
        }

        @Override
        public com.rabbitmq.stream.ConsumerBuilder consumerUpdateListener(
                com.rabbitmq.stream.ConsumerUpdateListener consumerUpdateListener) {
            return this;
        }

        @Override
        public com.rabbitmq.stream.ConsumerBuilder subscriptionListener(
                com.rabbitmq.stream.SubscriptionListener subscriptionListener) {
            return this;
        }

        @Override
        public com.rabbitmq.stream.ConsumerBuilder listeners(
                com.rabbitmq.stream.Resource.StateListener... listeners) {
            return this;
        }

        @Override
        public com.rabbitmq.stream.ConsumerBuilder.ManualTrackingStrategy manualTrackingStrategy() {
            return new NoopManualTrackingStrategy(this);
        }

        @Override
        public com.rabbitmq.stream.ConsumerBuilder.AutoTrackingStrategy autoTrackingStrategy() {
            return new NoopAutoTrackingStrategy(this);
        }

        @Override
        public com.rabbitmq.stream.ConsumerBuilder noTrackingStrategy() {
            return this;
        }

        @Override
        public ConsumerBuilder.FilterConfiguration filter() {
            throw new UnsupportedOperationException();
        }

        @Override
        public ConsumerBuilder.FlowConfiguration flow() {
            return new NoopFlowConfiguration(this);
        }

        @Override
        public com.rabbitmq.stream.Consumer build() {
            if (handler != null) {
                handler.handle(new FixedContext(4L), CODEC.messageBuilder().addData(new byte[0]).build());
                handler.handle(new FixedContext(14L), CODEC.messageBuilder().addData(new byte[0]).build());
            }
            return new NoopConsumer();
        }
    }

    private static final class FixedContext implements com.rabbitmq.stream.MessageHandler.Context {
        private final long offset;

        private FixedContext(long offset) {
            this.offset = offset;
        }

        @Override
        public long offset() {
            return offset;
        }

        @Override
        public void storeOffset() {
        }

        @Override
        public long timestamp() {
            return 0L;
        }

        @Override
        public long committedChunkId() {
            return 0L;
        }

        @Override
        public String stream() {
            return "test-stream";
        }

        @Override
        public com.rabbitmq.stream.Consumer consumer() {
            return null;
        }

        @Override
        public void processed() {
        }
    }

    private static final class ProbingErrorEnvironment implements com.rabbitmq.stream.Environment {
        @Override
        public com.rabbitmq.stream.ConsumerBuilder consumerBuilder() {
            return new ProbingErrorConsumerBuilder();
        }

        @Override
        public com.rabbitmq.stream.StreamCreator streamCreator() {
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
        public com.rabbitmq.stream.StreamStats queryStreamStats(String stream) {
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
        public com.rabbitmq.stream.ProducerBuilder producerBuilder() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void close() {
        }
    }

    private static final class FixedOffsetProbeEnvironment implements com.rabbitmq.stream.Environment {
        private final java.util.List<Long> offsets;
        private final long initialDelayMs;
        private final long interMessageDelayMs;

        private FixedOffsetProbeEnvironment(java.util.List<Long> offsets) {
            this(offsets, 0L, 0L);
        }

        private FixedOffsetProbeEnvironment(java.util.List<Long> offsets, long delayMs) {
            this(offsets, delayMs, 0L);
        }

        private FixedOffsetProbeEnvironment(
                java.util.List<Long> offsets, long initialDelayMs, long interMessageDelayMs) {
            this.offsets = offsets;
            this.initialDelayMs = initialDelayMs;
            this.interMessageDelayMs = interMessageDelayMs;
        }

        @Override
        public com.rabbitmq.stream.ConsumerBuilder consumerBuilder() {
            return new FixedOffsetProbeConsumerBuilder(offsets, initialDelayMs, interMessageDelayMs);
        }

        @Override
        public com.rabbitmq.stream.StreamCreator streamCreator() {
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
        public com.rabbitmq.stream.StreamStats queryStreamStats(String stream) {
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
        public com.rabbitmq.stream.ProducerBuilder producerBuilder() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void close() {
        }
    }

    private static final class ThrowingConsumerBuilderEnvironment implements com.rabbitmq.stream.Environment {
        private final RuntimeException error;

        private ThrowingConsumerBuilderEnvironment(RuntimeException error) {
            this.error = error;
        }

        @Override
        public com.rabbitmq.stream.ConsumerBuilder consumerBuilder() {
            throw error;
        }

        @Override
        public com.rabbitmq.stream.StreamCreator streamCreator() {
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
        public com.rabbitmq.stream.StreamStats queryStreamStats(String stream) {
            return new FixedStreamStats(0L);
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
        public com.rabbitmq.stream.ProducerBuilder producerBuilder() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void close() {
        }
    }

    private static final class ProbingErrorConsumerBuilder implements com.rabbitmq.stream.ConsumerBuilder {
        private com.rabbitmq.stream.MessageHandler handler;

        @Override
        public com.rabbitmq.stream.ConsumerBuilder stream(String stream) {
            return this;
        }

        @Override
        public com.rabbitmq.stream.ConsumerBuilder superStream(String superStream) {
            return this;
        }

        @Override
        public com.rabbitmq.stream.ConsumerBuilder offset(
                com.rabbitmq.stream.OffsetSpecification offsetSpecification) {
            return this;
        }

        @Override
        public com.rabbitmq.stream.ConsumerBuilder messageHandler(
                com.rabbitmq.stream.MessageHandler messageHandler) {
            this.handler = messageHandler;
            return this;
        }

        @Override
        public com.rabbitmq.stream.ConsumerBuilder name(String name) {
            return this;
        }

        @Override
        public com.rabbitmq.stream.ConsumerBuilder singleActiveConsumer() {
            return this;
        }

        @Override
        public com.rabbitmq.stream.ConsumerBuilder consumerUpdateListener(
                com.rabbitmq.stream.ConsumerUpdateListener consumerUpdateListener) {
            return this;
        }

        @Override
        public com.rabbitmq.stream.ConsumerBuilder subscriptionListener(
                com.rabbitmq.stream.SubscriptionListener subscriptionListener) {
            return this;
        }

        @Override
        public com.rabbitmq.stream.ConsumerBuilder listeners(
                com.rabbitmq.stream.Resource.StateListener... listeners) {
            return this;
        }

        @Override
        public com.rabbitmq.stream.ConsumerBuilder.ManualTrackingStrategy manualTrackingStrategy() {
            return new NoopManualTrackingStrategy(this);
        }

        @Override
        public com.rabbitmq.stream.ConsumerBuilder.AutoTrackingStrategy autoTrackingStrategy() {
            return new NoopAutoTrackingStrategy(this);
        }

        @Override
        public com.rabbitmq.stream.ConsumerBuilder noTrackingStrategy() {
            return this;
        }

        @Override
        public ConsumerBuilder.FilterConfiguration filter() {
            throw new UnsupportedOperationException();
        }

        @Override
        public ConsumerBuilder.FlowConfiguration flow() {
            return new NoopFlowConfiguration(this);
        }

        @Override
        public com.rabbitmq.stream.Consumer build() {
            if (handler != null && Thread.currentThread().isInterrupted()) {
                handler.handle(new FixedContext(1L), CODEC.messageBuilder().addData(new byte[0]).build());
            }
            return new ProbingErrorConsumer();
        }
    }

    private static final class FixedOffsetProbeConsumerBuilder implements com.rabbitmq.stream.ConsumerBuilder {
        private final java.util.List<Long> offsets;
        private final long initialDelayMs;
        private final long interMessageDelayMs;
        private com.rabbitmq.stream.MessageHandler handler;

        private FixedOffsetProbeConsumerBuilder(java.util.List<Long> offsets) {
            this(offsets, 0L, 0L);
        }

        private FixedOffsetProbeConsumerBuilder(java.util.List<Long> offsets, long delayMs) {
            this(offsets, delayMs, 0L);
        }

        private FixedOffsetProbeConsumerBuilder(
                java.util.List<Long> offsets, long initialDelayMs, long interMessageDelayMs) {
            this.offsets = offsets;
            this.initialDelayMs = initialDelayMs;
            this.interMessageDelayMs = interMessageDelayMs;
        }

        @Override
        public com.rabbitmq.stream.ConsumerBuilder stream(String stream) {
            return this;
        }

        @Override
        public com.rabbitmq.stream.ConsumerBuilder superStream(String superStream) {
            return this;
        }

        @Override
        public com.rabbitmq.stream.ConsumerBuilder offset(
                com.rabbitmq.stream.OffsetSpecification offsetSpecification) {
            return this;
        }

        @Override
        public com.rabbitmq.stream.ConsumerBuilder messageHandler(
                com.rabbitmq.stream.MessageHandler messageHandler) {
            this.handler = messageHandler;
            return this;
        }

        @Override
        public com.rabbitmq.stream.ConsumerBuilder name(String name) {
            return this;
        }

        @Override
        public com.rabbitmq.stream.ConsumerBuilder singleActiveConsumer() {
            return this;
        }

        @Override
        public com.rabbitmq.stream.ConsumerBuilder consumerUpdateListener(
                com.rabbitmq.stream.ConsumerUpdateListener consumerUpdateListener) {
            return this;
        }

        @Override
        public com.rabbitmq.stream.ConsumerBuilder subscriptionListener(
                com.rabbitmq.stream.SubscriptionListener subscriptionListener) {
            return this;
        }

        @Override
        public com.rabbitmq.stream.ConsumerBuilder listeners(
                com.rabbitmq.stream.Resource.StateListener... listeners) {
            return this;
        }

        @Override
        public com.rabbitmq.stream.ConsumerBuilder.ManualTrackingStrategy manualTrackingStrategy() {
            return new NoopManualTrackingStrategy(this);
        }

        @Override
        public com.rabbitmq.stream.ConsumerBuilder.AutoTrackingStrategy autoTrackingStrategy() {
            return new NoopAutoTrackingStrategy(this);
        }

        @Override
        public com.rabbitmq.stream.ConsumerBuilder noTrackingStrategy() {
            return this;
        }

        @Override
        public ConsumerBuilder.FilterConfiguration filter() {
            throw new UnsupportedOperationException();
        }

        @Override
        public ConsumerBuilder.FlowConfiguration flow() {
            return new NoopFlowConfiguration(this);
        }

        @Override
        public com.rabbitmq.stream.Consumer build() {
            if (handler != null) {
                if (initialDelayMs <= 0L && interMessageDelayMs <= 0L) {
                    for (Long offset : offsets) {
                        handler.handle(new FixedContext(offset),
                                CODEC.messageBuilder().addData(new byte[0]).build());
                    }
                } else {
                    Thread emitter = new Thread(() -> {
                        try {
                            if (initialDelayMs > 0L) {
                                Thread.sleep(initialDelayMs);
                            }
                            for (int i = 0; i < offsets.size(); i++) {
                                Long offset = offsets.get(i);
                                handler.handle(new FixedContext(offset),
                                        CODEC.messageBuilder().addData(new byte[0]).build());
                                if (interMessageDelayMs > 0L && i + 1 < offsets.size()) {
                                    Thread.sleep(interMessageDelayMs);
                                }
                            }
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }
                    }, "rabbitmq-microbatch-delayed-probe");
                    emitter.setDaemon(true);
                    emitter.start();
                }
            }
            return new NoopConsumer();
        }
    }

    private static final class ProbingErrorConsumer implements com.rabbitmq.stream.Consumer {
        @Override
        public void store(long offset) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void close() {
            throw new RuntimeException("close failed");
        }

        @Override
        public long storedOffset() {
            throw new UnsupportedOperationException();
        }
    }

    private static final class NoopConsumer implements com.rabbitmq.stream.Consumer {
        @Override
        public void store(long offset) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void close() {
        }

        @Override
        public long storedOffset() {
            throw new NoOffsetException("no offset");
        }
    }

    private static final class NoopManualTrackingStrategy
            implements com.rabbitmq.stream.ConsumerBuilder.ManualTrackingStrategy {
        private final com.rabbitmq.stream.ConsumerBuilder builder;

        private NoopManualTrackingStrategy(com.rabbitmq.stream.ConsumerBuilder builder) {
            this.builder = builder;
        }

        @Override
        public com.rabbitmq.stream.ConsumerBuilder.ManualTrackingStrategy checkInterval(
                java.time.Duration checkInterval) {
            return this;
        }

        @Override
        public com.rabbitmq.stream.ConsumerBuilder builder() {
            return builder;
        }
    }

    private static final class NoopAutoTrackingStrategy
            implements com.rabbitmq.stream.ConsumerBuilder.AutoTrackingStrategy {
        private final com.rabbitmq.stream.ConsumerBuilder builder;

        private NoopAutoTrackingStrategy(com.rabbitmq.stream.ConsumerBuilder builder) {
            this.builder = builder;
        }

        @Override
        public com.rabbitmq.stream.ConsumerBuilder.AutoTrackingStrategy messageCountBeforeStorage(
                int messageCountBeforeStorage) {
            return this;
        }

        @Override
        public com.rabbitmq.stream.ConsumerBuilder.AutoTrackingStrategy flushInterval(
                java.time.Duration flushInterval) {
            return this;
        }

        @Override
        public com.rabbitmq.stream.ConsumerBuilder builder() {
            return builder;
        }
    }

    private static final class NoopFlowConfiguration
            implements com.rabbitmq.stream.ConsumerBuilder.FlowConfiguration {
        private final com.rabbitmq.stream.ConsumerBuilder builder;

        private NoopFlowConfiguration(com.rabbitmq.stream.ConsumerBuilder builder) {
            this.builder = builder;
        }

        @Override
        public com.rabbitmq.stream.ConsumerBuilder.FlowConfiguration strategy(
                com.rabbitmq.stream.ConsumerFlowStrategy strategy) {
            return this;
        }

        @Override
        public com.rabbitmq.stream.ConsumerBuilder.FlowConfiguration initialCredits(
                int initialCredits) {
            return this;
        }

        @Override
        public com.rabbitmq.stream.ConsumerBuilder builder() {
            return builder;
        }
    }

    private static class OffsetTrackingEnvironment implements com.rabbitmq.stream.Environment {
        protected final java.util.List<Map.Entry<String, Long>> recordedOffsets =
                new java.util.ArrayList<>();

        @Override
        public com.rabbitmq.stream.StreamCreator streamCreator() {
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
        public com.rabbitmq.stream.StreamStats queryStreamStats(String stream) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void storeOffset(String reference, String stream, long offset) {
            recordedOffsets.add(Map.entry(stream, offset));
        }

        @Override
        public boolean streamExists(String stream) {
            throw new UnsupportedOperationException();
        }

        @Override
        public com.rabbitmq.stream.ProducerBuilder producerBuilder() {
            throw new UnsupportedOperationException();
        }

        @Override
        public com.rabbitmq.stream.ConsumerBuilder consumerBuilder() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void close() {
        }
    }

    private static final class QueryCountingOffsetTrackingEnvironment
            extends OffsetTrackingEnvironment {
        private int queryStatsCalls;

        @Override
        public com.rabbitmq.stream.StreamStats queryStreamStats(String stream) {
            queryStatsCalls++;
            throw new UnsupportedOperationException();
        }
    }

    private static final class ExecutionFailureOffsetTrackingEnvironment
            extends OffsetTrackingEnvironment {
        private final String failingStream;

        private ExecutionFailureOffsetTrackingEnvironment(String failingStream) {
            this.failingStream = failingStream;
        }

        @Override
        public void storeOffset(String reference, String stream, long offset) {
            if (failingStream.equals(stream)) {
                throw new AssertionError("synthetic future execution failure");
            }
            super.storeOffset(reference, stream, offset);
        }
    }

    private static final class CaughtStoreFailureOffsetTrackingEnvironment
            extends OffsetTrackingEnvironment {
        private final String failingStream;

        private CaughtStoreFailureOffsetTrackingEnvironment(String failingStream) {
            this.failingStream = failingStream;
        }

        @Override
        public void storeOffset(String reference, String stream, long offset) {
            if (failingStream.equals(stream)) {
                throw new RuntimeException("synthetic caught store failure");
            }
            super.storeOffset(reference, stream, offset);
        }
    }

    private static final class StoredOffsetWithStatsEnvironment implements Environment {
        private final Map<String, Long> offsets;
        private final Map<String, Long> firstOffsets;

        private StoredOffsetWithStatsEnvironment(
                Map<String, Long> offsets, Map<String, Long> firstOffsets) {
            this.offsets = offsets;
            this.firstOffsets = firstOffsets;
        }

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
            long first = firstOffsets.getOrDefault(stream, 0L);
            return new FixedStreamStats(first);
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
            return new StoredOffsetConsumerBuilder(offsets);
        }

        @Override
        public void close() {
        }
    }

    private static final class StoredOffsetEnvironment implements Environment {
        private final Map<String, Long> offsets;

        private StoredOffsetEnvironment(Map<String, Long> offsets) {
            this.offsets = offsets;
        }

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
            return new StoredOffsetConsumerBuilder(offsets);
        }

        @Override
        public void close() {
        }
    }

    private static final class StoredOffsetConsumerBuilder implements ConsumerBuilder {
        private final Map<String, Long> offsets;
        private String stream;

        private StoredOffsetConsumerBuilder(Map<String, Long> offsets) {
            this.offsets = offsets;
        }

        @Override
        public ConsumerBuilder stream(String stream) {
            this.stream = stream;
            return this;
        }

        @Override
        public ConsumerBuilder superStream(String superStream) {
            return this;
        }

        @Override
        public ConsumerBuilder offset(com.rabbitmq.stream.OffsetSpecification offsetSpecification) {
            return this;
        }

        @Override
        public ConsumerBuilder messageHandler(com.rabbitmq.stream.MessageHandler messageHandler) {
            return this;
        }

        @Override
        public ConsumerBuilder name(String name) {
            return this;
        }

        @Override
        public ConsumerBuilder singleActiveConsumer() {
            return this;
        }

        @Override
        public ConsumerBuilder consumerUpdateListener(
                com.rabbitmq.stream.ConsumerUpdateListener consumerUpdateListener) {
            return this;
        }

        @Override
        public ConsumerBuilder subscriptionListener(
                com.rabbitmq.stream.SubscriptionListener subscriptionListener) {
            return this;
        }

        @Override
        public ConsumerBuilder listeners(com.rabbitmq.stream.Resource.StateListener... listeners) {
            return this;
        }

        @Override
        public ManualTrackingStrategy manualTrackingStrategy() {
            return new NoopManualTrackingStrategy(this);
        }

        @Override
        public AutoTrackingStrategy autoTrackingStrategy() {
            return new NoopAutoTrackingStrategy(this);
        }

        @Override
        public ConsumerBuilder noTrackingStrategy() {
            return this;
        }

        @Override
        public ConsumerBuilder.FilterConfiguration filter() {
            throw new UnsupportedOperationException();
        }

        @Override
        public ConsumerBuilder.FlowConfiguration flow() {
            return new NoopFlowConfiguration(this);
        }

        @Override
        public com.rabbitmq.stream.Consumer build() {
            return new StoredOffsetConsumer(offsets.get(stream));
        }
    }

    private static final class StoredOffsetConsumer implements com.rabbitmq.stream.Consumer {
        private final Long storedOffset;

        private StoredOffsetConsumer(Long storedOffset) {
            this.storedOffset = storedOffset;
        }

        @Override
        public void store(long offset) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void close() {
        }

        @Override
        public long storedOffset() {
            if (storedOffset == null) {
                throw new NoOffsetException("no offset");
            }
            return storedOffset;
        }
    }
}
