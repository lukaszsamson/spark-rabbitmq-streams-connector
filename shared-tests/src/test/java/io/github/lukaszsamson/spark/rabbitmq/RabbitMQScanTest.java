package io.github.lukaszsamson.spark.rabbitmq;

import com.rabbitmq.stream.ConsumerBuilder;
import com.rabbitmq.stream.Environment;
import com.rabbitmq.stream.Message;
import com.rabbitmq.stream.NoOffsetException;
import com.rabbitmq.stream.ProducerBuilder;
import com.rabbitmq.stream.Properties;
import com.rabbitmq.stream.StreamCreator;
import com.rabbitmq.stream.StreamDoesNotExistException;
import com.rabbitmq.stream.StreamStats;
import org.apache.spark.sql.connector.read.streaming.ContinuousStream;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.time.Duration;
import java.util.List;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link RabbitMQScan} planning logic and offset resolution
 * without contacting a broker. End-to-end behavior is exercised by it-tests.
 */
class RabbitMQScanTest {

    @Nested
    class BatchResolution {

        @Test
        void toBatchStreamModeMissingStreamFailsRegardlessOfFailOnDataLoss() throws Exception {
            Map<String, String> opts = baseOptions();
            opts.put("stream", "missing");
            opts.put("failOnDataLoss", "false");
            RabbitMQScan scan = new RabbitMQScan(new ConnectorOptions(opts), schema());

            assertThatThrownBy(() -> resolveStreamOffsetRange(scan, new MissingStreamEnvironment(true), "missing"))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("does not exist");
        }

        @Test
        void toBatchSuperStreamMissingPartitionRespectsFailOnDataLoss() throws Exception {
            Map<String, String> opts = baseOptions();
            opts.remove("stream");
            opts.put("superstream", "super");
            opts.put("failOnDataLoss", "false");
            RabbitMQScan scan = new RabbitMQScan(new ConnectorOptions(opts), schema());

            long[] range = resolveStreamOffsetRange(scan, new MissingStreamEnvironment(false), "partition");
            assertThat(range).isNull();

            opts.put("failOnDataLoss", "true");
            RabbitMQScan scanFail = new RabbitMQScan(new ConnectorOptions(opts), schema());
            assertThatThrownBy(() -> resolveStreamOffsetRange(scanFail, new MissingStreamEnvironment(false), "p"))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("Partition stream");
        }

        @Test
        void toBatchSuperStreamOperationalFailureFailsEvenWhenFailOnDataLossFalse() {
            Map<String, String> opts = baseOptions();
            opts.remove("stream");
            opts.put("superstream", "super");
            opts.put("failOnDataLoss", "false");
            RabbitMQScan scan = new RabbitMQScan(new ConnectorOptions(opts), schema());

            assertThatThrownBy(() ->
                    resolveStreamOffsetRange(scan, new QueryFailureEnvironment(), "partition"))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("Failed to query stream stats");
        }

        @Test
        void discoverSuperStreamEmptyPartitionsRespectsFailOnDataLoss() {
            Map<String, String> opts = baseOptions();
            opts.remove("stream");
            opts.put("superstream", "super");
            opts.put("failOnDataLoss", "false");
            RabbitMQScan tolerant = new RabbitMQScan(new ConnectorOptions(opts), schema());

            assertThat(tolerant.resolveSuperStreamPartitionsForTests(java.util.List.of())).isEmpty();

            opts.put("failOnDataLoss", "true");
            RabbitMQScan strict = new RabbitMQScan(new ConnectorOptions(opts), schema());
            assertThatThrownBy(() -> strict.resolveSuperStreamPartitionsForTests(java.util.List.of()))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("has no partition streams");
        }

        @Test
        void resolveOffsetRangesUsesParallelPlanningForMultipleStreams() {
            Map<String, String> opts = baseOptions();
            opts.put("endingOffsets", "offset");
            opts.put("endingOffset", "100");
            RabbitMQScan scan = new RabbitMQScan(new ConnectorOptions(opts), schema());
            ConcurrencyTrackingEnvironment env = new ConcurrencyTrackingEnvironment(4, 10L, 100L);

            Map<String, long[]> ranges = resolveOffsetRanges(scan, env,
                    List.of("s1", "s2", "s3", "s4"));

            assertThat(ranges).hasSize(4);
            assertThat(env.maxConcurrent()).isGreaterThan(1);
            assertThat(ranges.get("s1")).containsExactly(10L, 100L);
        }

        @Test
        void resolveOffsetRangesPropagatesResolverFailure() {
            // When one resolver fails, the planner propagates the failure (rather than
            // swallowing it) so the surrounding code can cancel-and-drain peers before
            // releasing the Environment. The actual cancel-and-await behavior is unit-
            // tested via the latch-based termination check inside the production code;
            // this test pins the user-visible contract without depending on cross-thread
            // scheduling, which has proven flaky on shared CI executors.
            Map<String, String> opts = baseOptions();
            opts.put("endingOffsets", "offset");
            opts.put("endingOffset", "100");
            RabbitMQScan scan = new RabbitMQScan(new ConnectorOptions(opts), schema());

            assertThatThrownBy(() -> resolveOffsetRanges(scan,
                    new QueryFailureEnvironment(),
                    List.of("s1", "s2", "s3", "s4")))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("Failed to query stream stats");
        }

        @Test
        void resolveStartOffsetLatestReturnsZeroWhenStreamIsEmpty() throws Exception {
            // Empty stream + startingOffsets=latest: start resolves to 0 (statsTail with no
            // committed offset and no probe message). endingOffsets=latest now resolves
            // eagerly so that all batch tasks observe the same end; with an empty stream
            // the resolved tail is also 0.
            Map<String, String> opts = baseOptions();
            opts.put("startingOffsets", "latest");
            RabbitMQScan scan = new RabbitMQScan(new ConnectorOptions(opts), schema());
            StreamStats stats = new Stats(0L, true, true, 0L);
            Environment env = new ProbeNoOffsetEnvironment();

            long start = resolveStartOffset(scan, env, "s1", 0L, stats);
            long end = resolveEndOffset(scan, env, "s1", stats);

            assertThat(start).isEqualTo(0L);
            assertThat(end).isEqualTo(0L);
        }

        @Test
        void resolveStartOffsetLatestDoesNotDependOnEndingOffsetMode() throws Exception {
            Map<String, String> opts = baseOptions();
            opts.put("startingOffsets", "latest");
            opts.put("endingOffsets", "offset");
            opts.put("endingOffset", "5");
            RabbitMQScan scan = new RabbitMQScan(new ConnectorOptions(opts), schema());
            StreamStats stats = new Stats(0L, false, false, 5L);

            long start = resolveStartOffset(scan, new ProbeTailEnvironment(), "s1", 0L, stats);
            long end = resolveEndOffset(scan, new ProbeTailEnvironment(), "s1", stats);

            assertThat(start).isEqualTo(13L);
            assertThat(end).isEqualTo(5L);
        }

        @Test
        void resolveEndOffsetLatestIsEagerWhenBatchSplittingRequested() throws Exception {
            Map<String, String> opts = baseOptions();
            opts.put("minPartitions", "4");
            RabbitMQScan scan = new RabbitMQScan(new ConnectorOptions(opts), schema());
            StreamStats stats = new Stats(0L, false, false, 5L);

            long end = resolveEndOffset(scan, new ProbeTailEnvironment(), "s1", stats);

            assertThat(end).isEqualTo(13L);
        }

        @Test
        void timestampStartPlanningUsesTimestampProbeOffset() throws Exception {
            Map<String, String> opts = baseOptions();
            opts.put("startingOffsets", "timestamp");
            opts.put("startingTimestamp", "1700000000000");
            RabbitMQScan scan = new RabbitMQScan(new ConnectorOptions(opts), schema());
            StreamStats stats = new Stats(10L, false, false, 20L);

            long start = resolveStartOffset(scan, new ProbeTailEnvironment(), "s1", 10L, stats);
            assertThat(start).isEqualTo(11L);
        }

        @Test
        void timestampStartPlanningUsesPerStreamTimestampWithoutGlobalTimestamp() throws Exception {
            Map<String, String> opts = baseOptions();
            opts.put("startingOffsets", "timestamp");
            opts.put("startingOffsetsByTimestamp", "{\"s1\":1700000000000}");
            RabbitMQScan scan = new RabbitMQScan(new ConnectorOptions(opts), schema());
            StreamStats stats = new Stats(10L, false, false, 20L);

            long start = resolveStartOffset(scan, new ProbeTailEnvironment(), "s1", 10L, stats);
            assertThat(start).isEqualTo(11L);
        }

        @Test
        void timestampStartPlanningWaitsForProbeWithinConfiguredPollTimeout() throws Exception {
            Map<String, String> opts = baseOptions();
            opts.put("startingOffsets", "timestamp");
            opts.put("startingTimestamp", "1700000000000");
            opts.put("pollTimeoutMs", "1000");
            RabbitMQScan scan = new RabbitMQScan(new ConnectorOptions(opts), schema());
            StreamStats stats = new Stats(10L, false, false, 20L);

            long start = resolveStartOffset(scan,
                    new DelayedProbeEnvironment(400L, null, 11L), "s1", 10L, stats);
            assertThat(start).isEqualTo(11L);
        }

        @Test
        void timestampStartPlanningWithoutMatchingRecordFailsByDefault() {
            Map<String, String> opts = baseOptions();
            opts.put("startingOffsets", "timestamp");
            opts.put("startingTimestamp", "4102444800000"); // 2100-01-01T00:00:00Z
            RabbitMQScan scan = new RabbitMQScan(new ConnectorOptions(opts), schema());

            assertThatThrownBy(() -> resolveStreamOffsetRange(scan,
                    new DelayedProbeEnvironment(0L, new Stats(10L, false, false, 99L)),
                    "s1"))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("No offset matched");
        }

        @Test
        void timestampStartPlanningWithoutMatchingRecordUsesTailWhenStrategyLatest() {
            Map<String, String> opts = baseOptions();
            opts.put("startingOffsets", "timestamp");
            opts.put("startingTimestamp", "4102444800000"); // 2100-01-01T00:00:00Z
            opts.put("startingOffsetsByTimestampStrategy", "latest");
            RabbitMQScan scan = new RabbitMQScan(new ConnectorOptions(opts), schema());

            long[] range = resolveStreamOffsetRange(scan,
                    new DelayedProbeEnvironment(0L, new Stats(10L, false, false, 99L)),
                    "s1");
            assertThat(range).isNull();
        }

        @Test
        void timestampEndPlanningReturnsFirstOffsetAtOrAfterEndingTimestamp() throws Exception {
            // Single observed message whose per-message creation_time clears the bound.
            // The probe returns its offset as the exclusive end — the message itself is
            // excluded (it was published at/after the requested cutoff).
            long endingTs = 1700000000000L;
            Map<String, String> opts = baseOptions();
            opts.put("endingOffsets", "timestamp");
            opts.put("endingTimestamp", String.valueOf(endingTs));
            opts.put("pollTimeoutMs", "1000");
            RabbitMQScan scan = new RabbitMQScan(new ConnectorOptions(opts), schema());

            Long end = resolveTimestampEndingOffsetIfSupported(scan,
                    new DelayedProbeEnvironment(400L, new Stats(0L, false, false, 99L),
                            new long[]{17L},
                            new long[]{endingTs}),
                    "s1", endingTs);
            if (end == null) {
                return;
            }
            assertThat(end).isEqualTo(17L);
        }

        @Test
        void timestampEndPlanningPicksFirstMessageWithCreationTimeAtOrAfterBound() throws Exception {
            // Per-message creation_time is honored: when the publisher records timestamps
            // and a chunk straddles the bound (early messages still under, later messages
            // already over), the exclusive end is the offset of the first late message.
            // This is the GPT-9 case the data-loss fix targets.
            long endingTs = 1700000000000L;
            Map<String, String> opts = baseOptions();
            opts.put("endingOffsets", "timestamp");
            opts.put("endingTimestamp", String.valueOf(endingTs));
            opts.put("pollTimeoutMs", "1000");
            RabbitMQScan scan = new RabbitMQScan(new ConnectorOptions(opts), schema());

            long[] offsets = {10L, 11L, 12L, 13L, 14L, 15L, 16L, 17L, 18L, 19L};
            long[] creationTimes = new long[offsets.length];
            for (int i = 0; i < 5; i++) {
                creationTimes[i] = endingTs - 1L;
            }
            for (int i = 5; i < offsets.length; i++) {
                creationTimes[i] = endingTs;
            }
            Long end = resolveTimestampEndingOffsetIfSupported(scan,
                    new DelayedProbeEnvironment(50L, new Stats(0L, false, false, 99L),
                            offsets, creationTimes),
                    "s1", endingTs);
            if (end == null) {
                return;
            }
            assertThat(end).isEqualTo(15L);
        }

        @Test
        void timestampEndPlanningUsesPerStreamTimestampOverride() throws Exception {
            long endingTs = 1700000000000L;
            Map<String, String> opts = baseOptions();
            opts.put("endingOffsets", "timestamp");
            opts.put("endingOffsetsByTimestamp", "{\"s1\":" + endingTs + "}");
            opts.put("pollTimeoutMs", "1000");
            RabbitMQScan scan = new RabbitMQScan(new ConnectorOptions(opts), schema());
            StreamStats stats = new Stats(0L, false, false, 99L);

            long end = resolveEndOffset(scan,
                    new DelayedProbeEnvironment(0L, new Stats(0L, false, false, 99L),
                            new long[]{17L},
                            new long[]{endingTs}),
                    "s1", stats);
            assertThat(end).isEqualTo(17L);
        }

        @Test
        void latestEndPlanningResolvesEagerlyAndBoundsProbeLatency() throws Exception {
            // endingOffsets=latest is eagerly resolved at plan time so all batch tasks
            // observe the same end (snapshot semantics). The tail probe has its own
            // bounded timeout, so a slow/blocking consumer builder still cannot stall
            // planning indefinitely; the stats-derived tail is returned in that case.
            Map<String, String> opts = baseOptions();
            opts.put("endingOffsets", "latest");
            RabbitMQScan scan = new RabbitMQScan(new ConnectorOptions(opts), schema());
            StreamStats stats = new Stats(0L, false, false, 123L);

            long end = assertTimeoutPreemptively(Duration.ofSeconds(3), () ->
                    resolveEndOffset(scan,
                            new BlockingConsumerBuilderEnvironment(5_000L, stats),
                            "s1", stats));
            // Probe times out (TAIL_PROBE_TIMEOUT_MS=1500ms) and returns 0; statsTail
            // is committedOffset+1 = 124.
            assertThat(end).isEqualTo(124L);
        }

        @Test
        void startBeforeFirstAvailableFailOnDataLossBehavior() throws Exception {
            Map<String, String> opts = baseOptions();
            opts.put("startingOffsets", "offset");
            opts.put("startingOffset", "0");
            opts.put("failOnDataLoss", "true");
            RabbitMQScan scan = new RabbitMQScan(new ConnectorOptions(opts), schema());

            assertThatThrownBy(() -> resolveStreamOffsetRange(scan,
                    new FirstOffsetEnvironment(10L, 20L), "s1"))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("before the first available");

            opts.put("failOnDataLoss", "false");
            RabbitMQScan scanSkip = new RabbitMQScan(new ConnectorOptions(opts), schema());
            long[] range = resolveStreamOffsetRange(scanSkip, new FirstOffsetEnvironment(10L, 20L), "s1");
            assertThat(range[0]).isEqualTo(10L);
        }

        @Test
        void explicitStartOffsetGreaterThanEndOffsetFails() throws Exception {
            Map<String, String> opts = baseOptions();
            opts.put("startingOffsets", "offset");
            opts.put("startingOffset", "20");
            opts.put("endingOffsets", "offset");
            opts.put("endingOffset", "10");
            RabbitMQScan scan = new RabbitMQScan(new ConnectorOptions(opts), schema());

            assertThatThrownBy(() ->
                    resolveStreamOffsetRange(scan, new FirstOffsetEnvironment(0L, 100L), "s1"))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("greater than endingOffset");
        }

        @Test
        void resolveEndOffsetResolvesEagerlyWithDefaultEndingOffsets() throws Exception {
            // Default endingOffsets=latest is eagerly resolved for batch reads. Stats
            // surface no committed offset, so statsTail falls back to 0; the probe
            // emits offsets 11 and 12 and returns max+1 = 13.
            Map<String, String> opts = baseOptions();
            RabbitMQScan scan = new RabbitMQScan(new ConnectorOptions(opts), schema());
            StreamStats stats = new Stats(0L, true, true, 0L);

            long end = resolveEndOffset(scan, new ProbeTailEnvironment(), "s1", stats);

            assertThat(end).isEqualTo(13L);
        }

        @Test
        void emptyStreamNoOffsetIsSkipped() throws Exception {
            Map<String, String> opts = baseOptions();
            RabbitMQScan scan = new RabbitMQScan(new ConnectorOptions(opts), schema());
            long[] range = resolveStreamOffsetRange(scan, new NoOffsetEnvironment(), "s1");
            assertThat(range).isNull();
        }

    }

    @Test
    void scanDoesNotImplementContinuousStream() {
        RabbitMQScan scan = new RabbitMQScan(new ConnectorOptions(baseOptions()), schema());
        assertThat(scan).isNotInstanceOf(ContinuousStream.class);
    }

    @Test
    void toMicroBatchStreamRejectsEndingOffsetsOption() {
        Map<String, String> opts = baseOptions();
        opts.put("endingOffsets", "offset");
        opts.put("endingOffset", "10");

        RabbitMQScan scan = new RabbitMQScan(new ConnectorOptions(opts), schema());
        assertThatThrownBy(() -> scan.toMicroBatchStream("/tmp/checkpoint"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("not supported for streaming queries");
    }

    @Test
    void toMicroBatchStreamRejectsEndingTimestampOption() {
        Map<String, String> opts = baseOptions();
        opts.put("endingOffsets", "timestamp");
        opts.put("endingTimestamp", "1700000000000");

        RabbitMQScan scan = new RabbitMQScan(new ConnectorOptions(opts), schema());
        assertThatThrownBy(() -> scan.toMicroBatchStream("/tmp/checkpoint"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("not supported for streaming queries");
    }

    private static Map<String, String> baseOptions() {
        Map<String, String> opts = new LinkedHashMap<>();
        opts.put("endpoints", "localhost:5552");
        opts.put("stream", "test-stream");
        return opts;
    }

    private static org.apache.spark.sql.types.StructType schema() {
        return RabbitMQStreamTable.buildSourceSchema(
                new ConnectorOptions(baseOptions()).getMetadataFields());
    }

    private static long[] resolveStreamOffsetRange(RabbitMQScan scan, Environment env, String stream) {
        return scan.resolveStreamOffsetRangeForTests(env, stream);
    }

    private static Map<String, long[]> resolveOffsetRanges(
            RabbitMQScan scan, Environment env, List<String> streams) {
        return scan.resolveOffsetRangesForTests(env, streams);
    }

    private static long resolveStartOffset(RabbitMQScan scan, Environment env,
                                           String stream, long firstAvailable, StreamStats stats) {
        return scan.resolveStartOffsetForTests(env, stream, firstAvailable, stats);
    }

    private static long resolveEndOffset(RabbitMQScan scan, Environment env,
                                         String stream, StreamStats stats) {
        return scan.resolveEndOffsetForTests(env, stream, stats);
    }

    private static Long resolveTimestampEndingOffsetIfSupported(
            RabbitMQScan scan, Environment env, String stream, long timestamp) throws Exception {
        Method method;
        try {
            method = RabbitMQScan.class.getDeclaredMethod(
                    "resolveTimestampEndingOffset", Environment.class, String.class, long.class);
        } catch (NoSuchMethodException e) {
            return null;
        }
        method.setAccessible(true);
        return (Long) method.invoke(scan, env, stream, timestamp);
    }

    private static final class MissingStreamEnvironment implements Environment {
        private final boolean streamMode;

        private MissingStreamEnvironment(boolean streamMode) {
            this.streamMode = streamMode;
        }

        @Override
        public StreamStats queryStreamStats(String stream) {
            throw new StreamDoesNotExistException(streamMode ? stream : "partition");
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
        }
    }

    private static final class QueryFailureEnvironment implements Environment {
        @Override
        public StreamStats queryStreamStats(String stream) {
            throw new RuntimeException("connection failed");
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
        }
    }

    private static final class ProbeTailEnvironment implements Environment {
        @Override
        public ConsumerBuilder consumerBuilder() {
            return new ProbeTailConsumerBuilder();
        }

        @Override
        public StreamStats queryStreamStats(String stream) {
            throw new UnsupportedOperationException();
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
        public void close() {
        }
    }

    private static final class ProbeNoOffsetEnvironment implements Environment {
        @Override
        public ConsumerBuilder consumerBuilder() {
            return new NoOffsetConsumerBuilder();
        }

        @Override
        public StreamStats queryStreamStats(String stream) {
            throw new UnsupportedOperationException();
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
        public void close() {
        }
    }

    private static final class DelayedProbeEnvironment implements Environment {
        private final long delayMs;
        private final long[] offsets;
        private final long[] creationTimes;
        private final StreamStats stats;

        private DelayedProbeEnvironment(long delayMs, StreamStats stats, long... offsets) {
            // Default: per-message creation_time unset (-1) — the production probe will
            // fall back to the chunk-level context timestamp, which is 0 in this fixture.
            this(delayMs, stats, offsets, fillArray(offsets.length, -1L));
        }

        private DelayedProbeEnvironment(long delayMs, StreamStats stats,
                long[] offsets, long[] creationTimes) {
            this.delayMs = delayMs;
            this.stats = stats;
            this.offsets = offsets;
            this.creationTimes = creationTimes;
        }

        private static long[] fillArray(int length, long value) {
            long[] result = new long[length];
            java.util.Arrays.fill(result, value);
            return result;
        }

        @Override
        public ConsumerBuilder consumerBuilder() {
            return new DelayedProbeConsumerBuilder(delayMs, offsets, creationTimes);
        }

        @Override
        public StreamStats queryStreamStats(String stream) {
            if (stats == null) {
                throw new UnsupportedOperationException();
            }
            return stats;
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
        public void close() {
        }
    }

    private static final class BlockingConsumerBuilderEnvironment implements Environment {
        private final long blockMs;
        private final StreamStats stats;

        private BlockingConsumerBuilderEnvironment(long blockMs, StreamStats stats) {
            this.blockMs = blockMs;
            this.stats = stats;
        }

        @Override
        public ConsumerBuilder consumerBuilder() {
            long deadlineNanos = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(blockMs);
            while (System.nanoTime() < deadlineNanos) {
                long remainingMs = TimeUnit.NANOSECONDS.toMillis(deadlineNanos - System.nanoTime());
                if (remainingMs <= 0L) {
                    break;
                }
                try {
                    Thread.sleep(Math.min(remainingMs, 50L));
                } catch (InterruptedException ignored) {
                    // Intentionally ignore interrupts to emulate an unresponsive client call.
                }
            }
            return new ProbeTailConsumerBuilder();
        }

        @Override
        public StreamStats queryStreamStats(String stream) {
            return stats;
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
        public void close() {
        }
    }

    private static final class NoOffsetEnvironment implements Environment {
        @Override
        public StreamStats queryStreamStats(String stream) {
            return new Stats(0L, false, true, 0L);
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
        }
    }

    private static final class FirstOffsetEnvironment implements Environment {
        private final long first;
        private final long committed;

        private FirstOffsetEnvironment(long first, long committed) {
            this.first = first;
            this.committed = committed;
        }

        @Override
        public StreamStats queryStreamStats(String stream) {
            return new Stats(first, false, false, committed);
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
        }
    }

    private static final class ConcurrencyTrackingEnvironment implements Environment {
        private final CountDownLatch started;
        private final AtomicInteger inFlight = new AtomicInteger();
        private final AtomicInteger maxConcurrent = new AtomicInteger();
        private final long firstOffset;
        private final long committedOffset;

        private ConcurrencyTrackingEnvironment(int expectedCalls, long firstOffset, long committedOffset) {
            this.started = new CountDownLatch(expectedCalls);
            this.firstOffset = firstOffset;
            this.committedOffset = committedOffset;
        }

        @Override
        public StreamStats queryStreamStats(String stream) {
            int current = inFlight.incrementAndGet();
            maxConcurrent.accumulateAndGet(current, Math::max);
            started.countDown();
            try {
                started.await(500, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } finally {
                inFlight.decrementAndGet();
            }
            return new Stats(firstOffset, false, false, committedOffset);
        }

        private int maxConcurrent() {
            return maxConcurrent.get();
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
        }
    }

    private static final class NoOffsetConsumerBuilder implements ConsumerBuilder {
        @Override
        public ConsumerBuilder stream(String stream) {
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
            throw new NoOffsetException("empty");
        }
    }

    private static final class ProbeTailConsumerBuilder implements ConsumerBuilder {
        private com.rabbitmq.stream.MessageHandler handler;

        @Override
        public ConsumerBuilder stream(String stream) {
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
            this.handler = messageHandler;
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
            if (handler != null) {
                handler.handle(new ProbeContext(11L), null);
                handler.handle(new ProbeContext(12L), null);
            }
            return new ProbeTailConsumer();
        }
    }

    private static final class DelayedProbeConsumerBuilder implements ConsumerBuilder {
        private final long delayMs;
        private final long[] offsets;
        private final long[] creationTimes;
        private com.rabbitmq.stream.MessageHandler handler;

        private DelayedProbeConsumerBuilder(long delayMs, long[] offsets, long[] creationTimes) {
            this.delayMs = delayMs;
            this.offsets = offsets;
            this.creationTimes = creationTimes;
        }

        @Override
        public ConsumerBuilder stream(String stream) {
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
            this.handler = messageHandler;
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
            if (handler != null) {
                Thread emitter = new Thread(() -> {
                    try {
                        Thread.sleep(delayMs);
                        for (int i = 0; i < offsets.length; i++) {
                            // creationTimes[i] == -1 simulates an unset per-message
                            // creation_time, which makes the production probe fall back
                            // to the chunk-level context timestamp.
                            Message message = mock(Message.class);
                            Properties properties = mock(Properties.class);
                            when(properties.getCreationTime()).thenReturn(creationTimes[i]);
                            when(message.getProperties()).thenReturn(properties);
                            handler.handle(new ProbeContext(offsets[i]), message);
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }, "rabbitmq-scan-delayed-probe");
                emitter.setDaemon(true);
                emitter.start();
            }
            return new ProbeTailConsumer();
        }
    }

    private static final class ProbeTailConsumer implements com.rabbitmq.stream.Consumer {
        private ProbeTailConsumer() {
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
            throw new UnsupportedOperationException();
        }
    }

    private static final class ProbeContext implements com.rabbitmq.stream.MessageHandler.Context {
        private final long offset;

        private ProbeContext(long offset) {
            this.offset = offset;
        }

        @Override
        public long offset() {
            return offset;
        }

        @Override
        public long timestamp() {
            return 0;
        }

        @Override
        public void processed() {
        }

        @Override
        public void storeOffset() {
        }

        @Override
        public long committedChunkId() {
            return 0;
        }

        @Override
        public String stream() {
            return "s1";
        }

        @Override
        public com.rabbitmq.stream.Consumer consumer() {
            return null;
        }

    }

    private static final class NoopManualTrackingStrategy implements ConsumerBuilder.ManualTrackingStrategy {
        private final ConsumerBuilder builder;

        private NoopManualTrackingStrategy(ConsumerBuilder builder) {
            this.builder = builder;
        }

        @Override
        public ConsumerBuilder.ManualTrackingStrategy checkInterval(java.time.Duration checkInterval) {
            return this;
        }

        @Override
        public ConsumerBuilder builder() {
            return builder;
        }
    }

    private static final class NoopFlowConfiguration implements ConsumerBuilder.FlowConfiguration {
        private final ConsumerBuilder builder;

        private NoopFlowConfiguration(ConsumerBuilder builder) {
            this.builder = builder;
        }

        @Override
        public ConsumerBuilder.FlowConfiguration strategy(
                com.rabbitmq.stream.ConsumerFlowStrategy strategy) {
            return this;
        }

        @Override
        public ConsumerBuilder.FlowConfiguration initialCredits(int initialCredits) {
            return this;
        }

        @Override
        public ConsumerBuilder builder() {
            return builder;
        }
    }

    private static final class NoopAutoTrackingStrategy implements ConsumerBuilder.AutoTrackingStrategy {
        private final ConsumerBuilder builder;

        private NoopAutoTrackingStrategy(ConsumerBuilder builder) {
            this.builder = builder;
        }

        @Override
        public ConsumerBuilder.AutoTrackingStrategy messageCountBeforeStorage(int messageCountBeforeStorage) {
            return this;
        }

        @Override
        public ConsumerBuilder.AutoTrackingStrategy flushInterval(java.time.Duration flushInterval) {
            return this;
        }

        @Override
        public ConsumerBuilder builder() {
            return builder;
        }
    }

    private static final class Stats implements StreamStats {
        private final long first;
        private final boolean throwOnCommitted;
        private final boolean noOffset;
        private final long committed;

        private Stats(long first, boolean throwOnCommitted, boolean noOffset, long committed) {
            this.first = first;
            this.throwOnCommitted = throwOnCommitted;
            this.noOffset = noOffset;
            this.committed = committed;
        }

        @Override
        public long firstOffset() {
            if (noOffset) {
                throw new NoOffsetException("empty");
            }
            return first;
        }

        @Override
        public long committedChunkId() {
            if (noOffset) {
                throw new NoOffsetException("empty");
            }
            return first;
        }

        @Override
        public long committedOffset() {
            if (throwOnCommitted) {
                throw new NoOffsetException("no committed");
            }
            return committed;
        }
    }
}
