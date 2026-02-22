package io.github.lukaszsamson.spark.rabbitmq;

import com.rabbitmq.stream.ConsumerBuilder;
import com.rabbitmq.stream.Environment;
import com.rabbitmq.stream.NoOffsetException;
import com.rabbitmq.stream.ProducerBuilder;
import com.rabbitmq.stream.StreamCreator;
import com.rabbitmq.stream.StreamDoesNotExistException;
import com.rabbitmq.stream.StreamStats;
import org.apache.spark.sql.connector.read.streaming.ContinuousStream;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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
        void resolveStartOffsetLatestEqualsResolvedEndWhenNoData() throws Exception {
            Map<String, String> opts = baseOptions();
            opts.put("startingOffsets", "latest");
            RabbitMQScan scan = new RabbitMQScan(new ConnectorOptions(opts), schema());
            StreamStats stats = new Stats(0L, true, true, 0L);
            Environment env = new ProbeNoOffsetEnvironment();

            long start = resolveStartOffset(scan, env, "s1", 0L, stats);
            long end = resolveEndOffset(scan, env, "s1", stats);

            assertThat(start).isEqualTo(end);
            assertThat(start).isEqualTo(0L);
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
        void timestampEndPlanningWaitsForProbeWithinConfiguredPollTimeoutWhenSupported() throws Exception {
            Map<String, String> opts = baseOptions();
            opts.put("endingOffsets", "timestamp");
            opts.put("endingTimestamp", "1700000000000");
            opts.put("pollTimeoutMs", "1000");
            RabbitMQScan scan = new RabbitMQScan(new ConnectorOptions(opts), schema());

            Long end = resolveTimestampEndingOffsetIfSupported(scan,
                    new DelayedProbeEnvironment(400L, new Stats(0L, false, false, 99L), 17L),
                    "s1", 1700000000000L);
            if (end == null) {
                return;
            }
            assertThat(end).isEqualTo(17L);
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
        void resolveEndOffsetPrefersProbedTailOffsetWhenHigher() throws Exception {
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
        private final StreamStats stats;

        private DelayedProbeEnvironment(long delayMs, StreamStats stats, long... offsets) {
            this.delayMs = delayMs;
            this.stats = stats;
            this.offsets = offsets;
        }

        @Override
        public ConsumerBuilder consumerBuilder() {
            return new DelayedProbeConsumerBuilder(delayMs, offsets);
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
            throw new UnsupportedOperationException();
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
            throw new UnsupportedOperationException();
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
        private com.rabbitmq.stream.MessageHandler handler;

        private DelayedProbeConsumerBuilder(long delayMs, long[] offsets) {
            this.delayMs = delayMs;
            this.offsets = offsets;
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
            throw new UnsupportedOperationException();
        }

        @Override
        public com.rabbitmq.stream.Consumer build() {
            if (handler != null) {
                Thread emitter = new Thread(() -> {
                    try {
                        Thread.sleep(delayMs);
                        for (long offset : offsets) {
                            handler.handle(new ProbeContext(offset), null);
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
