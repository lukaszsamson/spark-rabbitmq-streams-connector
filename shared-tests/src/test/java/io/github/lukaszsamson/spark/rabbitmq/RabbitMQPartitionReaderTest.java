package io.github.lukaszsamson.spark.rabbitmq;

import com.rabbitmq.stream.Message;
import com.rabbitmq.stream.OffsetSpecification;
import com.rabbitmq.stream.Properties;
import com.rabbitmq.stream.codec.QpidProtonCodec;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.util.LongAccumulator;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.lang.reflect.Method;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for the push-to-pull bridging logic in RabbitMQPartitionReader.
 *
 * <p>These tests exercise the offset filtering, de-duplication, and timeout
 * behavior without requiring a real RabbitMQ broker. The actual consumer
 * integration is tested in the it-tests module.
 */
class RabbitMQPartitionReaderTest {

    private static final QpidProtonCodec CODEC = new QpidProtonCodec();

    // ======================================================================
    // InputPartition tests
    // ======================================================================

    @Nested
    class InputPartitionTest {

        @Test
        void partitionHoldsOffsetRange() {
            ConnectorOptions opts = minimalOptions();
            RabbitMQInputPartition partition = new RabbitMQInputPartition(
                    "test-stream", 10, 100, opts);

            assertThat(partition.getStream()).isEqualTo("test-stream");
            assertThat(partition.getStartOffset()).isEqualTo(10);
            assertThat(partition.getEndOffset()).isEqualTo(100);
            assertThat(partition.getOptions()).isSameAs(opts);
            assertThat(partition.isUseConfiguredStartingOffset()).isFalse();
        }

        @Test
        void partitionCanMarkConfiguredStartingOffset() {
            ConnectorOptions opts = minimalOptions();
            RabbitMQInputPartition partition = new RabbitMQInputPartition(
                    "test-stream", 10, 100, opts, true);

            assertThat(partition.isUseConfiguredStartingOffset()).isTrue();
        }

        @Test
        void partitionIsSerializable() throws Exception {
            ConnectorOptions opts = minimalOptions();
            RabbitMQInputPartition partition = new RabbitMQInputPartition(
                    "test-stream", 0, 1000, opts);

            // Serialize and deserialize
            var bos = new java.io.ByteArrayOutputStream();
            var oos = new java.io.ObjectOutputStream(bos);
            oos.writeObject(partition);
            oos.close();

            var bis = new java.io.ByteArrayInputStream(bos.toByteArray());
            var ois = new java.io.ObjectInputStream(bis);
            RabbitMQInputPartition deserialized = (RabbitMQInputPartition) ois.readObject();

            assertThat(deserialized.getStream()).isEqualTo("test-stream");
            assertThat(deserialized.getStartOffset()).isEqualTo(0);
            assertThat(deserialized.getEndOffset()).isEqualTo(1000);
        }

        @Test
        void toStringIsReadable() {
            RabbitMQInputPartition partition = new RabbitMQInputPartition(
                    "orders", 42, 100, minimalOptions());
            assertThat(partition.toString())
                    .contains("orders")
                    .contains("42")
                    .contains("100");
        }

        @Test
        void locationForStreamDoesNotReturnSyntheticPreferredLocations() {
            assertThat(RabbitMQInputPartition.locationForStream("orders-1")).isEmpty();
        }
    }

    // ======================================================================
    // Queue-based push-to-pull bridge tests
    // ======================================================================

    @Nested
    class QueueBridge {

        @Test
        void nextFastPathTerminatesWhenAlreadyAtEnd() throws Exception {
            RabbitMQInputPartition partition = new RabbitMQInputPartition(
                    "test-stream", 0, 10, minimalOptions());
            RabbitMQPartitionReader reader = new RabbitMQPartitionReader(partition, partition.getOptions());

            setPrivateField(reader, "lastEmittedOffset", 9L);

            assertThat(reader.next()).isFalse();
        }

        @Test
        void nextEmitsMessageAtOffsetZero() throws Exception {
            RabbitMQInputPartition partition = new RabbitMQInputPartition(
                    "test-stream", 0, 2, minimalOptions());
            RabbitMQPartitionReader reader = new RabbitMQPartitionReader(partition, partition.getOptions());

            BlockingQueue<RabbitMQPartitionReader.QueuedMessage> queue = new LinkedBlockingQueue<>();
            queue.add(new RabbitMQPartitionReader.QueuedMessage(
                    CODEC.messageBuilder().addData("a".getBytes()).build(),
                    0L, 0L, new NoopContext()));
            queue.add(new RabbitMQPartitionReader.QueuedMessage(
                    CODEC.messageBuilder().addData("b".getBytes()).build(),
                    1L, 0L, new NoopContext()));

            setPrivateField(reader, "queue", queue);
            setPrivateField(reader, "consumer", new NoopConsumer());

            assertThat(reader.next()).isTrue();
            assertThat(reader.get().getLong(2)).isEqualTo(0L);
            assertThat(reader.next()).isTrue();
            assertThat(reader.get().getLong(2)).isEqualTo(1L);
            assertThat(reader.next()).isFalse();
        }

        @Test
        void nextTimeoutMessageIncludesOffsets() throws Exception {
            Map<String, String> opts = new LinkedHashMap<>();
            opts.put("endpoints", "localhost:5552");
            opts.put("stream", "test-stream");
            opts.put("pollTimeoutMs", "5");
            opts.put("maxWaitMs", "10");

            RabbitMQInputPartition partition = new RabbitMQInputPartition(
                    "test-stream", 0, 10, new ConnectorOptions(opts));
            RabbitMQPartitionReader reader = new RabbitMQPartitionReader(partition, partition.getOptions());

            setPrivateField(reader, "consumer", new NoopConsumer());
            setPrivateField(reader, "queue", new LinkedBlockingQueue<>());

            assertThatThrownBy(reader::next)
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining("Last emitted offset")
                    .hasMessageContaining("target end offset");
        }

        @Test
        void nextTimesOutAtExactMaxWaitMs() throws Exception {
            Map<String, String> opts = new LinkedHashMap<>();
            opts.put("endpoints", "localhost:5552");
            opts.put("stream", "test-stream");
            opts.put("pollTimeoutMs", "5");
            opts.put("maxWaitMs", "5");

            RabbitMQInputPartition partition = new RabbitMQInputPartition(
                    "test-stream", 0, 10, new ConnectorOptions(opts));
            RabbitMQPartitionReader reader = new RabbitMQPartitionReader(partition, partition.getOptions());

            setPrivateField(reader, "consumer", new NoopConsumer());
            setPrivateField(reader, "queue", new LinkedBlockingQueue<>());

            assertThatThrownBy(reader::next)
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining("Timed out waiting for messages");
        }

        @Test
        void nextFailsFastWhenConsumerAlreadyClosed() throws Exception {
            Map<String, String> opts = new LinkedHashMap<>();
            opts.put("endpoints", "localhost:5552");
            opts.put("stream", "test-stream");
            opts.put("pollTimeoutMs", "5");
            opts.put("maxWaitMs", "30000");

            RabbitMQInputPartition partition = new RabbitMQInputPartition(
                    "test-stream", 0, 10, new ConnectorOptions(opts));
            RabbitMQPartitionReader reader = new RabbitMQPartitionReader(partition, partition.getOptions());

            setPrivateField(reader, "consumer", new NoopConsumer());
            setPrivateField(reader, "queue", new LinkedBlockingQueue<>());
            setPrivateField(reader, "consumerClosed", new java.util.concurrent.atomic.AtomicBoolean(true));

            long startNanos = System.nanoTime();
            assertThatThrownBy(reader::next)
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining("closed before reaching target end offset");
            long elapsedMs = java.util.concurrent.TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
            assertThat(elapsedMs).isLessThan(500L);
        }

        @Test
        void nextDetectsConsumerClosurePromptlyDuringLongPoll() throws Exception {
            Map<String, String> opts = new LinkedHashMap<>();
            opts.put("endpoints", "localhost:5552");
            opts.put("stream", "test-stream");
            opts.put("pollTimeoutMs", "30000");
            opts.put("maxWaitMs", "30000");

            RabbitMQInputPartition partition = new RabbitMQInputPartition(
                    "test-stream", 0, 10, new ConnectorOptions(opts));
            RabbitMQPartitionReader reader = new RabbitMQPartitionReader(partition, partition.getOptions());

            java.util.concurrent.atomic.AtomicBoolean closed = new java.util.concurrent.atomic.AtomicBoolean(false);
            setPrivateField(reader, "consumer", new NoopConsumer());
            setPrivateField(reader, "queue", new LinkedBlockingQueue<>());
            setPrivateField(reader, "consumerClosed", closed);

            java.util.concurrent.atomic.AtomicReference<Throwable> failure =
                    new java.util.concurrent.atomic.AtomicReference<>();
            long startNanos = System.nanoTime();
            Thread worker = new Thread(() -> {
                try {
                    reader.next();
                } catch (Throwable t) {
                    failure.set(t);
                }
            }, "partition-reader-closure-test");
            worker.start();

            Thread.sleep(50L);
            closed.set(true);
            worker.join(2_000L);
            if (worker.isAlive()) {
                worker.interrupt();
            }

            long elapsedMs = java.util.concurrent.TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
            assertThat(worker.isAlive()).isFalse();
            assertThat(failure.get())
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining("closed before reaching target end offset");
            assertThat(elapsedMs).isLessThan(1_000L);
        }

        @Test
        void nextReturnsFalsePromptlyWhenReaderClosedDuringLongPoll() throws Exception {
            Map<String, String> opts = new LinkedHashMap<>();
            opts.put("endpoints", "localhost:5552");
            opts.put("stream", "test-stream");
            opts.put("pollTimeoutMs", "30000");
            opts.put("maxWaitMs", "30000");

            RabbitMQInputPartition partition = new RabbitMQInputPartition(
                    "test-stream", 0, 10, new ConnectorOptions(opts));
            RabbitMQPartitionReader reader = new RabbitMQPartitionReader(partition, partition.getOptions());

            setPrivateField(reader, "consumer", new NoopConsumer());
            setPrivateField(reader, "queue", new LinkedBlockingQueue<>());

            java.util.concurrent.atomic.AtomicReference<Boolean> result =
                    new java.util.concurrent.atomic.AtomicReference<>();
            java.util.concurrent.atomic.AtomicReference<Throwable> failure =
                    new java.util.concurrent.atomic.AtomicReference<>();
            long startNanos = System.nanoTime();
            Thread worker = new Thread(() -> {
                try {
                    result.set(reader.next());
                } catch (Throwable t) {
                    failure.set(t);
                }
            }, "partition-reader-close-test");
            worker.start();

            Thread.sleep(50L);
            reader.close();
            worker.join(2_000L);
            if (worker.isAlive()) {
                worker.interrupt();
            }

            long elapsedMs = java.util.concurrent.TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
            assertThat(worker.isAlive()).isFalse();
            assertThat(failure.get()).isNull();
            assertThat(result.get()).isFalse();
            assertThat(elapsedMs).isLessThan(1_000L);
        }

        @Test
        void finishedFieldIsVolatileForCrossThreadVisibility() throws Exception {
            Field finishedField = findField(RabbitMQPartitionReader.class, "finished");
            assertThat(Modifier.isVolatile(finishedField.getModifiers())).isTrue();
        }

        @Test
        void nextTimestampInitialSplitWithoutInRangeDataTerminatesInsteadOfTimingOut() throws Exception {
            Map<String, String> opts = new LinkedHashMap<>();
            opts.put("endpoints", "localhost:5552");
            opts.put("stream", "test-stream");
            opts.put("startingOffsets", "timestamp");
            opts.put("startingTimestamp", "1700000000000");
            opts.put("pollTimeoutMs", "5");
            opts.put("maxWaitMs", "5");

            RabbitMQInputPartition partition = new RabbitMQInputPartition(
                    "test-stream", 0, 10, new ConnectorOptions(opts), true);
            RabbitMQPartitionReader reader = new RabbitMQPartitionReader(partition, partition.getOptions());

            setPrivateField(reader, "consumer", new NoopConsumer());
            setPrivateField(reader, "queue", new LinkedBlockingQueue<>());

            // Regression: initial timestamp-seek split can legitimately have no in-range rows.
            // It must terminate cleanly instead of failing the whole batch with timeout.
            assertThat(reader.next()).isFalse();
        }

        @Test
        void nextWithBrokerFilterCanTerminateAfterObservedInRangeProgressAndInactivity() throws Exception {
            Map<String, String> opts = new LinkedHashMap<>();
            opts.put("endpoints", "localhost:5552");
            opts.put("stream", "test-stream");
            opts.put("filterValues", "alpha");
            opts.put("filterValuePath", "application_properties.region");
            opts.put("pollTimeoutMs", "5");
            opts.put("maxWaitMs", "5");

            RabbitMQInputPartition partition = new RabbitMQInputPartition(
                    "test-stream", 0, 40, new ConnectorOptions(opts));
            RabbitMQPartitionReader reader = new RabbitMQPartitionReader(partition, partition.getOptions());

            setPrivateField(reader, "consumer", new NoopConsumer());
            setPrivateField(reader, "queue", new LinkedBlockingQueue<>());
            setPrivateField(reader, "lastObservedOffset", 34L);

            // Filtered bounded reads can legitimately stop before endOffset if no additional
            // matching records arrive after in-range progress has been observed.
            assertThat(reader.next()).isFalse();
        }

        @Test
        void nextMaxWaitIncludesTailProbeWallClockTime() throws Exception {
            Map<String, String> opts = new LinkedHashMap<>();
            opts.put("endpoints", "localhost:5552");
            opts.put("stream", "test-stream");
            opts.put("filterValues", "alpha");
            opts.put("filterValuePath", "application_properties.region");
            opts.put("pollTimeoutMs", "1");
            opts.put("maxWaitMs", "40");

            RabbitMQInputPartition partition = new RabbitMQInputPartition(
                    "test-stream", 0, 100, new ConnectorOptions(opts));
            RabbitMQPartitionReader reader = new RabbitMQPartitionReader(partition, partition.getOptions());

            setPrivateField(reader, "consumer", new NoopConsumer());
            setPrivateField(reader, "queue", new LinkedBlockingQueue<>());
            setPrivateField(reader, "lastObservedOffset", 0L);

            com.rabbitmq.stream.Environment env = mock(com.rabbitmq.stream.Environment.class);
            com.rabbitmq.stream.StreamStats stats = mock(com.rabbitmq.stream.StreamStats.class);
            com.rabbitmq.stream.ConsumerBuilder builder = mock(com.rabbitmq.stream.ConsumerBuilder.class);
            com.rabbitmq.stream.ConsumerBuilder.FlowConfiguration flow = mock(
                    com.rabbitmq.stream.ConsumerBuilder.FlowConfiguration.class);
            com.rabbitmq.stream.Consumer probe = mock(com.rabbitmq.stream.Consumer.class);

            when(env.queryStreamStats(anyString())).thenReturn(stats);
            when(stats.committedOffset()).thenReturn(0L);

            when(env.consumerBuilder()).thenReturn(builder);
            when(builder.stream(anyString())).thenReturn(builder);
            when(builder.offset(any(OffsetSpecification.class))).thenReturn(builder);
            when(builder.noTrackingStrategy()).thenReturn(builder);
            when(builder.messageHandler(any(com.rabbitmq.stream.MessageHandler.class))).thenReturn(builder);
            when(builder.flow()).thenReturn(flow);
            when(flow.strategy(any(com.rabbitmq.stream.ConsumerFlowStrategy.class))).thenReturn(flow);
            when(flow.builder()).thenReturn(builder);
            when(builder.build()).thenReturn(probe);

            setPrivateField(reader, "environment", env);

            long startNanos = System.nanoTime();
            assertThat(reader.next()).isFalse();
            long elapsedMs = java.util.concurrent.TimeUnit.NANOSECONDS.toMillis(
                    System.nanoTime() - startNanos);

            assertThat(elapsedMs).isLessThan(500L);
        }

        @Test
        void nextDedupSkipsDuplicateOffsets() throws Exception {
            RabbitMQInputPartition partition = new RabbitMQInputPartition(
                    "test-stream", 0, 3, minimalOptions());
            RabbitMQPartitionReader reader = new RabbitMQPartitionReader(partition, partition.getOptions());

            BlockingQueue<RabbitMQPartitionReader.QueuedMessage> queue = new LinkedBlockingQueue<>();
            queue.add(new RabbitMQPartitionReader.QueuedMessage(
                    CODEC.messageBuilder().addData("a".getBytes()).build(),
                    1L, 0L, new NoopContext()));
            queue.add(new RabbitMQPartitionReader.QueuedMessage(
                    CODEC.messageBuilder().addData("b".getBytes()).build(),
                    1L, 0L, new NoopContext()));
            queue.add(new RabbitMQPartitionReader.QueuedMessage(
                    CODEC.messageBuilder().addData("c".getBytes()).build(),
                    2L, 0L, new NoopContext()));

            setPrivateField(reader, "queue", queue);
            setPrivateField(reader, "consumer", new NoopConsumer());

            assertThat(reader.next()).isTrue();
            assertThat(reader.get().getLong(2)).isEqualTo(1L);
            assertThat(reader.next()).isTrue();
            assertThat(reader.get().getLong(2)).isEqualTo(2L);
            assertThat(reader.next()).isFalse();
        }

        @Test
        void nextSkipsMessagesBelowStartOffset() throws Exception {
            RabbitMQInputPartition partition = new RabbitMQInputPartition(
                    "test-stream", 5, 6, minimalOptions());
            RabbitMQPartitionReader reader = new RabbitMQPartitionReader(partition, partition.getOptions());

            BlockingQueue<RabbitMQPartitionReader.QueuedMessage> queue = new LinkedBlockingQueue<>();
            queue.add(new RabbitMQPartitionReader.QueuedMessage(
                    CODEC.messageBuilder().addData("a".getBytes()).build(),
                    3L, 0L, new NoopContext()));
            queue.add(new RabbitMQPartitionReader.QueuedMessage(
                    CODEC.messageBuilder().addData("b".getBytes()).build(),
                    5L, 0L, new NoopContext()));

            setPrivateField(reader, "queue", queue);
            setPrivateField(reader, "consumer", new NoopConsumer());

            assertThat(reader.next()).isTrue();
            assertThat(reader.next()).isFalse();
        }

        @Test
        void startOffsetOutOfRangeFailsWhenFailOnDataLossTrue() {
            RabbitMQInputPartition partition = new RabbitMQInputPartition(
                    "test-stream", 5, 6, minimalOptions());
            RabbitMQPartitionReader reader = new RabbitMQPartitionReader(partition, partition.getOptions());

            assertThatThrownBy(() -> reader.handleStartOffsetOutOfRange(7L))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("before first available")
                    .hasMessageContaining("failOnDataLoss=false");
            assertThat(reader.currentMetricsValues()[4].value()).isEqualTo(1L);
        }

        @Test
        void startOffsetOutOfRangeContinuesWhenFailOnDataLossFalse() {
            Map<String, String> opts = new LinkedHashMap<>();
            opts.put("endpoints", "localhost:5552");
            opts.put("stream", "test-stream");
            opts.put("failOnDataLoss", "false");

            RabbitMQInputPartition partition = new RabbitMQInputPartition(
                    "test-stream", 5, 6, new ConnectorOptions(opts));
            RabbitMQPartitionReader reader = new RabbitMQPartitionReader(partition, partition.getOptions());

            reader.handleStartOffsetOutOfRange(7L);
            assertThat(reader.currentMetricsValues()[4].value()).isEqualTo(1L);
        }

        @Test
        void nextEnforcesEndOffsetExclusive() throws Exception {
            RabbitMQInputPartition partition = new RabbitMQInputPartition(
                    "test-stream", 0, 10, minimalOptions());
            RabbitMQPartitionReader reader = new RabbitMQPartitionReader(partition, partition.getOptions());

            BlockingQueue<RabbitMQPartitionReader.QueuedMessage> queue = new LinkedBlockingQueue<>();
            queue.add(new RabbitMQPartitionReader.QueuedMessage(
                    CODEC.messageBuilder().addData("a".getBytes()).build(),
                    9L, 0L, new NoopContext()));
            queue.add(new RabbitMQPartitionReader.QueuedMessage(
                    CODEC.messageBuilder().addData("b".getBytes()).build(),
                    10L, 0L, new NoopContext()));

            setPrivateField(reader, "queue", queue);
            setPrivateField(reader, "consumer", new NoopConsumer());

            assertThat(reader.next()).isTrue();
            assertThat(reader.next()).isFalse();
        }

        @Test
        void nextStopsAtExactEndOffset() throws Exception {
            RabbitMQInputPartition partition = new RabbitMQInputPartition(
                    "test-stream", 3, 5, minimalOptions());
            RabbitMQPartitionReader reader = new RabbitMQPartitionReader(partition, partition.getOptions());

            BlockingQueue<RabbitMQPartitionReader.QueuedMessage> queue = new LinkedBlockingQueue<>();
            queue.add(new RabbitMQPartitionReader.QueuedMessage(
                    CODEC.messageBuilder().addData("a".getBytes()).build(),
                    3L, 0L, new NoopContext()));
            queue.add(new RabbitMQPartitionReader.QueuedMessage(
                    CODEC.messageBuilder().addData("b".getBytes()).build(),
                    4L, 0L, new NoopContext()));
            queue.add(new RabbitMQPartitionReader.QueuedMessage(
                    CODEC.messageBuilder().addData("c".getBytes()).build(),
                    5L, 0L, new NoopContext()));

            setPrivateField(reader, "queue", queue);
            setPrivateField(reader, "consumer", new NoopConsumer());

            assertThat(reader.next()).isTrue();
            assertThat(reader.get().getLong(2)).isEqualTo(3L);
            assertThat(reader.next()).isTrue();
            assertThat(reader.get().getLong(2)).isEqualTo(4L);
            assertThat(reader.next()).isFalse();
        }

        @Test
        void nextStopsWhenEndOffsetEqualsOne() throws Exception {
            RabbitMQInputPartition partition = new RabbitMQInputPartition(
                    "test-stream", 0, 1, minimalOptions());
            RabbitMQPartitionReader reader = new RabbitMQPartitionReader(partition, partition.getOptions());

            BlockingQueue<RabbitMQPartitionReader.QueuedMessage> queue = new LinkedBlockingQueue<>();
            queue.add(new RabbitMQPartitionReader.QueuedMessage(
                    CODEC.messageBuilder().addData("a".getBytes()).build(),
                    0L, 0L, new NoopContext()));

            setPrivateField(reader, "queue", queue);
            setPrivateField(reader, "consumer", new NoopConsumer());

            assertThat(reader.next()).isTrue();
            assertThat(reader.next()).isFalse();
        }

        @Test
        void nextDoesNotReapplyPostFilterInReaderLoop() throws Exception {
            Map<String, String> opts = new LinkedHashMap<>();
            opts.put("endpoints", "localhost:5552");
            opts.put("stream", "test-stream");
            opts.put("filterWarningOnMismatch", "false");

            RabbitMQInputPartition partition = new RabbitMQInputPartition(
                    "test-stream", 0, 3, new ConnectorOptions(opts));
            RabbitMQPartitionReader reader = new RabbitMQPartitionReader(partition, partition.getOptions());

            setPrivateField(reader, "postFilter", rejectAllPostFilter(reader));

            BlockingQueue<RabbitMQPartitionReader.QueuedMessage> queue = new LinkedBlockingQueue<>();
            queue.add(new RabbitMQPartitionReader.QueuedMessage(
                    CODEC.messageBuilder().addData("a".getBytes()).build(),
                    1L, 0L, new NoopContext()));
            queue.add(new RabbitMQPartitionReader.QueuedMessage(
                    CODEC.messageBuilder().addData("b".getBytes()).build(),
                    2L, 0L, new NoopContext()));

            setPrivateField(reader, "queue", queue);
            setPrivateField(reader, "consumer", new NoopConsumer());

            assertThat(reader.next()).isTrue();
            assertThat(reader.get().getLong(2)).isEqualTo(1L);
        }

        @Test
        void nextTracksRecordsReadAndBytesRead() throws Exception {
            RabbitMQInputPartition partition = new RabbitMQInputPartition(
                    "test-stream", 0, 4, minimalOptions());
            RabbitMQPartitionReader reader = new RabbitMQPartitionReader(partition, partition.getOptions());

            BlockingQueue<RabbitMQPartitionReader.QueuedMessage> queue = new SlowQueue<>(5);
            queue.add(new RabbitMQPartitionReader.QueuedMessage(
                    CODEC.messageBuilder().addData("a".getBytes()).build(),
                    0L, 0L, new NoopContext()));
            queue.add(new RabbitMQPartitionReader.QueuedMessage(
                    CODEC.messageBuilder().addData("bb".getBytes()).build(),
                    1L, 0L, new NoopContext()));
            queue.add(new RabbitMQPartitionReader.QueuedMessage(
                    CODEC.messageBuilder().addData("ccc".getBytes()).build(),
                    2L, 0L, new NoopContext()));

            setPrivateField(reader, "queue", queue);
            setPrivateField(reader, "consumer", new NoopConsumer());

            assertThat(reader.next()).isTrue();
            assertThat(reader.next()).isTrue();
            assertThat(reader.next()).isTrue();

            var metrics = reader.currentMetricsValues();
            assertThat(metrics[0].value()).isEqualTo(3L);
            assertThat(metrics[1].value()).isEqualTo(6L);
            assertThat(metrics[2].value()).isGreaterThanOrEqualTo(5L);
        }

        @Test
        void nextSurfacesConsumerError() throws Exception {
            RabbitMQInputPartition partition = new RabbitMQInputPartition(
                    "test-stream", 0, 10, minimalOptions());
            RabbitMQPartitionReader reader = new RabbitMQPartitionReader(partition, partition.getOptions());

            setPrivateField(reader, "consumer", new NoopConsumer());
            setPrivateField(reader, "consumerError", new java.util.concurrent.atomic.AtomicReference<>(
                    new RuntimeException("queue full")));

            assertThatThrownBy(reader::next)
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining("Consumer error");
        }

        @Test
        void nextInterruptedRestoresInterruptFlag() throws Exception {
            Map<String, String> opts = new LinkedHashMap<>();
            opts.put("endpoints", "localhost:5552");
            opts.put("stream", "test-stream");
            opts.put("pollTimeoutMs", "1");
            opts.put("maxWaitMs", "2");

            RabbitMQInputPartition partition = new RabbitMQInputPartition(
                    "test-stream", 0, 10, new ConnectorOptions(opts));
            RabbitMQPartitionReader reader = new RabbitMQPartitionReader(partition, partition.getOptions());

            setPrivateField(reader, "consumer", new NoopConsumer());
            setPrivateField(reader, "queue", new InterruptingQueue<>());

            assertThatThrownBy(reader::next)
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining("Interrupted");
            assertThat(Thread.currentThread().isInterrupted()).isTrue();
            Thread.interrupted();
        }

        @Test
        void callbackEnqueueInterruptSurfacesConsumerErrorWithoutInterruptingThread()
                throws Exception {
            RabbitMQInputPartition partition = new RabbitMQInputPartition(
                    "test-stream", 0, 10, minimalOptions());
            RabbitMQPartitionReader reader = new RabbitMQPartitionReader(partition, partition.getOptions());

            setPrivateField(reader, "queue", new InterruptingOfferQueue<>());
            assertThat(Thread.currentThread().isInterrupted()).isFalse();

            reader.enqueueFromCallback(new NoopContext(),
                    CODEC.messageBuilder().addData("x".getBytes()).build());

            assertThat(Thread.currentThread().isInterrupted()).isFalse();
            setPrivateField(reader, "consumer", new NoopConsumer());
            setPrivateField(reader, "queue",
                    new java.util.concurrent.LinkedBlockingQueue<RabbitMQPartitionReader.QueuedMessage>());

            assertThatThrownBy(reader::next)
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining("Consumer error")
                    .satisfies(error -> assertThat(error.getCause())
                            .isInstanceOf(IOException.class)
                            .hasMessage("Interrupted while enqueuing message at offset 0 on stream 'test-stream'"));
        }

        @Test
        void closeDrainsMetricsIntoMessageSizeTrackerOnce() throws Exception {
            RabbitMQInputPartition partition = new RabbitMQInputPartition(
                    "test-stream", 0, 10, minimalOptions());
            RabbitMQPartitionReader reader = new RabbitMQPartitionReader(partition, partition.getOptions());

            setPrivateField(reader, "payloadBytesRead", 20L);
            setPrivateField(reader, "recordsRead", 2L);

            reader.close();
            int first = MessageSizeTracker.drainAverage(100);
            int second = MessageSizeTracker.drainAverage(100);

            assertThat(first).isEqualTo(10);
            assertThat(second).isEqualTo(100);
        }

        @Test
        void closeDrainsMetricsIntoSparkAccumulatorsWhenPresent() throws Exception {
            LongAccumulator bytesAccumulator = new LongAccumulator();
            LongAccumulator recordsAccumulator = new LongAccumulator();
            RabbitMQInputPartition partition = new RabbitMQInputPartition(
                    "test-stream", 0, 10, minimalOptions(), false, null, "scope-x",
                    bytesAccumulator, recordsAccumulator);
            RabbitMQPartitionReader reader = new RabbitMQPartitionReader(partition, partition.getOptions());

            setPrivateField(reader, "payloadBytesRead", 30L);
            setPrivateField(reader, "recordsRead", 3L);

            reader.close();

            assertThat(bytesAccumulator.value()).isEqualTo(30L);
            assertThat(recordsAccumulator.value()).isEqualTo(3L);
            assertThat(MessageSizeTracker.drainAverage("scope-x", 99)).isEqualTo(99);
        }

        @Test
        void closeIsIdempotentWhenCalledTwice() throws Exception {
            RabbitMQInputPartition partition = new RabbitMQInputPartition(
                    "test-stream", 0, 10, minimalOptions());
            RabbitMQPartitionReader reader = new RabbitMQPartitionReader(partition, partition.getOptions());

            setPrivateField(reader, "payloadBytesRead", 20L);
            setPrivateField(reader, "recordsRead", 2L);

            reader.close();
            reader.close();

            // Regression: duplicate close must not record metrics twice or double-release resources.
            int first = MessageSizeTracker.drainAverage(100);
            int second = MessageSizeTracker.drainAverage(100);

            assertThat(first).isEqualTo(10);
            assertThat(second).isEqualTo(100);
        }

        @Test
        void currentMetricsValuesReflectCounts() throws Exception {
            RabbitMQInputPartition partition = new RabbitMQInputPartition(
                    "test-stream", 0, 10, minimalOptions());
            RabbitMQPartitionReader reader = new RabbitMQPartitionReader(partition, partition.getOptions());

            setPrivateField(reader, "recordsRead", 3L);
            setPrivateField(reader, "payloadBytesRead", 30L);
            setPrivateField(reader, "estimatedWireBytesRead", 45L);
            setPrivateField(reader, "pollWaitMs", 7L);

            var metrics = reader.currentMetricsValues();
            assertThat(metrics).hasSize(6);
            assertThat(metrics[0].value()).isEqualTo(3L);
            assertThat(metrics[1].value()).isEqualTo(30L);
            assertThat(metrics[2].value()).isEqualTo(45L);
            assertThat(metrics[3].value()).isEqualTo(7L);
            assertThat(metrics[4].value()).isEqualTo(0L); // offsetOutOfRange
            assertThat(metrics[5].value()).isEqualTo(0L); // dataLoss
        }

        @Test
        void resolveOffsetSpecUsesOffsetForNonTimestampModes() throws Exception {
            Map<String, String> opts = new LinkedHashMap<>();
            opts.put("endpoints", "localhost:5552");
            opts.put("stream", "test-stream");
            opts.put("startingOffsets", "latest");

            RabbitMQInputPartition partition = new RabbitMQInputPartition(
                    "test-stream", 12, 20, new ConnectorOptions(opts), false);
            RabbitMQPartitionReader reader = new RabbitMQPartitionReader(partition, partition.getOptions());

            OffsetSpecification spec = resolveOffsetSpec(reader);
            assertThat(spec).isEqualTo(OffsetSpecification.offset(12));
        }

        @Test
        void coerceMapToStringsHandlesNullsAndMixedTypes() throws Exception {
            Map<String, Object> source = new LinkedHashMap<>();
            source.put("a", 1);
            source.put("b", null);
            source.put("c", "value");

            @SuppressWarnings("unchecked")
            Map<String, String> result = (Map<String, String>) invokeStatic(
                    "coerceMapToStrings", new Class<?>[]{Map.class}, source);

            assertThat(result).containsEntry("a", "1");
            assertThat(result).containsEntry("b", null);
            assertThat(result).containsEntry("c", "value");
        }

        @Test
        void queuedMessageHoldsData() {
            Message msg = CODEC.messageBuilder().addData("hello".getBytes()).build();
            var qm = new RabbitMQPartitionReader.QueuedMessage(msg, 42, 1700000000000L, null);

            assertThat(qm.offset()).isEqualTo(42);
            assertThat(qm.chunkTimestampMillis()).isEqualTo(1700000000000L);
            assertThat(qm.message().getBodyAsBinary()).isEqualTo("hello".getBytes());
        }

        @Test
        void resolveOffsetSpecUsesExplicitOffsetForCheckpointedBatch() throws Exception {
            Map<String, String> opts = new LinkedHashMap<>();
            opts.put("endpoints", "localhost:5552");
            opts.put("stream", "test-stream");
            opts.put("startingOffsets", "earliest");

            RabbitMQInputPartition partition = new RabbitMQInputPartition(
                    "test-stream", 42, 100, new ConnectorOptions(opts), false);
            RabbitMQPartitionReader reader = new RabbitMQPartitionReader(partition, partition.getOptions());

            OffsetSpecification spec = resolveOffsetSpec(reader);
            assertThat(spec).isEqualTo(OffsetSpecification.offset(42));
        }

        @Test
        void resolveOffsetSpecUsesTimestampForInitialTimestampBatch() throws Exception {
            Map<String, String> opts = new LinkedHashMap<>();
            opts.put("endpoints", "localhost:5552");
            opts.put("stream", "test-stream");
            opts.put("startingOffsets", "timestamp");
            opts.put("startingTimestamp", "1700000000000");

            RabbitMQInputPartition partition = new RabbitMQInputPartition(
                    "test-stream", 0, 100, new ConnectorOptions(opts), true);
            RabbitMQPartitionReader reader = new RabbitMQPartitionReader(partition, partition.getOptions());

            OffsetSpecification spec = resolveOffsetSpec(reader);
            assertThat(spec).isEqualTo(OffsetSpecification.timestamp(1700000000000L));
        }

        @Test
        void resolveOffsetSpecUsesPerStreamTimestampWhenGlobalTimestampMissing() throws Exception {
            Map<String, String> opts = new LinkedHashMap<>();
            opts.put("endpoints", "localhost:5552");
            opts.put("stream", "test-stream");
            opts.put("startingOffsets", "timestamp");
            opts.put("startingOffsetsByTimestamp", "{\"test-stream\":1700000001234}");

            RabbitMQInputPartition partition = new RabbitMQInputPartition(
                    "test-stream", 0, 100, new ConnectorOptions(opts), true);
            RabbitMQPartitionReader reader = new RabbitMQPartitionReader(partition, partition.getOptions());

            OffsetSpecification spec = resolveOffsetSpec(reader);
            assertThat(spec).isEqualTo(OffsetSpecification.timestamp(1700000001234L));
        }

        @Test
        void nextTimestampInitialSplitSkipsMessagesBeforeConfiguredTimestamp() throws Exception {
            Map<String, String> opts = new LinkedHashMap<>();
            opts.put("endpoints", "localhost:5552");
            opts.put("stream", "test-stream");
            opts.put("startingOffsets", "timestamp");
            opts.put("startingTimestamp", "1700000000000");
            // Keep this bounded: default maxWaitMs is large and can mask hangs in CI.
            opts.put("pollTimeoutMs", "5");
            opts.put("maxWaitMs", "20");

            RabbitMQInputPartition partition = new RabbitMQInputPartition(
                    "test-stream", 0, 3, new ConnectorOptions(opts), true);
            RabbitMQPartitionReader reader = new RabbitMQPartitionReader(partition, partition.getOptions());

            BlockingQueue<RabbitMQPartitionReader.QueuedMessage> queue = new LinkedBlockingQueue<>();
            queue.add(new RabbitMQPartitionReader.QueuedMessage(
                    CODEC.messageBuilder().addData("old".getBytes()).build(),
                    0L, 1699999999999L, new NoopContext()));
            queue.add(new RabbitMQPartitionReader.QueuedMessage(
                    CODEC.messageBuilder().addData("new".getBytes()).build(),
                    1L, 1700000000000L, new NoopContext()));

            setPrivateField(reader, "queue", queue);
            setPrivateField(reader, "consumer", new NoopConsumer());

            assertThat(reader.next()).isTrue();
            assertThat(reader.get().getLong(2)).isEqualTo(1L);
            long startNanos = System.nanoTime();
            assertThat(reader.next()).isFalse();
            long elapsedMs = java.util.concurrent.TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
            // Regression guard: this path used to block ~300s on default maxWaitMs.
            assertThat(elapsedMs).isLessThan(500L);
        }

        @Test
        void nextTimestampInitialSplitAppliesPerStreamTimestampOverride() throws Exception {
            Map<String, String> opts = new LinkedHashMap<>();
            opts.put("endpoints", "localhost:5552");
            opts.put("stream", "test-stream");
            opts.put("startingOffsets", "timestamp");
            opts.put("startingTimestamp", "1700000005000");
            opts.put("startingOffsetsByTimestamp", "{\"test-stream\":1700000000000}");
            opts.put("pollTimeoutMs", "5");
            opts.put("maxWaitMs", "20");

            RabbitMQInputPartition partition = new RabbitMQInputPartition(
                    "test-stream", 0, 2, new ConnectorOptions(opts), true);
            RabbitMQPartitionReader reader = new RabbitMQPartitionReader(partition, partition.getOptions());

            BlockingQueue<RabbitMQPartitionReader.QueuedMessage> queue = new LinkedBlockingQueue<>();
            queue.add(new RabbitMQPartitionReader.QueuedMessage(
                    CODEC.messageBuilder().addData("new".getBytes()).build(),
                    1L, 1700000001000L, new NoopContext()));

            setPrivateField(reader, "queue", queue);
            setPrivateField(reader, "consumer", new NoopConsumer());

            assertThat(reader.next()).isTrue();
            assertThat(reader.get().getLong(2)).isEqualTo(1L);
            assertThat(reader.next()).isFalse();
        }

        @Test
        void probeLastMessageOffsetReturnsQuicklyWhenNoMessageArrives() throws Exception {
            Map<String, String> opts = new LinkedHashMap<>();
            opts.put("endpoints", "localhost:5552");
            opts.put("stream", "test-stream");
            opts.put("maxWaitMs", "20");

            RabbitMQInputPartition partition = new RabbitMQInputPartition(
                    "test-stream", 0, 100, new ConnectorOptions(opts));
            RabbitMQPartitionReader reader = new RabbitMQPartitionReader(partition, partition.getOptions());

            com.rabbitmq.stream.Environment env = mock(com.rabbitmq.stream.Environment.class);
            com.rabbitmq.stream.ConsumerBuilder builder = mock(com.rabbitmq.stream.ConsumerBuilder.class);
            com.rabbitmq.stream.ConsumerBuilder.FlowConfiguration flow = mock(
                    com.rabbitmq.stream.ConsumerBuilder.FlowConfiguration.class);
            com.rabbitmq.stream.Consumer probe = mock(com.rabbitmq.stream.Consumer.class);

            when(env.consumerBuilder()).thenReturn(builder);
            when(builder.stream(anyString())).thenReturn(builder);
            when(builder.offset(any(OffsetSpecification.class))).thenReturn(builder);
            when(builder.noTrackingStrategy()).thenReturn(builder);
            when(builder.messageHandler(any(com.rabbitmq.stream.MessageHandler.class))).thenReturn(builder);
            when(builder.flow()).thenReturn(flow);
            when(flow.strategy(any(com.rabbitmq.stream.ConsumerFlowStrategy.class))).thenReturn(flow);
            when(flow.builder()).thenReturn(builder);
            when(builder.build()).thenReturn(probe);

            setPrivateField(reader, "environment", env);

            Method probeMethod = findMethod(RabbitMQPartitionReader.class, "probeLastMessageOffset");
            probeMethod.setAccessible(true);
            long startNanos = System.nanoTime();
            long observed = (long) probeMethod.invoke(reader);
            long elapsedMs = java.util.concurrent.TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);

            assertThat(observed).isEqualTo(-1L);
            assertThat(elapsedMs).isLessThan(500L);
            verify(probe).close();
        }

        @Test
        void probeLastMessageOffsetReusesRecentProbeResult() throws Exception {
            Map<String, String> opts = new LinkedHashMap<>();
            opts.put("endpoints", "localhost:5552");
            opts.put("stream", "test-stream");
            opts.put("maxWaitMs", "20");

            RabbitMQInputPartition partition = new RabbitMQInputPartition(
                    "test-stream", 0, 100, new ConnectorOptions(opts));
            RabbitMQPartitionReader reader = new RabbitMQPartitionReader(partition, partition.getOptions());

            com.rabbitmq.stream.Environment env = mock(com.rabbitmq.stream.Environment.class);
            com.rabbitmq.stream.ConsumerBuilder builder = mock(com.rabbitmq.stream.ConsumerBuilder.class);
            com.rabbitmq.stream.ConsumerBuilder.FlowConfiguration flow = mock(
                    com.rabbitmq.stream.ConsumerBuilder.FlowConfiguration.class);
            com.rabbitmq.stream.Consumer probe = mock(com.rabbitmq.stream.Consumer.class);

            when(env.consumerBuilder()).thenReturn(builder);
            when(builder.stream(anyString())).thenReturn(builder);
            when(builder.offset(any(OffsetSpecification.class))).thenReturn(builder);
            when(builder.noTrackingStrategy()).thenReturn(builder);
            when(builder.messageHandler(any(com.rabbitmq.stream.MessageHandler.class))).thenReturn(builder);
            when(builder.flow()).thenReturn(flow);
            when(flow.strategy(any(com.rabbitmq.stream.ConsumerFlowStrategy.class))).thenReturn(flow);
            when(flow.builder()).thenReturn(builder);
            when(builder.build()).thenReturn(probe);

            setPrivateField(reader, "environment", env);

            Method probeMethod = findMethod(RabbitMQPartitionReader.class, "probeLastMessageOffset");
            probeMethod.setAccessible(true);
            long first = (long) probeMethod.invoke(reader);
            long second = (long) probeMethod.invoke(reader);

            assertThat(first).isEqualTo(-1L);
            assertThat(second).isEqualTo(-1L);
            verify(env, times(1)).consumerBuilder();
            verify(builder, times(1)).build();
            verify(probe, times(1)).close();
        }

        @Test
        void probeLastMessageOffsetReturnsMinusOneWhenEnvironmentIsCleared() throws Exception {
            RabbitMQInputPartition partition = new RabbitMQInputPartition(
                    "test-stream", 0, 100, minimalOptions());
            RabbitMQPartitionReader reader = new RabbitMQPartitionReader(partition, partition.getOptions());

            setPrivateField(reader, "environment", null);

            Method probeMethod = findMethod(RabbitMQPartitionReader.class, "probeLastMessageOffset");
            probeMethod.setAccessible(true);
            long observed = (long) probeMethod.invoke(reader);

            assertThat(observed).isEqualTo(-1L);
        }

        @Test
        void retentionAdvanceAfterInRangeProgressIsNotTreatedAsDataLoss() throws Exception {
            RabbitMQInputPartition partition = new RabbitMQInputPartition(
                    "test-stream", 50, 120, minimalOptions());
            RabbitMQPartitionReader reader = new RabbitMQPartitionReader(partition, partition.getOptions());

            com.rabbitmq.stream.Environment env = mock(com.rabbitmq.stream.Environment.class);
            com.rabbitmq.stream.StreamStats stats = mock(com.rabbitmq.stream.StreamStats.class);
            when(env.queryStreamStats("test-stream")).thenReturn(stats);
            when(stats.firstOffset()).thenReturn(60L);
            when(stats.committedOffset()).thenReturn(80L);
            setPrivateField(reader, "environment", env);
            setPrivateField(reader, "lastObservedOffset", 75L);

            assertThat(isPlannedRangeNoLongerReachableDueToDataLoss(reader)).isFalse();
        }

        @Test
        void retentionAdvanceBeforeAnyInRangeProgressIsTreatedAsDataLoss() throws Exception {
            RabbitMQInputPartition partition = new RabbitMQInputPartition(
                    "test-stream", 50, 120, minimalOptions());
            RabbitMQPartitionReader reader = new RabbitMQPartitionReader(partition, partition.getOptions());

            com.rabbitmq.stream.Environment env = mock(com.rabbitmq.stream.Environment.class);
            com.rabbitmq.stream.StreamStats stats = mock(com.rabbitmq.stream.StreamStats.class);
            when(env.queryStreamStats("test-stream")).thenReturn(stats);
            when(stats.firstOffset()).thenReturn(60L);
            when(stats.committedOffset()).thenReturn(80L);
            setPrivateField(reader, "environment", env);
            setPrivateField(reader, "lastObservedOffset", -1L);

            assertThat(isPlannedRangeNoLongerReachableDueToDataLoss(reader)).isTrue();
        }

        @Test
        void resolveSubscriptionOffsetSpecUsesLastEmittedOffsetWhenAvailable() throws Exception {
            RabbitMQInputPartition partition = new RabbitMQInputPartition(
                    "test-stream", 0, 100, minimalOptions());
            RabbitMQPartitionReader reader = new RabbitMQPartitionReader(partition, partition.getOptions());

            setPrivateField(reader, "lastEmittedOffset", 41L);
            setPrivateField(reader, "lastObservedOffset", 52L);

            OffsetSpecification resolved = resolveSubscriptionOffsetSpec(
                    reader, OffsetSpecification.first(), OffsetSpecification.offset(0));
            assertThat(resolved).isEqualTo(OffsetSpecification.offset(42));
        }

        @Test
        void resolveSubscriptionOffsetSpecFallsBackToLastObservedOffset() throws Exception {
            RabbitMQInputPartition partition = new RabbitMQInputPartition(
                    "test-stream", 0, 100, minimalOptions());
            RabbitMQPartitionReader reader = new RabbitMQPartitionReader(partition, partition.getOptions());

            setPrivateField(reader, "lastEmittedOffset", -1L);
            setPrivateField(reader, "lastObservedOffset", 9L);

            OffsetSpecification resolved = resolveSubscriptionOffsetSpec(
                    reader, OffsetSpecification.first(), OffsetSpecification.offset(0));
            assertThat(resolved).isEqualTo(OffsetSpecification.offset(10));
        }

        @Test
        void realTimeGetOffsetUsesLastObservedOffsetWhenAheadOfLastEmitted() throws Exception {
            RabbitMQInputPartition partition = new RabbitMQInputPartition(
                    "test-stream", 5, 100, minimalOptions());
            RabbitMQPartitionReader reader = new RabbitMQPartitionReader(partition, partition.getOptions());

            setPrivateField(reader, "lastEmittedOffset", 6L);
            setPrivateField(reader, "lastObservedOffset", 11L);

            // Spark 4.1 reader implements SupportsRealTimeRead#getOffset; older Spark variants don't.
            Method getOffsetMethod;
            try {
                getOffsetMethod = findMethod(RabbitMQPartitionReader.class, "getOffset");
            } catch (NoSuchMethodException ignored) {
                return;
            }
            getOffsetMethod.setAccessible(true);
            Object partitionOffset = getOffsetMethod.invoke(reader);
            Method getNextOffsetMethod = partitionOffset.getClass().getMethod("getNextOffset");
            long nextOffset = (long) getNextOffsetMethod.invoke(partitionOffset);
            assertThat(nextOffset).isEqualTo(12L);
        }

        @Test
        void resolveSubscriptionOffsetSpecFallsBackToConfiguredOffsetWhenNoProgress() throws Exception {
            RabbitMQInputPartition partition = new RabbitMQInputPartition(
                    "test-stream", 0, 100, minimalOptions());
            RabbitMQPartitionReader reader = new RabbitMQPartitionReader(partition, partition.getOptions());

            setPrivateField(reader, "lastEmittedOffset", -1L);
            setPrivateField(reader, "lastObservedOffset", -1L);

            OffsetSpecification resolved = resolveSubscriptionOffsetSpec(
                    reader, null, OffsetSpecification.offset(7));
            assertThat(resolved).isEqualTo(OffsetSpecification.offset(7));
        }

        @Test
        void resolveSingleActiveConsumerNameUsesBaseNameForSingleStream() throws Exception {
            Map<String, String> opts = new LinkedHashMap<>();
            opts.put("endpoints", "localhost:5552");
            opts.put("stream", "test-stream");
            opts.put("singleActiveConsumer", "true");
            opts.put("consumerName", "sac-reader");

            RabbitMQInputPartition partition = new RabbitMQInputPartition(
                    "test-stream", 0, 100, new ConnectorOptions(opts), false);
            RabbitMQPartitionReader reader = new RabbitMQPartitionReader(partition, partition.getOptions());

            String resolved = resolveSingleActiveConsumerName(reader);
            assertThat(resolved).isEqualTo("sac-reader");
        }

        @Test
        void resolveSingleActiveConsumerNameAppendsPartitionStreamForSuperstream() throws Exception {
            Map<String, String> opts = new LinkedHashMap<>();
            opts.put("endpoints", "localhost:5552");
            opts.put("superstream", "orders");
            opts.put("singleActiveConsumer", "true");
            opts.put("consumerName", "sac-reader");

            RabbitMQInputPartition partition = new RabbitMQInputPartition(
                    "orders-2", 0, 100, new ConnectorOptions(opts), false);
            RabbitMQPartitionReader reader = new RabbitMQPartitionReader(partition, partition.getOptions());

            String resolved = resolveSingleActiveConsumerName(reader);
            assertThat(resolved).isEqualTo("sac-reader-orders-2");
        }

        @Test
        void coercePropertiesToStringsIncludesZeroGroupSequence() {
            Properties props = mock(Properties.class);
            when(props.getGroupSequence()).thenReturn(0L);

            Map<String, String> coerced = BaseRabbitMQPartitionReader.coercePropertiesToStrings(props);

            assertThat(coerced).containsEntry("group_sequence", "0");
        }

        @Test
        void createsConfiguredPostFilter() throws Exception {
            Map<String, String> opts = new LinkedHashMap<>();
            opts.put("endpoints", "localhost:5552");
            opts.put("stream", "test-stream");
            opts.put("filterPostFilterClass", AcceptAllPostFilter.class.getName());

            Object postFilter = createPostFilter(new ConnectorOptions(opts));
            assertThat(postFilter).isNotNull();
            assertThat(accept(postFilter, new ConnectorMessageView(
                    new byte[0], Map.of(), Map.of(), Map.of()))).isTrue();
        }

        @Test
        void derivesPostFilterFromFilterValuesAndFilterValuePath() throws Exception {
            Map<String, String> opts = new LinkedHashMap<>();
            opts.put("endpoints", "localhost:5552");
            opts.put("stream", "test-stream");
            opts.put("filterValues", "alpha,beta");
            opts.put("filterValuePath", "application_properties.region");

            Object postFilter = createPostFilter(new ConnectorOptions(opts));
            assertThat(postFilter).isNotNull();
            assertThat(accept(postFilter, new ConnectorMessageView(
                    new byte[0], Map.of("region", "alpha"), Map.of(), Map.of()))).isTrue();
            assertThat(accept(postFilter, new ConnectorMessageView(
                    new byte[0], Map.of("region", "gamma"), Map.of(), Map.of()))).isFalse();
            assertThat(accept(postFilter, new ConnectorMessageView(
                    new byte[0], Map.of(), Map.of(), Map.of()))).isFalse();
        }

        @Test
        void derivedPostFilterCanMatchUnfilteredMessages() throws Exception {
            Map<String, String> opts = new LinkedHashMap<>();
            opts.put("endpoints", "localhost:5552");
            opts.put("stream", "test-stream");
            opts.put("filterValues", "alpha");
            opts.put("filterValuePath", "application_properties.region");
            opts.put("filterMatchUnfiltered", "true");

            Object postFilter = createPostFilter(new ConnectorOptions(opts));
            assertThat(postFilter).isNotNull();
            assertThat(accept(postFilter, new ConnectorMessageView(
                    new byte[0], Map.of(), Map.of(), Map.of()))).isTrue();
            assertThat(accept(postFilter, new ConnectorMessageView(
                    new byte[0], null, Map.of(), Map.of()))).isTrue();
        }

        @Test
        void doesNotDerivePostFilterWithoutFilterValuePath() throws Exception {
            Map<String, String> opts = new LinkedHashMap<>();
            opts.put("endpoints", "localhost:5552");
            opts.put("stream", "test-stream");
            opts.put("filterValues", "alpha,beta");

            Object postFilter = createPostFilter(new ConnectorOptions(opts));
            assertThat(postFilter).isNull();
        }
    }

    // ======================================================================
    // ReaderFactory tests
    // ======================================================================

    @Nested
    class ReaderFactoryTest {

        @Test
        void factoryIsSerializable() throws Exception {
            ConnectorOptions opts = minimalOptions();
            var schema = RabbitMQStreamTable.buildSourceSchema(opts.getMetadataFields());
            RabbitMQPartitionReaderFactory factory = new RabbitMQPartitionReaderFactory(opts, schema);

            var bos = new java.io.ByteArrayOutputStream();
            var oos = new java.io.ObjectOutputStream(bos);
            oos.writeObject(factory);
            oos.close();

            var bis = new java.io.ByteArrayInputStream(bos.toByteArray());
            var ois = new java.io.ObjectInputStream(bis);
            RabbitMQPartitionReaderFactory deserialized =
                    (RabbitMQPartitionReaderFactory) ois.readObject();
            assertThat(deserialized).isNotNull();
        }
    }

    // ======================================================================
    // Scan builder tests
    // ======================================================================

    @Nested
    class ScanBuilderTest {

        @Test
        void buildReturnsScan() {
            ConnectorOptions opts = minimalOptions();
            var schema = RabbitMQStreamTable.buildSourceSchema(opts.getMetadataFields());
            RabbitMQScanBuilder scanBuilder = new RabbitMQScanBuilder(opts, schema);

            var scan = scanBuilder.build();
            assertThat(scan).isInstanceOf(RabbitMQScan.class);
            assertThat(scan.readSchema()).isEqualTo(schema);
        }

        @Test
        void scanBuilderValidatesSourceOptions() {
            Map<String, String> opts = new LinkedHashMap<>();
            opts.put("endpoints", "localhost:5552");
            opts.put("stream", "test");
            opts.put("startingOffsets", "offset");
            // Missing startingOffset - should fail validation
            ConnectorOptions badOpts = new ConnectorOptions(opts);
            var schema = RabbitMQStreamTable.buildSourceSchema(badOpts.getMetadataFields());

            assertThatThrownBy(() -> new RabbitMQScanBuilder(badOpts, schema))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("startingOffset");
        }

        @Test
        void scanDescriptionIncludesStreamName() {
            ConnectorOptions opts = minimalOptions();
            var schema = RabbitMQStreamTable.buildSourceSchema(opts.getMetadataFields());
            var scan = new RabbitMQScan(opts, schema);

            assertThat(scan.description()).contains("test-stream");
        }
    }

    // ---- Helpers ----

    private static ConnectorOptions minimalOptions() {
        Map<String, String> opts = new LinkedHashMap<>();
        opts.put("endpoints", "localhost:5552");
        opts.put("stream", "test-stream");
        return new ConnectorOptions(opts);
    }

    private static OffsetSpecification resolveOffsetSpec(RabbitMQPartitionReader reader)
            throws Exception {
        Method method = findMethod(RabbitMQPartitionReader.class, "resolveOffsetSpec");
        method.setAccessible(true);
        return (OffsetSpecification) method.invoke(reader);
    }

    private static String resolveSingleActiveConsumerName(RabbitMQPartitionReader reader)
            throws Exception {
        Method method = findMethod(RabbitMQPartitionReader.class,
                "resolveSingleActiveConsumerName");
        method.setAccessible(true);
        return (String) method.invoke(reader);
    }

    private static OffsetSpecification resolveSubscriptionOffsetSpec(
            RabbitMQPartitionReader reader,
            OffsetSpecification subscriptionOffsetSpec,
            OffsetSpecification configuredOffsetSpec) throws Exception {
        Method method = findMethod(RabbitMQPartitionReader.class,
                "resolveSubscriptionOffsetSpec", OffsetSpecification.class, OffsetSpecification.class);
        method.setAccessible(true);
        return (OffsetSpecification) method.invoke(reader, subscriptionOffsetSpec, configuredOffsetSpec);
    }

    private static boolean isPlannedRangeNoLongerReachableDueToDataLoss(
            RabbitMQPartitionReader reader) throws Exception {
        Method method = findMethod(RabbitMQPartitionReader.class,
                "isPlannedRangeNoLongerReachableDueToDataLoss");
        method.setAccessible(true);
        return (boolean) method.invoke(reader);
    }

    private static Object createPostFilter(ConnectorOptions options)
            throws Exception {
        Method method = findMethod(RabbitMQPartitionReader.class,
                "createPostFilter", ConnectorOptions.class);
        method.setAccessible(true);
        return method.invoke(null, options);
    }

    private static boolean accept(Object postFilter, ConnectorMessageView message) throws Exception {
        Method method = postFilter.getClass().getMethod("accept", ConnectorMessageView.class);
        method.setAccessible(true);
        return (boolean) method.invoke(postFilter, message);
    }

    private static Object rejectAllPostFilter(RabbitMQPartitionReader reader) throws Exception {
        Field field = findField(reader.getClass(), "postFilter");
        field.setAccessible(true);
        Class<?> postFilterType = field.getType();
        return java.lang.reflect.Proxy.newProxyInstance(
                postFilterType.getClassLoader(),
                new Class<?>[]{postFilterType},
                (proxy, method, args) -> switch (method.getName()) {
                    case "accept" -> false;
                    case "toString" -> "RejectAllMessagePostFilter";
                    case "hashCode" -> System.identityHashCode(proxy);
                    case "equals" -> proxy == args[0];
                    default -> method.invoke(proxy, args);
                });
    }

    private static void setPrivateField(Object target, String fieldName, Object value)
            throws Exception {
        Field field = findField(target.getClass(), fieldName);
        field.setAccessible(true);
        field.set(target, value);
    }

    private static Field findField(Class<?> clazz, String fieldName) throws NoSuchFieldException {
        Class<?> current = clazz;
        while (current != null) {
            try {
                return current.getDeclaredField(fieldName);
            } catch (NoSuchFieldException e) {
                current = current.getSuperclass();
            }
        }
        throw new NoSuchFieldException(fieldName);
    }

    private static Method findMethod(Class<?> clazz, String methodName, Class<?>... parameterTypes)
            throws NoSuchMethodException {
        Class<?> current = clazz;
        while (current != null) {
            try {
                return current.getDeclaredMethod(methodName, parameterTypes);
            } catch (NoSuchMethodException e) {
                current = current.getSuperclass();
            }
        }
        throw new NoSuchMethodException(methodName);
    }

    private static Object invokeStatic(String methodName, Class<?>[] parameterTypes, Object... args)
            throws Exception {
        Method method = findMethod(RabbitMQPartitionReader.class, methodName, parameterTypes);
        method.setAccessible(true);
        return method.invoke(null, args);
    }

    public static class AcceptAllPostFilter implements ConnectorPostFilter {
        @Override
        public boolean accept(ConnectorMessageView message) {
            return true;
        }
    }

    private static final class RejectAllPostFilter implements ConnectorPostFilter {
        @Override
        public boolean accept(ConnectorMessageView message) {
            return false;
        }
    }

    private static final class NoopConsumer implements com.rabbitmq.stream.Consumer {
        @Override
        public void store(long offset) {
        }

        @Override
        public void close() {
        }

        @Override
        public long storedOffset() {
            return 0L;
        }
    }

    private static final class NoopContext implements com.rabbitmq.stream.MessageHandler.Context {
        @Override
        public long offset() {
            return 0;
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
            return "test-stream";
        }

        @Override
        public com.rabbitmq.stream.Consumer consumer() {
            return null;
        }

    }

    private static final class InterruptingQueue<T>
            extends java.util.concurrent.LinkedBlockingQueue<T> {
        @Override
        public T poll(long timeout, java.util.concurrent.TimeUnit unit)
                throws InterruptedException {
            throw new InterruptedException("interrupt");
        }
    }

    private static final class InterruptingOfferQueue<T>
            extends java.util.concurrent.LinkedBlockingQueue<T> {
        @Override
        public boolean offer(T value, long timeout, java.util.concurrent.TimeUnit unit)
                throws InterruptedException {
            throw new InterruptedException("interrupt");
        }
    }

    private static final class SlowQueue<T>
            extends java.util.concurrent.LinkedBlockingQueue<T> {
        private final long delayMs;

        private SlowQueue(long delayMs) {
            this.delayMs = delayMs;
        }

        @Override
        public T poll(long timeout, java.util.concurrent.TimeUnit unit)
                throws InterruptedException {
            Thread.sleep(delayMs);
            return super.poll(timeout, unit);
        }
    }
}
