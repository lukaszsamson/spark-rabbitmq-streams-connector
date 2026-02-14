package com.rabbitmq.spark.connector;

import com.rabbitmq.stream.Message;
import com.rabbitmq.stream.OffsetSpecification;
import com.rabbitmq.stream.codec.QpidProtonCodec;
import org.apache.spark.sql.catalyst.InternalRow;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.lang.reflect.Method;
import java.lang.reflect.Field;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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
            assertThat(reader.next()).isTrue();
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
        void nextAppliesPostFilterWithWarningToggle() throws Exception {
            Map<String, String> opts = new LinkedHashMap<>();
            opts.put("endpoints", "localhost:5552");
            opts.put("stream", "test-stream");
            opts.put("filterWarningOnMismatch", "false");

            RabbitMQInputPartition partition = new RabbitMQInputPartition(
                    "test-stream", 0, 3, new ConnectorOptions(opts));
            RabbitMQPartitionReader reader = new RabbitMQPartitionReader(partition, partition.getOptions());

            setPrivateField(reader, "postFilter", new RejectAllPostFilter());

            BlockingQueue<RabbitMQPartitionReader.QueuedMessage> queue = new LinkedBlockingQueue<>();
            queue.add(new RabbitMQPartitionReader.QueuedMessage(
                    CODEC.messageBuilder().addData("a".getBytes()).build(),
                    1L, 0L, new NoopContext()));
            queue.add(new RabbitMQPartitionReader.QueuedMessage(
                    CODEC.messageBuilder().addData("b".getBytes()).build(),
                    2L, 0L, new NoopContext()));

            setPrivateField(reader, "queue", queue);
            setPrivateField(reader, "consumer", new NoopConsumer());

            assertThat(reader.next()).isFalse();
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
            assertThat(metrics[2].value()).isGreaterThan(0L);
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
        void closeDrainsMetricsIntoMessageSizeTrackerOnce() throws Exception {
            RabbitMQInputPartition partition = new RabbitMQInputPartition(
                    "test-stream", 0, 10, minimalOptions());
            RabbitMQPartitionReader reader = new RabbitMQPartitionReader(partition, partition.getOptions());

            setPrivateField(reader, "bytesRead", 20L);
            setPrivateField(reader, "recordsRead", 2L);

            reader.close();
            int first = MessageSizeTracker.drainAverage(100);
            int second = MessageSizeTracker.drainAverage(100);

            assertThat(first).isEqualTo(10);
            assertThat(second).isEqualTo(100);
        }

        @Test
        void closeIsIdempotentWhenCalledTwice() throws Exception {
            RabbitMQInputPartition partition = new RabbitMQInputPartition(
                    "test-stream", 0, 10, minimalOptions());
            RabbitMQPartitionReader reader = new RabbitMQPartitionReader(partition, partition.getOptions());

            setPrivateField(reader, "bytesRead", 20L);
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
            setPrivateField(reader, "bytesRead", 30L);
            setPrivateField(reader, "readLatencyMs", 7L);

            var metrics = reader.currentMetricsValues();
            assertThat(metrics).hasSize(3);
            assertThat(metrics[0].value()).isEqualTo(3L);
            assertThat(metrics[1].value()).isEqualTo(30L);
            assertThat(metrics[2].value()).isEqualTo(7L);
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
        void createsConfiguredPostFilter() throws Exception {
            Map<String, String> opts = new LinkedHashMap<>();
            opts.put("endpoints", "localhost:5552");
            opts.put("stream", "test-stream");
            opts.put("filterPostFilterClass", AcceptAllPostFilter.class.getName());

            ConnectorPostFilter postFilter = createPostFilter(new ConnectorOptions(opts));
            assertThat(postFilter).isInstanceOf(AcceptAllPostFilter.class);
        }

        @Test
        void derivesPostFilterFromFilterValuesAndFilterValueColumn() throws Exception {
            Map<String, String> opts = new LinkedHashMap<>();
            opts.put("endpoints", "localhost:5552");
            opts.put("stream", "test-stream");
            opts.put("filterValues", "alpha,beta");
            opts.put("filterValueColumn", "region");

            ConnectorPostFilter postFilter = createPostFilter(new ConnectorOptions(opts));
            assertThat(postFilter).isNotNull();
            assertThat(postFilter.accept(new byte[0], Map.of("region", "alpha"))).isTrue();
            assertThat(postFilter.accept(new byte[0], Map.of("region", "gamma"))).isFalse();
            assertThat(postFilter.accept(new byte[0], Map.of())).isFalse();
        }

        @Test
        void derivedPostFilterCanMatchUnfilteredMessages() throws Exception {
            Map<String, String> opts = new LinkedHashMap<>();
            opts.put("endpoints", "localhost:5552");
            opts.put("stream", "test-stream");
            opts.put("filterValues", "alpha");
            opts.put("filterValueColumn", "region");
            opts.put("filterMatchUnfiltered", "true");

            ConnectorPostFilter postFilter = createPostFilter(new ConnectorOptions(opts));
            assertThat(postFilter).isNotNull();
            assertThat(postFilter.accept(new byte[0], Map.of())).isTrue();
            assertThat(postFilter.accept(new byte[0], null)).isTrue();
        }

        @Test
        void doesNotDerivePostFilterWithoutFilterValueColumn() throws Exception {
            Map<String, String> opts = new LinkedHashMap<>();
            opts.put("endpoints", "localhost:5552");
            opts.put("stream", "test-stream");
            opts.put("filterValues", "alpha,beta");

            ConnectorPostFilter postFilter = createPostFilter(new ConnectorOptions(opts));
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
        Method method = RabbitMQPartitionReader.class.getDeclaredMethod("resolveOffsetSpec");
        method.setAccessible(true);
        return (OffsetSpecification) method.invoke(reader);
    }

    private static String resolveSingleActiveConsumerName(RabbitMQPartitionReader reader)
            throws Exception {
        Method method = RabbitMQPartitionReader.class.getDeclaredMethod(
                "resolveSingleActiveConsumerName");
        method.setAccessible(true);
        return (String) method.invoke(reader);
    }

    private static ConnectorPostFilter createPostFilter(ConnectorOptions options)
            throws Exception {
        Method method = RabbitMQPartitionReader.class.getDeclaredMethod(
                "createPostFilter", ConnectorOptions.class);
        method.setAccessible(true);
        return (ConnectorPostFilter) method.invoke(null, options);
    }

    private static void setPrivateField(Object target, String fieldName, Object value)
            throws Exception {
        Field field = target.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(target, value);
    }

    private static Object invokeStatic(String methodName, Class<?>[] parameterTypes, Object... args)
            throws Exception {
        Method method = RabbitMQPartitionReader.class.getDeclaredMethod(methodName, parameterTypes);
        method.setAccessible(true);
        return method.invoke(null, args);
    }

    public static class AcceptAllPostFilter implements ConnectorPostFilter {
        @Override
        public boolean accept(byte[] messageBody, Map<String, String> applicationProperties) {
            return true;
        }
    }

    private static final class RejectAllPostFilter implements ConnectorPostFilter {
        @Override
        public boolean accept(byte[] messageBody, Map<String, String> applicationProperties) {
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
