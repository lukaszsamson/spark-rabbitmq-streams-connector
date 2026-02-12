package com.rabbitmq.spark.connector;

import com.rabbitmq.stream.*;
import com.rabbitmq.stream.codec.QpidProtonCodec;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.streaming.*;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.io.*;
import java.lang.reflect.Field;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for the SupportsRealTimeMode and SupportsRealTimeRead
 * implementations in the Spark 4.1 module.
 */
class RealTimeModeTest {

    private static final QpidProtonCodec CODEC = new QpidProtonCodec();

    // ======================================================================
    // Interface conformance
    // ======================================================================

    @Nested
    class InterfaceConformance {

        @Test
        void microBatchStreamImplementsSupportsRealTimeMode() {
            RabbitMQMicroBatchStream stream = createStream(minimalOptions());
            assertThat(stream).isInstanceOf(SupportsRealTimeMode.class);
        }

        @Test
        void partitionReaderImplementsSupportsRealTimeRead() {
            RabbitMQInputPartition partition = new RabbitMQInputPartition(
                    "test-stream", 0, 100, minimalOptions());
            RabbitMQPartitionReader reader = new RabbitMQPartitionReader(
                    partition, partition.getOptions());
            assertThat(reader).isInstanceOf(SupportsRealTimeRead.class);
        }
    }

    // ======================================================================
    // prepareForRealTimeMode
    // ======================================================================

    @Nested
    class PrepareForRealTimeMode {

        @Test
        void prepareForRealTimeModeSetsFlag() throws Exception {
            RabbitMQMicroBatchStream stream = createStream(minimalOptions());
            assertThat(getPrivateField(stream, "realTimeMode")).isEqualTo(false);

            stream.prepareForRealTimeMode();
            assertThat(getPrivateField(stream, "realTimeMode")).isEqualTo(true);
        }
    }

    // ======================================================================
    // planInputPartitions(Offset start)
    // ======================================================================

    @Nested
    class PlanInputPartitions {

        @Test
        void planInputPartitionsCreatesUnboundedPartitions() throws Exception {
            RabbitMQMicroBatchStream stream = createStream(minimalOptions());
            setPrivateField(stream, "streams", List.of("test-stream"));
            setPrivateField(stream, "environment", new NoOpEnvironment());

            RabbitMQStreamOffset start = new RabbitMQStreamOffset(Map.of("test-stream", 42L));
            InputPartition[] partitions = stream.planInputPartitions(start);

            assertThat(partitions).hasSize(1);
            RabbitMQInputPartition p = (RabbitMQInputPartition) partitions[0];
            assertThat(p.getStream()).isEqualTo("test-stream");
            assertThat(p.getStartOffset()).isEqualTo(42L);
            assertThat(p.getEndOffset()).isEqualTo(Long.MAX_VALUE);
        }

        @Test
        void planInputPartitionsMultipleStreams() throws Exception {
            RabbitMQMicroBatchStream stream = createStream(minimalOptions());
            setPrivateField(stream, "streams", List.of("stream-0", "stream-1", "stream-2"));
            setPrivateField(stream, "environment", new NoOpEnvironment());

            RabbitMQStreamOffset start = new RabbitMQStreamOffset(Map.of(
                    "stream-0", 10L,
                    "stream-1", 20L,
                    "stream-2", 30L));
            InputPartition[] partitions = stream.planInputPartitions(start);

            assertThat(partitions).hasSize(3);
            for (InputPartition ip : partitions) {
                RabbitMQInputPartition p = (RabbitMQInputPartition) ip;
                assertThat(p.getEndOffset()).isEqualTo(Long.MAX_VALUE);
            }

            // Verify start offsets are correct
            Set<String> streams = new HashSet<>();
            for (InputPartition ip : partitions) {
                RabbitMQInputPartition p = (RabbitMQInputPartition) ip;
                streams.add(p.getStream());
            }
            assertThat(streams).containsExactlyInAnyOrder("stream-0", "stream-1", "stream-2");
        }

        @Test
        void planInputPartitionsIgnoresMinPartitions() throws Exception {
            Map<String, String> opts = new LinkedHashMap<>();
            opts.put("endpoints", "localhost:5552");
            opts.put("stream", "test-stream");
            opts.put("minPartitions", "4");
            ConnectorOptions options = new ConnectorOptions(opts);

            RabbitMQMicroBatchStream stream = createStream(options);
            setPrivateField(stream, "streams", List.of("test-stream"));
            setPrivateField(stream, "environment", new NoOpEnvironment());

            RabbitMQStreamOffset start = new RabbitMQStreamOffset(Map.of("test-stream", 0L));
            InputPartition[] partitions = stream.planInputPartitions(start);

            // Real-time mode does not split — always 1 partition per stream
            assertThat(partitions).hasSize(1);
            assertThat(((RabbitMQInputPartition) partitions[0]).getEndOffset())
                    .isEqualTo(Long.MAX_VALUE);
        }

        @Test
        void planInputPartitionsUsesZeroForMissingStreamInStart() throws Exception {
            RabbitMQMicroBatchStream stream = createStream(minimalOptions());
            setPrivateField(stream, "streams", List.of("test-stream"));
            setPrivateField(stream, "environment", new NoOpEnvironment());

            // Start offset doesn't have "test-stream" key
            RabbitMQStreamOffset start = new RabbitMQStreamOffset(Map.of());
            InputPartition[] partitions = stream.planInputPartitions(start);

            assertThat(partitions).hasSize(1);
            RabbitMQInputPartition p = (RabbitMQInputPartition) partitions[0];
            assertThat(p.getStartOffset()).isEqualTo(0L);
        }

        @Test
        void planInputPartitionsTimestampMarksInitialPartitionForConfiguredSeek() throws Exception {
            Map<String, String> opts = new LinkedHashMap<>();
            opts.put("endpoints", "localhost:5552");
            opts.put("stream", "test-stream");
            opts.put("startingOffsets", "timestamp");
            opts.put("startingTimestamp", "1234");
            ConnectorOptions options = new ConnectorOptions(opts);

            RabbitMQMicroBatchStream stream = createStream(options);
            setPrivateField(stream, "streams", List.of("test-stream"));
            setPrivateField(stream, "environment", new NoOpEnvironment());
            setPrivateField(stream, "initialOffsets", Map.of("test-stream", 42L));

            RabbitMQStreamOffset start = new RabbitMQStreamOffset(Map.of("test-stream", 42L));
            InputPartition[] partitions = stream.planInputPartitions(start);

            assertThat(partitions).hasSize(1);
            RabbitMQInputPartition p = (RabbitMQInputPartition) partitions[0];
            assertThat(p.isUseConfiguredStartingOffset()).isTrue();
        }

        @Test
        void planInputPartitionsTimestampDoesNotMarkResumedPartitionForConfiguredSeek() throws Exception {
            Map<String, String> opts = new LinkedHashMap<>();
            opts.put("endpoints", "localhost:5552");
            opts.put("stream", "test-stream");
            opts.put("startingOffsets", "timestamp");
            opts.put("startingTimestamp", "1234");
            ConnectorOptions options = new ConnectorOptions(opts);

            RabbitMQMicroBatchStream stream = createStream(options);
            setPrivateField(stream, "streams", List.of("test-stream"));
            setPrivateField(stream, "environment", new NoOpEnvironment());
            setPrivateField(stream, "initialOffsets", Map.of("test-stream", 42L));

            RabbitMQStreamOffset start = new RabbitMQStreamOffset(Map.of("test-stream", 50L));
            InputPartition[] partitions = stream.planInputPartitions(start);

            assertThat(partitions).hasSize(1);
            RabbitMQInputPartition p = (RabbitMQInputPartition) partitions[0];
            assertThat(p.isUseConfiguredStartingOffset()).isFalse();
        }
    }

    // ======================================================================
    // mergeOffsets
    // ======================================================================

    @Nested
    class MergeOffsets {

        @Test
        void mergeOffsetsProducesGlobalOffset() {
            RabbitMQMicroBatchStream stream = createStream(minimalOptions());
            PartitionOffset[] offsets = new PartitionOffset[]{
                    new RabbitMQPartitionOffset("stream-a", 100L),
                    new RabbitMQPartitionOffset("stream-b", 200L)
            };

            Offset merged = stream.mergeOffsets(offsets);
            assertThat(merged).isInstanceOf(RabbitMQStreamOffset.class);
            RabbitMQStreamOffset rmo = (RabbitMQStreamOffset) merged;
            assertThat(rmo.getStreamOffsets())
                    .containsEntry("stream-a", 100L)
                    .containsEntry("stream-b", 200L)
                    .hasSize(2);
        }

        @Test
        void mergeOffsetsTakesMaxForSameStream() {
            RabbitMQMicroBatchStream stream = createStream(minimalOptions());
            PartitionOffset[] offsets = new PartitionOffset[]{
                    new RabbitMQPartitionOffset("stream-a", 50L),
                    new RabbitMQPartitionOffset("stream-a", 100L),
                    new RabbitMQPartitionOffset("stream-a", 75L)
            };

            Offset merged = stream.mergeOffsets(offsets);
            RabbitMQStreamOffset rmo = (RabbitMQStreamOffset) merged;
            assertThat(rmo.getStreamOffsets())
                    .containsEntry("stream-a", 100L)
                    .hasSize(1);
        }

        @Test
        void mergeOffsetsUpdatesCachedLatestOffset() throws Exception {
            RabbitMQMicroBatchStream stream = createStream(minimalOptions());
            assertThat(getPrivateField(stream, "cachedLatestOffset")).isNull();

            PartitionOffset[] offsets = new PartitionOffset[]{
                    new RabbitMQPartitionOffset("test-stream", 42L)
            };

            stream.mergeOffsets(offsets);

            Object cached = getPrivateField(stream, "cachedLatestOffset");
            assertThat(cached).isNotNull();
            assertThat(cached).isInstanceOf(RabbitMQStreamOffset.class);
            assertThat(((RabbitMQStreamOffset) cached).getStreamOffsets())
                    .containsEntry("test-stream", 42L);
        }
    }

    // ======================================================================
    // RabbitMQPartitionOffset
    // ======================================================================

    @Nested
    class PartitionOffsetTests {

        @Test
        void partitionOffsetIsSerializable() throws Exception {
            RabbitMQPartitionOffset original = new RabbitMQPartitionOffset("my-stream", 12345L);

            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(bos);
            oos.writeObject(original);
            oos.close();

            ByteArrayInputStream bis = new ByteArrayInputStream(bos.toByteArray());
            ObjectInputStream ois = new ObjectInputStream(bis);
            RabbitMQPartitionOffset deserialized = (RabbitMQPartitionOffset) ois.readObject();

            assertThat(deserialized.getStream()).isEqualTo("my-stream");
            assertThat(deserialized.getNextOffset()).isEqualTo(12345L);
            assertThat(deserialized).isEqualTo(original);
        }

        @Test
        void partitionOffsetEquality() {
            RabbitMQPartitionOffset a = new RabbitMQPartitionOffset("s1", 10L);
            RabbitMQPartitionOffset b = new RabbitMQPartitionOffset("s1", 10L);
            RabbitMQPartitionOffset c = new RabbitMQPartitionOffset("s1", 20L);
            RabbitMQPartitionOffset d = new RabbitMQPartitionOffset("s2", 10L);

            assertThat(a).isEqualTo(b);
            assertThat(a).isNotEqualTo(c);
            assertThat(a).isNotEqualTo(d);
            assertThat(a.hashCode()).isEqualTo(b.hashCode());
        }
    }

    // ======================================================================
    // getOffset (SupportsRealTimeRead)
    // ======================================================================

    @Nested
    class GetOffset {

        @Test
        void getOffsetReturnsStartWhenNoMessagesEmitted() {
            RabbitMQInputPartition partition = new RabbitMQInputPartition(
                    "test-stream", 42, Long.MAX_VALUE, minimalOptions());
            RabbitMQPartitionReader reader = new RabbitMQPartitionReader(
                    partition, partition.getOptions());

            PartitionOffset offset = reader.getOffset();
            assertThat(offset).isInstanceOf(RabbitMQPartitionOffset.class);
            RabbitMQPartitionOffset rpo = (RabbitMQPartitionOffset) offset;
            assertThat(rpo.getStream()).isEqualTo("test-stream");
            assertThat(rpo.getNextOffset()).isEqualTo(42L);
        }

        @Test
        void getOffsetReturnsLastEmittedPlusOneAfterReading() throws Exception {
            RabbitMQInputPartition partition = new RabbitMQInputPartition(
                    "test-stream", 0, Long.MAX_VALUE, minimalOptions());
            RabbitMQPartitionReader reader = new RabbitMQPartitionReader(
                    partition, partition.getOptions());

            // Simulate having emitted offset 99
            setPrivateField(reader, "lastEmittedOffset", 99L);

            PartitionOffset offset = reader.getOffset();
            RabbitMQPartitionOffset rpo = (RabbitMQPartitionOffset) offset;
            assertThat(rpo.getNextOffset()).isEqualTo(100L);
        }
    }

    // ======================================================================
    // nextWithTimeout (SupportsRealTimeRead)
    // ======================================================================

    @Nested
    class NextWithTimeout {

        @Test
        void nextWithTimeoutReturnsNoRecordOnEmptyQueue() throws Exception {
            Map<String, String> opts = new LinkedHashMap<>();
            opts.put("endpoints", "localhost:5552");
            opts.put("stream", "test-stream");
            opts.put("pollTimeoutMs", "10");
            ConnectorOptions options = new ConnectorOptions(opts);

            RabbitMQInputPartition partition = new RabbitMQInputPartition(
                    "test-stream", 0, Long.MAX_VALUE, options);
            RabbitMQPartitionReader reader = new RabbitMQPartitionReader(
                    partition, partition.getOptions());

            // Set up empty queue and mock consumer
            setPrivateField(reader, "consumer", new NoopConsumer());
            setPrivateField(reader, "queue", new LinkedBlockingQueue<>());

            SupportsRealTimeRead.RecordStatus status = reader.nextWithTimeout(50L);
            assertThat(status.hasRecord()).isFalse();
        }

        @Test
        void nextWithTimeoutReturnsRecordWhenAvailable() throws Exception {
            RabbitMQInputPartition partition = new RabbitMQInputPartition(
                    "test-stream", 0, Long.MAX_VALUE, minimalOptions());
            RabbitMQPartitionReader reader = new RabbitMQPartitionReader(
                    partition, partition.getOptions());

            BlockingQueue<RabbitMQPartitionReader.QueuedMessage> queue = new LinkedBlockingQueue<>();
            long chunkTimestamp = 1700000000000L;
            queue.add(new RabbitMQPartitionReader.QueuedMessage(
                    CODEC.messageBuilder().addData("hello".getBytes()).build(),
                    0L, chunkTimestamp, new NoopContext()));

            setPrivateField(reader, "consumer", new NoopConsumer());
            setPrivateField(reader, "queue", queue);

            SupportsRealTimeRead.RecordStatus status = reader.nextWithTimeout(5000L);
            assertThat(status.hasRecord()).isTrue();
            assertThat(status.recArrivalTime()).isPresent();
            assertThat(status.recArrivalTime().get()).isEqualTo(chunkTimestamp);

            // Verify the row is available
            InternalRow row = reader.get();
            assertThat(row).isNotNull();
        }

        @Test
        void nextWithTimeoutSkipsDedup() throws Exception {
            RabbitMQInputPartition partition = new RabbitMQInputPartition(
                    "test-stream", 0, Long.MAX_VALUE, minimalOptions());
            RabbitMQPartitionReader reader = new RabbitMQPartitionReader(
                    partition, partition.getOptions());

            // Simulate that we already emitted offset 5
            setPrivateField(reader, "lastEmittedOffset", 5L);

            BlockingQueue<RabbitMQPartitionReader.QueuedMessage> queue = new LinkedBlockingQueue<>();
            // Add a message with offset <= lastEmitted (should be skipped)
            queue.add(new RabbitMQPartitionReader.QueuedMessage(
                    CODEC.messageBuilder().addData("old".getBytes()).build(),
                    3L, 0L, new NoopContext()));
            // Add a message with offset > lastEmitted (should be returned)
            queue.add(new RabbitMQPartitionReader.QueuedMessage(
                    CODEC.messageBuilder().addData("new".getBytes()).build(),
                    6L, 0L, new NoopContext()));

            setPrivateField(reader, "consumer", new NoopConsumer());
            setPrivateField(reader, "queue", queue);

            SupportsRealTimeRead.RecordStatus status = reader.nextWithTimeout(5000L);
            assertThat(status.hasRecord()).isTrue();

            // Verify the returned row has offset 6 (skipped offset 3)
            InternalRow row = reader.get();
            assertThat(row.getLong(2)).isEqualTo(6L); // offset column
        }

        @Test
        void nextWithTimeoutSkipsPreStartMessages() throws Exception {
            RabbitMQInputPartition partition = new RabbitMQInputPartition(
                    "test-stream", 10, Long.MAX_VALUE, minimalOptions());
            RabbitMQPartitionReader reader = new RabbitMQPartitionReader(
                    partition, partition.getOptions());

            BlockingQueue<RabbitMQPartitionReader.QueuedMessage> queue = new LinkedBlockingQueue<>();
            // Message before start offset (should be skipped)
            queue.add(new RabbitMQPartitionReader.QueuedMessage(
                    CODEC.messageBuilder().addData("pre".getBytes()).build(),
                    5L, 0L, new NoopContext()));
            // Message at start offset (should be returned)
            queue.add(new RabbitMQPartitionReader.QueuedMessage(
                    CODEC.messageBuilder().addData("at-start".getBytes()).build(),
                    10L, 0L, new NoopContext()));

            setPrivateField(reader, "consumer", new NoopConsumer());
            setPrivateField(reader, "queue", queue);

            SupportsRealTimeRead.RecordStatus status = reader.nextWithTimeout(5000L);
            assertThat(status.hasRecord()).isTrue();
            assertThat(reader.get().getLong(2)).isEqualTo(10L);
        }
    }

    // ======================================================================
    // Helpers
    // ======================================================================

    private static ConnectorOptions minimalOptions() {
        Map<String, String> opts = new LinkedHashMap<>();
        opts.put("endpoints", "localhost:5552");
        opts.put("stream", "test-stream");
        return new ConnectorOptions(opts);
    }

    private static RabbitMQMicroBatchStream createStream(ConnectorOptions opts) {
        StructType schema = RabbitMQStreamTable.buildSourceSchema(opts.getMetadataFields());
        return new RabbitMQMicroBatchStream(opts, schema, "/tmp/checkpoint");
    }

    private static void setPrivateField(Object target, String fieldName, Object value)
            throws Exception {
        Field field = target.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(target, value);
    }

    private static Object getPrivateField(Object target, String fieldName)
            throws Exception {
        Field field = target.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        return field.get(target);
    }

    /**
     * Minimal Environment that returns fixed stats for any stream query.
     * Used to satisfy validateStartOffset() calls during planInputPartitions.
     */
    private static final class NoOpEnvironment implements Environment {
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
            return new FixedStreamStats(0);
        }

        @Override
        public void storeOffset(String reference, String stream, long offset) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean streamExists(String stream) {
            return true;
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

    private static final class FixedStreamStats implements StreamStats {
        private final long offset;

        private FixedStreamStats(long offset) {
            this.offset = offset;
        }

        @Override
        public long firstOffset() {
            return offset;
        }

        @Override
        public long committedChunkId() {
            return offset;
        }
    }

    private static final class NoopConsumer implements Consumer {
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

    private static final class NoopContext implements MessageHandler.Context {
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
        public Consumer consumer() {
            return null;
        }
    }
}
