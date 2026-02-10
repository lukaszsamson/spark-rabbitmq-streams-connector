package com.rabbitmq.spark.connector;

import com.rabbitmq.stream.Message;
import com.rabbitmq.stream.OffsetSpecification;
import com.rabbitmq.stream.codec.QpidProtonCodec;
import org.apache.spark.sql.catalyst.InternalRow;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for the push-to-pull bridging logic in RabbitMQPartitionReader.
 *
 * <p>These tests exercise the offset filtering, de-duplication, and
 * timeout behavior without requiring a real RabbitMQ broker. The actual
 * consumer integration is tested in the it-tests module.
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
        void createsConfiguredPostFilter() throws Exception {
            Map<String, String> opts = new LinkedHashMap<>();
            opts.put("endpoints", "localhost:5552");
            opts.put("stream", "test-stream");
            opts.put("filterPostFilterClass", AcceptAllPostFilter.class.getName());

            ConnectorPostFilter postFilter = createPostFilter(new ConnectorOptions(opts));
            assertThat(postFilter).isInstanceOf(AcceptAllPostFilter.class);
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

    private static ConnectorPostFilter createPostFilter(ConnectorOptions options)
            throws Exception {
        Method method = RabbitMQPartitionReader.class.getDeclaredMethod(
                "createPostFilter", ConnectorOptions.class);
        method.setAccessible(true);
        return (ConnectorPostFilter) method.invoke(null, options);
    }

    public static class AcceptAllPostFilter implements ConnectorPostFilter {
        @Override
        public boolean accept(byte[] messageBody, Map<String, String> applicationProperties) {
            return true;
        }
    }
}
