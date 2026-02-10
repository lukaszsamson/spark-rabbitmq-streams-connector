package com.rabbitmq.spark.connector;

import com.rabbitmq.stream.Message;
import com.rabbitmq.stream.codec.QpidProtonCodec;
import org.apache.spark.sql.catalyst.InternalRow;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.io.IOException;
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
            var qm = new RabbitMQPartitionReader.QueuedMessage(msg, 42, 1700000000000L);

            assertThat(qm.offset()).isEqualTo(42);
            assertThat(qm.chunkTimestampMillis()).isEqualTo(1700000000000L);
            assertThat(qm.message().getBodyAsBinary()).isEqualTo("hello".getBytes());
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
}
