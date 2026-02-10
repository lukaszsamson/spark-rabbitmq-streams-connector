package com.rabbitmq.spark.connector;

import org.apache.spark.sql.connector.metric.CustomMetric;
import org.apache.spark.sql.connector.metric.CustomSumMetric;
import org.apache.spark.sql.connector.write.*;
import org.apache.spark.sql.connector.write.streaming.StreamingDataWriterFactory;
import org.apache.spark.sql.connector.write.streaming.StreamingWrite;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.*;

import static org.assertj.core.api.Assertions.*;

/**
 * Unit tests for the RabbitMQ Streams write path.
 *
 * <p>Tests cover WriteBuilder, Write, BatchWrite, StreamingWrite,
 * DataWriterFactory, WriterCommitMessage, and SinkMetrics without
 * requiring a real RabbitMQ broker.
 */
class RabbitMQWriteTest {

    // ======================================================================
    // WriteBuilder
    // ======================================================================

    @Nested
    class WriteBuilderTests {

        @Test
        void buildReturnsRabbitMQWrite() {
            ConnectorOptions opts = minimalSinkOptions();
            Write write = new RabbitMQWriteBuilder(opts, minimalSinkSchema(), "query-1").build();
            assertThat(write).isInstanceOf(RabbitMQWrite.class);
        }

        @Test
        void validatesOptionsAtConstruction() {
            // compression without subEntrySize should fail
            Map<String, String> map = minimalSinkMap();
            map.put("compression", "gzip");
            // subEntrySize not set → should fail
            ConnectorOptions opts = new ConnectorOptions(map);
            assertThatThrownBy(() ->
                    new RabbitMQWriteBuilder(opts, minimalSinkSchema(), "q"))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("subEntrySize");
        }

        @Test
        void validatesSchemaAtConstruction() {
            // Schema without 'value' column should fail
            StructType badSchema = new StructType(new StructField[]{
                    new StructField("not_value", DataTypes.StringType, true, Metadata.empty()),
            });
            ConnectorOptions opts = minimalSinkOptions();
            assertThatThrownBy(() ->
                    new RabbitMQWriteBuilder(opts, badSchema, "q"))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("value");
        }

        @Test
        void validatesUnknownColumnsRejectedByDefault() {
            StructType schemaWithExtra = new StructType(new StructField[]{
                    new StructField("value", DataTypes.BinaryType, false, Metadata.empty()),
                    new StructField("unknown_col", DataTypes.StringType, true, Metadata.empty()),
            });
            ConnectorOptions opts = minimalSinkOptions();
            assertThatThrownBy(() ->
                    new RabbitMQWriteBuilder(opts, schemaWithExtra, "q"))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("unknown_col");
        }

        @Test
        void unknownColumnsAllowedWhenIgnoreIsTrue() {
            Map<String, String> map = minimalSinkMap();
            map.put("ignoreUnknownColumns", "true");
            ConnectorOptions opts = new ConnectorOptions(map);
            StructType schemaWithExtra = new StructType(new StructField[]{
                    new StructField("value", DataTypes.BinaryType, false, Metadata.empty()),
                    new StructField("unknown_col", DataTypes.StringType, true, Metadata.empty()),
            });
            // Should not throw
            Write write = new RabbitMQWriteBuilder(opts, schemaWithExtra, "q").build();
            assertThat(write).isNotNull();
        }

        @Test
        void validatesCustomRoutingRequiresPartitionerClass() {
            Map<String, String> map = new LinkedHashMap<>();
            map.put("endpoints", "localhost:5552");
            map.put("superstream", "my-super");
            map.put("routingStrategy", "custom");
            // partitionerClass not set → should fail
            ConnectorOptions opts = new ConnectorOptions(map);
            assertThatThrownBy(() ->
                    new RabbitMQWriteBuilder(opts, minimalSinkSchema(), "q"))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("partitionerClass");
        }
    }

    // ======================================================================
    // Write
    // ======================================================================

    @Nested
    class WriteTests {

        @Test
        void descriptionContainsStreamName() {
            ConnectorOptions opts = minimalSinkOptions();
            Write write = new RabbitMQWriteBuilder(opts, minimalSinkSchema(), "q").build();
            assertThat(write.description()).contains("test-stream");
        }

        @Test
        void descriptionContainsSuperStreamName() {
            Map<String, String> map = new LinkedHashMap<>();
            map.put("endpoints", "localhost:5552");
            map.put("superstream", "my-super");
            ConnectorOptions opts = new ConnectorOptions(map);
            Write write = new RabbitMQWriteBuilder(opts, minimalSinkSchema(), "q").build();
            assertThat(write.description()).contains("my-super");
        }

        @Test
        void toBatchReturnsBatchWrite() {
            Write write = buildWrite();
            assertThat(write.toBatch()).isInstanceOf(RabbitMQBatchWrite.class);
        }

        @Test
        void toStreamingReturnsStreamingWrite() {
            Write write = buildWrite();
            assertThat(write.toStreaming()).isInstanceOf(RabbitMQStreamingWrite.class);
        }

        @Test
        void supportedCustomMetricsIncludesWriteMetrics() {
            Write write = buildWrite();
            CustomMetric[] metrics = write.supportedCustomMetrics();
            assertThat(metrics).hasSize(2);

            Set<String> names = new HashSet<>();
            for (CustomMetric m : metrics) {
                names.add(m.name());
                assertThat(m).isInstanceOf(CustomSumMetric.class);
            }
            assertThat(names).containsExactlyInAnyOrder("recordsWritten", "bytesWritten");
        }
    }

    // ======================================================================
    // BatchWrite
    // ======================================================================

    @Nested
    class BatchWriteTests {

        @Test
        void createBatchWriterFactoryReturnsDataWriterFactory() {
            Write write = buildWrite();
            BatchWrite batchWrite = write.toBatch();
            DataWriterFactory factory = batchWrite.createBatchWriterFactory(
                    mockPhysicalWriteInfo(4));
            assertThat(factory).isInstanceOf(RabbitMQDataWriterFactory.class);
        }

        @Test
        void useCommitCoordinatorIsTrue() {
            Write write = buildWrite();
            BatchWrite batchWrite = write.toBatch();
            assertThat(batchWrite.useCommitCoordinator()).isTrue();
        }

        @Test
        void commitDoesNotThrow() {
            Write write = buildWrite();
            BatchWrite batchWrite = write.toBatch();
            WriterCommitMessage[] messages = {
                    new RabbitMQWriterCommitMessage(0, 1, 100, 5000),
                    new RabbitMQWriterCommitMessage(1, 2, 200, 10000),
            };
            assertThatCode(() -> batchWrite.commit(messages)).doesNotThrowAnyException();
        }

        @Test
        void abortDoesNotThrow() {
            Write write = buildWrite();
            BatchWrite batchWrite = write.toBatch();
            assertThatCode(() -> batchWrite.abort(new WriterCommitMessage[0]))
                    .doesNotThrowAnyException();
        }
    }

    // ======================================================================
    // StreamingWrite
    // ======================================================================

    @Nested
    class StreamingWriteTests {

        @Test
        void createStreamingWriterFactoryReturnsFactory() {
            Write write = buildWrite();
            StreamingWrite streamingWrite = write.toStreaming();
            StreamingDataWriterFactory factory = streamingWrite.createStreamingWriterFactory(
                    mockPhysicalWriteInfo(2));
            assertThat(factory).isInstanceOf(RabbitMQDataWriterFactory.class);
        }

        @Test
        void useCommitCoordinatorIsTrue() {
            Write write = buildWrite();
            StreamingWrite streamingWrite = write.toStreaming();
            assertThat(streamingWrite.useCommitCoordinator()).isTrue();
        }

        @Test
        void commitDoesNotThrow() {
            Write write = buildWrite();
            StreamingWrite streamingWrite = write.toStreaming();
            WriterCommitMessage[] messages = {
                    new RabbitMQWriterCommitMessage(0, 1, 50, 2500),
            };
            assertThatCode(() -> streamingWrite.commit(0L, messages))
                    .doesNotThrowAnyException();
        }

        @Test
        void commitIsIdempotent() {
            Write write = buildWrite();
            StreamingWrite streamingWrite = write.toStreaming();
            WriterCommitMessage[] messages = {
                    new RabbitMQWriterCommitMessage(0, 1, 50, 2500),
            };
            // Call commit twice for the same epoch
            assertThatCode(() -> {
                streamingWrite.commit(0L, messages);
                streamingWrite.commit(0L, messages);
            }).doesNotThrowAnyException();
        }

        @Test
        void abortDoesNotThrow() {
            Write write = buildWrite();
            StreamingWrite streamingWrite = write.toStreaming();
            assertThatCode(() -> streamingWrite.abort(0L, new WriterCommitMessage[0]))
                    .doesNotThrowAnyException();
        }
    }

    // ======================================================================
    // WriterCommitMessage
    // ======================================================================

    @Nested
    class CommitMessageTests {

        @Test
        void carriesPartitionAndMetrics() {
            RabbitMQWriterCommitMessage msg = new RabbitMQWriterCommitMessage(3, 42, 100, 5000);
            assertThat(msg.getPartitionId()).isEqualTo(3);
            assertThat(msg.getTaskId()).isEqualTo(42);
            assertThat(msg.getRecordsWritten()).isEqualTo(100);
            assertThat(msg.getBytesWritten()).isEqualTo(5000);
        }

        @Test
        void toStringContainsInfo() {
            RabbitMQWriterCommitMessage msg = new RabbitMQWriterCommitMessage(1, 2, 50, 2000);
            String s = msg.toString();
            assertThat(s).contains("partitionId=1");
            assertThat(s).contains("recordsWritten=50");
            assertThat(s).contains("bytesWritten=2000");
        }

        @Test
        void isSerializable() {
            RabbitMQWriterCommitMessage msg = new RabbitMQWriterCommitMessage(0, 1, 10, 500);
            assertThat(msg).isInstanceOf(WriterCommitMessage.class);
            assertThat(msg).isInstanceOf(java.io.Serializable.class);
        }
    }

    // ======================================================================
    // DataWriterFactory
    // ======================================================================

    @Nested
    class DataWriterFactoryTests {

        @Test
        void factoryIsSerializable() {
            RabbitMQDataWriterFactory factory = new RabbitMQDataWriterFactory(
                    minimalSinkOptions(), minimalSinkSchema());
            assertThat(factory).isInstanceOf(java.io.Serializable.class);
            assertThat(factory).isInstanceOf(DataWriterFactory.class);
        }

        @Test
        void factoryIsAlsoStreamingFactory() {
            RabbitMQDataWriterFactory factory = new RabbitMQDataWriterFactory(
                    minimalSinkOptions(), minimalSinkSchema());
            assertThat(factory).isInstanceOf(StreamingDataWriterFactory.class);
        }

        @Test
        void createWriterReturnsDataWriter() {
            RabbitMQDataWriterFactory factory = new RabbitMQDataWriterFactory(
                    minimalSinkOptions(), minimalSinkSchema());
            // createWriter returns a DataWriter; it won't connect until write() is called
            var writer = factory.createWriter(0, 1);
            assertThat(writer).isInstanceOf(RabbitMQDataWriter.class);
            // Clean up
            assertThatCode(writer::close).doesNotThrowAnyException();
        }

        @Test
        void createStreamingWriterReturnsDataWriter() {
            RabbitMQDataWriterFactory factory = new RabbitMQDataWriterFactory(
                    minimalSinkOptions(), minimalSinkSchema());
            var writer = factory.createWriter(0, 1, 0L);
            assertThat(writer).isInstanceOf(RabbitMQDataWriter.class);
            assertThatCode(writer::close).doesNotThrowAnyException();
        }
    }

    // ======================================================================
    // SinkMetrics
    // ======================================================================

    @Nested
    class SinkMetricsTests {

        @Test
        void metricsHaveCorrectNames() {
            assertThat(RabbitMQSinkMetrics.RECORDS_WRITTEN).isEqualTo("recordsWritten");
            assertThat(RabbitMQSinkMetrics.BYTES_WRITTEN).isEqualTo("bytesWritten");
        }

        @Test
        void supportedMetricsAreSumMetrics() {
            for (CustomMetric metric : RabbitMQSinkMetrics.SUPPORTED_METRICS) {
                assertThat(metric).isInstanceOf(CustomSumMetric.class);
            }
        }

        @Test
        void taskMetricReturnsCorrectValues() {
            var metric = RabbitMQSinkMetrics.taskMetric("testMetric", 42);
            assertThat(metric.name()).isEqualTo("testMetric");
            assertThat(metric.value()).isEqualTo(42);
        }

        @Test
        void metricsHaveDescriptions() {
            for (CustomMetric metric : RabbitMQSinkMetrics.SUPPORTED_METRICS) {
                assertThat(metric.description()).isNotNull().isNotEmpty();
            }
        }
    }

    // ======================================================================
    // DataWriter (no-broker tests)
    // ======================================================================

    @Nested
    class DataWriterLifecycleTests {

        @Test
        void closeIsIdempotentWithoutInit() {
            RabbitMQDataWriter writer = new RabbitMQDataWriter(
                    minimalSinkOptions(), minimalSinkSchema(), 0, 1, -1);
            // close() without any write() should not throw
            assertThatCode(writer::close).doesNotThrowAnyException();
            assertThatCode(writer::close).doesNotThrowAnyException();
        }

        @Test
        void abortWithoutInitDoesNotThrow() {
            RabbitMQDataWriter writer = new RabbitMQDataWriter(
                    minimalSinkOptions(), minimalSinkSchema(), 0, 1, -1);
            assertThatCode(writer::abort).doesNotThrowAnyException();
        }

        @Test
        void metricsStartAtZero() {
            RabbitMQDataWriter writer = new RabbitMQDataWriter(
                    minimalSinkOptions(), minimalSinkSchema(), 0, 1, -1);
            var metrics = writer.currentMetricsValues();
            assertThat(metrics).hasSize(2);
            for (var m : metrics) {
                assertThat(m.value()).isEqualTo(0);
            }
        }
    }

    // ======================================================================
    // Table integration
    // ======================================================================

    @Nested
    class TableIntegration {

        @Test
        void newWriteBuilderReturnsBuildableWrite() {
            Map<String, String> map = minimalSinkMap();
            ConnectorOptions opts = new ConnectorOptions(map);
            opts.validateCommon();
            RabbitMQStreamTable table = new RabbitMQStreamTable(opts);

            // Create a mock LogicalWriteInfo
            StructType sinkSchema = minimalSinkSchema();
            WriteBuilder wb = table.newWriteBuilder(new TestLogicalWriteInfo(sinkSchema, "test-q"));
            assertThat(wb).isInstanceOf(RabbitMQWriteBuilder.class);

            Write write = wb.build();
            assertThat(write).isInstanceOf(RabbitMQWrite.class);
            assertThat(write.toBatch()).isInstanceOf(RabbitMQBatchWrite.class);
            assertThat(write.toStreaming()).isInstanceOf(RabbitMQStreamingWrite.class);
        }
    }

    // ======================================================================
    // Helpers
    // ======================================================================

    private static Map<String, String> minimalSinkMap() {
        Map<String, String> map = new LinkedHashMap<>();
        map.put("endpoints", "localhost:5552");
        map.put("stream", "test-stream");
        return map;
    }

    private static ConnectorOptions minimalSinkOptions() {
        return new ConnectorOptions(minimalSinkMap());
    }

    private static StructType minimalSinkSchema() {
        return new StructType(new StructField[]{
                new StructField("value", DataTypes.BinaryType, false, Metadata.empty()),
        });
    }

    private static Write buildWrite() {
        return new RabbitMQWriteBuilder(minimalSinkOptions(), minimalSinkSchema(), "q").build();
    }

    private static PhysicalWriteInfo mockPhysicalWriteInfo(int numPartitions) {
        return () -> numPartitions;
    }

    /**
     * Minimal LogicalWriteInfo implementation for testing.
     */
    private static class TestLogicalWriteInfo implements LogicalWriteInfo {
        private final StructType schema;
        private final String queryId;

        TestLogicalWriteInfo(StructType schema, String queryId) {
            this.schema = schema;
            this.queryId = queryId;
        }

        @Override
        public org.apache.spark.sql.util.CaseInsensitiveStringMap options() {
            return new org.apache.spark.sql.util.CaseInsensitiveStringMap(Map.of());
        }

        @Override
        public String queryId() {
            return queryId;
        }

        @Override
        public StructType schema() {
            return schema;
        }
    }
}
