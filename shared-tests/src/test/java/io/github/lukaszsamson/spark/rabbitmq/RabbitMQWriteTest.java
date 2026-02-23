package io.github.lukaszsamson.spark.rabbitmq;

import org.apache.spark.sql.connector.metric.CustomMetric;
import org.apache.spark.sql.connector.metric.CustomSumMetric;
import org.apache.spark.sql.connector.write.*;
import org.apache.spark.sql.connector.write.streaming.StreamingDataWriterFactory;
import org.apache.spark.sql.connector.write.streaming.StreamingWrite;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import static org.assertj.core.api.Assertions.*;

/**
 * Unit tests for the RabbitMQ Streams write path.
 *
 * <p>Tests cover WriteBuilder, Write, BatchWrite, StreamingWrite,
 * DataWriterFactory, WriterCommitMessage, and SinkMetrics without
 * requiring a real RabbitMQ broker.
 */
class RabbitMQWriteTest {

    @AfterEach
    void tearDown() {
        EnvironmentPool.getInstance().closeAll();
    }

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

        @Test
        void validatesHashFunctionClassRequiresSuperStreamHashRouting() {
            Map<String, String> map = new LinkedHashMap<>();
            map.put("endpoints", "localhost:5552");
            map.put("stream", "my-stream");
            map.put("hashFunctionClass", TestHashFunction.class.getName());
            ConnectorOptions opts = new ConnectorOptions(map);
            assertThatThrownBy(() ->
                    new RabbitMQWriteBuilder(opts, minimalSinkSchema(), "q"))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("hashFunctionClass");
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
            assertThat(metrics).hasSize(6);

            Set<String> names = new HashSet<>();
            for (CustomMetric m : metrics) {
                names.add(m.name());
                assertThat(m).isInstanceOf(CustomSumMetric.class);
            }
            assertThat(names).containsExactlyInAnyOrder(
                    "recordsWritten", "payloadBytesWritten", "estimatedWireBytesWritten",
                    "writeLatencyMs",
                    "publishConfirms", "publishErrors");
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
        void commitResetsWriterCommitMessages() {
            Write write = buildWrite();
            BatchWrite batchWrite = write.toBatch();
            WriterCommitMessage[] messages = {
                    new RabbitMQWriterCommitMessage(0, 1, 100, 5000),
                    new RabbitMQWriterCommitMessage(1, 2, 200, 10000),
            };
            batchWrite.commit(messages);
            assertThat(messages[0]).isNull();
            assertThat(messages[1]).isNull();
        }

        @Test
        void abortDoesNotClearMessages() {
            Write write = buildWrite();
            BatchWrite batchWrite = write.toBatch();
            WriterCommitMessage[] messages = {
                    new RabbitMQWriterCommitMessage(0, 1, 100, 5000)
            };
            batchWrite.abort(messages);
            assertThat(messages[0]).isNotNull();
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
        void useCommitCoordinatorIsFalse() {
            Write write = buildWrite();
            StreamingWrite streamingWrite = write.toStreaming();
            assertThat(streamingWrite.useCommitCoordinator()).isFalse();
        }

        @Test
        void commitResetsWriterCommitMessages() {
            Write write = buildWrite();
            StreamingWrite streamingWrite = write.toStreaming();
            WriterCommitMessage[] messages = {
                    new RabbitMQWriterCommitMessage(0, 1, 50, 2500),
            };
            streamingWrite.commit(0L, messages);
            assertThat(messages[0]).isNull();
        }

        @Test
        void commitIsIdempotentOnClearedMessages() {
            Write write = buildWrite();
            StreamingWrite streamingWrite = write.toStreaming();
            WriterCommitMessage[] messages = {
                    new RabbitMQWriterCommitMessage(0, 1, 50, 2500),
            };
            // Call commit twice for the same epoch
            streamingWrite.commit(0L, messages);
            streamingWrite.commit(0L, messages);
            assertThat(messages[0]).isNull();
        }

        @Test
        void abortDoesNotClearMessages() {
            Write write = buildWrite();
            StreamingWrite streamingWrite = write.toStreaming();
            WriterCommitMessage[] messages = {
                    new RabbitMQWriterCommitMessage(0, 1, 50, 2500),
            };
            streamingWrite.abort(0L, messages);
            assertThat(messages[0]).isNotNull();
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
            RabbitMQWriterCommitMessage restored = serializeRoundTrip(msg);
            assertThat(restored.getPartitionId()).isEqualTo(0);
            assertThat(restored.getTaskId()).isEqualTo(1);
            assertThat(restored.getRecordsWritten()).isEqualTo(10);
            assertThat(restored.getBytesWritten()).isEqualTo(500);
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
            Object restored = serializeRoundTrip(factory);
            assertThat(restored).isInstanceOf(RabbitMQDataWriterFactory.class);
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
            assertThat(writer.currentMetricsValues()).hasSize(6);
        }

        @Test
        void createStreamingWriterReturnsDataWriter() {
            RabbitMQDataWriterFactory factory = new RabbitMQDataWriterFactory(
                    minimalSinkOptions(), minimalSinkSchema());
            var writer = factory.createWriter(0, 1, 0L);
            assertThat(writer).isInstanceOf(RabbitMQDataWriter.class);
            assertThat(writer.currentMetricsValues()).hasSize(6);
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
            assertThat(RabbitMQSinkMetrics.PAYLOAD_BYTES_WRITTEN).isEqualTo("payloadBytesWritten");
            assertThat(RabbitMQSinkMetrics.ESTIMATED_WIRE_BYTES_WRITTEN)
                    .isEqualTo("estimatedWireBytesWritten");
            assertThat(RabbitMQSinkMetrics.WRITE_LATENCY_MS).isEqualTo("writeLatencyMs");
            assertThat(RabbitMQSinkMetrics.PUBLISH_CONFIRMS).isEqualTo("publishConfirms");
            assertThat(RabbitMQSinkMetrics.PUBLISH_ERRORS).isEqualTo("publishErrors");
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
        void closeIsIdempotentWithoutInit() throws Exception {
            RabbitMQDataWriter writer = new RabbitMQDataWriter(
                    minimalSinkOptions(), minimalSinkSchema(), 0, 1, -1);
            // close() without any write() should not throw
            writer.close();
            writer.close();

            assertThat(getPrivateField(writer, "producer")).isNull();
            assertThat(getPrivateField(writer, "environment")).isNull();
            assertThat(getPrivateField(writer, "pooledEnvironment")).isEqualTo(false);
        }

        @Test
        void abortWithoutInitDoesNotThrow() throws Exception {
            RabbitMQDataWriter writer = new RabbitMQDataWriter(
                    minimalSinkOptions(), minimalSinkSchema(), 0, 1, -1);
            writer.abort();

            assertThat(getPrivateField(writer, "producer")).isNull();
            assertThat(getPrivateField(writer, "environment")).isNull();
            assertThat(getPrivateField(writer, "pooledEnvironment")).isEqualTo(false);
        }

        @Test
        void closeIsOneShotEvenWhenProducerCloseFails() throws Exception {
            RabbitMQDataWriter writer = new RabbitMQDataWriter(
                    minimalSinkOptions(), minimalSinkSchema(), 0, 1, -1);
            ThrowingCloseProducer producer = new ThrowingCloseProducer();
            setPrivateField(writer, "producer", producer);

            writer.close();
            writer.close();

            assertThat(producer.closeCalls).isEqualTo(1);
        }

        @Test
        void metricsStartAtZero() {
            RabbitMQDataWriter writer = new RabbitMQDataWriter(
                    minimalSinkOptions(), minimalSinkSchema(), 0, 1, -1);
            var metrics = writer.currentMetricsValues();
            assertThat(metrics).hasSize(6);
            for (var m : metrics) {
                assertThat(m.value()).isEqualTo(0);
            }
        }
    }

    @Nested
    class DataWriterBehaviorTests {

        @Test
        void writeThrowsWhenPriorSendErrorExists() throws Exception {
            RabbitMQDataWriter writer = new RabbitMQDataWriter(
                    minimalSinkOptions(), minimalSinkSchema(), 0, 1, -1);
            setPrivateField(writer, "sendError", new java.util.concurrent.atomic.AtomicReference<>(
                    new RuntimeException("boom")));

            assertThatThrownBy(() -> writer.write(new GenericInternalRow(new Object[]{"x".getBytes()})))
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining("Previous send failed");
        }

        @Test
        void writeFailsOnStreamModeMismatch() throws Exception {
            StructType schema = new StructType(new StructField[]{
                    new StructField("value", DataTypes.BinaryType, false, Metadata.empty()),
                    new StructField("stream", DataTypes.StringType, true, Metadata.empty()),
            });
            RabbitMQDataWriter writer = new RabbitMQDataWriter(
                    minimalSinkOptions(), schema, 0, 1, -1);

            setPrivateField(writer, "producer", new NoopProducer());

            InternalRow row = new GenericInternalRow(new Object[]{
                    "body".getBytes(), UTF8String.fromString("other-stream")
            });

            assertThatThrownBy(() -> writer.write(row))
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining("Row targets stream");
        }

        @Test
        void writeFailureAfterLazyInitReleasesEnvironmentAndClosesProducer() throws Exception {
            StructType schema = new StructType(new StructField[]{
                    new StructField("value", DataTypes.BinaryType, false, Metadata.empty()),
                    new StructField("stream", DataTypes.StringType, true, Metadata.empty()),
            });
            ConnectorOptions options = minimalSinkOptions();
            RabbitMQDataWriter writer = new RabbitMQDataWriter(
                    options, schema, 0, 1, -1);
            CapturingProducerBuilder builder = new CapturingProducerBuilder();
            CloseTrackingProducer producer = new CloseTrackingProducer(false);
            builder.customProducer = producer;
            seedEnvironmentPool(options, new BuilderEnvironment(builder));

            assertThat(getRefCount(options)).isEqualTo(1);
            InternalRow row = new GenericInternalRow(new Object[]{
                    "body".getBytes(), UTF8String.fromString("other-stream")
            });

            assertThatThrownBy(() -> writer.write(row))
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining("Row targets stream");

            assertThat(producer.closeCalls).isEqualTo(1);
            assertThat(getRefCount(options)).isEqualTo(1);
            assertThat(getPrivateField(writer, "producer")).isNull();
            assertThat(getPrivateField(writer, "environment")).isNull();
            assertThat(getPrivateField(writer, "pooledEnvironment")).isEqualTo(false);
        }

        @Test
        void writeRequiresRoutingKeyForSuperStreamHashKey() throws Exception {
            Map<String, String> opts = new LinkedHashMap<>();
            opts.put("endpoints", "localhost:5552");
            opts.put("superstream", "super");
            opts.put("routingStrategy", "hash");
            ConnectorOptions options = new ConnectorOptions(opts);

            RabbitMQDataWriter writer = new RabbitMQDataWriter(
                    options, minimalSinkSchema(), 0, 1, -1);
            setPrivateField(writer, "producer", new NoopProducer());

            InternalRow row = new GenericInternalRow(new Object[]{"body".getBytes()});

            assertThatThrownBy(() -> writer.write(row))
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining("Routing key is required");
        }

        @Test
        void writeWithoutRoutingKeyDoesNotInvokeCustomRoutingStrategy() throws Exception {
            TestRoutingStrategy.invoked = false;
            TestRoutingStrategy.routeLookupInvoked = false;
            Map<String, String> opts = new LinkedHashMap<>();
            opts.put("endpoints", "localhost:5552");
            opts.put("superstream", "super");
            opts.put("routingStrategy", "custom");
            opts.put("partitionerClass", TestRoutingStrategy.class.getName());
            ConnectorOptions options = new ConnectorOptions(opts);

            RabbitMQDataWriter writer = new RabbitMQDataWriter(
                    options, minimalSinkSchema(), 0, 1, -1);
            setPrivateField(writer, "producer", new NoopProducer());
            setPrivateField(writer, "nextPublishingId", -1L);

            InternalRow row = new GenericInternalRow(new Object[]{"body".getBytes()});

            writer.write(row);
            assertThat(TestRoutingStrategy.invoked).isFalse();
            assertThat(TestRoutingStrategy.routeLookupInvoked).isFalse();
        }

        @Test
        void commitFlushesConfirmedMessages() throws Exception {
            Map<String, String> opts = minimalSinkMap();
            ConnectorOptions options = new ConnectorOptions(opts);

            RabbitMQDataWriter writer = new RabbitMQDataWriter(
                    options, minimalSinkSchema(), 0, 1, -1);
            BatchTrackingProducer producer = new BatchTrackingProducer();
            setPrivateField(writer, "producer", producer);

            for (int i = 0; i < 5; i++) {
                writer.write(new GenericInternalRow(new Object[]{("m" + i).getBytes()}));
            }

            assertThat(producer.confirmedMessages()).isEqualTo(5);
            assertThat(writer.commit()).isInstanceOf(RabbitMQWriterCommitMessage.class);
        }

        @Test
        void deriveStreamingProducerNameUsesEpochAndStartsPublishingIdAtZero() throws Exception {
            Map<String, String> opts = minimalSinkMap();
            opts.put("producerName", "dedup");
            ConnectorOptions options = new ConnectorOptions(opts);

            RabbitMQDataWriter writer = new RabbitMQDataWriter(
                    options, minimalSinkSchema(), 2, 7, 11);
            CapturingProducerBuilder builder = new CapturingProducerBuilder();
            seedEnvironmentPool(options, new BuilderEnvironment(builder));

            writer.write(new GenericInternalRow(new Object[]{"x".getBytes()}));

            assertThat(builder.name).isEqualTo("dedup-p2-e11");
            long nextId = (long) getPrivateField(writer, "nextPublishingId");
            assertThat(nextId).isEqualTo(1L);
        }

        @Test
        void dedupProducerNamesInBatchMustAvoidTaskAttemptCollisions() throws Exception {
            Map<String, String> opts = minimalSinkMap();
            opts.put("producerName", "dedup");
            ConnectorOptions options = new ConnectorOptions(opts);

            CapturingProducerBuilder builder = new CapturingProducerBuilder(true);
            seedEnvironmentPool(options, new BuilderEnvironment(builder));

            RabbitMQDataWriter firstAttempt = new RabbitMQDataWriter(
                    options, minimalSinkSchema(), 2, 7, -1);
            RabbitMQDataWriter secondAttempt = new RabbitMQDataWriter(
                    options, minimalSinkSchema(), 2, 8, -1);

            InternalRow row = new GenericInternalRow(new Object[]{"x".getBytes()});
            firstAttempt.write(row);
            secondAttempt.write(row);

            assertThat(builder.activeProducerNames).contains("dedup-p2-t7", "dedup-p2-t8");
            assertThat(builder.activeProducerNames).hasSize(2);
        }

        @Test
        void coercePropertiesToStringsIncludesZeroGroupSequence() throws Exception {
            com.rabbitmq.stream.Properties props = org.mockito.Mockito.mock(
                    com.rabbitmq.stream.Properties.class);
            org.mockito.Mockito.when(props.getGroupSequence()).thenReturn(0L);

            Method method = RabbitMQDataWriter.class.getDeclaredMethod(
                    "coercePropertiesToStrings", com.rabbitmq.stream.Properties.class);
            method.setAccessible(true);
            @SuppressWarnings("unchecked")
            Map<String, String> coerced = (Map<String, String>) method.invoke(null, props);

            assertThat(coerced).containsEntry("group_sequence", "0");
        }

        @Test
        void extractRoutingKeyReturnsCorrectValue() throws Exception {
            StructType schema = new StructType(new StructField[]{
                    new StructField("value", DataTypes.BinaryType, false, Metadata.empty()),
                    new StructField("routing_key", DataTypes.StringType, true, Metadata.empty()),
            });
            Map<String, String> opts = new LinkedHashMap<>();
            opts.put("endpoints", "localhost:5552");
            opts.put("superstream", "super");
            opts.put("routingStrategy", "hash");
            ConnectorOptions options = new ConnectorOptions(opts);

            RabbitMQDataWriter writer = new RabbitMQDataWriter(
                    options, schema, 0, 1, -1);
            setPrivateField(writer, "producer", new NoopProducer());

            InternalRow row = new GenericInternalRow(new Object[]{
                    "body".getBytes(), UTF8String.fromString("rk-1")
            });

            writer.write(row);
            var metrics = writer.currentMetricsValues();
            assertThat(metrics[0].value()).isEqualTo(1L);
        }

        @Test
        void commitTimesOutWhenConfirmsOutstanding() throws Exception {
            Map<String, String> opts = minimalSinkMap();
            opts.put("publisherConfirmTimeoutMs", "1");
            ConnectorOptions options = new ConnectorOptions(opts);

            RabbitMQDataWriter writer = new RabbitMQDataWriter(
                    options, minimalSinkSchema(), 0, 1, -1);
            setPrivateField(writer, "outstandingConfirms", new java.util.concurrent.atomic.AtomicLong(1));

            assertThatThrownBy(writer::commit)
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining("Timed out waiting for publisher confirms");
        }

        @Test
        void commitWithZeroConfirmTimeoutWaitsForCompletionWithoutImmediateTimeout() throws Exception {
            Map<String, String> opts = minimalSinkMap();
            opts.put("publisherConfirmTimeoutMs", "0");
            ConnectorOptions options = new ConnectorOptions(opts);

            RabbitMQDataWriter writer = new RabbitMQDataWriter(
                    options, minimalSinkSchema(), 0, 1, -1);
            java.util.concurrent.atomic.AtomicLong outstanding = new java.util.concurrent.atomic.AtomicLong(1);
            setPrivateField(writer, "outstandingConfirms", outstanding);

            Object confirmMonitor = getPrivateField(writer, "confirmMonitor");
            Thread confirmer = new Thread(() -> {
                try {
                    Thread.sleep(100L);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                outstanding.set(0L);
                synchronized (confirmMonitor) {
                    confirmMonitor.notifyAll();
                }
            }, "writer-confirm-timeout-zero-test");
            confirmer.start();

            WriterCommitMessage msg = writer.commit();
            confirmer.join(1_000L);

            assertThat(msg).isInstanceOf(RabbitMQWriterCommitMessage.class);
        }

        @Test
        void commitWithSubSecondConfirmTimeoutUsesClientMinimumWindow() throws Exception {
            Map<String, String> opts = minimalSinkMap();
            opts.put("publisherConfirmTimeoutMs", "1");
            ConnectorOptions options = new ConnectorOptions(opts);

            RabbitMQDataWriter writer = new RabbitMQDataWriter(
                    options, minimalSinkSchema(), 0, 1, -1);
            java.util.concurrent.atomic.AtomicLong outstanding = new java.util.concurrent.atomic.AtomicLong(1);
            setPrivateField(writer, "outstandingConfirms", outstanding);

            Object confirmMonitor = getPrivateField(writer, "confirmMonitor");
            Thread confirmer = new Thread(() -> {
                try {
                    Thread.sleep(100L);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                outstanding.set(0L);
                synchronized (confirmMonitor) {
                    confirmMonitor.notifyAll();
                }
            }, "writer-confirm-timeout-subsecond-test");
            confirmer.start();

            WriterCommitMessage msg = writer.commit();
            confirmer.join(1_000L);

            assertThat(msg).isInstanceOf(RabbitMQWriterCommitMessage.class);
        }

        @Test
        void commitFailsWhenConfirmationNegative() throws Exception {
            RabbitMQDataWriter writer = new RabbitMQDataWriter(
                    minimalSinkOptions(), minimalSinkSchema(), 0, 1, -1);
            setPrivateField(writer, "sendError", new java.util.concurrent.atomic.AtomicReference<>(
                    new IOException("confirm failed")));

            assertThatThrownBy(writer::commit)
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining("failed confirmation");
        }

        @Test
        void commitSucceedsWhenAllConfirmsComplete() throws Exception {
            RabbitMQDataWriter writer = new RabbitMQDataWriter(
                    minimalSinkOptions(), minimalSinkSchema(), 0, 1, -1);
            setPrivateField(writer, "outstandingConfirms", new java.util.concurrent.atomic.AtomicLong(0));

            WriterCommitMessage msg = writer.commit();
            assertThat(msg).isInstanceOf(RabbitMQWriterCommitMessage.class);
        }

        @Test
        void confirmationCallbackDecrementsOutstandingAndWakesWaiters() throws Exception {
            RabbitMQDataWriter writer = new RabbitMQDataWriter(
                    minimalSinkOptions(), minimalSinkSchema(), 0, 1, -1);
            setPrivateField(writer, "producer", new CapturingProducer(true));

            writer.write(new GenericInternalRow(new Object[]{"x".getBytes()}));

            var outstanding = (java.util.concurrent.atomic.AtomicLong) getPrivateField(
                    writer, "outstandingConfirms");
            assertThat(outstanding.get()).isEqualTo(0L);
        }

        @Test
        void dedupPublishingIdInitializationAndIncrement() throws Exception {
            Map<String, String> opts = minimalSinkMap();
            opts.put("producerName", "dedup");
            ConnectorOptions options = new ConnectorOptions(opts);

            RabbitMQDataWriter writer = new RabbitMQDataWriter(
                    options, minimalSinkSchema(), 0, 1, -1);
            setPrivateField(writer, "producer", new PublishingIdProducer(5L));
            setPrivateField(writer, "nextPublishingId", 6L);

            writer.write(new GenericInternalRow(new Object[]{"a".getBytes()}));
            writer.write(new GenericInternalRow(new Object[]{"b".getBytes()}));

            long nextId = (long) getPrivateField(writer, "nextPublishingId");
            assertThat(nextId).isEqualTo(8L);
        }

        @Test
        void explicitPublishingIdColumnOverridesAutoPublishingId() throws Exception {
            Map<String, String> opts = minimalSinkMap();
            opts.put("producerName", "dedup");
            ConnectorOptions options = new ConnectorOptions(opts);

            CapturingPublishingIdProducer producer = new CapturingPublishingIdProducer();
            RabbitMQDataWriter writer = new RabbitMQDataWriter(
                    options, sinkSchemaWithPublishingId(), 0, 1, -1);
            setPrivateField(writer, "producer", producer);
            setPrivateField(writer, "nextPublishingId", 6L);

            writer.write(new GenericInternalRow(new Object[]{"a".getBytes(), 10L}));
            writer.write(new GenericInternalRow(new Object[]{"b".getBytes(), null}));

            assertThat(producer.publishingIds).containsExactly(10L, 11L);
            long nextId = (long) getPrivateField(writer, "nextPublishingId");
            assertThat(nextId).isEqualTo(12L);
        }

        @Test
        void rejectsNegativeExplicitPublishingId() throws Exception {
            ConnectorOptions options = minimalSinkOptions();
            RabbitMQDataWriter writer = new RabbitMQDataWriter(
                    options, sinkSchemaWithPublishingId(), 0, 1, -1);
            setPrivateField(writer, "producer", new NoopProducer());

            assertThatThrownBy(() -> writer.write(
                    new GenericInternalRow(new Object[]{"a".getBytes(), -1L})))
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining("publishing_id")
                    .hasMessageContaining(">= 0");
        }

        @Test
        void abortClosesResourcesEvenWhenProducerInitFailed() throws Exception {
            RabbitMQDataWriter writer = new RabbitMQDataWriter(
                    minimalSinkOptions(), minimalSinkSchema(), 0, 1, -1);
            CloseTrackingEnvironment env = new CloseTrackingEnvironment();
            setPrivateField(writer, "environment", env);

            writer.abort();
            assertThat(env.closed).isTrue();
        }

        @Test
        void metricsBytesWrittenTracksBodySizes() throws Exception {
            RabbitMQDataWriter writer = new RabbitMQDataWriter(
                    minimalSinkOptions(), minimalSinkSchema(), 0, 1, -1);
            setPrivateField(writer, "producer", new NoopProducer());

            writer.write(new GenericInternalRow(new Object[]{"x".getBytes()}));
            writer.write(new GenericInternalRow(new Object[]{new byte[0]}));

            var metrics = writer.currentMetricsValues();
            assertThat(metrics[1].value()).isEqualTo(1L);
        }

        @Test
        void metricsIncludePublishConfirmsAndLatencyOnSuccessfulSend() throws Exception {
            RabbitMQDataWriter writer = new RabbitMQDataWriter(
                    minimalSinkOptions(), minimalSinkSchema(), 0, 1, -1);
            setPrivateField(writer, "producer", new SlowConfirmProducer(10));

            writer.write(new GenericInternalRow(new Object[]{"x".getBytes()}));

            Map<String, Long> metrics = metricsByName(writer.currentMetricsValues());
            assertThat(metrics.get("publishConfirms")).isEqualTo(1L);
            assertThat(metrics.get("publishErrors")).isEqualTo(0L);
            assertThat(metrics.get("writeLatencyMs")).isGreaterThanOrEqualTo(5L);
        }

        @Test
        void initProducerAppliesOptions() throws Exception {
            Map<String, String> opts = minimalSinkMap();
            opts.put("maxInFlight", "7");
            opts.put("publisherConfirmTimeoutMs", "1234");
            opts.put("enqueueTimeoutMs", "0");
            opts.put("batchPublishingDelayMs", "9");
            opts.put("retryOnRecovery", "false");
            opts.put("dynamicBatch", "true");
            ConnectorOptions options = new ConnectorOptions(opts);

            RabbitMQDataWriter writer = new RabbitMQDataWriter(
                    options, minimalSinkSchema(), 0, 1, -1);
            CapturingProducerBuilder builder = new CapturingProducerBuilder();
            seedEnvironmentPool(options, new BuilderEnvironment(builder));

            writer.write(new GenericInternalRow(new Object[]{"x".getBytes()}));

            assertThat(builder.maxInFlight).isEqualTo(7);
            assertThat(builder.confirmTimeoutMs).isEqualTo(1234L);
            assertThat(builder.enqueueTimeoutMs).isEqualTo(0L);
            assertThat(builder.batchDelayMs).isEqualTo(9L);
            assertThat(builder.retryOnRecovery).isEqualTo(Boolean.FALSE);
            assertThat(builder.dynamicBatch).isEqualTo(Boolean.TRUE);
        }

        @Test
        void initProducerPreservesZeroConfirmTimeout() throws Exception {
            Map<String, String> opts = minimalSinkMap();
            opts.put("publisherConfirmTimeoutMs", "0");
            ConnectorOptions options = new ConnectorOptions(opts);

            RabbitMQDataWriter writer = new RabbitMQDataWriter(
                    options, minimalSinkSchema(), 0, 1, -1);
            CapturingProducerBuilder builder = new CapturingProducerBuilder();
            seedEnvironmentPool(options, new BuilderEnvironment(builder));

            writer.write(new GenericInternalRow(new Object[]{"x".getBytes()}));

            assertThat(builder.confirmTimeoutMs).isEqualTo(0L);
        }

        @Test
        void initProducerClampsSubSecondConfirmTimeout() throws Exception {
            Map<String, String> opts = minimalSinkMap();
            opts.put("publisherConfirmTimeoutMs", "800");
            ConnectorOptions options = new ConnectorOptions(opts);

            RabbitMQDataWriter writer = new RabbitMQDataWriter(
                    options, minimalSinkSchema(), 0, 1, -1);
            CapturingProducerBuilder builder = new CapturingProducerBuilder();
            seedEnvironmentPool(options, new BuilderEnvironment(builder));

            writer.write(new GenericInternalRow(new Object[]{"x".getBytes()}));

            assertThat(builder.confirmTimeoutMs).isEqualTo(1000L);
        }

        @Test
        void initProducerListenerSetsSendErrorOnClosed() throws Exception {
            RabbitMQDataWriter writer = new RabbitMQDataWriter(
                    minimalSinkOptions(), minimalSinkSchema(), 0, 1, -1);
            CapturingProducerBuilder builder = new CapturingProducerBuilder();
            seedEnvironmentPool(minimalSinkOptions(), new BuilderEnvironment(builder));

            writer.write(new GenericInternalRow(new Object[]{"x".getBytes()}));

            builder.listener.handle(new FakeStateListenerContext(
                    com.rabbitmq.stream.Resource.State.OPEN,
                    com.rabbitmq.stream.Resource.State.CLOSED));

            var sendError = (java.util.concurrent.atomic.AtomicReference<?>) getPrivateField(
                    writer, "sendError");
            assertThat(sendError.get()).isInstanceOf(IOException.class);
        }

        @Test
        void initProducerFailureAfterBuildCleansUpProducerAndEnvironment() throws Exception {
            Map<String, String> opts = minimalSinkMap();
            opts.put("producerName", "dedup");
            ConnectorOptions options = new ConnectorOptions(opts);

            RabbitMQDataWriter writer = new RabbitMQDataWriter(
                    options, minimalSinkSchema(), 0, 1, -1);
            CapturingProducerBuilder builder = new CapturingProducerBuilder();
            CloseTrackingProducer producer = new CloseTrackingProducer(true);
            builder.customProducer = producer;
            seedEnvironmentPool(options, new BuilderEnvironment(builder));

            assertThat(getRefCount(options)).isEqualTo(1);
            assertThatThrownBy(() -> writer.write(new GenericInternalRow(new Object[]{"x".getBytes()})))
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining("Failed to initialize producer");

            assertThat(producer.closeCalls).isEqualTo(1);
            assertThat(getRefCount(options)).isEqualTo(1);
            assertThat(getPrivateField(writer, "producer")).isNull();
            assertThat(getPrivateField(writer, "environment")).isNull();
            assertThat(getPrivateField(writer, "pooledEnvironment")).isEqualTo(false);
        }

        @Test
        void commitUnblocksPromptlyWhenProducerClosesWithOutstandingConfirms() throws Exception {
            Map<String, String> opts = minimalSinkMap();
            opts.put("publisherConfirmTimeoutMs", "30000");
            ConnectorOptions options = new ConnectorOptions(opts);

            RabbitMQDataWriter writer = new RabbitMQDataWriter(
                    options, minimalSinkSchema(), 0, 1, -1);
            CapturingProducerBuilder builder = new CapturingProducerBuilder();
            seedEnvironmentPool(options, new BuilderEnvironment(builder));
            writer.write(new GenericInternalRow(new Object[]{"x".getBytes()}));

            setPrivateField(writer, "outstandingConfirms", new java.util.concurrent.atomic.AtomicLong(1));

            java.util.concurrent.atomic.AtomicReference<Throwable> commitError =
                    new java.util.concurrent.atomic.AtomicReference<>();
            Thread commitThread = new Thread(() -> {
                try {
                    writer.commit();
                } catch (Throwable t) {
                    commitError.set(t);
                }
            }, "rabbitmq-writer-commit-test");
            commitThread.start();

            long deadlineNanos = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(2);
            while (commitThread.getState() != Thread.State.WAITING
                    && commitThread.getState() != Thread.State.TIMED_WAITING
                    && System.nanoTime() < deadlineNanos) {
                Thread.sleep(5);
            }

            builder.listener.handle(new FakeStateListenerContext(
                    com.rabbitmq.stream.Resource.State.OPEN,
                    com.rabbitmq.stream.Resource.State.CLOSED));

            commitThread.join(2_000L);
            if (commitThread.isAlive()) {
                commitThread.interrupt();
            }

            assertThat(commitThread.isAlive()).isFalse();
            assertThat(commitError.get())
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining("failed confirmation");
        }

        @Test
        void superstreamCustomRoutingStrategyInvokesStrategy() throws Exception {
            TestRoutingStrategy.invoked = false;
            TestRoutingStrategy.routeLookupInvoked = false;
            Map<String, String> opts = new LinkedHashMap<>();
            opts.put("endpoints", "localhost:5552");
            opts.put("superstream", "super");
            opts.put("routingStrategy", "custom");
            opts.put("partitionerClass", TestRoutingStrategy.class.getName());
            ConnectorOptions options = new ConnectorOptions(opts);

            StructType schema = new StructType(new StructField[]{
                    new StructField("value", DataTypes.BinaryType, false, Metadata.empty()),
                    new StructField("routing_key", DataTypes.StringType, true, Metadata.empty()),
            });
            RabbitMQDataWriter writer = new RabbitMQDataWriter(
                    options, schema, 0, 1, -1);
            CapturingProducerBuilder builder = new CapturingProducerBuilder();
            seedEnvironmentPool(options, new BuilderEnvironment(builder));

            writer.write(new GenericInternalRow(new Object[]{
                    "x".getBytes(), UTF8String.fromString("rk-1")
            }));

            assertThat(TestRoutingStrategy.invoked).isTrue();
            assertThat(TestRoutingStrategy.routeLookupInvoked).isTrue();
        }

        @Test
        void superstreamHashRoutingUsesCustomHashFunction() throws Exception {
            TestHashFunction.invoked = false;
            Map<String, String> opts = new LinkedHashMap<>();
            opts.put("endpoints", "localhost:5552");
            opts.put("superstream", "super");
            opts.put("routingStrategy", "hash");
            opts.put("hashFunctionClass", TestHashFunction.class.getName());
            ConnectorOptions options = new ConnectorOptions(opts);

            StructType schema = new StructType(new StructField[]{
                    new StructField("value", DataTypes.BinaryType, false, Metadata.empty()),
                    new StructField("routing_key", DataTypes.StringType, true, Metadata.empty()),
            });
            RabbitMQDataWriter writer = new RabbitMQDataWriter(
                    options, schema, 0, 1, -1);
            CapturingProducerBuilder builder = new CapturingProducerBuilder();
            seedEnvironmentPool(options, new BuilderEnvironment(builder));

            writer.write(new GenericInternalRow(new Object[]{
                    "x".getBytes(), UTF8String.fromString("rk-1")
            }));

            assertThat(builder.customHashConfigured).isTrue();
            assertThat(builder.customHashSampleValue).isEqualTo(7);
            assertThat(TestHashFunction.invoked).isTrue();
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
            assertThat(write.description()).contains("stream=test-stream");
            assertThat(write.toBatch()).isInstanceOf(RabbitMQBatchWrite.class);
            assertThat(write.toStreaming()).isInstanceOf(RabbitMQStreamingWrite.class);
        }

        @Test
        void newWriteBuilderRejectsInvalidSchema() {
            Map<String, String> map = minimalSinkMap();
            ConnectorOptions opts = new ConnectorOptions(map);
            opts.validateCommon();
            RabbitMQStreamTable table = new RabbitMQStreamTable(opts);

            StructType badSchema = new StructType(new StructField[]{
                    new StructField("value", DataTypes.StringType, false, Metadata.empty()),
            });

            assertThatThrownBy(() -> table.newWriteBuilder(
                    new TestLogicalWriteInfo(badSchema, "test-q")))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("value")
                    .hasMessageContaining("binary");
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

    private static StructType sinkSchemaWithPublishingId() {
        return new StructType(new StructField[]{
                new StructField("value", DataTypes.BinaryType, false, Metadata.empty()),
                new StructField("publishing_id", DataTypes.LongType, true, Metadata.empty()),
        });
    }

    private static Write buildWrite() {
        return new RabbitMQWriteBuilder(minimalSinkOptions(), minimalSinkSchema(), "q").build();
    }

    private static PhysicalWriteInfo mockPhysicalWriteInfo(int numPartitions) {
        return () -> numPartitions;
    }

    private static void setPrivateField(Object target, String fieldName, Object value)
            throws Exception {
        var field = target.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(target, value);
    }

    private static void seedEnvironmentPool(ConnectorOptions options, com.rabbitmq.stream.Environment env)
            throws Exception {
        EnvironmentPool.EnvironmentKey key = EnvironmentPool.EnvironmentKey.from(options);
        getPoolMap().put(key, newEntry(env));
    }

    @SuppressWarnings("unchecked")
    private static ConcurrentHashMap<EnvironmentPool.EnvironmentKey, Object> getPoolMap()
            throws Exception {
        Field poolField = EnvironmentPool.class.getDeclaredField("pool");
        poolField.setAccessible(true);
        return (ConcurrentHashMap<EnvironmentPool.EnvironmentKey, Object>) poolField.get(
                EnvironmentPool.getInstance());
    }

    private static int getRefCount(ConnectorOptions options) throws Exception {
        EnvironmentPool.EnvironmentKey key = EnvironmentPool.EnvironmentKey.from(options);
        Object entry = getPoolMap().get(key);
        Field refCountField = entry.getClass().getDeclaredField("refCount");
        refCountField.setAccessible(true);
        java.util.concurrent.atomic.AtomicInteger refCount =
                (java.util.concurrent.atomic.AtomicInteger) refCountField.get(entry);
        return refCount.get();
    }

    private static Object newEntry(com.rabbitmq.stream.Environment environment) throws Exception {
        Class<?> entryClass = Class.forName("io.github.lukaszsamson.spark.rabbitmq.EnvironmentPool$PooledEntry");
        Constructor<?> ctor = entryClass.getDeclaredConstructor(com.rabbitmq.stream.Environment.class);
        ctor.setAccessible(true);
        return ctor.newInstance(environment);
    }

    private static Object getPrivateField(Object target, String fieldName)
            throws Exception {
        var field = target.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        return field.get(target);
    }

    private static Map<String, Long> metricsByName(org.apache.spark.sql.connector.metric.CustomTaskMetric[] metrics) {
        Map<String, Long> byName = new HashMap<>();
        for (var metric : metrics) {
            byName.put(metric.name(), metric.value());
        }
        return byName;
    }

    @SuppressWarnings("unchecked")
    private static <T> T serializeRoundTrip(T value) {
        try {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            try (ObjectOutputStream oos = new ObjectOutputStream(out)) {
                oos.writeObject(value);
            }
            try (ObjectInputStream ois = new ObjectInputStream(
                    new ByteArrayInputStream(out.toByteArray()))) {
                return (T) ois.readObject();
            }
        } catch (Exception e) {
            throw new AssertionError("Serialization round-trip failed", e);
        }
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

    private static final class NoopProducer implements com.rabbitmq.stream.Producer {
        private static final com.rabbitmq.stream.codec.QpidProtonCodec CODEC =
                new com.rabbitmq.stream.codec.QpidProtonCodec();

        @Override
        public com.rabbitmq.stream.MessageBuilder messageBuilder() {
            return CODEC.messageBuilder();
        }

        @Override
        public long getLastPublishingId() {
            return 0L;
        }

        @Override
        public void send(com.rabbitmq.stream.Message message,
                         com.rabbitmq.stream.ConfirmationHandler confirmationHandler) {
            confirmationHandler.handle(new com.rabbitmq.stream.ConfirmationStatus(message, true, (short) 0));
        }

        @Override
        public void close() {
        }
    }

    private static final class CloseTrackingProducer implements com.rabbitmq.stream.Producer {
        private static final com.rabbitmq.stream.codec.QpidProtonCodec CODEC =
                new com.rabbitmq.stream.codec.QpidProtonCodec();
        private final boolean failOnLastPublishingId;
        private int closeCalls;

        private CloseTrackingProducer(boolean failOnLastPublishingId) {
            this.failOnLastPublishingId = failOnLastPublishingId;
        }

        @Override
        public com.rabbitmq.stream.MessageBuilder messageBuilder() {
            return CODEC.messageBuilder();
        }

        @Override
        public long getLastPublishingId() {
            if (failOnLastPublishingId) {
                throw new RuntimeException("getLastPublishingId failed");
            }
            return 0L;
        }

        @Override
        public void send(com.rabbitmq.stream.Message message,
                         com.rabbitmq.stream.ConfirmationHandler confirmationHandler) {
            confirmationHandler.handle(new com.rabbitmq.stream.ConfirmationStatus(message, true, (short) 0));
        }

        @Override
        public void close() {
            closeCalls++;
        }
    }

    private static final class CapturingProducer implements com.rabbitmq.stream.Producer {
        private static final com.rabbitmq.stream.codec.QpidProtonCodec CODEC =
                new com.rabbitmq.stream.codec.QpidProtonCodec();

        private final boolean confirmSuccess;

        private CapturingProducer(boolean confirmSuccess) {
            this.confirmSuccess = confirmSuccess;
        }

        @Override
        public com.rabbitmq.stream.MessageBuilder messageBuilder() {
            return CODEC.messageBuilder();
        }

        @Override
        public long getLastPublishingId() {
            return 0L;
        }

        @Override
        public void send(com.rabbitmq.stream.Message message,
                         com.rabbitmq.stream.ConfirmationHandler confirmationHandler) {
            confirmationHandler.handle(new com.rabbitmq.stream.ConfirmationStatus(
                    message, confirmSuccess, (short) (confirmSuccess ? 0 : 1)));
        }

        @Override
        public void close() {
        }
    }

    private static final class SlowConfirmProducer implements com.rabbitmq.stream.Producer {
        private static final com.rabbitmq.stream.codec.QpidProtonCodec CODEC =
                new com.rabbitmq.stream.codec.QpidProtonCodec();
        private final long delayMs;

        private SlowConfirmProducer(long delayMs) {
            this.delayMs = delayMs;
        }

        @Override
        public com.rabbitmq.stream.MessageBuilder messageBuilder() {
            return CODEC.messageBuilder();
        }

        @Override
        public long getLastPublishingId() {
            return 0L;
        }

        @Override
        public void send(com.rabbitmq.stream.Message message,
                         com.rabbitmq.stream.ConfirmationHandler confirmationHandler) {
            try {
                Thread.sleep(delayMs);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
            confirmationHandler.handle(new com.rabbitmq.stream.ConfirmationStatus(message, true, (short) 0));
        }

        @Override
        public void close() {
        }
    }

    private static final class PublishingIdProducer implements com.rabbitmq.stream.Producer {
        private static final com.rabbitmq.stream.codec.QpidProtonCodec CODEC =
                new com.rabbitmq.stream.codec.QpidProtonCodec();

        private final long lastPublishingId;

        private PublishingIdProducer(long lastPublishingId) {
            this.lastPublishingId = lastPublishingId;
        }

        @Override
        public com.rabbitmq.stream.MessageBuilder messageBuilder() {
            return CODEC.messageBuilder();
        }

        @Override
        public long getLastPublishingId() {
            return lastPublishingId;
        }

        @Override
        public void send(com.rabbitmq.stream.Message message,
                         com.rabbitmq.stream.ConfirmationHandler confirmationHandler) {
            confirmationHandler.handle(new com.rabbitmq.stream.ConfirmationStatus(message, true, (short) 0));
        }

        @Override
        public void close() {
        }
    }

    private static final class CapturingPublishingIdProducer implements com.rabbitmq.stream.Producer {
        private static final com.rabbitmq.stream.codec.QpidProtonCodec CODEC =
                new com.rabbitmq.stream.codec.QpidProtonCodec();
        private final java.util.List<Long> publishingIds = new java.util.ArrayList<>();

        @Override
        public com.rabbitmq.stream.MessageBuilder messageBuilder() {
            return CODEC.messageBuilder();
        }

        @Override
        public long getLastPublishingId() {
            return 0L;
        }

        @Override
        public void send(com.rabbitmq.stream.Message message,
                         com.rabbitmq.stream.ConfirmationHandler confirmationHandler) {
            if (message.hasPublishingId()) {
                publishingIds.add(message.getPublishingId());
            }
            confirmationHandler.handle(new com.rabbitmq.stream.ConfirmationStatus(message, true, (short) 0));
        }

        @Override
        public void close() {
        }
    }

    private static final class ThrowingCloseProducer implements com.rabbitmq.stream.Producer {
        private static final com.rabbitmq.stream.codec.QpidProtonCodec CODEC =
                new com.rabbitmq.stream.codec.QpidProtonCodec();
        private int closeCalls;

        @Override
        public com.rabbitmq.stream.MessageBuilder messageBuilder() {
            return CODEC.messageBuilder();
        }

        @Override
        public long getLastPublishingId() {
            return 0L;
        }

        @Override
        public void send(com.rabbitmq.stream.Message message,
                         com.rabbitmq.stream.ConfirmationHandler confirmationHandler) {
            confirmationHandler.handle(new com.rabbitmq.stream.ConfirmationStatus(message, true, (short) 0));
        }

        @Override
        public void close() {
            closeCalls++;
            throw new RuntimeException("boom");
        }
    }

    private static final class CloseTrackingEnvironment implements com.rabbitmq.stream.Environment {
        private boolean closed = false;

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
        public com.rabbitmq.stream.ConsumerBuilder consumerBuilder() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void close() {
            closed = true;
        }
    }

    private static final class BuilderEnvironment implements com.rabbitmq.stream.Environment {
        private final CapturingProducerBuilder builder;

        private BuilderEnvironment(CapturingProducerBuilder builder) {
            this.builder = builder;
        }

        @Override
        public com.rabbitmq.stream.ProducerBuilder producerBuilder() {
            return builder;
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
        public com.rabbitmq.stream.ConsumerBuilder consumerBuilder() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void close() {
        }
    }

    private static final class CapturingProducerBuilder implements com.rabbitmq.stream.ProducerBuilder {
        private final boolean failOnDuplicateName;
        private final Set<String> activeProducerNames;
        private com.rabbitmq.stream.Producer customProducer;
        private int maxInFlight;
        private long confirmTimeoutMs;
        private long enqueueTimeoutMs;
        private int batchSize;
        private long batchDelayMs;
        private Boolean retryOnRecovery;
        private Boolean dynamicBatch;
        private com.rabbitmq.stream.Resource.StateListener listener;
        private String name;
        private boolean customHashConfigured;
        private int customHashSampleValue;

        private CapturingProducerBuilder() {
            this(false);
        }

        private CapturingProducerBuilder(boolean failOnDuplicateName) {
            this.failOnDuplicateName = failOnDuplicateName;
            this.activeProducerNames = failOnDuplicateName
                    ? Collections.newSetFromMap(new ConcurrentHashMap<>())
                    : Set.of();
        }

        @Override
        public com.rabbitmq.stream.ProducerBuilder stream(String stream) {
            return this;
        }

        @Override
        public com.rabbitmq.stream.ProducerBuilder superStream(String superStream) {
            return this;
        }

        @Override
        public com.rabbitmq.stream.ProducerBuilder name(String name) {
            this.name = name;
            return this;
        }

        @Override
        public com.rabbitmq.stream.ProducerBuilder subEntrySize(int subEntrySize) {
            return this;
        }

        @Override
        public com.rabbitmq.stream.ProducerBuilder compression(com.rabbitmq.stream.compression.Compression compression) {
            return this;
        }

        @Override
        public com.rabbitmq.stream.ProducerBuilder batchSize(int batchSize) {
            this.batchSize = batchSize;
            return this;
        }

        @Override
        public com.rabbitmq.stream.ProducerBuilder batchPublishingDelay(java.time.Duration delay) {
            this.batchDelayMs = delay.toMillis();
            return this;
        }

        @Override
        public com.rabbitmq.stream.ProducerBuilder maxUnconfirmedMessages(int maxUnconfirmedMessages) {
            this.maxInFlight = maxUnconfirmedMessages;
            return this;
        }

        @Override
        public com.rabbitmq.stream.ProducerBuilder confirmTimeout(java.time.Duration timeout) {
            this.confirmTimeoutMs = timeout.toMillis();
            return this;
        }

        @Override
        public com.rabbitmq.stream.ProducerBuilder enqueueTimeout(java.time.Duration timeout) {
            this.enqueueTimeoutMs = timeout.toMillis();
            return this;
        }

        @Override
        public com.rabbitmq.stream.ProducerBuilder retryOnRecovery(boolean retryOnRecovery) {
            this.retryOnRecovery = retryOnRecovery;
            return this;
        }

        @Override
        public com.rabbitmq.stream.ProducerBuilder dynamicBatch(boolean dynamicBatch) {
            this.dynamicBatch = dynamicBatch;
            return this;
        }

        @Override
        public com.rabbitmq.stream.ProducerBuilder listeners(
                com.rabbitmq.stream.Resource.StateListener... listeners) {
            if (listeners.length > 0) {
                this.listener = listeners[0];
            }
            return this;
        }

        @Override
        public com.rabbitmq.stream.ProducerBuilder filterValue(
                java.util.function.Function<com.rabbitmq.stream.Message, String> extractor) {
            return this;
        }

        @Override
        public com.rabbitmq.stream.ProducerBuilder.RoutingConfiguration routing(
                java.util.function.Function<com.rabbitmq.stream.Message, String> extractor) {
            return new CapturingRoutingConfiguration(this);
        }

        @Override
        public com.rabbitmq.stream.Producer build() {
            if (customProducer != null) {
                return customProducer;
            }
            if (failOnDuplicateName && name != null && !activeProducerNames.add(name)) {
                throw new com.rabbitmq.stream.StreamException(
                        "Error while declaring publisher: 17 (PRECONDITION_FAILED). " +
                                "Could not assign producer to client.");
            }
            if (failOnDuplicateName && name != null) {
                return new NameTrackingProducer(activeProducerNames, name);
            }
            return new NoopProducer();
        }
    }

    private static final class NameTrackingProducer implements com.rabbitmq.stream.Producer {
        private static final com.rabbitmq.stream.codec.QpidProtonCodec CODEC =
                new com.rabbitmq.stream.codec.QpidProtonCodec();
        private final Set<String> activeProducerNames;
        private final String producerName;

        private NameTrackingProducer(Set<String> activeProducerNames, String producerName) {
            this.activeProducerNames = activeProducerNames;
            this.producerName = producerName;
        }

        @Override
        public com.rabbitmq.stream.MessageBuilder messageBuilder() {
            return CODEC.messageBuilder();
        }

        @Override
        public long getLastPublishingId() {
            return 0L;
        }

        @Override
        public void send(com.rabbitmq.stream.Message message,
                         com.rabbitmq.stream.ConfirmationHandler confirmationHandler) {
            confirmationHandler.handle(new com.rabbitmq.stream.ConfirmationStatus(message, true, (short) 0));
        }

        @Override
        public void close() {
            activeProducerNames.remove(producerName);
        }
    }

    private static final class BatchTrackingProducer implements com.rabbitmq.stream.Producer {
        private static final com.rabbitmq.stream.codec.QpidProtonCodec CODEC =
                new com.rabbitmq.stream.codec.QpidProtonCodec();
        private int confirmedMessages;

        @Override
        public com.rabbitmq.stream.MessageBuilder messageBuilder() {
            return CODEC.messageBuilder();
        }

        @Override
        public long getLastPublishingId() {
            return 0L;
        }

        @Override
        public void send(com.rabbitmq.stream.Message message,
                         com.rabbitmq.stream.ConfirmationHandler confirmationHandler) {
            confirmedMessages++;
            confirmationHandler.handle(new com.rabbitmq.stream.ConfirmationStatus(message, true, (short) 0));
        }

        @Override
        public void close() {
        }

        private int confirmedMessages() {
            return confirmedMessages;
        }
    }

    private static final class CapturingRoutingConfiguration
            implements com.rabbitmq.stream.ProducerBuilder.RoutingConfiguration {
        private final CapturingProducerBuilder builder;
        private static final com.rabbitmq.stream.codec.QpidProtonCodec CODEC =
                new com.rabbitmq.stream.codec.QpidProtonCodec();

        private CapturingRoutingConfiguration(CapturingProducerBuilder builder) {
            this.builder = builder;
        }

        @Override
        public com.rabbitmq.stream.ProducerBuilder.RoutingConfiguration hash() {
            return this;
        }

        @Override
        public com.rabbitmq.stream.ProducerBuilder.RoutingConfiguration hash(
                java.util.function.ToIntFunction<String> hash) {
            builder.customHashConfigured = true;
            builder.customHashSampleValue = hash.applyAsInt("sample-routing-key");
            return this;
        }

        @Override
        public com.rabbitmq.stream.ProducerBuilder.RoutingConfiguration key() {
            return this;
        }

        @Override
        public com.rabbitmq.stream.ProducerBuilder.RoutingConfiguration strategy(
                com.rabbitmq.stream.RoutingStrategy routingStrategy) {
            com.rabbitmq.stream.Message message = CODEC.messageBuilder()
                    .applicationProperties().entry("routing_key", "rk-1")
                    .messageBuilder()
                    .addData(new byte[0])
                    .build();
            routingStrategy.route(message, new RoutingMetadata(java.util.List.of("p1")));
            return this;
        }

        @Override
        public com.rabbitmq.stream.ProducerBuilder producerBuilder() {
            return builder;
        }
    }

    private static final class FakeStateListenerContext implements com.rabbitmq.stream.Resource.Context {
        @Override
        public com.rabbitmq.stream.Resource resource() {
            return null;
        }

        private final com.rabbitmq.stream.Resource.State from;
        private final com.rabbitmq.stream.Resource.State to;

        private FakeStateListenerContext(com.rabbitmq.stream.Resource.State from,
                                         com.rabbitmq.stream.Resource.State to) {
            this.from = from;
            this.to = to;
        }

        @Override
        public com.rabbitmq.stream.Resource.State currentState() {
            return to;
        }

        @Override
        public com.rabbitmq.stream.Resource.State previousState() {
            return from;
        }

    }

    public static final class TestRoutingStrategy implements ConnectorRoutingStrategy {
        static boolean invoked = false;
        static boolean routeLookupInvoked = false;

        @Override
        public java.util.List<String> route(
                ConnectorMessageView message, ConnectorRoutingStrategy.Metadata metadata) {
            invoked = true;
            String routingKey = message.valueAtPath("application_properties.routing_key");
            if (routingKey != null) {
                routeLookupInvoked = true;
                java.util.List<String> resolved = metadata.route(routingKey);
                if (!resolved.isEmpty()) {
                    return java.util.List.of(resolved.get(0));
                }
            }
            return java.util.List.of(metadata.partitions().get(0));
        }
    }

    public static final class TestHashFunction implements ConnectorHashFunction {
        static boolean invoked = false;

        @Override
        public int hash(String routingKey) {
            invoked = true;
            return 7;
        }
    }

    private static final class RoutingMetadata implements com.rabbitmq.stream.RoutingStrategy.Metadata {
        private final java.util.List<String> partitions;

        private RoutingMetadata(java.util.List<String> partitions) {
            this.partitions = partitions;
        }

        @Override
        public java.util.List<String> partitions() {
            return partitions;
        }

        @Override
        public java.util.List<String> route(String routingKey) {
            return partitions;
        }
    }
}
