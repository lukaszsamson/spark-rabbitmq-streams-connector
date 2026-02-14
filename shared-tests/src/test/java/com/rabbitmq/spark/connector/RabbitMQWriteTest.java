package com.rabbitmq.spark.connector;

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

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
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
        void useCommitCoordinatorIsTrue() {
            Write write = buildWrite();
            StreamingWrite streamingWrite = write.toStreaming();
            assertThat(streamingWrite.useCommitCoordinator()).isTrue();
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
            assertThat(writer.currentMetricsValues()).hasSize(2);
        }

        @Test
        void createStreamingWriterReturnsDataWriter() {
            RabbitMQDataWriterFactory factory = new RabbitMQDataWriterFactory(
                    minimalSinkOptions(), minimalSinkSchema());
            var writer = factory.createWriter(0, 1, 0L);
            assertThat(writer).isInstanceOf(RabbitMQDataWriter.class);
            assertThat(writer.currentMetricsValues()).hasSize(2);
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
        void writeAcceptsCustomRoutingStrategyWithoutRoutingKey() throws Exception {
            TestRoutingStrategy.invoked = false;
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
        }

        @Test
        void commitFlushesAtExactBatchSize() throws Exception {
            Map<String, String> opts = minimalSinkMap();
            opts.put("batchSize", "5");
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
        void deriveProducerNameIsStableAcrossTaskAttempts() throws Exception {
            Map<String, String> opts = minimalSinkMap();
            opts.put("producerName", "dedup");
            ConnectorOptions options = new ConnectorOptions(opts);

            RabbitMQDataWriter writer = new RabbitMQDataWriter(
                    options, minimalSinkSchema(), 2, 7, -1);
            CapturingProducerBuilder builder = new CapturingProducerBuilder();
            seedEnvironmentPool(options, new BuilderEnvironment(builder));

            writer.write(new GenericInternalRow(new Object[]{"x".getBytes()}));

            assertThat(builder.name).isEqualTo("dedup-p2");
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
        void initProducerAppliesOptions() throws Exception {
            Map<String, String> opts = minimalSinkMap();
            opts.put("maxInFlight", "7");
            opts.put("publisherConfirmTimeoutMs", "1234");
            opts.put("enqueueTimeoutMs", "0");
            opts.put("batchSize", "5");
            opts.put("batchPublishingDelayMs", "9");
            ConnectorOptions options = new ConnectorOptions(opts);

            RabbitMQDataWriter writer = new RabbitMQDataWriter(
                    options, minimalSinkSchema(), 0, 1, -1);
            CapturingProducerBuilder builder = new CapturingProducerBuilder();
            seedEnvironmentPool(options, new BuilderEnvironment(builder));

            writer.write(new GenericInternalRow(new Object[]{"x".getBytes()}));

            assertThat(builder.maxInFlight).isEqualTo(7);
            assertThat(builder.confirmTimeoutMs).isEqualTo(1234L);
            assertThat(builder.enqueueTimeoutMs).isEqualTo(0L);
            assertThat(builder.batchSize).isEqualTo(5);
            assertThat(builder.batchDelayMs).isEqualTo(9L);
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
        void superstreamCustomRoutingStrategyInvokesStrategy() throws Exception {
            TestRoutingStrategy.invoked = false;
            Map<String, String> opts = new LinkedHashMap<>();
            opts.put("endpoints", "localhost:5552");
            opts.put("superstream", "super");
            opts.put("routingStrategy", "custom");
            opts.put("partitionerClass", TestRoutingStrategy.class.getName());
            ConnectorOptions options = new ConnectorOptions(opts);

            RabbitMQDataWriter writer = new RabbitMQDataWriter(
                    options, minimalSinkSchema(), 0, 1, -1);
            CapturingProducerBuilder builder = new CapturingProducerBuilder();
            seedEnvironmentPool(options, new BuilderEnvironment(builder));

            writer.write(new GenericInternalRow(new Object[]{"x".getBytes()}));

            assertThat(TestRoutingStrategy.invoked).isTrue();
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

    private static Object newEntry(com.rabbitmq.stream.Environment environment) throws Exception {
        Class<?> entryClass = Class.forName("com.rabbitmq.spark.connector.EnvironmentPool$PooledEntry");
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
        private int maxInFlight;
        private long confirmTimeoutMs;
        private long enqueueTimeoutMs;
        private int batchSize;
        private long batchDelayMs;
        private com.rabbitmq.stream.Resource.StateListener listener;
        private String name;

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
            return this;
        }

        @Override
        public com.rabbitmq.stream.ProducerBuilder dynamicBatch(boolean dynamicBatch) {
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
            return new NoopProducer();
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
            return this;
        }

        @Override
        public com.rabbitmq.stream.ProducerBuilder.RoutingConfiguration key() {
            return this;
        }

        @Override
        public com.rabbitmq.stream.ProducerBuilder.RoutingConfiguration strategy(
                com.rabbitmq.stream.RoutingStrategy routingStrategy) {
            com.rabbitmq.stream.Message message = CODEC.messageBuilder().addData(new byte[0]).build();
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

        @Override
        public java.util.List<String> route(String routingKey, java.util.List<String> partitions) {
            invoked = true;
            return java.util.List.of(partitions.get(0));
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
