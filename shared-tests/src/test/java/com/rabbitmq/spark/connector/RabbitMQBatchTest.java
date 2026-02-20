package com.rabbitmq.spark.connector;

import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link RabbitMQBatch} split planning and partitioning
 * logic without contacting a broker. Broker-facing reads are covered by
 * it-tests.
 */
class RabbitMQBatchTest {

    private static final StructType SCHEMA = new StructType(new StructField[]{
            new StructField("value", DataTypes.BinaryType, false, Metadata.empty()),
            new StructField("stream", DataTypes.StringType, false, Metadata.empty()),
            new StructField("offset", DataTypes.LongType, false, Metadata.empty()),
            new StructField("chunk_timestamp", DataTypes.TimestampType, false, Metadata.empty()),
    });

    private static ConnectorOptions baseOptions() {
        Map<String, String> opts = new LinkedHashMap<>();
        opts.put("endpoints", "localhost:5552");
        opts.put("stream", "test-stream");
        return new ConnectorOptions(opts);
    }

    private static ConnectorOptions optionsWithMinPartitions(int minPartitions) {
        Map<String, String> opts = new LinkedHashMap<>();
        opts.put("endpoints", "localhost:5552");
        opts.put("stream", "test-stream");
        opts.put("minPartitions", String.valueOf(minPartitions));
        return new ConnectorOptions(opts);
    }

    // ======================================================================
    // Empty ranges
    // ======================================================================

    @Nested
    class EmptyRanges {

        @Test
        void emptyOffsetsReturnsNoPartitions() {
            RabbitMQBatch batch = new RabbitMQBatch(baseOptions(), SCHEMA, Map.of());
            InputPartition[] partitions = batch.planInputPartitions();
            assertThat(partitions).isEmpty();
        }
    }

    // ======================================================================
    // Single stream, no splitting
    // ======================================================================

    @Nested
    class SingleStreamNoSplit {

        @Test
        void oneStreamOnePartition() {
            Map<String, long[]> ranges = Map.of("s1", new long[]{0, 1000});
            RabbitMQBatch batch = new RabbitMQBatch(baseOptions(), SCHEMA, ranges);

            InputPartition[] partitions = batch.planInputPartitions();
            assertThat(partitions).hasSize(1);

            RabbitMQInputPartition p = (RabbitMQInputPartition) partitions[0];
            assertThat(p.getStream()).isEqualTo("s1");
            assertThat(p.getStartOffset()).isEqualTo(0);
            assertThat(p.getEndOffset()).isEqualTo(1000);
        }
    }

    // ======================================================================
    // Multiple streams, no splitting
    // ======================================================================

    @Nested
    class MultipleStreamsNoSplit {

        @Test
        void multipleStreamsOnePartitionEach() {
            Map<String, long[]> ranges = new LinkedHashMap<>();
            ranges.put("s1", new long[]{0, 500});
            ranges.put("s2", new long[]{100, 600});
            ranges.put("s3", new long[]{200, 700});

            RabbitMQBatch batch = new RabbitMQBatch(baseOptions(), SCHEMA, ranges);
            InputPartition[] partitions = batch.planInputPartitions();

            assertThat(partitions).hasSize(3);
            assertThat(((RabbitMQInputPartition) partitions[0]).getStream()).isEqualTo("s1");
            assertThat(((RabbitMQInputPartition) partitions[1]).getStream()).isEqualTo("s2");
            assertThat(((RabbitMQInputPartition) partitions[2]).getStream()).isEqualTo("s3");
        }
    }

    // ======================================================================
    // minPartitions splitting
    // ======================================================================

    @Nested
    class MinPartitionsSplitting {

        @Test
        void noSplitWhenMinPartitionsLessThanStreams() {
            Map<String, long[]> ranges = new LinkedHashMap<>();
            ranges.put("s1", new long[]{0, 500});
            ranges.put("s2", new long[]{0, 500});

            ConnectorOptions opts = optionsWithMinPartitions(2);
            RabbitMQBatch batch = new RabbitMQBatch(opts, SCHEMA, ranges);

            InputPartition[] partitions = batch.planInputPartitions();
            assertThat(partitions).hasSize(2); // no additional splitting
        }

        @Test
        void splitSingleStreamIntoMultiple() {
            Map<String, long[]> ranges = Map.of("s1", new long[]{0, 1000});

            ConnectorOptions opts = optionsWithMinPartitions(4);
            RabbitMQBatch batch = new RabbitMQBatch(opts, SCHEMA, ranges);

            InputPartition[] partitions = batch.planInputPartitions();
            assertThat(partitions).hasSize(4);

            // Verify ranges are contiguous and cover [0, 1000)
            long prev = 0;
            for (InputPartition p : partitions) {
                RabbitMQInputPartition rp = (RabbitMQInputPartition) p;
                assertThat(rp.getStream()).isEqualTo("s1");
                assertThat(rp.getStartOffset()).isEqualTo(prev);
                assertThat(rp.getEndOffset()).isGreaterThan(prev);
                prev = rp.getEndOffset();
            }
            assertThat(prev).isEqualTo(1000);
        }

        @Test
        void splitEvenlyAcrossStreams() {
            Map<String, long[]> ranges = new LinkedHashMap<>();
            ranges.put("s1", new long[]{0, 900});  // 90% of data
            ranges.put("s2", new long[]{0, 100});  // 10% of data

            ConnectorOptions opts = optionsWithMinPartitions(10);
            RabbitMQBatch batch = new RabbitMQBatch(opts, SCHEMA, ranges);

            InputPartition[] partitions = batch.planInputPartitions();

            assertThat(partitions).hasSize(10);

            // splits should be distributed evenly by stream
            long s1Splits = java.util.Arrays.stream(partitions)
                    .filter(p -> ((RabbitMQInputPartition) p).getStream().equals("s1"))
                    .count();
            long s2Splits = java.util.Arrays.stream(partitions)
                    .filter(p -> ((RabbitMQInputPartition) p).getStream().equals("s2"))
                    .count();
            assertThat(s1Splits).isEqualTo(5);
            assertThat(s2Splits).isEqualTo(5);
        }

        @Test
        void planWithSplittingExactPartitionCount() {
            Map<String, long[]> ranges = new LinkedHashMap<>();
            ranges.put("s1", new long[]{0, 500});
            ranges.put("s2", new long[]{0, 500});

            ConnectorOptions opts = optionsWithMinPartitions(4);
            RabbitMQBatch batch = new RabbitMQBatch(opts, SCHEMA, ranges);

            InputPartition[] partitions = batch.planInputPartitions();
            assertThat(partitions).hasSize(4);
        }

        @Test
        void allocateSplitsRemainderDeterminismOnTies() throws Exception {
            Map<String, long[]> ranges = new LinkedHashMap<>();
            ranges.put("s1", new long[]{0, 100});
            ranges.put("s2", new long[]{0, 100});
            ranges.put("s3", new long[]{0, 100});

            RabbitMQBatch batch = new RabbitMQBatch(optionsWithMinPartitions(4), SCHEMA, ranges);
            @SuppressWarnings("unchecked")
            Map<String, Integer> splits = (Map<String, Integer>) invokePrivate(
                    batch, "allocateSplits", new Class<?>[]{int.class, long.class}, 4, 300L);

            assertThat(splits).containsEntry("s1", 2);
            assertThat(splits).containsEntry("s2", 1);
            assertThat(splits).containsEntry("s3", 1);
        }

        @Test
        void splitStreamAvoidsZeroSizedPartitionsWhenSplitsExceedMessages() throws Exception {
            Map<String, long[]> ranges = Map.of("s1", new long[]{0, 2});
            RabbitMQBatch batch = new RabbitMQBatch(optionsWithMinPartitions(5), SCHEMA, ranges);

            java.util.List<InputPartition> partitions = new java.util.ArrayList<>();
            invokePrivate(batch, "splitStream",
                    new Class<?>[]{java.util.List.class, String.class, long.class, long.class, int.class},
                    partitions, "s1", 0L, 2L, 5);

            assertThat(partitions).hasSize(2);
        }

        @Test
        void planWithoutSplittingPreservesInputOrder() {
            Map<String, long[]> ranges = new LinkedHashMap<>();
            ranges.put("s2", new long[]{0, 10});
            ranges.put("s1", new long[]{0, 10});

            RabbitMQBatch batch = new RabbitMQBatch(baseOptions(), SCHEMA, ranges);
            InputPartition[] partitions = batch.planInputPartitions();

            assertThat(((RabbitMQInputPartition) partitions[0]).getStream()).isEqualTo("s2");
            assertThat(((RabbitMQInputPartition) partitions[1]).getStream()).isEqualTo("s1");
        }

        @Test
        void zeroMessageTotalReturnsEmptyPartitions() {
            Map<String, long[]> ranges = new LinkedHashMap<>();
            ranges.put("s1", new long[]{10, 10});
            ranges.put("s2", new long[]{5, 5});

            RabbitMQBatch batch = new RabbitMQBatch(optionsWithMinPartitions(3), SCHEMA, ranges);
            InputPartition[] partitions = batch.planInputPartitions();

            assertThat(partitions).isEmpty();
        }

        @Test
        void splitPreservesContiguousRanges() {
            Map<String, long[]> ranges = Map.of("s1", new long[]{100, 350});

            ConnectorOptions opts = optionsWithMinPartitions(3);
            RabbitMQBatch batch = new RabbitMQBatch(opts, SCHEMA, ranges);

            InputPartition[] partitions = batch.planInputPartitions();
            assertThat(partitions).hasSize(3);

            // Verify contiguous coverage of [100, 350)
            long prev = 100;
            for (InputPartition p : partitions) {
                RabbitMQInputPartition rp = (RabbitMQInputPartition) p;
                assertThat(rp.getStartOffset()).isEqualTo(prev);
                prev = rp.getEndOffset();
            }
            assertThat(prev).isEqualTo(350);
        }

        @Test
        void singleMessageStreamNotSplitFurther() {
            Map<String, long[]> ranges = Map.of("s1", new long[]{42, 43});

            ConnectorOptions opts = optionsWithMinPartitions(5);
            RabbitMQBatch batch = new RabbitMQBatch(opts, SCHEMA, ranges);

            InputPartition[] partitions = batch.planInputPartitions();
            assertThat(partitions).hasSize(1);
            RabbitMQInputPartition rp = (RabbitMQInputPartition) partitions[0];
            assertThat(rp.getStartOffset()).isEqualTo(42);
            assertThat(rp.getEndOffset()).isEqualTo(43);
        }
    }

    // ======================================================================
    // Reader factory
    // ======================================================================

    @Nested
    class ReaderFactory {

        @Test
        void createReaderFactoryReturnsNonNull() {
            RabbitMQBatch batch = new RabbitMQBatch(baseOptions(), SCHEMA, Map.of());
            PartitionReaderFactory factory = batch.createReaderFactory();
            assertThat(factory).isInstanceOf(RabbitMQPartitionReaderFactory.class);

            RabbitMQInputPartition partition = new RabbitMQInputPartition("test-stream", 0, 1, baseOptions());
            var reader = factory.createReader(partition);
            assertThat(reader).isInstanceOf(RabbitMQPartitionReader.class);
            assertThat(reader.currentMetricsValues()).hasSize(3);
        }
    }

    private static Object invokePrivate(Object target, String methodName,
                                        Class<?>[] parameterTypes, Object... args) throws Exception {
        var method = target.getClass().getDeclaredMethod(methodName, parameterTypes);
        method.setAccessible(true);
        return method.invoke(target, args);
    }
}
