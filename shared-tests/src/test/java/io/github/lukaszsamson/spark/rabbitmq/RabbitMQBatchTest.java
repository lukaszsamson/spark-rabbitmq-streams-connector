package io.github.lukaszsamson.spark.rabbitmq;

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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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

    private static ConnectorOptions optionsWithMaxRecordsPerPartition(long maxRecords) {
        Map<String, String> opts = new LinkedHashMap<>();
        opts.put("endpoints", "localhost:5552");
        opts.put("stream", "test-stream");
        opts.put("maxRecordsPerPartition", String.valueOf(maxRecords));
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
        void singleActiveConsumerWithMinPartitionsSplitThrows() {
            Map<String, String> opts = new LinkedHashMap<>();
            opts.put("endpoints", "localhost:5552");
            opts.put("stream", "test-stream");
            opts.put("singleActiveConsumer", "true");
            opts.put("consumerName", "sac-reader");
            opts.put("minPartitions", "4");
            RabbitMQBatch batch = new RabbitMQBatch(
                    new ConnectorOptions(opts), SCHEMA, Map.of("s1", new long[]{0, 1000}));

            assertThatThrownBy(batch::planInputPartitions)
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("singleActiveConsumer")
                    .hasMessageContaining("split");
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
            Map<String, Integer> minimumSplits = new LinkedHashMap<>();
            minimumSplits.put("s1", 1);
            minimumSplits.put("s2", 1);
            minimumSplits.put("s3", 1);
            @SuppressWarnings("unchecked")
            Map<String, Integer> splits = (Map<String, Integer>) invokePrivate(
                    batch, "allocateSplits", new Class<?>[]{int.class, Map.class},
                    4, minimumSplits);

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
    // maxRecordsPerPartition splitting
    // ======================================================================

    @Nested
    class MaxRecordsPerPartitionSplitting {

        @Test
        void splitSingleStreamByMaxRecords() {
            Map<String, long[]> ranges = Map.of("s1", new long[]{0, 1000});
            ConnectorOptions opts = optionsWithMaxRecordsPerPartition(300);
            RabbitMQBatch batch = new RabbitMQBatch(opts, SCHEMA, ranges);

            InputPartition[] partitions = batch.planInputPartitions();
            assertThat(partitions).hasSize(4); // ceil(1000/300) = 4

            // Verify contiguous coverage
            long prev = 0;
            for (InputPartition p : partitions) {
                RabbitMQInputPartition rp = (RabbitMQInputPartition) p;
                assertThat(rp.getStream()).isEqualTo("s1");
                assertThat(rp.getStartOffset()).isEqualTo(prev);
                prev = rp.getEndOffset();
            }
            assertThat(prev).isEqualTo(1000);
        }

        @Test
        void singleActiveConsumerWithMaxRecordsSplitThrows() {
            Map<String, String> opts = new LinkedHashMap<>();
            opts.put("endpoints", "localhost:5552");
            opts.put("stream", "test-stream");
            opts.put("singleActiveConsumer", "true");
            opts.put("consumerName", "sac-reader");
            opts.put("maxRecordsPerPartition", "300");
            RabbitMQBatch batch = new RabbitMQBatch(
                    new ConnectorOptions(opts), SCHEMA, Map.of("s1", new long[]{0, 1000}));

            assertThatThrownBy(batch::planInputPartitions)
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("singleActiveConsumer")
                    .hasMessageContaining("split");
        }

        @Test
        void noSplitWhenRangeFitsInOnePartition() {
            Map<String, long[]> ranges = Map.of("s1", new long[]{0, 100});
            ConnectorOptions opts = optionsWithMaxRecordsPerPartition(500);
            RabbitMQBatch batch = new RabbitMQBatch(opts, SCHEMA, ranges);

            InputPartition[] partitions = batch.planInputPartitions();
            assertThat(partitions).hasSize(1);
        }

        @Test
        void splitMultipleStreamsIndependently() {
            Map<String, long[]> ranges = new LinkedHashMap<>();
            ranges.put("s1", new long[]{0, 1000}); // 1000 records -> 5 splits
            ranges.put("s2", new long[]{0, 100});  // 100 records  -> 1 split

            ConnectorOptions opts = optionsWithMaxRecordsPerPartition(200);
            RabbitMQBatch batch = new RabbitMQBatch(opts, SCHEMA, ranges);

            InputPartition[] partitions = batch.planInputPartitions();
            assertThat(partitions).hasSize(6); // 5 + 1

            long s1Count = java.util.Arrays.stream(partitions)
                    .filter(p -> ((RabbitMQInputPartition) p).getStream().equals("s1"))
                    .count();
            long s2Count = java.util.Arrays.stream(partitions)
                    .filter(p -> ((RabbitMQInputPartition) p).getStream().equals("s2"))
                    .count();
            assertThat(s1Count).isEqualTo(5);
            assertThat(s2Count).isEqualTo(1);
        }

        @Test
        void minPartitionsOverridesWhenLarger() {
            Map<String, long[]> ranges = Map.of("s1", new long[]{0, 1000});

            // maxRecordsPerPartition=500 would give 2 splits, but minPartitions=4 requires more
            Map<String, String> opts = new LinkedHashMap<>();
            opts.put("endpoints", "localhost:5552");
            opts.put("stream", "test-stream");
            opts.put("maxRecordsPerPartition", "500");
            opts.put("minPartitions", "4");
            ConnectorOptions options = new ConnectorOptions(opts);

            RabbitMQBatch batch = new RabbitMQBatch(options, SCHEMA, ranges);
            InputPartition[] partitions = batch.planInputPartitions();
            assertThat(partitions).hasSize(4);
        }

        @Test
        void minPartitionsWithMaxRecordsPreservesMaxRecordsBoundOnSkew() {
            Map<String, long[]> ranges = new LinkedHashMap<>();
            ranges.put("s1", new long[]{0, 1000});
            ranges.put("s2", new long[]{0, 10});

            Map<String, String> opts = new LinkedHashMap<>();
            opts.put("endpoints", "localhost:5552");
            opts.put("stream", "test-stream");
            opts.put("maxRecordsPerPartition", "100");
            opts.put("minPartitions", "12");
            RabbitMQBatch batch = new RabbitMQBatch(new ConnectorOptions(opts), SCHEMA, ranges);

            InputPartition[] partitions = batch.planInputPartitions();
            assertThat(partitions).hasSize(12);

            long s1Splits = 0;
            long maxS1Span = 0;
            for (InputPartition partition : partitions) {
                RabbitMQInputPartition p = (RabbitMQInputPartition) partition;
                long span = p.getEndOffset() - p.getStartOffset();
                if ("s1".equals(p.getStream())) {
                    s1Splits++;
                    maxS1Span = Math.max(maxS1Span, span);
                }
            }
            assertThat(s1Splits).isGreaterThanOrEqualTo(10);
            assertThat(maxS1Span).isLessThanOrEqualTo(100L);
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
            assertThat(reader.currentMetricsValues()).hasSize(6);
        }
    }

    private static Object invokePrivate(Object target, String methodName,
                                        Class<?>[] parameterTypes, Object... args) throws Exception {
        var method = target.getClass().getDeclaredMethod(methodName, parameterTypes);
        method.setAccessible(true);
        return method.invoke(target, args);
    }
}
