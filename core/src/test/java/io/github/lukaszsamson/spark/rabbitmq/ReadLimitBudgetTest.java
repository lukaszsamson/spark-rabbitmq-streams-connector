package io.github.lukaszsamson.spark.rabbitmq;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for {@link ReadLimitBudget} deterministic budget distribution.
 */
class ReadLimitBudgetTest {

    @Nested
    class RecordBudget {

        @Test
        void allDataFitsWithinBudget() {
            Map<String, Long> start = Map.of("s1", 0L, "s2", 0L);
            Map<String, Long> tail = Map.of("s1", 50L, "s2", 50L);

            Map<String, Long> result = ReadLimitBudget.distributeRecordBudget(start, tail, 200);
            assertThat(result).isEqualTo(tail);
        }

        @Test
        void budgetExactlyMatchesPending() {
            Map<String, Long> start = Map.of("s1", 0L);
            Map<String, Long> tail = Map.of("s1", 100L);

            Map<String, Long> result = ReadLimitBudget.distributeRecordBudget(start, tail, 100);
            assertThat(result).containsEntry("s1", 100L);
        }

        @Test
        void budgetLessThanPending() {
            Map<String, Long> start = Map.of("s1", 0L);
            Map<String, Long> tail = Map.of("s1", 1000L);

            Map<String, Long> result = ReadLimitBudget.distributeRecordBudget(start, tail, 50);
            assertThat(result.get("s1")).isEqualTo(50L);
        }

        @Test
        void evenDistributionTwoStreams() {
            Map<String, Long> start = Map.of("s1", 0L, "s2", 0L);
            Map<String, Long> tail = Map.of("s1", 300L, "s2", 100L);

            Map<String, Long> result = ReadLimitBudget.distributeRecordBudget(start, tail, 100);

            assertThat(result.get("s1")).isEqualTo(50L);
            assertThat(result.get("s2")).isEqualTo(50L);
        }

        @Test
        void eachNonEmptyStreamGetsAtLeastOne() {
            Map<String, Long> start = Map.of("s1", 0L, "s2", 0L, "s3", 0L);
            Map<String, Long> tail = Map.of("s1", 10000L, "s2", 1L, "s3", 1L);

            Map<String, Long> result = ReadLimitBudget.distributeRecordBudget(start, tail, 5);

            assertThat(result.get("s2")).isGreaterThanOrEqualTo(1L);
            assertThat(result.get("s3")).isGreaterThanOrEqualTo(1L);
        }

        @Test
        void zeroBudgetReturnsStartOffsets() {
            Map<String, Long> start = Map.of("s1", 10L);
            Map<String, Long> tail = Map.of("s1", 100L);

            Map<String, Long> result = ReadLimitBudget.distributeRecordBudget(start, tail, 0);
            assertThat(result).containsEntry("s1", 10L);
        }

        @Test
        void noPendingDataReturnsStartOffsets() {
            Map<String, Long> start = Map.of("s1", 100L);
            Map<String, Long> tail = Map.of("s1", 100L);

            Map<String, Long> result = ReadLimitBudget.distributeRecordBudget(start, tail, 50);
            assertThat(result).containsEntry("s1", 100L);
        }

        @Test
        void nonZeroStartOffsets() {
            Map<String, Long> start = Map.of("s1", 100L);
            Map<String, Long> tail = Map.of("s1", 200L);

            Map<String, Long> result = ReadLimitBudget.distributeRecordBudget(start, tail, 50);
            assertThat(result.get("s1")).isEqualTo(150L); // start + budget
        }

        @Test
        void allocationDoesNotExceedTail() {
            Map<String, Long> start = Map.of("s1", 0L, "s2", 0L);
            Map<String, Long> tail = Map.of("s1", 5L, "s2", 1000L);

            Map<String, Long> result = ReadLimitBudget.distributeRecordBudget(start, tail, 100);
            assertThat(result.get("s1")).isLessThanOrEqualTo(5L);
        }
    }

    @Nested
    class ByteBudget {

        @Test
        void convertsBytesToRecords() {
            Map<String, Long> start = Map.of("s1", 0L);
            Map<String, Long> tail = Map.of("s1", 1000L);

            // 10240 bytes / 1024 bytes per message = 10 records
            Map<String, Long> result = ReadLimitBudget.distributeByteBudget(
                    start, tail, 10240, 1024);
            assertThat(result.get("s1")).isEqualTo(10L);
        }

        @Test
        void atLeastOneRecord() {
            Map<String, Long> start = Map.of("s1", 0L);
            Map<String, Long> tail = Map.of("s1", 1000L);

            // Very small byte budget
            Map<String, Long> result = ReadLimitBudget.distributeByteBudget(
                    start, tail, 1, 1024);
            assertThat(result.get("s1")).isGreaterThanOrEqualTo(1L);
        }

        @Test
        void zeroByteBudgetReturnsStartOffsetsUnchanged() {
            Map<String, Long> start = Map.of("s1", 10L, "s2", 20L);
            Map<String, Long> tail = Map.of("s1", 1000L, "s2", 2000L);

            Map<String, Long> result = ReadLimitBudget.distributeByteBudget(
                    start, tail, 0, 1024);
            assertThat(result).containsEntry("s1", 10L).containsEntry("s2", 20L);
        }

        @Test
        void negativeByteBudgetReturnsStartOffsetsUnchanged() {
            Map<String, Long> start = Map.of("s1", 10L);
            Map<String, Long> tail = Map.of("s1", 1000L);

            Map<String, Long> result = ReadLimitBudget.distributeByteBudget(
                    start, tail, -42, 1024);
            assertThat(result).containsEntry("s1", 10L);
        }
    }

    @Nested
    class Rotation {

        @Test
        void subEligibleSizeBudgetRotatesAcrossStreams() {
            // 4 streams, budget of 2 records each trigger ⇒ 2/4 streams get a
            // record per call. Without rotation, s1+s2 always win and s3+s4 starve.
            Map<String, Long> start = Map.of("s1", 0L, "s2", 0L, "s3", 0L, "s4", 0L);
            Map<String, Long> tail = Map.of("s1", 100L, "s2", 100L, "s3", 100L, "s4", 100L);

            // Sum across 4 successive triggers — every stream should have been
            // served at least once, totaling exactly the 4 * 2 = 8 records.
            long s1 = 0, s2 = 0, s3 = 0, s4 = 0;
            for (long rot = 0; rot < 4; rot++) {
                Map<String, Long> result = ReadLimitBudget.distributeRecordBudget(
                        start, tail, 2, rot);
                s1 += result.get("s1");
                s2 += result.get("s2");
                s3 += result.get("s3");
                s4 += result.get("s4");
            }
            assertThat(s1).isPositive();
            assertThat(s2).isPositive();
            assertThat(s3).isPositive();
            assertThat(s4).isPositive();
            assertThat(s1 + s2 + s3 + s4).isEqualTo(8L);
        }

        @Test
        void remainderRotatesAcrossStreams() {
            // 3 streams, budget of 4 ⇒ baseShare=1 each, remainder=1 to one stream.
            // Across 3 successive triggers the bonus record should rotate.
            Map<String, Long> start = Map.of("s1", 0L, "s2", 0L, "s3", 0L);
            Map<String, Long> tail = Map.of("s1", 100L, "s2", 100L, "s3", 100L);

            long s1 = 0, s2 = 0, s3 = 0;
            for (long rot = 0; rot < 3; rot++) {
                Map<String, Long> result = ReadLimitBudget.distributeRecordBudget(
                        start, tail, 4, rot);
                s1 += result.get("s1");
                s2 += result.get("s2");
                s3 += result.get("s3");
            }
            // Each stream got the +1 bonus exactly once across 3 triggers.
            assertThat(s1).isEqualTo(4L);
            assertThat(s2).isEqualTo(4L);
            assertThat(s3).isEqualTo(4L);
        }

        @Test
        void rotationZeroMatchesUnrotatedBehavior() {
            Map<String, Long> start = Map.of("s1", 0L, "s2", 0L);
            Map<String, Long> tail = Map.of("s1", 50L, "s2", 50L);

            Map<String, Long> withDefault = ReadLimitBudget.distributeRecordBudget(
                    start, tail, 30);
            Map<String, Long> withZero = ReadLimitBudget.distributeRecordBudget(
                    start, tail, 30, 0L);
            assertThat(withZero).isEqualTo(withDefault);
        }
    }

    @Nested
    class MostRestrictive {

        @Test
        void takesMinimumPerStream() {
            Map<String, Long> a = Map.of("s1", 100L, "s2", 200L);
            Map<String, Long> b = Map.of("s1", 50L, "s2", 300L);

            Map<String, Long> result = ReadLimitBudget.mostRestrictive(a, b);
            assertThat(result).containsEntry("s1", 50L);
            assertThat(result).containsEntry("s2", 200L);
        }

        @Test
        void handlesDisjointKeys() {
            Map<String, Long> a = Map.of("s1", 100L);
            Map<String, Long> b = Map.of("s2", 200L);

            Map<String, Long> result = ReadLimitBudget.mostRestrictive(a, b);
            assertThat(result).containsEntry("s1", 100L);
            assertThat(result).containsEntry("s2", 200L);
        }

        @Test
        void identicalMaps() {
            Map<String, Long> a = Map.of("s1", 100L);
            Map<String, Long> result = ReadLimitBudget.mostRestrictive(a, a);
            assertThat(result).containsEntry("s1", 100L);
        }
    }

}
