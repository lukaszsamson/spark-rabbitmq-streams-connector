package com.rabbitmq.spark.connector;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for {@link ReadLimitBudget} proportional budget distribution.
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
        void proportionalDistributionTwoStreams() {
            Map<String, Long> start = Map.of("s1", 0L, "s2", 0L);
            Map<String, Long> tail = Map.of("s1", 300L, "s2", 100L);

            Map<String, Long> result = ReadLimitBudget.distributeRecordBudget(start, tail, 100);

            long s1End = result.get("s1");
            long s2End = result.get("s2");
            // s1 has 75% of data → gets ~75 of budget, s2 gets ~25
            assertThat(s1End + s2End).isEqualTo(100L);
            assertThat(s1End).isGreaterThan(s2End);
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

    @Nested
    class ProportionalAllocation {

        @Test
        void singleStreamGetsFullBudget() {
            Map<String, Long> pending = Map.of("s1", 100L);
            Map<String, Long> result = ReadLimitBudget.allocateProportionally(pending, 50);
            assertThat(result.get("s1")).isEqualTo(50L);
        }

        @Test
        void emptyStreamGetsZero() {
            // Use LinkedHashMap for deterministic ordering
            Map<String, Long> pending = new LinkedHashMap<>();
            pending.put("s1", 100L);
            pending.put("s2", 0L);

            Map<String, Long> result = ReadLimitBudget.allocateProportionally(pending, 50);
            assertThat(result.get("s2")).isEqualTo(0L);
            assertThat(result.get("s1")).isEqualTo(50L);
        }

        @Test
        void equalSharesForEqualPending() {
            Map<String, Long> pending = new LinkedHashMap<>();
            pending.put("s1", 100L);
            pending.put("s2", 100L);

            Map<String, Long> result = ReadLimitBudget.allocateProportionally(pending, 100);
            assertThat(result.get("s1")).isEqualTo(50L);
            assertThat(result.get("s2")).isEqualTo(50L);
        }

        @Test
        void remainderDistributedByLargestFraction() {
            Map<String, Long> pending = new LinkedHashMap<>();
            pending.put("s1", 70L);
            pending.put("s2", 30L);

            // Budget of 10: s1 gets 7, s2 gets 3
            Map<String, Long> result = ReadLimitBudget.allocateProportionally(pending, 10);
            assertThat(result.get("s1") + result.get("s2")).isLessThanOrEqualTo(10L);
            assertThat(result.get("s1")).isGreaterThan(result.get("s2"));
        }
    }
}
