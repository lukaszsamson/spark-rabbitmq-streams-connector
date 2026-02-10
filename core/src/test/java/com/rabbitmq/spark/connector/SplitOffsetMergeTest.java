package com.rabbitmq.spark.connector;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.List;

import static com.rabbitmq.spark.connector.SplitOffsetMerge.SplitResult;
import static org.assertj.core.api.Assertions.assertThat;

class SplitOffsetMergeTest {

    @Nested
    class BasicMerge {

        @Test
        void emptyListReturnsBase() {
            assertThat(SplitOffsetMerge.merge(100, List.of())).isEqualTo(100);
        }

        @Test
        void nullListReturnsBase() {
            assertThat(SplitOffsetMerge.merge(100, null)).isEqualTo(100);
        }

        @Test
        void singleFullyCommittedSplit() {
            List<SplitResult> splits = List.of(new SplitResult(0, 100, 100));
            assertThat(SplitOffsetMerge.merge(0, splits)).isEqualTo(100);
        }

        @Test
        void singlePartiallyCommittedSplit() {
            List<SplitResult> splits = List.of(new SplitResult(0, 100, 50));
            assertThat(SplitOffsetMerge.merge(0, splits)).isEqualTo(0);
        }
    }

    @Nested
    class ContiguousSplits {

        @Test
        void allContiguousAndComplete() {
            List<SplitResult> splits = List.of(
                    new SplitResult(0, 100, 100),
                    new SplitResult(100, 200, 200),
                    new SplitResult(200, 300, 300)
            );
            assertThat(SplitOffsetMerge.merge(0, splits)).isEqualTo(300);
        }

        @Test
        void middleSplitIncomplete() {
            // base=0, [0,100] commit, [100,200] fail, [200,300] commit => merged = 100
            List<SplitResult> splits = List.of(
                    new SplitResult(0, 100, 100),
                    new SplitResult(100, 200, 150), // incomplete
                    new SplitResult(200, 300, 300)
            );
            assertThat(SplitOffsetMerge.merge(0, splits)).isEqualTo(100);
        }

        @Test
        void firstSplitIncomplete() {
            List<SplitResult> splits = List.of(
                    new SplitResult(0, 100, 50), // incomplete
                    new SplitResult(100, 200, 200),
                    new SplitResult(200, 300, 300)
            );
            assertThat(SplitOffsetMerge.merge(0, splits)).isEqualTo(0);
        }

        @Test
        void lastSplitMissing() {
            List<SplitResult> splits = List.of(
                    new SplitResult(0, 100, 100),
                    new SplitResult(100, 200, 200)
                    // [200, 300] missing
            );
            assertThat(SplitOffsetMerge.merge(0, splits)).isEqualTo(200);
        }
    }

    @Nested
    class UnorderedAndDuplicate {

        @Test
        void unorderedSplitsAreSorted() {
            List<SplitResult> splits = List.of(
                    new SplitResult(200, 300, 300),
                    new SplitResult(0, 100, 100),
                    new SplitResult(100, 200, 200)
            );
            assertThat(SplitOffsetMerge.merge(0, splits)).isEqualTo(300);
        }

        @Test
        void duplicateSplitsKeepMaxCommitted() {
            List<SplitResult> splits = List.of(
                    new SplitResult(0, 100, 50),   // first attempt: partial
                    new SplitResult(0, 100, 100),  // retry: complete
                    new SplitResult(100, 200, 200)
            );
            assertThat(SplitOffsetMerge.merge(0, splits)).isEqualTo(200);
        }

        @Test
        void duplicateRetryNotComplete() {
            List<SplitResult> splits = List.of(
                    new SplitResult(0, 100, 30),
                    new SplitResult(0, 100, 70) // retry still not complete
            );
            assertThat(SplitOffsetMerge.merge(0, splits)).isEqualTo(0);
        }
    }

    @Nested
    class NonZeroBase {

        @Test
        void baseNotAtZero() {
            List<SplitResult> splits = List.of(
                    new SplitResult(500, 600, 600),
                    new SplitResult(600, 700, 700)
            );
            assertThat(SplitOffsetMerge.merge(500, splits)).isEqualTo(700);
        }

        @Test
        void splitDoesNotStartAtBase() {
            // base=100, but first split starts at 200 — gap, no advance
            List<SplitResult> splits = List.of(
                    new SplitResult(200, 300, 300)
            );
            assertThat(SplitOffsetMerge.merge(100, splits)).isEqualTo(100);
        }

        @Test
        void baseAlreadyPastSplits() {
            List<SplitResult> splits = List.of(
                    new SplitResult(0, 100, 100),
                    new SplitResult(100, 200, 200)
            );
            assertThat(SplitOffsetMerge.merge(200, splits)).isEqualTo(200);
        }
    }

    @Nested
    class SpecExample {

        @Test
        void specExamplePartialCompletion() {
            // "base=0, splits [0,100] commit, [100,200] fail, [200,300] commit => merged=100"
            List<SplitResult> splits = List.of(
                    new SplitResult(0, 100, 100),
                    new SplitResult(200, 300, 300) // [100,200] missing entirely
            );
            assertThat(SplitOffsetMerge.merge(0, splits)).isEqualTo(100);
        }
    }
}
