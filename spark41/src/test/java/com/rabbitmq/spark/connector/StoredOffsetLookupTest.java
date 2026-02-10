package com.rabbitmq.spark.connector;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for {@link StoredOffsetLookup} configuration and error classification.
 *
 * <p>Tests that exercise actual broker lookups are in the integration test suite.
 * These unit tests verify the bounded concurrency constant, error classification,
 * and LookupResult structure.
 */
class StoredOffsetLookupTest {

    // ======================================================================
    // Concurrency bound
    // ======================================================================

    @Nested
    class ConcurrencyTests {

        @Test
        void maxConcurrentLookupsIsBounded() {
            // Verify the constant is set to a reasonable value (< 50 broker limit)
            assertThat(StoredOffsetLookup.MAX_CONCURRENT_LOOKUPS)
                    .isGreaterThan(0)
                    .isLessThanOrEqualTo(50);
        }

        @Test
        void maxConcurrentLookupsDefaultIs20() {
            assertThat(StoredOffsetLookup.MAX_CONCURRENT_LOOKUPS).isEqualTo(20);
        }
    }

    // ======================================================================
    // LookupResult
    // ======================================================================

    @Nested
    class LookupResultTests {

        @Test
        void emptyResultHasNoFailures() {
            var result = new StoredOffsetLookup.LookupResult(
                    java.util.Map.of(), java.util.List.of());
            assertThat(result.hasFailures()).isFalse();
            assertThat(result.getOffsets()).isEmpty();
            assertThat(result.getFailedStreams()).isEmpty();
        }

        @Test
        void resultWithOffsetsAndNoFailures() {
            var offsets = java.util.Map.of("stream1", 10L, "stream2", 20L);
            var result = new StoredOffsetLookup.LookupResult(
                    offsets, java.util.List.of());
            assertThat(result.hasFailures()).isFalse();
            assertThat(result.getOffsets()).containsEntry("stream1", 10L);
            assertThat(result.getOffsets()).containsEntry("stream2", 20L);
        }

        @Test
        void resultWithFailures() {
            var offsets = java.util.Map.of("stream1", 10L);
            var failed = java.util.List.of("stream2", "stream3");
            var result = new StoredOffsetLookup.LookupResult(offsets, failed);
            assertThat(result.hasFailures()).isTrue();
            assertThat(result.getFailedStreams()).containsExactly("stream2", "stream3");
            assertThat(result.getOffsets()).containsOnlyKeys("stream1");
        }
    }

    // ======================================================================
    // NonFatalLookupException
    // ======================================================================

    @Nested
    class NonFatalExceptionTests {

        @Test
        void nonFatalExceptionCarriesMessageAndCause() {
            var cause = new RuntimeException("tracking limit reached");
            var ex = new StoredOffsetLookup.NonFatalLookupException(
                    "Non-fatal failure", cause);
            assertThat(ex.getMessage()).isEqualTo("Non-fatal failure");
            assertThat(ex.getCause()).isSameAs(cause);
        }

        @Test
        void nonFatalExceptionIsRuntimeException() {
            var ex = new StoredOffsetLookup.NonFatalLookupException(
                    "test", new RuntimeException());
            assertThat(ex).isInstanceOf(RuntimeException.class);
        }
    }
}
