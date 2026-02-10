package com.rabbitmq.spark.connector;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import com.rabbitmq.stream.ConsumerBuilder;
import com.rabbitmq.stream.Environment;
import com.rabbitmq.stream.ProducerBuilder;
import com.rabbitmq.stream.StreamCreator;
import com.rabbitmq.stream.StreamStats;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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
    // Lookup behavior
    // ======================================================================

    @Nested
    class LookupBehaviorTests {

        @Test
        void lookupWithDetailsReturnsEmptyForEmptyStreams() {
            Environment env = new LocatorEnvironment(Map.of());
            StoredOffsetLookup.LookupResult result =
                    StoredOffsetLookup.lookupWithDetails(env, "c", java.util.List.of());
            assertThat(result.getOffsets()).isEmpty();
            assertThat(result.getFailedStreams()).isEmpty();
        }

        @Test
        void successfulLookupReturnsStoredPlusOne() {
            Environment env = new LocatorEnvironment(Map.of(
                    "s1", LocatorResponse.ok(41L)
            ));
            StoredOffsetLookup.LookupResult result =
                    StoredOffsetLookup.lookupWithDetails(env, "c", java.util.List.of("s1"));

            assertThat(result.getOffsets()).containsEntry("s1", 42L);
        }

        @Test
        void noOffsetOmittedFromResult() {
            Environment env = new LocatorEnvironment(Map.of(
                    "s1", LocatorResponse.noOffset()
            ));
            StoredOffsetLookup.LookupResult result =
                    StoredOffsetLookup.lookupWithDetails(env, "c", java.util.List.of("s1"));

            assertThat(result.getOffsets()).isEmpty();
        }

        @Test
        void fatalExceptionClassifiedByMessage() {
            Exception fatal = new RuntimeException("authentication failed");
            assertThatThrownBy(() -> invokeIsFatal(fatal)).isInstanceOf(IllegalStateException.class);
        }

        @Test
        void fatalExceptionClassifiedByCause() {
            Exception fatal = new RuntimeException(new com.rabbitmq.stream.StreamDoesNotExistException("s"));
            assertThatThrownBy(() -> invokeIsFatal(fatal)).isInstanceOf(IllegalStateException.class);
        }

        @Test
        void nonFatalExceptionsReportedInFailedStreams() {
            Environment env = new LocatorEnvironment(Map.of(
                    "s1", LocatorResponse.error((short) 2)
            ));
            StoredOffsetLookup.LookupResult result =
                    StoredOffsetLookup.lookupWithDetails(env, "c", java.util.List.of("s1"));

            assertThat(result.getFailedStreams()).containsExactly("s1");
        }

        @Test
        void interruptedDuringCollectionRestoresInterrupt() throws Exception {
            Thread.currentThread().interrupt();
            Environment env = new InterruptingEnvironment();

            assertThatThrownBy(() -> StoredOffsetLookup.lookupWithDetails(env, "c", java.util.List.of("s1")))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("Interrupted");
            assertThat(Thread.currentThread().isInterrupted()).isTrue();
            Thread.interrupted();
        }

        @Test
        void directLocatorPathPreferredWhenAvailable() {
            Environment env = new LocatorEnvironment(Map.of(
                    "s1", LocatorResponse.ok(5L)
            ));
            StoredOffsetLookup.LookupResult result =
                    StoredOffsetLookup.lookupWithDetails(env, "c", java.util.List.of("s1"));
            assertThat(result.getOffsets()).containsEntry("s1", 6L);
        }

        @Test
        void locatorNoOffsetMapsToNoOffsetBehavior() {
            Environment env = new LocatorEnvironment(Map.of(
                    "s1", LocatorResponse.noOffset()
            ));
            StoredOffsetLookup.LookupResult result =
                    StoredOffsetLookup.lookupWithDetails(env, "c", java.util.List.of("s1"));
            assertThat(result.getOffsets()).isEmpty();
        }

        @Test
        void locatorNonOkResponseProducesFailure() {
            Environment env = new LocatorEnvironment(Map.of(
                    "s1", LocatorResponse.error((short) 3)
            ));
            StoredOffsetLookup.LookupResult result =
                    StoredOffsetLookup.lookupWithDetails(env, "c", java.util.List.of("s1"));
            assertThat(result.getFailedStreams()).containsExactly("s1");
        }

        @Test
        void fallbackToConsumerLookupWhenLocatorUnavailable() {
            Environment env = new EnvironmentWithConsumer(7L);
            StoredOffsetLookup.LookupResult result =
                    StoredOffsetLookup.lookupWithDetails(env, "c", java.util.List.of("s1"));
            assertThat(result.getOffsets()).containsEntry("s1", 8L);
        }

        @Test
        void findMethodTraversesSuperclasses() throws Exception {
            Object response = new DerivedResponse();
            var method = StoredOffsetLookup.class.getDeclaredMethod(
                    "findMethod", Class.class, String.class);
            method.setAccessible(true);
            var found = (java.lang.reflect.Method) method.invoke(null, response.getClass(), "isOk");
            assertThat(found.getDeclaringClass()).isEqualTo(BaseResponse.class);
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

    private static void invokeIsFatal(Exception e) {
        try {
            var method = StoredOffsetLookup.class.getDeclaredMethod("isFatalError", Exception.class);
            method.setAccessible(true);
            boolean fatal = (boolean) method.invoke(null, e);
            if (fatal) {
                throw new IllegalStateException("fatal");
            }
        } catch (ReflectiveOperationException ex) {
            throw new RuntimeException(ex);
        }
    }

    private static final class LocatorEnvironment implements Environment {
        private final Map<String, LocatorResponse> responses;

        private LocatorEnvironment(Map<String, LocatorResponse> responses) {
            this.responses = responses;
        }

        @SuppressWarnings("unused")
        public Object locator() {
            return new Locator(responses);
        }

        @Override
        public StreamCreator streamCreator() {
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
        public StreamStats queryStreamStats(String stream) {
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
        public ProducerBuilder producerBuilder() {
            throw new UnsupportedOperationException();
        }

        @Override
        public ConsumerBuilder consumerBuilder() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void close() {
        }
    }

    private static final class EnvironmentWithConsumer implements Environment {
        private final long storedOffset;

        private EnvironmentWithConsumer(long storedOffset) {
            this.storedOffset = storedOffset;
        }

        @Override
        public ConsumerBuilder consumerBuilder() {
            return new FixedOffsetConsumerBuilder(storedOffset);
        }

        @Override
        public StreamCreator streamCreator() {
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
        public StreamStats queryStreamStats(String stream) {
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
        public ProducerBuilder producerBuilder() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void close() {
        }
    }

    private static final class FixedOffsetConsumerBuilder implements ConsumerBuilder {
        private final long storedOffset;

        private FixedOffsetConsumerBuilder(long storedOffset) {
            this.storedOffset = storedOffset;
        }

        @Override
        public ConsumerBuilder stream(String stream) {
            return this;
        }

        @Override
        public ConsumerBuilder superStream(String superStream) {
            return this;
        }

        @Override
        public ConsumerBuilder offset(com.rabbitmq.stream.OffsetSpecification offsetSpecification) {
            return this;
        }

        @Override
        public ConsumerBuilder messageHandler(com.rabbitmq.stream.MessageHandler messageHandler) {
            return this;
        }

        @Override
        public ConsumerBuilder name(String name) {
            return this;
        }

        @Override
        public ConsumerBuilder singleActiveConsumer() {
            return this;
        }

        @Override
        public ConsumerBuilder consumerUpdateListener(
                com.rabbitmq.stream.ConsumerUpdateListener consumerUpdateListener) {
            return this;
        }

        @Override
        public ConsumerBuilder subscriptionListener(
                com.rabbitmq.stream.SubscriptionListener subscriptionListener) {
            return this;
        }

        @Override
        public ConsumerBuilder listeners(com.rabbitmq.stream.Resource.StateListener... listeners) {
            return this;
        }

        @Override
        public ManualTrackingStrategy manualTrackingStrategy() {
            return new NoopManualTrackingStrategy(this);
        }

        @Override
        public AutoTrackingStrategy autoTrackingStrategy() {
            return new NoopAutoTrackingStrategy(this);
        }

        @Override
        public ConsumerBuilder noTrackingStrategy() {
            return this;
        }

        @Override
        public ConsumerBuilder.FilterConfiguration filter() {
            throw new UnsupportedOperationException();
        }

        @Override
        public ConsumerBuilder.FlowConfiguration flow() {
            throw new UnsupportedOperationException();
        }

        @Override
        public com.rabbitmq.stream.Consumer build() {
            return new FixedOffsetConsumer(storedOffset);
        }
    }

    private static final class FixedOffsetConsumer implements com.rabbitmq.stream.Consumer {
        private final long storedOffset;

        private FixedOffsetConsumer(long storedOffset) {
            this.storedOffset = storedOffset;
        }

        @Override
        public void store(long offset) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void close() {
        }

        @Override
        public long storedOffset() {
            return storedOffset;
        }
    }

    private static final class NoopManualTrackingStrategy implements ConsumerBuilder.ManualTrackingStrategy {
        private final ConsumerBuilder builder;

        private NoopManualTrackingStrategy(ConsumerBuilder builder) {
            this.builder = builder;
        }

        @Override
        public ConsumerBuilder.ManualTrackingStrategy checkInterval(java.time.Duration checkInterval) {
            return this;
        }

        @Override
        public ConsumerBuilder builder() {
            return builder;
        }
    }

    private static final class NoopAutoTrackingStrategy implements ConsumerBuilder.AutoTrackingStrategy {
        private final ConsumerBuilder builder;

        private NoopAutoTrackingStrategy(ConsumerBuilder builder) {
            this.builder = builder;
        }

        @Override
        public ConsumerBuilder.AutoTrackingStrategy messageCountBeforeStorage(int messageCountBeforeStorage) {
            return this;
        }

        @Override
        public ConsumerBuilder.AutoTrackingStrategy flushInterval(java.time.Duration flushInterval) {
            return this;
        }

        @Override
        public ConsumerBuilder builder() {
            return builder;
        }
    }

    private static final class Locator {
        private final Map<String, LocatorResponse> responses;

        private Locator(Map<String, LocatorResponse> responses) {
            this.responses = responses;
        }

        @SuppressWarnings("unused")
        public Object client() {
            return new LocatorClient(responses);
        }
    }

    private static final class LocatorClient {
        private final Map<String, LocatorResponse> responses;

        private LocatorClient(Map<String, LocatorResponse> responses) {
            this.responses = responses;
        }

        @SuppressWarnings("unused")
        public Object queryOffset(String consumerName, String stream) {
            LocatorResponse response = responses.get(stream);
            if (response == null) {
                return LocatorResponse.noOffset();
            }
            return response;
        }
    }

    private static class BaseResponse {
        public boolean isOk() {
            return true;
        }
    }

    private static final class DerivedResponse extends BaseResponse {
    }

    private static final class LocatorResponse {
        private final boolean ok;
        private final short responseCode;
        private final long offset;

        private LocatorResponse(boolean ok, short responseCode, long offset) {
            this.ok = ok;
            this.responseCode = responseCode;
            this.offset = offset;
        }

        static LocatorResponse ok(long offset) {
            return new LocatorResponse(true, (short) 1, offset);
        }

        static LocatorResponse noOffset() {
            return new LocatorResponse(false,
                    com.rabbitmq.stream.Constants.RESPONSE_CODE_NO_OFFSET, 0L);
        }

        static LocatorResponse error(short code) {
            return new LocatorResponse(false, code, 0L);
        }

        @SuppressWarnings("unused")
        public boolean isOk() {
            return ok;
        }

        @SuppressWarnings("unused")
        public long getOffset() {
            return offset;
        }

        @SuppressWarnings("unused")
        public short getResponseCode() {
            return responseCode;
        }
    }

    private static final class InterruptingEnvironment implements Environment {
        @Override
        public ConsumerBuilder consumerBuilder() {
            return new InterruptingConsumerBuilder();
        }

        @Override
        public StreamCreator streamCreator() {
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
        public StreamStats queryStreamStats(String stream) {
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
        public ProducerBuilder producerBuilder() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void close() {
        }
    }

    private static final class InterruptingConsumerBuilder implements ConsumerBuilder {
        @Override
        public ConsumerBuilder stream(String stream) {
            return this;
        }

        @Override
        public ConsumerBuilder superStream(String superStream) {
            return this;
        }

        @Override
        public ConsumerBuilder offset(com.rabbitmq.stream.OffsetSpecification offsetSpecification) {
            return this;
        }

        @Override
        public ConsumerBuilder messageHandler(com.rabbitmq.stream.MessageHandler messageHandler) {
            return this;
        }

        @Override
        public ConsumerBuilder name(String name) {
            return this;
        }

        @Override
        public ConsumerBuilder singleActiveConsumer() {
            return this;
        }

        @Override
        public ConsumerBuilder consumerUpdateListener(
                com.rabbitmq.stream.ConsumerUpdateListener consumerUpdateListener) {
            return this;
        }

        @Override
        public ConsumerBuilder subscriptionListener(
                com.rabbitmq.stream.SubscriptionListener subscriptionListener) {
            return this;
        }

        @Override
        public ConsumerBuilder listeners(com.rabbitmq.stream.Resource.StateListener... listeners) {
            return this;
        }

        @Override
        public ManualTrackingStrategy manualTrackingStrategy() {
            return new NoopManualTrackingStrategy(this);
        }

        @Override
        public AutoTrackingStrategy autoTrackingStrategy() {
            return new NoopAutoTrackingStrategy(this);
        }

        @Override
        public ConsumerBuilder noTrackingStrategy() {
            return this;
        }

        @Override
        public ConsumerBuilder.FilterConfiguration filter() {
            throw new UnsupportedOperationException();
        }

        @Override
        public ConsumerBuilder.FlowConfiguration flow() {
            throw new UnsupportedOperationException();
        }

        @Override
        public com.rabbitmq.stream.Consumer build() {
            Thread.currentThread().interrupt();
            return new FixedOffsetConsumer(0L);
        }
    }
}
