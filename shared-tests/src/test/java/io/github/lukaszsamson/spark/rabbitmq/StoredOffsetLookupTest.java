package io.github.lukaszsamson.spark.rabbitmq;

import com.rabbitmq.stream.ConsumerBuilder;
import com.rabbitmq.stream.Environment;
import com.rabbitmq.stream.NoOffsetException;
import com.rabbitmq.stream.ProducerBuilder;
import com.rabbitmq.stream.StreamCreator;
import com.rabbitmq.stream.StreamStats;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class StoredOffsetLookupTest {

    @Nested
    class LookupBehaviorTests {

        @Test
        void lookupWithDetailsReturnsEmptyForEmptyStreams() {
            Environment env = new FixedBehaviorEnvironment(Map.of());
            StoredOffsetLookup.LookupResult result =
                    StoredOffsetLookup.lookupWithDetails(env, "c", List.of());
            assertThat(result.getOffsets()).isEmpty();
            assertThat(result.getFailedStreams()).isEmpty();
        }

        @Test
        void successfulLookupReturnsStoredPlusOne() {
            Environment env = new FixedBehaviorEnvironment(Map.of(
                    "s1", LookupBehavior.withStoredOffset(41L)
            ));
            StoredOffsetLookup.LookupResult result =
                    StoredOffsetLookup.lookupWithDetails(env, "c", List.of("s1"));

            assertThat(result.getOffsets()).containsEntry("s1", 42L);
        }

        @Test
        void noOffsetOmittedFromResult() {
            Environment env = new FixedBehaviorEnvironment(Map.of(
                    "s1", LookupBehavior.withoutOffset()
            ));
            StoredOffsetLookup.LookupResult result =
                    StoredOffsetLookup.lookupWithDetails(env, "c", List.of("s1"));

            assertThat(result.getOffsets()).isEmpty();
            assertThat(result.getFailedStreams()).isEmpty();
        }

        @Test
        void fatalLookupFailureIsPropagated() {
            Environment env = new FixedBehaviorEnvironment(Map.of(
                    "s1", LookupBehavior.fatalFailure()
            ));

            assertThatThrownBy(() -> StoredOffsetLookup.lookupWithDetails(env, "c", List.of("s1")))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("Failed to look up stored offset");
        }

        @Test
        void nonFatalExceptionsReportedInFailedStreams() {
            Environment env = new FixedBehaviorEnvironment(Map.of(
                    "s1", LookupBehavior.nonFatalFailure()
            ));
            StoredOffsetLookup.LookupResult result =
                    StoredOffsetLookup.lookupWithDetails(env, "c", List.of("s1"));

            assertThat(result.getOffsets()).isEmpty();
            assertThat(result.getFailedStreams()).containsExactly("s1");
        }

        @Test
        void hasFailuresReturnsTrueWhenAnyStreamFailed() {
            Environment env = new FixedBehaviorEnvironment(Map.of(
                    "s1", LookupBehavior.withStoredOffset(41L),
                    "s2", LookupBehavior.nonFatalFailure()
            ));

            StoredOffsetLookup.LookupResult result =
                    StoredOffsetLookup.lookupWithDetails(env, "c", List.of("s1", "s2"));

            assertThat(result.getOffsets()).containsEntry("s1", 42L);
            assertThat(result.getFailedStreams()).containsExactly("s2");
            assertThat(result.hasFailures()).isTrue();
        }

        @Test
        void lookupOmitsFailedStreamsFromOffsetsMap() {
            Environment env = new FixedBehaviorEnvironment(Map.of(
                    "s1", LookupBehavior.withStoredOffset(1L),
                    "s2", LookupBehavior.nonFatalFailure()
            ));

            Map<String, Long> result = StoredOffsetLookup.lookup(env, "c", List.of("s1", "s2"));

            assertThat(result).containsEntry("s1", 2L);
            assertThat(result).doesNotContainKey("s2");
        }

        @Test
        void interruptedDuringCollectionRestoresInterrupt() {
            Thread.currentThread().interrupt();
            Environment env = new FixedBehaviorEnvironment(Map.of(
                    "s1", LookupBehavior.withStoredOffset(1L)
            ));

            assertThatThrownBy(() -> StoredOffsetLookup.lookupWithDetails(env, "c", List.of("s1")))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("Interrupted");
            assertThat(Thread.currentThread().isInterrupted()).isTrue();
            Thread.interrupted();
        }

        @Test
        void stalledLookupTimesOutAndFailsFast() {
            Environment env = new FixedBehaviorEnvironment(Map.of(
                    "s1", LookupBehavior.delayedStoredOffset(1L, 500L)
            ));

            long startNanos = System.nanoTime();
            assertThatThrownBy(() -> StoredOffsetLookup.lookupWithDetails(env, "c",
                    List.of("s1"), 20L))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("Timed out waiting for stored offset lookup");
            long elapsedMs = java.util.concurrent.TimeUnit.NANOSECONDS.toMillis(
                    System.nanoTime() - startNanos);
            assertThat(elapsedMs).isLessThan(1000L);
        }

        @Test
        void fatalExceptionClassifiedByMessage() {
            Exception fatal = new RuntimeException("authentication failed");
            assertThatThrownBy(() -> invokeIsFatal(fatal)).isInstanceOf(IllegalStateException.class);
        }

        @Test
        void fatalExceptionClassifiedByCause() {
            Exception fatal = new RuntimeException(
                    new com.rabbitmq.stream.AuthenticationFailureException("auth"));
            assertThatThrownBy(() -> invokeIsFatal(fatal)).isInstanceOf(IllegalStateException.class);
        }

        @Test
        void streamDoesNotExistIsNotFatalByCause() {
            Exception nonFatal = new RuntimeException(
                    new com.rabbitmq.stream.StreamDoesNotExistException("s"));
            invokeIsFatal(nonFatal);
        }

        @Test
        void streamDoesNotExistIsNotFatalByMessage() {
            Exception nonFatal = new RuntimeException("stream does not exist");
            invokeIsFatal(nonFatal);
        }
    }

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

    private record LookupBehavior(
            Long storedOffset, boolean noOffset, boolean fatal, boolean nonFatal, long buildDelayMs) {
        static LookupBehavior withStoredOffset(long value) {
            return new LookupBehavior(value, false, false, false, 0L);
        }

        static LookupBehavior withoutOffset() {
            return new LookupBehavior(null, true, false, false, 0L);
        }

        static LookupBehavior fatalFailure() {
            return new LookupBehavior(null, false, true, false, 0L);
        }

        static LookupBehavior nonFatalFailure() {
            return new LookupBehavior(null, false, false, true, 0L);
        }

        static LookupBehavior delayedStoredOffset(long value, long delayMs) {
            return new LookupBehavior(value, false, false, false, delayMs);
        }
    }

    private static final class FixedBehaviorEnvironment implements Environment {
        private final Map<String, LookupBehavior> behaviors;

        private FixedBehaviorEnvironment(Map<String, LookupBehavior> behaviors) {
            this.behaviors = new LinkedHashMap<>(behaviors);
        }

        @Override
        public ConsumerBuilder consumerBuilder() {
            return new FixedBehaviorConsumerBuilder(behaviors);
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

    private static final class FixedBehaviorConsumerBuilder implements ConsumerBuilder {
        private final Map<String, LookupBehavior> behaviors;
        private String stream;

        private FixedBehaviorConsumerBuilder(Map<String, LookupBehavior> behaviors) {
            this.behaviors = behaviors;
        }

        @Override
        public ConsumerBuilder stream(String stream) {
            this.stream = stream;
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
            LookupBehavior behavior = behaviors.getOrDefault(stream, LookupBehavior.withoutOffset());
            if (behavior.buildDelayMs() > 0) {
                try {
                    Thread.sleep(behavior.buildDelayMs());
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException("interrupted", e);
                }
            }
            if (behavior.fatal()) {
                throw new RuntimeException("authentication failed");
            }
            if (behavior.nonFatal()) {
                throw new RuntimeException("tracking consumers limit reached");
            }
            if (behavior.noOffset()) {
                return new NoOffsetConsumer();
            }
            return new FixedOffsetConsumer(behavior.storedOffset());
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

    private static final class NoOffsetConsumer implements com.rabbitmq.stream.Consumer {
        @Override
        public void store(long offset) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void close() {
        }

        @Override
        public long storedOffset() {
            throw new NoOffsetException("no stored offset");
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
}
