package io.github.lukaszsamson.spark.rabbitmq;

import com.rabbitmq.stream.Environment;
import com.rabbitmq.stream.StreamDoesNotExistException;
import com.rabbitmq.stream.StreamException;
import com.rabbitmq.stream.StreamNotAvailableException;
import com.rabbitmq.stream.StreamStats;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class StreamStatsCacheTest {

    @Test
    void retriesTransientStatsFailureAndCachesSuccessfulSnapshot() {
        Environment env = mock(Environment.class);
        StreamStats live = stats(3L, 8L, 7L);
        when(env.queryStreamStats("stream"))
                .thenThrow(new NullPointerException("transient null RPC response"))
                .thenReturn(live);

        StreamStatsCache cache = new StreamStatsCache(1_000L);
        StreamStats first = cache.getOrLoad(env, "stream");
        StreamStats cached = cache.getOrLoad(env, "stream");

        assertThat(first.firstOffset()).isEqualTo(3L);
        assertThat(first.committedOffset()).isEqualTo(8L);
        assertThat(first.committedChunkId()).isEqualTo(7L);
        assertThat(cached).isSameAs(first);
        verify(env, times(2)).queryStreamStats("stream");
    }

    @Test
    void exhaustsBoundedRetriesAndPropagatesLastTransientFailure() {
        Environment env = mock(Environment.class);
        StreamException first = new StreamException("first locator failure");
        StreamException second = new StreamException("second locator failure");
        StreamException last = new StreamException("last locator failure");
        when(env.queryStreamStats("stream")).thenThrow(first, second, last);

        StreamStatsCache cache = new StreamStatsCache(0L);

        assertThatThrownBy(() -> cache.getOrLoad(env, "stream"))
                .isSameAs(last);
        verify(env, times(3)).queryStreamStats("stream");
    }

    @Test
    void doesNotRetryMissingOrUnavailableStreams() {
        assertNonRetryable(new StreamDoesNotExistException("stream"));
        assertNonRetryable(new StreamNotAvailableException("stream"));
    }

    @Test
    void doesNotRetryWhenQueryCancellationInterruptedTheThread() {
        Environment env = mock(Environment.class);
        StreamException interrupted = new StreamException(
                "stats query interrupted", new InterruptedException("query stopped"));
        when(env.queryStreamStats("stream")).thenAnswer(invocation -> {
            Thread.currentThread().interrupt();
            throw interrupted;
        });

        try {
            assertThatThrownBy(() -> new StreamStatsCache(0L).getOrLoad(env, "stream"))
                    .isSameAs(interrupted);
            assertThat(Thread.currentThread().isInterrupted()).isTrue();
            verify(env).queryStreamStats("stream");
        } finally {
            Thread.interrupted();
        }
    }

    private static void assertNonRetryable(RuntimeException failure) {
        Environment env = mock(Environment.class);
        when(env.queryStreamStats("stream")).thenThrow(failure);

        assertThatThrownBy(() -> new StreamStatsCache(0L).getOrLoad(env, "stream"))
                .isSameAs(failure);
        verify(env).queryStreamStats("stream");
    }

    private static StreamStats stats(long first, long committed, long chunk) {
        StreamStats stats = mock(StreamStats.class);
        when(stats.firstOffset()).thenReturn(first);
        when(stats.committedOffset()).thenReturn(committed);
        when(stats.committedChunkId()).thenReturn(chunk);
        return stats;
    }
}
