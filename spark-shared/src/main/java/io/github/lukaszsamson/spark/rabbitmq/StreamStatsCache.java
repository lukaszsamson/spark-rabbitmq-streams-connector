package io.github.lukaszsamson.spark.rabbitmq;

import com.rabbitmq.stream.Environment;
import com.rabbitmq.stream.NoOffsetException;
import com.rabbitmq.stream.StreamStats;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * Per-stream TTL cache for {@link StreamStats} snapshots.
 *
 * <p>RabbitMQ stream-source planning consults {@code queryStreamStats} from
 * several call sites within a single trigger ({@code latestOffset},
 * {@code planInputPartitions}, {@code validateStartOffset}). Each call is a
 * round-trip to the broker. This cache collapses repeated lookups within a
 * configured window down to one RPC per stream per trigger.
 *
 * <p>Snapshots capture the three fields that planning needs — {@code firstOffset},
 * {@code committedOffset}, {@code committedChunkId} — and re-expose them through
 * the {@link StreamStats} interface so existing helpers continue to work
 * unchanged.
 */
final class StreamStatsCache {

    /** Sentinel for "field threw NoOffsetException at capture time". */
    private static final long NO_OFFSET = Long.MIN_VALUE;
    /** Sentinel for "field is unsupported on this broker (older version)". */
    private static final long UNSUPPORTED = Long.MIN_VALUE + 1;

    private final long ttlNanos;
    private final ConcurrentHashMap<String, Entry> entries = new ConcurrentHashMap<>();

    StreamStatsCache(long ttlMs) {
        this.ttlNanos = TimeUnit.MILLISECONDS.toNanos(Math.max(0L, ttlMs));
    }

    /**
     * Return a fresh snapshot for {@code stream}, querying the broker only when no
     * unexpired entry is available. The returned {@link StreamStats} is a
     * point-in-time view of the broker fields.
     */
    StreamStats getOrLoad(Environment env, String stream) {
        long nowNanos = System.nanoTime();
        Entry cached = entries.get(stream);
        if (cached != null && nowNanos < cached.expiresAtNanos) {
            return cached.stats;
        }
        // Best-effort de-duplication only. Concurrent callers can still race between
        // get() and put() and issue duplicate queryStreamStats calls for the same
        // stream during a cache miss. We tolerate that rare stampede because
        // correctness only requires that every read sees a consistent snapshot.
        StreamStats fresh = capture(env.queryStreamStats(stream));
        long expiresAtNanos = nowNanos + ttlNanos;
        entries.put(stream, new Entry(fresh, expiresAtNanos));
        return fresh;
    }

    /** Drop a stale entry, e.g. after detecting a topology change for {@code stream}. */
    void invalidate(String stream) {
        entries.remove(stream);
    }

    /** Drop every cached entry. */
    void clear() {
        entries.clear();
    }

    /** Drop entries for streams not in the {@code activeStreams} set. */
    void retainOnly(java.util.Set<String> activeStreams) {
        entries.keySet().removeIf(s -> !activeStreams.contains(s));
    }

    private static StreamStats capture(StreamStats live) {
        // firstOffset() and committedChunkId() throw only NoOffsetException for the
        // empty-stream case on every supported broker; let other RuntimeExceptions
        // bubble so unexpected failures aren't silently masked as "unsupported".
        long firstOffset;
        try {
            firstOffset = live.firstOffset();
        } catch (NoOffsetException e) {
            firstOffset = NO_OFFSET;
        }
        long committedChunkId;
        try {
            committedChunkId = live.committedChunkId();
        } catch (NoOffsetException e) {
            committedChunkId = NO_OFFSET;
        }
        // committedOffset() additionally throws non-NoOffset RuntimeExceptions on
        // older brokers (RabbitMQ < 4.3) where it isn't supported. Treat those as
        // UNSUPPORTED so callers fall back to committedChunkId, which is the same
        // contract resolveTailOffset() expected of the live broker.
        long committedOffset;
        try {
            committedOffset = live.committedOffset();
        } catch (NoOffsetException e) {
            committedOffset = NO_OFFSET;
        } catch (RuntimeException e) {
            committedOffset = UNSUPPORTED;
        }
        return new SnapshotStreamStats(firstOffset, committedOffset, committedChunkId);
    }

    private record Entry(StreamStats stats, long expiresAtNanos) {}

    /**
     * Immutable snapshot view that re-throws {@link NoOffsetException} for fields
     * that were unavailable when captured, matching the live broker contract.
     */
    private record SnapshotStreamStats(
            long firstOffsetValue,
            long committedOffsetValue,
            long committedChunkIdValue) implements StreamStats {

        @Override
        public long firstOffset() {
            return unwrap(firstOffsetValue, "firstOffset");
        }

        @Override
        public long committedOffset() {
            return unwrap(committedOffsetValue, "committedOffset");
        }

        @Override
        public long committedChunkId() {
            return unwrap(committedChunkIdValue, "committedChunkId");
        }

        private static long unwrap(long value, String field) {
            if (value == NO_OFFSET) {
                throw new NoOffsetException(field + " unavailable");
            }
            if (value == UNSUPPORTED) {
                throw new RuntimeException(field + " unavailable");
            }
            return value;
        }
    }
}
