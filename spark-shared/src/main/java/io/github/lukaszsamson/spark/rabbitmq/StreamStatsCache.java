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
    /** Sentinel for "field threw a non-NoOffset RuntimeException at capture time". */
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
        // Capture outside the cache so concurrent loaders don't double-count
        // even though queryStreamStats is normally cheap. We tolerate the rare
        // duplicate query during a stampede; correctness only requires that
        // every read sees a consistent snapshot.
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
        long firstOffset = readOrSentinel(live::firstOffset);
        long committedOffset = readOrSentinel(live::committedOffset);
        long committedChunkId = readOrSentinel(live::committedChunkId);
        return new SnapshotStreamStats(firstOffset, committedOffset, committedChunkId);
    }

    private static long readOrSentinel(LongSupplierWithExceptions supplier) {
        try {
            return supplier.get();
        } catch (NoOffsetException e) {
            return NO_OFFSET;
        } catch (RuntimeException e) {
            return UNSUPPORTED;
        }
    }

    @FunctionalInterface
    private interface LongSupplierWithExceptions {
        long get();
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
