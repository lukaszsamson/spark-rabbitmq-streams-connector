package io.github.lukaszsamson.spark.rabbitmq;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.ConcurrentHashMap;

/**
 * JVM-local accumulator for tracking actual message sizes across partition
 * readers, enabling the driver to maintain a running average for byte-based
 * admission control.
 *
 * <p>Metrics are tracked per scope to avoid cross-query contamination in
 * shared-driver JVMs. This tracker is a fallback path used when Spark
 * accumulators are unavailable; in distributed execution, the preferred
 * transport for reader byte/record totals is driver-visible accumulators.
 */
final class MessageSizeTracker {

    private static final String DEFAULT_SCOPE = "__default__";
    private static final ConcurrentHashMap<String, Totals> totalsByScope = new ConcurrentHashMap<>();

    private MessageSizeTracker() {}

    private static final class Totals {
        private final AtomicLong totalBytes = new AtomicLong();
        private final AtomicLong totalRecords = new AtomicLong();
    }

    private static String normalizeScope(String scope) {
        if (scope == null || scope.isBlank()) {
            return DEFAULT_SCOPE;
        }
        return scope;
    }

    /**
     * Record bytes and records from a completed partition reader.
     * Called from executor-side PartitionReader on close.
     */
    static void record(long bytes, long records) {
        record(null, bytes, records);
    }

    static void record(String scope, long bytes, long records) {
        Totals totals = totalsByScope.computeIfAbsent(normalizeScope(scope), ignored -> new Totals());
        totals.totalBytes.addAndGet(bytes);
        totals.totalRecords.addAndGet(records);
    }

    /**
     * Drain accumulated stats and compute average message size.
     * Called from driver-side MicroBatchStream during commit.
     *
     * @param currentEstimate the current estimate to return if no data accumulated
     * @return updated average message size, or currentEstimate if no new data
     */
    static int drainAverage(int currentEstimate) {
        return drainAverage(null, currentEstimate);
    }

    static int drainAverage(String scope, int currentEstimate) {
        Totals totals = totalsByScope.get(normalizeScope(scope));
        if (totals == null) {
            return currentEstimate;
        }
        long bytes = totals.totalBytes.getAndSet(0);
        long records = totals.totalRecords.getAndSet(0);
        if (records <= 0) {
            return currentEstimate;
        }
        return Math.max(1, (int) (bytes / records));
    }

    static void clear(String scope) {
        totalsByScope.remove(normalizeScope(scope));
    }
}
