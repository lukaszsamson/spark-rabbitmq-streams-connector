package io.github.lukaszsamson.spark.rabbitmq;

import java.util.concurrent.atomic.AtomicLong;

/**
 * JVM-local accumulator for tracking actual message sizes across partition
 * readers, enabling the driver to maintain a running average for byte-based
 * admission control.
 *
 * <p>In Spark local mode, readers and the driver share a JVM, so the running
 * average is exact. In distributed mode, only co-located reader metrics are
 * captured; the estimate starts from the configured {@code estimatedMessageSizeBytes}
 * and improves as local tasks contribute.
 */
final class MessageSizeTracker {

    private static final AtomicLong totalBytes = new AtomicLong();
    private static final AtomicLong totalRecords = new AtomicLong();

    private MessageSizeTracker() {}

    /**
     * Record bytes and records from a completed partition reader.
     * Called from executor-side PartitionReader on close.
     */
    static void record(long bytes, long records) {
        totalBytes.addAndGet(bytes);
        totalRecords.addAndGet(records);
    }

    /**
     * Drain accumulated stats and compute average message size.
     * Called from driver-side MicroBatchStream during commit.
     *
     * @param currentEstimate the current estimate to return if no data accumulated
     * @return updated average message size, or currentEstimate if no new data
     */
    static int drainAverage(int currentEstimate) {
        long bytes = totalBytes.getAndSet(0);
        long records = totalRecords.getAndSet(0);
        if (records <= 0) {
            return currentEstimate;
        }
        return Math.max(1, (int) (bytes / records));
    }
}
