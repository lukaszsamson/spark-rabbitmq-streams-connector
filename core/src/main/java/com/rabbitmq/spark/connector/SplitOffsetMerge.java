package com.rabbitmq.spark.connector;

import java.util.*;

/**
 * Merges split-partition offsets back into a single checkpoint offset per stream.
 *
 * <p>When {@code minPartitions} splits a single stream into multiple Spark partitions,
 * each split reports its own committed offset range. This algorithm merges those ranges
 * back into a single contiguous next-offset for the stream.
 *
 * <p>Algorithm: given {@code base} (current next offset) and split results
 * {@code (start_i, end_i, committed_i)}, dedupe by {@code (start_i, end_i)} keeping
 * the max {@code committed_i} per range, sort by {@code start_i}, then advance while
 * {@code start_i == base} and {@code committed_i == end_i}, setting
 * {@code base = end_i}.
 */
public final class SplitOffsetMerge {

    private SplitOffsetMerge() {}

    /**
     * A split result representing one partition's committed state.
     *
     * @param startOffset inclusive start of the split range
     * @param endOffset exclusive end of the split range
     * @param committedOffset the offset actually committed for this split
     *                        (equals endOffset when fully committed)
     */
    public record SplitResult(long startOffset, long endOffset, long committedOffset) {}

    /**
     * Merge split results into a single next-offset for a stream.
     *
     * @param base the current next offset (checkpoint position before this batch)
     * @param splits the split results (may be empty, unordered, or contain duplicates)
     * @return the merged next offset (advanced as far as contiguous completed splits allow)
     */
    public static long merge(long base, List<SplitResult> splits) {
        if (splits == null || splits.isEmpty()) {
            return base;
        }

        // Dedupe by (start, end), keeping max committedOffset per range
        Map<Long, Map<Long, Long>> deduped = new LinkedHashMap<>();
        for (SplitResult split : splits) {
            deduped
                .computeIfAbsent(split.startOffset(), k -> new LinkedHashMap<>())
                .merge(split.endOffset(), split.committedOffset(), Math::max);
        }

        // Flatten and sort by startOffset
        List<SplitResult> sorted = new ArrayList<>();
        for (Map.Entry<Long, Map<Long, Long>> outer : deduped.entrySet()) {
            long start = outer.getKey();
            for (Map.Entry<Long, Long> inner : outer.getValue().entrySet()) {
                sorted.add(new SplitResult(start, inner.getKey(), inner.getValue()));
            }
        }
        sorted.sort(Comparator.comparingLong(SplitResult::startOffset));

        // Advance base through contiguous fully-committed splits
        long current = base;
        for (SplitResult split : sorted) {
            if (split.startOffset() == current && split.committedOffset() == split.endOffset()) {
                current = split.endOffset();
            } else {
                break;
            }
        }

        return current;
    }
}
