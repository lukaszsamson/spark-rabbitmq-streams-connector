package io.github.lukaszsamson.spark.rabbitmq;

import java.util.*;

/**
 * Distributes a read budget (records or bytes) across streams using deterministic,
 * even-per-stream allocation.
 *
 * <p>Offset deltas ({@code tail - start}) are treated as range caps, not message counts.
 *
 * <p>Algorithm:
 * <ol>
 *   <li>Compute available range span ({@code max(0, tail - start)}) per stream</li>
 *   <li>If total available span ≤ budget, return tail offsets (all data fits)</li>
 *   <li>Otherwise, allocate budget evenly across non-empty streams, capped by available span</li>
 *   <li>Redistribute any leftover budget from capped streams to remaining streams</li>
 * </ol>
 */
public final class ReadLimitBudget {

    private ReadLimitBudget() {}

    /**
     * Distribute a record budget evenly across non-empty streams, capped by each
     * stream's available offset span. Equivalent to
     * {@link #distributeRecordBudget(Map, Map, long, long)} with {@code rotation = 0}.
     *
     * @param startOffsets  stream → start offset (inclusive)
     * @param tailOffsets   stream → tail offset (exclusive)
     * @param budget        max records to allow across all streams
     * @return stream → end offset (exclusive), capped by budget; streams with
     *         no pending data are returned with their start offset
     */
    public static Map<String, Long> distributeRecordBudget(
            Map<String, Long> startOffsets,
            Map<String, Long> tailOffsets,
            long budget) {
        return distributeRecordBudget(startOffsets, tailOffsets, budget, 0L);
    }

    /**
     * Distribute a record budget evenly across non-empty streams, capped by each
     * stream's available offset span. Allocation order is rotated by
     * {@code rotation mod eligible.size()} so that callers passing an
     * incrementing trigger counter avoid starving the tail of the eligible
     * list when {@code budget < count} or when {@code budget % count != 0}.
     */
    public static Map<String, Long> distributeRecordBudget(
            Map<String, Long> startOffsets,
            Map<String, Long> tailOffsets,
            long budget,
            long rotation) {

        if (budget <= 0) {
            return new LinkedHashMap<>(startOffsets);
        }

        // Compute available range span per stream
        Map<String, Long> pending = new LinkedHashMap<>();
        long totalPending = 0;
        for (Map.Entry<String, Long> entry : tailOffsets.entrySet()) {
            String stream = entry.getKey();
            long start = startOffsets.getOrDefault(stream, 0L);
            long tail = entry.getValue();
            long p = Math.max(0, tail - start);
            pending.put(stream, p);
            totalPending += p;
        }

        if (totalPending == 0) {
            return new LinkedHashMap<>(startOffsets);
        }

        // All data fits within budget
        if (totalPending <= budget) {
            return new LinkedHashMap<>(tailOffsets);
        }

        // Even allocation across streams, capped by available range spans.
        Map<String, Long> allocated = allocateEvenlyWithCaps(pending, budget, rotation);

        // Compute end offsets
        Map<String, Long> endOffsets = new LinkedHashMap<>();
        for (Map.Entry<String, Long> entry : tailOffsets.entrySet()) {
            String stream = entry.getKey();
            long start = startOffsets.getOrDefault(stream, 0L);
            long alloc = allocated.getOrDefault(stream, 0L);
            long end = Math.min(start + alloc, entry.getValue());
            endOffsets.put(stream, end);
        }
        return endOffsets;
    }

    /**
     * Distribute a byte budget by converting bytes to a record budget and applying
     * the same even-per-stream capped distribution as record budgets. Equivalent
     * to {@link #distributeByteBudget(Map, Map, long, int, long)} with
     * {@code rotation = 0}.
     */
    public static Map<String, Long> distributeByteBudget(
            Map<String, Long> startOffsets,
            Map<String, Long> tailOffsets,
            long byteBudget,
            int estimatedMessageSize) {
        return distributeByteBudget(startOffsets, tailOffsets, byteBudget,
                estimatedMessageSize, 0L);
    }

    /**
     * Distribute a byte budget by converting bytes to a record budget and applying
     * the same rotated even-per-stream capped distribution as
     * {@link #distributeRecordBudget(Map, Map, long, long)}.
     */
    public static Map<String, Long> distributeByteBudget(
            Map<String, Long> startOffsets,
            Map<String, Long> tailOffsets,
            long byteBudget,
            int estimatedMessageSize,
            long rotation) {

        long recordBudget = Math.max(1, byteBudget / Math.max(1, estimatedMessageSize));
        return distributeRecordBudget(startOffsets, tailOffsets, recordBudget, rotation);
    }

    /**
     * Given two sets of end offsets, return the element-wise minimum (most restrictive).
     */
    public static Map<String, Long> mostRestrictive(
            Map<String, Long> a, Map<String, Long> b) {
        Map<String, Long> result = new LinkedHashMap<>();
        Set<String> allStreams = new LinkedHashSet<>();
        allStreams.addAll(a.keySet());
        allStreams.addAll(b.keySet());
        for (String stream : allStreams) {
            long va = a.getOrDefault(stream, Long.MAX_VALUE);
            long vb = b.getOrDefault(stream, Long.MAX_VALUE);
            result.put(stream, Math.min(va, vb));
        }
        return result;
    }

    /**
     * Allocate budget evenly across entries, capped by their per-entry capacities.
     *
     * <p>Streams are processed in insertion order for deterministic tie-breaking,
     * but the start of the iteration is rotated by {@code rotation mod size} so
     * that successive callers passing an incrementing rotation distribute the
     * remainder (and any sub-eligible-size budget) round-robin across all
     * streams instead of always favoring the first {@code remainder} entries.
     */
    static Map<String, Long> allocateEvenlyWithCaps(
            Map<String, Long> capacity, long budget, long rotation) {

        Map<String, Long> allocation = new LinkedHashMap<>();
        for (String stream : capacity.keySet()) {
            allocation.put(stream, 0L);
        }
        if (budget <= 0) {
            return allocation;
        }

        List<String> eligible = new ArrayList<>();
        for (Map.Entry<String, Long> entry : capacity.entrySet()) {
            if (entry.getValue() > 0) {
                eligible.add(entry.getKey());
            }
        }

        long remaining = budget;
        while (remaining > 0 && !eligible.isEmpty()) {
            int size = eligible.size();
            long baseShare = remaining / size;
            long remainder = remaining % size;
            int rot = (int) Math.floorMod(rotation, (long) size);

            if (baseShare == 0) {
                // Hand out one record at a time starting from the rotated index
                // so streams further down the eligible list also get a turn.
                for (int j = 0; j < size; j++) {
                    if (remaining == 0) {
                        break;
                    }
                    String stream = eligible.get((j + rot) % size);
                    long used = allocation.getOrDefault(stream, 0L);
                    long cap = Math.max(0L, capacity.getOrDefault(stream, 0L) - used);
                    if (cap <= 0) {
                        continue;
                    }
                    allocation.put(stream, used + 1);
                    remaining--;
                }
                break;
            }

            long spent = 0;
            List<String> nextEligible = new ArrayList<>();
            for (int i = 0; i < size; i++) {
                String stream = eligible.get(i);
                long used = allocation.getOrDefault(stream, 0L);
                long cap = Math.max(0L, capacity.getOrDefault(stream, 0L) - used);
                if (cap <= 0) {
                    continue;
                }

                int distFromRotatedStart = (int) Math.floorMod((long) i - rot, (long) size);
                long requested = baseShare + (distFromRotatedStart < remainder ? 1 : 0);
                long granted = Math.min(requested, cap);
                allocation.put(stream, used + granted);
                spent += granted;

                if (cap > granted) {
                    nextEligible.add(stream);
                }
            }

            if (spent == 0) {
                break;
            }
            remaining -= spent;
            eligible = nextEligible;
        }

        return allocation;
    }

}
