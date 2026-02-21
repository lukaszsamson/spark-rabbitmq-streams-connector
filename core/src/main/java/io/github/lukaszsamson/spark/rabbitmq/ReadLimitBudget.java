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
     * stream's available offset span.
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
        Map<String, Long> allocated = allocateEvenlyWithCaps(pending, budget);

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
     * the same even-per-stream capped distribution as record budgets.
     *
     * @param startOffsets          stream → start offset (inclusive)
     * @param tailOffsets           stream → tail offset (exclusive)
     * @param byteBudget            max bytes to allow
     * @param estimatedMessageSize  estimated bytes per message
     * @return stream → end offset (exclusive)
     */
    public static Map<String, Long> distributeByteBudget(
            Map<String, Long> startOffsets,
            Map<String, Long> tailOffsets,
            long byteBudget,
            int estimatedMessageSize) {

        long recordBudget = Math.max(1, byteBudget / Math.max(1, estimatedMessageSize));
        return distributeRecordBudget(startOffsets, tailOffsets, recordBudget);
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
     * <p>Streams are processed in insertion order for deterministic tie-breaking.
     */
    static Map<String, Long> allocateEvenlyWithCaps(
            Map<String, Long> capacity, long budget) {

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
            long baseShare = remaining / eligible.size();
            long remainder = remaining % eligible.size();

            if (baseShare == 0) {
                for (String stream : eligible) {
                    if (remaining == 0) {
                        break;
                    }
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
            for (int i = 0; i < eligible.size(); i++) {
                String stream = eligible.get(i);
                long used = allocation.getOrDefault(stream, 0L);
                long cap = Math.max(0L, capacity.getOrDefault(stream, 0L) - used);
                if (cap <= 0) {
                    continue;
                }

                long requested = baseShare + (i < remainder ? 1 : 0);
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

    /**
     * Allocate a budget proportionally across entries.
     * Each non-empty entry gets at least 1.
     * Uses floor allocation with remainder distributed by largest fractional share.
     */
    static Map<String, Long> allocateProportionally(
            Map<String, Long> pending, long budget) {

        Map<String, Long> allocation = new LinkedHashMap<>();
        Map<String, Double> fractional = new LinkedHashMap<>();
        long allocated = 0;

        // Pre-compute total pending once (O(n) instead of O(n^2))
        long totalPending = 0;
        for (long p : pending.values()) {
            totalPending += Math.max(0, p);
        }
        if (totalPending == 0) {
            for (String stream : pending.keySet()) {
                allocation.put(stream, 0L);
            }
            return allocation;
        }

        long nonEmptyCount = pending.values().stream().filter(p -> p > 0).count();
        if (budget <= nonEmptyCount) {
            for (String stream : pending.keySet()) {
                allocation.put(stream, 0L);
            }
            pending.entrySet().stream()
                    .filter(e -> e.getValue() > 0)
                    .sorted(Map.Entry.<String, Long>comparingByValue().reversed())
                    .limit(budget)
                    .forEach(e -> allocation.put(e.getKey(), 1L));
            return allocation;
        }

        for (Map.Entry<String, Long> entry : pending.entrySet()) {
            String stream = entry.getKey();
            long p = entry.getValue();
            if (p <= 0) {
                allocation.put(stream, 0L);
                continue;
            }

            double share = (double) p / totalPending * budget;
            long floor = Math.max(1, (long) share);
            allocation.put(stream, floor);
            fractional.put(stream, share - floor);
            allocated += floor;
        }

        // Distribute remainder by largest fractional share
        long remaining = budget - allocated;
        if (remaining > 0) {
            fractional.entrySet().stream()
                    .sorted(Map.Entry.<String, Double>comparingByValue().reversed())
                    .limit(remaining)
                    .forEach(e -> allocation.merge(e.getKey(), 1L, Long::sum));
        }

        // Ensure no allocation exceeds pending
        for (Map.Entry<String, Long> entry : pending.entrySet()) {
            String stream = entry.getKey();
            long alloc = allocation.getOrDefault(stream, 0L);
            if (alloc > entry.getValue()) {
                allocation.put(stream, entry.getValue());
            }
        }

        return allocation;
    }
}
