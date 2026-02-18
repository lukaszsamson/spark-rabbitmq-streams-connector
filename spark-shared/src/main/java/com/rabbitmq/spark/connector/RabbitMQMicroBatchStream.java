package com.rabbitmq.spark.connector;

import com.rabbitmq.stream.Environment;
import com.rabbitmq.stream.NoOffsetException;
import com.rabbitmq.stream.StreamDoesNotExistException;
import com.rabbitmq.stream.StreamStats;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.connector.read.streaming.*;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

/**
 * Core micro-batch streaming source for RabbitMQ Streams.
 *
 * <p>Implements the full streaming source lifecycle including:
 * <ul>
 *   <li>{@link MicroBatchStream} – core offset-based micro-batch reads</li>
 *   <li>{@link SupportsAdmissionControl} – rate limiting via
 *       {@code maxRecordsPerTrigger} and {@code maxBytesPerTrigger}</li>
 *   <li>{@link SupportsTriggerAvailableNow} – snapshot-based bounded processing</li>
 *   <li>{@link ReportsSourceMetrics} – lag metrics for query progress</li>
 * </ul>
 */
final class RabbitMQMicroBatchStream
        implements MicroBatchStream, SupportsAdmissionControl,
                   SupportsTriggerAvailableNow, ReportsSourceMetrics {

    private static final Logger LOG = LoggerFactory.getLogger(RabbitMQMicroBatchStream.class);

    /** Timeout for broker offset commit (best-effort, Spark checkpoint is source of truth). */
    private static final int COMMIT_TIMEOUT_SECONDS = 30;

    private final ConnectorOptions options;
    private final StructType schema;
    private final String checkpointLocation;
    private final String effectiveConsumerName;
    private final ExecutorService brokerCommitExecutor;

    /** Discovered streams (lazily initialized). */
    private volatile List<String> streams;

    /** Driver-side Environment for broker queries (lazily initialized). */
    private volatile Environment environment;

    /** Cached latest offset for reportLatestOffset(). */
    private volatile RabbitMQStreamOffset cachedLatestOffset;
    /** Cached true tail offset (before ReadLimit application). */
    private volatile RabbitMQStreamOffset cachedTailOffset;

    /**
     * Snapshot of tail offsets captured by {@link #prepareForTriggerAvailableNow()}.
     * When non-null, {@link #latestOffset(Offset, ReadLimit)} will not exceed these.
     */
    private volatile Map<String, Long> availableNowSnapshot;

    /** Running average message size in bytes, for byte-based admission control. */
    private int estimatedMessageSize;
    /** Initial offsets resolved for this stream instance (used for timestamp first batch seek). */
    private volatile Map<String, Long> initialOffsets;
    /** Last end offsets successfully persisted to broker (next offsets). */
    private volatile Map<String, Long> lastStoredEndOffsets;

    RabbitMQMicroBatchStream(ConnectorOptions options, StructType schema,
                              String checkpointLocation) {
        this.options = options;
        this.schema = schema;
        this.checkpointLocation = checkpointLocation;
        this.effectiveConsumerName = deriveConsumerName(options, checkpointLocation);
        this.brokerCommitExecutor = Executors.newFixedThreadPool(
                Math.max(1, Math.min(
                        Runtime.getRuntime().availableProcessors(),
                        StoredOffsetLookup.MAX_CONCURRENT_LOOKUPS)));
        this.estimatedMessageSize = options.getEstimatedMessageSizeBytes();
        if (options.getConsumerName() == null && this.effectiveConsumerName != null) {
            LOG.info("consumerName not set; derived stable name '{}' from checkpoint location",
                    this.effectiveConsumerName);
        }
    }

    // ---- SparkDataStream lifecycle ----

    @Override
    public Offset initialOffset() {
        List<String> streams = discoverStreams();
        String consumerName = effectiveConsumerName;
        boolean consumerNameExplicit = options.getConsumerName() != null
                && !options.getConsumerName().isEmpty();

        if (!options.isServerSideOffsetTracking(true)) {
            LOG.debug("Server-side offset tracking disabled, skipping broker offset lookup");
            Map<String, Long> offsets = new LinkedHashMap<>();
            for (String stream : streams) {
                offsets.put(stream, resolveStartingOffset(stream));
            }
            this.initialOffsets = new LinkedHashMap<>(offsets);
            LOG.info("Initial offsets from startingOffsets={}: {}", options.getStartingOffsets(), offsets);
            return new RabbitMQStreamOffset(offsets);
        }

        // 1. Try broker stored offsets if consumerName is set
        if (consumerName != null && !consumerName.isEmpty()) {
            try {
                StoredOffsetLookup.LookupResult result =
                        StoredOffsetLookup.lookupWithDetails(
                                getEnvironment(), consumerName, streams);

                Map<String, Long> stored = result.getOffsets();

                if (result.hasFailures()) {
                    if (consumerNameExplicit) {
                        // Explicit consumerName: fail fast on non-fatal lookup failures
                        // (e.g. tracking-consumer limits)
                        throw new IllegalStateException(
                                "Stored offset lookup failed for consumer '" + consumerName +
                                        "' on streams: " + result.getFailedStreams() +
                                        ". Since consumerName is explicitly configured, " +
                                        "non-fatal lookup failures are treated as fatal.");
                    }
                    LOG.warn("Stored offset lookup had non-fatal failures for derived consumer '{}'"
                                    + " on streams {}. Falling back to startingOffsets for those streams.",
                            consumerName, result.getFailedStreams());
                }

                if (!stored.isEmpty()) {
                    LOG.info("Recovered stored offsets from broker for consumer '{}': {}",
                            consumerName, stored);
                    // Fill in any missing streams with starting offset resolution
                    Map<String, Long> merged = new LinkedHashMap<>(stored);
                    for (String stream : streams) {
                        merged.putIfAbsent(stream, resolveStartingOffset(stream));
                    }
                    this.initialOffsets = new LinkedHashMap<>(merged);
                    return new RabbitMQStreamOffset(merged);
                }
                LOG.info("No stored offsets found for consumer '{}', falling back to startingOffsets",
                        consumerName);
            } catch (IllegalStateException e) {
                // Fatal error (auth, connection, stream does not exist) — fail fast
                throw e;
            } catch (Exception e) {
                if (consumerNameExplicit) {
                    // Explicit consumerName: fail fast on lookup errors
                    throw new IllegalStateException(
                            "Failed to look up stored offsets for consumer '" +
                                    consumerName + "'", e);
                }
                LOG.warn("Failed to look up stored offsets for derived consumer '{}', "
                                + "falling back to startingOffsets",
                        consumerName, e);
            }
        }

        // 2. Fall back to startingOffsets
        Map<String, Long> offsets = new LinkedHashMap<>();
        for (String stream : streams) {
            offsets.put(stream, resolveStartingOffset(stream));
        }
        this.initialOffsets = new LinkedHashMap<>(offsets);
        LOG.info("Initial offsets from startingOffsets={}: {}", options.getStartingOffsets(), offsets);
        return new RabbitMQStreamOffset(offsets);
    }

    @Override
    public Offset deserializeOffset(String json) {
        return RabbitMQStreamOffset.fromJson(json);
    }

    @Override
    public void commit(Offset end) {
        // Update running average message size from completed batch readers
        int updatedSize = MessageSizeTracker.drainAverage(estimatedMessageSize);
        if (updatedSize != estimatedMessageSize) {
            LOG.debug("Updated estimated message size: {} -> {} bytes",
                    estimatedMessageSize, updatedSize);
            estimatedMessageSize = updatedSize;
        }

        RabbitMQStreamOffset endOffset = (RabbitMQStreamOffset) end;
        persistBrokerOffsets(endOffset.getStreamOffsets());
    }

    @Override
    public void stop() {
        // Spark can stop AvailableNow queries after the final processed batch without invoking
        // source commit for that terminal offset. Persist the latest known end offsets as
        // best-effort broker metadata before shutting down commit resources.
        RabbitMQStreamOffset latest = cachedLatestOffset;
        if (latest != null) {
            try {
                persistBrokerOffsets(latest.getStreamOffsets());
            } catch (Exception e) {
                LOG.warn("Failed to persist broker offsets during stop()", e);
            }
        }

        brokerCommitExecutor.shutdownNow();
        if (environment != null) {
            try {
                environment.close();
            } catch (Exception e) {
                LOG.warn("Error closing environment", e);
            }
            environment = null;
        }
    }

    // ---- MicroBatchStream ----

    @Override
    public Offset latestOffset() {
        return latestOffset(null, ReadLimit.allAvailable());
    }

    @Override
    public InputPartition[] planInputPartitions(Offset start, Offset end) {
        RabbitMQStreamOffset startOffset = (RabbitMQStreamOffset) start;
        RabbitMQStreamOffset endOffset = (RabbitMQStreamOffset) end;

        // Collect validated per-stream ranges
        Map<String, long[]> validRanges = new LinkedHashMap<>();
        for (Map.Entry<String, Long> entry : endOffset.getStreamOffsets().entrySet()) {
            String stream = entry.getKey();
            long endOff = entry.getValue();
            long startOff = startOffset.getStreamOffsets().getOrDefault(stream, endOff);

            if (endOff <= startOff) {
                continue;
            }

            // Check for retention truncation: if requested start is below first available
            startOff = validateStartOffset(stream, startOff, endOff);
            if (startOff < 0) {
                // Stream should be skipped (deleted or empty)
                continue;
            }
            if (endOff <= startOff) {
                continue;
            }

            validRanges.put(stream, new long[]{startOff, endOff});
        }

        // Apply minPartitions splitting if configured
        List<InputPartition> partitions = new ArrayList<>();
        Integer minPartitions = options.getMinPartitions();
        if (minPartitions != null && minPartitions > validRanges.size() && !validRanges.isEmpty()) {
            planWithSplitting(partitions, validRanges, minPartitions);
        } else {
            for (Map.Entry<String, long[]> entry : validRanges.entrySet()) {
                long rangeStart = entry.getValue()[0];
                partitions.add(new RabbitMQInputPartition(
                        entry.getKey(), rangeStart, entry.getValue()[1], options,
                        useConfiguredStartingOffset(entry.getKey(), rangeStart)));
            }
        }

        LOG.info("Planned {} input partitions for micro-batch [{} → {}]",
                partitions.size(), start, end);
        return partitions.toArray(new InputPartition[0]);
    }

    /**
     * Split streams into more partitions when minPartitions > stream count.
     * Each stream gets splits proportional to its share of total pending messages.
     */
    private void planWithSplitting(List<InputPartition> partitions,
                                    Map<String, long[]> ranges, int minPartitions) {
        long totalMessages = 0;
        for (long[] range : ranges.values()) {
            totalMessages += range[1] - range[0];
        }
        if (totalMessages == 0) {
            return;
        }

        // Allocate splits per stream proportional to message count
        Map<String, Integer> splitsPerStream = new LinkedHashMap<>();
        Map<String, Double> fractional = new LinkedHashMap<>();
        int allocated = 0;

        for (Map.Entry<String, long[]> entry : ranges.entrySet()) {
            String stream = entry.getKey();
            long messages = entry.getValue()[1] - entry.getValue()[0];
            double share = (double) messages / totalMessages * minPartitions;
            int floor = Math.max(1, (int) share);
            splitsPerStream.put(stream, floor);
            fractional.put(stream, share - floor);
            allocated += floor;
        }

        // Distribute remainder by largest fractional share
        int remaining = minPartitions - allocated;
        if (remaining > 0) {
            fractional.entrySet().stream()
                    .sorted(Map.Entry.<String, Double>comparingByValue().reversed())
                    .limit(remaining)
                    .forEach(e -> splitsPerStream.merge(e.getKey(), 1, Integer::sum));
        }

        // Create split partitions
        for (Map.Entry<String, long[]> entry : ranges.entrySet()) {
            String stream = entry.getKey();
            long start = entry.getValue()[0];
            long end = entry.getValue()[1];
            int numSplits = splitsPerStream.getOrDefault(stream, 1);
            long messages = end - start;

            if (numSplits <= 1 || messages <= 1) {
                partitions.add(new RabbitMQInputPartition(
                        stream, start, end, options,
                        useConfiguredStartingOffset(stream, start)));
                continue;
            }

            long chunkSize = messages / numSplits;
            long rem = messages % numSplits;
            long currentStart = start;
            for (int i = 0; i < numSplits; i++) {
                long splitSize = chunkSize + (i < rem ? 1 : 0);
                if (splitSize == 0) break;
                long splitEnd = currentStart + splitSize;
                partitions.add(new RabbitMQInputPartition(
                        stream, currentStart, splitEnd, options,
                        useConfiguredStartingOffset(stream, currentStart)));
                currentStart = splitEnd;
            }
        }

        LOG.info("Applied minPartitions={} splitting: {} partitions from {} streams",
                minPartitions, partitions.size(), ranges.size());
    }

    /**
     * Validate a partition's start offset against the broker's first available offset.
     * Handles retention truncation and stream deletion per failOnDataLoss setting.
     *
     * @return the validated start offset, or -1 if the partition should be skipped
     */
    private long validateStartOffset(String stream, long startOff, long endOff) {
        Environment env;
        try {
            env = getEnvironment();
        } catch (Exception e) {
            // Cannot connect to broker for validation — proceed without check
            LOG.debug("Cannot validate offsets for stream '{}': {}", stream, e.getMessage());
            return startOff;
        }

        try {
            StreamStats stats = env.queryStreamStats(stream);
            long firstAvailable = stats.firstOffset();
            if (startOff < firstAvailable) {
                if (options.isFailOnDataLoss()) {
                    throw new IllegalStateException(
                            "Requested start offset " + startOff +
                                    " is before the first available offset " + firstAvailable +
                                    " in stream '" + stream + "'. Data may have been lost due to " +
                                    "retention policy. Set failOnDataLoss=false to advance.");
                }
                LOG.warn("Start offset {} is before first available {} in stream '{}', " +
                                "advancing to first available (data loss detected)",
                        startOff, firstAvailable, stream);
                return firstAvailable;
            }
            return startOff;
        } catch (NoOffsetException e) {
            // Stream is empty
            return -1;
        } catch (com.rabbitmq.stream.StreamDoesNotExistException e) {
            if (options.isFailOnDataLoss()) {
                throw new IllegalStateException(
                        "Stream '" + stream + "' no longer exists. " +
                                "Set failOnDataLoss=false to skip.", e);
            }
            LOG.warn("Stream '{}' no longer exists, skipping partition (failOnDataLoss=false)",
                    stream);
            return -1;
        } catch (IllegalStateException e) {
            throw e;
        } catch (Exception e) {
            // Non-fatal: cannot validate, proceed without check
            LOG.warn("Failed to validate stream '{}' offsets: {}", stream, e.getMessage());
            return startOff;
        }
    }

    @Override
    public PartitionReaderFactory createReaderFactory() {
        return new RabbitMQPartitionReaderFactory(options, schema);
    }

    // ---- SupportsAdmissionControl ----

    @Override
    public ReadLimit getDefaultReadLimit() {
        Long maxRows = options.getMaxRecordsPerTrigger();
        Long maxBytes = options.getMaxBytesPerTrigger();

        if (maxRows != null && maxBytes != null) {
            ReadLimit maxBytesLimit = createReadMaxBytesLimit(maxBytes);
            if (maxBytesLimit != null) {
                return ReadLimit.compositeLimit(new ReadLimit[]{
                        ReadLimit.maxRows(maxRows),
                        maxBytesLimit
                });
            }
            long bytesAsRows = Math.max(1, maxBytes / estimatedMessageSize);
            return ReadLimit.compositeLimit(new ReadLimit[]{
                    ReadLimit.maxRows(maxRows),
                    ReadLimit.maxRows(bytesAsRows)
            });
        }
        if (maxRows != null) {
            return ReadLimit.maxRows(maxRows);
        }
        if (maxBytes != null) {
            ReadLimit maxBytesLimit = createReadMaxBytesLimit(maxBytes);
            if (maxBytesLimit != null) {
                return maxBytesLimit;
            }
            long bytesAsRows = Math.max(1, maxBytes / estimatedMessageSize);
            return ReadLimit.maxRows(bytesAsRows);
        }
        return ReadLimit.allAvailable();
    }

    @Override
    public Offset latestOffset(Offset startOffset, ReadLimit limit) {
        Map<String, Long> tailOffsets = availableNowSnapshot != null
                ? new LinkedHashMap<>(availableNowSnapshot)
                : queryTailOffsets();

        // Guard: if tailOffsets is empty, no streams could be queried
        if (tailOffsets.isEmpty()) {
            LOG.debug("Tail offsets query returned empty map");
            return startOffset != null ? startOffset : new RabbitMQStreamOffset(Map.of());
        }

        if (startOffset == null) {
            RabbitMQStreamOffset latest = new RabbitMQStreamOffset(tailOffsets);
            cachedTailOffset = latest;
            cachedLatestOffset = latest;
            return latest;
        }

        RabbitMQStreamOffset start = (RabbitMQStreamOffset) startOffset;
        Map<String, Long> startMap = start.getStreamOffsets();
        Map<String, Long> effectiveStartMap = new LinkedHashMap<>(startMap);
        for (String stream : tailOffsets.keySet()) {
            effectiveStartMap.computeIfAbsent(stream, this::resolveStartingOffset);
        }

        // Ensure tail offsets never go backwards due to failed per-stream queries
        // (queryStreamTailOffset returns 0 on failure which may be less than start)
        Map<String, Long> safeTail = new LinkedHashMap<>(tailOffsets);
        for (Map.Entry<String, Long> entry : safeTail.entrySet()) {
            long startOff = effectiveStartMap.getOrDefault(entry.getKey(), 0L);
            if (entry.getValue() < startOff) {
                entry.setValue(startOff);
            }
        }
        tailOffsets = safeTail;
        cachedTailOffset = new RabbitMQStreamOffset(tailOffsets);

        // Check if any stream has new data
        boolean hasNewData = false;
        for (Map.Entry<String, Long> entry : tailOffsets.entrySet()) {
            long startOff = effectiveStartMap.getOrDefault(entry.getKey(), 0L);
            if (entry.getValue() > startOff) {
                hasNewData = true;
                break;
            }
        }
        if (!hasNewData) {
            LOG.debug("No new data available, returning stable start offsets");
            if (effectiveStartMap.equals(startMap)) {
                cachedLatestOffset = start;
                return start;
            }
            RabbitMQStreamOffset expanded = new RabbitMQStreamOffset(effectiveStartMap);
            cachedLatestOffset = expanded;
            return expanded;
        }

        // Apply read limit budget
        Map<String, Long> endOffsets = applyReadLimit(effectiveStartMap, tailOffsets, limit);

        RabbitMQStreamOffset latest = new RabbitMQStreamOffset(endOffsets);
        cachedLatestOffset = latest;
        return latest;
    }

    @Override
    public Offset reportLatestOffset() {
        return cachedTailOffset != null ? cachedTailOffset : cachedLatestOffset;
    }

    // ---- SupportsTriggerAvailableNow ----

    @Override
    public void prepareForTriggerAvailableNow() {
        Map<String, Long> snapshot = queryTailOffsetsForAvailableNow();
        this.availableNowSnapshot = snapshot;
        LOG.info("Trigger.AvailableNow: snapshot tail offsets = {}", snapshot);
    }

    // ---- ReportsSourceMetrics ----

    @Override
    public Map<String, String> metrics(Optional<Offset> latestConsumedOffset) {
        Map<String, String> metrics = new LinkedHashMap<>();
        RabbitMQStreamOffset tail = cachedTailOffset != null ? cachedTailOffset : cachedLatestOffset;

        if (tail == null || latestConsumedOffset.isEmpty()) {
            metrics.put("minOffsetsBehindLatest", "0");
            metrics.put("maxOffsetsBehindLatest", "0");
            metrics.put("avgOffsetsBehindLatest", "0.0");
            return metrics;
        }

        RabbitMQStreamOffset consumed = (RabbitMQStreamOffset) latestConsumedOffset.get();
        Map<String, Long> tailMap = tail.getStreamOffsets();
        Map<String, Long> consumedMap = consumed.getStreamOffsets();

        long minLag = Long.MAX_VALUE;
        long maxLag = 0;
        long totalLag = 0;
        int count = 0;

        for (Map.Entry<String, Long> entry : tailMap.entrySet()) {
            String stream = entry.getKey();
            long tailOff = entry.getValue();
            long consumedOff = consumedMap.getOrDefault(stream, 0L);
            long lag = Math.max(0, tailOff - consumedOff);
            minLag = Math.min(minLag, lag);
            maxLag = Math.max(maxLag, lag);
            totalLag += lag;
            count++;
        }

        if (count == 0) {
            minLag = 0;
        }

        double avgLag = count > 0 ? (double) totalLag / count : 0.0;

        metrics.put("minOffsetsBehindLatest", String.valueOf(minLag));
        metrics.put("maxOffsetsBehindLatest", String.valueOf(maxLag));
        metrics.put("avgOffsetsBehindLatest", String.format(java.util.Locale.ROOT, "%.1f", avgLag));
        return metrics;
    }

    // ---- Read limit dispatch ----

    private Map<String, Long> applyReadLimit(
            Map<String, Long> startOffsets,
            Map<String, Long> tailOffsets,
            ReadLimit limit) {

        if (limit instanceof ReadAllAvailable) {
            return tailOffsets;
        }

        if (limit instanceof ReadMaxRows maxRows) {
            return ReadLimitBudget.distributeRecordBudget(
                    startOffsets, tailOffsets, maxRows.maxRows());
        }

        Long maxBytes = extractReadMaxBytes(limit);
        if (maxBytes != null) {
            return ReadLimitBudget.distributeByteBudget(
                    startOffsets, tailOffsets, maxBytes, estimatedMessageSize);
        }

        if (limit instanceof CompositeReadLimit composite) {
            Map<String, Long> result = tailOffsets;
            for (ReadLimit component : composite.getReadLimits()) {
                Map<String, Long> componentEnd = applyReadLimit(
                        startOffsets, tailOffsets, component);
                result = ReadLimitBudget.mostRestrictive(result, componentEnd);
            }
            return result;
        }

        // Unknown limit type: treat as ReadAllAvailable
        LOG.debug("Unknown ReadLimit type {}, treating as ReadAllAvailable",
                limit.getClass().getName());
        return tailOffsets;
    }

    // ---- Internal helpers ----

    private synchronized List<String> discoverStreams() {
        if (options.isStreamMode()) {
            if (streams == null) {
                streams = List.of(options.getStream());
            }
            return streams;
        }

        List<String> previous = streams;
        try {
            List<String> discovered = SuperStreamPartitionDiscovery.discoverPartitions(
                    getEnvironment(), options.getSuperStream());
            if (discovered.isEmpty()) {
                if (options.isFailOnDataLoss()) {
                    throw new IllegalStateException(
                            "Superstream '" + options.getSuperStream() +
                                    "' has no partition streams");
                }
                LOG.warn("Superstream '{}' currently has no partition streams; returning empty topology "
                                + "because failOnDataLoss=false",
                        options.getSuperStream());
                streams = List.of();
                return streams;
            }

            if (previous == null) {
                LOG.info("Discovered {} partition streams for superstream '{}'",
                        discovered.size(), options.getSuperStream());
            } else if (!new LinkedHashSet<>(previous).equals(new LinkedHashSet<>(discovered))) {
                LOG.info("Refreshed partition streams for superstream '{}': {} -> {}",
                        options.getSuperStream(), previous.size(), discovered.size());
            }
            streams = discovered;
        } catch (Exception e) {
            if (previous == null) {
                if (options.isFailOnDataLoss()) {
                    throw e;
                }
                LOG.warn("Failed to discover initial topology for superstream '{}'; returning empty topology "
                                + "because failOnDataLoss=false: {}",
                        options.getSuperStream(), e.getMessage());
                streams = List.of();
                return streams;
            }
            LOG.warn("Failed to refresh superstream '{}' partition streams, using cached topology: {}",
                    options.getSuperStream(), e.getMessage());
            streams = previous;
        }
        if (streams.isEmpty() && options.isFailOnDataLoss()) {
            throw new IllegalStateException(
                    "Superstream '" + options.getSuperStream() +
                            "' has no partition streams");
        }
        return streams;
    }

    private Environment getEnvironment() {
        Environment env = environment;
        if (env != null) {
            return env;
        }
        synchronized (this) {
            env = environment;
            if (env == null) {
                env = EnvironmentBuilderHelper.buildEnvironment(options);
                environment = env;
            }
        }
        return env;
    }

    /**
     * Query tail offsets for all streams from the broker.
     *
     * @return map of stream → end offset (exclusive, committedChunkId + 1)
     */
    private Map<String, Long> queryTailOffsets() {
        List<String> streams = discoverStreams();
        Environment env = getEnvironment();
        Map<String, Long> tailOffsets = new LinkedHashMap<>();

        for (String stream : streams) {
            long tail = queryStreamTailOffsetForLatest(env, stream);
            tailOffsets.put(stream, tail);
        }
        return tailOffsets;
    }

    private Map<String, Long> queryTailOffsetsForAvailableNow() {
        List<String> streams = discoverStreams();
        Environment env = getEnvironment();
        Map<String, Long> tailOffsets = new LinkedHashMap<>();
        for (String stream : streams) {
            tailOffsets.put(stream, queryStreamTailOffsetForAvailableNow(env, stream));
        }
        return tailOffsets;
    }

    /**
     * Query the tail offset for a single stream.
     *
     * @return end offset (exclusive) — committedChunkId + 1 or 0 if empty
     */
    private long queryStreamTailOffset(Environment env, String stream) {
        StreamStats stats;
        try {
            stats = env.queryStreamStats(stream);
        } catch (com.rabbitmq.stream.StreamDoesNotExistException e) {
            // Stream deleted (topology change or manual deletion)
            if (options.isFailOnDataLoss()) {
                throw new IllegalStateException(
                        "Stream '" + stream + "' does not exist. " +
                                "It may have been deleted. Set failOnDataLoss=false to skip.", e);
            }
            LOG.warn("Stream '{}' does not exist, skipping (failOnDataLoss=false)", stream);
            return 0;
        } catch (Exception e) {
            if (options.isFailOnDataLoss()) {
                throw new IllegalStateException(
                        "Failed to query stream stats for '" + stream + "'", e);
            }
            LOG.warn("Failed to query stream stats for '{}': {}", stream, e.getMessage());
            return 0;
        }

        return resolveTailOffset(stats);
    }

    private long probeTailOffsetFromLastMessage(Environment env, String stream) {
        BlockingQueue<Long> observedOffsets = new LinkedBlockingQueue<>();
        com.rabbitmq.stream.Consumer probe = null;
        try {
            probe = env.consumerBuilder()
                    .stream(stream)
                    .offset(com.rabbitmq.stream.OffsetSpecification.last())
                    .noTrackingStrategy()
                    .messageHandler((context, message) -> observedOffsets.offer(context.offset()))
                    .build();

            Long first = observedOffsets.poll(250, TimeUnit.MILLISECONDS);
            if (first == null) {
                return 0;
            }

            long maxSeen = first;
            while (true) {
                Long next = observedOffsets.poll(40, TimeUnit.MILLISECONDS);
                if (next == null) {
                    break;
                }
                if (next > maxSeen) {
                    maxSeen = next;
                }
            }
            return maxSeen + 1;
        } catch (NoOffsetException e) {
            return 0;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return 0;
        } catch (Exception e) {
            LOG.debug("Unable to probe last message tail offset for stream '{}': {}",
                    stream, e.getMessage());
            return 0;
        } finally {
            if (probe != null) {
                try {
                    probe.close();
                } catch (Exception e) {
                    LOG.debug("Error closing tail probe consumer for stream '{}'", stream, e);
                }
            }
        }
    }

    private ReadLimit createReadMaxBytesLimit(long maxBytes) {
        try {
            Method maxBytesFactory = ReadLimit.class.getMethod("maxBytes", long.class);
            Object value = maxBytesFactory.invoke(null, maxBytes);
            if (value instanceof ReadLimit readLimit) {
                return readLimit;
            }
        } catch (NoSuchMethodException e) {
            // Spark 3.5 API does not expose ReadLimit.maxBytes(...): callers fall back to maxRows.
        } catch (IllegalAccessException | InvocationTargetException e) {
            LOG.debug("Unable to invoke ReadLimit.maxBytes({}): {}", maxBytes, e.getMessage());
        }
        return null;
    }

    private Long extractReadMaxBytes(ReadLimit limit) {
        if (!"org.apache.spark.sql.connector.read.streaming.ReadMaxBytes"
                .equals(limit.getClass().getName())) {
            return null;
        }
        try {
            Method maxBytes = limit.getClass().getMethod("maxBytes");
            Object value = maxBytes.invoke(limit);
            if (value instanceof Number number) {
                return number.longValue();
            }
        } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
            LOG.debug("Unable to read ReadMaxBytes value from {}: {}",
                    limit.getClass().getName(), e.getMessage());
        }
        return null;
    }

    /**
     * Resolve the starting offset for a single stream based on the configured
     * {@code startingOffsets} mode.
     */
    private long resolveStartingOffset(String stream) {
        return switch (options.getStartingOffsets()) {
            case EARLIEST -> resolveFirstAvailable(stream);
            case LATEST -> queryStreamTailOffsetForLatest(getEnvironment(), stream);
            case OFFSET -> options.getStartingOffset();
            case TIMESTAMP -> resolveTimestampStartingOffset(getEnvironment(), stream);
        };
    }

    /**
     * Resolve a stable planning start offset for timestamp mode by probing the broker with
     * {@link com.rabbitmq.stream.OffsetSpecification#timestamp(long)}.
     *
     * <p>Using firstAvailable as a planning lower bound causes empty early split ranges
     * when the timestamp maps much later in the stream. This method aligns planning with
     * broker timestamp seek behavior.
     */
    private long resolveTimestampStartingOffset(Environment env, String stream) {
        long firstAvailable = resolveFirstAvailable(stream);
        BlockingQueue<Long> observedOffsets = new LinkedBlockingQueue<>();
        com.rabbitmq.stream.Consumer probe = null;
        try {
            probe = env.consumerBuilder()
                    .stream(stream)
                    .offset(com.rabbitmq.stream.OffsetSpecification.timestamp(
                            options.getStartingTimestamp()))
                    .noTrackingStrategy()
                    .messageHandler((context, message) -> observedOffsets.offer(context.offset()))
                    .build();

            Long observed = observedOffsets.poll(250, TimeUnit.MILLISECONDS);
            if (observed != null) {
                return Math.max(firstAvailable, observed);
            }

            // No records at/after timestamp: plan an empty range near the tail.
            long tailExclusive = queryStreamTailOffsetForLatest(env, stream);
            return Math.max(firstAvailable, tailExclusive);
        } catch (NoOffsetException e) {
            return firstAvailable;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return firstAvailable;
        } catch (Exception e) {
            LOG.warn("Failed to resolve timestamp start offset for stream '{}': {}",
                    stream, e.getMessage());
            return firstAvailable;
        } finally {
            if (probe != null) {
                try {
                    probe.close();
                } catch (Exception e) {
                    LOG.debug("Error closing timestamp-start probe consumer for stream '{}'", stream, e);
                }
            }
        }
    }

    private long queryStreamTailOffsetForLatest(Environment env, String stream) {
        long statsTail = queryStreamTailOffset(env, stream);
        long probedTail = probeTailOffsetFromLastMessage(env, stream);
        return Math.max(statsTail, probedTail);
    }

    private long queryStreamTailOffsetForAvailableNow(Environment env, String stream) {
        long statsTail = queryStreamTailOffset(env, stream);
        long probedTail = probeTailOffsetFromLastMessage(env, stream);
        return mergeTailOffsetsForAvailableNow(statsTail, probedTail);
    }

    private long mergeTailOffsetsForAvailableNow(long statsTail, long probedTail) {
        return Math.max(statsTail, probedTail);
    }

    /**
     * Resolve the first available offset for a stream.
     *
     * @return first offset, or 0 if the stream is empty
     */
    private long resolveFirstAvailable(String stream) {
        try {
            StreamStats stats = getEnvironment().queryStreamStats(stream);
            return stats.firstOffset();
        } catch (NoOffsetException e) {
            return 0;
        } catch (com.rabbitmq.stream.StreamDoesNotExistException e) {
            if (options.isFailOnDataLoss()) {
                throw new IllegalStateException(
                        "Stream '" + stream + "' does not exist. Set failOnDataLoss=false to skip.", e);
            }
            LOG.warn("Stream '{}' does not exist, using offset 0 (failOnDataLoss=false)", stream);
            return 0;
        } catch (Exception e) {
            LOG.warn("Failed to query first offset for stream '{}': {}", stream, e.getMessage());
            return 0;
        }
    }

    private boolean useConfiguredStartingOffset(String stream, long startOffset) {
        if (options.getStartingOffsets() != StartingOffsetsMode.TIMESTAMP) {
            return false;
        }
        Map<String, Long> initial = this.initialOffsets;
        if (initial == null) {
            return false;
        }
        Long initialOffset = initial.get(stream);
        return initialOffset != null && initialOffset == startOffset;
    }

    private static String deriveConsumerName(ConnectorOptions options, String checkpointLocation) {
        String configured = options.getConsumerName();
        if (configured != null && !configured.isBlank()) {
            return configured;
        }
        if (checkpointLocation == null || checkpointLocation.isBlank()) {
            return null;
        }
        return "spark-rmq-" + Integer.toUnsignedString(checkpointLocation.hashCode(), 16);
    }

    static long resolveTailOffset(StreamStats stats) {
        // Prefer committedOffset() (RabbitMQ 4.3+) via reflection so this can
        // compile against older stream-client artifacts that don't expose it.
        try {
            Method committedOffset = stats.getClass().getMethod("committedOffset");
            Object value = committedOffset.invoke(stats);
            if (value instanceof Number n) {
                return n.longValue() + 1;
            }
        } catch (NoSuchMethodException e) {
            // Older client artifact; fall back to committedChunkId below.
        } catch (InvocationTargetException e) {
            if (!(e.getTargetException() instanceof NoOffsetException)) {
                LOG.debug("committedOffset() lookup failed, falling back to committedChunkId(): {}",
                        e.getTargetException().toString());
            }
            // no committed offset yet or transient issue: fall through to fallback.
        } catch (IllegalAccessException e) {
            LOG.debug("Unable to access committedOffset() via reflection, falling back: {}",
                    e.toString());
        }

        // Fallback approximation for older broker/client combinations.
        try {
            return stats.committedChunkId() + 1;
        } catch (NoOffsetException e) {
            return 0;
        }
    }

    private void persistBrokerOffsets(Map<String, Long> endOffsets) {
        if (endOffsets == null || endOffsets.isEmpty()) {
            return;
        }
        if (!options.isServerSideOffsetTracking(true)) {
            return;
        }
        String consumerName = effectiveConsumerName;
        if (consumerName == null || consumerName.isEmpty()) {
            return;
        }
        if (lastStoredEndOffsets != null && lastStoredEndOffsets.equals(endOffsets)) {
            return;
        }

        List<Map.Entry<String, Long>> toStore = new ArrayList<>();
        for (Map.Entry<String, Long> entry : endOffsets.entrySet()) {
            if (entry.getValue() > 0) {
                toStore.add(entry);
            }
        }
        if (toStore.isEmpty()) {
            return;
        }

        Environment env = getEnvironment();
        List<Future<?>> futures = new ArrayList<>();
        for (Map.Entry<String, Long> entry : toStore) {
            String stream = entry.getKey();
            if (!options.isFailOnDataLoss() && isMissingStream(env, stream)) {
                LOG.debug("Skipping broker offset store for deleted stream '{}' (failOnDataLoss=false)",
                        stream);
                continue;
            }
            long lastProcessed = entry.getValue() - 1;
            futures.add(brokerCommitExecutor.submit(() -> {
                try {
                    env.storeOffset(consumerName, stream, lastProcessed);
                    LOG.debug("Stored offset {} for consumer '{}' on stream '{}'",
                            lastProcessed, consumerName, stream);
                } catch (Exception e) {
                    LOG.warn("Failed to store offset {} for consumer '{}' on stream '{}': {}",
                            lastProcessed, consumerName, stream, e.getMessage());
                }
            }));
        }

        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(COMMIT_TIMEOUT_SECONDS);
        boolean cancelOutstanding = false;
        for (Future<?> f : futures) {
            long remaining = deadline - System.nanoTime();
            if (remaining <= 0) {
                LOG.warn("Broker offset commit timed out after {}s " +
                                "(best-effort, Spark checkpoint is source of truth)",
                        COMMIT_TIMEOUT_SECONDS);
                cancelOutstanding = true;
                break;
            }
            try {
                f.get(remaining, TimeUnit.NANOSECONDS);
            } catch (TimeoutException e) {
                LOG.warn("Broker offset commit timed out after {}s " +
                                "(best-effort, Spark checkpoint is source of truth)",
                        COMMIT_TIMEOUT_SECONDS);
                cancelOutstanding = true;
                break;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                cancelOutstanding = true;
                break;
            } catch (ExecutionException e) {
                // Already logged in task
            }
        }
        if (cancelOutstanding) {
            for (Future<?> f : futures) {
                if (!f.isDone()) {
                    f.cancel(true);
                }
            }
        }

        lastStoredEndOffsets = new LinkedHashMap<>(endOffsets);
    }

    private boolean isMissingStream(Environment env, String stream) {
        try {
            env.queryStreamStats(stream);
            return false;
        } catch (StreamDoesNotExistException e) {
            return true;
        } catch (Exception e) {
            return false;
        }
    }
}
