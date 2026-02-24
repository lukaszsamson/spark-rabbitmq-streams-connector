package io.github.lukaszsamson.spark.rabbitmq;

import com.rabbitmq.stream.ConsumerFlowStrategy;
import com.rabbitmq.stream.Environment;
import com.rabbitmq.stream.NoOffsetException;
import com.rabbitmq.stream.StreamDoesNotExistException;
import com.rabbitmq.stream.StreamStats;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.connector.read.streaming.*;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.util.LongAccumulator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.ToLongFunction;
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
 *
 * <p>This is the base implementation shared across all Spark versions.
 * Spark-version-specific subclasses (e.g. for {@code SupportsRealTimeMode} in
 * Spark 4.1) extend this class and add only the version-specific methods.
 */
class BaseRabbitMQMicroBatchStream
        implements MicroBatchStream, SupportsAdmissionControl,
                   SupportsTriggerAvailableNow, ReportsSourceMetrics {

    static final Logger LOG = LoggerFactory.getLogger(BaseRabbitMQMicroBatchStream.class);

    /** Timeout for broker offset commit (best-effort, Spark checkpoint is source of truth). */
    static final int COMMIT_TIMEOUT_SECONDS = 30;
    /** Reuse latest-offset tail probe results briefly to avoid per-trigger consumer churn. */
    static final long LATEST_TAIL_PROBE_CACHE_WINDOW_MS = 1_000L;
    /** Reuse full latestOffset() results briefly to keep per-trigger planning stable. */
    static final long LATEST_OFFSET_RESULT_CACHE_WINDOW_MS = 250L;
    static final long TAIL_PROBE_WAIT_MS = 250L;
    static final long TAIL_PROBE_DRAIN_WAIT_MS = 40L;
    static final long TAIL_PROBE_IDLE_GRACE_MS = 200L;
    static final long TAIL_PROBE_MAX_EXTRA_WAIT_MS = 750L;
    static final long MIN_TIMESTAMP_START_PROBE_TIMEOUT_MS = 250L;
    private static final String DERIVED_CONSUMER_NAME_PREFIX = "spark-rmq-";
    private static final int DERIVED_CONSUMER_NAME_HASH_HEX_LENGTH = 16;

    final ConnectorOptions options;
    final StructType schema;
    final String checkpointLocation;
    final String effectiveConsumerName;
    final String messageSizeTrackerScope;
    final ExecutorService brokerCommitExecutor;
    final LongAccumulator messageSizeBytesAccumulator;
    final LongAccumulator messageSizeRecordsAccumulator;

    /** Discovered streams (lazily initialized). */
    volatile List<String> streams;

    /** Driver-side Environment for broker queries (lazily initialized). */
    volatile Environment environment;

    /** Cached latest offset for reportLatestOffset(). */
    volatile RabbitMQStreamOffset cachedLatestOffset;
    /** Cached true tail offset (before ReadLimit application). */
    volatile RabbitMQStreamOffset cachedTailOffset;
    /** Short-lived cache of latestOffset(start, limit) result for repeated trigger calls. */
    volatile LatestOffsetInvocationCache latestOffsetInvocationCache;

    /**
     * Snapshot of tail offsets captured by {@link #prepareForTriggerAvailableNow()}.
     * When non-null, {@link #latestOffset(Offset, ReadLimit)} will not exceed these.
     */
    volatile Map<String, Long> availableNowSnapshot;

    /** Running average message size in bytes, for byte-based admission control. */
    volatile int estimatedMessageSize;
    /** Initial offsets resolved for this stream instance (used for timestamp first batch seek). */
    volatile Map<String, Long> initialOffsets;
    /** Last end offsets successfully persisted to broker (next offsets). */
    volatile Map<String, Long> lastStoredEndOffsets;
    /** Last end offsets committed by Spark (next offsets). */
    volatile Map<String, Long> lastCommittedEndOffsets;
    /** Timestamp (epoch millis) of the last trigger that produced a non-empty batch. */
    volatile long lastTriggerMillis = System.currentTimeMillis();
    /** Recent per-stream tail probe results used by latestOffset planning. */
    final ConcurrentHashMap<String, CachedTailProbe> latestTailProbeCache = new ConcurrentHashMap<>();
    /** Last driver-side accumulator totals already applied to the running estimate. */
    volatile long lastAccumulatedMessageBytes;
    volatile long lastAccumulatedMessageRecords;
    /** Guards compound read-modify-write updates on mutable admission-control state. */
    final Object mutableStateLock = new Object();

    record CachedTailProbe(long tailExclusive, long expiresAtNanos) {}
    record LatestOffsetInvocationCache(
            String startOffsetJson,
            String readLimitKey,
            RabbitMQStreamOffset latestOffset,
            RabbitMQStreamOffset tailOffset,
            long expiresAtNanos) {}

    BaseRabbitMQMicroBatchStream(ConnectorOptions options, StructType schema,
                              String checkpointLocation) {
        this.options = options;
        this.schema = schema;
        this.checkpointLocation = checkpointLocation;
        this.effectiveConsumerName = deriveConsumerName(options, checkpointLocation);
        this.messageSizeTrackerScope = deriveMessageSizeTrackerScope(
                checkpointLocation, this.effectiveConsumerName);
        this.brokerCommitExecutor = Executors.newFixedThreadPool(
                Math.max(1, Math.min(
                        Runtime.getRuntime().availableProcessors(),
                        StoredOffsetLookup.MAX_CONCURRENT_LOOKUPS)));
        LongAccumulator[] accumulators = createMessageSizeAccumulators(this.messageSizeTrackerScope);
        this.messageSizeBytesAccumulator = accumulators[0];
        this.messageSizeRecordsAccumulator = accumulators[1];
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
                    return mergeStoredOffsetsWithStartingOffsets(stored, streams);
                }
                LOG.info("No stored offsets found for consumer '{}', falling back to startingOffsets",
                        consumerName);
            } catch (IllegalStateException e) {
                // Fatal error (auth/connection) — fail fast
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

    private RabbitMQStreamOffset mergeStoredOffsetsWithStartingOffsets(
            Map<String, Long> storedOffsets, List<String> streams) {
        // Fill in any missing streams with starting offset resolution.
        Map<String, Long> merged = new LinkedHashMap<>(storedOffsets);
        for (String stream : streams) {
            merged.putIfAbsent(stream, resolveStartingOffset(stream));
        }
        this.initialOffsets = new LinkedHashMap<>(merged);
        return new RabbitMQStreamOffset(merged);
    }

    @Override
    public Offset deserializeOffset(String json) {
        return RabbitMQStreamOffset.fromJson(json);
    }

    @Override
    public void commit(Offset end) {
        clearLatestOffsetInvocationCache();

        // Update running average message size from completed batch readers.
        // Prefer Spark accumulators so executor JVM metrics are visible to the driver.
        int previousEstimate = currentEstimatedMessageSize();
        int updatedSize = drainAverageFromAccumulators();
        if (updatedSize == previousEstimate) {
            updatedSize = MessageSizeTracker.drainAverage(
                    messageSizeTrackerScope, previousEstimate);
        }
        int effectivePreviousEstimate = setEstimatedMessageSizeAndGetPrevious(updatedSize);
        if (updatedSize != effectivePreviousEstimate) {
            LOG.debug("Updated estimated message size: {} -> {} bytes",
                    effectivePreviousEstimate, updatedSize);
        }

        RabbitMQStreamOffset endOffset = (RabbitMQStreamOffset) end;
        Map<String, Long> committed = new LinkedHashMap<>(endOffset.getStreamOffsets());
        lastCommittedEndOffsets = committed;
        persistBrokerOffsets(committed);
    }

    @Override
    public void stop() {
        clearLatestOffsetInvocationCache();

        // Persist best-effort broker offsets before shutdown.
        // Prefer explicit source commits, then checkpoint-committed offsets.
        Map<String, Long> committed = resolveStopPersistenceOffsets();
        if (committed != null && !committed.isEmpty()) {
            try {
                persistBrokerOffsets(committed);
            } catch (Exception e) {
                LOG.warn("Failed to persist broker offsets during stop()", e);
            }
        }

        brokerCommitExecutor.shutdownNow();
        latestTailProbeCache.clear();
        MessageSizeTracker.clear(messageSizeTrackerScope);
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
        clearLatestOffsetInvocationCache();

        RabbitMQStreamOffset startOffset = (RabbitMQStreamOffset) start;
        RabbitMQStreamOffset endOffset = (RabbitMQStreamOffset) end;

        // Collect validated per-stream ranges
        Map<String, long[]> validRanges = new LinkedHashMap<>();
        Map<String, Long> startOffsets = startOffset.getStreamOffsets();
        for (Map.Entry<String, Long> entry : endOffset.getStreamOffsets().entrySet()) {
            String stream = entry.getKey();
            long endOff = entry.getValue();
            Long knownStart = startOffsets.get(stream);
            if (knownStart == null && !options.isSuperStreamMode()) {
                LOG.warn("Start offset for stream '{}' is missing in start map; skipping split " +
                        "to avoid backfilling from configured startingOffsets", stream);
                continue;
            }
            long startOff = knownStart != null ? knownStart : resolveStartingOffset(stream);

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

        // Apply maxRecordsPerPartition and/or minPartitions splitting if configured
        List<InputPartition> partitions = new ArrayList<>();
        Integer minPartitions = options.getMinPartitions();
        Long maxRecordsPerPartition = options.getMaxRecordsPerPartition();

        if (!validRanges.isEmpty() &&
                (minPartitions != null || maxRecordsPerPartition != null)) {
            // Compute split counts per stream from maxRecordsPerPartition
            Map<String, Integer> splitsPerStream = new LinkedHashMap<>();
            int totalPartitions = 0;
            for (Map.Entry<String, long[]> entry : validRanges.entrySet()) {
                long span = Math.max(0L, entry.getValue()[1] - entry.getValue()[0]);
                int parts = 1;
                if (maxRecordsPerPartition != null && span > maxRecordsPerPartition) {
                    parts = (int) Math.min(Integer.MAX_VALUE,
                            (span + maxRecordsPerPartition - 1) / maxRecordsPerPartition);
                }
                splitsPerStream.put(entry.getKey(), parts);
                totalPartitions += parts;
            }

            // If minPartitions requires more, use the existing even-distribution logic
            if (minPartitions != null && minPartitions > totalPartitions) {
                planWithSplitting(partitions, validRanges, splitsPerStream, minPartitions);
            } else if (totalPartitions > validRanges.size()) {
                // maxRecordsPerPartition caused splitting
                for (Map.Entry<String, long[]> entry : validRanges.entrySet()) {
                    String stream = entry.getKey();
                    long rangeStart = entry.getValue()[0];
                    long rangeEnd = entry.getValue()[1];
                    int numSplits = splitsPerStream.get(stream);
                    splitStreamForMaxRecords(partitions, stream, rangeStart, rangeEnd, numSplits);
                }
            } else {
                // Neither caused additional splitting
                addOnePartitionPerStream(partitions, validRanges);
            }
        } else {
            addOnePartitionPerStream(partitions, validRanges);
        }

        LOG.info("Planned {} input partitions for micro-batch [{} → {}]",
                partitions.size(), start, end);
        return partitions.toArray(new InputPartition[0]);
    }

    /**
     * Split streams into more partitions when minPartitions > stream count.
     * Split counts are distributed deterministically while preserving any
     * existing per-stream minimum split counts (e.g. from maxRecordsPerPartition).
     */
    private void planWithSplitting(List<InputPartition> partitions,
                                    Map<String, long[]> ranges,
                                    Map<String, Integer> minimumSplitsPerStream,
                                    int minPartitions) {
        long totalOffsetSpan = 0;
        for (long[] range : ranges.values()) {
            totalOffsetSpan += Math.max(0L, range[1] - range[0]);
        }
        if (totalOffsetSpan == 0) {
            return;
        }

        // Allocate deterministic split counts per stream.
        Map<String, Integer> splitsPerStream = new LinkedHashMap<>();
        for (String stream : ranges.keySet()) {
            splitsPerStream.put(stream,
                    Math.max(1, minimumSplitsPerStream.getOrDefault(stream, 1)));
        }

        int allocatedPartitions = 0;
        for (int splitCount : splitsPerStream.values()) {
            allocatedPartitions += splitCount;
        }

        if (allocatedPartitions < minPartitions) {
            int extraPartitions = minPartitions - allocatedPartitions;
            int streamCount = ranges.size();
            int baseExtra = extraPartitions / streamCount;
            int remainder = extraPartitions % streamCount;
            int streamIndex = 0;
            for (String stream : ranges.keySet()) {
                int additional = baseExtra + (streamIndex < remainder ? 1 : 0);
                if (additional > 0) {
                    splitsPerStream.put(stream, splitsPerStream.get(stream) + additional);
                }
                streamIndex++;
            }
        }

        // Create split partitions
        for (Map.Entry<String, long[]> entry : ranges.entrySet()) {
            String stream = entry.getKey();
            long start = entry.getValue()[0];
            long end = entry.getValue()[1];
            int numSplits = splitsPerStream.getOrDefault(stream, 1);
            long offsetSpan = end - start;
            String[] location = RabbitMQInputPartition.locationForStream(stream);

            if (numSplits <= 1 || offsetSpan <= 1) {
                partitions.add(new RabbitMQInputPartition(
                        stream, start, end, options,
                        useConfiguredStartingOffset(stream, start), location,
                        messageSizeTrackerScope,
                        messageSizeBytesAccumulator,
                        messageSizeRecordsAccumulator));
                continue;
            }

            long chunkSize = offsetSpan / numSplits;
            long rem = offsetSpan % numSplits;
            long currentStart = start;
            for (int i = 0; i < numSplits; i++) {
                long splitSize = chunkSize + (i < rem ? 1 : 0);
                if (splitSize == 0) break;
                long splitEnd = currentStart + splitSize;
                partitions.add(new RabbitMQInputPartition(
                        stream, currentStart, splitEnd, options,
                        useConfiguredStartingOffset(stream, currentStart), location,
                        messageSizeTrackerScope,
                        messageSizeBytesAccumulator,
                        messageSizeRecordsAccumulator));
                currentStart = splitEnd;
            }
        }

        LOG.info("Applied minPartitions={} splitting: {} partitions from {} streams",
                minPartitions, partitions.size(), ranges.size());
    }

    private void addOnePartitionPerStream(List<InputPartition> partitions,
                                           Map<String, long[]> ranges) {
        for (Map.Entry<String, long[]> entry : ranges.entrySet()) {
            String stream = entry.getKey();
            long rangeStart = entry.getValue()[0];
            partitions.add(new RabbitMQInputPartition(
                    stream, rangeStart, entry.getValue()[1], options,
                    useConfiguredStartingOffset(stream, rangeStart),
                    RabbitMQInputPartition.locationForStream(stream),
                    messageSizeTrackerScope,
                    messageSizeBytesAccumulator,
                    messageSizeRecordsAccumulator));
        }
    }

    private void splitStreamForMaxRecords(List<InputPartition> partitions, String stream,
                                           long start, long end, int numSplits) {
        long offsetSpan = end - start;
        String[] location = RabbitMQInputPartition.locationForStream(stream);
        if (numSplits <= 1 || offsetSpan <= 1) {
            partitions.add(new RabbitMQInputPartition(
                    stream, start, end, options,
                    useConfiguredStartingOffset(stream, start), location,
                    messageSizeTrackerScope,
                    messageSizeBytesAccumulator,
                    messageSizeRecordsAccumulator));
            return;
        }
        long chunkSize = offsetSpan / numSplits;
        long rem = offsetSpan % numSplits;
        long currentStart = start;
        for (int i = 0; i < numSplits; i++) {
            long splitSize = chunkSize + (i < rem ? 1 : 0);
            if (splitSize == 0) break;
            long splitEnd = currentStart + splitSize;
            partitions.add(new RabbitMQInputPartition(
                    stream, currentStart, splitEnd, options,
                    useConfiguredStartingOffset(stream, currentStart), location,
                    messageSizeTrackerScope,
                    messageSizeBytesAccumulator,
                    messageSizeRecordsAccumulator));
            currentStart = splitEnd;
        }
    }

    /**
     * Validate a partition's start offset against the broker's first available offset.
     * Handles retention truncation and stream deletion per failOnDataLoss setting.
     *
     * @return the validated start offset, or -1 if the partition should be skipped
     */
    long validateStartOffset(String stream, long startOff, long endOff) {
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
            // Real-time mode uses an unbounded end offset and must keep empty streams active
            // so data published later on the same stream can still be consumed.
            if (endOff == Long.MAX_VALUE) {
                return startOff;
            }
            // Finite ranges can safely skip empty streams for the current trigger.
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
        Long minRows = options.getMinOffsetsPerTrigger();

        ReadLimit upperLimit = buildUpperLimit(maxRows, maxBytes);

        if (minRows != null) {
            ReadLimit minLimit = ReadLimit.minRows(minRows, options.getMaxTriggerDelayMs());
            if (upperLimit != null) {
                return ReadLimit.compositeLimit(new ReadLimit[]{minLimit, upperLimit});
            }
            return minLimit;
        }
        return upperLimit != null ? upperLimit : ReadLimit.allAvailable();
    }

    // Spark 3.5 compat: uses reflection for ReadMaxBytes (not available in 3.5).
    // Overridden in spark40/spark41 subclasses to use ReadLimit.maxBytes() directly.
    ReadLimit buildUpperLimit(Long maxRows, Long maxBytes) {
        int estimatedSize = currentEstimatedMessageSize();
        if (maxRows != null && maxBytes != null) {
            ReadLimit maxBytesLimit = createReadMaxBytesLimit(maxBytes);
            if (maxBytesLimit != null) {
                return ReadLimit.compositeLimit(new ReadLimit[]{
                        ReadLimit.maxRows(maxRows),
                        maxBytesLimit
                });
            }
            long bytesAsRows = Math.max(1, maxBytes / estimatedSize);
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
            long bytesAsRows = Math.max(1, maxBytes / estimatedSize);
            return ReadLimit.maxRows(bytesAsRows);
        }
        return null;
    }

    @Override
    public Offset latestOffset(Offset startOffset, ReadLimit limit) {
        RabbitMQStreamOffset invocationCached = getCachedLatestOffsetInvocation(startOffset, limit);
        if (invocationCached != null) {
            return invocationCached;
        }

        Map<String, Long> tailOffsets = availableNowSnapshot != null
                ? new LinkedHashMap<>(availableNowSnapshot)
                : queryTailOffsets();

        // Guard: if tailOffsets is empty, no streams could be queried
        if (tailOffsets.isEmpty()) {
            LOG.debug("Tail offsets query returned empty map");
            return startOffset != null ? startOffset : new RabbitMQStreamOffset(Map.of());
        }

        RabbitMQStreamOffset start = startOffset == null
                ? null
                : (RabbitMQStreamOffset) startOffset;
        Map<String, Long> startMap = start == null
                ? Map.of()
                : start.getStreamOffsets();
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
            if (start != null && effectiveStartMap.equals(startMap)) {
                cachedLatestOffset = start;
                cacheLatestOffsetInvocation(startOffset, limit, start, cachedTailOffset);
                return start;
            }
            RabbitMQStreamOffset expanded = new RabbitMQStreamOffset(effectiveStartMap);
            cachedLatestOffset = expanded;
            cacheLatestOffsetInvocation(startOffset, limit, expanded, cachedTailOffset);
            return expanded;
        }

        // Apply read limit budget
        Map<String, Long> endOffsets = applyReadLimit(effectiveStartMap, tailOffsets, limit);

        RabbitMQStreamOffset latest = new RabbitMQStreamOffset(endOffsets);
        cachedLatestOffset = latest;
        cacheLatestOffsetInvocation(startOffset, limit, latest, cachedTailOffset);
        return latest;
    }

    @Override
    public Offset reportLatestOffset() {
        return cachedTailOffset != null ? cachedTailOffset : cachedLatestOffset;
    }

    // ---- SupportsTriggerAvailableNow ----

    @Override
    public void prepareForTriggerAvailableNow() {
        clearLatestOffsetInvocationCache();
        Map<String, Long> snapshot = queryTailOffsetsForAvailableNow();
        this.availableNowSnapshot = snapshot;
        LOG.info("Trigger.AvailableNow: snapshot tail offsets = {}", snapshot);
    }

    private RabbitMQStreamOffset getCachedLatestOffsetInvocation(Offset startOffset, ReadLimit limit) {
        LatestOffsetInvocationCache cached = latestOffsetInvocationCache;
        if (cached == null) {
            return null;
        }
        long nowNanos = System.nanoTime();
        if (nowNanos >= cached.expiresAtNanos()) {
            latestOffsetInvocationCache = null;
            return null;
        }
        if (!Objects.equals(cached.startOffsetJson(), latestOffsetStartCacheKey(startOffset))
                || !Objects.equals(cached.readLimitKey(), readLimitCacheKey(limit))) {
            return null;
        }
        cachedTailOffset = cached.tailOffset();
        cachedLatestOffset = cached.latestOffset();
        return cached.latestOffset();
    }

    private void cacheLatestOffsetInvocation(
            Offset startOffset,
            ReadLimit limit,
            RabbitMQStreamOffset latestOffset,
            RabbitMQStreamOffset tailOffset) {
        latestOffsetInvocationCache = new LatestOffsetInvocationCache(
                latestOffsetStartCacheKey(startOffset),
                readLimitCacheKey(limit),
                latestOffset,
                tailOffset,
                System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(LATEST_OFFSET_RESULT_CACHE_WINDOW_MS));
    }

    private void clearLatestOffsetInvocationCache() {
        latestOffsetInvocationCache = null;
    }

    private String latestOffsetStartCacheKey(Offset startOffset) {
        return startOffset == null ? null : startOffset.json();
    }

    private String readLimitCacheKey(ReadLimit limit) {
        if (limit == null) {
            return "null";
        }
        if (limit instanceof ReadAllAvailable) {
            return "allAvailable";
        }
        if (limit instanceof ReadMaxRows maxRows) {
            return "maxRows:" + maxRows.maxRows();
        }
        if (limit instanceof ReadMinRows minRows) {
            return "minRows:" + minRows.minRows() + ":" + minRows.maxTriggerDelayMs();
        }
        Long maxBytes = extractReadMaxBytes(limit);
        if (maxBytes != null) {
            return "maxBytes:" + maxBytes;
        }
        if (limit instanceof CompositeReadLimit composite) {
            StringBuilder signature = new StringBuilder("composite[");
            ReadLimit[] components = composite.getReadLimits();
            for (int i = 0; i < components.length; i++) {
                if (i > 0) {
                    signature.append(',');
                }
                signature.append(readLimitCacheKey(components[i]));
            }
            return signature.append(']').toString();
        }
        return "unknown:" + limit.getClass().getName();
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

    // Spark 3.5 compat: uses reflection for ReadMaxBytes (not available in 3.5).
    // Overridden in spark40/spark41 subclasses to use instanceof ReadMaxBytes directly.
    Map<String, Long> applyReadLimit(
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

        if (limit instanceof ReadMinRows minRows) {
            return handleReadMinRowsCore(
                    minRows.minRows(), minRows.maxTriggerDelayMs(),
                    startOffsets, tailOffsets);
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

    /**
     * Check whether enough new data is available or the max trigger delay has expired.
     * If neither, return startOffsets (skip this trigger). Otherwise return tail.
     */
    final Map<String, Long> handleReadMinRowsCore(long minRows, long maxDelayMs,
                                                    Map<String, Long> startOffsets,
                                                    Map<String, Long> tailOffsets) {
        // Calculate total available data
        long totalAvailable = 0;
        for (Map.Entry<String, Long> entry : tailOffsets.entrySet()) {
            long startOff = startOffsets.getOrDefault(entry.getKey(), 0L);
            totalAvailable += Math.max(0, entry.getValue() - startOff);
        }

        long nowMillis = System.currentTimeMillis();
        boolean processBatch;
        boolean dueToDelayExpiry;
        synchronized (mutableStateLock) {
            if (totalAvailable >= minRows) {
                lastTriggerMillis = nowMillis;
                processBatch = true;
                dueToDelayExpiry = false;
            } else if ((nowMillis - lastTriggerMillis) >= maxDelayMs) {
                lastTriggerMillis = nowMillis;
                processBatch = true;
                dueToDelayExpiry = true;
            } else {
                processBatch = false;
                dueToDelayExpiry = false;
            }
        }

        // Enough data available — proceed with the batch.
        if (processBatch && !dueToDelayExpiry) {
            return tailOffsets;
        }

        // Max delay expired — proceed even with insufficient data.
        if (processBatch) {
            LOG.debug("Max trigger delay of {}ms expired, processing batch with {} records",
                    maxDelayMs, totalAvailable);
            return tailOffsets;
        }

        // Not enough data and delay not expired — skip this trigger
        LOG.debug("Delaying batch: {} records available < minOffsetsPerTrigger={}, " +
                "delay not expired", totalAvailable, minRows);
        return startOffsets;
    }

    // Spark 3.5 compat: ReadLimit.maxBytes() does not exist in 3.5. Not called on 4.x paths.
    ReadLimit createReadMaxBytesLimit(long maxBytes) {
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

    // Spark 3.5 compat: ReadMaxBytes class does not exist in 3.5. Not called on 4.x paths.
    Long extractReadMaxBytes(ReadLimit limit) {
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

    // ---- Internal helpers ----

    synchronized List<String> discoverStreams() {
        if (options.isStreamMode()) {
            if (streams == null) {
                streams = List.of(options.getStream());
            }
            return streams;
        }

        List<String> previous = streams;
        try {
            List<String> discovered = SuperStreamPartitionDiscovery.discoverPartitions(
                    options, options.getSuperStream());
            if (discovered.isEmpty()) {
                if (options.isFailOnDataLoss()) {
                    throw new IllegalStateException(
                            "Superstream '" + options.getSuperStream() +
                                    "' has no partition streams");
                }
                if (previous != null && !previous.isEmpty()) {
                    LOG.warn("Superstream '{}' discovery returned no partition streams; "
                                    + "preserving cached topology ({} streams) because failOnDataLoss=false",
                            options.getSuperStream(), previous.size());
                    streams = previous;
                    return streams;
                }
                LOG.warn("Superstream '{}' currently has no partition streams; returning empty topology "
                                + "because failOnDataLoss=false",
                        options.getSuperStream());
                streams = List.of();
                return streams;
            }

            if (previous != null && !options.isFailOnDataLoss()) {
                LinkedHashSet<String> merged = new LinkedHashSet<>(previous);
                merged.addAll(discovered);
                if (merged.size() != discovered.size()) {
                    LOG.warn("Superstream '{}' discovery dropped {} cached partition streams; "
                                    + "preserving cached topology because failOnDataLoss=false",
                            options.getSuperStream(), merged.size() - discovered.size());
                    discovered = new ArrayList<>(merged);
                }
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

    Environment getEnvironment() {
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
        if (!latestTailProbeCache.isEmpty()) {
            Set<String> activeStreams = new HashSet<>(streams);
            latestTailProbeCache.keySet().removeIf(stream -> !activeStreams.contains(stream));
        }
        return queryTailOffsetsInParallel(
                streams,
                stream -> queryStreamTailOffsetForLatest(env, stream));
    }

    private Map<String, Long> queryTailOffsetsForAvailableNow() {
        List<String> streams = discoverStreams();
        Environment env = getEnvironment();
        return queryTailOffsetsInParallel(
                streams,
                stream -> queryStreamTailOffsetForAvailableNow(env, stream));
    }

    private Map<String, Long> queryTailOffsetsInParallel(
            List<String> streams, ToLongFunction<String> tailResolver) {
        Map<String, Long> tailOffsets = new LinkedHashMap<>();
        if (streams.isEmpty()) {
            return tailOffsets;
        }
        if (streams.size() == 1) {
            String stream = streams.get(0);
            tailOffsets.put(stream, tailResolver.applyAsLong(stream));
            return tailOffsets;
        }

        Map<String, CompletableFuture<Long>> futures = new LinkedHashMap<>();
        for (String stream : streams) {
            futures.put(stream, CompletableFuture.supplyAsync(
                    () -> tailResolver.applyAsLong(stream), brokerCommitExecutor));
        }

        try {
            for (String stream : streams) {
                tailOffsets.put(stream, futures.get(stream).join());
            }
            return tailOffsets;
        } catch (CompletionException e) {
            futures.values().forEach(future -> future.cancel(true));
            throw unwrapCompletionException(e);
        } catch (CancellationException e) {
            throw new IllegalStateException("Tail offset query was cancelled", e);
        }
    }

    private RuntimeException unwrapCompletionException(Throwable throwable) {
        Throwable cause = throwable;
        while ((cause instanceof CompletionException || cause instanceof ExecutionException)
                && cause.getCause() != null) {
            cause = cause.getCause();
        }
        if (cause instanceof RuntimeException runtimeException) {
            return runtimeException;
        }
        if (cause instanceof Error error) {
            throw error;
        }
        return new IllegalStateException("Tail offset query failed", cause);
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

    long probeTailOffsetFromLastMessage(Environment env, String stream) {
        try {
            long maxSeen = probeLastMessageOffsetInclusive(env, stream, TAIL_PROBE_WAIT_MS);
            if (maxSeen < 0) {
                return 0;
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
        }
    }

    static long probeLastMessageOffsetInclusive(
            Environment env, String stream, long firstMessageWaitMs)
            throws InterruptedException {
        ArrayBlockingQueue<Long> observedOffsets = new ArrayBlockingQueue<>(1);
        AtomicLong maxObservedOffset = new AtomicLong(-1L);
        com.rabbitmq.stream.Consumer probe = null;
        try {
            probe = env.consumerBuilder()
                    .stream(stream)
                    .offset(com.rabbitmq.stream.OffsetSpecification.last())
                    .noTrackingStrategy()
                    .messageHandler((context, message) -> {
                        long offset = context.offset();
                        maxObservedOffset.accumulateAndGet(offset, Math::max);
                        observedOffsets.offer(offset);
                    })
                    .flow()
                    .initialCredits(1)
                    // Probe callback never calls context.processed(), so use a strategy that
                    // does not depend on processed() to keep chunk delivery moving.
                    .strategy(ConsumerFlowStrategy.creditOnChunkArrival(1))
                    .builder()
                    .build();

            long firstWaitMs = Math.max(1L, firstMessageWaitMs);
            Long first = observedOffsets.poll(firstWaitMs, TimeUnit.MILLISECONDS);
            if (first == null) {
                return -1L;
            }

            long maxSeen = Math.max(first, maxObservedOffset.get());
            long extraWaitMs = Math.min(
                    TAIL_PROBE_MAX_EXTRA_WAIT_MS,
                    Math.max(TAIL_PROBE_DRAIN_WAIT_MS, firstWaitMs * 3));
            long idleGraceMs = Math.min(
                    extraWaitMs,
                    Math.max(TAIL_PROBE_DRAIN_WAIT_MS, TAIL_PROBE_IDLE_GRACE_MS));
            long deadlineNanos = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(extraWaitMs);
            long lastAdvanceNanos = System.nanoTime();
            while (true) {
                long remainingNanos = deadlineNanos - System.nanoTime();
                if (remainingNanos <= 0) {
                    break;
                }
                long remainingMs = Math.max(1L, TimeUnit.NANOSECONDS.toMillis(remainingNanos));
                long pollMs = Math.max(1L, Math.min(TAIL_PROBE_DRAIN_WAIT_MS, remainingMs));
                Long next = observedOffsets.poll(pollMs, TimeUnit.MILLISECONDS);
                long observed = maxObservedOffset.get();
                boolean advanced = false;
                if (next != null && next > maxSeen) {
                    maxSeen = next;
                    advanced = true;
                }
                if (observed > maxSeen) {
                    maxSeen = observed;
                    advanced = true;
                }
                if (advanced) {
                    lastAdvanceNanos = System.nanoTime();
                    continue;
                }
                long idleMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - lastAdvanceNanos);
                if (idleMs >= idleGraceMs) {
                    break;
                }
            }
            return maxSeen;
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

    /**
     * Resolve the starting offset for a single stream based on the configured
     * {@code startingOffsets} mode.
     */
    long resolveStartingOffset(String stream) {
        // Check for per-stream timestamp override
        Map<String, Long> perStreamTs = options.getStartingOffsetsByTimestamp();
        if (perStreamTs != null && perStreamTs.containsKey(stream)) {
            return resolveTimestampStartingOffset(getEnvironment(), stream, perStreamTs.get(stream));
        }
        return switch (options.getStartingOffsets()) {
            case EARLIEST -> resolveFirstAvailable(stream);
            case LATEST -> queryStreamTailOffsetForLatest(getEnvironment(), stream);
            case OFFSET -> options.getStartingOffset();
            case TIMESTAMP -> resolveTimestampStartingOffset(
                    getEnvironment(), stream, options.getStartingTimestamp());
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
    private long resolveTimestampStartingOffset(Environment env, String stream, long timestamp) {
        long firstAvailable = resolveFirstAvailable(stream);
        BlockingQueue<Long> observedOffsets = new LinkedBlockingQueue<>();
        com.rabbitmq.stream.Consumer probe = null;
        try {
            probe = env.consumerBuilder()
                    .stream(stream)
                    .offset(com.rabbitmq.stream.OffsetSpecification.timestamp(timestamp))
                    .noTrackingStrategy()
                    .messageHandler((context, message) -> observedOffsets.offer(context.offset()))
                    .flow()
                    .initialCredits(1)
                    .strategy(ConsumerFlowStrategy.creditWhenHalfMessagesProcessed(1))
                    .builder()
                    .build();

            Long observed = observedOffsets.poll(timestampStartProbeTimeoutMs(), TimeUnit.MILLISECONDS);
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
            throw new IllegalStateException(
                    "Interrupted resolving timestamp start offset for stream '" + stream + "'", e);
        } catch (IllegalStateException e) {
            throw e;
        } catch (Exception e) {
            LOG.warn("Failed to resolve timestamp start offset for stream '{}': {}",
                    stream, e.getMessage());
            throw new IllegalStateException(
                    "Failed to resolve timestamp start offset for stream '" + stream + "'", e);
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

    private long timestampStartProbeTimeoutMs() {
        return Math.max(MIN_TIMESTAMP_START_PROBE_TIMEOUT_MS, options.getPollTimeoutMs());
    }

    private long queryStreamTailOffsetForLatest(Environment env, String stream) {
        long statsTail = queryStreamTailOffset(env, stream);
        long probedTail = probeTailOffsetForLatestWithCache(env, stream);
        return Math.max(statsTail, probedTail);
    }

    private long probeTailOffsetForLatestWithCache(Environment env, String stream) {
        long nowNanos = System.nanoTime();
        CachedTailProbe cached = latestTailProbeCache.get(stream);
        if (cached != null && nowNanos < cached.expiresAtNanos()) {
            return cached.tailExclusive();
        }
        long probedTail = probeTailOffsetFromLastMessage(env, stream);
        latestTailProbeCache.put(
                stream,
                new CachedTailProbe(
                        probedTail,
                        nowNanos + TimeUnit.MILLISECONDS.toNanos(LATEST_TAIL_PROBE_CACHE_WINDOW_MS)));
        return probedTail;
    }

    private long queryStreamTailOffsetForAvailableNow(Environment env, String stream) {
        long statsTail = queryStreamTailOffset(env, stream);
        long probedTail = probeTailOffsetForLatestWithCache(env, stream);
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
        } catch (IllegalStateException e) {
            throw e;
        } catch (Exception e) {
            throw new IllegalStateException(
                    "Failed to query first offset for stream '" + stream + "'", e);
        }
    }

    boolean useConfiguredStartingOffset(String stream, long startOffset) {
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

    static String deriveConsumerName(ConnectorOptions options, String checkpointLocation) {
        String configured = options.getConsumerName();
        if (configured != null && !configured.isBlank()) {
            return configured;
        }
        if (checkpointLocation == null || checkpointLocation.isBlank()) {
            return null;
        }
        String legacyHash = Integer.toUnsignedString(checkpointLocation.hashCode(), 16);
        String strongHash = sha256Hex(checkpointLocation);
        return DERIVED_CONSUMER_NAME_PREFIX
                + legacyHash
                + "-"
                + strongHash.substring(0, DERIVED_CONSUMER_NAME_HASH_HEX_LENGTH);
    }

    static String deriveLegacyConsumerName(String checkpointLocation) {
        if (checkpointLocation == null || checkpointLocation.isBlank()) {
            return null;
        }
        return DERIVED_CONSUMER_NAME_PREFIX
                + Integer.toUnsignedString(checkpointLocation.hashCode(), 16);
    }

    private static String sha256Hex(String value) {
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256")
                    .digest(value.getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(digest);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is unavailable", e);
        }
    }

    private static LongAccumulator[] createMessageSizeAccumulators(String scope) {
        try {
            Option<SparkSession> activeSession = SparkSession.getActiveSession();
            if (activeSession == null || activeSession.isEmpty()) {
                return new LongAccumulator[]{null, null};
            }
            SparkContext sparkContext = activeSession.get().sparkContext();
            if (sparkContext == null) {
                return new LongAccumulator[]{null, null};
            }
            String suffix = (scope == null || scope.isBlank())
                    ? "default"
                    : Integer.toUnsignedString(scope.hashCode(), 16);
            LongAccumulator bytes = sparkContext.longAccumulator(
                    "sparkling-rabbit-message-bytes-" + suffix);
            LongAccumulator records = sparkContext.longAccumulator(
                    "sparkling-rabbit-message-records-" + suffix);
            return new LongAccumulator[]{bytes, records};
        } catch (RuntimeException e) {
            LOG.debug("Unable to initialize Spark accumulators for message-size tracking: {}",
                    e.toString());
            return new LongAccumulator[]{null, null};
        }
    }

    private int drainAverageFromAccumulators() {
        if (messageSizeBytesAccumulator == null || messageSizeRecordsAccumulator == null) {
            return currentEstimatedMessageSize();
        }
        long totalBytes = messageSizeBytesAccumulator.value();
        long totalRecords = messageSizeRecordsAccumulator.value();
        synchronized (mutableStateLock) {
            int currentEstimate = estimatedMessageSize;
            long deltaBytes = totalBytes - lastAccumulatedMessageBytes;
            long deltaRecords = totalRecords - lastAccumulatedMessageRecords;
            if (deltaBytes < 0 || deltaRecords <= 0) {
                return currentEstimate;
            }
            lastAccumulatedMessageBytes = totalBytes;
            lastAccumulatedMessageRecords = totalRecords;
            return Math.max(1, (int) (deltaBytes / deltaRecords));
        }
    }

    private int currentEstimatedMessageSize() {
        synchronized (mutableStateLock) {
            return estimatedMessageSize;
        }
    }

    private int setEstimatedMessageSizeAndGetPrevious(int newEstimate) {
        synchronized (mutableStateLock) {
            int previous = estimatedMessageSize;
            estimatedMessageSize = newEstimate;
            return previous;
        }
    }

    private static String deriveMessageSizeTrackerScope(
            String checkpointLocation, String effectiveConsumerName) {
        if (checkpointLocation != null && !checkpointLocation.isBlank()) {
            return "cp-" + Integer.toUnsignedString(checkpointLocation.hashCode(), 16);
        }
        if (effectiveConsumerName != null && !effectiveConsumerName.isBlank()) {
            return "consumer-" + effectiveConsumerName;
        }
        return null;
    }

    static long resolveTailOffset(StreamStats stats) {
        try {
            return stats.committedOffset() + 1;
        } catch (NoOffsetException e) {
            // Broker does not have a committed offset yet, fall back below.
        } catch (RuntimeException e) {
            LOG.debug("committedOffset() failed, falling back to committedChunkId(): {}",
                    e.toString());
        }

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
        AtomicBoolean storeFailureObserved = new AtomicBoolean(false);
        for (Map.Entry<String, Long> entry : toStore) {
            String stream = entry.getKey();
            long lastProcessed = entry.getValue() - 1;
            futures.add(brokerCommitExecutor.submit(() -> {
                try {
                    env.storeOffset(consumerName, stream, lastProcessed);
                    LOG.debug("Stored offset {} for consumer '{}' on stream '{}'",
                            lastProcessed, consumerName, stream);
                } catch (Exception e) {
                    if (!options.isFailOnDataLoss() && isMissingStreamException(e)) {
                        LOG.debug("Skipping broker offset store for deleted stream '{}' " +
                                        "(failOnDataLoss=false)",
                                stream);
                        return;
                    }
                    storeFailureObserved.set(true);
                    LOG.warn("Failed to store offset {} for consumer '{}' on stream '{}': {}",
                            lastProcessed, consumerName, stream, e.getMessage());
                }
            }));
        }

        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(COMMIT_TIMEOUT_SECONDS);
        boolean cancelOutstanding = false;
        boolean commitObservationComplete = true;
        for (Future<?> f : futures) {
            long remaining = deadline - System.nanoTime();
            if (remaining <= 0) {
                LOG.warn("Broker offset commit timed out after {}s " +
                                "(best-effort, Spark checkpoint is source of truth)",
                        COMMIT_TIMEOUT_SECONDS);
                cancelOutstanding = true;
                commitObservationComplete = false;
                break;
            }
            try {
                f.get(remaining, TimeUnit.NANOSECONDS);
            } catch (TimeoutException e) {
                LOG.warn("Broker offset commit timed out after {}s " +
                                "(best-effort, Spark checkpoint is source of truth)",
                        COMMIT_TIMEOUT_SECONDS);
                cancelOutstanding = true;
                commitObservationComplete = false;
                break;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                cancelOutstanding = true;
                commitObservationComplete = false;
                break;
            } catch (ExecutionException e) {
                // Already logged in task
                commitObservationComplete = false;
            }
        }
        if (cancelOutstanding) {
            for (Future<?> f : futures) {
                if (!f.isDone()) {
                    f.cancel(true);
                }
            }
        }
        if (storeFailureObserved.get()) {
            commitObservationComplete = false;
        }

        if (commitObservationComplete) {
            lastStoredEndOffsets = new LinkedHashMap<>(endOffsets);
        }
    }

    private Map<String, Long> resolveStopPersistenceOffsets() {
        Map<String, Long> committed = lastCommittedEndOffsets;
        if (committed != null && !committed.isEmpty()) {
            return committed;
        }

        Map<String, Long> fromCheckpoint = loadCommittedOffsetsFromCheckpoint();
        if (fromCheckpoint != null && !fromCheckpoint.isEmpty()) {
            return fromCheckpoint;
        }

        if (shouldPersistCachedLatestOffsetOnStop()) {
            RabbitMQStreamOffset latest = cachedLatestOffset;
            if (latest != null && !latest.getStreamOffsets().isEmpty()) {
                return latest.getStreamOffsets();
            }
        }
        return null;
    }

    private Map<String, Long> loadCommittedOffsetsFromCheckpoint() {
        Path checkpointPath = toLocalCheckpointPath();
        if (checkpointPath == null) {
            return null;
        }
        Path commitsDir = checkpointPath.resolve("commits");
        Path offsetsDir = checkpointPath.resolve("offsets");
        if (!Files.isDirectory(commitsDir) || !Files.isDirectory(offsetsDir)) {
            return null;
        }

        try {
            long latestCommittedBatch = latestBatchId(commitsDir);
            if (latestCommittedBatch < 0) {
                return null;
            }
            Path offsetFile = offsetsDir.resolve(Long.toString(latestCommittedBatch));
            if (!Files.isRegularFile(offsetFile)) {
                return null;
            }

            List<String> lines = Files.readAllLines(offsetFile, StandardCharsets.UTF_8);
            if (lines.size() < 3) {
                return null;
            }

            Set<String> knownStreams = new LinkedHashSet<>();
            List<String> discovered = streams;
            if (discovered != null) {
                knownStreams.addAll(discovered);
            }

            for (int i = 2; i < lines.size(); i++) {
                String raw = lines.get(i).trim();
                if (raw.isEmpty() || "-".equals(raw)) {
                    continue;
                }
                try {
                    Map<String, Long> parsed = RabbitMQStreamOffset.fromJson(raw).getStreamOffsets();
                    if (parsed.isEmpty()) {
                        continue;
                    }
                    if (!knownStreams.isEmpty() && Collections.disjoint(knownStreams, parsed.keySet())) {
                        continue;
                    }
                    return new LinkedHashMap<>(parsed);
                } catch (RuntimeException ignored) {
                    // Not this source's offset line.
                }
            }
        } catch (Exception e) {
            LOG.debug("Unable to load committed offsets from checkpoint '{}': {}",
                    checkpointLocation, e.toString());
        }
        return null;
    }

    private Path toLocalCheckpointPath() {
        if (checkpointLocation == null || checkpointLocation.isBlank()) {
            return null;
        }
        try {
            URI uri = URI.create(checkpointLocation);
            if (uri.getScheme() != null) {
                if (!"file".equalsIgnoreCase(uri.getScheme())) {
                    return null;
                }
                return Paths.get(uri);
            }
        } catch (Exception ignored) {
            // Fall through to plain path parsing below.
        }
        try {
            return Paths.get(checkpointLocation);
        } catch (Exception e) {
            return null;
        }
    }

    private long latestBatchId(Path logDir) throws java.io.IOException {
        long latest = -1L;
        try (DirectoryStream<Path> files = Files.newDirectoryStream(logDir)) {
            for (Path file : files) {
                long id = parseBatchId(file.getFileName().toString());
                if (id > latest) {
                    latest = id;
                }
            }
        }
        return latest;
    }

    private long parseBatchId(String fileName) {
        if (fileName == null || fileName.isEmpty()) {
            return -1L;
        }
        int dotIdx = fileName.indexOf('.');
        String base = dotIdx >= 0 ? fileName.substring(0, dotIdx) : fileName;
        if (base.isEmpty()) {
            return -1L;
        }
        for (int i = 0; i < base.length(); i++) {
            if (!Character.isDigit(base.charAt(i))) {
                return -1L;
            }
        }
        try {
            return Long.parseLong(base);
        } catch (NumberFormatException e) {
            return -1L;
        }
    }

    boolean shouldPersistCachedLatestOffsetOnStop() {
        return availableNowSnapshot != null;
    }

    private boolean isMissingStreamException(Throwable throwable) {
        Throwable current = throwable;
        while (current != null) {
            if (current instanceof StreamDoesNotExistException) {
                return true;
            }
            String message = current.getMessage();
            if (message != null && (message.contains("STREAM_DOES_NOT_EXIST")
                    || message.contains("does not exist")
                    || message.contains("has no partition streams"))) {
                return true;
            }
            current = current.getCause();
        }
        return false;
    }
}
