package io.github.lukaszsamson.spark.rabbitmq;

import com.rabbitmq.stream.Consumer;
import com.rabbitmq.stream.Environment;
import com.rabbitmq.stream.NoOffsetException;
import com.rabbitmq.stream.OffsetSpecification;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Looks up stored consumer offsets from the RabbitMQ broker.
 *
 * <p>Uses temporary named consumers with {@code manualTrackingStrategy()} to query
 * the broker for the last stored offset per stream. The RabbitMQ stream client
 * requires tracking-enabled consumers for {@code storedOffset()}.
 *
 * <p>Stored offsets in RabbitMQ are <em>last-processed</em> offsets.
 * The returned map contains <em>next</em> offsets ({@code stored + 1}).
 *
 * <p>Concurrent lookups are bounded by a semaphore to avoid exceeding the
 * broker's tracking consumer limit (~50 per connection). For superstreams with
 * many partitions, lookups are serialized in batches.
 */
final class StoredOffsetLookup {

    private static final Logger LOG = LoggerFactory.getLogger(StoredOffsetLookup.class);

    /** Maximum concurrent tracking consumers per lookup batch. */
    static final int MAX_CONCURRENT_LOOKUPS = 20;
    static final long LOOKUP_FUTURE_TIMEOUT_MS = 30_000L;
    private static final AtomicInteger LOOKUP_THREAD_COUNTER = new AtomicInteger(0);
    private static final ExecutorService SHARED_LOOKUP_EXECUTOR = createSharedLookupExecutor();

    private StoredOffsetLookup() {}

    private static ExecutorService createSharedLookupExecutor() {
        ExecutorService executor = Executors.newFixedThreadPool(
                MAX_CONCURRENT_LOOKUPS,
                runnable -> {
                    Thread thread = new Thread(
                            runnable,
                            "rabbitmq-stored-offset-lookup-" + LOOKUP_THREAD_COUNTER.incrementAndGet());
                    thread.setDaemon(true);
                    return thread;
                });
        Runtime.getRuntime().addShutdownHook(new Thread(
                executor::shutdownNow, "rabbitmq-stored-offset-lookup-shutdown"));
        return executor;
    }

    /**
     * Result of a stored offset lookup, distinguishing between successful
     * lookups and non-fatal failures.
     */
    static final class LookupResult {
        private final Map<String, Long> offsets;
        private final List<String> failedStreams;
        private final boolean interruptedOrTimedOut;

        LookupResult(
                Map<String, Long> offsets,
                List<String> failedStreams,
                boolean interruptedOrTimedOut) {
            this.offsets = offsets;
            this.failedStreams = failedStreams;
            this.interruptedOrTimedOut = interruptedOrTimedOut;
        }

        Map<String, Long> getOffsets() { return offsets; }
        List<String> getFailedStreams() { return failedStreams; }
        boolean hasFailures() { return !failedStreams.isEmpty(); }
        boolean wasInterruptedOrTimedOut() { return interruptedOrTimedOut; }
    }

    /**
     * Look up stored offsets for the given streams with bounded concurrency.
     *
     * @param env          an active Environment
     * @param consumerName the consumer reference name
     * @param streams      streams to query
     * @return map of stream to next offset for streams that have a stored offset;
     *         streams with no stored offset are omitted
     * @throws IllegalStateException if a fatal error occurs (auth, connection)
     */
    static Map<String, Long> lookup(Environment env, String consumerName,
                                     List<String> streams) {
        LookupResult result = lookupWithDetails(env, consumerName, streams);
        return result.getOffsets();
    }

    /**
     * Look up stored offsets with detailed results including failed streams.
     *
     * @param env          an active Environment
     * @param consumerName the consumer reference name
     * @param streams      streams to query
     * @return result containing offsets and any non-fatal failures
     * @throws IllegalStateException if a fatal error occurs (auth, connection)
     */
    static LookupResult lookupWithDetails(Environment env, String consumerName,
                                            List<String> streams) {
        return lookupWithDetails(
                env,
                consumerName,
                streams,
                LOOKUP_FUTURE_TIMEOUT_MS,
                SHARED_LOOKUP_EXECUTOR);
    }

    static LookupResult lookupWithDetails(Environment env, String consumerName,
                                            List<String> streams, long futureTimeoutMs) {
        return lookupWithDetails(
                env,
                consumerName,
                streams,
                futureTimeoutMs,
                SHARED_LOOKUP_EXECUTOR);
    }

    static LookupResult lookupWithDetails(Environment env, String consumerName,
                                          List<String> streams, ExecutorService executor) {
        return lookupWithDetails(
                env,
                consumerName,
                streams,
                LOOKUP_FUTURE_TIMEOUT_MS,
                executor);
    }

    static LookupResult lookupWithDetails(Environment env, String consumerName,
                                            List<String> streams, long futureTimeoutMs,
                                            ExecutorService executor) {
        Map<String, Long> offsets = new LinkedHashMap<>();
        List<String> failed = new java.util.ArrayList<>();
        boolean interruptedOrTimedOut = false;

        if (streams.isEmpty()) {
            return new LookupResult(offsets, failed, false);
        }

        long effectiveTimeoutMs = Math.max(1L, futureTimeoutMs);
        long deadlineNanos = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(effectiveTimeoutMs);
        ExecutorService effectiveExecutor =
                executor != null ? executor : SHARED_LOOKUP_EXECUTOR;
        Map<String, Future<Long>> futures = new LinkedHashMap<>();
        try {
            // Submit all lookups concurrently
            for (String stream : streams) {
                futures.put(stream, effectiveExecutor.submit(
                        () -> lookupStream(env, consumerName, stream)));
            }

            // Collect results, preserving insertion order
            for (Map.Entry<String, Future<Long>> entry : futures.entrySet()) {
                if (Thread.currentThread().isInterrupted()) {
                    Thread.currentThread().interrupt();
                    markRemainingStreamsFailed(streams, failed, offsets);
                    interruptedOrTimedOut = true;
                    LOG.warn("Interrupted during stored offset lookup for consumer '{}'; "
                                    + "falling back for unresolved streams: {}",
                            consumerName, failed);
                    break;
                }
                String stream = entry.getKey();
                try {
                    long remainingNanos = deadlineNanos - System.nanoTime();
                    if (remainingNanos <= 0) {
                        throw new TimeoutException("global lookup deadline exceeded");
                    }
                    Long nextOffset = entry.getValue().get(remainingNanos, TimeUnit.NANOSECONDS);
                    if (nextOffset != null) {
                        offsets.put(stream, nextOffset);
                    }
                } catch (ExecutionException e) {
                    Throwable cause = e.getCause();
                    if (cause instanceof IllegalStateException) {
                        // Fatal error — propagate immediately
                        throw (IllegalStateException) cause;
                    }
                    if (cause instanceof NonFatalLookupException) {
                        LOG.warn("Non-fatal lookup failure for consumer '{}' on stream '{}': {}",
                                consumerName, stream, cause.getMessage());
                        failed.add(stream);
                    } else {
                        LOG.warn("Unexpected lookup failure for consumer '{}' on stream '{}': {}",
                                consumerName, stream,
                                cause != null ? cause.getMessage() : e.getMessage());
                        failed.add(stream);
                    }
                } catch (TimeoutException e) {
                    entry.getValue().cancel(true);
                    if (!failed.contains(stream) && !offsets.containsKey(stream)) {
                        failed.add(stream);
                    }
                    markRemainingStreamsFailed(streams, failed, offsets);
                    interruptedOrTimedOut = true;
                    LOG.warn("Timed out waiting for stored offset lookup for consumer '{}' "
                                    + "after {}ms; falling back for unresolved streams: {}",
                            consumerName, effectiveTimeoutMs, failed, e);
                    break;
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    markRemainingStreamsFailed(streams, failed, offsets);
                    interruptedOrTimedOut = true;
                    LOG.warn("Interrupted during stored offset lookup for consumer '{}'; "
                                    + "falling back for unresolved streams: {}",
                            consumerName, failed, e);
                    break;
                }
            }
        } finally {
            for (Future<Long> future : futures.values()) {
                future.cancel(true);
            }
        }
        return new LookupResult(offsets, failed, interruptedOrTimedOut);
    }

    private static void markRemainingStreamsFailed(
            List<String> streams, List<String> failed, Map<String, Long> offsets) {
        for (String stream : streams) {
            if (!offsets.containsKey(stream) && !failed.contains(stream)) {
                failed.add(stream);
            }
        }
    }

    /**
     * Look up the stored offset for a single stream.
     *
     * @return the next offset (stored + 1), or {@code null} if no offset is stored
     * @throws NonFatalLookupException for non-fatal lookup failures (e.g., consumer limit)
     * @throws IllegalStateException for fatal errors (auth, connection)
     */
    private static Long lookupStream(Environment env, String consumerName, String stream) {
        Consumer tempConsumer = null;
        try {
            tempConsumer = env.consumerBuilder()
                    .stream(stream)
                    .name(consumerName)
                    .manualTrackingStrategy()
                    .builder()
                    .offset(OffsetSpecification.next())
                    .messageHandler((ctx, msg) -> {
                        // No-op: we only need storedOffset(), not message consumption
                    })
                    .build();

            long stored = tempConsumer.storedOffset();
            LOG.debug("Found stored offset {} for consumer '{}' on stream '{}'",
                    stored, consumerName, stream);
            return stored + 1; // Convert last-processed to next offset
        } catch (NoOffsetException e) {
            LOG.debug("No stored offset for consumer '{}' on stream '{}'",
                    consumerName, stream);
            return null;
        } catch (Exception e) {
            if (isFatalError(e)) {
                throw new IllegalStateException(
                        "Failed to look up stored offset for consumer '" + consumerName +
                                "' on stream '" + stream + "'", e);
            }
            throw new NonFatalLookupException(
                    "Non-fatal failure looking up offset for consumer '" + consumerName +
                            "' on stream '" + stream + "': " + e.getMessage(), e);
        } finally {
            if (tempConsumer != null) {
                try {
                    tempConsumer.close();
                } catch (Exception e) {
                    LOG.debug("Error closing temp consumer for offset lookup on '{}'",
                            stream, e);
                }
            }
        }
    }

    /**
     * Determine if an exception represents a fatal error that should not be
     * retried or fallen back from.
     *
     * <p>Stored offset lookup should fail fast for all errors except a small
     * set of known non-fatal conditions (missing stream / tracking-consumer
     * capacity limits). Unknown failures are treated as fatal to avoid silent
     * fallback to startingOffsets.
     */
    private static boolean isFatalError(Exception e) {
        return !isKnownNonFatalError(e);
    }

    private static boolean isKnownNonFatalError(Throwable error) {
        Throwable current = error;
        while (current != null) {
            if (current instanceof com.rabbitmq.stream.StreamDoesNotExistException
                    || current instanceof com.rabbitmq.stream.StreamNotAvailableException) {
                return true;
            }
            String msg = current.getMessage();
            if (msg != null && isTrackingConsumerLimitMessage(msg)) {
                return true;
            }
            if (msg != null) {
                String lowerMsg = msg.toLowerCase(java.util.Locale.ROOT);
                if (lowerMsg.contains("stream does not exist")
                        || lowerMsg.contains("stream not available")) {
                    return true;
                }
            }
            current = current.getCause();
        }
        return false;
    }

    private static boolean isTrackingConsumerLimitMessage(String message) {
        String lowerMsg = message.toLowerCase(java.util.Locale.ROOT);
        if (lowerMsg.contains("max tracking consumers")
                || lowerMsg.contains("tracking consumer limit")
                || lowerMsg.contains("maxtrackingconsumersbyconnection")) {
            return true;
        }
        boolean mentionsTrackingConsumer = lowerMsg.contains("tracking consumer")
                || lowerMsg.contains("tracking consumers")
                || lowerMsg.contains("tracking-consumer");
        boolean mentionsCapacity = lowerMsg.contains("limit")
                || lowerMsg.contains("max")
                || lowerMsg.contains("reached")
                || lowerMsg.contains("too many")
                || lowerMsg.contains("precondition_failed");
        return mentionsTrackingConsumer && mentionsCapacity;
    }

    /**
     * Exception indicating a non-fatal lookup failure that can be
     * recovered from by falling back to startingOffsets.
     */
    static final class NonFatalLookupException extends RuntimeException {
        NonFatalLookupException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
