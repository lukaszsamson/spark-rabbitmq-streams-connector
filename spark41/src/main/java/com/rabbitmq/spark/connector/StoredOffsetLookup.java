package com.rabbitmq.spark.connector;

import com.rabbitmq.stream.Consumer;
import com.rabbitmq.stream.Environment;
import com.rabbitmq.stream.NoOffsetException;
import com.rabbitmq.stream.OffsetSpecification;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Semaphore;

/**
 * Looks up stored consumer offsets from the RabbitMQ broker.
 *
 * <p>Uses temporary named consumers with {@code noTrackingStrategy()} to query
 * the broker for the last stored offset per stream. This is the recommended
 * approach since {@link Environment} does not expose a direct
 * {@code queryOffset()} API.
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

    private StoredOffsetLookup() {}

    /**
     * Result of a stored offset lookup, distinguishing between successful
     * lookups and non-fatal failures.
     */
    static final class LookupResult {
        private final Map<String, Long> offsets;
        private final List<String> failedStreams;

        LookupResult(Map<String, Long> offsets, List<String> failedStreams) {
            this.offsets = offsets;
            this.failedStreams = failedStreams;
        }

        Map<String, Long> getOffsets() { return offsets; }
        List<String> getFailedStreams() { return failedStreams; }
        boolean hasFailures() { return !failedStreams.isEmpty(); }
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
        Map<String, Long> offsets = new LinkedHashMap<>();
        List<String> failed = new java.util.ArrayList<>();
        Semaphore semaphore = new Semaphore(MAX_CONCURRENT_LOOKUPS);

        for (String stream : streams) {
            try {
                semaphore.acquire();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(
                        "Interrupted during stored offset lookup", e);
            }
            try {
                Long nextOffset = lookupStream(env, consumerName, stream);
                if (nextOffset != null) {
                    offsets.put(stream, nextOffset);
                }
            } catch (NonFatalLookupException e) {
                LOG.warn("Non-fatal lookup failure for consumer '{}' on stream '{}': {}",
                        consumerName, stream, e.getMessage());
                failed.add(stream);
            } finally {
                semaphore.release();
            }
        }
        return new LookupResult(offsets, failed);
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
                    .noTrackingStrategy()
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
     * <p>Fatal errors include authentication failures, connection failures,
     * and stream-does-not-exist errors.
     */
    private static boolean isFatalError(Exception e) {
        String msg = e.getMessage();
        if (msg == null) {
            msg = "";
        }
        String lowerMsg = msg.toLowerCase(java.util.Locale.ROOT);

        // Authentication/authorization failures
        if (lowerMsg.contains("authentication") || lowerMsg.contains("access refused")
                || lowerMsg.contains("unauthorized") || lowerMsg.contains("sasl")) {
            return true;
        }

        // Connection failures
        if (lowerMsg.contains("connection refused") || lowerMsg.contains("connection closed")
                || lowerMsg.contains("cannot connect")) {
            return true;
        }

        // Stream does not exist
        if (lowerMsg.contains("stream does not exist") || lowerMsg.contains("no such stream")) {
            return true;
        }

        // Check cause chain for known fatal exception types
        Throwable cause = e;
        while (cause != null) {
            if (cause instanceof com.rabbitmq.stream.StreamDoesNotExistException) {
                return true;
            }
            if (cause instanceof com.rabbitmq.stream.AuthenticationFailureException) {
                return true;
            }
            cause = cause.getCause();
        }

        return false;
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
