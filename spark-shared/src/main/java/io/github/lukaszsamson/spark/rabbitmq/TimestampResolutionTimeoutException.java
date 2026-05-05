package io.github.lukaszsamson.spark.rabbitmq;

/**
 * Thrown when a timestamp probe consumer does not deliver a result within the
 * configured probe budget. Distinct from a broker-confirmed no-match
 * ({@link com.rabbitmq.stream.NoOffsetException}); a timeout means the planner
 * could not establish whether a matching offset exists, so silently jumping to
 * tail or earliest could skip or over-include records.
 */
public final class TimestampResolutionTimeoutException extends IllegalStateException {
    private static final long serialVersionUID = 1L;

    TimestampResolutionTimeoutException(String message) {
        super(message);
    }
}
