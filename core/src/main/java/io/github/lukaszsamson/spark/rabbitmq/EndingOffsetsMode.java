package io.github.lukaszsamson.spark.rabbitmq;

import java.util.Locale;

/**
 * Ending offset specification for bounded reads.
 */
public enum EndingOffsetsMode {
    /** Read up to the current tail offset at planning time. */
    LATEST,
    /** Read up to a specific numeric offset (exclusive). */
    OFFSET,
    /** Read up to offsets resolved from a timestamp. */
    TIMESTAMP;

    public static EndingOffsetsMode fromString(String value) {
        try {
            return valueOf(value.toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(
                    "Invalid endingOffsets value: '" + value +
                            "'. Must be one of: latest, offset, timestamp");
        }
    }
}
