package io.github.lukaszsamson.spark.rabbitmq;

import java.util.Locale;

/**
 * Strategy for handling {@code startingOffsets=timestamp} when no offset can be resolved.
 */
public enum StartingOffsetsByTimestampStrategy {
    /** Fail query initialization when no record exists at/after the requested timestamp. */
    ERROR,
    /** Use the latest offset (tail) when no record exists at/after the requested timestamp. */
    LATEST;

    public static StartingOffsetsByTimestampStrategy fromString(String value) {
        try {
            return valueOf(value.toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(
                    "Invalid startingOffsetsByTimestampStrategy value: '" + value
                            + "'. Must be one of: error, latest");
        }
    }
}
