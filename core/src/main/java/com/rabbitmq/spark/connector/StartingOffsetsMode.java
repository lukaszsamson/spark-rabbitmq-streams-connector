package com.rabbitmq.spark.connector;

import java.util.Locale;

/**
 * Starting offset specification for source reads.
 */
public enum StartingOffsetsMode {
    /** Start from the first available message. */
    EARLIEST,
    /** Start from the next message after the current tail. */
    LATEST,
    /** Start from a specific numeric offset (inclusive). */
    OFFSET,
    /** Start from a specific timestamp (epoch millis). */
    TIMESTAMP;

    public static StartingOffsetsMode fromString(String value) {
        try {
            return valueOf(value.toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(
                    "Invalid startingOffsets value: '" + value +
                            "'. Must be one of: earliest, latest, offset, timestamp");
        }
    }
}
