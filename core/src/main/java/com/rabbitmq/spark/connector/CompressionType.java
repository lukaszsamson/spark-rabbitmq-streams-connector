package com.rabbitmq.spark.connector;

import java.util.Locale;

/**
 * Compression algorithm for sub-entry batching on the sink path.
 */
public enum CompressionType {
    NONE,
    GZIP,
    SNAPPY,
    LZ4,
    ZSTD;

    public static CompressionType fromString(String value) {
        try {
            return valueOf(value.toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(
                    "Invalid compression value: '" + value +
                            "'. Must be one of: none, gzip, snappy, lz4, zstd");
        }
    }
}
