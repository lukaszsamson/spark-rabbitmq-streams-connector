package io.github.lukaszsamson.spark.rabbitmq;

import java.util.Locale;

/**
 * Routing strategy for superstream producers.
 */
public enum RoutingStrategyType {
    /** Hash-based routing (MurmurHash3 of routing key modulo partition count). */
    HASH,
    /** Key-based routing using superstream bindings. */
    KEY,
    /** User-provided routing strategy via {@code partitionerClass}. */
    CUSTOM;

    public static RoutingStrategyType fromString(String value) {
        try {
            return valueOf(value.toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(
                    "Invalid routingStrategy value: '" + value +
                            "'. Must be one of: hash, key, custom");
        }
    }
}
