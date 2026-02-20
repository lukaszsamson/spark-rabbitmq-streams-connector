package com.rabbitmq.spark.connector;

import java.io.Serializable;
/**
 * Post-filter for stream messages after broker-side filtering.
 *
 * <p>When broker-side stream filtering is enabled, false positives from the
 * Bloom filter may pass through. This filter runs on executors to eliminate
 * false positives client-side.
 *
 * <p>Implementations must have a public no-arg constructor and be
 * {@link Serializable} (they are shipped to executors).
 *
 * <p>This is a connector-defined interface that avoids exposing shaded
 * RabbitMQ client types to user code.
 */
public interface ConnectorPostFilter extends Serializable {

    /**
     * Test whether a message should be included in the output.
     *
     * @return {@code true} to include the message, {@code false} to discard it
     */
    boolean accept(ConnectorMessageView message);
}
