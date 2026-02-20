package com.rabbitmq.spark.connector;

import io.micrometer.observation.ObservationRegistry;

/**
 * Extension point for providing a Micrometer {@link ObservationRegistry}.
 *
 * <p>Implementations are loaded via {@code observationRegistryProviderClass}
 * and must provide a public no-arg constructor.
 */
public interface ConnectorObservationRegistryProvider {

    /**
     * Create an observation registry for the given connector options.
     *
     * @param options parsed connector options
     * @return observation registry used to build the RabbitMQ micrometer collector
     */
    ObservationRegistry create(ConnectorOptions options);
}
