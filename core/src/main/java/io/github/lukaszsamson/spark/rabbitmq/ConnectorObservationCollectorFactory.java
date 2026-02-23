package io.github.lukaszsamson.spark.rabbitmq;

/**
 * Extension point for providing a RabbitMQ Stream observation collector.
 *
 * <p>Implementations are loaded via {@code observationCollectorClass} and must
 * provide a public no-arg constructor.
 *
 * <p>To avoid exposing shaded dependency types in the connector API, this method
 * returns {@link Object}. The returned value must be compatible with
 * {@code com.rabbitmq.stream.ObservationCollector}; the connector validates this at runtime.
 */
public interface ConnectorObservationCollectorFactory {

    /**
     * Create an observation collector for the given connector options.
     *
     * @param options parsed connector options
     * @return observation collector object compatible with
     *         {@code com.rabbitmq.stream.ObservationCollector}
     */
    Object create(ConnectorOptions options);
}
