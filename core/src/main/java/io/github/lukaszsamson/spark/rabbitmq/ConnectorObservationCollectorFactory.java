package io.github.lukaszsamson.spark.rabbitmq;

import com.rabbitmq.stream.ObservationCollector;

/**
 * Extension point for providing a RabbitMQ Stream {@link ObservationCollector}.
 *
 * <p>Implementations are loaded via {@code observationCollectorClass} and must
 * provide a public no-arg constructor.
 */
public interface ConnectorObservationCollectorFactory {

    /**
     * Create an observation collector for the given connector options.
     *
     * @param options parsed connector options
     * @return observation collector to install on the environment builder
     */
    ObservationCollector<?> create(ConnectorOptions options);
}
