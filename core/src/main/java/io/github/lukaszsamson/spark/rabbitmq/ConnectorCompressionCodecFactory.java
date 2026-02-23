package io.github.lukaszsamson.spark.rabbitmq;

/**
 * Extension point for providing a RabbitMQ Stream compression codec factory.
 *
 * <p>Implementations are loaded via {@code compressionCodecFactoryClass} and must
 * provide a public no-arg constructor.
 *
 * <p>To avoid exposing shaded dependency types in the connector API, this method
 * returns {@link Object}. The returned value must be compatible with
 * {@code com.rabbitmq.stream.compression.CompressionCodecFactory}; the connector
 * validates this at runtime.
 */
public interface ConnectorCompressionCodecFactory {

    /**
     * Create a compression codec factory for the given connector options.
     *
     * @param options parsed connector options
     * @return codec factory object compatible with
     *         {@code com.rabbitmq.stream.compression.CompressionCodecFactory}
     */
    Object create(ConnectorOptions options);
}
