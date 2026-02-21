package io.github.lukaszsamson.spark.rabbitmq;

import com.rabbitmq.stream.compression.CompressionCodecFactory;

/**
 * Extension point for providing a RabbitMQ Stream {@link CompressionCodecFactory}.
 *
 * <p>Implementations are loaded via {@code compressionCodecFactoryClass} and must
 * provide a public no-arg constructor.
 */
public interface ConnectorCompressionCodecFactory {

    /**
     * Create a compression codec factory for the given connector options.
     *
     * @param options parsed connector options
     * @return compression codec factory to install on the environment builder
     */
    CompressionCodecFactory create(ConnectorOptions options);
}
