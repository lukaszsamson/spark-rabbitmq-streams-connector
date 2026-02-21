package io.github.lukaszsamson.spark.rabbitmq;

import com.rabbitmq.stream.Environment;
import com.rabbitmq.stream.Producer;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Discovers partition streams for a RabbitMQ superstream.
 */
final class SuperStreamPartitionDiscovery {

    private SuperStreamPartitionDiscovery() {}

    /**
     * Discover partition streams using connector connection options.
     *
     * @param options connector options with connection configuration
     * @param superStream the superstream name
     * @return ordered list of partition stream names
     */
    static List<String> discoverPartitions(ConnectorOptions options, String superStream) {
        Environment environment = EnvironmentPool.getInstance().acquire(options);
        try {
            return discoverPartitions(superStream,
                    stream -> discoverPartitionsViaEnvironment(environment, stream));
        } finally {
            EnvironmentPool.getInstance().release(options);
        }
    }

    static List<String> discoverPartitions(String superStream, PartitionsQuery query) {
        try {
            return query.partitions(superStream);
        } catch (Exception e) {
            throw new IllegalStateException(
                    "Unable to discover superstream partitions for '" + superStream + "'", e);
        }
    }

    private static List<String> discoverPartitionsViaEnvironment(Environment environment,
                                                                 String superStream) {
        AtomicReference<List<String>> partitions = new AtomicReference<>(List.of());
        Producer producer = environment.producerBuilder()
                .superStream(superStream)
                .routing(message -> "")
                .strategy((message, metadata) -> {
                    partitions.set(List.copyOf(metadata.partitions()));
                    return List.of();
                })
                .producerBuilder()
                .build();
        try {
            producer.send(producer.messageBuilder().addData(new byte[0]).build(),
                    status -> {});
            return partitions.get();
        } finally {
            producer.close();
        }
    }

    @FunctionalInterface
    interface PartitionsQuery {
        List<String> partitions(String superStream) throws Exception;
    }
}
