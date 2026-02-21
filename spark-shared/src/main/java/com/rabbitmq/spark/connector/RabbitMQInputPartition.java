package com.rabbitmq.spark.connector;

import org.apache.spark.sql.connector.read.InputPartition;

/**
 * A serializable input partition for RabbitMQ stream reads.
 *
 * <p>Carries the stream name, offset range, and connector options needed
 * to create a consumer on the executor.
 *
 * <p>Supports {@link #preferredLocations()} for deterministic executor
 * affinity: the same stream is scheduled to the same executor across
 * micro-batches, improving connection ({@link EnvironmentPool}) reuse.
 */
public final class RabbitMQInputPartition implements InputPartition {
    private static final long serialVersionUID = 1L;

    private final String stream;
    private final long startOffset;
    private final long endOffset;
    private final ConnectorOptions options;
    private final boolean useConfiguredStartingOffset;
    private final String[] preferredLocations;

    /**
     * @param stream the RabbitMQ stream name (or partition stream name for superstreams)
     * @param startOffset inclusive start offset
     * @param endOffset exclusive end offset
     * @param options the connector options (serializable)
     */
    public RabbitMQInputPartition(String stream, long startOffset, long endOffset,
                                   ConnectorOptions options) {
        this(stream, startOffset, endOffset, options, false);
    }

    /**
     * @param stream the RabbitMQ stream name (or partition stream name for superstreams)
     * @param startOffset inclusive start offset
     * @param endOffset exclusive end offset
     * @param options the connector options (serializable)
     * @param useConfiguredStartingOffset whether reader initialization should honor
     *                                    configured starting mode semantics
     */
    public RabbitMQInputPartition(String stream, long startOffset, long endOffset,
                                  ConnectorOptions options, boolean useConfiguredStartingOffset) {
        this(stream, startOffset, endOffset, options, useConfiguredStartingOffset, null);
    }

    /**
     * @param stream the RabbitMQ stream name (or partition stream name for superstreams)
     * @param startOffset inclusive start offset
     * @param endOffset exclusive end offset
     * @param options the connector options (serializable)
     * @param useConfiguredStartingOffset whether reader initialization should honor
     *                                    configured starting mode semantics
     * @param preferredLocations executor locality hints for this partition (may be null)
     */
    public RabbitMQInputPartition(String stream, long startOffset, long endOffset,
                                  ConnectorOptions options, boolean useConfiguredStartingOffset,
                                  String[] preferredLocations) {
        this.stream = stream;
        this.startOffset = startOffset;
        this.endOffset = endOffset;
        this.options = options;
        this.useConfiguredStartingOffset = useConfiguredStartingOffset;
        this.preferredLocations = preferredLocations;
    }

    public String getStream() {
        return stream;
    }

    public long getStartOffset() {
        return startOffset;
    }

    public long getEndOffset() {
        return endOffset;
    }

    public ConnectorOptions getOptions() {
        return options;
    }

    public boolean isUseConfiguredStartingOffset() {
        return useConfiguredStartingOffset;
    }

    @Override
    public String[] preferredLocations() {
        return preferredLocations != null ? preferredLocations : new String[0];
    }

    /**
     * Compute a deterministic preferred-location hint for a stream name.
     * This ensures the same stream is consistently scheduled to the same
     * executor across micro-batches, maximising {@link EnvironmentPool} reuse.
     *
     * <p>The location is a synthetic host string ({@code "rabbitmq-executor-N"})
     * that Spark uses as a scheduling preference. The consistency of the hash
     * is what matters, not the actual host name.
     *
     * @param stream the stream name
     * @return a single-element array with the preferred location
     */
    static String[] locationForStream(String stream) {
        // Use unsigned 31-bit hash to get a stable positive bucket.
        // The bucket count is high enough that collisions are rare across
        // typical partition counts, but low enough that executor affinity
        // clusters related partitions rather than fragmenting across all nodes.
        int bucket = (stream.hashCode() & 0x7FFFFFFF) % 1000;
        return new String[]{"rabbitmq-executor-" + bucket};
    }

    @Override
    public String toString() {
        return "RabbitMQInputPartition{stream='" + stream + "', offsets=[" +
                startOffset + ", " + endOffset + ")}";
    }
}
