package com.rabbitmq.spark.connector;

import org.apache.spark.sql.connector.read.InputPartition;

/**
 * A serializable input partition for RabbitMQ stream reads.
 *
 * <p>Carries the stream name, offset range, and connector options needed
 * to create a consumer on the executor.
 */
public final class RabbitMQInputPartition implements InputPartition {
    private static final long serialVersionUID = 1L;

    private final String stream;
    private final long startOffset;
    private final long endOffset;
    private final ConnectorOptions options;
    private final boolean useConfiguredStartingOffset;

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
        this.stream = stream;
        this.startOffset = startOffset;
        this.endOffset = endOffset;
        this.options = options;
        this.useConfiguredStartingOffset = useConfiguredStartingOffset;
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
    public String toString() {
        return "RabbitMQInputPartition{stream='" + stream + "', offsets=[" +
                startOffset + ", " + endOffset + ")}";
    }
}
