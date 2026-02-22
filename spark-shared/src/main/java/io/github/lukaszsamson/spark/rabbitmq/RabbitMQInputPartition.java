package io.github.lukaszsamson.spark.rabbitmq;

import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.util.LongAccumulator;

/**
 * A serializable input partition for RabbitMQ stream reads.
 *
 * <p>Carries the stream name, offset range, and connector options needed
 * to create a consumer on the executor.
 *
 * <p>Supports {@link #preferredLocations()} locality hints when valid
 * Spark scheduler locations are available.
 */
public final class RabbitMQInputPartition implements InputPartition {
    private static final long serialVersionUID = 1L;

    private final String stream;
    private final long startOffset;
    private final long endOffset;
    private final ConnectorOptions options;
    private final boolean useConfiguredStartingOffset;
    private final String[] preferredLocations;
    private final String messageSizeTrackerScope;
    private final LongAccumulator messageSizeBytesAccumulator;
    private final LongAccumulator messageSizeRecordsAccumulator;

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
        this(stream, startOffset, endOffset, options, useConfiguredStartingOffset,
                preferredLocations, null);
    }

    public RabbitMQInputPartition(String stream, long startOffset, long endOffset,
                                  ConnectorOptions options, boolean useConfiguredStartingOffset,
                                  String[] preferredLocations, String messageSizeTrackerScope) {
        this(stream, startOffset, endOffset, options, useConfiguredStartingOffset,
                preferredLocations, messageSizeTrackerScope, null, null);
    }

    public RabbitMQInputPartition(String stream, long startOffset, long endOffset,
                                  ConnectorOptions options, boolean useConfiguredStartingOffset,
                                  String[] preferredLocations, String messageSizeTrackerScope,
                                  LongAccumulator messageSizeBytesAccumulator,
                                  LongAccumulator messageSizeRecordsAccumulator) {
        this.stream = stream;
        this.startOffset = startOffset;
        this.endOffset = endOffset;
        this.options = options;
        this.useConfiguredStartingOffset = useConfiguredStartingOffset;
        this.preferredLocations = preferredLocations;
        this.messageSizeTrackerScope = messageSizeTrackerScope;
        this.messageSizeBytesAccumulator = messageSizeBytesAccumulator;
        this.messageSizeRecordsAccumulator = messageSizeRecordsAccumulator;
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

    public String getMessageSizeTrackerScope() {
        return messageSizeTrackerScope;
    }

    public LongAccumulator getMessageSizeBytesAccumulator() {
        return messageSizeBytesAccumulator;
    }

    public LongAccumulator getMessageSizeRecordsAccumulator() {
        return messageSizeRecordsAccumulator;
    }

    @Override
    public String[] preferredLocations() {
        return preferredLocations != null ? preferredLocations : new String[0];
    }

    /**
     * Return preferred locations for the partition stream.
     *
     * <p>Synthetic location strings are intentionally disabled. Spark expects
     * preferred locations to map to real scheduler locations (e.g. hosts or
     * executor cache locations); fabricated values degrade to ANY locality.
     *
     * @param stream the stream name
     * @return an empty array (no preferred location hint)
     */
    static String[] locationForStream(String stream) {
        return new String[0];
    }

    @Override
    public String toString() {
        return "RabbitMQInputPartition{stream='" + stream + "', offsets=[" +
                startOffset + ", " + endOffset + ")}";
    }
}
