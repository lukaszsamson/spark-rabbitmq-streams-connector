package io.github.lukaszsamson.spark.rabbitmq;

import org.apache.spark.sql.connector.read.streaming.Offset;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Spark {@link Offset} implementation that maps stream names to next offsets.
 *
 * <p>The JSON format is: {@code {"stream-a": 12345, "stream-b": 67890}}.
 * Offsets stored here are <em>next</em> offsets (exclusive upper bounds).
 *
 * <p>Uses a {@link TreeMap} internally so that JSON serialization is
 * deterministic—Spark relies on identical JSON for equal offsets.
 */
public final class RabbitMQStreamOffset extends Offset {

    private static final Pattern ENTRY_PATTERN =
            Pattern.compile("\"((?:\\\\.|[^\"\\\\])*)\"\\s*:\\s*(-?\\d+)");

    private final TreeMap<String, Long> streamOffsets;

    /**
     * @param streamOffsets stream name → next offset (exclusive); defensively copied
     */
    public RabbitMQStreamOffset(Map<String, Long> streamOffsets) {
        this.streamOffsets = new TreeMap<>(streamOffsets);
    }

    /** Returns an unmodifiable view of the stream-name → next-offset map. */
    public Map<String, Long> getStreamOffsets() {
        return Collections.unmodifiableMap(streamOffsets);
    }

    @Override
    public String json() {
        if (streamOffsets.isEmpty()) {
            return "{}";
        }
        StringBuilder sb = new StringBuilder("{");
        boolean first = true;
        for (Map.Entry<String, Long> entry : streamOffsets.entrySet()) {
            if (!first) {
                sb.append(",");
            }
            sb.append("\"").append(escapeJson(entry.getKey())).append("\":")
              .append(entry.getValue());
            first = false;
        }
        sb.append("}");
        return sb.toString();
    }

    /**
     * Deserialize an offset from its JSON representation.
     *
     * @param json the JSON string, e.g. {@code {"orders":200}}
     * @return the deserialized offset
     * @throws IllegalArgumentException if the JSON is null or empty
     */
    public static RabbitMQStreamOffset fromJson(String json) {
        if (json == null || json.isBlank()) {
            throw new IllegalArgumentException("Offset JSON must not be null or blank");
        }

        Map<String, Long> offsets = new LinkedHashMap<>();
        Matcher matcher = ENTRY_PATTERN.matcher(json);
        while (matcher.find()) {
            String stream = unescapeJson(matcher.group(1));
            long offset = Long.parseLong(matcher.group(2));
            offsets.put(stream, offset);
        }
        return new RabbitMQStreamOffset(offsets);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof RabbitMQStreamOffset that)) return false;
        return streamOffsets.equals(that.streamOffsets);
    }

    @Override
    public int hashCode() {
        return streamOffsets.hashCode();
    }

    @Override
    public String toString() {
        return "RabbitMQStreamOffset" + json();
    }

    private static String escapeJson(String s) {
        return s.replace("\\", "\\\\").replace("\"", "\\\"");
    }

    private static String unescapeJson(String s) {
        return s.replace("\\\\", "\\").replace("\\\"", "\"");
    }
}
