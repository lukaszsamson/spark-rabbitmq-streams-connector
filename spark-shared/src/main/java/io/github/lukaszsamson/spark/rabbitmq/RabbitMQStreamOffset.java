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
     * @throws IllegalArgumentException if the JSON is null, empty, or malformed
     */
    public static RabbitMQStreamOffset fromJson(String json) {
        if (json == null || json.isBlank()) {
            throw new IllegalArgumentException("Offset JSON must not be null or blank");
        }

        String trimmed = json.trim();
        if (!trimmed.startsWith("{") || !trimmed.endsWith("}")) {
            throw new IllegalArgumentException("Malformed offset JSON: " + json);
        }

        String body = trimmed.substring(1, trimmed.length() - 1).trim();
        if (body.isEmpty()) {
            return new RabbitMQStreamOffset(Collections.emptyMap());
        }

        Map<String, Long> offsets = new LinkedHashMap<>();
        Matcher matcher = ENTRY_PATTERN.matcher(body);
        int position = 0;
        while (position < body.length()) {
            matcher.region(position, body.length());
            if (!matcher.lookingAt()) {
                throw new IllegalArgumentException("Malformed offset JSON: " + json);
            }
            String stream = unescapeJson(matcher.group(1));
            long offset = Long.parseLong(matcher.group(2));
            offsets.put(stream, offset);
            position = matcher.end();

            while (position < body.length() && Character.isWhitespace(body.charAt(position))) {
                position++;
            }
            if (position >= body.length()) {
                break;
            }
            if (body.charAt(position) != ',') {
                throw new IllegalArgumentException("Malformed offset JSON: " + json);
            }
            position++;
            while (position < body.length() && Character.isWhitespace(body.charAt(position))) {
                position++;
            }
            if (position >= body.length()) {
                throw new IllegalArgumentException("Malformed offset JSON: " + json);
            }
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
        StringBuilder out = new StringBuilder(s.length() + 8);
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            switch (c) {
                case '"' -> out.append("\\\"");
                case '\\' -> out.append("\\\\");
                case '\b' -> out.append("\\b");
                case '\f' -> out.append("\\f");
                case '\n' -> out.append("\\n");
                case '\r' -> out.append("\\r");
                case '\t' -> out.append("\\t");
                default -> {
                    if (c < 0x20) {
                        out.append("\\u");
                        String hex = Integer.toHexString(c);
                        for (int j = hex.length(); j < 4; j++) {
                            out.append('0');
                        }
                        out.append(hex);
                    } else {
                        out.append(c);
                    }
                }
            }
        }
        return out.toString();
    }

    private static String unescapeJson(String s) {
        StringBuilder out = new StringBuilder(s.length());
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c != '\\') {
                out.append(c);
                continue;
            }
            if (i + 1 >= s.length()) {
                throw new IllegalArgumentException("Malformed escaped JSON string");
            }
            char esc = s.charAt(++i);
            switch (esc) {
                case '"' -> out.append('"');
                case '\\' -> out.append('\\');
                case '/' -> out.append('/');
                case 'b' -> out.append('\b');
                case 'f' -> out.append('\f');
                case 'n' -> out.append('\n');
                case 'r' -> out.append('\r');
                case 't' -> out.append('\t');
                case 'u' -> {
                    if (i + 4 >= s.length()) {
                        throw new IllegalArgumentException("Malformed unicode escape in JSON string");
                    }
                    String hex = s.substring(i + 1, i + 5);
                    try {
                        out.append((char) Integer.parseInt(hex, 16));
                    } catch (NumberFormatException e) {
                        throw new IllegalArgumentException(
                                "Malformed unicode escape in JSON string", e);
                    }
                    i += 4;
                }
                default -> throw new IllegalArgumentException("Malformed escaped JSON string");
            }
        }
        return out.toString();
    }
}
