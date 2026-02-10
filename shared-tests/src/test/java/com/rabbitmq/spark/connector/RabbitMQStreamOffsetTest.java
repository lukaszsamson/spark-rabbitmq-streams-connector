package com.rabbitmq.spark.connector;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TreeMap;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for {@link RabbitMQStreamOffset} JSON serialization/deserialization
 * and equality semantics without broker access.
 */
class RabbitMQStreamOffsetTest {

    @Nested
    class JsonSerialization {

        @Test
        void emptyOffsetSerializesToEmptyObject() {
            RabbitMQStreamOffset offset = new RabbitMQStreamOffset(Map.of());
            assertThat(offset.json()).isEqualTo("{}");
        }

        @Test
        void singleStreamOffset() {
            RabbitMQStreamOffset offset = new RabbitMQStreamOffset(Map.of("orders", 200L));
            assertThat(offset.json()).isEqualTo("{\"orders\":200}");
        }

        @Test
        void multipleStreamOffsets() {
            // TreeMap ensures alphabetical ordering
            Map<String, Long> offsets = new TreeMap<>();
            offsets.put("stream-a", 12345L);
            offsets.put("stream-b", 67890L);
            RabbitMQStreamOffset offset = new RabbitMQStreamOffset(offsets);
            assertThat(offset.json()).isEqualTo("{\"stream-a\":12345,\"stream-b\":67890}");
        }

        @Test
        void deterministicOrderingRegardlessOfInputOrder() {
            Map<String, Long> offsets1 = new LinkedHashMap<>();
            offsets1.put("z-stream", 1L);
            offsets1.put("a-stream", 2L);

            Map<String, Long> offsets2 = new LinkedHashMap<>();
            offsets2.put("a-stream", 2L);
            offsets2.put("z-stream", 1L);

            RabbitMQStreamOffset o1 = new RabbitMQStreamOffset(offsets1);
            RabbitMQStreamOffset o2 = new RabbitMQStreamOffset(offsets2);

            assertThat(o1.json()).isEqualTo(o2.json());
        }

        @Test
        void zeroOffset() {
            RabbitMQStreamOffset offset = new RabbitMQStreamOffset(Map.of("s", 0L));
            assertThat(offset.json()).isEqualTo("{\"s\":0}");
        }

        @Test
        void largeOffset() {
            RabbitMQStreamOffset offset = new RabbitMQStreamOffset(
                    Map.of("big", Long.MAX_VALUE));
            assertThat(offset.json()).contains(String.valueOf(Long.MAX_VALUE));
        }
    }

    @Nested
    class JsonDeserialization {

        @Test
        void roundTripSingleStream() {
            RabbitMQStreamOffset original = new RabbitMQStreamOffset(Map.of("orders", 200L));
            RabbitMQStreamOffset parsed = RabbitMQStreamOffset.fromJson(original.json());
            assertThat(parsed).isEqualTo(original);
        }

        @Test
        void roundTripMultipleStreams() {
            Map<String, Long> offsets = new TreeMap<>();
            offsets.put("stream-a", 12345L);
            offsets.put("stream-b", 67890L);
            offsets.put("stream-c", 0L);
            RabbitMQStreamOffset original = new RabbitMQStreamOffset(offsets);
            RabbitMQStreamOffset parsed = RabbitMQStreamOffset.fromJson(original.json());
            assertThat(parsed).isEqualTo(original);
        }

        @Test
        void roundTripEmptyOffset() {
            RabbitMQStreamOffset original = new RabbitMQStreamOffset(Map.of());
            RabbitMQStreamOffset parsed = RabbitMQStreamOffset.fromJson(original.json());
            assertThat(parsed).isEqualTo(original);
        }

        @Test
        void parseWithWhitespace() {
            String json = "{ \"orders\" : 42 , \"events\" : 100 }";
            RabbitMQStreamOffset parsed = RabbitMQStreamOffset.fromJson(json);
            assertThat(parsed.getStreamOffsets()).containsEntry("orders", 42L);
            assertThat(parsed.getStreamOffsets()).containsEntry("events", 100L);
        }

        @Test
        void nullJsonThrows() {
            assertThatThrownBy(() -> RabbitMQStreamOffset.fromJson(null))
                    .isInstanceOf(IllegalArgumentException.class);
        }

        @Test
        void blankJsonThrows() {
            assertThatThrownBy(() -> RabbitMQStreamOffset.fromJson("   "))
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Nested
    class Equality {

        @Test
        void equalOffsets() {
            RabbitMQStreamOffset a = new RabbitMQStreamOffset(Map.of("s", 42L));
            RabbitMQStreamOffset b = new RabbitMQStreamOffset(Map.of("s", 42L));
            assertThat(a).isEqualTo(b);
            assertThat(a.hashCode()).isEqualTo(b.hashCode());
        }

        @Test
        void differentOffsetsNotEqual() {
            RabbitMQStreamOffset a = new RabbitMQStreamOffset(Map.of("s", 42L));
            RabbitMQStreamOffset b = new RabbitMQStreamOffset(Map.of("s", 43L));
            assertThat(a).isNotEqualTo(b);
        }

        @Test
        void differentStreamsNotEqual() {
            RabbitMQStreamOffset a = new RabbitMQStreamOffset(Map.of("s1", 42L));
            RabbitMQStreamOffset b = new RabbitMQStreamOffset(Map.of("s2", 42L));
            assertThat(a).isNotEqualTo(b);
        }

        @Test
        void equalToSelf() {
            RabbitMQStreamOffset a = new RabbitMQStreamOffset(Map.of("s", 42L));
            assertThat(a).isEqualTo(a);
        }

        @Test
        void notEqualToNull() {
            RabbitMQStreamOffset a = new RabbitMQStreamOffset(Map.of("s", 42L));
            assertThat(a).isNotEqualTo(null);
        }
    }

    @Nested
    class StreamOffsets {

        @Test
        void getStreamOffsetsIsUnmodifiable() {
            RabbitMQStreamOffset offset = new RabbitMQStreamOffset(Map.of("s", 1L));
            assertThatThrownBy(() -> offset.getStreamOffsets().put("x", 2L))
                    .isInstanceOf(UnsupportedOperationException.class);
        }

        @Test
        void defensiveCopyOnConstruction() {
            Map<String, Long> mutable = new LinkedHashMap<>();
            mutable.put("s", 1L);
            RabbitMQStreamOffset offset = new RabbitMQStreamOffset(mutable);

            // Mutate original map
            mutable.put("s", 999L);

            // Offset should be unchanged
            assertThat(offset.getStreamOffsets().get("s")).isEqualTo(1L);
        }
    }

    @Test
    void toStringContainsJson() {
        RabbitMQStreamOffset offset = new RabbitMQStreamOffset(Map.of("test", 42L));
        assertThat(offset.toString()).contains("test").contains("42");
    }
}
