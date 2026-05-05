package io.github.lukaszsamson.spark.rabbitmq;

import com.rabbitmq.stream.Message;
import com.rabbitmq.stream.Properties;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class MessageSizeEstimatorTest {

    @Test
    void utf8SizeMatchesJdkUtf8LengthForCommonStrings() throws Exception {
        assertThat(invokeUtf8Size(null)).isEqualTo(0);
        assertThat(invokeUtf8Size("")).isEqualTo(0);
        assertThat(invokeUtf8Size("ascii")).isEqualTo(utf8Bytes("ascii"));
        assertThat(invokeUtf8Size("zażółć")).isEqualTo(utf8Bytes("zażółć"));
        assertThat(invokeUtf8Size("🙂")).isEqualTo(utf8Bytes("🙂"));
        assertThat(invokeUtf8Size("a🙂b€ł")).isEqualTo(utf8Bytes("a🙂b€ł"));
    }

    @Test
    void estimatedWireBytesUsesUtf8SizingAcrossPayloadMapsAndProperties() {
        Message message = mock(Message.class);
        Properties properties = mock(Properties.class);

        byte[] body = new byte[]{1, 2, 3};
        Map<String, Object> appProps = new LinkedHashMap<>();
        appProps.put("k🙂", "v€");
        Map<String, Object> annotations = new LinkedHashMap<>();
        annotations.put("ż", "ą");

        when(message.getBodyAsBinary()).thenReturn(body);
        when(message.getApplicationProperties()).thenReturn(appProps);
        when(message.getMessageAnnotations()).thenReturn(annotations);
        when(message.getProperties()).thenReturn(properties);
        when(properties.getGroupId()).thenReturn("g🙂");
        when(properties.getGroupSequence()).thenReturn(-1L);

        long expected = body.length
                + utf8Bytes("k🙂") + utf8Bytes("v€")
                + utf8Bytes("ż") + utf8Bytes("ą")
                + utf8Bytes("group_id") + utf8Bytes("g🙂");

        assertThat(MessageSizeEstimator.estimatedWireBytes(message)).isEqualTo(expected);
    }

    @Test
    void estimatedWireBytesIncludesGroupSequenceWhenZero() {
        Message message = mock(Message.class);
        Properties properties = mock(Properties.class);

        when(message.getBodyAsBinary()).thenReturn(null);
        when(message.getApplicationProperties()).thenReturn(Map.of());
        when(message.getMessageAnnotations()).thenReturn(Map.of());
        when(message.getProperties()).thenReturn(properties);
        when(properties.getGroupSequence()).thenReturn(0L);

        long expected = utf8Bytes("group_sequence") + utf8Bytes("0");
        assertThat(MessageSizeEstimator.estimatedWireBytes(message)).isEqualTo(expected);
    }

    @Test
    void nestedAmqpValuesUseConstantFallbackInsteadOfToString() {
        Message message = mock(Message.class);
        Properties properties = mock(Properties.class);

        // Build a nested value whose toString() would otherwise dominate the
        // estimate. The estimator must use a small constant fallback instead.
        StringBuilder big = new StringBuilder();
        for (int i = 0; i < 4096; i++) {
            big.append('x');
        }
        Map<String, Object> nestedMap = Map.of("inner", big.toString());
        List<Object> nestedList = List.of(big.toString(), big.toString());

        Map<String, Object> appProps = new LinkedHashMap<>();
        appProps.put("m", nestedMap);
        appProps.put("l", nestedList);
        // Numeric and boolean values still contribute their textual size so
        // existing telemetry is unaffected.
        appProps.put("n", 12345L);
        appProps.put("b", Boolean.TRUE);

        when(message.getBodyAsBinary()).thenReturn(null);
        when(message.getApplicationProperties()).thenReturn(appProps);
        when(message.getMessageAnnotations()).thenReturn(Map.of());
        when(message.getProperties()).thenReturn(properties);
        when(properties.getGroupSequence()).thenReturn(-1L);

        long estimate = MessageSizeEstimator.estimatedWireBytes(message);

        // Two nested-value fallbacks (16 bytes each) + scalar values, far less
        // than the ~12k bytes a toString() walk would have produced.
        assertThat(estimate).isLessThan(512L);
        assertThat(estimate).isGreaterThan(0L);
    }

    @Test
    void payloadBytesReturnsZeroForNonBinaryBody() {
        Message message = mock(Message.class);
        when(message.getBodyAsBinary()).thenThrow(new IllegalStateException("non-binary body"));

        assertThat(MessageSizeEstimator.payloadBytes(message)).isZero();
    }

    private static int utf8Bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8).length;
    }

    private static int invokeUtf8Size(String value) throws Exception {
        Method method = MessageSizeEstimator.class.getDeclaredMethod("utf8Size", String.class);
        method.setAccessible(true);
        return (int) method.invoke(null, value);
    }
}
