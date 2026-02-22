package io.github.lukaszsamson.spark.rabbitmq;

import com.rabbitmq.stream.Message;
import com.rabbitmq.stream.Properties;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
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

        long expected = body.length
                + utf8Bytes("k🙂") + utf8Bytes("v€")
                + utf8Bytes("ż") + utf8Bytes("ą")
                + utf8Bytes("group_id") + utf8Bytes("g🙂");

        assertThat(MessageSizeEstimator.estimatedWireBytes(message)).isEqualTo(expected);
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
