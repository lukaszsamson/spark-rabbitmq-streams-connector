package io.github.lukaszsamson.spark.rabbitmq;

import com.rabbitmq.stream.Message;
import com.rabbitmq.stream.Properties;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class MessageViewCoercionTest {

    @Test
    void toMessageViewHandlesNonBinaryBodyGracefully() {
        Message message = mock(Message.class);
        when(message.getBodyAsBinary()).thenThrow(new IllegalStateException("non-binary body"));
        when(message.getApplicationProperties()).thenReturn(Map.of());
        when(message.getMessageAnnotations()).thenReturn(Map.of());
        when(message.getProperties()).thenReturn(null);

        ConnectorMessageView view = MessageViewCoercion.toMessageView(message);

        assertThat(view.getBody()).isNull();
    }

    @Test
    void coercePropertiesToStringsOmitsZeroTimestamps() {
        // QpidProtonCodec returns NULL_TIMESTAMP = 0L for absent AMQP timestamps.
        // coercePropertiesToStrings must omit keys with 0 values, not include "0".
        Properties props = mock(Properties.class);
        when(props.getCreationTime()).thenReturn(0L);
        when(props.getAbsoluteExpiryTime()).thenReturn(0L);
        when(props.getGroupSequence()).thenReturn(-1L);

        Map<String, String> result = MessageViewCoercion.coercePropertiesToStrings(props);

        assertThat(result).doesNotContainKey("creation_time");
        assertThat(result).doesNotContainKey("absolute_expiry_time");
    }

    @Test
    void coercePropertiesToStringsIncludesPositiveTimestamps() {
        Properties props = mock(Properties.class);
        when(props.getCreationTime()).thenReturn(1700000000000L);
        when(props.getAbsoluteExpiryTime()).thenReturn(1700000100000L);
        when(props.getGroupSequence()).thenReturn(-1L);

        Map<String, String> result = MessageViewCoercion.coercePropertiesToStrings(props);

        assertThat(result).containsEntry("creation_time", "1700000000000");
        assertThat(result).containsEntry("absolute_expiry_time", "1700000100000");
    }

    @Test
    void coerceMapToStringsPreservesNullValues() {
        Map<String, Object> source = new LinkedHashMap<>();
        source.put("a", null);
        source.put("b", 1);

        Map<String, String> coerced = MessageViewCoercion.coerceMapToStrings(source);

        assertThat(coerced).containsEntry("a", null).containsEntry("b", "1");
    }
}
