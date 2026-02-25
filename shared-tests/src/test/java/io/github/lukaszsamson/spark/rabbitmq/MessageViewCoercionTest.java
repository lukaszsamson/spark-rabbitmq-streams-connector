package io.github.lukaszsamson.spark.rabbitmq;

import com.rabbitmq.stream.Message;
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
    void coerceMapToStringsPreservesNullValues() {
        Map<String, Object> source = new LinkedHashMap<>();
        source.put("a", null);
        source.put("b", 1);

        Map<String, String> coerced = MessageViewCoercion.coerceMapToStrings(source);

        assertThat(coerced).containsEntry("a", null).containsEntry("b", "1");
    }
}
