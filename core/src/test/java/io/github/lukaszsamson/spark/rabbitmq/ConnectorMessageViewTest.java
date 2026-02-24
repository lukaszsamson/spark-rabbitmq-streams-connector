package io.github.lukaszsamson.spark.rabbitmq;

import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ConnectorMessageViewTest {

    @Test
    void constructorDefensivelyCopiesBody() {
        byte[] body = new byte[]{1, 2, 3};
        ConnectorMessageView view = new ConnectorMessageView(body, null, null, null);

        body[0] = 9;

        assertThat(view.getBody()).containsExactly((byte) 1, (byte) 2, (byte) 3);
    }

    @Test
    void getBodyReturnsCopy() {
        ConnectorMessageView view = new ConnectorMessageView(new byte[]{1, 2, 3}, null, null, null);

        byte[] first = view.getBody();
        first[1] = 9;

        assertThat(view.getBody()).containsExactly((byte) 1, (byte) 2, (byte) 3);
    }

    @Test
    void constructorAcceptsNullValuedMapEntries() {
        Map<String, String> applicationProperties = new LinkedHashMap<>();
        applicationProperties.put("region", null);
        applicationProperties.put("tenant", "acme");

        ConnectorMessageView view = new ConnectorMessageView(
                new byte[]{1}, applicationProperties, Map.of(), Map.of());

        assertThat(view.getApplicationProperties())
                .containsEntry("region", null)
                .containsEntry("tenant", "acme");
    }

    @Test
    void mapGettersReturnImmutableViews() {
        ConnectorMessageView view = new ConnectorMessageView(
                new byte[]{1},
                new LinkedHashMap<>(Map.of("key", "value")),
                Map.of(),
                Map.of());

        assertThatThrownBy(() -> view.getApplicationProperties().put("x", "y"))
                .isInstanceOf(UnsupportedOperationException.class);
    }
}
