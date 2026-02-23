package io.github.lukaszsamson.spark.rabbitmq;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

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
}
