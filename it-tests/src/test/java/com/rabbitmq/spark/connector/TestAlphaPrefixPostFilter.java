package com.rabbitmq.spark.connector;

import java.nio.charset.StandardCharsets;

/**
 * Test post-filter that accepts messages whose body starts with "alpha-".
 */
public class TestAlphaPrefixPostFilter implements ConnectorPostFilter {
    private static final long serialVersionUID = 1L;

    @Override
    public boolean accept(ConnectorMessageView message) {
        byte[] messageBody = message.getBody();
        if (messageBody == null) {
            return false;
        }
        String body = new String(messageBody, StandardCharsets.UTF_8);
        return body.startsWith("alpha-");
    }
}
