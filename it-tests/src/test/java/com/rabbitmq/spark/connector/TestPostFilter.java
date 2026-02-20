package com.rabbitmq.spark.connector;

/**
 * Test post-filter that accepts messages whose body starts with "keep-".
 * Used by IT-FILTER-003.
 */
public class TestPostFilter implements ConnectorPostFilter {

    @Override
    public boolean accept(ConnectorMessageView message) {
        byte[] messageBody = message.getBody();
        if (messageBody == null) return false;
        return new String(messageBody).startsWith("keep-");
    }
}
