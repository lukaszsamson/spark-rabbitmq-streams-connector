package com.rabbitmq.spark.connector;

import java.util.Map;

/**
 * Test post-filter that accepts messages whose body starts with "keep-".
 * Used by IT-FILTER-003.
 */
public class TestPostFilter implements ConnectorPostFilter {

    @Override
    public boolean accept(byte[] messageBody, Map<String, String> applicationProperties) {
        if (messageBody == null) return false;
        return new String(messageBody).startsWith("keep-");
    }
}
