package com.rabbitmq.spark.connector;

import java.util.List;

/**
 * Test routing strategy that routes all messages to the first partition.
 * Used by IT-SINK-007.
 */
public class TestRoutingStrategy implements ConnectorRoutingStrategy {

    @Override
    public List<String> route(ConnectorMessageView message, Metadata metadata) {
        return List.of(metadata.partitions().get(0));
    }
}
