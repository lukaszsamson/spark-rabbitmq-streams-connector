package com.rabbitmq.spark.connector;

import java.util.List;

/** Test fixture for extension class validation tests. */
public class TestRoutingStrategy implements ConnectorRoutingStrategy {
    private static final long serialVersionUID = 1L;

    @Override
    public List<String> route(String routingKey, List<String> partitions) {
        return partitions.subList(0, 1);
    }
}
