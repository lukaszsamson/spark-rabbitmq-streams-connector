package com.rabbitmq.spark.connector;

import java.util.List;

/** Test fixture for extension class validation tests. */
public class TestRoutingStrategy implements ConnectorRoutingStrategy {
    private static final long serialVersionUID = 1L;

    @Override
    public List<String> route(ConnectorMessageView message, Metadata metadata) {
        return metadata.partitions().subList(0, 1);
    }
}
