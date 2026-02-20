package com.rabbitmq.spark.connector;

import java.util.List;

/**
 * Routes using broker binding metadata lookup via {@link Metadata#route(String)}.
 */
public class TestMetadataRouteRoutingStrategy implements ConnectorRoutingStrategy {

    @Override
    public List<String> route(ConnectorMessageView message, Metadata metadata) {
        String routingKey = message.valueAtPath("application_properties.routing_key");
        if (routingKey == null || routingKey.isEmpty()) {
            return List.of();
        }
        return metadata.route(routingKey);
    }
}
