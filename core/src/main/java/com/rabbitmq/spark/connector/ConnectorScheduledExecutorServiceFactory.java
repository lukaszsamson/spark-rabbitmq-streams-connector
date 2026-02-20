package com.rabbitmq.spark.connector;

import java.util.concurrent.ScheduledExecutorService;

/**
 * Creates a custom {@link ScheduledExecutorService} for RabbitMQ stream
 * environment internals (batch scheduling, recovery, topology updates).
 */
public interface ConnectorScheduledExecutorServiceFactory {

    /**
     * Create a scheduled executor service instance.
     *
     * @param options connector options
     * @return executor service to use
     */
    ScheduledExecutorService create(ConnectorOptions options);
}

