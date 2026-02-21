package io.github.lukaszsamson.spark.rabbitmq;

/**
 * Test hash function that forces all routing keys to the same hash bucket.
 */
public class TestHashFunction implements ConnectorHashFunction {

    @Override
    public int hash(String routingKey) {
        return 0;
    }
}
