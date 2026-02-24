package io.github.lukaszsamson.spark.rabbitmq;

import com.rabbitmq.stream.Message;
import com.rabbitmq.stream.MessageHandler;
import com.rabbitmq.stream.ObservationCollector;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Test-only observation collector factory.
 *
 * <p>Used by integration tests to verify that observationCollectorClass is
 * wired through connector options into the RabbitMQ stream client.
 */
public class TestObservationCollectorFactory implements ConnectorObservationCollectorFactory {

    private static final AtomicLong PRE_PUBLISH_COUNT = new AtomicLong(0);
    private static final AtomicLong PUBLISHED_COUNT = new AtomicLong(0);
    private static final AtomicLong SUBSCRIBE_COUNT = new AtomicLong(0);
    private static final AtomicLong HANDLE_COUNT = new AtomicLong(0);

    @Override
    public ObservationCollector<?> create(ConnectorOptions options) {
        return new CountingObservationCollector();
    }

    public static void reset() {
        PRE_PUBLISH_COUNT.set(0);
        PUBLISHED_COUNT.set(0);
        SUBSCRIBE_COUNT.set(0);
        HANDLE_COUNT.set(0);
    }

    public static long prePublishCount() {
        return PRE_PUBLISH_COUNT.get();
    }

    public static long publishedCount() {
        return PUBLISHED_COUNT.get();
    }

    public static long subscribeCount() {
        return SUBSCRIBE_COUNT.get();
    }

    public static long handleCount() {
        return HANDLE_COUNT.get();
    }

    private static final class CountingObservationCollector
            implements ObservationCollector<Object> {

        @Override
        public Object prePublish(String stream, Message message) {
            PRE_PUBLISH_COUNT.incrementAndGet();
            return null;
        }

        @Override
        public void published(Object context, Message message) {
            PUBLISHED_COUNT.incrementAndGet();
        }

        @Override
        public MessageHandler subscribe(MessageHandler handler) {
            SUBSCRIBE_COUNT.incrementAndGet();
            return (ctx, msg) -> {
                HANDLE_COUNT.incrementAndGet();
                handler.handle(ctx, msg);
            };
        }
    }
}
