package io.github.lukaszsamson.spark.rabbitmq;

import com.rabbitmq.stream.Consumer;
import com.rabbitmq.stream.Message;
import com.rabbitmq.stream.MessageHandler;
import org.apache.spark.sql.connector.read.streaming.SupportsRealTimeRead.RecordStatus;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.concurrent.BlockingQueue;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

class RabbitMQPartitionReaderRealTimeCloseTest {

    @Test
    void nextWithTimeoutReturnsNoRecordWhenReaderAlreadyClosed() throws Exception {
        RabbitMQPartitionReader reader = newReader();

        // Simulate already initialized consumer and a normal close/cancel path.
        setPrivateField(reader, "consumer", new NoopConsumer());
        reader.close();

        RecordStatus status = reader.nextWithTimeout(50L);
        assertThat(status.hasRecord()).isFalse();
    }

    @Test
    void nextWithTimeoutReturnsNoRecordWhenFinishedAndConsumerClosed() throws Exception {
        RabbitMQPartitionReader reader = newReader();
        setPrivateField(reader, "consumer", new NoopConsumer());
        setPrivateField(reader, "finished", true);
        AtomicBoolean consumerClosed = (AtomicBoolean) getPrivateField(reader, "consumerClosed");
        consumerClosed.set(true);

        RecordStatus status = reader.nextWithTimeout(50L);
        assertThat(status.hasRecord()).isFalse();
    }

    @Test
    void nextWithTimeoutAdvancesObservedOffsetForPreStartSkippedRecords() throws Exception {
        RabbitMQPartitionReader reader = newReader(10L, 100L);
        setPrivateField(reader, "consumer", new NoopConsumer());

        BlockingQueue<BaseRabbitMQPartitionReader.QueuedMessage> queue = queue(reader);
        queue.add(queuedMessage(2L));
        queue.add(queuedMessage(9L));

        RecordStatus status = reader.nextWithTimeout(20L);
        assertThat(status.hasRecord()).isFalse();
        assertThat((long) getPrivateField(reader, "lastObservedOffset")).isEqualTo(9L);
    }

    @Test
    void nextWithTimeoutAdvancesObservedOffsetForDeduplicatedRecords() throws Exception {
        RabbitMQPartitionReader reader = newReader(0L, 100L);
        setPrivateField(reader, "consumer", new NoopConsumer());
        setPrivateField(reader, "lastEmittedOffset", 15L);
        setPrivateField(reader, "lastObservedOffset", 14L);

        BlockingQueue<BaseRabbitMQPartitionReader.QueuedMessage> queue = queue(reader);
        queue.add(queuedMessage(15L));

        RecordStatus status = reader.nextWithTimeout(20L);
        assertThat(status.hasRecord()).isFalse();
        assertThat((long) getPrivateField(reader, "lastObservedOffset")).isEqualTo(15L);
    }

    private static RabbitMQPartitionReader newReader() {
        return newReader(0L, 100L);
    }

    private static RabbitMQPartitionReader newReader(long startOffset, long endOffset) {
        RabbitMQInputPartition partition = new RabbitMQInputPartition(
                "test-stream", startOffset, endOffset, minimalOptions());
        return new RabbitMQPartitionReader(partition, partition.getOptions());
    }

    private static ConnectorOptions minimalOptions() {
        Map<String, String> opts = new LinkedHashMap<>();
        opts.put("endpoints", "localhost:5552");
        opts.put("stream", "test-stream");
        opts.put("pollTimeoutMs", "5");
        opts.put("maxWaitMs", "50");
        return new ConnectorOptions(opts);
    }

    private static Object getPrivateField(Object target, String fieldName) throws Exception {
        Field field = findField(target.getClass(), fieldName);
        field.setAccessible(true);
        return field.get(target);
    }

    private static void setPrivateField(Object target, String fieldName, Object value)
            throws Exception {
        Field field = findField(target.getClass(), fieldName);
        field.setAccessible(true);
        field.set(target, value);
    }

    private static Field findField(Class<?> type, String fieldName) throws NoSuchFieldException {
        Class<?> current = type;
        while (current != null) {
            try {
                return current.getDeclaredField(fieldName);
            } catch (NoSuchFieldException ignored) {
                current = current.getSuperclass();
            }
        }
        throw new NoSuchFieldException(fieldName);
    }

    @SuppressWarnings("unchecked")
    private static BlockingQueue<BaseRabbitMQPartitionReader.QueuedMessage> queue(
            RabbitMQPartitionReader reader) throws Exception {
        return (BlockingQueue<BaseRabbitMQPartitionReader.QueuedMessage>)
                getPrivateField(reader, "queue");
    }

    private static BaseRabbitMQPartitionReader.QueuedMessage queuedMessage(long offset) {
        Message message = mock(Message.class);
        MessageHandler.Context context = mock(MessageHandler.Context.class);
        return new BaseRabbitMQPartitionReader.QueuedMessage(message, offset, 0L, context);
    }

    private static final class NoopConsumer implements Consumer {
        @Override
        public void store(long offset) {
            // no-op
        }

        @Override
        public long storedOffset() {
            return 0L;
        }

        @Override
        public void close() {
            // no-op
        }
    }
}
