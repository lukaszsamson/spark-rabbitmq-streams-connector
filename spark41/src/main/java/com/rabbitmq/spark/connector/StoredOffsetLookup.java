package com.rabbitmq.spark.connector;

import com.rabbitmq.stream.Consumer;
import com.rabbitmq.stream.Environment;
import com.rabbitmq.stream.NoOffsetException;
import com.rabbitmq.stream.OffsetSpecification;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Looks up stored consumer offsets from the RabbitMQ broker.
 *
 * <p>Uses temporary named consumers with {@code noTrackingStrategy()} to query
 * the broker for the last stored offset per stream. This is the recommended
 * approach since {@link Environment} does not expose a direct
 * {@code queryOffset()} API.
 *
 * <p>Stored offsets in RabbitMQ are <em>last-processed</em> offsets.
 * The returned map contains <em>next</em> offsets ({@code stored + 1}).
 */
final class StoredOffsetLookup {

    private static final Logger LOG = LoggerFactory.getLogger(StoredOffsetLookup.class);

    private StoredOffsetLookup() {}

    /**
     * Look up stored offsets for the given streams.
     *
     * @param env          an active Environment
     * @param consumerName the consumer reference name
     * @param streams      streams to query
     * @return map of stream → next offset for streams that have a stored offset;
     *         streams with no stored offset are omitted
     */
    static Map<String, Long> lookup(Environment env, String consumerName,
                                     List<String> streams) {
        Map<String, Long> offsets = new LinkedHashMap<>();
        for (String stream : streams) {
            Long nextOffset = lookupStream(env, consumerName, stream);
            if (nextOffset != null) {
                offsets.put(stream, nextOffset);
            }
        }
        return offsets;
    }

    /**
     * Look up the stored offset for a single stream.
     *
     * @return the next offset (stored + 1), or {@code null} if no offset is stored
     */
    private static Long lookupStream(Environment env, String consumerName, String stream) {
        Consumer tempConsumer = null;
        try {
            tempConsumer = env.consumerBuilder()
                    .stream(stream)
                    .name(consumerName)
                    .noTrackingStrategy()
                    .offset(OffsetSpecification.next())
                    .messageHandler((ctx, msg) -> {
                        // No-op: we only need storedOffset(), not message consumption
                    })
                    .build();

            long stored = tempConsumer.storedOffset();
            LOG.debug("Found stored offset {} for consumer '{}' on stream '{}'",
                    stored, consumerName, stream);
            return stored + 1; // Convert last-processed to next offset
        } catch (NoOffsetException e) {
            LOG.debug("No stored offset for consumer '{}' on stream '{}'",
                    consumerName, stream);
            return null;
        } catch (Exception e) {
            // Fatal errors (auth, connection) should propagate
            LOG.warn("Failed to look up stored offset for consumer '{}' on stream '{}': {}",
                    consumerName, stream, e.getMessage());
            throw new IllegalStateException(
                    "Failed to look up stored offset for consumer '" + consumerName +
                            "' on stream '" + stream + "'", e);
        } finally {
            if (tempConsumer != null) {
                try {
                    tempConsumer.close();
                } catch (Exception e) {
                    LOG.debug("Error closing temp consumer for offset lookup on '{}'",
                            stream, e);
                }
            }
        }
    }
}
