package com.rabbitmq.spark.connector;

import io.micrometer.observation.ObservationRegistry;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.EventLoopGroup;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static org.assertj.core.api.Assertions.*;

class ConnectorOptionsTest {

    // ---- Helper ----

    /** Minimal valid options for a single-stream configuration. */
    private static Map<String, String> minimalStreamOptions() {
        Map<String, String> opts = new HashMap<>();
        opts.put("stream", "my-stream");
        opts.put("endpoints", "localhost:5552");
        return opts;
    }

    /** Minimal valid options for a superstream configuration. */
    private static Map<String, String> minimalSuperStreamOptions() {
        Map<String, String> opts = new HashMap<>();
        opts.put("superstream", "my-super-stream");
        opts.put("endpoints", "localhost:5552");
        return opts;
    }

    // ========================================================================
    // Common option parsing
    // ========================================================================

    @Nested
    class CommonParsing {

        @Test
        void parsesEndpoints() {
            var opts = new ConnectorOptions(minimalStreamOptions());
            assertThat(opts.getEndpoints()).isEqualTo("localhost:5552");
        }

        @Test
        void parsesUris() {
            var map = minimalStreamOptions();
            map.put("uris", "rabbitmq-stream://host1:5552,rabbitmq-stream://host2:5552");
            var opts = new ConnectorOptions(map);
            assertThat(opts.getUris()).isEqualTo(
                    "rabbitmq-stream://host1:5552,rabbitmq-stream://host2:5552");
        }

        @Test
        void parsesAuthOptions() {
            var map = minimalStreamOptions();
            map.put("username", "user1");
            map.put("password", "secret");
            map.put("vhost", "/my-vhost");
            var opts = new ConnectorOptions(map);
            assertThat(opts.getUsername()).isEqualTo("user1");
            assertThat(opts.getPassword()).isEqualTo("secret");
            assertThat(opts.getVhost()).isEqualTo("/my-vhost");
        }

        @Test
        void nullAuthOptionsByDefault() {
            var opts = new ConnectorOptions(minimalStreamOptions());
            assertThat(opts.getUsername()).isNull();
            assertThat(opts.getPassword()).isNull();
            assertThat(opts.getVhost()).isNull();
        }

        @Test
        void parsesTlsOptions() {
            var map = minimalStreamOptions();
            map.put("tls", "true");
            map.put("tls.truststore", "/path/to/ts.jks");
            map.put("tls.truststorePassword", "ts-pass");
            map.put("tls.keystore", "/path/to/ks.jks");
            map.put("tls.keystorePassword", "ks-pass");
            map.put("tls.trustAll", "true");
            var opts = new ConnectorOptions(map);
            assertThat(opts.isTls()).isTrue();
            assertThat(opts.getTlsTruststore()).isEqualTo("/path/to/ts.jks");
            assertThat(opts.getTlsTruststorePassword()).isEqualTo("ts-pass");
            assertThat(opts.getTlsKeystore()).isEqualTo("/path/to/ks.jks");
            assertThat(opts.getTlsKeystorePassword()).isEqualTo("ks-pass");
            assertThat(opts.isTlsTrustAll()).isTrue();
        }

        @Test
        void tlsDefaultsToFalse() {
            var opts = new ConnectorOptions(minimalStreamOptions());
            assertThat(opts.isTls()).isFalse();
            assertThat(opts.isTlsTrustAll()).isFalse();
        }

        @Test
        void parsesStreamMode() {
            var opts = new ConnectorOptions(minimalStreamOptions());
            assertThat(opts.isStreamMode()).isTrue();
            assertThat(opts.isSuperStreamMode()).isFalse();
            assertThat(opts.getStream()).isEqualTo("my-stream");
        }

        @Test
        void parsesSuperStreamMode() {
            var opts = new ConnectorOptions(minimalSuperStreamOptions());
            assertThat(opts.isStreamMode()).isFalse();
            assertThat(opts.isSuperStreamMode()).isTrue();
            assertThat(opts.getSuperStream()).isEqualTo("my-super-stream");
        }

        @Test
        void parsesConsumerName() {
            var map = minimalStreamOptions();
            map.put("consumerName", "my-consumer");
            var opts = new ConnectorOptions(map);
            assertThat(opts.getConsumerName()).isEqualTo("my-consumer");
        }

        @Test
        void consumerNameNullByDefault() {
            var opts = new ConnectorOptions(minimalStreamOptions());
            assertThat(opts.getConsumerName()).isNull();
        }

        @Test
        void parsesFailOnDataLoss() {
            var map = minimalStreamOptions();
            map.put("failOnDataLoss", "false");
            var opts = new ConnectorOptions(map);
            assertThat(opts.isFailOnDataLoss()).isFalse();
        }

        @Test
        void failOnDataLossDefaultsToTrue() {
            var opts = new ConnectorOptions(minimalStreamOptions());
            assertThat(opts.isFailOnDataLoss()).isTrue();
        }

        @Test
        void parsesAddressResolverClass() {
            var map = minimalStreamOptions();
            map.put("addressResolverClass", "com.example.MyResolver");
            var opts = new ConnectorOptions(map);
            assertThat(opts.getAddressResolverClass()).isEqualTo("com.example.MyResolver");
        }

        @Test
        void parsesAddressResolverClassFromLowercasedKey() {
            var map = minimalStreamOptions();
            map.put("addressresolverclass", "com.example.MyResolver");
            var opts = new ConnectorOptions(map);
            assertThat(opts.getAddressResolverClass()).isEqualTo("com.example.MyResolver");
        }

        @Test
        void parsesObservationCollectorClass() {
            var map = minimalStreamOptions();
            map.put("observationCollectorClass", TestObservationCollectorFactory.class.getName());
            var opts = new ConnectorOptions(map);
            assertThat(opts.getObservationCollectorClass())
                    .isEqualTo(TestObservationCollectorFactory.class.getName());
        }

        @Test
        void parsesObservationRegistryProviderClass() {
            var map = minimalStreamOptions();
            map.put("observationRegistryProviderClass",
                    TestObservationRegistryProvider.class.getName());
            var opts = new ConnectorOptions(map);
            assertThat(opts.getObservationRegistryProviderClass())
                    .isEqualTo(TestObservationRegistryProvider.class.getName());
        }

        @Test
        void parsesLazyAndNettyCustomizationOptions() {
            var map = minimalStreamOptions();
            map.put("lazyInitialization", "true");
            map.put("scheduledExecutorService", TestScheduledExecutorServiceFactory.class.getName());
            map.put("netty.eventLoopGroup", TestNettyEventLoopGroupFactory.class.getName());
            map.put("netty.byteBufAllocator", TestNettyByteBufAllocatorFactory.class.getName());
            map.put("netty.channelCustomizer", TestNettyChannelCustomizer.class.getName());
            map.put("netty.bootstrapCustomizer", TestNettyBootstrapCustomizer.class.getName());

            var opts = new ConnectorOptions(map);
            assertThat(opts.isLazyInitialization()).isTrue();
            assertThat(opts.getScheduledExecutorService())
                    .isEqualTo(TestScheduledExecutorServiceFactory.class.getName());
            assertThat(opts.getNettyEventLoopGroup())
                    .isEqualTo(TestNettyEventLoopGroupFactory.class.getName());
            assertThat(opts.getNettyByteBufAllocator())
                    .isEqualTo(TestNettyByteBufAllocatorFactory.class.getName());
            assertThat(opts.getNettyChannelCustomizer())
                    .isEqualTo(TestNettyChannelCustomizer.class.getName());
            assertThat(opts.getNettyBootstrapCustomizer())
                    .isEqualTo(TestNettyBootstrapCustomizer.class.getName());
        }

        @Test
        void parsesEnvironmentTuningOptions() {
            var map = minimalStreamOptions();
            map.put("environmentId", "spark-rmq-prod");
            map.put("rpcTimeoutMs", "15000");
            map.put("requestedHeartbeatSeconds", "30");
            map.put("forceReplicaForConsumers", "true");
            map.put("forceLeaderForProducers", "false");
            map.put("locatorConnectionCount", "3");
            map.put("recoveryBackOffDelayPolicy", "PT5S");
            map.put("topologyUpdateBackOffDelayPolicy", "PT5S,PT1S");
            map.put("maxProducersByConnection", "64");
            map.put("maxConsumersByConnection", "32");
            map.put("maxTrackingConsumersByConnection", "16");

            var opts = new ConnectorOptions(map);
            assertThat(opts.getEnvironmentId()).isEqualTo("spark-rmq-prod");
            assertThat(opts.getRpcTimeoutMs()).isEqualTo(15000L);
            assertThat(opts.getRequestedHeartbeatSeconds()).isEqualTo(30L);
            assertThat(opts.getForceReplicaForConsumers()).isEqualTo(Boolean.TRUE);
            assertThat(opts.getForceLeaderForProducers()).isEqualTo(Boolean.FALSE);
            assertThat(opts.getLocatorConnectionCount()).isEqualTo(3);
            assertThat(opts.getRecoveryBackOffDelayPolicy()).isEqualTo("PT5S");
            assertThat(opts.getTopologyUpdateBackOffDelayPolicy()).isEqualTo("PT5S,PT1S");
            assertThat(opts.getMaxProducersByConnection()).isEqualTo(64);
            assertThat(opts.getMaxConsumersByConnection()).isEqualTo(32);
            assertThat(opts.getMaxTrackingConsumersByConnection()).isEqualTo(16);
        }
    }

    // ========================================================================
    // Metadata fields parsing
    // ========================================================================

    @Nested
    class MetadataFieldsParsing {

        @Test
        void allFieldsByDefault() {
            var opts = new ConnectorOptions(minimalStreamOptions());
            assertThat(opts.getMetadataFields()).isEqualTo(MetadataField.ALL);
        }

        @Test
        void parsesSubset() {
            var map = minimalStreamOptions();
            map.put("metadataFields", "properties,creation_time");
            var opts = new ConnectorOptions(map);
            assertThat(opts.getMetadataFields()).containsExactlyInAnyOrder(
                    MetadataField.PROPERTIES, MetadataField.CREATION_TIME);
        }

        @Test
        void parsesEmptyString() {
            var map = minimalStreamOptions();
            map.put("metadataFields", "");
            var opts = new ConnectorOptions(map);
            assertThat(opts.getMetadataFields()).isEmpty();
        }

        @Test
        void toleratesWhitespace() {
            var map = minimalStreamOptions();
            map.put("metadataFields", " properties , routing_key ");
            var opts = new ConnectorOptions(map);
            assertThat(opts.getMetadataFields()).containsExactlyInAnyOrder(
                    MetadataField.PROPERTIES, MetadataField.ROUTING_KEY);
        }

        @Test
        void rejectsUnknownField() {
            var map = minimalStreamOptions();
            map.put("metadataFields", "properties,bogus");
            assertThatThrownBy(() -> new ConnectorOptions(map))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("bogus");
        }
    }

    // ========================================================================
    // Source option parsing
    // ========================================================================

    @Nested
    class SourceParsing {

        @Test
        void startingOffsetsDefaultsToEarliest() {
            var opts = new ConnectorOptions(minimalStreamOptions());
            assertThat(opts.getStartingOffsets()).isEqualTo(StartingOffsetsMode.EARLIEST);
        }

        @Test
        void parsesStartingOffsetsLatest() {
            var map = minimalStreamOptions();
            map.put("startingOffsets", "latest");
            var opts = new ConnectorOptions(map);
            assertThat(opts.getStartingOffsets()).isEqualTo(StartingOffsetsMode.LATEST);
        }

        @Test
        void parsesStartingOffsetsOffset() {
            var map = minimalStreamOptions();
            map.put("startingOffsets", "offset");
            map.put("startingOffset", "42");
            var opts = new ConnectorOptions(map);
            assertThat(opts.getStartingOffsets()).isEqualTo(StartingOffsetsMode.OFFSET);
            assertThat(opts.getStartingOffset()).isEqualTo(42L);
        }

        @Test
        void parsesStartingOffsetsTimestamp() {
            var map = minimalStreamOptions();
            map.put("startingOffsets", "timestamp");
            map.put("startingTimestamp", "1700000000000");
            var opts = new ConnectorOptions(map);
            assertThat(opts.getStartingOffsets()).isEqualTo(StartingOffsetsMode.TIMESTAMP);
            assertThat(opts.getStartingTimestamp()).isEqualTo(1700000000000L);
        }

        @Test
        void startingOffsetsCaseInsensitive() {
            var map = minimalStreamOptions();
            map.put("startingOffsets", "LATEST");
            var opts = new ConnectorOptions(map);
            assertThat(opts.getStartingOffsets()).isEqualTo(StartingOffsetsMode.LATEST);
        }

        @Test
        void rejectsInvalidStartingOffsets() {
            var map = minimalStreamOptions();
            map.put("startingOffsets", "bogus");
            assertThatThrownBy(() -> new ConnectorOptions(map))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("startingOffsets")
                    .hasMessageContaining("bogus");
        }

        @Test
        void endingOffsetsDefaultsToLatest() {
            var opts = new ConnectorOptions(minimalStreamOptions());
            assertThat(opts.getEndingOffsets()).isEqualTo(EndingOffsetsMode.LATEST);
        }

        @Test
        void parsesEndingOffsetsOffset() {
            var map = minimalStreamOptions();
            map.put("endingOffsets", "offset");
            map.put("endingOffset", "999");
            var opts = new ConnectorOptions(map);
            assertThat(opts.getEndingOffsets()).isEqualTo(EndingOffsetsMode.OFFSET);
            assertThat(opts.getEndingOffset()).isEqualTo(999L);
        }

        @Test
        void parsesMaxRecordsPerTrigger() {
            var map = minimalStreamOptions();
            map.put("maxRecordsPerTrigger", "5000");
            var opts = new ConnectorOptions(map);
            assertThat(opts.getMaxRecordsPerTrigger()).isEqualTo(5000L);
        }

        @Test
        void maxRecordsPerTriggerNullByDefault() {
            var opts = new ConnectorOptions(minimalStreamOptions());
            assertThat(opts.getMaxRecordsPerTrigger()).isNull();
        }

        @Test
        void parsesMaxBytesPerTrigger() {
            var map = minimalStreamOptions();
            map.put("maxBytesPerTrigger", "1048576");
            var opts = new ConnectorOptions(map);
            assertThat(opts.getMaxBytesPerTrigger()).isEqualTo(1048576L);
        }

        @Test
        void parsesMinPartitions() {
            var map = minimalStreamOptions();
            map.put("minPartitions", "8");
            var opts = new ConnectorOptions(map);
            assertThat(opts.getMinPartitions()).isEqualTo(8);
        }

        @Test
        void parsesServerSideOffsetTracking() {
            var map = minimalStreamOptions();
            map.put("serverSideOffsetTracking", "false");
            var opts = new ConnectorOptions(map);
            assertThat(opts.getServerSideOffsetTracking()).isFalse();
            assertThat(opts.isServerSideOffsetTracking(true)).isFalse();
            assertThat(opts.isServerSideOffsetTracking(false)).isFalse();
        }

        @Test
        void serverSideOffsetTrackingDefaultsBasedOnMode() {
            var opts = new ConnectorOptions(minimalStreamOptions());
            assertThat(opts.getServerSideOffsetTracking()).isNull();
            assertThat(opts.isServerSideOffsetTracking(true)).isTrue();
            assertThat(opts.isServerSideOffsetTracking(false)).isFalse();
        }

        @Test
        void parsesFilterValues() {
            var map = minimalStreamOptions();
            map.put("filterValues", "foo,bar,baz");
            var opts = new ConnectorOptions(map);
            assertThat(opts.getFilterValues()).containsExactly("foo", "bar", "baz");
        }

        @Test
        void filterValuesEmptyByDefault() {
            var opts = new ConnectorOptions(minimalStreamOptions());
            assertThat(opts.getFilterValues()).isEmpty();
        }

        @Test
        void parsesFilterBooleans() {
            var map = minimalStreamOptions();
            map.put("filterMatchUnfiltered", "true");
            map.put("filterWarningOnMismatch", "false");
            var opts = new ConnectorOptions(map);
            assertThat(opts.isFilterMatchUnfiltered()).isTrue();
            assertThat(opts.isFilterWarningOnMismatch()).isFalse();
        }

        @Test
        void filterBooleansDefaults() {
            var opts = new ConnectorOptions(minimalStreamOptions());
            assertThat(opts.isFilterMatchUnfiltered()).isFalse();
            assertThat(opts.isFilterWarningOnMismatch()).isTrue();
        }

        @Test
        void parsesReaderTuningOptions() {
            var map = minimalStreamOptions();
            map.put("pollTimeoutMs", "5000");
            map.put("maxWaitMs", "60000");
            map.put("callbackEnqueueTimeoutMs", "250");
            map.put("initialCredits", "3");
            map.put("queueCapacity", "20000");
            map.put("estimatedMessageSizeBytes", "2048");
            var opts = new ConnectorOptions(map);
            assertThat(opts.getPollTimeoutMs()).isEqualTo(5000L);
            assertThat(opts.getMaxWaitMs()).isEqualTo(60000L);
            assertThat(opts.getCallbackEnqueueTimeoutMs()).isEqualTo(250L);
            assertThat(opts.getInitialCredits()).isEqualTo(3);
            assertThat(opts.getQueueCapacity()).isEqualTo(20000);
            assertThat(opts.getEstimatedMessageSizeBytes()).isEqualTo(2048);
        }

        @Test
        void readerTuningDefaults() {
            var opts = new ConnectorOptions(minimalStreamOptions());
            assertThat(opts.getPollTimeoutMs()).isEqualTo(30_000L);
            assertThat(opts.getMaxWaitMs()).isEqualTo(300_000L);
            assertThat(opts.getCallbackEnqueueTimeoutMs()).isEqualTo(5_000L);
            assertThat(opts.getInitialCredits()).isEqualTo(1);
            assertThat(opts.getQueueCapacity()).isEqualTo(10_000);
            assertThat(opts.getEstimatedMessageSizeBytes()).isEqualTo(1024);
        }

        @Test
        void parsesSingleActiveConsumer() {
            var map = minimalStreamOptions();
            map.put("consumerName", "orders-consumer");
            map.put("singleActiveConsumer", "true");

            var opts = new ConnectorOptions(map);
            assertThat(opts.isSingleActiveConsumer()).isTrue();
        }

        @Test
        void singleActiveConsumerDefaultsToFalse() {
            var opts = new ConnectorOptions(minimalStreamOptions());
            assertThat(opts.isSingleActiveConsumer()).isFalse();
        }

        @Test
        void rejectsInvalidLong() {
            var map = minimalStreamOptions();
            map.put("startingOffset", "not-a-number");
            assertThatThrownBy(() -> new ConnectorOptions(map))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("startingOffset")
                    .hasMessageContaining("not-a-number");
        }

        @Test
        void rejectsInvalidBoolean() {
            var map = minimalStreamOptions();
            map.put("failOnDataLoss", "maybe");
            assertThatThrownBy(() -> new ConnectorOptions(map))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("failOnDataLoss")
                    .hasMessageContaining("maybe");
        }

        @Test
        void rejectsInvalidEndingOffsets() {
            var map = minimalStreamOptions();
            map.put("endingOffsets", "bogus");
            assertThatThrownBy(() -> new ConnectorOptions(map))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("endingOffsets")
                    .hasMessageContaining("bogus");
        }

        @Test
        void rejectsInvalidRoutingStrategy() {
            var map = minimalStreamOptions();
            map.put("routingStrategy", "invalid");
            assertThatThrownBy(() -> new ConnectorOptions(map))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("routingStrategy")
                    .hasMessageContaining("invalid");
        }

        @Test
        void rejectsInvalidCompression() {
            var map = minimalStreamOptions();
            map.put("compression", "brotli");
            assertThatThrownBy(() -> new ConnectorOptions(map))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("compression")
                    .hasMessageContaining("brotli");
        }

        @Test
        void rejectsInvalidIntegerMinPartitions() {
            var map = minimalStreamOptions();
            map.put("minPartitions", "abc");
            assertThatThrownBy(() -> new ConnectorOptions(map))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("minPartitions")
                    .hasMessageContaining("abc");
        }

        @Test
        void rejectsInvalidIntegerMaxInFlight() {
            var map = minimalStreamOptions();
            map.put("maxInFlight", "xyz");
            assertThatThrownBy(() -> new ConnectorOptions(map))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("maxInFlight")
                    .hasMessageContaining("xyz");
        }

        @Test
        void rejectsInvalidIntegerQueueCapacity() {
            var map = minimalStreamOptions();
            map.put("queueCapacity", "notanumber");
            assertThatThrownBy(() -> new ConnectorOptions(map))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("queueCapacity")
                    .hasMessageContaining("notanumber");
        }

        @Test
        void rejectsInvalidIntegerInitialCredits() {
            var map = minimalStreamOptions();
            map.put("initialCredits", "1.5");
            assertThatThrownBy(() -> new ConnectorOptions(map))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("initialCredits")
                    .hasMessageContaining("1.5");
        }

        @Test
        void rejectsRemovedBatchSizeOption() {
            var map = minimalStreamOptions();
            map.put("batchSize", "big");
            var opts = new ConnectorOptions(map);
            assertThatThrownBy(opts::validateForSink)
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("batchSize")
                    .hasMessageContaining("removed");
        }

        @Test
        void rejectsInvalidIntegerSubEntrySize() {
            var map = minimalStreamOptions();
            map.put("subEntrySize", "");
            assertThatThrownBy(() -> new ConnectorOptions(map))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("subEntrySize");
        }

        @Test
        void rejectsInvalidLongMaxRecordsPerTrigger() {
            var map = minimalStreamOptions();
            map.put("maxRecordsPerTrigger", "lots");
            assertThatThrownBy(() -> new ConnectorOptions(map))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("maxRecordsPerTrigger")
                    .hasMessageContaining("lots");
        }

        @Test
        void rejectsInvalidLongMaxBytesPerTrigger() {
            var map = minimalStreamOptions();
            map.put("maxBytesPerTrigger", "many");
            assertThatThrownBy(() -> new ConnectorOptions(map))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("maxBytesPerTrigger")
                    .hasMessageContaining("many");
        }

        @Test
        void rejectsInvalidLongPublisherConfirmTimeoutMs() {
            var map = minimalStreamOptions();
            map.put("publisherConfirmTimeoutMs", "slow");
            assertThatThrownBy(() -> new ConnectorOptions(map))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("publisherConfirmTimeoutMs")
                    .hasMessageContaining("slow");
        }

        @Test
        void parsesFilterPostFilterClass() {
            var map = minimalStreamOptions();
            map.put("filterPostFilterClass", "com.example.MyFilter");
            var opts = new ConnectorOptions(map);
            assertThat(opts.getFilterPostFilterClass()).isEqualTo("com.example.MyFilter");
        }

    }

    // ========================================================================
    // Sink option parsing
    // ========================================================================

    @Nested
    class SinkParsing {

        @Test
        void parsesProducerName() {
            var map = minimalStreamOptions();
            map.put("producerName", "my-producer");
            var opts = new ConnectorOptions(map);
            assertThat(opts.getProducerName()).isEqualTo("my-producer");
        }

        @Test
        void parsesConfirmOptions() {
            var map = minimalStreamOptions();
            map.put("publisherConfirmTimeoutMs", "5000");
            map.put("maxInFlight", "100");
            map.put("enqueueTimeoutMs", "3000");
            map.put("retryOnRecovery", "false");
            map.put("dynamicBatch", "true");
            var opts = new ConnectorOptions(map);
            assertThat(opts.getPublisherConfirmTimeoutMs()).isEqualTo(5000L);
            assertThat(opts.getMaxInFlight()).isEqualTo(100);
            assertThat(opts.getEnqueueTimeoutMs()).isEqualTo(3000L);
            assertThat(opts.getRetryOnRecovery()).isEqualTo(Boolean.FALSE);
            assertThat(opts.getDynamicBatch()).isEqualTo(Boolean.TRUE);
        }

        @Test
        void enqueueTimeoutDefault() {
            var opts = new ConnectorOptions(minimalStreamOptions());
            assertThat(opts.getEnqueueTimeoutMs()).isEqualTo(10_000L);
        }

        @Test
        void parsesBatchingOptions() {
            var map = minimalStreamOptions();
            map.put("batchPublishingDelayMs", "200");
            map.put("subEntrySize", "10");
            map.put("dynamicBatch", "true");
            var opts = new ConnectorOptions(map);
            assertThat(opts.getBatchPublishingDelayMs()).isEqualTo(200L);
            assertThat(opts.getSubEntrySize()).isEqualTo(10);
            assertThat(opts.getDynamicBatch()).isEqualTo(Boolean.TRUE);
        }

        @Test
        void parsesCompression() {
            for (String name : new String[]{"gzip", "snappy", "lz4", "zstd", "none"}) {
                var map = minimalStreamOptions();
                map.put("compression", name);
                var opts = new ConnectorOptions(map);
                assertThat(opts.getCompression())
                        .isEqualTo(CompressionType.fromString(name));
            }
        }

        @Test
        void compressionDefaultsToNone() {
            var opts = new ConnectorOptions(minimalStreamOptions());
            assertThat(opts.getCompression()).isEqualTo(CompressionType.NONE);
        }

        @Test
        void parsesRoutingStrategy() {
            for (String name : new String[]{"hash", "key", "custom"}) {
                var map = minimalStreamOptions();
                map.put("routingStrategy", name);
                if ("custom".equals(name)) {
                    map.put("partitionerClass", TestRoutingStrategy.class.getName());
                }
                var opts = new ConnectorOptions(map);
                assertThat(opts.getRoutingStrategy())
                        .isEqualTo(RoutingStrategyType.fromString(name));
            }
        }

        @Test
        void parsesHashFunctionClass() {
            var map = minimalSuperStreamOptions();
            map.put("routingStrategy", "hash");
            map.put("hashFunctionClass", TestHashFunction.class.getName());
            var opts = new ConnectorOptions(map);
            assertThat(opts.getHashFunctionClass()).isEqualTo(TestHashFunction.class.getName());
        }

        @Test
        void routingStrategyDefaultsToHash() {
            var opts = new ConnectorOptions(minimalStreamOptions());
            assertThat(opts.getRoutingStrategy()).isEqualTo(RoutingStrategyType.HASH);
        }

        @Test
        void parsesFilterValuePath() {
            var map = minimalStreamOptions();
            map.put("filterValuePath", "application_properties.region");
            var opts = new ConnectorOptions(map);
            assertThat(opts.getFilterValuePath()).isEqualTo("application_properties.region");
        }

        @Test
        void parsesFilterValueExtractorClass() {
            var map = minimalStreamOptions();
            map.put("filterValueExtractorClass", TestFilterValueExtractor.class.getName());
            var opts = new ConnectorOptions(map);
            assertThat(opts.getFilterValueExtractorClass()).isEqualTo(
                    TestFilterValueExtractor.class.getName());
        }

        @Test
        void parsesCompressionCodecFactoryClass() {
            var map = minimalStreamOptions();
            map.put("compressionCodecFactoryClass", TestCompressionCodecFactory.class.getName());
            var opts = new ConnectorOptions(map);
            assertThat(opts.getCompressionCodecFactoryClass())
                    .isEqualTo(TestCompressionCodecFactory.class.getName());
        }

        @Test
        void parsesIgnoreUnknownColumns() {
            var map = minimalStreamOptions();
            map.put("ignoreUnknownColumns", "true");
            var opts = new ConnectorOptions(map);
            assertThat(opts.isIgnoreUnknownColumns()).isTrue();
        }

        @Test
        void ignoreUnknownColumnsDefaultsFalse() {
            var opts = new ConnectorOptions(minimalStreamOptions());
            assertThat(opts.isIgnoreUnknownColumns()).isFalse();
        }
    }

    // ========================================================================
    // Resource management parsing
    // ========================================================================

    @Nested
    class ResourceManagementParsing {

        @Test
        void environmentIdleTimeoutDefault() {
            var opts = new ConnectorOptions(minimalStreamOptions());
            assertThat(opts.getEnvironmentIdleTimeoutMs()).isEqualTo(60_000L);
        }

        @Test
        void parsesCustomEnvironmentIdleTimeout() {
            var map = minimalStreamOptions();
            map.put("environmentIdleTimeoutMs", "120000");
            var opts = new ConnectorOptions(map);
            assertThat(opts.getEnvironmentIdleTimeoutMs()).isEqualTo(120_000L);
        }

        @Test
        void parsesZeroIdleTimeout() {
            var map = minimalStreamOptions();
            map.put("environmentIdleTimeoutMs", "0");
            var opts = new ConnectorOptions(map);
            assertThat(opts.getEnvironmentIdleTimeoutMs()).isEqualTo(0L);
        }
    }

    // ========================================================================
    // Common validation
    // ========================================================================

    @Nested
    class CommonValidation {

        @Test
        void validMinimalStreamConfig() {
            var opts = new ConnectorOptions(minimalStreamOptions());
            assertThatCode(opts::validateCommon).doesNotThrowAnyException();
        }

        @Test
        void validMinimalSuperStreamConfig() {
            var opts = new ConnectorOptions(minimalSuperStreamOptions());
            assertThatCode(opts::validateCommon).doesNotThrowAnyException();
        }

        @Test
        void validWithUrisInsteadOfEndpoints() {
            var map = new HashMap<String, String>();
            map.put("stream", "my-stream");
            map.put("uris", "rabbitmq-stream://localhost:5552");
            var opts = new ConnectorOptions(map);
            assertThatCode(opts::validateCommon).doesNotThrowAnyException();
        }

        @Test
        void rejectsBothStreamAndSuperStream() {
            var map = minimalStreamOptions();
            map.put("superstream", "my-super-stream");
            var opts = new ConnectorOptions(map);
            assertThatThrownBy(opts::validateCommon)
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("stream")
                    .hasMessageContaining("superstream")
                    .hasMessageContaining("both");
        }

        @Test
        void rejectsNeitherStreamNorSuperStream() {
            var map = new HashMap<String, String>();
            map.put("endpoints", "localhost:5552");
            var opts = new ConnectorOptions(map);
            assertThatThrownBy(opts::validateCommon)
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("stream")
                    .hasMessageContaining("superstream")
                    .hasMessageContaining("neither");
        }

        @Test
        void rejectsNoEndpointsOrUris() {
            var map = new HashMap<String, String>();
            map.put("stream", "my-stream");
            var opts = new ConnectorOptions(map);
            assertThatThrownBy(opts::validateCommon)
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("endpoints")
                    .hasMessageContaining("uris");
        }

        @Test
        void rejectsTrustAllWithoutTls() {
            var map = minimalStreamOptions();
            map.put("tls.trustAll", "true");
            var opts = new ConnectorOptions(map);
            assertThatThrownBy(opts::validateCommon)
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("tls.trustAll")
                    .hasMessageContaining("tls");
        }

        @Test
        void allowsTrustAllWithTls() {
            var map = minimalStreamOptions();
            map.put("tls", "true");
            map.put("tls.trustAll", "true");
            var opts = new ConnectorOptions(map);
            assertThatCode(opts::validateCommon).doesNotThrowAnyException();
        }

        @Test
        void treatsEmptyStreamAsAbsent() {
            var map = new HashMap<String, String>();
            map.put("stream", "");
            map.put("superstream", "my-super-stream");
            map.put("endpoints", "localhost:5552");
            var opts = new ConnectorOptions(map);
            assertThatCode(opts::validateCommon).doesNotThrowAnyException();
            assertThat(opts.isSuperStreamMode()).isTrue();
        }

        @Test
        void rejectsInvalidAddressResolverClassNotFound() {
            var map = minimalStreamOptions();
            map.put("addressResolverClass", "com.nonexistent.Resolver");
            var opts = new ConnectorOptions(map);
            assertThatThrownBy(opts::validateCommon)
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("addressResolverClass")
                    .hasMessageContaining("not found");
        }

        @Test
        void rejectsAddressResolverClassWrongType() {
            var map = minimalStreamOptions();
            map.put("addressResolverClass", "java.lang.String");
            var opts = new ConnectorOptions(map);
            assertThatThrownBy(opts::validateCommon)
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("addressResolverClass")
                    .hasMessageContaining("does not implement");
        }

        @Test
        void rejectsInvalidObservationCollectorClassNotFound() {
            var map = minimalStreamOptions();
            map.put("observationCollectorClass", "com.nonexistent.CollectorFactory");
            var opts = new ConnectorOptions(map);
            assertThatThrownBy(opts::validateCommon)
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("observationCollectorClass")
                    .hasMessageContaining("not found");
        }

        @Test
        void rejectsObservationCollectorClassWrongType() {
            var map = minimalStreamOptions();
            map.put("observationCollectorClass", "java.lang.String");
            var opts = new ConnectorOptions(map);
            assertThatThrownBy(opts::validateCommon)
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("observationCollectorClass")
                    .hasMessageContaining("does not implement");
        }

        @Test
        void rejectsInvalidObservationRegistryProviderClassNotFound() {
            var map = minimalStreamOptions();
            map.put("observationRegistryProviderClass", "com.nonexistent.RegistryProvider");
            var opts = new ConnectorOptions(map);
            assertThatThrownBy(opts::validateCommon)
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("observationRegistryProviderClass")
                    .hasMessageContaining("not found");
        }

        @Test
        void rejectsObservationRegistryProviderClassWrongType() {
            var map = minimalStreamOptions();
            map.put("observationRegistryProviderClass", "java.lang.String");
            var opts = new ConnectorOptions(map);
            assertThatThrownBy(opts::validateCommon)
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("observationRegistryProviderClass")
                    .hasMessageContaining("does not implement");
        }

        @Test
        void rejectsInvalidCompressionCodecFactoryClassNotFound() {
            var map = minimalStreamOptions();
            map.put("compressionCodecFactoryClass", "com.nonexistent.CodecFactory");
            var opts = new ConnectorOptions(map);
            assertThatThrownBy(opts::validateCommon)
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("compressionCodecFactoryClass")
                    .hasMessageContaining("not found");
        }

        @Test
        void rejectsCompressionCodecFactoryClassWrongType() {
            var map = minimalStreamOptions();
            map.put("compressionCodecFactoryClass", "java.lang.String");
            var opts = new ConnectorOptions(map);
            assertThatThrownBy(opts::validateCommon)
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("compressionCodecFactoryClass")
                    .hasMessageContaining("does not implement");
        }

        @Test
        void rejectsScheduledExecutorServiceFactoryWrongType() {
            var map = minimalStreamOptions();
            map.put("scheduledExecutorService", "java.lang.String");
            var opts = new ConnectorOptions(map);
            assertThatThrownBy(opts::validateCommon)
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("scheduledExecutorService")
                    .hasMessageContaining("does not implement");
        }

        @Test
        void rejectsNettyEventLoopGroupFactoryWrongType() {
            var map = minimalStreamOptions();
            map.put("netty.eventLoopGroup", "java.lang.String");
            var opts = new ConnectorOptions(map);
            assertThatThrownBy(opts::validateCommon)
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("netty.eventLoopGroup")
                    .hasMessageContaining("does not implement");
        }

        @Test
        void rejectsNonPositiveRpcTimeout() {
            var map = minimalStreamOptions();
            map.put("rpcTimeoutMs", "0");
            var opts = new ConnectorOptions(map);
            assertThatThrownBy(opts::validateCommon)
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("rpcTimeoutMs");
        }

        @Test
        void rejectsNonPositiveRequestedHeartbeat() {
            var map = minimalStreamOptions();
            map.put("requestedHeartbeatSeconds", "-1");
            var opts = new ConnectorOptions(map);
            assertThatThrownBy(opts::validateCommon)
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("requestedHeartbeatSeconds");
        }

        @Test
        void rejectsNonPositiveLocatorConnectionCount() {
            var map = minimalStreamOptions();
            map.put("locatorConnectionCount", "0");
            var opts = new ConnectorOptions(map);
            assertThatThrownBy(opts::validateCommon)
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("locatorConnectionCount");
        }

        @Test
        void rejectsNonPositiveMaxProducersByConnection() {
            var map = minimalStreamOptions();
            map.put("maxProducersByConnection", "0");
            var opts = new ConnectorOptions(map);
            assertThatThrownBy(opts::validateCommon)
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("maxProducersByConnection");
        }

        @Test
        void rejectsNonPositiveMaxConsumersByConnection() {
            var map = minimalStreamOptions();
            map.put("maxConsumersByConnection", "-1");
            var opts = new ConnectorOptions(map);
            assertThatThrownBy(opts::validateCommon)
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("maxConsumersByConnection");
        }

        @Test
        void rejectsNonPositiveMaxTrackingConsumersByConnection() {
            var map = minimalStreamOptions();
            map.put("maxTrackingConsumersByConnection", "0");
            var opts = new ConnectorOptions(map);
            assertThatThrownBy(opts::validateCommon)
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("maxTrackingConsumersByConnection");
        }

        @Test
        void rejectsRemovedFilterValueColumnOption() {
            var map = minimalStreamOptions();
            map.put("filterValueColumn", "region");
            var opts = new ConnectorOptions(map);
            assertThatThrownBy(opts::validateCommon)
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("filterValueColumn")
                    .hasMessageContaining("removed");
        }
    }

    // ========================================================================
    // Source validation
    // ========================================================================

    @Nested
    class SourceValidation {

        @Test
        void validSourceConfig() {
            var opts = new ConnectorOptions(minimalStreamOptions());
            assertThatCode(opts::validateForSource).doesNotThrowAnyException();
        }

        @Test
        void rejectsOffsetModeWithoutStartingOffset() {
            var map = minimalStreamOptions();
            map.put("startingOffsets", "offset");
            var opts = new ConnectorOptions(map);
            assertThatThrownBy(opts::validateForSource)
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("startingOffset")
                    .hasMessageContaining("required");
        }

        @Test
        void rejectsTimestampModeWithoutStartingTimestamp() {
            var map = minimalStreamOptions();
            map.put("startingOffsets", "timestamp");
            var opts = new ConnectorOptions(map);
            assertThatThrownBy(opts::validateForSource)
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("startingTimestamp")
                    .hasMessageContaining("required");
        }

        @Test
        void rejectsEndingOffsetModeWithoutEndingOffset() {
            var map = minimalStreamOptions();
            map.put("endingOffsets", "offset");
            var opts = new ConnectorOptions(map);
            assertThatThrownBy(opts::validateForSource)
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("endingOffset")
                    .hasMessageContaining("required");
        }

        @Test
        void rejectsNegativeStartingOffset() {
            var map = minimalStreamOptions();
            map.put("startingOffsets", "offset");
            map.put("startingOffset", "-1");
            var opts = new ConnectorOptions(map);
            assertThatThrownBy(opts::validateForSource)
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("startingOffset")
                    .hasMessageContaining(">= 0");
        }

        @Test
        void rejectsNonPositiveStartingTimestamp() {
            var map = minimalStreamOptions();
            map.put("startingOffsets", "timestamp");
            map.put("startingTimestamp", "0");
            var opts = new ConnectorOptions(map);
            assertThatThrownBy(opts::validateForSource)
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("startingTimestamp")
                    .hasMessageContaining("> 0");
        }

        @Test
        void rejectsNonPositiveMaxRecordsPerTrigger() {
            var map = minimalStreamOptions();
            map.put("maxRecordsPerTrigger", "0");
            var opts = new ConnectorOptions(map);
            assertThatThrownBy(opts::validateForSource)
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("maxRecordsPerTrigger");
        }

        @Test
        void rejectsNonPositiveMaxBytesPerTrigger() {
            var map = minimalStreamOptions();
            map.put("maxBytesPerTrigger", "-100");
            var opts = new ConnectorOptions(map);
            assertThatThrownBy(opts::validateForSource)
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("maxBytesPerTrigger");
        }

        @Test
        void rejectsNonPositiveMinPartitions() {
            var map = minimalStreamOptions();
            map.put("minPartitions", "0");
            var opts = new ConnectorOptions(map);
            assertThatThrownBy(opts::validateForSource)
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("minPartitions");
        }

        @Test
        void rejectsNonPositivePollTimeoutMs() {
            var map = minimalStreamOptions();
            map.put("pollTimeoutMs", "0");
            var opts = new ConnectorOptions(map);
            assertThatThrownBy(opts::validateForSource)
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("pollTimeoutMs");
        }

        @Test
        void rejectsNonPositiveMaxWaitMs() {
            var map = minimalStreamOptions();
            map.put("maxWaitMs", "-1");
            var opts = new ConnectorOptions(map);
            assertThatThrownBy(opts::validateForSource)
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("maxWaitMs");
        }

        @Test
        void rejectsNegativeCallbackEnqueueTimeoutMs() {
            var map = minimalStreamOptions();
            map.put("callbackEnqueueTimeoutMs", "-1");
            var opts = new ConnectorOptions(map);
            assertThatThrownBy(opts::validateForSource)
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("callbackEnqueueTimeoutMs");
        }

        @Test
        void allowsZeroCallbackEnqueueTimeoutMs() {
            var map = minimalStreamOptions();
            map.put("callbackEnqueueTimeoutMs", "0");
            var opts = new ConnectorOptions(map);
            assertThatCode(opts::validateForSource).doesNotThrowAnyException();
        }

        @Test
        void rejectsNonPositiveInitialCredits() {
            var map = minimalStreamOptions();
            map.put("initialCredits", "0");
            var opts = new ConnectorOptions(map);
            assertThatThrownBy(opts::validateForSource)
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("initialCredits");
        }

        @Test
        void rejectsNonPositiveQueueCapacity() {
            var map = minimalStreamOptions();
            map.put("queueCapacity", "0");
            var opts = new ConnectorOptions(map);
            assertThatThrownBy(opts::validateForSource)
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("queueCapacity");
        }

        @Test
        void rejectsNonPositiveEstimatedMessageSize() {
            var map = minimalStreamOptions();
            map.put("estimatedMessageSizeBytes", "-1");
            var opts = new ConnectorOptions(map);
            assertThatThrownBy(opts::validateForSource)
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("estimatedMessageSizeBytes");
        }

        @Test
        void rejectsSingleActiveConsumerWithoutConsumerName() {
            var map = minimalStreamOptions();
            map.put("singleActiveConsumer", "true");
            var opts = new ConnectorOptions(map);
            assertThatThrownBy(opts::validateForSource)
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("consumerName")
                    .hasMessageContaining("singleActiveConsumer");
        }

        @Test
        void acceptsSingleActiveConsumerWithConsumerName() {
            var map = minimalStreamOptions();
            map.put("singleActiveConsumer", "true");
            map.put("consumerName", "orders-consumer");
            var opts = new ConnectorOptions(map);
            assertThatCode(opts::validateForSource).doesNotThrowAnyException();
        }

        @Test
        void acceptsValidOffsetConfig() {
            var map = minimalStreamOptions();
            map.put("startingOffsets", "offset");
            map.put("startingOffset", "100");
            map.put("endingOffsets", "offset");
            map.put("endingOffset", "200");
            var opts = new ConnectorOptions(map);
            assertThatCode(opts::validateForSource).doesNotThrowAnyException();
        }

        @Test
        void acceptsValidTimestampConfig() {
            var map = minimalStreamOptions();
            map.put("startingOffsets", "timestamp");
            map.put("startingTimestamp", "1700000000000");
            var opts = new ConnectorOptions(map);
            assertThatCode(opts::validateForSource).doesNotThrowAnyException();
        }

        @Test
        void sourceValidationAlsoValidatesCommon() {
            var map = new HashMap<String, String>();
            map.put("stream", "s");
            // missing endpoints/uris
            var opts = new ConnectorOptions(map);
            assertThatThrownBy(opts::validateForSource)
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("endpoints");
        }

        @Test
        void rejectsInvalidFilterPostFilterClassNotFound() {
            var map = minimalStreamOptions();
            map.put("filterPostFilterClass", "com.nonexistent.Filter");
            var opts = new ConnectorOptions(map);
            assertThatThrownBy(opts::validateForSource)
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("filterPostFilterClass")
                    .hasMessageContaining("not found");
        }

        @Test
        void rejectsFilterPostFilterClassWrongType() {
            var map = minimalStreamOptions();
            map.put("filterPostFilterClass", "java.lang.String");
            var opts = new ConnectorOptions(map);
            assertThatThrownBy(opts::validateForSource)
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("filterPostFilterClass")
                    .hasMessageContaining("does not implement");
        }

        @Test
        void rejectsRemovedFilterPostFilterClassV2Option() {
            var map = minimalStreamOptions();
            map.put("filterPostFilterClassV2", "com.example.MyFilter");
            var opts = new ConnectorOptions(map);
            assertThatThrownBy(opts::validateForSource)
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("filterPostFilterClassV2")
                    .hasMessageContaining("removed");
        }

        @Test
        void rejectsNegativeEndingOffset() {
            var map = minimalStreamOptions();
            map.put("endingOffsets", "offset");
            map.put("endingOffset", "-5");
            var opts = new ConnectorOptions(map);
            assertThatThrownBy(opts::validateForSource)
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("endingOffset")
                    .hasMessageContaining(">= 0");
        }
    }

    // ========================================================================
    // Sink validation
    // ========================================================================

    @Nested
    class SinkValidation {

        @Test
        void validSinkConfig() {
            var opts = new ConnectorOptions(minimalStreamOptions());
            assertThatCode(opts::validateForSink).doesNotThrowAnyException();
        }

        @Test
        void rejectsCompressionWithoutSubEntrySize() {
            var map = minimalStreamOptions();
            map.put("compression", "gzip");
            var opts = new ConnectorOptions(map);
            assertThatThrownBy(opts::validateForSink)
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("compression")
                    .hasMessageContaining("subEntrySize");
        }

        @Test
        void rejectsCompressionWithSubEntrySizeOne() {
            var map = minimalStreamOptions();
            map.put("compression", "snappy");
            map.put("subEntrySize", "1");
            var opts = new ConnectorOptions(map);
            assertThatThrownBy(opts::validateForSink)
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("subEntrySize");
        }

        @Test
        void acceptsCompressionWithSubEntrySizeGreaterThanOne() {
            var map = minimalStreamOptions();
            map.put("compression", "lz4");
            map.put("subEntrySize", "10");
            var opts = new ConnectorOptions(map);
            assertThatCode(opts::validateForSink).doesNotThrowAnyException();
        }

        @Test
        void rejectsCustomRoutingWithoutPartitionerClass() {
            var map = minimalStreamOptions();
            map.put("routingStrategy", "custom");
            var opts = new ConnectorOptions(map);
            assertThatThrownBy(opts::validateForSink)
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("partitionerClass")
                    .hasMessageContaining("required")
                    .hasMessageContaining("custom");
        }

        @Test
        void acceptsCustomRoutingWithPartitionerClass() {
            var map = minimalStreamOptions();
            map.put("routingStrategy", "custom");
            map.put("partitionerClass", TestRoutingStrategy.class.getName());
            var opts = new ConnectorOptions(map);
            assertThatCode(opts::validateForSink).doesNotThrowAnyException();
        }

        @Test
        void rejectsNonPositiveMaxInFlight() {
            var map = minimalStreamOptions();
            map.put("maxInFlight", "0");
            var opts = new ConnectorOptions(map);
            assertThatThrownBy(opts::validateForSink)
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("maxInFlight");
        }

        @Test
        void acceptsZeroEnqueueTimeoutMs() {
            var map = minimalStreamOptions();
            map.put("enqueueTimeoutMs", "0");
            var opts = new ConnectorOptions(map);
            assertThatCode(opts::validateForSink).doesNotThrowAnyException();
        }

        @Test
        void rejectsNegativeEnqueueTimeoutMs() {
            var map = minimalStreamOptions();
            map.put("enqueueTimeoutMs", "-1");
            var opts = new ConnectorOptions(map);
            assertThatThrownBy(opts::validateForSink)
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("enqueueTimeoutMs");
        }

        @Test
        void rejectsBatchSizeOption() {
            var map = minimalStreamOptions();
            map.put("batchSize", "10");
            var opts = new ConnectorOptions(map);
            assertThatThrownBy(opts::validateForSink)
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("batchSize")
                    .hasMessageContaining("removed");
        }

        @Test
        void rejectsNonPositiveSubEntrySize() {
            var map = minimalStreamOptions();
            map.put("subEntrySize", "0");
            var opts = new ConnectorOptions(map);
            assertThatThrownBy(opts::validateForSink)
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("subEntrySize");
        }

        @Test
        void acceptsZeroPublisherConfirmTimeout() {
            var map = minimalStreamOptions();
            map.put("publisherConfirmTimeoutMs", "0");
            var opts = new ConnectorOptions(map);
            assertThatCode(opts::validateForSink).doesNotThrowAnyException();
        }

        @Test
        void rejectsNegativePublisherConfirmTimeout() {
            var map = minimalStreamOptions();
            map.put("publisherConfirmTimeoutMs", "-1");
            var opts = new ConnectorOptions(map);
            assertThatThrownBy(opts::validateForSink)
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("publisherConfirmTimeoutMs");
        }

        @Test
        void sinkValidationAlsoValidatesCommon() {
            var map = new HashMap<String, String>();
            map.put("stream", "s");
            // missing endpoints/uris
            var opts = new ConnectorOptions(map);
            assertThatThrownBy(opts::validateForSink)
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("endpoints");
        }

        @Test
        void rejectsInvalidPartitionerClassNotFound() {
            var map = minimalStreamOptions();
            map.put("routingStrategy", "custom");
            map.put("partitionerClass", "com.nonexistent.Router");
            var opts = new ConnectorOptions(map);
            assertThatThrownBy(opts::validateForSink)
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("partitionerClass")
                    .hasMessageContaining("not found");
        }

        @Test
        void rejectsHashFunctionClassOutsideSuperStreamMode() {
            var map = minimalStreamOptions();
            map.put("routingStrategy", "hash");
            map.put("hashFunctionClass", TestHashFunction.class.getName());
            var opts = new ConnectorOptions(map);
            assertThatThrownBy(opts::validateForSink)
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("hashFunctionClass")
                    .hasMessageContaining("superstream");
        }

        @Test
        void rejectsHashFunctionClassWhenRoutingStrategyIsNotHash() {
            var map = minimalSuperStreamOptions();
            map.put("routingStrategy", "key");
            map.put("hashFunctionClass", TestHashFunction.class.getName());
            var opts = new ConnectorOptions(map);
            assertThatThrownBy(opts::validateForSink)
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("hashFunctionClass")
                    .hasMessageContaining("routingStrategy")
                    .hasMessageContaining("hash");
        }

        @Test
        void rejectsInvalidHashFunctionClassNotFound() {
            var map = minimalSuperStreamOptions();
            map.put("routingStrategy", "hash");
            map.put("hashFunctionClass", "com.nonexistent.HashFunction");
            var opts = new ConnectorOptions(map);
            assertThatThrownBy(opts::validateForSink)
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("hashFunctionClass")
                    .hasMessageContaining("not found");
        }

        @Test
        void rejectsHashFunctionClassWrongType() {
            var map = minimalSuperStreamOptions();
            map.put("routingStrategy", "hash");
            map.put("hashFunctionClass", "java.lang.String");
            var opts = new ConnectorOptions(map);
            assertThatThrownBy(opts::validateForSink)
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("hashFunctionClass")
                    .hasMessageContaining("does not implement");
        }

        @Test
        void rejectsPartitionerClassWrongType() {
            var map = minimalStreamOptions();
            map.put("routingStrategy", "custom");
            map.put("partitionerClass", "java.lang.String");
            var opts = new ConnectorOptions(map);
            assertThatThrownBy(opts::validateForSink)
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("partitionerClass")
                    .hasMessageContaining("does not implement");
        }

        @Test
        void rejectsNonPositiveBatchPublishingDelay() {
            var map = minimalStreamOptions();
            map.put("batchPublishingDelayMs", "-10");
            var opts = new ConnectorOptions(map);
            assertThatThrownBy(opts::validateForSink)
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("batchPublishingDelayMs");
        }

        @Test
        void rejectsBothFilterValuePathAndExtractorClass() {
            var map = minimalStreamOptions();
            map.put("filterValuePath", "application_properties.region");
            map.put("filterValueExtractorClass", TestFilterValueExtractor.class.getName());
            var opts = new ConnectorOptions(map);
            assertThatThrownBy(opts::validateForSink)
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("Only one")
                    .hasMessageContaining("filterValuePath")
                    .hasMessageContaining("filterValueExtractorClass");
        }

        @Test
        void rejectsUnknownFilterValuePathRoot() {
            var map = minimalStreamOptions();
            map.put("filterValuePath", "headers.region");
            var opts = new ConnectorOptions(map);
            assertThatThrownBy(opts::validateForSink)
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("filterValuePath")
                    .hasMessageContaining("root");
        }

        @Test
        void rejectsInvalidFilterValueExtractorClassType() {
            var map = minimalStreamOptions();
            map.put("filterValueExtractorClass", "java.lang.String");
            var opts = new ConnectorOptions(map);
            assertThatThrownBy(opts::validateForSink)
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("filterValueExtractorClass")
                    .hasMessageContaining("does not implement");
        }
    }

    // ========================================================================
    // Extension loader
    // ========================================================================

    @Nested
    class ExtensionLoading {

        @Test
        void rejectsClassNotFound() {
            assertThatThrownBy(() -> ExtensionLoader.load(
                    "com.nonexistent.Foo", ConnectorAddressResolver.class, "addressResolverClass"))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("not found")
                    .hasMessageContaining("com.nonexistent.Foo");
        }

        @Test
        void rejectsWrongType() {
            assertThatThrownBy(() -> ExtensionLoader.load(
                    "java.lang.String", ConnectorAddressResolver.class, "addressResolverClass"))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("does not implement");
        }
    }

    public static final class TestObservationCollectorFactory
            implements ConnectorObservationCollectorFactory {
        @Override
        public com.rabbitmq.stream.ObservationCollector<?> create(ConnectorOptions options) {
            return com.rabbitmq.stream.ObservationCollector.NO_OP;
        }
    }

    public static final class TestCompressionCodecFactory
            implements ConnectorCompressionCodecFactory {
        @Override
        public com.rabbitmq.stream.compression.CompressionCodecFactory create(
                ConnectorOptions options) {
            return compression -> null;
        }
    }

    public static final class TestObservationRegistryProvider
            implements ConnectorObservationRegistryProvider {
        @Override
        public ObservationRegistry create(ConnectorOptions options) {
            return ObservationRegistry.create();
        }
    }

    public static final class TestFilterValueExtractor implements ConnectorFilterValueExtractor {
        @Override
        public String extract(ConnectorMessageView message) {
            return message.valueAtPath("application_properties.region");
        }
    }

    public static final class TestHashFunction implements ConnectorHashFunction {
        @Override
        public int hash(String routingKey) {
            return 0;
        }
    }

    public static final class TestPostFilterLegacy implements ConnectorPostFilter {
        @Override
        public boolean accept(ConnectorMessageView message) {
            return true;
        }
    }

    public static final class TestScheduledExecutorServiceFactory
            implements ConnectorScheduledExecutorServiceFactory {
        @Override
        public ScheduledExecutorService create(ConnectorOptions options) {
            return Executors.newSingleThreadScheduledExecutor();
        }
    }

    public static final class TestNettyEventLoopGroupFactory
            implements ConnectorNettyEventLoopGroupFactory {
        @Override
        public EventLoopGroup create(ConnectorOptions options) {
            return null;
        }
    }

    public static final class TestNettyByteBufAllocatorFactory
            implements ConnectorNettyByteBufAllocatorFactory {
        @Override
        public ByteBufAllocator create(ConnectorOptions options) {
            return null;
        }
    }

    public static final class TestNettyChannelCustomizer implements ConnectorNettyChannelCustomizer {
        @Override
        public void customize(Channel channel) {
        }
    }

    public static final class TestNettyBootstrapCustomizer implements ConnectorNettyBootstrapCustomizer {
        @Override
        public void customize(Bootstrap bootstrap) {
        }
    }
}
