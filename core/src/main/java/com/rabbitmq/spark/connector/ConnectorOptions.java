package com.rabbitmq.spark.connector;

import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Parsed and validated configuration for the RabbitMQ Streams Spark connector.
 *
 * <p>Constructed from the raw option map provided by Spark. Call
 * {@link #validateCommon()} at {@code TableProvider.getTable()} time,
 * {@link #validateForSource()} at {@code ScanBuilder} construction, and
 * {@link #validateForSink()} at {@code WriteBuilder} construction.
 */
public final class ConnectorOptions implements Serializable {
    private static final long serialVersionUID = 1L;

    // ---- Option key constants ----

    // Common
    public static final String ENDPOINTS = "endpoints";
    public static final String URIS = "uris";
    public static final String USERNAME = "username";
    public static final String PASSWORD = "password";
    public static final String VHOST = "vhost";
    public static final String TLS = "tls";
    public static final String TLS_TRUSTSTORE = "tls.truststore";
    public static final String TLS_TRUSTSTORE_PASSWORD = "tls.truststorePassword";
    public static final String TLS_KEYSTORE = "tls.keystore";
    public static final String TLS_KEYSTORE_PASSWORD = "tls.keystorePassword";
    public static final String TLS_TRUST_ALL = "tls.trustAll";
    public static final String STREAM = "stream";
    public static final String SUPERSTREAM = "superstream";
    public static final String CONSUMER_NAME = "consumerName";
    public static final String METADATA_FIELDS = "metadataFields";
    public static final String FAIL_ON_DATA_LOSS = "failOnDataLoss";
    public static final String ADDRESS_RESOLVER_CLASS = "addressResolverClass";
    public static final String OBSERVATION_COLLECTOR_CLASS = "observationCollectorClass";
    public static final String OBSERVATION_REGISTRY_PROVIDER_CLASS =
            "observationRegistryProviderClass";
    public static final String ENVIRONMENT_ID = "environmentId";
    public static final String RPC_TIMEOUT_MS = "rpcTimeoutMs";
    public static final String REQUESTED_HEARTBEAT_SECONDS = "requestedHeartbeatSeconds";
    public static final String FORCE_REPLICA_FOR_CONSUMERS = "forceReplicaForConsumers";
    public static final String FORCE_LEADER_FOR_PRODUCERS = "forceLeaderForProducers";
    public static final String LOCATOR_CONNECTION_COUNT = "locatorConnectionCount";
    public static final String RECOVERY_BACK_OFF_DELAY_POLICY = "recoveryBackOffDelayPolicy";
    public static final String TOPOLOGY_UPDATE_BACK_OFF_DELAY_POLICY =
            "topologyUpdateBackOffDelayPolicy";
    public static final String MAX_PRODUCERS_BY_CONNECTION = "maxProducersByConnection";
    public static final String MAX_CONSUMERS_BY_CONNECTION = "maxConsumersByConnection";
    public static final String MAX_TRACKING_CONSUMERS_BY_CONNECTION =
            "maxTrackingConsumersByConnection";

    // Source
    public static final String STARTING_OFFSETS = "startingOffsets";
    public static final String STARTING_OFFSET = "startingOffset";
    public static final String STARTING_TIMESTAMP = "startingTimestamp";
    public static final String ENDING_OFFSETS = "endingOffsets";
    public static final String ENDING_OFFSET = "endingOffset";
    public static final String MAX_RECORDS_PER_TRIGGER = "maxRecordsPerTrigger";
    public static final String MAX_BYTES_PER_TRIGGER = "maxBytesPerTrigger";
    public static final String MIN_PARTITIONS = "minPartitions";
    public static final String SERVER_SIDE_OFFSET_TRACKING = "serverSideOffsetTracking";
    public static final String FILTER_VALUES = "filterValues";
    public static final String FILTER_MATCH_UNFILTERED = "filterMatchUnfiltered";
    public static final String FILTER_POST_FILTER_CLASS = "filterPostFilterClass";
    public static final String FILTER_WARNING_ON_MISMATCH = "filterWarningOnMismatch";
    public static final String POLL_TIMEOUT_MS = "pollTimeoutMs";
    public static final String MAX_WAIT_MS = "maxWaitMs";
    public static final String CALLBACK_ENQUEUE_TIMEOUT_MS = "callbackEnqueueTimeoutMs";
    public static final String INITIAL_CREDITS = "initialCredits";
    public static final String QUEUE_CAPACITY = "queueCapacity";
    public static final String ESTIMATED_MESSAGE_SIZE_BYTES = "estimatedMessageSizeBytes";
    public static final String SINGLE_ACTIVE_CONSUMER = "singleActiveConsumer";

    // Sink
    public static final String PRODUCER_NAME = "producerName";
    public static final String PUBLISHER_CONFIRM_TIMEOUT_MS = "publisherConfirmTimeoutMs";
    public static final String MAX_IN_FLIGHT = "maxInFlight";
    public static final String ENQUEUE_TIMEOUT_MS = "enqueueTimeoutMs";
    public static final String BATCH_PUBLISHING_DELAY_MS = "batchPublishingDelayMs";
    public static final String COMPRESSION = "compression";
    public static final String SUB_ENTRY_SIZE = "subEntrySize";
    public static final String RETRY_ON_RECOVERY = "retryOnRecovery";
    public static final String DYNAMIC_BATCH = "dynamicBatch";
    public static final String COMPRESSION_CODEC_FACTORY_CLASS = "compressionCodecFactoryClass";
    public static final String ROUTING_STRATEGY = "routingStrategy";
    public static final String PARTITIONER_CLASS = "partitionerClass";
    public static final String FILTER_VALUE_PATH = "filterValuePath";
    public static final String FILTER_VALUE_EXTRACTOR_CLASS = "filterValueExtractorClass";
    public static final String IGNORE_UNKNOWN_COLUMNS = "ignoreUnknownColumns";
    public static final String REMOVED_BATCH_SIZE = "batchSize";
    public static final String REMOVED_FILTER_VALUE_COLUMN = "filterValueColumn";
    public static final String REMOVED_FILTER_POST_FILTER_CLASS_V2 = "filterPostFilterClassV2";

    // Resource management
    public static final String ENVIRONMENT_IDLE_TIMEOUT_MS = "environmentIdleTimeoutMs";

    // ---- Default values ----

    public static final String DEFAULT_METADATA_FIELDS =
            "properties,application_properties,message_annotations,creation_time,routing_key";
    public static final boolean DEFAULT_FAIL_ON_DATA_LOSS = true;
    public static final boolean DEFAULT_TLS = false;
    public static final boolean DEFAULT_TLS_TRUST_ALL = false;
    public static final StartingOffsetsMode DEFAULT_STARTING_OFFSETS = StartingOffsetsMode.EARLIEST;
    public static final EndingOffsetsMode DEFAULT_ENDING_OFFSETS = EndingOffsetsMode.LATEST;
    public static final boolean DEFAULT_FILTER_MATCH_UNFILTERED = false;
    public static final boolean DEFAULT_FILTER_WARNING_ON_MISMATCH = true;
    public static final long DEFAULT_POLL_TIMEOUT_MS = 30_000L;
    public static final long DEFAULT_MAX_WAIT_MS = 300_000L;
    public static final long DEFAULT_CALLBACK_ENQUEUE_TIMEOUT_MS = 5_000L;
    public static final int DEFAULT_INITIAL_CREDITS = 1;
    public static final int DEFAULT_QUEUE_CAPACITY = 10_000;
    public static final int DEFAULT_ESTIMATED_MESSAGE_SIZE_BYTES = 1024;
    public static final long DEFAULT_ENQUEUE_TIMEOUT_MS = 10_000L;
    public static final CompressionType DEFAULT_COMPRESSION = CompressionType.NONE;
    public static final RoutingStrategyType DEFAULT_ROUTING_STRATEGY = RoutingStrategyType.HASH;
    public static final boolean DEFAULT_IGNORE_UNKNOWN_COLUMNS = false;
    public static final long DEFAULT_ENVIRONMENT_IDLE_TIMEOUT_MS = 60_000L;

    // ---- Parsed fields ----

    // Common
    private final String endpoints;
    private final String uris;
    private final String username;
    private final String password;
    private final String vhost;
    private final boolean tls;
    private final String tlsTruststore;
    private final String tlsTruststorePassword;
    private final String tlsKeystore;
    private final String tlsKeystorePassword;
    private final boolean tlsTrustAll;
    private final String stream;
    private final String superstream;
    private final String consumerName;
    private final Set<MetadataField> metadataFields;
    private final boolean failOnDataLoss;
    private final String addressResolverClass;
    private final String observationCollectorClass;
    private final String observationRegistryProviderClass;
    private final String environmentId;
    private final Long rpcTimeoutMs;
    private final Long requestedHeartbeatSeconds;
    private final Boolean forceReplicaForConsumers;
    private final Boolean forceLeaderForProducers;
    private final Integer locatorConnectionCount;
    private final String recoveryBackOffDelayPolicy;
    private final String topologyUpdateBackOffDelayPolicy;
    private final Integer maxProducersByConnection;
    private final Integer maxConsumersByConnection;
    private final Integer maxTrackingConsumersByConnection;

    // Source
    private final StartingOffsetsMode startingOffsets;
    private final Long startingOffset;
    private final Long startingTimestamp;
    private final EndingOffsetsMode endingOffsets;
    private final Long endingOffset;
    private final Long maxRecordsPerTrigger;
    private final Long maxBytesPerTrigger;
    private final Integer minPartitions;
    private final Boolean serverSideOffsetTracking;
    private final List<String> filterValues;
    private final boolean filterMatchUnfiltered;
    private final String filterPostFilterClass;
    private final boolean filterWarningOnMismatch;
    private final long pollTimeoutMs;
    private final long maxWaitMs;
    private final long callbackEnqueueTimeoutMs;
    private final int initialCredits;
    private final int queueCapacity;
    private final int estimatedMessageSizeBytes;
    private final boolean singleActiveConsumer;

    // Sink
    private final String producerName;
    private final Long publisherConfirmTimeoutMs;
    private final Integer maxInFlight;
    private final long enqueueTimeoutMs;
    private final Long batchPublishingDelayMs;
    private final CompressionType compression;
    private final Integer subEntrySize;
    private final Boolean retryOnRecovery;
    private final Boolean dynamicBatch;
    private final String compressionCodecFactoryClass;
    private final RoutingStrategyType routingStrategy;
    private final String partitionerClass;
    private final String filterValuePath;
    private final String filterValueExtractorClass;
    private final boolean ignoreUnknownColumns;

    // Removed options (kept only for fail-fast validation)
    private final String removedBatchSize;
    private final String removedFilterValueColumn;
    private final String removedFilterPostFilterClassV2;

    // Resource management
    private final long environmentIdleTimeoutMs;

    /**
     * Parse connector options from the raw option map.
     *
     * @param options raw option map (typically from Spark's CaseInsensitiveStringMap)
     */
    public ConnectorOptions(Map<String, String> options) {
        // Common
        this.endpoints = getString(options, ENDPOINTS);
        this.uris = getString(options, URIS);
        this.username = getString(options, USERNAME);
        this.password = getString(options, PASSWORD);
        this.vhost = getString(options, VHOST);
        this.tls = getBoolean(options, TLS, DEFAULT_TLS);
        this.tlsTruststore = getString(options, TLS_TRUSTSTORE);
        this.tlsTruststorePassword = getString(options, TLS_TRUSTSTORE_PASSWORD);
        this.tlsKeystore = getString(options, TLS_KEYSTORE);
        this.tlsKeystorePassword = getString(options, TLS_KEYSTORE_PASSWORD);
        this.tlsTrustAll = getBoolean(options, TLS_TRUST_ALL, DEFAULT_TLS_TRUST_ALL);
        this.stream = getString(options, STREAM);
        this.superstream = getString(options, SUPERSTREAM);
        this.consumerName = getString(options, CONSUMER_NAME);
        this.metadataFields = parseMetadataFields(
                getString(options, METADATA_FIELDS, DEFAULT_METADATA_FIELDS));
        this.failOnDataLoss = getBoolean(options, FAIL_ON_DATA_LOSS, DEFAULT_FAIL_ON_DATA_LOSS);
        this.addressResolverClass = getString(options, ADDRESS_RESOLVER_CLASS);
        this.observationCollectorClass = getString(options, OBSERVATION_COLLECTOR_CLASS);
        this.observationRegistryProviderClass = getString(options, OBSERVATION_REGISTRY_PROVIDER_CLASS);
        this.environmentId = getString(options, ENVIRONMENT_ID);
        this.rpcTimeoutMs = getLong(options, RPC_TIMEOUT_MS);
        this.requestedHeartbeatSeconds = getLong(options, REQUESTED_HEARTBEAT_SECONDS);
        this.forceReplicaForConsumers = getNullableBoolean(options, FORCE_REPLICA_FOR_CONSUMERS);
        this.forceLeaderForProducers = getNullableBoolean(options, FORCE_LEADER_FOR_PRODUCERS);
        this.locatorConnectionCount = getInteger(options, LOCATOR_CONNECTION_COUNT);
        this.recoveryBackOffDelayPolicy = getString(options, RECOVERY_BACK_OFF_DELAY_POLICY);
        this.topologyUpdateBackOffDelayPolicy = getString(
                options, TOPOLOGY_UPDATE_BACK_OFF_DELAY_POLICY);
        this.maxProducersByConnection = getInteger(options, MAX_PRODUCERS_BY_CONNECTION);
        this.maxConsumersByConnection = getInteger(options, MAX_CONSUMERS_BY_CONNECTION);
        this.maxTrackingConsumersByConnection = getInteger(
                options, MAX_TRACKING_CONSUMERS_BY_CONNECTION);

        // Source
        this.startingOffsets = parseEnum(options, STARTING_OFFSETS,
                StartingOffsetsMode::fromString, DEFAULT_STARTING_OFFSETS);
        this.startingOffset = getLong(options, STARTING_OFFSET);
        this.startingTimestamp = getLong(options, STARTING_TIMESTAMP);
        this.endingOffsets = parseEnum(options, ENDING_OFFSETS,
                EndingOffsetsMode::fromString, DEFAULT_ENDING_OFFSETS);
        this.endingOffset = getLong(options, ENDING_OFFSET);
        this.maxRecordsPerTrigger = getLong(options, MAX_RECORDS_PER_TRIGGER);
        this.maxBytesPerTrigger = getLong(options, MAX_BYTES_PER_TRIGGER);
        this.minPartitions = getInteger(options, MIN_PARTITIONS);
        this.serverSideOffsetTracking = getNullableBoolean(options, SERVER_SIDE_OFFSET_TRACKING);
        this.filterValues = parseCommaSeparated(getString(options, FILTER_VALUES));
        this.filterMatchUnfiltered = getBoolean(options, FILTER_MATCH_UNFILTERED,
                DEFAULT_FILTER_MATCH_UNFILTERED);
        this.filterPostFilterClass = getString(options, FILTER_POST_FILTER_CLASS);
        this.filterWarningOnMismatch = getBoolean(options, FILTER_WARNING_ON_MISMATCH,
                DEFAULT_FILTER_WARNING_ON_MISMATCH);
        this.pollTimeoutMs = getLongPrimitive(options, POLL_TIMEOUT_MS, DEFAULT_POLL_TIMEOUT_MS);
        this.maxWaitMs = getLongPrimitive(options, MAX_WAIT_MS, DEFAULT_MAX_WAIT_MS);
        this.callbackEnqueueTimeoutMs = getLongPrimitive(options, CALLBACK_ENQUEUE_TIMEOUT_MS,
                DEFAULT_CALLBACK_ENQUEUE_TIMEOUT_MS);
        this.initialCredits = getIntPrimitive(options, INITIAL_CREDITS, DEFAULT_INITIAL_CREDITS);
        this.queueCapacity = getIntPrimitive(options, QUEUE_CAPACITY, DEFAULT_QUEUE_CAPACITY);
        this.estimatedMessageSizeBytes = getIntPrimitive(options, ESTIMATED_MESSAGE_SIZE_BYTES,
                DEFAULT_ESTIMATED_MESSAGE_SIZE_BYTES);
        this.singleActiveConsumer = getBoolean(options, SINGLE_ACTIVE_CONSUMER, false);

        // Sink
        this.producerName = getString(options, PRODUCER_NAME);
        this.publisherConfirmTimeoutMs = getLong(options, PUBLISHER_CONFIRM_TIMEOUT_MS);
        this.maxInFlight = getInteger(options, MAX_IN_FLIGHT);
        this.enqueueTimeoutMs = getLongPrimitive(options, ENQUEUE_TIMEOUT_MS,
                DEFAULT_ENQUEUE_TIMEOUT_MS);
        this.batchPublishingDelayMs = getLong(options, BATCH_PUBLISHING_DELAY_MS);
        this.compression = parseEnum(options, COMPRESSION,
                CompressionType::fromString, DEFAULT_COMPRESSION);
        this.subEntrySize = getInteger(options, SUB_ENTRY_SIZE);
        this.retryOnRecovery = getNullableBoolean(options, RETRY_ON_RECOVERY);
        this.dynamicBatch = getNullableBoolean(options, DYNAMIC_BATCH);
        this.compressionCodecFactoryClass = getString(options, COMPRESSION_CODEC_FACTORY_CLASS);
        this.routingStrategy = parseEnum(options, ROUTING_STRATEGY,
                RoutingStrategyType::fromString, DEFAULT_ROUTING_STRATEGY);
        this.partitionerClass = getString(options, PARTITIONER_CLASS);
        this.filterValuePath = getString(options, FILTER_VALUE_PATH);
        this.filterValueExtractorClass = getString(options, FILTER_VALUE_EXTRACTOR_CLASS);
        this.ignoreUnknownColumns = getBoolean(options, IGNORE_UNKNOWN_COLUMNS,
                DEFAULT_IGNORE_UNKNOWN_COLUMNS);

        this.removedBatchSize = getString(options, REMOVED_BATCH_SIZE);
        this.removedFilterValueColumn = getString(options, REMOVED_FILTER_VALUE_COLUMN);
        this.removedFilterPostFilterClassV2 = getString(options, REMOVED_FILTER_POST_FILTER_CLASS_V2);

        // Resource management
        this.environmentIdleTimeoutMs = getLongPrimitive(options, ENVIRONMENT_IDLE_TIMEOUT_MS,
                DEFAULT_ENVIRONMENT_IDLE_TIMEOUT_MS);
    }

    // ---- Validation ----

    /**
     * Validate common options. Call at {@code TableProvider.getTable()} time.
     *
     * @throws IllegalArgumentException if any common option is invalid
     */
    public void validateCommon() {
        if (removedBatchSize != null) {
            throw new IllegalArgumentException(
                    "'" + REMOVED_BATCH_SIZE + "' has been removed for RabbitMQ stream client 1.5+. " +
                            "Use '" + BATCH_PUBLISHING_DELAY_MS + "' and '" + DYNAMIC_BATCH +
                            "' instead.");
        }
        if (removedFilterValueColumn != null) {
            throw new IllegalArgumentException(
                    "'" + REMOVED_FILTER_VALUE_COLUMN + "' has been removed. " +
                            "Use '" + FILTER_VALUE_PATH + "' or '" +
                            FILTER_VALUE_EXTRACTOR_CLASS + "' instead.");
        }
        if (removedFilterPostFilterClassV2 != null) {
            throw new IllegalArgumentException(
                    "'" + REMOVED_FILTER_POST_FILTER_CLASS_V2 + "' has been removed. " +
                            "Use '" + FILTER_POST_FILTER_CLASS + "' with ConnectorMessageView-based " +
                            "ConnectorPostFilter implementations.");
        }

        // Exactly one of stream or superstream
        boolean hasStream = stream != null && !stream.isEmpty();
        boolean hasSuperStream = superstream != null && !superstream.isEmpty();
        if (hasStream && hasSuperStream) {
            throw new IllegalArgumentException(
                    "Exactly one of '" + STREAM + "' or '" + SUPERSTREAM +
                            "' must be specified, but both were provided");
        }
        if (!hasStream && !hasSuperStream) {
            throw new IllegalArgumentException(
                    "Exactly one of '" + STREAM + "' or '" + SUPERSTREAM +
                            "' must be specified, but neither was provided");
        }

        // At least one of endpoints or uris
        boolean hasEndpoints = endpoints != null && !endpoints.isEmpty();
        boolean hasUris = uris != null && !uris.isEmpty();
        if (!hasEndpoints && !hasUris) {
            throw new IllegalArgumentException(
                    "At least one of '" + ENDPOINTS + "' or '" + URIS +
                            "' must be specified");
        }

        // TLS-related: trustAll requires tls=true
        if (tlsTrustAll && !tls) {
            throw new IllegalArgumentException(
                    "'" + TLS_TRUST_ALL + "' requires '" + TLS + "' to be true");
        }

        // Validate extension class: addressResolverClass
        if (addressResolverClass != null && !addressResolverClass.isEmpty()) {
            ExtensionLoader.load(addressResolverClass, ConnectorAddressResolver.class,
                    ADDRESS_RESOLVER_CLASS);
        }
        if (observationCollectorClass != null && !observationCollectorClass.isEmpty()) {
            ExtensionLoader.load(observationCollectorClass, ConnectorObservationCollectorFactory.class,
                    OBSERVATION_COLLECTOR_CLASS);
        }
        if (observationRegistryProviderClass != null && !observationRegistryProviderClass.isEmpty()) {
            ExtensionLoader.load(observationRegistryProviderClass,
                    ConnectorObservationRegistryProvider.class,
                    OBSERVATION_REGISTRY_PROVIDER_CLASS);
        }
        if (compressionCodecFactoryClass != null && !compressionCodecFactoryClass.isEmpty()) {
            ExtensionLoader.load(compressionCodecFactoryClass,
                    ConnectorCompressionCodecFactory.class,
                    COMPRESSION_CODEC_FACTORY_CLASS);
        }
        if (rpcTimeoutMs != null && rpcTimeoutMs <= 0) {
            throw new IllegalArgumentException(
                    "'" + RPC_TIMEOUT_MS + "' must be > 0, got: " + rpcTimeoutMs);
        }
        if (requestedHeartbeatSeconds != null && requestedHeartbeatSeconds <= 0) {
            throw new IllegalArgumentException(
                    "'" + REQUESTED_HEARTBEAT_SECONDS + "' must be > 0, got: " +
                            requestedHeartbeatSeconds);
        }
        if (locatorConnectionCount != null && locatorConnectionCount <= 0) {
            throw new IllegalArgumentException(
                    "'" + LOCATOR_CONNECTION_COUNT + "' must be > 0, got: " +
                            locatorConnectionCount);
        }
        if (maxProducersByConnection != null && maxProducersByConnection <= 0) {
            throw new IllegalArgumentException(
                    "'" + MAX_PRODUCERS_BY_CONNECTION + "' must be > 0, got: " +
                            maxProducersByConnection);
        }
        if (maxConsumersByConnection != null && maxConsumersByConnection <= 0) {
            throw new IllegalArgumentException(
                    "'" + MAX_CONSUMERS_BY_CONNECTION + "' must be > 0, got: " +
                            maxConsumersByConnection);
        }
        if (maxTrackingConsumersByConnection != null && maxTrackingConsumersByConnection <= 0) {
            throw new IllegalArgumentException(
                    "'" + MAX_TRACKING_CONSUMERS_BY_CONNECTION + "' must be > 0, got: " +
                            maxTrackingConsumersByConnection);
        }
    }

    /**
     * Validate source-specific options. Call at {@code ScanBuilder} construction.
     * Also validates common options.
     *
     * @throws IllegalArgumentException if any source option is invalid
     */
    public void validateForSource() {
        validateCommon();

        // startingOffsets=offset requires startingOffset
        if (startingOffsets == StartingOffsetsMode.OFFSET && startingOffset == null) {
            throw new IllegalArgumentException(
                    "'" + STARTING_OFFSET + "' is required when '" + STARTING_OFFSETS +
                            "' is 'offset'");
        }
        // startingOffsets=timestamp requires startingTimestamp
        if (startingOffsets == StartingOffsetsMode.TIMESTAMP && startingTimestamp == null) {
            throw new IllegalArgumentException(
                    "'" + STARTING_TIMESTAMP + "' is required when '" + STARTING_OFFSETS +
                            "' is 'timestamp'");
        }
        // endingOffsets=offset requires endingOffset
        if (endingOffsets == EndingOffsetsMode.OFFSET && endingOffset == null) {
            throw new IllegalArgumentException(
                    "'" + ENDING_OFFSET + "' is required when '" + ENDING_OFFSETS +
                            "' is 'offset'");
        }
        // Numeric range checks
        if (startingOffset != null && startingOffset < 0) {
            throw new IllegalArgumentException(
                    "'" + STARTING_OFFSET + "' must be >= 0, got: " + startingOffset);
        }
        if (startingTimestamp != null && startingTimestamp <= 0) {
            throw new IllegalArgumentException(
                    "'" + STARTING_TIMESTAMP + "' must be > 0 (epoch millis), got: " +
                            startingTimestamp);
        }
        if (endingOffset != null && endingOffset < 0) {
            throw new IllegalArgumentException(
                    "'" + ENDING_OFFSET + "' must be >= 0, got: " + endingOffset);
        }
        if (maxRecordsPerTrigger != null && maxRecordsPerTrigger <= 0) {
            throw new IllegalArgumentException(
                    "'" + MAX_RECORDS_PER_TRIGGER + "' must be > 0, got: " + maxRecordsPerTrigger);
        }
        if (maxBytesPerTrigger != null && maxBytesPerTrigger <= 0) {
            throw new IllegalArgumentException(
                    "'" + MAX_BYTES_PER_TRIGGER + "' must be > 0, got: " + maxBytesPerTrigger);
        }
        if (minPartitions != null && minPartitions <= 0) {
            throw new IllegalArgumentException(
                    "'" + MIN_PARTITIONS + "' must be > 0, got: " + minPartitions);
        }
        if (pollTimeoutMs <= 0) {
            throw new IllegalArgumentException(
                    "'" + POLL_TIMEOUT_MS + "' must be > 0, got: " + pollTimeoutMs);
        }
        if (maxWaitMs <= 0) {
            throw new IllegalArgumentException(
                    "'" + MAX_WAIT_MS + "' must be > 0, got: " + maxWaitMs);
        }
        if (callbackEnqueueTimeoutMs < 0) {
            throw new IllegalArgumentException(
                    "'" + CALLBACK_ENQUEUE_TIMEOUT_MS + "' must be >= 0, got: " +
                            callbackEnqueueTimeoutMs);
        }
        if (initialCredits <= 0) {
            throw new IllegalArgumentException(
                    "'" + INITIAL_CREDITS + "' must be > 0, got: " + initialCredits);
        }
        if (queueCapacity <= 0) {
            throw new IllegalArgumentException(
                    "'" + QUEUE_CAPACITY + "' must be > 0, got: " + queueCapacity);
        }
        if (estimatedMessageSizeBytes <= 0) {
            throw new IllegalArgumentException(
                    "'" + ESTIMATED_MESSAGE_SIZE_BYTES + "' must be > 0, got: " +
                            estimatedMessageSizeBytes);
        }
        if (singleActiveConsumer && (consumerName == null || consumerName.isEmpty())) {
            throw new IllegalArgumentException(
                    "'" + CONSUMER_NAME + "' is required when '" + SINGLE_ACTIVE_CONSUMER +
                            "' is true");
        }

        // Validate extension class: filterPostFilterClass
        if (filterPostFilterClass != null && !filterPostFilterClass.isEmpty()) {
            ExtensionLoader.load(filterPostFilterClass, ConnectorPostFilter.class,
                    FILTER_POST_FILTER_CLASS);
        }

        if (filterValuePath != null && !filterValuePath.isEmpty()) {
            ConnectorMessagePath.validate(filterValuePath, FILTER_VALUE_PATH);
        }
    }

    /**
     * Validate sink-specific options. Call at {@code WriteBuilder} construction.
     * Also validates common options.
     *
     * @throws IllegalArgumentException if any sink option is invalid
     */
    public void validateForSink() {
        validateCommon();

        // compression requires subEntrySize > 1
        if (compression != CompressionType.NONE) {
            if (subEntrySize == null || subEntrySize <= 1) {
                throw new IllegalArgumentException(
                        "'" + COMPRESSION + "=" + compression.name().toLowerCase(Locale.ROOT) +
                                "' requires '" + SUB_ENTRY_SIZE + "' > 1");
            }
        }

        // routingStrategy=custom requires partitionerClass
        if (routingStrategy == RoutingStrategyType.CUSTOM) {
            if (partitionerClass == null || partitionerClass.isEmpty()) {
                throw new IllegalArgumentException(
                        "'" + PARTITIONER_CLASS + "' is required when '" + ROUTING_STRATEGY +
                                "' is 'custom'");
            }
        }

        // Numeric range checks
        if (maxInFlight != null && maxInFlight <= 0) {
            throw new IllegalArgumentException(
                    "'" + MAX_IN_FLIGHT + "' must be > 0, got: " + maxInFlight);
        }
        if (enqueueTimeoutMs < 0) {
            throw new IllegalArgumentException(
                    "'" + ENQUEUE_TIMEOUT_MS + "' must be >= 0, got: " + enqueueTimeoutMs);
        }
        if (subEntrySize != null && subEntrySize <= 0) {
            throw new IllegalArgumentException(
                    "'" + SUB_ENTRY_SIZE + "' must be > 0, got: " + subEntrySize);
        }
        if (publisherConfirmTimeoutMs != null && publisherConfirmTimeoutMs < 0) {
            throw new IllegalArgumentException(
                    "'" + PUBLISHER_CONFIRM_TIMEOUT_MS + "' must be >= 0, got: " +
                            publisherConfirmTimeoutMs);
        }
        if (batchPublishingDelayMs != null && batchPublishingDelayMs < 0) {
            throw new IllegalArgumentException(
                    "'" + BATCH_PUBLISHING_DELAY_MS + "' must be >= 0, got: " +
                            batchPublishingDelayMs);
        }

        // Validate extension class: partitionerClass
        if (partitionerClass != null && !partitionerClass.isEmpty()) {
            ExtensionLoader.load(partitionerClass, ConnectorRoutingStrategy.class,
                    PARTITIONER_CLASS);
        }

        if (filterValuePath != null && !filterValuePath.isEmpty()
                && filterValueExtractorClass != null && !filterValueExtractorClass.isEmpty()) {
            throw new IllegalArgumentException(
                    "Only one of '" + FILTER_VALUE_PATH + "' or '" +
                            FILTER_VALUE_EXTRACTOR_CLASS + "' can be set");
        }
        if (filterValuePath != null && !filterValuePath.isEmpty()) {
            ConnectorMessagePath.validate(filterValuePath, FILTER_VALUE_PATH);
        }
        if (filterValueExtractorClass != null && !filterValueExtractorClass.isEmpty()) {
            ExtensionLoader.load(filterValueExtractorClass, ConnectorFilterValueExtractor.class,
                    FILTER_VALUE_EXTRACTOR_CLASS);
        }
    }

    // ---- Stream mode helpers ----

    /** Returns {@code true} if configured in single-stream mode. */
    public boolean isStreamMode() {
        return stream != null && !stream.isEmpty();
    }

    /** Returns {@code true} if configured in superstream mode. */
    public boolean isSuperStreamMode() {
        return superstream != null && !superstream.isEmpty();
    }

    /**
     * Returns the effective server-side offset tracking setting.
     *
     * @param isStreaming {@code true} for streaming queries, {@code false} for batch
     * @return whether to store offsets in RabbitMQ broker on commit
     */
    public boolean isServerSideOffsetTracking(boolean isStreaming) {
        if (serverSideOffsetTracking != null) {
            return serverSideOffsetTracking;
        }
        return isStreaming; // default: true for streaming, false for batch
    }

    // ---- Getters: Common ----

    public String getEndpoints() { return endpoints; }
    public String getUris() { return uris; }
    public String getUsername() { return username; }
    public String getPassword() { return password; }
    public String getVhost() { return vhost; }
    public boolean isTls() { return tls; }
    public String getTlsTruststore() { return tlsTruststore; }
    public String getTlsTruststorePassword() { return tlsTruststorePassword; }
    public String getTlsKeystore() { return tlsKeystore; }
    public String getTlsKeystorePassword() { return tlsKeystorePassword; }
    public boolean isTlsTrustAll() { return tlsTrustAll; }
    public String getStream() { return stream; }
    public String getSuperStream() { return superstream; }
    public String getConsumerName() { return consumerName; }
    public Set<MetadataField> getMetadataFields() { return metadataFields; }
    public boolean isFailOnDataLoss() { return failOnDataLoss; }
    public String getAddressResolverClass() { return addressResolverClass; }
    public String getObservationCollectorClass() { return observationCollectorClass; }
    public String getObservationRegistryProviderClass() { return observationRegistryProviderClass; }
    public String getEnvironmentId() { return environmentId; }
    public Long getRpcTimeoutMs() { return rpcTimeoutMs; }
    public Long getRequestedHeartbeatSeconds() { return requestedHeartbeatSeconds; }
    public Boolean getForceReplicaForConsumers() { return forceReplicaForConsumers; }
    public Boolean getForceLeaderForProducers() { return forceLeaderForProducers; }
    public Integer getLocatorConnectionCount() { return locatorConnectionCount; }
    public String getRecoveryBackOffDelayPolicy() { return recoveryBackOffDelayPolicy; }
    public String getTopologyUpdateBackOffDelayPolicy() { return topologyUpdateBackOffDelayPolicy; }
    public Integer getMaxProducersByConnection() { return maxProducersByConnection; }
    public Integer getMaxConsumersByConnection() { return maxConsumersByConnection; }
    public Integer getMaxTrackingConsumersByConnection() {
        return maxTrackingConsumersByConnection;
    }

    // ---- Getters: Source ----

    public StartingOffsetsMode getStartingOffsets() { return startingOffsets; }
    public Long getStartingOffset() { return startingOffset; }
    public Long getStartingTimestamp() { return startingTimestamp; }
    public EndingOffsetsMode getEndingOffsets() { return endingOffsets; }
    public Long getEndingOffset() { return endingOffset; }
    public Long getMaxRecordsPerTrigger() { return maxRecordsPerTrigger; }
    public Long getMaxBytesPerTrigger() { return maxBytesPerTrigger; }
    public Integer getMinPartitions() { return minPartitions; }
    public Boolean getServerSideOffsetTracking() { return serverSideOffsetTracking; }
    public List<String> getFilterValues() { return filterValues; }
    public boolean isFilterMatchUnfiltered() { return filterMatchUnfiltered; }
    public String getFilterPostFilterClass() { return filterPostFilterClass; }
    public boolean isFilterWarningOnMismatch() { return filterWarningOnMismatch; }
    public long getPollTimeoutMs() { return pollTimeoutMs; }
    public long getMaxWaitMs() { return maxWaitMs; }
    public long getCallbackEnqueueTimeoutMs() { return callbackEnqueueTimeoutMs; }
    public int getInitialCredits() { return initialCredits; }
    public int getQueueCapacity() { return queueCapacity; }
    public int getEstimatedMessageSizeBytes() { return estimatedMessageSizeBytes; }
    public boolean isSingleActiveConsumer() { return singleActiveConsumer; }

    // ---- Getters: Sink ----

    public String getProducerName() { return producerName; }
    public Long getPublisherConfirmTimeoutMs() { return publisherConfirmTimeoutMs; }
    public Integer getMaxInFlight() { return maxInFlight; }
    public long getEnqueueTimeoutMs() { return enqueueTimeoutMs; }
    public Long getBatchPublishingDelayMs() { return batchPublishingDelayMs; }
    public CompressionType getCompression() { return compression; }
    public Integer getSubEntrySize() { return subEntrySize; }
    public Boolean getRetryOnRecovery() { return retryOnRecovery; }
    public Boolean getDynamicBatch() { return dynamicBatch; }
    public String getCompressionCodecFactoryClass() { return compressionCodecFactoryClass; }
    public RoutingStrategyType getRoutingStrategy() { return routingStrategy; }
    public String getPartitionerClass() { return partitionerClass; }
    public String getFilterValuePath() { return filterValuePath; }
    public String getFilterValueExtractorClass() { return filterValueExtractorClass; }
    public boolean isIgnoreUnknownColumns() { return ignoreUnknownColumns; }

    // ---- Getters: Resource management ----

    public long getEnvironmentIdleTimeoutMs() { return environmentIdleTimeoutMs; }

    // ---- Parsing helpers ----

    private static String lookupOption(Map<String, String> options, String key) {
        String value = options.get(key);
        if (value != null || options.containsKey(key)) {
            return value;
        }
        String lowerKey = key.toLowerCase(Locale.ROOT);
        value = options.get(lowerKey);
        if (value != null || options.containsKey(lowerKey)) {
            return value;
        }
        for (Map.Entry<String, String> entry : options.entrySet()) {
            if (entry.getKey() != null && entry.getKey().equalsIgnoreCase(key)) {
                return entry.getValue();
            }
        }
        return null;
    }

    private static String getString(Map<String, String> options, String key) {
        return lookupOption(options, key);
    }

    private static String getString(Map<String, String> options, String key, String defaultValue) {
        String value = lookupOption(options, key);
        return (value != null) ? value : defaultValue;
    }

    private static boolean getBoolean(Map<String, String> options, String key,
                                      boolean defaultValue) {
        String value = lookupOption(options, key);
        if (value == null) {
            return defaultValue;
        }
        String lower = value.trim().toLowerCase(Locale.ROOT);
        if ("true".equals(lower)) return true;
        if ("false".equals(lower)) return false;
        throw new IllegalArgumentException(
                "'" + key + "' must be 'true' or 'false', got: '" + value + "'");
    }

    private static Boolean getNullableBoolean(Map<String, String> options, String key) {
        String value = lookupOption(options, key);
        if (value == null) {
            return null;
        }
        String lower = value.trim().toLowerCase(Locale.ROOT);
        if ("true".equals(lower)) return Boolean.TRUE;
        if ("false".equals(lower)) return Boolean.FALSE;
        throw new IllegalArgumentException(
                "'" + key + "' must be 'true' or 'false', got: '" + value + "'");
    }

    private static Long getLong(Map<String, String> options, String key) {
        String value = lookupOption(options, key);
        if (value == null) {
            return null;
        }
        try {
            return Long.parseLong(value.trim());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                    "'" + key + "' must be a valid long, got: '" + value + "'");
        }
    }

    private static long getLongPrimitive(Map<String, String> options, String key,
                                         long defaultValue) {
        Long value = getLong(options, key);
        return (value != null) ? value : defaultValue;
    }

    private static Integer getInteger(Map<String, String> options, String key) {
        String value = lookupOption(options, key);
        if (value == null) {
            return null;
        }
        try {
            return Integer.parseInt(value.trim());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                    "'" + key + "' must be a valid integer, got: '" + value + "'");
        }
    }

    private static int getIntPrimitive(Map<String, String> options, String key, int defaultValue) {
        Integer value = getInteger(options, key);
        return (value != null) ? value : defaultValue;
    }

    private static <T> T parseEnum(Map<String, String> options, String key,
                                   java.util.function.Function<String, T> parser, T defaultValue) {
        String value = lookupOption(options, key);
        if (value == null) {
            return defaultValue;
        }
        return parser.apply(value.trim());
    }

    private static Set<MetadataField> parseMetadataFields(String value) {
        if (value == null || value.trim().isEmpty()) {
            return Collections.emptySet();
        }
        return Arrays.stream(value.split(","))
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .map(MetadataField::fromString)
                .collect(Collectors.toCollection(() -> EnumSet.noneOf(MetadataField.class)));
    }

    private static List<String> parseCommaSeparated(String value) {
        if (value == null || value.trim().isEmpty()) {
            return Collections.emptyList();
        }
        return Arrays.stream(value.split(","))
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .toList();
    }
}
