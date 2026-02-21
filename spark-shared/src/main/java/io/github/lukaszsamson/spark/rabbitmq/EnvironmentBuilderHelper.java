package io.github.lukaszsamson.spark.rabbitmq;

import com.rabbitmq.stream.Address;
import com.rabbitmq.stream.BackOffDelayPolicy;
import com.rabbitmq.stream.Environment;
import com.rabbitmq.stream.EnvironmentBuilder;
import com.rabbitmq.stream.observation.micrometer.MicrometerObservationCollectorBuilder;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.EventLoopGroup;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.TrustManagerFactory;
import java.io.FileInputStream;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Builds a RabbitMQ {@link Environment} from {@link ConnectorOptions}.
 *
 * <p>Handles endpoints/URIs, TLS, credentials, vhost, and address resolver configuration.
 */
final class EnvironmentBuilderHelper {

    private static final Logger LOG = LoggerFactory.getLogger(EnvironmentBuilderHelper.class);

    private static final int DEFAULT_STREAM_PORT = 5552;
    private static final int DEFAULT_STREAM_TLS_PORT = 5551;

    private EnvironmentBuilderHelper() {}

    /**
     * Build a RabbitMQ Environment from connector options.
     *
     * @param options parsed connector options
     * @return a configured and connected Environment
     */
    static Environment buildEnvironment(ConnectorOptions options) {
        EnvironmentBuilder builder = Environment.builder();

        configureConnection(builder, options);
        configureCredentials(builder, options);
        configureTls(builder, options);
        configureAddressResolver(builder, options);
        configureObservationCollector(builder, options);
        configureCompressionCodecFactory(builder, options);
        configureTuning(builder, options);
        configureExecutorAndNetty(builder, options);

        return builder.build();
    }

    private static void configureConnection(EnvironmentBuilder builder, ConnectorOptions options) {
        if (options.getUris() != null && !options.getUris().isEmpty()) {
            List<String> uris = Arrays.stream(options.getUris().split(","))
                    .map(String::trim)
                    .filter(s -> !s.isEmpty())
                    .toList();
            builder.uris(uris);
        } else if (options.getEndpoints() != null && !options.getEndpoints().isEmpty()) {
            String scheme = options.isTls() ? "rabbitmq-stream+tls" : "rabbitmq-stream";
            List<String> uris = new ArrayList<>();
            for (String endpoint : options.getEndpoints().split(",")) {
                String trimmed = endpoint.trim();
                if (trimmed.isEmpty()) continue;
                String[] parts = trimmed.split(":");
                String host = parts[0];
                int port = parts.length > 1
                        ? Integer.parseInt(parts[1].trim())
                        : (options.isTls() ? DEFAULT_STREAM_TLS_PORT : DEFAULT_STREAM_PORT);
                uris.add(scheme + "://" + host + ":" + port);
            }
            builder.uris(uris);
        }
    }

    private static void configureCredentials(EnvironmentBuilder builder,
                                              ConnectorOptions options) {
        if (options.getUsername() != null) {
            builder.username(options.getUsername());
        }
        if (options.getPassword() != null) {
            builder.password(options.getPassword());
        }
        if (options.getVhost() != null) {
            builder.virtualHost(options.getVhost());
        }
    }

    private static void configureTls(EnvironmentBuilder builder, ConnectorOptions options) {
        if (!options.isTls()) {
            return;
        }
        if (options.isTlsTrustAll()) {
            builder.tls().trustEverything().environmentBuilder();
            return;
        }

        SslContext sslContext = buildSslContext(options);
        if (sslContext != null) {
            builder.tls().sslContext(sslContext).environmentBuilder();
        } else {
            builder.tls().environmentBuilder();
        }
    }

    private static void configureAddressResolver(EnvironmentBuilder builder,
                                                  ConnectorOptions options) {
        String resolverClass = options.getAddressResolverClass();
        if (resolverClass == null || resolverClass.isEmpty()) {
            return;
        }
        ConnectorAddressResolver resolver = ExtensionLoader.load(
                resolverClass, ConnectorAddressResolver.class,
                ConnectorOptions.ADDRESS_RESOLVER_CLASS);
        builder.addressResolver(address -> {
            ConnectorAddressResolver.Address resolved = resolver.resolve(
                    new ConnectorAddressResolver.Address(address.host(), address.port()));
            return new Address(resolved.host(), resolved.port());
        });
    }

    private static void configureObservationCollector(EnvironmentBuilder builder,
                                                      ConnectorOptions options) {
        String collectorClass = options.getObservationCollectorClass();
        String providerClass = options.getObservationRegistryProviderClass();
        if (collectorClass != null && !collectorClass.isEmpty()
                && providerClass != null && !providerClass.isEmpty()) {
            LOG.warn("Both '{}' and '{}' are set; '{}' takes precedence",
                    ConnectorOptions.OBSERVATION_COLLECTOR_CLASS,
                    ConnectorOptions.OBSERVATION_REGISTRY_PROVIDER_CLASS,
                    ConnectorOptions.OBSERVATION_COLLECTOR_CLASS);
        }
        if (collectorClass != null && !collectorClass.isEmpty()) {
            ConnectorObservationCollectorFactory factory = ExtensionLoader.load(
                    collectorClass, ConnectorObservationCollectorFactory.class,
                    ConnectorOptions.OBSERVATION_COLLECTOR_CLASS);
            var collector = factory.create(options);
            if (collector == null) {
                throw new IllegalArgumentException(
                        "Class specified by '" + ConnectorOptions.OBSERVATION_COLLECTOR_CLASS +
                                "' returned null ObservationCollector");
            }
            builder.observationCollector(collector);
            return;
        }

        if (providerClass == null || providerClass.isEmpty()) {
            return;
        }
        ConnectorObservationRegistryProvider provider = ExtensionLoader.load(
                providerClass, ConnectorObservationRegistryProvider.class,
                ConnectorOptions.OBSERVATION_REGISTRY_PROVIDER_CLASS);
        var observationRegistry = provider.create(options);
        if (observationRegistry == null) {
            throw new IllegalArgumentException(
                    "Class specified by '" + ConnectorOptions.OBSERVATION_REGISTRY_PROVIDER_CLASS +
                            "' returned null ObservationRegistry");
        }
        builder.observationCollector(new MicrometerObservationCollectorBuilder()
                .registry(observationRegistry)
                .build());
        LOG.info("Configured RabbitMQ stream Micrometer observation collector from '{}'",
                ConnectorOptions.OBSERVATION_REGISTRY_PROVIDER_CLASS);
    }

    private static void configureCompressionCodecFactory(EnvironmentBuilder builder,
                                                         ConnectorOptions options) {
        String factoryClass = options.getCompressionCodecFactoryClass();
        if (factoryClass == null || factoryClass.isEmpty()) {
            return;
        }
        ConnectorCompressionCodecFactory factory = ExtensionLoader.load(
                factoryClass, ConnectorCompressionCodecFactory.class,
                ConnectorOptions.COMPRESSION_CODEC_FACTORY_CLASS);
        var codecFactory = factory.create(options);
        if (codecFactory == null) {
            throw new IllegalArgumentException(
                    "Class specified by '" + ConnectorOptions.COMPRESSION_CODEC_FACTORY_CLASS +
                            "' returned null CompressionCodecFactory");
        }
        builder.compressionCodecFactory(codecFactory);
    }

    private static void configureTuning(EnvironmentBuilder builder, ConnectorOptions options) {
        builder.lazyInitialization(options.isLazyInitialization());

        if (options.getEnvironmentId() != null && !options.getEnvironmentId().isEmpty()) {
            builder.id(options.getEnvironmentId());
        }
        if (options.getRpcTimeoutMs() != null) {
            builder.rpcTimeout(Duration.ofMillis(options.getRpcTimeoutMs()));
        }
        if (options.getRequestedHeartbeatSeconds() != null) {
            builder.requestedHeartbeat(Duration.ofSeconds(options.getRequestedHeartbeatSeconds()));
        }
        if (options.getForceReplicaForConsumers() != null) {
            builder.forceReplicaForConsumers(options.getForceReplicaForConsumers());
        }
        if (options.getForceLeaderForProducers() != null) {
            builder.forceLeaderForProducers(options.getForceLeaderForProducers());
        }
        if (options.getLocatorConnectionCount() != null) {
            builder.locatorConnectionCount(options.getLocatorConnectionCount());
        }
        if (options.getRecoveryBackOffDelayPolicy() != null
                && !options.getRecoveryBackOffDelayPolicy().isEmpty()) {
            builder.recoveryBackOffDelayPolicy(parseBackOffDelayPolicy(
                    options.getRecoveryBackOffDelayPolicy(),
                    ConnectorOptions.RECOVERY_BACK_OFF_DELAY_POLICY));
        }
        if (options.getTopologyUpdateBackOffDelayPolicy() != null
                && !options.getTopologyUpdateBackOffDelayPolicy().isEmpty()) {
            builder.topologyUpdateBackOffDelayPolicy(parseBackOffDelayPolicy(
                    options.getTopologyUpdateBackOffDelayPolicy(),
                    ConnectorOptions.TOPOLOGY_UPDATE_BACK_OFF_DELAY_POLICY));
        }
        if (options.getMaxProducersByConnection() != null) {
            builder.maxProducersByConnection(options.getMaxProducersByConnection());
        }
        if (options.getMaxConsumersByConnection() != null) {
            builder.maxConsumersByConnection(options.getMaxConsumersByConnection());
        }
        if (options.getMaxTrackingConsumersByConnection() != null) {
            builder.maxTrackingConsumersByConnection(options.getMaxTrackingConsumersByConnection());
        }
    }

    private static void configureExecutorAndNetty(EnvironmentBuilder builder, ConnectorOptions options) {
        String schedulerFactoryClass = options.getScheduledExecutorService();
        if (schedulerFactoryClass != null && !schedulerFactoryClass.isEmpty()) {
            ConnectorScheduledExecutorServiceFactory factory = ExtensionLoader.load(
                    schedulerFactoryClass,
                    ConnectorScheduledExecutorServiceFactory.class,
                    ConnectorOptions.SCHEDULED_EXECUTOR_SERVICE);
            var scheduler = factory.create(options);
            if (scheduler == null) {
                throw new IllegalArgumentException(
                        "Class specified by '" + ConnectorOptions.SCHEDULED_EXECUTOR_SERVICE +
                                "' returned null ScheduledExecutorService");
            }
            builder.scheduledExecutorService(scheduler);
        }

        EnvironmentBuilder.NettyConfiguration netty = null;
        boolean configured = false;

        String eventLoopFactoryClass = options.getNettyEventLoopGroup();
        if (eventLoopFactoryClass != null && !eventLoopFactoryClass.isEmpty()) {
            ConnectorNettyEventLoopGroupFactory factory = ExtensionLoader.load(
                    eventLoopFactoryClass,
                    ConnectorNettyEventLoopGroupFactory.class,
                    ConnectorOptions.NETTY_EVENT_LOOP_GROUP);
            EventLoopGroup eventLoopGroup = factory.create(options);
            if (eventLoopGroup == null) {
                throw new IllegalArgumentException(
                        "Class specified by '" + ConnectorOptions.NETTY_EVENT_LOOP_GROUP +
                                "' returned null EventLoopGroup");
            }
            netty = builder.netty();
            configured = true;
            netty.eventLoopGroup(eventLoopGroup);
        }

        String allocatorFactoryClass = options.getNettyByteBufAllocator();
        if (allocatorFactoryClass != null && !allocatorFactoryClass.isEmpty()) {
            ConnectorNettyByteBufAllocatorFactory factory = ExtensionLoader.load(
                    allocatorFactoryClass,
                    ConnectorNettyByteBufAllocatorFactory.class,
                    ConnectorOptions.NETTY_BYTE_BUF_ALLOCATOR);
            ByteBufAllocator allocator = factory.create(options);
            if (allocator == null) {
                throw new IllegalArgumentException(
                        "Class specified by '" + ConnectorOptions.NETTY_BYTE_BUF_ALLOCATOR +
                                "' returned null ByteBufAllocator");
            }
            if (!configured) {
                netty = builder.netty();
                configured = true;
            }
            netty.byteBufAllocator(allocator);
        }

        String channelCustomizerClass = options.getNettyChannelCustomizer();
        if (channelCustomizerClass != null && !channelCustomizerClass.isEmpty()) {
            ConnectorNettyChannelCustomizer customizer = ExtensionLoader.load(
                    channelCustomizerClass,
                    ConnectorNettyChannelCustomizer.class,
                    ConnectorOptions.NETTY_CHANNEL_CUSTOMIZER);
            if (!configured) {
                netty = builder.netty();
                configured = true;
            }
            netty.channelCustomizer((Channel channel) -> customizer.customize(channel));
        }

        String bootstrapCustomizerClass = options.getNettyBootstrapCustomizer();
        if (bootstrapCustomizerClass != null && !bootstrapCustomizerClass.isEmpty()) {
            ConnectorNettyBootstrapCustomizer customizer = ExtensionLoader.load(
                    bootstrapCustomizerClass,
                    ConnectorNettyBootstrapCustomizer.class,
                    ConnectorOptions.NETTY_BOOTSTRAP_CUSTOMIZER);
            if (!configured) {
                netty = builder.netty();
                configured = true;
            }
            netty.bootstrapCustomizer((Bootstrap bootstrap) -> customizer.customize(bootstrap));
        }

        if (configured) {
            netty.environmentBuilder();
        }
    }

    private static BackOffDelayPolicy parseBackOffDelayPolicy(String rawValue, String optionName) {
        try {
            String[] parts = rawValue.split(",");
            if (parts.length == 1) {
                return BackOffDelayPolicy.fixed(Duration.parse(parts[0].trim()));
            } else if (parts.length == 2) {
                return BackOffDelayPolicy.fixedWithInitialDelay(
                        Duration.parse(parts[0].trim()),
                        Duration.parse(parts[1].trim()));
            } else if (parts.length == 3) {
                return BackOffDelayPolicy.fixedWithInitialDelay(
                        Duration.parse(parts[0].trim()),
                        Duration.parse(parts[1].trim()),
                        Duration.parse(parts[2].trim()));
            } else {
                throw new IllegalArgumentException(
                        "Expected 1, 2, or 3 comma-separated ISO-8601 durations");
            }
        } catch (Exception e) {
            throw new IllegalArgumentException(
                    "Invalid '" + optionName + "' value '" + rawValue + "'. " +
                            "Expected ISO-8601 durations: 'PT5S', 'PT5S,PT1S', or " +
                            "'PT5S,PT1S,PT1M'.", e);
        }
    }

    private static SslContext buildSslContext(ConnectorOptions options) {
        boolean hasTruststore = options.getTlsTruststore() != null && !options.getTlsTruststore().isEmpty();
        boolean hasKeystore = options.getTlsKeystore() != null && !options.getTlsKeystore().isEmpty();
        if (!hasTruststore && !hasKeystore) {
            return null;
        }

        try {
            SslContextBuilder sslBuilder = SslContextBuilder.forClient();

            if (hasTruststore) {
                KeyStore trustStore = loadJks(
                        options.getTlsTruststore(), options.getTlsTruststorePassword());
                TrustManagerFactory tmf = TrustManagerFactory.getInstance(
                        TrustManagerFactory.getDefaultAlgorithm());
                tmf.init(trustStore);
                sslBuilder.trustManager(tmf);
            }

            if (hasKeystore) {
                KeyStore keyStore = loadJks(options.getTlsKeystore(), options.getTlsKeystorePassword());
                KeyManagerFactory kmf = KeyManagerFactory.getInstance(
                        KeyManagerFactory.getDefaultAlgorithm());
                kmf.init(keyStore, passwordChars(options.getTlsKeystorePassword()));
                sslBuilder.keyManager(kmf);
            }

            return sslBuilder.build();
        } catch (GeneralSecurityException | IOException e) {
            throw new IllegalArgumentException("Failed to initialize TLS JKS configuration", e);
        }
    }

    private static KeyStore loadJks(String path, String password)
            throws GeneralSecurityException, IOException {
        KeyStore keyStore = KeyStore.getInstance("JKS");
        try (FileInputStream in = new FileInputStream(path)) {
            keyStore.load(in, passwordChars(password));
        }
        return keyStore;
    }

    private static char[] passwordChars(String password) {
        return password != null ? password.toCharArray() : new char[0];
    }
}
