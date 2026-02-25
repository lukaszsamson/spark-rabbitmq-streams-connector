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
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashSet;
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
            int defaultPort = options.isTls() ? DEFAULT_STREAM_TLS_PORT : DEFAULT_STREAM_PORT;
            List<String> uris = new ArrayList<>();
            for (String endpoint : options.getEndpoints().split(",")) {
                String trimmed = endpoint.trim();
                if (trimmed.isEmpty()) continue;
                uris.add(endpointToUri(scheme, trimmed, defaultPort));
            }
            builder.uris(uris);
        }
    }

    private static String endpointToUri(String scheme, String endpoint, int defaultPort) {
        try {
            URI parsed = new URI(scheme + "://" + endpoint);
            String host = parsed.getHost();
            if (host == null || host.isEmpty()) {
                throw new IllegalArgumentException(
                        "Invalid endpoint '" + endpoint + "'. Expected host, host:port, [ipv6], or [ipv6]:port");
            }
            int port = parsed.getPort() >= 0 ? parsed.getPort() : defaultPort;
            return new URI(scheme, null, host, port, null, null, null).toString();
        } catch (URISyntaxException | IllegalArgumentException e) {
            throw new IllegalArgumentException(
                    "Invalid endpoint '" + endpoint + "'. Expected host, host:port, [ipv6], or [ipv6]:port",
                    e);
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
            Object collectorObject = factory.create(options);
            if (collectorObject == null) {
                throw new IllegalArgumentException(
                        "Class specified by '" + ConnectorOptions.OBSERVATION_COLLECTOR_CLASS +
                                "' returned null ObservationCollector");
            }
            com.rabbitmq.stream.ObservationCollector<?> collector =
                    toObservationCollector(collectorObject);
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

    private static com.rabbitmq.stream.ObservationCollector<?> toObservationCollector(
            Object collectorObject) {
        if (collectorObject instanceof com.rabbitmq.stream.ObservationCollector<?> collector) {
            return collector;
        }
        if (!hasMethod(collectorObject.getClass(), "prePublish", 2)
                || !hasMethod(collectorObject.getClass(), "published", 2)
                || !hasMethod(collectorObject.getClass(), "subscribe", 1)) {
            throw new IllegalArgumentException(
                    "Class specified by '" + ConnectorOptions.OBSERVATION_COLLECTOR_CLASS +
                            "' returned unsupported type '" + collectorObject.getClass().getName() +
                            "', expected com.rabbitmq.stream.ObservationCollector");
        }

        Object bridged = createInterfaceBridge(
                com.rabbitmq.stream.ObservationCollector.class,
                collectorObject);
        @SuppressWarnings("unchecked")
        com.rabbitmq.stream.ObservationCollector<?> collector =
                (com.rabbitmq.stream.ObservationCollector<?>) bridged;
        return collector;
    }

    private static boolean hasMethod(Class<?> type, String name, int parameterCount) {
        for (Method method : type.getMethods()) {
            if (method.getName().equals(name) && method.getParameterCount() == parameterCount) {
                return true;
            }
        }
        return false;
    }

    private static Object createInterfaceBridge(Class<?> targetInterface, Object delegate) {
        return Proxy.newProxyInstance(
                targetInterface.getClassLoader(),
                new Class<?>[] {targetInterface},
                (proxy, method, args) -> invokeBridgedMethod(proxy, delegate, method, args));
    }

    private static Object invokeBridgedMethod(
            Object proxy, Object delegate, Method method, Object[] args)
            throws Throwable {
        if (method.getDeclaringClass() == Object.class) {
            return method.invoke(delegate, args);
        }
        if (method.isDefault()) {
            return InvocationHandler.invokeDefault(proxy, method, args);
        }

        Method delegateMethod = findByNameAndArity(delegate.getClass(), method);
        if (delegateMethod == null) {
            throw new IllegalArgumentException(
                    "Could not bridge method '" + method.getName() + "' for type '" +
                            delegate.getClass().getName() + "'");
        }

        Object[] sourceArgs = args == null ? new Object[0] : args;
        Object[] adaptedArgs = adaptArguments(sourceArgs, delegateMethod.getParameterTypes());
        try {
            Object result = delegateMethod.invoke(delegate, adaptedArgs);
            return adaptValue(result, method.getReturnType());
        } catch (InvocationTargetException e) {
            Throwable cause = e.getCause();
            throw cause != null ? cause : e;
        }
    }

    private static Method findByNameAndArity(Class<?> delegateType, Method method) {
        for (Method candidate : delegateType.getMethods()) {
            if (candidate.getName().equals(method.getName())
                    && candidate.getParameterCount() == method.getParameterCount()) {
                return candidate;
            }
        }
        return null;
    }

    private static Object[] adaptArguments(Object[] values, Class<?>[] targetTypes) {
        Object[] adapted = new Object[targetTypes.length];
        for (int i = 0; i < targetTypes.length; i++) {
            adapted[i] = adaptValue(values[i], targetTypes[i]);
        }
        return adapted;
    }

    private static Object adaptValue(Object value, Class<?> targetType) {
        if (value == null) {
            return null;
        }
        if (targetType == Void.TYPE || targetType == Void.class) {
            return null;
        }
        if (targetType.isInstance(value)) {
            return value;
        }
        if (targetType.isPrimitive()) {
            return value;
        }
        if (targetType.isInterface()) {
            return createInterfaceBridge(targetType, value);
        }
        throw new IllegalArgumentException(
                "Cannot adapt value of type '" + value.getClass().getName() +
                        "' to '" + targetType.getName() + "'");
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
        Object codecFactoryObject = factory.create(options);
        if (codecFactoryObject == null) {
            throw new IllegalArgumentException(
                    "Class specified by '" + ConnectorOptions.COMPRESSION_CODEC_FACTORY_CLASS +
                            "' returned null CompressionCodecFactory");
        }
        if (!(codecFactoryObject instanceof com.rabbitmq.stream.compression.CompressionCodecFactory codecFactory)) {
            throw new IllegalArgumentException(
                    "Class specified by '" + ConnectorOptions.COMPRESSION_CODEC_FACTORY_CLASS +
                            "' returned unsupported type '" + codecFactoryObject.getClass().getName() +
                            "', expected com.rabbitmq.stream.compression.CompressionCodecFactory");
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
            Object eventLoopGroupObject = factory.create(options);
            if (eventLoopGroupObject == null) {
                throw new IllegalArgumentException(
                        "Class specified by '" + ConnectorOptions.NETTY_EVENT_LOOP_GROUP +
                                "' returned null EventLoopGroup");
            }
            if (!(eventLoopGroupObject instanceof EventLoopGroup eventLoopGroup)) {
                throw new IllegalArgumentException(
                        "Class specified by '" + ConnectorOptions.NETTY_EVENT_LOOP_GROUP +
                                "' returned unsupported type '" + eventLoopGroupObject.getClass().getName() +
                                "', expected io.netty.channel.EventLoopGroup");
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
            Object allocatorObject = factory.create(options);
            if (allocatorObject == null) {
                throw new IllegalArgumentException(
                        "Class specified by '" + ConnectorOptions.NETTY_BYTE_BUF_ALLOCATOR +
                                "' returned null ByteBufAllocator");
            }
            if (!(allocatorObject instanceof ByteBufAllocator allocator)) {
                throw new IllegalArgumentException(
                        "Class specified by '" + ConnectorOptions.NETTY_BYTE_BUF_ALLOCATOR +
                                "' returned unsupported type '" + allocatorObject.getClass().getName() +
                                "', expected io.netty.buffer.ByteBufAllocator");
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
                KeyStore trustStore = loadKeyStore(
                        options.getTlsTruststore(), options.getTlsTruststorePassword());
                TrustManagerFactory tmf = TrustManagerFactory.getInstance(
                        TrustManagerFactory.getDefaultAlgorithm());
                tmf.init(trustStore);
                sslBuilder.trustManager(tmf);
            }

            if (hasKeystore) {
                KeyStore keyStore = loadKeyStore(
                        options.getTlsKeystore(), options.getTlsKeystorePassword());
                KeyManagerFactory kmf = KeyManagerFactory.getInstance(
                        KeyManagerFactory.getDefaultAlgorithm());
                kmf.init(keyStore, passwordChars(options.getTlsKeystorePassword()));
                sslBuilder.keyManager(kmf);
            }

            return sslBuilder.build();
        } catch (GeneralSecurityException | IOException e) {
            throw new IllegalArgumentException("Failed to initialize TLS keystore/truststore", e);
        }
    }

    private static KeyStore loadKeyStore(String path, String password)
            throws GeneralSecurityException, IOException {
        String lowerPath = path == null ? "" : path.toLowerCase(java.util.Locale.ROOT);
        List<String> candidateTypes = new ArrayList<>();
        if (lowerPath.endsWith(".jks")) {
            candidateTypes.add("JKS");
        } else if (lowerPath.endsWith(".p12") || lowerPath.endsWith(".pfx")) {
            candidateTypes.add("PKCS12");
        }
        candidateTypes.add(KeyStore.getDefaultType());
        candidateTypes.add("JKS");
        candidateTypes.add("PKCS12");

        GeneralSecurityException lastSecurityError = null;
        IOException lastIoError = null;
        for (String type : new LinkedHashSet<>(candidateTypes)) {
            try (FileInputStream in = new FileInputStream(path)) {
                KeyStore keyStore = KeyStore.getInstance(type);
                keyStore.load(in, passwordChars(password));
                return keyStore;
            } catch (GeneralSecurityException e) {
                lastSecurityError = e;
            } catch (IOException e) {
                lastIoError = e;
            }
        }
        if (lastSecurityError != null) {
            throw lastSecurityError;
        }
        if (lastIoError != null) {
            throw lastIoError;
        }
        throw new GeneralSecurityException("Unable to load keystore: " + path);
    }

    private static char[] passwordChars(String password) {
        return password != null ? password.toCharArray() : new char[0];
    }
}
