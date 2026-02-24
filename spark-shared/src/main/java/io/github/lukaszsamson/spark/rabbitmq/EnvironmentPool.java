package io.github.lukaszsamson.spark.rabbitmq;

import com.rabbitmq.stream.Environment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Executor-side JVM singleton pool of RabbitMQ {@link Environment} instances.
 *
 * <p>Environments are keyed by connection configuration (endpoints, credentials,
 * TLS settings, etc.). Callers {@link #acquire(ConnectorOptions)} an environment
 * (creating one if necessary) and {@link #release(ConnectorOptions)} it when done.
 * When the reference count drops to zero, the environment is scheduled for eviction
 * after a configurable idle timeout. A subsequent acquire before eviction fires
 * cancels the eviction and reuses the existing environment.
 *
 * <p>This avoids creating a new TCP connection for every partition reader or writer
 * task on the same executor JVM.
 */
final class EnvironmentPool {

    private static final Logger LOG = LoggerFactory.getLogger(EnvironmentPool.class);

    private static final EnvironmentPool INSTANCE = new EnvironmentPool();

    static EnvironmentPool getInstance() {
        return INSTANCE;
    }

    /**
     * Connection identity key. Two ConnectorOptions with the same key will
     * share an Environment.
     */
    record EnvironmentKey(
            String endpoints,
            String uris,
            String username,
            String password,
            String vhost,
            boolean tls,
            boolean tlsTrustAll,
            String tlsTruststore,
            String tlsTruststorePassword,
            String tlsKeystore,
            String tlsKeystorePassword,
            String addressResolverClass,
            String observationCollectorClass,
            String observationRegistryProviderClass,
            boolean lazyInitialization,
            String environmentId,
            Long rpcTimeoutMs,
            Long requestedHeartbeatSeconds,
            Boolean forceReplicaForConsumers,
            Boolean forceLeaderForProducers,
            Integer locatorConnectionCount,
            String recoveryBackOffDelayPolicy,
            String topologyUpdateBackOffDelayPolicy,
            Integer maxProducersByConnection,
            Integer maxConsumersByConnection,
            Integer maxTrackingConsumersByConnection,
            String scheduledExecutorService,
            String nettyEventLoopGroup,
            String nettyByteBufAllocator,
            String nettyChannelCustomizer,
            String nettyBootstrapCustomizer,
            String compressionCodecFactoryClass
    ) {
        static EnvironmentKey from(ConnectorOptions options) {
            return new EnvironmentKey(
                    options.getEndpoints(),
                    options.getUris(),
                    options.getUsername(),
                    options.getPassword(),
                    options.getVhost(),
                    options.isTls(),
                    options.isTlsTrustAll(),
                    options.getTlsTruststore(),
                    options.getTlsTruststorePassword(),
                    options.getTlsKeystore(),
                    options.getTlsKeystorePassword(),
                    options.getAddressResolverClass(),
                    options.getObservationCollectorClass(),
                    options.getObservationRegistryProviderClass(),
                    options.isLazyInitialization(),
                    options.getEnvironmentId(),
                    options.getRpcTimeoutMs(),
                    options.getRequestedHeartbeatSeconds(),
                    options.getForceReplicaForConsumers(),
                    options.getForceLeaderForProducers(),
                    options.getLocatorConnectionCount(),
                    options.getRecoveryBackOffDelayPolicy(),
                    options.getTopologyUpdateBackOffDelayPolicy(),
                    options.getMaxProducersByConnection(),
                    options.getMaxConsumersByConnection(),
                    options.getMaxTrackingConsumersByConnection(),
                    options.getScheduledExecutorService(),
                    options.getNettyEventLoopGroup(),
                    options.getNettyByteBufAllocator(),
                    options.getNettyChannelCustomizer(),
                    options.getNettyBootstrapCustomizer(),
                    options.getCompressionCodecFactoryClass()
            );
        }
    }

    private static final class PooledEntry {
        final Environment environment;
        final AtomicInteger refCount = new AtomicInteger(1);
        volatile ScheduledFuture<?> evictionTask;

        PooledEntry(Environment environment) {
            this.environment = Objects.requireNonNull(environment);
        }
    }

    private final ConcurrentHashMap<EnvironmentKey, PooledEntry> pool = new ConcurrentHashMap<>();

    private final Object evictionSchedulerLock = new Object();
    private volatile ScheduledExecutorService evictionScheduler = createEvictionScheduler();

    private EnvironmentPool() {
        Runtime.getRuntime().addShutdownHook(new Thread(this::shutdownEvictionScheduler,
                "rmq-env-pool-shutdown"));
    }

    /**
     * Acquire an Environment for the given options. If a pooled environment
     * exists for the same connection key, its reference count is incremented.
     * Otherwise, a new environment is created and added to the pool.
     *
     * @param options connector options (connection parameters)
     * @return a shared Environment instance
     */
    Environment acquire(ConnectorOptions options) {
        EnvironmentKey key = EnvironmentKey.from(options);

        while (true) {
            // Fast path: reuse existing entry if present.
            PooledEntry existing = pool.get(key);
            if (existing != null) {
                Environment acquired = tryAcquireExistingEntry(key, existing);
                if (acquired != null) {
                    return acquired;
                }
                continue;
            }

            // Create outside map atomic operations to avoid blocking CHM bin locks.
            Environment createdEnvironment = EnvironmentBuilderHelper.buildEnvironment(options);
            PooledEntry createdEntry = new PooledEntry(createdEnvironment);
            PooledEntry raced = pool.putIfAbsent(key, createdEntry);
            if (raced == null) {
                LOG.info("Created new pooled environment for key {}", key.endpoints());
                return createdEnvironment;
            }

            closeUnusedEnvironment(key, createdEnvironment);
            Environment acquired = tryAcquireExistingEntry(key, raced);
            if (acquired != null) {
                return acquired;
            }
        }
    }

    private Environment tryAcquireExistingEntry(EnvironmentKey key, PooledEntry entry) {
        synchronized (entry) {
            if (pool.get(key) != entry) {
                return null;
            }
            int current = entry.refCount.get();
            if (current < 0) {
                pool.remove(key, entry);
                return null;
            }
            entry.refCount.set(current + 1);
            ScheduledFuture<?> eviction = entry.evictionTask;
            if (eviction != null) {
                eviction.cancel(false);
                entry.evictionTask = null;
            }
            LOG.debug("Reusing pooled environment for key {}, refCount={}",
                    key.endpoints(), current + 1);
            return entry.environment;
        }
    }

    private void closeUnusedEnvironment(EnvironmentKey key, Environment environment) {
        try {
            environment.close();
        } catch (Exception e) {
            LOG.debug("Error closing race-created environment for key {}", key.endpoints(), e);
        }
    }

    /**
     * Release a reference to the pooled Environment for the given options.
     * When the reference count drops to zero, the environment is scheduled
     * for eviction after the configured idle timeout.
     *
     * @param options connector options (must match the acquire call)
     */
    void release(ConnectorOptions options) {
        EnvironmentKey key = EnvironmentKey.from(options);
        PooledEntry entry = pool.get(key);
        if (entry == null) {
            LOG.warn("Release called for unknown environment key {}", key.endpoints());
            return;
        }

        synchronized (entry) {
            int remaining = entry.refCount.decrementAndGet();
            if (remaining < 0) {
                LOG.warn("Environment refCount went negative for key {}", key.endpoints());
                entry.refCount.set(0);
                return;
            }

            if (remaining == 0) {
                long idleTimeoutMs = options.getEnvironmentIdleTimeoutMs();
                LOG.debug("Scheduling eviction for environment key {} in {}ms",
                        key.endpoints(), idleTimeoutMs);
                ScheduledFuture<?> previousEviction = entry.evictionTask;
                if (previousEviction != null) {
                    previousEviction.cancel(false);
                }
                entry.evictionTask = getOrCreateEvictionScheduler().schedule(
                        () -> evict(key, entry), idleTimeoutMs, TimeUnit.MILLISECONDS);
            }
        }
    }

    private void evict(EnvironmentKey key, PooledEntry entry) {
        boolean removed;
        synchronized (entry) {
            if (pool.get(key) != entry) {
                return;
            }
            // Final check + removal must be atomic with 0->1 reacquire.
            if (entry.refCount.get() != 0) {
                entry.evictionTask = null;
                return;
            }
            removed = pool.remove(key, entry);
            if (removed) {
                entry.evictionTask = null;
            }
        }
        if (!removed) {
            return;
        }
        LOG.info("Evicting idle environment for key {}", key.endpoints());
        try {
            entry.environment.close();
        } catch (Exception e) {
            LOG.warn("Error closing evicted environment for key {}", key.endpoints(), e);
        }
    }

    /**
     * Returns the current number of pooled environments.
     * Intended for testing and monitoring.
     */
    int size() {
        return pool.size();
    }

    /**
     * Closes all pooled environments and clears the pool.
     * Intended for testing cleanup.
     */
    void closeAll() {
        pool.forEach((key, entry) -> {
            synchronized (entry) {
                entry.refCount.set(0);
                ScheduledFuture<?> eviction = entry.evictionTask;
                if (eviction != null) {
                    eviction.cancel(false);
                    entry.evictionTask = null;
                }
                try {
                    entry.environment.close();
                } catch (Exception e) {
                    LOG.debug("Error closing environment during pool shutdown", e);
                }
            }
        });
        pool.clear();
        shutdownEvictionScheduler();
    }

    // Visible for tests
    void shutdownEvictionScheduler() {
        synchronized (evictionSchedulerLock) {
            evictionScheduler.shutdownNow();
        }
    }

    // Visible for tests
    boolean isEvictionSchedulerShutdown() {
        return evictionScheduler.isShutdown();
    }

    private ScheduledExecutorService getOrCreateEvictionScheduler() {
        ScheduledExecutorService scheduler = evictionScheduler;
        if (!scheduler.isShutdown()) {
            return scheduler;
        }
        synchronized (evictionSchedulerLock) {
            if (evictionScheduler.isShutdown()) {
                evictionScheduler = createEvictionScheduler();
            }
            return evictionScheduler;
        }
    }

    private static ScheduledExecutorService createEvictionScheduler() {
        return Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "rmq-env-pool-evictor");
            t.setDaemon(true);
            return t;
        });
    }
}
