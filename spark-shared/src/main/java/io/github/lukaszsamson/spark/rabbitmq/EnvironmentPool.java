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
        final CompletableFuture<Environment> environmentFuture = new CompletableFuture<>();
        volatile Environment environment;
        final AtomicInteger refCount = new AtomicInteger(1);
        volatile ScheduledFuture<?> evictionTask;

        PooledEntry() {
            this.refCount.set(0);
        }

        PooledEntry(Environment environment) {
            Environment validated = Objects.requireNonNull(environment);
            this.environment = validated;
            this.environmentFuture.complete(validated);
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

            // Reserve a shared in-flight slot so concurrent acquires for the same key
            // wait on one connection attempt instead of creating a thundering herd.
            PooledEntry pendingEntry = new PooledEntry();
            PooledEntry raced = pool.putIfAbsent(key, pendingEntry);
            if (raced != null) {
                Environment acquired = tryAcquireExistingEntry(key, raced);
                if (acquired != null) {
                    return acquired;
                }
                continue;
            }

            try {
                Environment createdEnvironment = EnvironmentBuilderHelper.buildEnvironment(options);
                pendingEntry.environment = createdEnvironment;
                pendingEntry.environmentFuture.complete(createdEnvironment);
                LOG.info("Created new pooled environment for key {}", key.endpoints());

                Environment acquired = tryAcquireExistingEntry(key, pendingEntry);
                if (acquired != null) {
                    return acquired;
                }
                closeUnusedEnvironment(key, createdEnvironment);
            } catch (Throwable t) {
                pendingEntry.environmentFuture.completeExceptionally(t);
                pool.remove(key, pendingEntry);
                throw propagateAcquireFailure(t, key);
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
        }

        try {
            return awaitEnvironment(entry, key);
        } catch (RuntimeException e) {
            rollbackFailedAcquire(key, entry);
            throw e;
        }
    }

    private Environment awaitEnvironment(PooledEntry entry, EnvironmentKey key) {
        try {
            Environment environment = entry.environmentFuture.get();
            entry.environment = environment;
            return environment;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(
                    "Interrupted while acquiring environment for key " + key.endpoints(), e);
        } catch (ExecutionException e) {
            Throwable cause = e.getCause() != null ? e.getCause() : e;
            throw propagateAcquireFailure(cause, key);
        }
    }

    private void rollbackFailedAcquire(EnvironmentKey key, PooledEntry entry) {
        synchronized (entry) {
            int remaining = entry.refCount.decrementAndGet();
            if (remaining < 0) {
                entry.refCount.set(0);
                remaining = 0;
            }
            if (remaining == 0 && entry.environmentFuture.isCompletedExceptionally()) {
                pool.remove(key, entry);
            }
        }
    }

    private RuntimeException propagateAcquireFailure(Throwable failure, EnvironmentKey key) {
        if (failure instanceof RuntimeException runtimeException) {
            return runtimeException;
        }
        return new IllegalStateException(
                "Failed to acquire environment for key " + key.endpoints(), failure);
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
            Environment environment = entry.environment;
            if (environment != null) {
                environment.close();
            }
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
