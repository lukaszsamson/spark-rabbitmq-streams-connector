package com.rabbitmq.spark.connector;

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
            String addressResolverClass
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
                    options.getAddressResolverClass()
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

    private final ScheduledExecutorService evictionScheduler =
            Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "rmq-env-pool-evictor");
                t.setDaemon(true);
                return t;
            });

    private EnvironmentPool() {}

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
            PooledEntry existing = pool.get(key);
            if (existing != null) {
                // Try to increment refcount; if it's > 0, the entry is still alive
                int current = existing.refCount.get();
                if (current > 0 && existing.refCount.compareAndSet(current, current + 1)) {
                    // Cancel pending eviction if any
                    ScheduledFuture<?> eviction = existing.evictionTask;
                    if (eviction != null) {
                        eviction.cancel(false);
                        existing.evictionTask = null;
                    }
                    LOG.debug("Reusing pooled environment for key {}, refCount={}",
                            key.endpoints(), current + 1);
                    return existing.environment;
                }
                // Entry is being evicted (refCount=0), remove and create new
                pool.remove(key, existing);
            }

            // Create a new environment
            Environment env = EnvironmentBuilderHelper.buildEnvironment(options);
            PooledEntry newEntry = new PooledEntry(env);
            PooledEntry prev = pool.putIfAbsent(key, newEntry);
            if (prev == null) {
                LOG.info("Created new pooled environment for key {}", key.endpoints());
                return env;
            }
            // Another thread beat us; close our env and retry
            try {
                env.close();
            } catch (Exception e) {
                LOG.debug("Error closing duplicate environment", e);
            }
            // Loop to try acquiring the one that was inserted
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
            entry.evictionTask = evictionScheduler.schedule(
                    () -> evict(key, entry), idleTimeoutMs, TimeUnit.MILLISECONDS);
        }
    }

    private void evict(EnvironmentKey key, PooledEntry entry) {
        // Only evict if refCount is still 0 (no one reacquired)
        if (entry.refCount.get() == 0 && pool.remove(key, entry)) {
            LOG.info("Evicting idle environment for key {}", key.endpoints());
            try {
                entry.environment.close();
            } catch (Exception e) {
                LOG.warn("Error closing evicted environment for key {}", key.endpoints(), e);
            }
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
            entry.refCount.set(0);
            ScheduledFuture<?> eviction = entry.evictionTask;
            if (eviction != null) {
                eviction.cancel(false);
            }
            try {
                entry.environment.close();
            } catch (Exception e) {
                LOG.debug("Error closing environment during pool shutdown", e);
            }
        });
        pool.clear();
    }
}
