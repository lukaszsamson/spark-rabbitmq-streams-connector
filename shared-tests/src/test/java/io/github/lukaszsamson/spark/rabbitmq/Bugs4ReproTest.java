package io.github.lukaszsamson.spark.rabbitmq;

import org.apache.spark.sql.connector.read.streaming.MicroBatchStream;
import org.apache.spark.sql.connector.read.streaming.Offset;
import org.apache.spark.sql.connector.read.streaming.ReadLimit;
import org.apache.spark.sql.connector.read.streaming.SupportsAdmissionControl;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit-level reproductions and regression tests for the issues documented in
 * {@code sparkling_rabbit_demo/BUGS-4.md} (discovered 2026-05-07 against
 * connector commit {@code ea6beac}).
 *
 * <p>BUG-4-1 and BUG-4-4 turned out to be infrastructure / test-design issues,
 * not connector regressions — see the comment blocks below. The two remaining
 * issues (BUG-4-2 and BUG-4-3 / BUG-4-5) reproduce here and are pinned by
 * passing tests against the fixes applied in:
 * <ul>
 *   <li>{@link BaseRabbitMQMicroBatchStream} — invocation-cache wall-clock
 *       bypass for {@code ReadMinRows} (fixes BUG-4-2).</li>
 *   <li>{@link RabbitMQScan} — split-budget retry on slow consumer attach for
 *       timestamp probes (fixes BUG-4-3 / BUG-4-5).</li>
 * </ul>
 */
class Bugs4ReproTest {

    // ====================================================================
    // BUG-4-3 / BUG-4-5 — timestamp probe budget eaten by slow consumer attach
    //
    // Root-cause analysis (after reading BUGS_TRIAGE_GPT_3.txt §GPT3-2 and
    // BUGS_TRIAGE_CLAUDE_3.txt §GPT3-2):
    //
    // Commit 8835fe4 ("Fix timestamp probe timeout semantics (GPT3-2,
    // CLAUDE3-6)") was correct in principle — silently falling back to
    // resolveLatestOffset on probe timeout conflated "broker confirmed
    // no match" (NoOffsetException) with "we can't tell" (timeout), and
    // could over-include / skip records. The triage explicitly wants
    // INCONCLUSIVE timeouts to fail planning, NOT silently fall through
    // to tail. Reverting to the pre-8835fe4 behavior would re-introduce
    // GPT3-2.
    //
    // The integration symptom (BUGS-4.md BUG-4-3) is different from
    // GPT3-2's data-loss concern: on a remote broker behind a load
    // balancer, the consumer's `build()` synchronously attaches, and
    // that attach can eat the full pollTimeoutMs budget before any
    // message is observed. The bug report itself notes the latest-offset
    // probe was given an escalating retry ladder (250 ms / 1 s / 5 s)
    // precisely because remote brokers regularly need >250 ms to attach
    // and the timestamp probes lacked the same hardening.
    //
    // Fix: split the pollTimeoutMs budget into two attempts and rebuild
    // the probe consumer between them (RabbitMQScan.splitProbeBudget).
    // A transient slow attach is recovered on attempt 2 within the same
    // total budget, while truly INCONCLUSIVE cases still hit the throw —
    // so commit 8835fe4's correctness fix is preserved.
    // ====================================================================

    @Nested
    @DisplayName("BUG-4-3 / BUG-4-5: endingOffsets=timestamp probe must retry on slow attach")
    class TimestampEndProbeTimeout {

        @Test
        @DisplayName("Slow first attach is recovered by a second probe attempt within total pollTimeoutMs")
        void timestampEndingProbeRetriesOnSlowAttach() throws Exception {
            // pollTimeoutMs=2000 → splitProbeBudget gives [1000ms, 1000ms].
            // First probe attempt: silent — never delivers a message.
            // Second probe attempt: delivers offset 17 with ts == endingTs immediately.
            // Without the retry, the resolver would fail the first 1000 ms wait and throw.
            // With the retry, it succeeds on attempt 2 and returns 17.
            long endingTs = 1_700_000_000_000L;
            Map<String, String> opts = baseStreamOpts();
            opts.put("endingOffsets", "timestamp");
            opts.put("endingTimestamp", String.valueOf(endingTs));
            opts.put("pollTimeoutMs", "2000");
            RabbitMQScan scan = new RabbitMQScan(new ConnectorOptions(opts), schema());

            RetryProbeEnvironment env = new RetryProbeEnvironment(
                    new Stats(0L, false, false, 99L),
                    /* deliverOnAttempt= */ 2,
                    /* observedOffset= */ 17L,
                    /* observedTimestamp= */ endingTs);

            Long resolved = invokeResolveTimestampEndingOffset(scan, env, "s1", endingTs);
            assertThat(resolved)
                    .as("BUG-4-3: probe must retry once on slow attach so a transient first-shot "
                            + "stall does not consume the entire pollTimeoutMs budget.")
                    .isEqualTo(17L);
            assertThat(env.attemptCount())
                    .as("Resolver should attempt at least twice when the first probe is silent.")
                    .isGreaterThanOrEqualTo(2);
        }

        @Test
        @DisplayName("Prove-absence: sparse stream with last_msg.ts < cutoff returns tail without timestamp probe")
        void timestampEndingProveAbsenceReturnsTailOnSparseStream() throws Exception {
            // BUG-4-3 root scenario: ls-test had no recent traffic, so
            // OffsetSpecification.timestamp(now-30s) attached at the tail and waited
            // forever for a chunk that never arrived. The triage (GPT3-2) suggested
            // "use a resolver that can prove absence" as an alternative to the strict
            // throw-on-timeout. The prove-absence pre-check inspects the last message's
            // timestamp; when it is strictly before the cutoff, all currently-available
            // messages are PROVABLY before the cutoff and the tail (= last_offset + 1)
            // is the deterministic exclusive end.
            long endingTs = 1_700_000_000_000L;
            long lastMessageTs = endingTs - 5_000L;       // last message older than cutoff
            Map<String, String> opts = baseStreamOpts();
            opts.put("endingOffsets", "timestamp");
            opts.put("endingTimestamp", String.valueOf(endingTs));
            opts.put("pollTimeoutMs", "1500");
            RabbitMQScan scan = new RabbitMQScan(new ConnectorOptions(opts), schema());

            // Scripted env delivers immediately on attempt 1. The first attempt the
            // resolver makes is the prove-absence probe (OffsetSpec.last()). Its
            // callback receives offset=42 with chunk_ts < endingTs → resolver returns
            // 42 + 1 = 43 without ever invoking the regular timestamp probe.
            // Note: proveAllBeforeCutoff tracks the MAXIMUM observed messageOrChunkTimestamp
            // across the chunk (not just the last message). A publisher cannot spoof
            // absence by delivering the last message with a backdated creation_time
            // below the cutoff if an earlier message in the same chunk exceeds it (P2 fix).
            RetryProbeEnvironment env = new RetryProbeEnvironment(
                    new Stats(0L, false, false, 99L),
                    /* deliverOnAttempt= */ 1,
                    /* observedOffset= */ 42L,
                    /* observedTimestamp= */ lastMessageTs);

            Long resolved = invokeResolveTimestampEndingOffset(scan, env, "s1", endingTs);
            assertThat(resolved)
                    .as("BUG-4-3 prove-absence: when last_msg.ts < cutoff, resolver must "
                            + "return last_offset + 1 (= 43) instead of throwing.")
                    .isEqualTo(43L);
            assertThat(env.attemptCount())
                    .as("Only the prove-absence probe should run; the regular timestamp probe "
                            + "must be short-circuited.")
                    .isEqualTo(1);
        }

        @Test
        @DisplayName("Prove-absence inconclusive (last_msg.ts >= cutoff) falls through to regular probe")
        void timestampEndingProveAbsenceFallsThroughWhenLastMessageIsAtOrAfter() throws Exception {
            // When the last message is at-or-after the cutoff, prove-absence cannot
            // claim the tail. The resolver must fall through to the regular timestamp
            // probe (which then either finds the boundary or throws).
            long endingTs = 1_700_000_000_000L;
            Map<String, String> opts = baseStreamOpts();
            opts.put("endingOffsets", "timestamp");
            opts.put("endingTimestamp", String.valueOf(endingTs));
            opts.put("pollTimeoutMs", "1500");
            RabbitMQScan scan = new RabbitMQScan(new ConnectorOptions(opts), schema());

            // Scripted env: every attempt delivers offset=17 with chunk_ts == endingTs.
            // Attempt 1 = prove-absence probe → last_msg.ts (== endingTs) NOT < cutoff,
            //   returns -1L (inconclusive).
            // Attempt 2 = regular timestamp probe → handler sees ts >= endingTs,
            //   countdown fires, returns 17.
            RetryProbeEnvironment env = new RetryProbeEnvironment(
                    new Stats(0L, false, false, 99L),
                    /* deliverOnAttempt= */ 1,
                    /* observedOffset= */ 17L,
                    /* observedTimestamp= */ endingTs);

            Long resolved = invokeResolveTimestampEndingOffset(scan, env, "s1", endingTs);
            assertThat(resolved)
                    .as("Last message at-or-after cutoff: prove-absence must NOT claim the "
                            + "tail (would over-include); resolver falls through to the "
                            + "regular timestamp probe which finds the boundary at 17.")
                    .isEqualTo(17L);
            assertThat(env.attemptCount())
                    .as("Both the prove-absence probe and the regular timestamp probe should run.")
                    .isGreaterThanOrEqualTo(2);
        }

        @Test
        @DisplayName("Truly INCONCLUSIVE timeout (every attempt silent) still fails per GPT3-2")
        void timestampEndingProbeTrulyInconclusiveStillFails() throws Exception {
            // No probe attempt ever delivers a message at/after the cutoff. The triage
            // decision (GPT3-2) is "fail planning" — silently jumping to tail would risk
            // over-including records published after the cutoff. Verify the throw is
            // preserved even when the retry exhausts all attempts.
            long endingTs = 1_700_000_000_000L;
            Map<String, String> opts = baseStreamOpts();
            opts.put("endingOffsets", "timestamp");
            opts.put("endingTimestamp", String.valueOf(endingTs));
            opts.put("pollTimeoutMs", "1500");
            RabbitMQScan scan = new RabbitMQScan(new ConnectorOptions(opts), schema());

            RetryProbeEnvironment env = new RetryProbeEnvironment(
                    new Stats(0L, false, false, 99L),
                    /* deliverOnAttempt= */ Integer.MAX_VALUE,
                    /* observedOffset= */ 0L,
                    /* observedTimestamp= */ 0L);

            try {
                invokeResolveTimestampEndingOffset(scan, env, "s1", endingTs);
                throw new AssertionError("Expected TimestampResolutionTimeoutException");
            } catch (TimestampResolutionTimeoutException expected) {
                assertThat(expected.getMessage())
                        .contains("Timed out resolving ending timestamp")
                        .contains("probe attempts");
            }
        }
    }

    // ====================================================================
    // BUG-4-1 — availableNow second run after checkpoint commit hangs
    //
    // Status: NOT A CONNECTOR REGRESSION on this code path.
    //
    // Reproduced against the live broker (rabbit.simplefx.com:5552 with
    // the Spark Connect cluster from sparkling_rabbit_demo/). The first
    // availableNow run hung at the very first batch, but the cause is in
    // the executor stderr — not in the connector:
    //
    //   $SPARK_HOME/work/app-*/<id>/stderr:
    //     Caused by: java.io.IOException: Failed to connect to
    //       ip-10-16-50-177.eu-central-1.compute.internal/10.16.50.177:59734
    //     Caused by: io.netty.channel.AbstractChannel$AnnotatedConnectException:
    //       finishConnect(..) failed with error(-60): Operation timed out
    //
    // The Spark driver advertises a stale AWS-internal hostname; local
    // executors cannot reach it, so tasks never run and the batch never
    // commits. The connector's unit-level invariant (start == snapshot
    // implies latestOffset returns start) holds.
    //
    // Integration repro is tests/test_available_now_checkpoint.py — needs a
    // Spark cluster with a reachable driver hostname (SPARK_LOCAL_IP=127.0.0.1
    // or similar) before it can isolate any real connector issue.
    // ====================================================================

    // ====================================================================
    // BUG-4-2 — minOffsetsPerTrigger + maxTriggerDelay never delivers data
    //
    // Root-cause analysis:
    //
    // Commit 5e3ef1c added a 250 ms latestOffset(start, limit) invocation
    // cache to "keep per-trigger planning stable" — useful when Spark
    // double-calls latestOffset for the same trigger. But the cache memoizes
    // the FULL result, including the wall-clock-dependent decision made by
    // handleReadMinRowsCore (which compares now() against lastTriggerMillis
    // + maxTriggerDelayMs). A cached "skip" result outlives the deadline
    // when maxTriggerDelay falls inside the cache window, suppressing the
    // delay-expiry release.
    //
    // Fix: bypass the invocation cache for any limit that contains a
    // ReadMinRows component. The broker-tail probe cache (latestTailProbeCache)
    // is unaffected, so we still avoid per-trigger consumer churn.
    // ====================================================================

    @Nested
    @DisplayName("BUG-4-2: ReadMinRows + maxTriggerDelay must fire at delay expiry, not stale cache")
    class MinOffsetsTriggerDelayCache {

        @Test
        @DisplayName("Two latestOffset calls with same key bracketing the delay window — second must release")
        void readMinRowsDelayExpiryNotMaskedByInvocationCache() throws Exception {
            // availableNowSnapshot pins the tail at 1_000 so latestOffset is deterministic.
            // maxTriggerDelay = 50 ms < cache TTL (250 ms). Pre-fix, the cached "skip"
            // result from trigger 1 was returned for trigger 2 even after the delay had
            // already expired, hiding the maxTriggerDelay deadline. Post-fix, ReadMinRows
            // bypasses the invocation cache so trigger 2 re-evaluates handleReadMinRowsCore.
            Map<String, String> opts = baseStreamOpts();
            opts.put("startingOffsets", "earliest");

            MicroBatchStream stream = createStream(opts);
            try {
                Map<String, Long> snapshot = Map.of("test-stream", 1_000L);
                setPrivateField(stream, "availableNowSnapshot", snapshot);

                RabbitMQStreamOffset start = new RabbitMQStreamOffset(Map.of("test-stream", 10L));
                ReadLimit minRows = ReadLimit.minRows(999_999L, 50L);

                Offset first = ((SupportsAdmissionControl) stream).latestOffset(start, minRows);
                assertThat(((RabbitMQStreamOffset) first).getStreamOffsets())
                        .as("Trigger 1 inside the delay window: skip — return start.")
                        .containsEntry("test-stream", 10L);

                Thread.sleep(80L);

                Offset second = ((SupportsAdmissionControl) stream).latestOffset(start, minRows);
                assertThat(((RabbitMQStreamOffset) second).getStreamOffsets())
                        .as("BUG-4-2: after maxTriggerDelay expires, ReadMinRows must release "
                                + "the batch (return tail) — the invocation cache must not mask "
                                + "the wall-clock deadline.")
                        .containsEntry("test-stream", 1_000L);
            } finally {
                closeQuietly(stream);
            }
        }
    }

    // ====================================================================
    // BUG-4-4 — startingOffsets=latest streaming on ls-test produces no rows
    //
    // Status: NOT A CONNECTOR BUG — invalid scenario.
    //
    // After commit 394c5da ("Remove broker-offset resume path and rename
    // storeBrokerOffsets option"), `serverSideOffsetTracking` is an alias
    // for `storeBrokerOffsets` and gates ONLY a write-side telemetry path
    // (persistBrokerOffsets at BaseRabbitMQMicroBatchStream.java:2160-2164,
    // the only call site). The read path does not consult this option, so
    // enabling SST cannot stall reading.
    //
    // The integration test
    // (tests/test_streaming_resume_edges.py::latest_stop_resume_with_sst_checkpoint)
    // starts startingOffsets=latest on `ls-test` and waits 45 s for >=1 CSV
    // row WITHOUT actively publishing in phase 1. The sibling test
    // available_now_latest_checkpoint_idempotent passes on the same stream
    // because it does call _write_marker_messages first. The "regression"
    // is the test's missing publisher, not the connector.
    // ====================================================================

    // -------- helpers --------

    private static Map<String, String> baseStreamOpts() {
        Map<String, String> opts = new LinkedHashMap<>();
        opts.put("endpoints", "localhost:5552");
        opts.put("stream", "test-stream");
        return opts;
    }

    private static StructType schema() {
        return RabbitMQStreamTable.buildSourceSchema(
                new ConnectorOptions(baseStreamOpts()).getMetadataFields());
    }

    private static MicroBatchStream createStream(Map<String, String> opts) {
        ConnectorOptions options = new ConnectorOptions(opts);
        var s = RabbitMQStreamTable.buildSourceSchema(options.getMetadataFields());
        return new RabbitMQScan(options, s).toMicroBatchStream("/tmp/checkpoint-bug4-repro");
    }

    private static void closeQuietly(MicroBatchStream stream) {
        try {
            stream.stop();
        } catch (RuntimeException ignored) {
            // Best-effort cleanup.
        }
    }

    private static void setPrivateField(Object target, String name, Object value) throws Exception {
        Class<?> c = target.getClass();
        while (c != null) {
            try {
                Field f = c.getDeclaredField(name);
                f.setAccessible(true);
                f.set(target, value);
                return;
            } catch (NoSuchFieldException ignored) {
                c = c.getSuperclass();
            }
        }
        throw new NoSuchFieldException(name);
    }

    private static Long invokeResolveTimestampEndingOffset(
            RabbitMQScan scan,
            com.rabbitmq.stream.Environment env,
            String stream,
            long timestamp) throws Exception {
        Method method = RabbitMQScan.class.getDeclaredMethod(
                "resolveTimestampEndingOffset",
                com.rabbitmq.stream.Environment.class, String.class, long.class);
        method.setAccessible(true);
        try {
            return (Long) method.invoke(scan, env, stream, timestamp);
        } catch (java.lang.reflect.InvocationTargetException e) {
            Throwable cause = e.getCause();
            if (cause instanceof RuntimeException re) {
                throw re;
            }
            throw e;
        }
    }

    /**
     * Test fixture that simulates a flaky broker: the first {@code deliverOnAttempt - 1}
     * consumer builds return a silent consumer (no message ever delivered), modeling a
     * slow attach that consumes the per-attempt budget. From {@code deliverOnAttempt}
     * onward the consumer delivers a single message synchronously on build.
     */
    private static final class RetryProbeEnvironment implements com.rabbitmq.stream.Environment {

        private final com.rabbitmq.stream.StreamStats stats;
        private final int deliverOnAttempt;
        private final long observedOffset;
        private final long observedTimestamp;
        private final AtomicInteger attempts = new AtomicInteger();

        RetryProbeEnvironment(com.rabbitmq.stream.StreamStats stats,
                              int deliverOnAttempt,
                              long observedOffset,
                              long observedTimestamp) {
            this.stats = stats;
            this.deliverOnAttempt = deliverOnAttempt;
            this.observedOffset = observedOffset;
            this.observedTimestamp = observedTimestamp;
        }

        int attemptCount() {
            return attempts.get();
        }

        @Override
        public com.rabbitmq.stream.StreamStats queryStreamStats(String stream) {
            return stats;
        }

        @Override
        public com.rabbitmq.stream.ConsumerBuilder consumerBuilder() {
            int attempt = attempts.incrementAndGet();
            boolean shouldDeliver = attempt >= deliverOnAttempt;
            return new ScriptedConsumerBuilder(shouldDeliver, observedOffset, observedTimestamp);
        }

        @Override public com.rabbitmq.stream.StreamCreator streamCreator() { throw new UnsupportedOperationException(); }
        @Override public void deleteStream(String stream) { throw new UnsupportedOperationException(); }
        @Override public void deleteSuperStream(String superStream) { throw new UnsupportedOperationException(); }
        @Override public void storeOffset(String reference, String stream, long offset) { throw new UnsupportedOperationException(); }
        @Override public boolean streamExists(String stream) { throw new UnsupportedOperationException(); }
        @Override public com.rabbitmq.stream.ProducerBuilder producerBuilder() { throw new UnsupportedOperationException(); }
        @Override public void close() { }
    }

    /**
     * Consumer builder that either silently builds (no callback) or fires exactly one
     * message on {@code build()} with a configured offset and chunk timestamp. The
     * timestamp probe's MessageHandler is invoked synchronously inside build() so the
     * subsequent await/poll returns immediately.
     */
    private static final class ScriptedConsumerBuilder implements com.rabbitmq.stream.ConsumerBuilder {

        private final boolean deliver;
        private final long offset;
        private final long timestamp;
        private com.rabbitmq.stream.MessageHandler handler;

        ScriptedConsumerBuilder(boolean deliver, long offset, long timestamp) {
            this.deliver = deliver;
            this.offset = offset;
            this.timestamp = timestamp;
        }

        @Override public com.rabbitmq.stream.ConsumerBuilder stream(String stream) { return this; }
        @Override public com.rabbitmq.stream.ConsumerBuilder superStream(String superStream) { return this; }
        @Override public com.rabbitmq.stream.ConsumerBuilder offset(com.rabbitmq.stream.OffsetSpecification s) { return this; }
        @Override public com.rabbitmq.stream.ConsumerBuilder messageHandler(com.rabbitmq.stream.MessageHandler h) { this.handler = h; return this; }
        @Override public com.rabbitmq.stream.ConsumerBuilder name(String name) { return this; }
        @Override public com.rabbitmq.stream.ConsumerBuilder singleActiveConsumer() { return this; }
        @Override public com.rabbitmq.stream.ConsumerBuilder consumerUpdateListener(com.rabbitmq.stream.ConsumerUpdateListener l) { return this; }
        @Override public com.rabbitmq.stream.ConsumerBuilder subscriptionListener(com.rabbitmq.stream.SubscriptionListener l) { return this; }
        @Override public com.rabbitmq.stream.ConsumerBuilder listeners(com.rabbitmq.stream.Resource.StateListener... l) { return this; }
        @Override public ManualTrackingStrategy manualTrackingStrategy() { throw new UnsupportedOperationException(); }
        @Override public AutoTrackingStrategy autoTrackingStrategy() { throw new UnsupportedOperationException(); }
        @Override public com.rabbitmq.stream.ConsumerBuilder noTrackingStrategy() { return this; }
        @Override public FilterConfiguration filter() { throw new UnsupportedOperationException(); }
        @Override public FlowConfiguration flow() {
            return new FlowConfiguration() {
                @Override public FlowConfiguration initialCredits(int initialCredits) { return this; }
                @Override public FlowConfiguration strategy(com.rabbitmq.stream.ConsumerFlowStrategy strategy) { return this; }
                @Override public com.rabbitmq.stream.ConsumerBuilder builder() { return ScriptedConsumerBuilder.this; }
            };
        }

        @Override
        public com.rabbitmq.stream.Consumer build() {
            if (deliver && handler != null) {
                handler.handle(new ProbeContext(offset, timestamp), null);
            }
            return new com.rabbitmq.stream.Consumer() {
                @Override public void store(long offset) { }
                @Override public long storedOffset() { return 0; }
                @Override public void close() { }
            };
        }
    }

    /**
     * Minimal MessageHandler.Context carrying offset + chunk timestamp.
     * {@code proveAllBeforeCutoff} uses {@link
     * io.github.lukaszsamson.spark.rabbitmq.RabbitMQScan#messageOrChunkTimestamp}
     * (per-message {@code creation_time} when set, otherwise chunk-level timestamp)
     * but tracks the <em>maximum</em> observed timestamp across all messages in the
     * chunk. This fixes the P2 review concern: if any earlier message in the last
     * chunk has {@code creation_time ≥ cutoff}, the maximum will capture it and
     * prove-absence returns {@code -1L} (inconclusive) rather than incorrectly
     * claiming the tail. We pass {@code null} as the message here so the probe
     * falls back to {@code context.timestamp()}, which is the chunk-level timestamp
     * passed to this constructor — matching the scripted sparse-stream scenario.
     */
    private static final class ProbeContext implements com.rabbitmq.stream.MessageHandler.Context {
        private final long offset;
        private final long timestamp;

        ProbeContext(long offset, long timestamp) {
            this.offset = offset;
            this.timestamp = timestamp;
        }

        @Override public long offset() { return offset; }
        @Override public long timestamp() { return timestamp; }
        @Override public long committedChunkId() { return offset; }
        @Override public String stream() { return "s1"; }
        @Override public com.rabbitmq.stream.Consumer consumer() { return null; }
        @Override public void processed() { }
        @Override public void storeOffset() { }
    }

    /** Minimal StreamStats fake. */
    private static final class Stats implements com.rabbitmq.stream.StreamStats {
        private final long firstOffset;
        private final boolean firstOffsetThrows;
        private final boolean committedOffsetThrows;
        private final long committedOffset;

        Stats(long firstOffset, boolean firstOffsetThrows,
              boolean committedOffsetThrows, long committedOffset) {
            this.firstOffset = firstOffset;
            this.firstOffsetThrows = firstOffsetThrows;
            this.committedOffsetThrows = committedOffsetThrows;
            this.committedOffset = committedOffset;
        }

        @Override
        public long firstOffset() {
            if (firstOffsetThrows) throw new com.rabbitmq.stream.NoOffsetException("no offset");
            return firstOffset;
        }

        @Override
        public long committedOffset() {
            if (committedOffsetThrows) throw new com.rabbitmq.stream.NoOffsetException("no offset");
            return committedOffset;
        }

        @Override
        public long committedChunkId() {
            return committedOffset;
        }
    }
}
