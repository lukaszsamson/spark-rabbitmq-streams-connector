# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [Unreleased]

### Changed
- Release automation now uses Central Publishing Maven Plugin `0.11.0`, waits for automatic publication, and can resume an already-validated Central deployment without uploading duplicate artifacts.

## [0.2.0] - 2026-07-13

### Added
- Spark 4.1 real-time mode support (`SupportsRealTimeMode`) for low-latency Structured Streaming reads; unsupported with `minPartitions`/`maxRecordsPerPartition`/`maxRecordsPerTrigger`/`maxBytesPerTrigger`/`minOffsetsPerTrigger`/`maxWaitMs`.

### Changed
- **Breaking:** Broker-stored offsets are no longer used for query recovery. On a fresh start (no Spark checkpoint), the connector resolves initial offsets from configured `startingOffsets` / `startingOffsetsByTimestamp` only, matching Kafka source semantics. Users that previously relied on broker-stored offsets to resume across query restarts must rely on Spark checkpoints (or set `startingOffsets` explicitly).
- **Breaking (option rename):** `serverSideOffsetTracking` is renamed to `storeBrokerOffsets` and its semantics are clarified as best-effort write-only telemetry. The old name remains accepted as a deprecated alias for one release and emits a warning at parse time.
- Timestamp planning now distinguishes broker-confirmed no-match from probe-budget exhaustion. A timed-out probe fails with `TimestampResolutionTimeoutException` instead of silently falling back to tail or earliest and potentially skipping or over-including records.
- `minOffsetsPerTrigger > maxRecordsPerTrigger` is rejected during source validation.
- Duplicate stream keys in checkpoint offset JSON are rejected instead of silently keeping the last value.
- Bumped RabbitMQ Stream Java client to `1.6.0`, including more resilient compression error handling, tolerant broker-version parsing, a `resolve_offset_spec` availability fix, and dependency updates.

### Fixed
- Stopping a streaming query while a broker statistics request is in flight no longer turns Spark's intentional thread interruption into a terminal `StreamingQueryException`.
- `startingOffsets=timestamp` with a timestamp beyond all currently-available data on a non-empty stream now resolves as a broker-provable no-match instead of burning the full `pollTimeoutMs` probe budget and failing with `TimestampResolutionTimeoutException`. This makes `startingOffsetsByTimestampStrategy=latest` actually reachable in its primary use case (falls back to tail, producing an empty batch until new data arrives); with the default `error` strategy the planner now fails fast with the descriptive no-match error. Applies to both batch planning and streaming `initialOffset` resolution; the starting-timestamp resolvers reuse the same prove-absence pre-check the ending-timestamp resolver already had, and the pre-check's elapsed time is charged against the `pollTimeoutMs` probe budget so planning wall-clock stays bounded.
- Streaming `startingOffsetsByTimestampStrategy=latest` fallback no longer silently skips records published after the query starts. The fallback start is an offset decision, so the per-stream timestamp anchor is disabled: previously the first micro-batch attached readers with `OffsetSpecification.timestamp(ts)`, which never delivers when the timestamp is beyond all data, and the first non-empty offset range committed empty — advancing the checkpoint past unread records. This also fixes the pre-existing empty-stream (`NoOffsetException`) fallback path, which had the same latent silent-skip.
- Structured Streaming checkpoint resume no longer stalls or skips post-checkpoint messages when `storeBrokerOffsets=true` inserts tracking-only physical offsets into a stream.
- Tail discovery now probes past tracking-only entries and prefers observed message offsets when broker statistics lag fresh publishes or temporarily overshoot after stream recreation.
- Superstream partition churn treats temporarily unavailable partitions consistently with missing partitions: strict mode fails, while `failOnDataLoss=false` skips and recovers.
- Failed micro-batches retain their planned offset range for replay instead of advancing the checkpoint past unread records.
- Broker offset persistence invalidates the per-stream statistics cache so subsequent planning observes newly committed telemetry.
- Sink publishing-ID high-watermark, publisher-confirm accounting, and retry identity handling were hardened to preserve streaming deduplication guarantees.
- Matching an `application_properties` `routing_key` entry against the explicit `routing_key` column is now case-insensitive, preventing duplicate wire entries.

### Removed
- Internal `StoredOffsetLookup` helper and its temporary tracking-consumer offset-recovery path.

## [0.1.0] - 2026-04-02

### Added
- Spark DataSource V2 connector for RabbitMQ Streams (`rabbitmq_streams` provider name).
- Batch read and write support for streams and superstreams.
- Structured Streaming micro-batch source and sink.
- Spark 3.5 (Scala 2.12 and 2.13), 4.0, and 4.1 support.
- Java 17 and 21 compatibility.
- Source schema with fixed columns (`value`, `stream`, `offset`, `chunk_timestamp`) and optional metadata columns (`properties`, `application_properties`, `message_annotations`, `creation_time`, `routing_key`).
- Sink schema with `value`, optional routing key, AMQP properties, and application properties.
- Offset handling: `earliest`, `latest`, `offset`, and `timestamp` starting modes.
- Spark checkpoint integration as source of truth for offsets.
- Optional server-side offset tracking via RabbitMQ `storeOffset()` and broker-offset recovery on startup.
- Admission control via `maxRecordsPerTrigger` and `maxBytesPerTrigger`.
- `Trigger.AvailableNow` support with tail offset snapshot.
- `minPartitions` for splitting streams into multiple Spark partitions by offset ranges.
- Split-offset merge algorithm for contiguous checkpoint advancement.
- Superstream support: topology discovery, partition-per-stream reads, routing strategies (`hash`, `key`, `custom`).
- Publisher confirms with optional deduplication via `producerName`.
- Speculation-safe producer naming with `taskId` suffix.
- Stream filtering (RabbitMQ 3.13+): `filterValues`, `filterMatchUnfiltered`, client-side post-filter.
- Producer-side filter values via `filterValuePath` or `filterValueExtractorClass`.
- Credit-based backpressure and configurable queue capacity.
- `failOnDataLoss` handling for retention truncation and missing partitions.
- TLS support with JKS keystore/truststore.
- Executor-side `Environment` pool with idle eviction.
- Source metrics via `ReportsSourceMetrics` and task-level `CustomMetric`s.
- Shaded dependencies (RabbitMQ client, Netty, QPID, compression codecs) to avoid Spark classpath conflicts.
- Extension interfaces: `ConnectorAddressResolver`, `ConnectorPostFilter`, `ConnectorRoutingStrategy`, `ConnectorObservationCollectorFactory`, `ConnectorCompressionCodecFactory`.
- Sink schema validation with `ignoreUnknownColumns` option.
- Single active consumer support.
- Compression support (`gzip`, `snappy`, `lz4`, `zstd`) with sub-entry batching.
- Producer option support for `dynamicBatch`, `retryOnRecovery`, and `compressionCodecFactoryClass`.
- Superstream hash routing option support for `hashFunctionClass`.
- Custom superstream routing strategy now receives full message view and metadata route lookup.
- Environment option support for `observationCollectorClass`, `requestedHeartbeatSeconds`,
  `locatorConnectionCount`, `forceReplicaForConsumers`, `forceLeaderForProducers`,
  and connection backoff policy options.
- Micrometer observation registry integration via `observationRegistryProviderClass`.
- Task metric semantics update: `payloadBytesRead`, `estimatedWireBytesRead`, `pollWaitMs`,
  `payloadBytesWritten`, and `estimatedWireBytesWritten`.
- Source planning now uses deterministic even-per-stream allocation for `minPartitions`,
  `maxRecordsPerTrigger`, and `maxBytesPerTrigger` budgets (offset spans act as caps, not weighting).
- Environment tuning option support for `lazyInitialization`, `scheduledExecutorService`,
  `netty.eventLoopGroup`, `netty.byteBufAllocator`, `netty.channelCustomizer`,
  and `netty.bootstrapCustomizer`.
- Sink schema support for optional `publishing_id` per-row dedup publishing ID override.
