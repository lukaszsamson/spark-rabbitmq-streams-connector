# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [Unreleased]
### Changed
- **Breaking:** Broker-stored offsets are no longer used for query recovery. On a fresh start (no Spark checkpoint), the connector resolves initial offsets from configured `startingOffsets` / `startingOffsetsByTimestamp` only, matching Kafka source semantics. Users that previously relied on broker-stored offsets to resume across query restarts must rely on Spark checkpoints (or set `startingOffsets` explicitly).
- **Breaking (option rename):** `serverSideOffsetTracking` is renamed to `storeBrokerOffsets` and its semantics are clarified as best-effort write-only telemetry. The old name remains accepted as a deprecated alias for one release and emits a warning at parse time.
- Bumped RabbitMQ Stream Java client to `1.6.0`. Picks up resilient handling of `Throwable` from compression codecs (native LZ4/Zstd `Error`s no longer kill the consumer pipeline; producer messages are nacked locally), tolerant broker-version parsing for Tanzu/custom builds, a fix for the `resolve_offset_spec` availability check, and dependency bumps (Netty 4.2.12, lz4-java 1.11, amqp-client 5.29).

### Removed
- Internal `StoredOffsetLookup` helper and its temporary tracking-consumer offset-recovery path.

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
- Optional broker offset telemetry via `storeBrokerOffsets` (write-only).
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
- RabbitMQ Stream Java client baseline upgraded to `1.5.x`.
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

### Changed
- Timestamp probe semantics on planning now distinguish broker-confirmed no-match from
  probe-budget exhaustion. A timed-out probe fails planning with
  `TimestampResolutionTimeoutException` for both `startingOffsets=timestamp` and
  `endingOffsets=timestamp`, regardless of `startingOffsetsByTimestampStrategy`.
  **Breaking**: previously, a probe timeout was treated as confirmed no-match —
  with `startingOffsetsByTimestampStrategy=latest` the start would silently jump
  to tail, and `endingOffsets=timestamp` would silently fall back to the stream
  tail. Both behaviors could skip or over-include records under operational
  delay. Increase `pollTimeoutMs` to extend the probe budget when a timeout is
  observed.
- Reject `minOffsetsPerTrigger > maxRecordsPerTrigger` at source validation
  (Kafka-parity message). Previously the conflict was silently accepted.
- `RabbitMQStreamOffset.fromJson` now rejects duplicate stream keys instead of
  silently keeping only the last value.
- Sink: matching of an `application_properties` `routing_key` map entry against
  the explicit `routing_key` column is now case-insensitive, so mixed-case
  variants no longer produce a duplicate wire entry.

### Fixed
- Broker offset persistence now invalidates the per-stream stats cache after a
  successful `storeOffset`, so subsequent planning observes the new committed
  offset instead of a stale cached snapshot.
