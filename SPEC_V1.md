# RabbitMQ Streams and Superstreams Connector for Apache Spark (SPEC_V1)

## Purpose
Provide a Spark DataSource V2 connector that reads from and writes to RabbitMQ Streams and Superstreams using the native RabbitMQ Streams transport. The connector must support batch and Structured Streaming (micro-batch), use RabbitMQ stream offsets as the position model, and persist broker offsets as best-effort metadata aligned with Spark commits.

Target runtime: JVM 17-21 (Spark 4.0/4.1 compatibility range).

## Goals
- Source and sink support for RabbitMQ Streams and Superstreams.
- Structured Streaming micro-batch support with Spark checkpoint integration.
- Native RabbitMQ Streams protocol via the official Java client library.
- Stream-offset-based consumption (no AMQP acknowledgements or message IDs).
- Server-side offset tracking via RabbitMQ Streams as best-effort metadata aligned with Spark commits.
- Operable in batch mode with bounded reads and bounded writes.

## Non-goals
- Support for AMQP 0-9-1.
- Spark DStream (legacy streaming) support.
- Exactly-once sink guarantees without broker-side deduplication constraints.
- Automatic creation and management of RabbitMQ nodes and clusters.

## Terminology
- Stream: Append-only log in RabbitMQ Streams.
- Superstream: Logical stream composed of multiple partitioned streams.
- Partition stream: A concrete stream underlying a superstream.
- Stream offset: Monotonic offset assigned by RabbitMQ Streams per stream.
- Offset tracking: RabbitMQ Streams feature to store consumer offsets in the broker.

## Implementation choices
- Language: Java (primary) only. Scala is not used for V1.
- Build: Maven.
- Packaging: shaded RabbitMQ Streams client and transitive Netty/codec dependencies to avoid Spark classpath conflicts.

## Spark DataSource V2 interfaces
- Provider: public `TableProvider` + `DataSourceRegister` for short name `rabbitmq_streams`.
- Table: `Table` with capabilities `{BATCH_READ, BATCH_WRITE, MICRO_BATCH_READ, STREAMING_WRITE, ACCEPT_ANY_SCHEMA}`.
- `ACCEPT_ANY_SCHEMA` applies to writes; the connector validates sink schemas explicitly and can fail fast unless `ignoreUnknownColumns=true`.
- Read path:
  - `ScanBuilder` -> `Scan` -> `Batch` (batch reads)
  - `Scan` -> `MicroBatchStream` (streaming)
  - `SupportsAdmissionControl` for `ReadLimit` handling and `reportLatestOffset()`
  - `SupportsTriggerAvailableNow`
  - `ReportsSourceMetrics`
  - `MicroBatchStream` lifecycle: `initialOffset()`, `latestOffset()`, `deserializeOffset()`, `commit()`, `stop()`

### Spark lifecycle callbacks
- `MicroBatchStream.initialOffset()`: resolve and persist the starting offsets for a new query.
- `MicroBatchStream.latestOffset(start, limit)`: compute the end offsets for the current trigger; return `start` when no new data is available.
- `MicroBatchStream.deserializeOffset(json)`: parse offset JSON from checkpoint.
- `MicroBatchStream.commit(end)`: commit offsets (Spark checkpoint + optional broker storage).
- `MicroBatchStream.stop()`: close readers and return pooled resources.
- `SupportsAdmissionControl.reportLatestOffset()`: return cached tail offsets for progress/metrics without re-querying the broker.
- `Batch.planInputPartitions()`: plan partitions once with fixed offset ranges.
- `Batch.createReaderFactory()`: create a serializable reader factory used for batch and micro-batch.
- Write path:
  - `WriteBuilder` -> `Write` -> `BatchWrite`
  - `Write` -> `StreamingWrite`

## Connector identity
- Provider name: `rabbitmq_streams`.
- Table name: `stream` or `superstream` supplied via options.
- Registration: `META-INF/services/org.apache.spark.sql.sources.DataSourceRegister`.

## Data model
### Source schema (default)
- `value` (binary, required)
- `stream` (string, required)
- `offset` (long, required)
- `chunk_timestamp` (timestamp, required; from `MessageHandler.Context.timestamp()`)
- `properties` (struct, optional; mirrors AMQP 1.0 properties)
- `application_properties` (map<string,string>, optional; values coerced to string)
- `message_annotations` (map<string,string>, optional; values coerced to string)
- `creation_time` (timestamp, optional; convenience alias for `properties.creation_time`)
- `routing_key` (string, optional; only if explicitly provided by producer in application properties)

### AMQP properties struct
Struct fields (all nullable) and Spark SQL types:
- `message_id` (string; coerced from string|ulong|binary|uuid)
- `user_id` (binary)
- `to` (string)
- `subject` (string)
- `reply_to` (string)
- `correlation_id` (string; coerced from string|ulong|binary|uuid)
- `content_type` (string)
- `content_encoding` (string)
- `absolute_expiry_time` (timestamp)
- `creation_time` (timestamp)
- `group_id` (string)
- `group_sequence` (long)
- `reply_to_group_id` (string)

### Sink input schema
- `value` (binary, required)
- `stream` (string, optional; only in stream mode)
- `routing_key` (string, optional; required for superstream if routing strategy requires it)
- `properties` (struct, optional; AMQP properties)
- `application_properties` (map<string,string>, optional)
- `message_annotations` (map<string,string>, optional)
- `creation_time` (timestamp, optional)

Type coercion notes:
- `application_properties` and `message_annotations` values are coerced to string (lossy) in V1.
- If `routing_key` is needed on read, it must be explicitly stored by producers (e.g., in `application_properties`).
- All timestamp fields (`chunk_timestamp`, `properties.creation_time`, `properties.absolute_expiry_time`) use epoch milliseconds; convert millis to micros for Spark `TimestampType`.
- Top-level `creation_time` is always equal to `properties.creation_time`.

## Schema projection
- Fixed source columns always present: `value`, `stream`, `offset`, `chunk_timestamp`.
- Optional metadata columns controlled by `metadataFields`: `properties`, `application_properties`, `message_annotations`, `creation_time`, `routing_key`.
- `metadataFields` never removes fixed columns; it only toggles optional metadata columns.

## Sink schema handling
- By default, fail-fast on unrecognized columns to avoid silent misconfiguration.
- Optional `ignoreUnknownColumns` (bool; default false) can allow extra columns to be ignored.

## Source design
### Read modes
- Batch read: bounded by `startingOffsets` and `endingOffsets`.
- Structured Streaming: micro-batch only; `startingOffsets` can be `earliest`, `latest`, `offset`, or `timestamp`.

### Batch read
- `Scan.toBatch()` resolves starting and ending offsets (endingOffsets=latest resolved once).
- `Batch.planInputPartitions()` creates one partition per stream (or per split when `minPartitions` is set).
- `Batch.createReaderFactory()` returns the same reader factory used by micro-batch.
- No commit or broker offset tracking occurs.
- If the stream is empty (`StreamStats` throws `NoOffsetException`), the batch produces zero rows and `planInputPartitions()` returns an empty array.

### Offset specification mapping
- `earliest` -> `OffsetSpecification.first()`
- `latest` -> `OffsetSpecification.next()`
- `offset` -> `OffsetSpecification.offset(n)` (inclusive)
- `timestamp` -> `OffsetSpecification.timestamp(epochMillis)` (may return messages before the timestamp)
- For empty streams and `startingOffsets=latest`, the initial offset is the logical 0 (the next offset once the first message appears).

### Partitioning and parallelism
- Streams: one Spark input partition per stream by default.
- Superstreams: one Spark input partition per partition stream.
- Optional `minPartitions` can split a single stream into multiple Spark partitions by offset ranges:
  - Each split is a distinct consumer with a unique consumer name.
  - The connector enforces `[startOffset, endOffset)` client-side and discards excess messages from the final chunk.
  - This increases connections and can discard some messages at chunk boundaries.
- For superstreams with `minPartitions > partitionStreamCount`, split individual partition streams by offset ranges (proportional to pending data). If `minPartitions <= partitionStreamCount`, no additional splitting occurs.

### Split offset merge
- Checkpoint offsets remain keyed by partition stream name.
- For split streams, advance the stream checkpoint to the largest contiguous committed end starting from the current checkpoint.
- Algorithm: given `base` (current next offset) and split results `(start_i, end_i, committed_i)`, first dedupe by `(start_i,end_i)` keeping the max `committed_i` per range, then sort by `start_i`, and advance while `start_i == base` and `committed_i == end_i`, setting `base = end_i` for each contiguous split.
- If any split is missing or incomplete, the merged checkpoint does not advance beyond the last contiguous end.
- Retries reprocess only the missing range; duplicates are possible but no gaps are allowed.
- Example: base=0, splits [0,100] commit, [100,200] fail, [200,300] commit => merged next offset remains 100.

### Push-to-pull bridging
- RabbitMQ Streams client is push-based; Spark readers are pull-based.
- Each `PartitionReader` wraps a consumer that pushes into a bounded queue.
- `next()` pulls from the queue with a timeout and stops when `endOffset` is reached.
- Poll timeout and max wait are configurable; if max wait is exceeded before `endOffset`, the reader fails the task (no dynamic end-offset lowering) to avoid committing unread offsets.
- Queue capacity bounds memory use; consumer credit is adjusted to the queue size.
- Queue capacity is configurable (default 10000 messages) and should accommodate a typical chunk; the callback may block if the queue is full.
- Use reader-side offset de-dup (`offset <= lastEmittedOffset`) on reconnection; only override subscription offsets if de-dup is disabled.
- Register a `Resource.StateListener` on consumers to log RECOVERING/CLOSED transitions.
- Queue backpressure: pause credit grants when the queue is near capacity to avoid blocking Netty threads.

### Offset handling
- Each partition tracks a `stream offset` range: `[startOffset, endOffset)`.
- Stored offsets in RabbitMQ are last-processed offsets; resume uses `stored + 1`.
- Spark checkpoints are the source of truth.
- On startup:
  - If a Spark checkpoint exists, use it.
  - Else, if RabbitMQ offset tracking has an entry for the configured consumer name, use it (handle `NoOffsetException`).
    - For superstreams, create a temporary named consumer per partition stream with `noTrackingStrategy()`, call `storedOffset()`, then close it.
    - For single streams, create a temporary named consumer with `noTrackingStrategy()`, call `storedOffset()`, then close it.
    - If stored offsets cannot be queried:
      - For auth/config/connection errors, fail fast regardless of `consumerName`.
      - For tracking-consumer limits or other non-fatal lookup constraints, fail fast when `consumerName` is explicitly set; otherwise fall back to `startingOffsets` with a warning.
  - Else, use `startingOffsets`.
- During consumption:
  - Use `noTrackingStrategy()` to avoid storing offsets ahead of Spark commits.
- On commit (`MicroBatchStream.commit`):
  - Persist offsets in Spark checkpoint (required by Spark).
  - If `serverSideOffsetTracking` is true, store last-processed offsets in RabbitMQ via `Environment.storeOffset(reference, stream, offset)` as best-effort metadata (streams and superstreams).
  - If `serverSideOffsetTracking` is false, do not write broker offsets.
  - Store offsets concurrently across partition streams to avoid commit latency amplification.
  - Consider asynchronous best-effort storage to avoid blocking commit; Spark checkpoint remains the source of truth.

### Offset commit latency
- `Environment.storeOffset()` is a blocking call that may wait up to ~10s for verification; sequential calls can amplify commit latency.
- Recommended model: dispatch `storeOffset` calls asynchronously (bounded executor) and wait with a per-batch timeout; broker tracking is best-effort and does not gate Spark commits.

### Micro-batch planning
- Use `SupportsAdmissionControl` to apply `ReadLimit`:
  - `maxRecordsPerTrigger` maps to `ReadLimit.maxRows(n)`; distribute proportionally to pending data per partition stream (Spark input partitions), at least 1 record per non-empty partition.
  - `maxBytesPerTrigger` maps to `ReadLimit.maxBytes(n)`; distribute proportionally using estimated bytes per partition (best-effort).
  - Byte estimation: maintain a running average message size from previous batches; the first batch uses `estimatedMessageSizeBytes` (default 1024).
  - Rounding strategy: allocate floor shares, then distribute remainder by largest fractional share.
  - `getDefaultReadLimit()` returns `maxRows`, `maxBytes`, composite, or `allAvailable` depending on configured limits.
  - `latestOffset(start, ReadLimit)` dispatch:
    - `ReadAllAvailable`: return full tail offsets.
    - `ReadMaxRows`: apply proportional record budget.
    - `ReadMaxBytes`: apply proportional byte budget using estimated bytes/message.
    - `CompositeReadLimit`: apply the most restrictive of its components.
    - Unknown types: treat as `ReadAllAvailable`.
- `latestOffset()` uses broker stats:
  - Prefer `StreamStats.committedOffset()` when available (RabbitMQ 4.3+).
  - Fallback to `StreamStats.committedChunkId()` (approximate) for RabbitMQ 4.0/4.1.
  - If `StreamStats` throws `NoOffsetException` (empty stream), return `startOffset` (no new data).
  - If no partition has new data beyond `startOffset`, return `startOffset` to skip the trigger.
- For `endingOffsets=latest`, use the chosen tail approximation; note potential staleness.
- For batch reads, `endingOffsets=latest` is resolved once during `Scan.toBatch()` and remains fixed for the batch execution.
### Trigger.AvailableNow
- `prepareForTriggerAvailableNow()` snapshots tail offsets for each partition stream using `StreamStats` and fixes them as the query target.
- Subsequent `latestOffset()` calls must not exceed the snapshot even if new data arrives.
- The query processes all data up to the snapshot and then terminates.
- Handle empty streams (`NoOffsetException`) by recording no data for that partition.

### Backpressure and flow control
- Consumer credit is chunk-based; one credit yields a chunk.
- The reader grants low initial credits (1-2), then adds credits as the queue drains.
- When close to `endOffset`, stop granting credits and discard any messages beyond the range.
- Override client defaults to set initial credits to 1 (configurable) and use `creditOnChunkArrival`.

### Fault tolerance
- Source is at-least-once relative to Spark.
- If a task fails before commit, Spark will re-read the same offset range.
- `failOnDataLoss` behavior:
  - If requested start offset is below the stream retention floor, fail when true; otherwise log and advance to first available offset.
  - If a partition stream is missing (superstream topology change or deletion), fail when true; otherwise skip that partition and continue.
  - In batch mode, if any partition is missing or truncated and `failOnDataLoss=true`, fail the batch; otherwise return partial results.

### Startup validation
- Validate stream/superstream existence at planning time using `Environment.streamExists()` or by catching `StreamDoesNotExistException` from `queryStreamStats()`.
- Fail fast with a descriptive error when the stream is missing.
- Validate broker version when version-gated options are set:
  - `filterValues` and producer-side filter extraction options require RabbitMQ 3.13+.
  - `committedOffset()` requires RabbitMQ 4.3+; on older versions, fall back to `committedChunkId()` with a warning.
  - Unsupported combinations fail with a descriptive error at planning time.

## Resource management
- `Environment` instances are cached per executor JVM, keyed by connection configuration.
- Producers/consumers are acquired on writer/reader creation and released on close.
- The pool evicts idle environments after a configurable timeout.
- `InputPartition` and `PartitionReaderFactory` are serialized to executors; they carry only configuration (stream name, offsets, connection params). RabbitMQ client instances are acquired lazily from the executor pool.
- Tracking consumers are named consumers used for offset storage/lookup; auto-tracking is never enabled.

## Sink design
### Write modes
- Batch write: standard DataSource V2 batch write.
- Structured Streaming: micro-batch only with epoch-based writers.

### Producer behavior
- Uses RabbitMQ Streams `Producer` with publisher confirms enabled.
- Optional deduplication is enabled via `producerName` and monotonically increasing `publishingId` per writer task.
- Derived producer name for streaming writes: `producerName` + `-p` + `partitionId` + `-t` + `taskId` (avoids speculative conflicts; cross-retry dedup not guaranteed).
- Deduplication constraints:
  - Only one live producer per `producerName`.
  - Publishing must be single-threaded per producer name.
  - Deduplication is not guaranteed when sub-entry batching/compression is enabled.
  - Speculative execution: include `taskId` in derived producer names or disable deduplication when speculative tasks are enabled.
- `retryOnRecovery` remains enabled for at-least-once semantics.
- Register a `Resource.StateListener` on producers to log RECOVERING/CLOSED transitions.

### Producer send model
- `DataWriter.write()` enqueues sends and tracks outstanding confirms; if a prior send failed, throw on subsequent writes.
- `DataWriter.commit()` waits for all outstanding confirms (with timeout) and fails the commit if any confirmation failed.
- `DataWriter.abort()` closes or returns the producer to the pool; unconfirmed messages may be lost.
- `enqueueTimeoutMs` controls how long `send()` will block when `maxInFlight` is reached before failing the write.

### StreamingWrite commit/abort
- `StreamingWrite.commit()` is idempotent; repeated commits for the same epoch are safe.
- `StreamingWrite.abort()` is a no-op; per-writer cleanup happens in `DataWriter.abort()`.

### BatchWrite commit/abort
- `BatchWrite.commit()` is a no-op; persistence is handled by individual `DataWriter.commit()`.
- `BatchWrite.abort()` is a no-op; per-writer cleanup happens in `DataWriter.abort()`.

### Batch write failure semantics
- Batch writes are at-least-once and may be partially visible if some tasks commit and others fail.
- Retries may re-send data for failed partitions; duplicates are possible.

### Ordering
- Preserve order within each Spark partition writer.
- No global ordering across partitions.

### Fault tolerance
- At-least-once delivery in Structured Streaming.
- With deduplication enabled and constraints satisfied, duplicates are suppressed per producer name.

## Speculative execution
- Default Spark commit coordinator is enabled.
- Deduplication must use producer names that avoid collisions across speculative tasks (include `taskId`).

## Commit coordinator
- `StreamingWrite.useCommitCoordinator() = true`.
- `BatchWrite.useCommitCoordinator() = true`.

## Speculative execution with deduplication
- If `producerName` is set and Spark speculation is enabled, derive producer names with `taskId` to avoid concurrent name collisions.
- When speculation is enabled, deduplication is guaranteed only within a single task attempt; cross-retry dedup is not guaranteed.
- Alternative (optional): detect speculation and force `producerName` to be unset (disable dedup) unless the user explicitly opts in to taskId-based naming.
- This tradeoff means duplicate messages may still occur across task retries even with dedup enabled.

## Superstreams support
### Topology discovery
- The stream Java client queries superstream topology (partitions and routing rules) from broker metadata.
- Partitions are derived from the superstream exchange bindings; routing rules are the bindings themselves.
- The connector must obtain the partition list on the driver to plan `InputPartition`s:
  - Instantiate a lightweight superstream producer to access routing metadata and `partitions()`, then close it.
  - Cache the topology for the query lifetime and refresh on restart.
- Document that topology discovery depends on broker metadata and requires correct `advertised_host`/`advertised_port` when applicable.

### Source
- One Spark input partition per partition stream.
- Use the partition stream name in offsets and metrics.

### Sink
- Routing is handled by the RabbitMQ superstream producer.
- Routing strategies:
  - `hash`: default; hashes routing key (MurmurHash3 32-bit) modulo partition count.
  - `key`: resolves routes using superstream bindings; can route to multiple partition streams.
  - `custom`: user-provided `RoutingStrategy`.
- Default behavior when routing key is absent and routing strategy requires it: fail fast with a clear error. Users must provide a routing key or a custom routing strategy. This matches the client requirement that applications provide a routing key extractor for superstream producers.
- `stream` column is ignored in superstream mode; routing determines the partition stream.
- Reconfiguration: on query restart, refresh superstream topology from the broker.
- Deduplication constraints apply per `(producerName, stream)`; multiple concurrent tasks must not share the same `producerName` for the same partition stream.
- In stream mode, all rows must target the configured `stream`; if a `stream` column is present, it is validated to match and mismatches cause an error.

## Configuration
### Common options
- `endpoints` (required unless `uris` is provided): comma-separated host:port for RabbitMQ Streams. Connector constructs URIs from `username`, `password`, `vhost`, and `tls`.
- `uris` (optional): explicit list of `rabbitmq-stream://` URIs; overrides `endpoints`.
- `username`, `password`, `vhost` (optional)
- `tls` (bool)
- `tls.truststore`, `tls.truststorePassword`, `tls.keystore`, `tls.keystorePassword` (JKS; converted to Netty `SslContext`)
- `tls.trustAll` (bool; dev only)
- `stream` (string) or `superstream` (string, mutually exclusive)
- `consumerName` (string, optional; stable across restarts)
- Default consumerName:
  - Single stream: `queryId`.
  - Superstream: `queryId` (partition stream name already disambiguates offsets).
  - `minPartitions` splits: `queryId` + split id.
- For batch reads, default consumerName includes a random suffix and server-side offset tracking is disabled by default.
- `metadataFields` (string list; default `properties,application_properties,message_annotations,creation_time,routing_key`)
- `failOnDataLoss` (bool; default true)
- `addressResolverClass` (string, optional; connector-specific interface to avoid shaded RabbitMQ types)

### Options validation
- Validate options at `TableProvider.getTable()` / `ScanBuilder` construction.
- Exactly one of `stream` or `superstream` must be set.
- At least one of `endpoints` or `uris` must be set.
- Invalid combinations fail with `IllegalArgumentException` and a descriptive message.

### Source options
- `startingOffsets` (earliest|latest|offset|timestamp)
- `startingOffset` (long; used when startingOffsets=offset)
- `startingTimestamp` (epoch millis only; used when startingOffsets=timestamp)
- `endingOffsets` (latest|offset)
- `endingOffset` (long; used when endingOffsets=offset)
- `maxRecordsPerTrigger` (long)
- `maxBytesPerTrigger` (long, best-effort; uses estimated message size)
- `minPartitions` (int)
- `serverSideOffsetTracking` (bool; default true for streaming, false for batch; commit-time storage only, not auto-tracking)
- `filterValues` (comma-separated; requires RabbitMQ 3.13+ and broker-side filtering enabled)
- `filterMatchUnfiltered` (bool; default false)
- `filterPostFilterClass` (string, optional; connector post-filter interface with full message view)
- `filterWarningOnMismatch` (bool; default true; log when post-filter drops messages due to Bloom false positives)
- `pollTimeoutMs` (long; default 30000)
- `maxWaitMs` (long; default 300000)
- `initialCredits` (int; default 1)
- `queueCapacity` (int; default 10000 messages; should accommodate typical chunk sizes; credit-based backpressure prevents overflow for larger chunks)
- `estimatedMessageSizeBytes` (int; default 1024; used for initial max-bytes estimation)

### Sink options
- `producerName` (string; enables deduplication if set)
- `publisherConfirmTimeoutMs` (long)
- `maxInFlight` (int; maps to `maxUnconfirmedMessages`)
- `enqueueTimeoutMs` (long; default 10000; controls how long `send()` blocks when `maxInFlight` is reached before failing)
- `batchSize` (int)
- `batchPublishingDelayMs` (long; may be overridden by dynamic batching)
- `compression` (none|gzip|snappy|lz4|zstd; requires sub-entry batching)
- `subEntrySize` (int; >1 enables batching/compression; disables dedup guarantees)
- `routingStrategy` (hash|key|custom)
- `partitionerClass` (string; for custom routing)
- `filterValuePath` (string; producer-side filter path: `application_properties.*`, `message_annotations.*`, or `properties.*`)
- `filterValueExtractorClass` (string; custom producer-side filter extractor class)
- `filterValueColumn` is removed (hard cutoff); connector fails fast if provided.
- `filterPostFilterClassV2` is removed (hard cutoff); connector fails fast if provided.
- `ignoreUnknownColumns` (bool; default false; when true, extra input columns are ignored)

### Auth extensions (future)
- OAuth 2 client credentials and custom `CredentialsProvider` are planned but not required for V1.

## Security
- TLS support via RabbitMQ Streams client settings, with JKS to Netty `SslContext` bridging.
- Username/password and vhost support.
- No credentials stored in checkpoints; use Spark config redaction patterns.

## Metrics and observability
- Source metrics via `ReportsSourceMetrics` (string key-values in query progress):
  - `minOffsetsBehindLatest`, `maxOffsetsBehindLatest`, `avgOffsetsBehindLatest` computed as `max(0, tail - consumed)` where tail is from `reportLatestOffset()` and consumed is `latestConsumedOffset`.
- Task-level metrics via `CustomMetric`:
  - `recordsRead`, `bytesRead`, `readLatencyMs`, `recordsWritten`, `bytesWritten`, `writeLatencyMs`, `publishConfirms`, `publishErrors`.
- Log structured errors with stream and offset context.

## Offset serialization
- Offset JSON maps partition stream names to next offsets:
  - Example: `{ "stream-a": 12345, "stream-b": 67890 }`.
- Example for a batch processing `[100, 200)` in stream `orders`:
  - Spark checkpoint JSON: `{ "orders": 200 }` (next offset)
  - RabbitMQ storeOffset: `storeOffset("consumer", "orders", 199)` (last processed)

## Startup offset discovery
- If no Spark checkpoint exists, attempt to read a stored broker offset for the consumer name.
- If no broker offset exists (`NoOffsetException`), fall back to `startingOffsets`.
- `Environment` does not expose a direct `queryOffset()` API; stored offset lookup uses temporary named consumers per stream/partition (startup cost: one consumer create/close per partition stream).
- Internal client APIs are not used for offset queries (unstable).
- Stored offset lookup failure classes:
  - Fatal: auth/config/connection errors (fail fast).
  - Non-fatal: tracking-consumer limits or bounded-concurrency limits (eligible for fallback when `consumerName` is not explicit).
  - Timeouts/consumer creation failures are treated as fatal unless explicitly configured as non-fatal.
- For superstreams with many partitions, stored-offset lookup must be bounded to avoid hitting tracking-consumer limits (50 per connection): perform lookup with a bounded concurrency pool and reuse connections across partition-offset lookup operations; if limits are exceeded, fail fast when `consumerName` is explicit, otherwise fall back to `startingOffsets` with a warning.

## Compatibility and packaging
- Spark 4.0 and 4.1 DataSource V2 APIs.
- Build for Java 17 bytecode; runtime compatible with Java 17-21.
- Shade RabbitMQ Streams client and transitive Netty/codec dependencies to avoid Spark classpath conflicts; relocate packages and document that only NIO transport is supported when shaded.

## Connector extension interfaces
- `addressResolverClass` must implement a connector-defined interface (not RabbitMQ client types), have a public no-arg constructor, and run on the driver.
- `filterPostFilterClass` must implement a connector-defined filter interface (not RabbitMQ client types), have a public no-arg constructor, and run on executors.
- `partitionerClass` must implement a connector-defined routing interface (not RabbitMQ client types) and run on executors; routing must be deterministic for a given input.

## Requirements
- Minimum RabbitMQ version: 3.11 (superstreams, single active consumer).
- Recommended RabbitMQ version: 4.3+ (precise tail offset via `committedOffset()`).
- Optional features by version:
  - 3.13+: stream filtering (filter options)
  - 4.3+: precise end offsets; 4.0/4.1 use `committedChunkId()` approximation.

## Optional compatibility with Spark 3.4/3.5 (outline)
- Separate artifact or build profile targeting Spark 3.4/3.5 with Scala 2.12 dependencies.
- Replace or conditionally compile Spark 4.x-only interfaces:
  - `SupportsTriggerAvailableNow` (present in 3.3+, but verify signatures)
  - `SupportsAdmissionControl` exists, but `ReadLimit` behavior differs; align with Kafka 3.5 connector.
  - Exclude `SupportsRealTimeMode` (Spark 4.x only).
- Adjust module dependencies and shading to match Spark 3.4/3.5 Netty/Guava versions.
- The connector is pure Java, but the Spark API artifacts are Scala-versioned (`spark-sql_2.12` for 3.4/3.5; `spark-sql_2.13` for 4.x).
- Limit features to those available in Spark 3.4/3.5 (no Spark 4.x-only APIs).

## SupportsRealTimeMode (Spark 4.x) design
- Plan for a V1.1 implementation.
- Mode: unbounded `planInputPartitions(start)` with end offsets resolved by stream tail.
- `mergeOffsets` aggregates latest offsets from partitions into a single Offset JSON map.
- Uses the same push-to-pull adapter and credit flow control.
- Offsets are still committed via `MicroBatchStream.commit` semantics; real-time mode uses driver-coordinated commits.

## Testing strategy
- Unit tests for option parsing, schema mapping, and offset conversions.
- Integration tests using RabbitMQ in Docker via Testcontainers (stream plugin enabled).
- Structured Streaming tests using Spark 4.0/4.1 local mode.
- Push-to-pull adapter tests (queue bounds, slow/fast producers, empty stream).
- Offset JSON serialization round-trip tests.
- Superstream topology change tests.
- Split-offset merge tests with partial completion, retries, and out-of-order split results.
- TLS JKS-to-SslContext conversion tests.
- Trigger.AvailableNow tests.

## Implementation plan
### Milestone 0: Project Bootstrap
Deliverables:
- Maven multi-module skeleton (`core`, `spark40`, `spark41`, `it-tests`)
- Shading/relocation setup for RabbitMQ Streams + Netty
- CI pipeline (build, unit, integration matrix)

Acceptance criteria:
- `mvn -q -DskipTests package` succeeds on Java 17 and 21
- Shaded jar has relocated RabbitMQ/Netty packages
- Spark 4.0 and 4.1 profiles build cleanly

### Milestone 1: Options, Validation, and Table Provider
Deliverables:
- `TableProvider` + `DataSourceRegister` (`rabbitmq_streams`)
- Option parser + validation layer
- Source/sink mode resolution (`stream` vs `superstream`)
- Extension class loading contracts (`addressResolverClass`, etc.)

Acceptance criteria:
- Invalid combinations fail fast with descriptive errors
- Source and sink option defaults match spec
- Unit tests cover all option parsing and validation branches

### Milestone 2: Schema and Row Mapping
Deliverables:
- Source schema builder with `metadataFields` projection
- Sink schema validator with `ignoreUnknownColumns`
- AMQP properties mapping + timestamp conversion (millis -> micros)

Acceptance criteria:
- Fixed source fields always present
- Optional metadata columns toggle correctly
- Sink rejects unknown fields unless `ignoreUnknownColumns=true`
- Unit tests for all field mappings/coercions

### Milestone 3: Batch Read Path
Deliverables:
- `ScanBuilder -> Scan -> Batch -> InputPartition -> PartitionReader`
- Offset spec resolution (`earliest/latest/offset/timestamp`)
- Bounded range reads with `[startOffset, endOffset)`
- `minPartitions` range splitting + split-offset merge algorithm

Acceptance criteria:
- Batch reads produce exact bounded ranges
- Empty stream returns zero partitions/rows correctly
- Split merge behavior passes partial/retry/out-of-order tests
- Integration tests with Testcontainers pass

### Milestone 4: Micro-Batch Source (Core Streaming)
Deliverables:
- `MicroBatchStream` lifecycle implementation
- Offset JSON serialization/deserialization
- Startup offset discovery (checkpoint first, broker fallback policy)
- Commit path with optional best-effort broker `storeOffset`

Acceptance criteria:
- `latestOffset(start)` returns `start` when no data
- Commit updates Spark checkpoint reliably
- Broker offset tracking behavior matches `serverSideOffsetTracking`
- Restart scenarios preserve at-least-once semantics in tests

### Milestone 5: Admission Control, Trigger.AvailableNow, and Metrics
Deliverables:
- `SupportsAdmissionControl` (`maxRecordsPerTrigger`, `maxBytesPerTrigger`)
- `SupportsTriggerAvailableNow` snapshot behavior
- `ReportsSourceMetrics` + task `CustomMetric`s
- Lag metric clamping (`max(0, tail - consumed)`)

Acceptance criteria:
- Read limits constrain planned offsets as specified
- AvailableNow drains to snapshot then terminates
- Metrics emitted in query progress and executor task outputs
- Streaming integration tests validate limits and trigger semantics

### Milestone 6: Sink (Batch + Streaming Writes)
Deliverables:
- `WriteBuilder -> Write -> BatchWrite/StreamingWrite`
- `DataWriter` confirm tracking, retry/error handling
- Dedup producer naming strategy with speculation-safe suffixes
- Superstream routing (`hash/key/custom`) + routing-key validation

Acceptance criteria:
- Writes are at-least-once with documented partial visibility for batch
- Confirm failures fail writer commit
- Dedup behavior matches constraints (including sub-entry limitations)
- Superstream routing behavior validated by integration tests

### Milestone 7: Resource Management, Fault Handling, and Hardening
Deliverables:
- Executor-side `Environment` pool and eviction
- Consumer/producer state listeners and structured logs
- Backpressure/credit flow controls
- `failOnDataLoss` handling for retention/topology cases

Acceptance criteria:
- No leaked consumers/producers/environments under stress tests
- Expected behavior on retention truncation and missing partitions
- Startup lookup bounded for high-partition superstreams
- Soak tests complete without deadlock or unbounded memory growth

### Milestone 8: Release Readiness
Deliverables:
- End-to-end docs (usage, options, semantics, limitations)
- Compatibility matrix (Spark 3.5/4.0/4.1, RabbitMQ versions)
- Example jobs (batch source/sink, streaming source/sink, superstream)
- Versioned release artifact + changelog

Acceptance criteria:
- Full test suite green in CI
- Docs cover operational gotchas and failure semantics
- Reproducible demo runs against Testcontainers
- Release candidate signed off

## Suggested execution cadence
1. Foundation first: M0-M2
2. Source path complete: M3-M5
3. Sink path + hardening: M6-M7
4. Release prep: M8

## Filtering defaults
- When both `filterValuePath` and `filterValueExtractorClass` are absent, no producer-side filter value is set. Messages are published without a filter value and will be delivered only to consumers with `filterMatchUnfiltered = true`.

## Appendix: References
- RabbitMQ Stream Java Client super streams (topology, routing strategies, producer requirements): https://rabbitmq.github.io/rabbitmq-stream-java-client/stable/htmlsingle/#super-streams
- RabbitMQ Streams overview and super streams concepts: https://www.rabbitmq.com/docs/streams
- Stream protocol metadata command and advertised host/port guidance: https://www.rabbitmq.com/docs/stream-connections#metadata-command
## Capacity planning
- Each `Environment` manages multiple connections; defaults: 256 producers/connection, 256 consumers/connection, 50 tracking consumers/connection.
- Per executor: consumers/producers are pooled by environment; total connections scale with partitions (ceil(P/256) for consumers, ceil(W/256) for producers) plus locator connections.
- Tracking consumers (named consumers) are limited to 50 per connection; superstreams with >50 partitions will use multiple tracking connections.
