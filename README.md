# Sparkling Rabbit

RabbitMQ Streams and Superstreams connector for Apache Spark.

Provides a Spark DataSource V2 connector that reads from and writes to [RabbitMQ Streams](https://www.rabbitmq.com/docs/streams) and [Superstreams](https://www.rabbitmq.com/docs/streams#super-streams) using the native RabbitMQ Streams protocol. Supports batch and Structured Streaming (micro-batch) modes with Spark checkpoint integration and optional server-side offset tracking.

## Features

- Batch and Structured Streaming (micro-batch) source and sink
- RabbitMQ Streams and Superstreams support
- Native RabbitMQ Streams protocol via the official [Java client library](https://rabbitmq.github.io/rabbitmq-stream-java-client/stable/htmlsingle/)
- Stream-offset-based consumption with Spark checkpoint as source of truth
- Optional server-side offset tracking (best-effort, aligned with Spark commits)
- Backpressure via credit-based flow control
- Admission control (`maxRecordsPerTrigger`, `maxBytesPerTrigger`, `minOffsetsPerTrigger`)
- `Trigger.AvailableNow` support
- Publisher confirms with optional deduplication
- Stream filtering (RabbitMQ 3.13+)
- TLS with JKS keystore/truststore support
- Shaded dependencies to avoid Spark classpath conflicts

## Compatibility matrix

| Component | Supported versions |
|-----------|--------------------|
| Apache Spark | 3.5.x, 4.0.x, 4.1.x |
| Java | 17, 21 |
| RabbitMQ | 3.11+ (minimum), 3.13+ (filtering), 4.0+ (superstreams improvements), 4.3+ (precise tail offsets via `committedOffset()`) |
| rabbitmq-stream-java-client | 1.5.x |
| Scala | 2.12 (Spark 3.5 only), 2.13 |

### Artifacts

| Spark version | Scala | Artifact |
|---------------|-------|----------|
| 3.5.x | 2.13 | `spark-rabbitmq-streams-connector-spark35` |
| 3.5.x | 2.12 | `spark-rabbitmq-streams-connector-spark35_2.12` |
| 4.0.x | 2.13 | `spark-rabbitmq-streams-connector-spark40` |
| 4.1.x | 2.13 | `spark-rabbitmq-streams-connector-spark41` |

## Quick start

### Maven dependency

```xml
<dependency>
    <groupId>io.github.lukaszsamson</groupId>
    <artifactId>spark-rabbitmq-streams-connector-spark41</artifactId>
    <version>0.1.0-SNAPSHOT</version>
</dependency>
```

### Batch read

```java
Dataset<Row> df = spark.read()
    .format("rabbitmq_streams")
    .option("endpoints", "localhost:5552")
    .option("stream", "my-stream")
    .option("startingOffsets", "earliest")
    .load();

df.show();
```

### Batch write

```java
df.write()
    .format("rabbitmq_streams")
    .mode("append")
    .option("endpoints", "localhost:5552")
    .option("stream", "my-stream")
    .save();
```

### Structured Streaming source

```java
Dataset<Row> stream = spark.readStream()
    .format("rabbitmq_streams")
    .option("endpoints", "localhost:5552")
    .option("stream", "my-stream")
    .option("startingOffsets", "earliest")
    .load();

StreamingQuery query = stream.writeStream()
    .format("console")
    .option("checkpointLocation", "/tmp/checkpoint")
    .trigger(Trigger.ProcessingTime("10 seconds"))
    .start();
```

### Structured Streaming sink

```java
stream.writeStream()
    .format("rabbitmq_streams")
    .option("endpoints", "localhost:5552")
    .option("stream", "output-stream")
    .option("checkpointLocation", "/tmp/checkpoint")
    .trigger(Trigger.ProcessingTime("10 seconds"))
    .start();
```

## Data model

### Source schema

The source produces rows with fixed columns and optional metadata columns.

**Fixed columns** (always present):

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `value` | binary | no | Message body |
| `stream` | string | no | Stream name (partition stream name for superstreams) |
| `offset` | long | no | Stream offset |
| `chunk_timestamp` | timestamp | no | Chunk timestamp from `MessageHandler.Context.timestamp()` |

**Optional metadata columns** (controlled by `metadataFields`):

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `properties` | struct | yes | AMQP 1.0 properties (see below) |
| `application_properties` | map&lt;string,string&gt; | yes | Application properties (values coerced to string) |
| `message_annotations` | map&lt;string,string&gt; | yes | Message annotations (values coerced to string) |
| `creation_time` | timestamp | yes | Convenience alias for `properties.creation_time` |
| `routing_key` | string | yes | Routing key (only if stored by producer in application properties) |

**AMQP 1.0 properties struct** (all fields nullable):

| Field | Type |
|-------|------|
| `message_id` | string (coerced from string/ulong/binary/uuid) |
| `user_id` | binary |
| `to` | string |
| `subject` | string |
| `reply_to` | string |
| `correlation_id` | string (coerced from string/ulong/binary/uuid) |
| `content_type` | string |
| `content_encoding` | string |
| `absolute_expiry_time` | timestamp |
| `creation_time` | timestamp |
| `group_id` | string |
| `group_sequence` | long |
| `reply_to_group_id` | string |

### Sink schema

| Column | Type | Required | Description |
|--------|------|----------|-------------|
| `value` | binary | yes | Message body |
| `publishing_id` | long | no | Explicit publishing ID override for deduplication |
| `stream` | string | no | Target stream (stream mode only; validated to match configured stream) |
| `routing_key` | string | no | Routing key (required for superstream if routing strategy needs it) |
| `properties` | struct | no | AMQP 1.0 properties |
| `application_properties` | map&lt;string,string&gt; | no | Application properties |
| `message_annotations` | map&lt;string,string&gt; | no | Message annotations |
| `creation_time` | timestamp | no | Creation time |

By default, unrecognized columns cause an error. Set `ignoreUnknownColumns=true` to silently ignore extra columns.

## Configuration reference

### Connection options

| Option | Type | Required | Default | Description |
|--------|------|----------|---------|-------------|
| `endpoints` | string | yes* | — | Comma-separated `host:port` for RabbitMQ Streams |
| `uris` | string | yes* | — | Explicit `rabbitmq-stream://` URIs; overrides `endpoints` |
| `username` | string | no | — | RabbitMQ username |
| `password` | string | no | — | RabbitMQ password |
| `vhost` | string | no | — | RabbitMQ virtual host |
| `tls` | bool | no | false | Enable TLS |
| `tls.truststore` | string | no | — | JKS truststore path |
| `tls.truststorePassword` | string | no | — | Truststore password |
| `tls.keystore` | string | no | — | JKS keystore path |
| `tls.keystorePassword` | string | no | — | Keystore password |
| `tls.trustAll` | bool | no | false | Trust all certificates (dev only; requires `tls=true`) |
| `stream` | string | yes** | — | Stream name |
| `superstream` | string | yes** | — | Superstream name |
| `consumerName` | string | no | queryId | Consumer name for offset tracking |
| `metadataFields` | string | no | `properties,application_properties,message_annotations,creation_time,routing_key` | Comma-separated list of optional metadata columns |
| `failOnDataLoss` | bool | no | true | Fail on data loss (retention truncation, missing partitions) |
| `addressResolverClass` | string | no | — | Custom address resolver (connector interface) |
| `observationCollectorClass` | string | no | — | Custom observation collector (connector interface) |
| `observationRegistryProviderClass` | string | no | — | Micrometer `ObservationRegistry` provider (connector interface) |
| `environmentId` | string | no | — | Environment pool key override |

\* At least one of `endpoints` or `uris` must be set.
\*\* Exactly one of `stream` or `superstream` must be set.

### Environment options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `rpcTimeoutMs` | long | — | RPC timeout for broker operations |
| `requestedHeartbeatSeconds` | long | — | Heartbeat interval |
| `lazyInitialization` | bool | false | Delay connection opening until first environment use |
| `scheduledExecutorService` | string | — | Scheduled executor service factory class |
| `netty.eventLoopGroup` | string | — | Netty event loop group factory class |
| `netty.byteBufAllocator` | string | — | Netty byte buffer allocator factory class |
| `netty.channelCustomizer` | string | — | Netty channel customizer class |
| `netty.bootstrapCustomizer` | string | — | Netty bootstrap customizer class |
| `forceReplicaForConsumers` | bool | — | Force consumers to connect to replicas |
| `forceLeaderForProducers` | bool | — | Force producers to connect to leader |
| `locatorConnectionCount` | int | — | Number of locator connections |
| `recoveryBackOffDelayPolicy` | string | — | Recovery backoff policy |
| `topologyUpdateBackOffDelayPolicy` | string | — | Topology update backoff policy |
| `maxProducersByConnection` | int | 256 | Max producers per connection |
| `maxConsumersByConnection` | int | 256 | Max consumers per connection |
| `maxTrackingConsumersByConnection` | int | 50 | Max tracking consumers per connection |
| `environmentIdleTimeoutMs` | long | 60000 | Idle environment eviction timeout |

### Source options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `startingOffsets` | string | earliest | `earliest`, `latest`, `offset`, or `timestamp` |
| `startingOffset` | long | — | Starting offset (required when `startingOffsets=offset`) |
| `startingTimestamp` | long | — | Starting timestamp in epoch millis (required when `startingOffsets=timestamp`) |
| `startingOffsetsByTimestamp` | string | — | Per-stream starting timestamps as JSON map (e.g. `{"stream-0": 1700000000000}`). Overrides `startingTimestamp` for individual streams when `startingOffsets=timestamp` |
| `endingOffsets` | string | latest | `latest`, `offset`, or `timestamp` (batch only) |
| `endingOffset` | long | — | Ending offset (required when `endingOffsets=offset`) |
| `endingTimestamp` | long | — | Ending timestamp in epoch millis (required when `endingOffsets=timestamp`, unless `endingOffsetsByTimestamp` is set) |
| `endingOffsetsByTimestamp` | string | — | Per-stream ending timestamps as JSON map (e.g. `{"stream-0": 1700000000000}`). Overrides `endingTimestamp` for individual streams when `endingOffsets=timestamp` |
| `maxRecordsPerTrigger` | long | — | Max records per micro-batch trigger |
| `maxBytesPerTrigger` | long | — | Max bytes per trigger (best-effort, uses estimated message size) |
| `minOffsetsPerTrigger` | long | — | Min records before triggering a micro-batch (streaming only). Delays processing until threshold is met or `maxTriggerDelay` expires |
| `maxTriggerDelay` | string | 15m | Max delay before processing a micro-batch regardless of `minOffsetsPerTrigger`. Duration format (e.g. `30s`, `5m`, `1h`) |
| `minPartitions` | int | — | Minimum Spark input partitions (splits streams by offset ranges) |
| `maxRecordsPerPartition` | long | — | Max records per Spark input partition. Streams with more records are split into smaller partitions. Can be combined with `minPartitions` |
| `serverSideOffsetTracking` | bool | true (streaming) / false (batch) | Store offsets in RabbitMQ broker on commit |
| `singleActiveConsumer` | bool | false | Enable single active consumer (requires `consumerName`) |
| `pollTimeoutMs` | long | 30000 | Queue poll timeout per pull |
| `maxWaitMs` | long | 300000 | Max wait before failing a reader task |
| `callbackEnqueueTimeoutMs` | long | 5000 | Callback enqueue timeout |
| `initialCredits` | int | 1 | Initial chunk credits |
| `queueCapacity` | int | 10000 | Internal queue capacity (messages) |
| `estimatedMessageSizeBytes` | int | 1024 | Initial message size estimate for byte-based limits |

### Filtering options (RabbitMQ 3.13+)

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `filterValues` | string | — | Comma-separated filter values for server-side filtering |
| `filterMatchUnfiltered` | bool | false | Also deliver messages without a filter value |
| `filterPostFilterClass` | string | — | Custom client-side post-filter (message-view connector interface) |
| `filterWarningOnMismatch` | bool | true | Log warnings when post-filter drops messages |

### Sink options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `producerName` | string | — | Producer name (enables deduplication if set) |
| `publishing_id` | long column | — | Optional per-row publishing ID override in sink schema |
| `publisherConfirmTimeoutMs` | long | — | Publisher confirm timeout (`0` disables timeout and waits for confirms indefinitely) |
| `maxInFlight` | int | — | Max unconfirmed messages |
| `enqueueTimeoutMs` | long | 10000 | Send blocking timeout when `maxInFlight` reached |
| `batchPublishingDelayMs` | long | — | Batch publishing delay |
| `compression` | string | none | Compression: `none`, `gzip`, `snappy`, `lz4`, `zstd` (requires `subEntrySize > 1`) |
| `subEntrySize` | int | — | Sub-entry size (>1 enables batching/compression; disables dedup guarantees) |
| `retryOnRecovery` | bool | — | Retry on connection recovery |
| `dynamicBatch` | bool | — | Enable dynamic batching |
| `compressionCodecFactoryClass` | string | — | Custom compression codec factory (connector interface) |
| `routingStrategy` | string | hash | Superstream routing: `hash`, `key`, or `custom` |
| `hashFunctionClass` | string | — | Custom hash function class (optional, only for `routingStrategy=hash`) |
| `partitionerClass` | string | — | Custom routing strategy class (required when `routingStrategy=custom`) |
| `filterValuePath` | string | — | Producer-side filter value path (`application_properties.*`, `message_annotations.*`, `properties.*`) |
| `filterValueExtractorClass` | string | — | Custom producer-side filter value extractor class |
| `ignoreUnknownColumns` | bool | false | Ignore unrecognized input columns instead of failing |

## Offset handling and semantics

### At-least-once delivery

The connector provides at-least-once semantics for both source and sink:
- **Source**: If a task fails before Spark commits, the same offset range is re-read on retry.
- **Sink**: Publisher confirms ensure messages are persisted. With deduplication enabled (`producerName` set), streaming retries in the same `(partitionId, epochId)` reuse the same producer identity and publishing sequence.

### Offset sources (priority order)

On startup, the connector resolves starting offsets in this order:
1. **Spark checkpoint** (if present) — always takes precedence
2. **RabbitMQ stored offset** (if `consumerName` is set and broker has a stored offset)
3. **`startingOffsets` option** — fallback

### Server-side offset tracking

When `serverSideOffsetTracking=true` (default for streaming), the connector stores the last-processed offset in RabbitMQ on each Spark commit via `Environment.storeOffset()`. This is best-effort metadata — Spark checkpoint remains the source of truth.

### Offset serialization

Checkpoint offsets are JSON maps of stream name to next offset:
```json
{"my-stream": 12345}
```

For superstreams, each partition stream has its own entry:
```json
{"orders-0": 500, "orders-1": 750, "orders-2": 300}
```

## Superstreams

### Source

One Spark input partition is created per partition stream. Offsets are tracked per partition stream name.

### Sink

Routing is handled by the RabbitMQ superstream producer. Available routing strategies:
- **`hash`** (default): Hashes the routing key (MurmurHash3 32-bit) modulo partition count.
- `hashFunctionClass` can override the default hash when `routingStrategy=hash`.
- **`key`**: Routes using superstream bindings; can route to multiple partition streams.
- **`custom`**: User-provided routing via `partitionerClass` with access to full
  `ConnectorMessageView` and routing metadata (`partitions()` and `route(routingKey)`).

The `stream` column is ignored in superstream mode; routing determines the target partition stream.

### Topology discovery

The connector discovers superstream partitions from broker metadata at planning time. Ensure `advertised_host`/`advertised_port` are correctly configured when running behind load balancers or in containers.

## Operational notes

### Backpressure

The connector uses credit-based flow control. Each reader starts with `initialCredits` (default 1) chunk credits and grants additional credits as the internal queue drains. Queue capacity (`queueCapacity`, default 10000) bounds memory usage.

### `failOnDataLoss`

When `true` (default), the connector fails if:
- The requested start offset is below the stream retention floor (truncated data)
- A partition stream is missing (superstream topology change or deletion)

When `false`, the connector logs a warning and continues: advancing to the first available offset for truncated streams, or skipping missing partitions.

### Capacity planning

- Each `Environment` manages multiple connections with defaults: 256 producers/connection, 256 consumers/connection, 50 tracking consumers/connection.
- Connections scale with partitions: `ceil(P/256)` for consumers, `ceil(W/256)` for producers.
- Superstreams with >50 partitions will use multiple tracking connections for offset lookup.

### TLS

TLS is configured via JKS keystores/truststores converted to Netty `SslContext` internally. Set `tls=true` and provide keystore/truststore paths and passwords. `tls.trustAll=true` disables certificate verification (development only).

### Deduplication constraints

- Only one live producer per `producerName` is allowed.
- Publishing must be single-threaded per producer name.
- Deduplication is not guaranteed when sub-entry batching/compression is enabled (`subEntrySize > 1`).
- Streaming deduplication names are derived as `producerName + "-p" + partitionId + "-e" + epochId` and start publishing IDs at `0` for each epoch-scoped producer identity.
- Batch deduplication names remain task-scoped (`... + "-t" + taskId`) to avoid attempt collisions.

## Extension interfaces

The connector provides extension points via connector-defined interfaces (not RabbitMQ client types) to avoid shaded dependency issues:

| Option | Interface | Runs on | Description |
|--------|-----------|---------|-------------|
| `addressResolverClass` | `ConnectorAddressResolver` | Driver | Custom address resolution |
| `filterPostFilterClass` | `ConnectorPostFilter` | Executors | Client-side message filtering |
| `filterValueExtractorClass` | `ConnectorFilterValueExtractor` | Executors | Producer-side filter value derivation |
| `hashFunctionClass` | `ConnectorHashFunction` | Executors | Custom hash function for superstream hash routing |
| `partitionerClass` | `ConnectorRoutingStrategy` | Executors | Custom superstream routing with message view + metadata lookup |
| `observationCollectorClass` | `ConnectorObservationCollectorFactory` | Both | Custom observability |
| `observationRegistryProviderClass` | `ConnectorObservationRegistryProvider` | Both | Provide Micrometer observation registry |
| `scheduledExecutorService` | `ConnectorScheduledExecutorServiceFactory` | Both | Custom scheduled executor service |
| `netty.eventLoopGroup` | `ConnectorNettyEventLoopGroupFactory` | Both | Custom Netty event loop group |
| `netty.byteBufAllocator` | `ConnectorNettyByteBufAllocatorFactory` | Both | Custom Netty byte buffer allocator |
| `netty.channelCustomizer` | `ConnectorNettyChannelCustomizer` | Both | Customize Netty channel setup |
| `netty.bootstrapCustomizer` | `ConnectorNettyBootstrapCustomizer` | Both | Customize Netty bootstrap setup |
| `compressionCodecFactoryClass` | `ConnectorCompressionCodecFactory` | Executors | Custom compression codec |

To keep connector interfaces free of shaded dependency types, low-level RabbitMQ/Netty
hooks use `Object` in their method signatures. The connector validates runtime types:
`ObservationCollector`, `CompressionCodecFactory`, `EventLoopGroup`,
`ByteBufAllocator`, `Channel`, and `Bootstrap`.

All extension classes must have a public no-arg constructor.

## Limitations

- No AMQP 0-9-1 support — uses native RabbitMQ Streams protocol only.
- No Spark DStream (legacy streaming) support.
- Exactly-once sink guarantees require broker-side deduplication constraints to be satisfied.
- `application_properties` and `message_annotations` values are coerced to string (lossy).
- Byte-based admission control (`maxBytesPerTrigger`) is best-effort, based on estimated message sizes.
- `minPartitions` splitting increases connections and may discard messages at chunk boundaries.
- `Trigger.AvailableNow` snapshots tail offsets once; new data arriving after the snapshot is not included.

## Building from source

### Prerequisites

- Java 17 or 21
- Maven 3.8+
- Docker (for integration tests)

### Build

```bash
# Build all modules (skip tests)
mvn -q -DskipTests package

# Build for a specific Spark version
mvn -q -DskipTests package -Pspark41
mvn -q -DskipTests package -Pspark40
mvn -q -DskipTests package -Pspark35
```

### Test

```bash
# Unit tests
mvn verify -pl '!it-tests'

# Integration tests (requires Docker)
mvn verify -pl it-tests -am
mvn verify -pl it-tests -am -Pspark35
mvn verify -pl it-tests -am -Pspark35-scala212
mvn verify -pl it-tests -am -Pspark40
mvn verify -pl it-tests -am -Pspark41

# All tests
mvn verify
```

See [TESTING.md](TESTING.md) for detailed testing instructions including mutation testing and Docker configuration.

## License

Apache License 2.0
