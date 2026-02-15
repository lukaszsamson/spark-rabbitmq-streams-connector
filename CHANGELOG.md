# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [Unreleased]

## [0.1.0] - 2026-02-15

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
- Optional server-side offset tracking via RabbitMQ `storeOffset()`.
- Broker offset recovery on startup (checkpoint > broker > startingOffsets).
- Admission control via `maxRecordsPerTrigger` and `maxBytesPerTrigger`.
- `Trigger.AvailableNow` support with tail offset snapshot.
- `minPartitions` for splitting streams into multiple Spark partitions by offset ranges.
- Split-offset merge algorithm for contiguous checkpoint advancement.
- Superstream support: topology discovery, partition-per-stream reads, routing strategies (`hash`, `key`, `custom`).
- Publisher confirms with optional deduplication via `producerName`.
- Speculation-safe producer naming with `taskId` suffix.
- Stream filtering (RabbitMQ 3.13+): `filterValues`, `filterMatchUnfiltered`, client-side post-filter.
- Producer-side filter values via `filterValueColumn`.
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
