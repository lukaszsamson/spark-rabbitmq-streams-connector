# Testing

## Prerequisites

- Java 17+
- Maven 3.8+
- Docker (for integration tests only)

## Unit Tests

Unit tests live in `core`, `spark35`, `spark40`, and `spark41` modules. They use mocks and do not require Docker. The `spark35_2.12` module compiles the same sources against Spark 3.5 with Scala 2.12.

```bash
mvn verify -pl core,spark35,spark35_2.12,spark40,spark41 -am
```

## Mutation Testing (PIT)

Mutation testing is configured in the `spark35-tests`, `spark35_2.12-tests`, `spark40-tests`, and `spark41-tests` modules
to exercise the shared unit test suite against each Spark version.

```bash
mvn -pl spark35-tests -am org.pitest:pitest-maven:mutationCoverage
```

```bash
mvn -pl spark35_2.12-tests -am org.pitest:pitest-maven:mutationCoverage
```

```bash
mvn -pl spark40-tests -am org.pitest:pitest-maven:mutationCoverage
```

```bash
mvn -pl spark41-tests -am org.pitest:pitest-maven:mutationCoverage
```

## Integration Tests

Integration tests live in the `it-tests` module. They use [Testcontainers](https://java.testcontainers.org/) to run RabbitMQ in Docker with the stream plugin enabled.

### Running

By default, integration tests run against Spark 4.1:

```bash
mvn verify -pl it-tests -am
```

Use a profile to run against a specific Spark version:

```bash
# Spark 3.5 (Scala 2.13)
mvn verify -pl it-tests -am -Pspark35

# Spark 3.5 (Scala 2.12)
mvn verify -pl it-tests -am -Pspark35-scala212

# Spark 4.0
mvn verify -pl it-tests -am -Pspark40

# Spark 4.1 (default, profile optional)
mvn verify -pl it-tests -am -Pspark41
```

Run a subset of integration tests:

```bash
mvn verify -pl it-tests -am -Dit.test=BatchReadIT,SuperStreamIT
```

### Docker configuration

Testcontainers discovers Docker automatically. If Docker is not available, tests are **skipped** (not failed) via `@Testcontainers(disabledWithoutDocker = true)`.

On macOS with Docker Desktop, if the default socket at `/var/run/docker.sock` does not exist, set `DOCKER_HOST` to point to the actual socket:

```bash
export DOCKER_HOST=unix://$HOME/.docker/run/docker.sock
```

Alternatively, enable "Allow the default Docker socket to be used" in Docker Desktop settings.

### What the integration tests cover

| Class | Tests |
|-------|-------|
| `BatchReadIT` | Batch reads: all messages, empty stream, offset ranges, content, metadata fields, monotonic offsets, minPartitions, failOnDataLoss, non-existent stream, environment pool reuse |
| `BatchWriteIT` | Batch writes: write and read back, application properties, round-trip via connector |
| `StreamingIT` | Structured Streaming: Trigger.AvailableNow, maxRecordsPerTrigger, streaming write, checkpoint resume, server-side offset tracking, broker offset recovery |
| `SuperStreamIT` | Superstreams: batch read across partitions, batch write with routing, routing_key column, streaming write, partition stream names, failOnDataLoss with deleted partitions |

## Running All Tests

```bash
mvn verify
```
