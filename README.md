![License](http://img.shields.io/:license-apache%202.0-brightgreen.svg)
![Maven Central](https://img.shields.io/maven-central/v/io.debezium/debezium-connector-milvus)
![Build Status](https://img.shields.io/github/actions/workflow/status/debezium/debezium-connector-milvus/maven.yml?branch=main&logo=github&label=Maven%20CI)
![User chat](https://img.shields.io/badge/chat-users-brightgreen.svg)
![Developer chat](https://img.shields.io/badge/chat-devs-brightgreen.svg)
![Google Group](https://img.shields.io/:mailing%20list-debezium-brightgreen.svg)
![Stack Overflow](http://img.shields.io/:stack%20overflow-debezium-brightgreen.svg)

Copyright Debezium Authors.
Licensed under the [Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0).

# Debezium Connector for Milvus

Debezium is an open source project that provides a low latency data streaming platform for change data capture (CDC).

This repository contains the incubating connector for [Milvus](https://milvus.io/), the open source vector database, which is in an **early stage of its development**.
You are encouraged to explore this connector and test it, but it is not recommended yet for production usage.
E.g. the format of emitted messages may change, specific features may not be implemented yet etc.

## Supported features

- Captures row-level inserts and deletes from Milvus collections and emits them as Debezium change events.
- Performs an initial snapshot of existing collection data at `Strong` consistency, anchored to the etcd channel checkpoint so that streaming resumes from the MQ offset the checkpoint records. The handoff is at-least-once: writes that land between the checkpoint and the snapshot query can be emitted both as snapshot (`op=r`) and streaming (`op=c`/`op=d`) events.
- Streams changes from the Milvus physical channel (pchannel) on the Milvus message queue backend, with support for both the msgpack batch and single protobuf message wire formats. The format is auto-detected by default by probing the pchannel when streaming starts from the stored offset on restart, re-probing from the earliest message if nothing recognizable follows it; set `milvus.wire.format` explicitly to skip the probe. If the probe finds no data messages, the connector falls back to `msgpack_batch` and logs a warning.
- Re-orders events across virtual channels (vchannels) into strict Milvus TSO order using the timetick watermark protocol, so downstream consumers observe changes in the same order Milvus committed them.
- Infers collection schemas dynamically, from the first insert event when streaming and from the Milvus metadata API during the snapshot, so collections do not need to be declared up front.
- Maps Milvus vector types to Debezium logical types, including `io.debezium.data.vector.FloatVector` for float vectors and `io.debezium.data.Json` for JSON and geometry fields.
- Filters captured collections with include/exclude lists of literal names or Java regular expressions.
- Supports interval-based heartbeats, Debezium notifications, and AVRO/JSON Connect converters.
- Exposes snapshot and streaming metrics over JMX, including Milvus-specific TSO watermark lag.



## How the connector works

Milvus does not expose a change stream over its gRPC API. Instead, Milvus publishes every write to an internal message queue (MQ) channel before it persists the write, and the connector reads that channel directly. Reading changes therefore requires access to three Milvus components:

1. **The MQ backend (Kafka).** The connector subscribes to the pchannel topic (`milvus.pchannel.name`, e.g. `by-dev-rootcoord-dml_0`) and consumes the raw insert, delete, and timetick messages Milvus writes there.
2. **etcd.** Milvus stores the per-channel checkpoint (its `guarantee_ts` TSO and the corresponding MQ offset) in etcd. The connector reads it to anchor the snapshot and to resume streaming at the offset the checkpoint records. The Milvus API does not expose checkpoint data, so the connector needs direct etcd access.
3. **The Milvus gRPC API.** The connector uses it during the snapshot to list collections, read their schemas, and query rows, and during streaming to resolve the schema of a collection it has not yet seen in an insert event.

The task runs the standard Debezium pipeline. During the snapshot phase it reads the etcd checkpoint, queries every included collection with `consistency_level=Strong`, and emits each row as an `op=r` event. The checkpoint `guarantee_ts` value is recorded for traceability but is not applied to the query: the Milvus SDK manages `guarantee_ts` internally, so the snapshot reflects the collection state at query time, which is later than the checkpoint TSO. The task then records `snapshot_completed=true` and hands off to streaming, which seeks the MQ consumer to the checkpoint offset. Changes written between the checkpoint and the snapshot query can therefore appear in both phases; downstream consumers should treat the stream as at-least-once and de-duplicate by primary key.

While streaming, the connector deserializes each raw MQ message and buffers it in the timetick ordering engine. Milvus multiplexes several vchannels onto one pchannel, and each vchannel advances its own timetick. The engine holds an event until the global watermark, which is the minimum timetick across all vchannels, passes that event's TSO. At that point no vchannel can still produce anything older, so the engine releases the event in strict TSO order. The connector dispatches released events through the Debezium `EventDispatcher` and only then advances the offset, so it never commits an offset for an event it has not emitted. If a vchannel stops advancing its timetick for longer than `milvus.timetick.stall.timeout.ms`, the engine force-flushes what it has buffered rather than stalling indefinitely.

## Requirements

- Milvus 2.5 or later, deployed with **Kafka as the MQ backend** (`mq.type: kafka` in the Milvus configuration). Deployments backed by Pulsar or RocksMQ are not supported.
- Network access from the connector to the Milvus gRPC endpoint, to the Kafka cluster Milvus uses as its MQ, and to the etcd cluster backing Milvus.
- Java 17 or later.



## Example configuration

```json
{
  "name": "milvus-connector",
  "config": {
    "connector.class": "io.debezium.connector.milvus.MilvusConnector",
    "topic.prefix": "milvus",
    "milvus.uri": "http://localhost:19530",
    "milvus.database": "default",
    "milvus.etcd.endpoints": "http://localhost:2379",
    "milvus.etcd.root.path": "by-dev",
    "milvus.kafka.bootstrap.servers": "localhost:9092",
    "milvus.pchannel.name": "by-dev-rootcoord-dml_0",
    "milvus.collection.include.list": "orders.*",
    "snapshot.mode": "initial"
  }
}
```

The connector supports a single task; `tasks.max` values greater than `1` are ignored.

## Limitations

- The connector supports a single task and consumes a single pchannel per connector instance.
- Only Kafka is supported as the Milvus MQ backend.
- The connector emits no update (`op=u`) events. Milvus implements an upsert as a delete followed by an insert, and the connector emits that pair.
- Delete events carry a primary-key-only `before` image, because Milvus does not publish the prior state of a deleted entity.
- Collection DDL (create/drop) is tracked internally for ordering but is not emitted as schema change events.



## Building and testing the Milvus connector

Building this connector first requires the main [debezium](https://github.com/debezium/debezium) code repository to be built locally using `mvn clean install`.

After that, running `mvn install` will compile all code and run the unit and integration tests. If there are any compile problems or any of the unit tests fail, the build will stop immediately. Otherwise, the command will continue to create the module's artifacts, start the Docker containers required by the integration tests, run the integration tests, stop the containers (even if there are integration test failures), and run checkstyle on the code. If there are still no problems, the build will then install the module's artifacts into the local Maven repository.

You should always default to using `mvn install`, especially prior to committing changes to Git. However, there are a few situations where you may want to run a different Maven command, described below.

## Using the Milvus connector with Kafka Connect

The Milvus connector is designed to work with [Kafka Connect](http://kafka.apache.org/documentation.html#connect) and to be deployed to a Kafka Connect runtime service. The deployed connector will monitor one Milvus cluster and write all change events to Kafka topics, which can be independently consumed by one or more clients. Kafka Connect can be distributed to provide fault tolerance to ensure the connector is running and continually keeping up with changes in the database.

Kafka Connect can also be run standalone as a single process, although doing so is not tolerant of failures.

## Embedding the Milvus connector

The Milvus connector can also be used as a library without Kafka or Kafka Connect, enabling applications and services to directly connect to a Milvus cluster and obtain the ordered change events. This approach requires the application to record the progress of the connector so that upon restart the connector can continue where it left off. Therefore, this may be a useful approach for less critical use cases. For production use cases, we highly recommend using this connector with Kafka and Kafka Connect.

## Testing

This module contains both unit tests and integration tests.

A *unit test* is a JUnit test class named `*Test.java` or `Test*.java` that never requires or uses external services, though it can use the file system and can run any components within the same JVM process. They should run very quickly, be independent of each other, and clean up after itself.

An *integration test* is a JUnit test class named `*IT.java` or `IT*.java` that runs against a real Milvus cluster. Milvus only produces a change stream when its full stack is running, so the build starts four containers on a shared Docker network: etcd, MinIO, Kafka, and Milvus. The Milvus container is configured to use Kafka as its MQ backend (see `src/test/resources/milvus-it-user.yaml`). The build starts the containers in the `pre-integration-test` phase and stops and removes them in `post-integration-test`, regardless of whether the tests succeed or fail.

### Running some tests

If you are trying to get the test methods in a single integration test class to pass and would rather not run *all* of the integration tests, you can instruct Maven to just run that one integration test class and to skip all of the others. For example, use the following command to run the tests in the `MilvusStreamingPipelineIT.java` class:

```
$ mvn -Dit.test=MilvusStreamingPipelineIT install
```

Of course, wildcards also work:

```
$ mvn -Dit.test=MilvusStreaming*IT install
```

These commands will automatically manage the Docker containers.

### Debugging tests

If you want to debug integration tests by stepping through them in your IDE, using the `mvn install` command will be problematic since it will not wait for your IDE's breakpoints. It is typically far easier to simply start the containers and leave them running so that they are available when you run the integration test(s). The following command:

```
$ mvn docker:start
```

will start etcd, MinIO, Kafka, and Milvus. Now you can use your IDE to run/debug one or more integration tests. Be sure to run the tests with VM arguments that define the required system properties, including:

- `milvus.uri` - the Milvus gRPC endpoint; defaults to `http://localhost:19530`, which is what this module's containers expose
- `kafka.bootstrap.servers` - the Kafka cluster Milvus publishes its MQ channels to; defaults to `localhost:9092`

For example, you can define these properties by passing these arguments to the JVM:

```
-Dmilvus.uri=http://localhost:19530 -Dkafka.bootstrap.servers=localhost:9092
```

When you are finished running the integration tests from your IDE, you have to stop and remove the containers before you can run the next build:

```
$ mvn docker:stop
```



### Analyzing the database

Sometimes you may want to inspect the state of Milvus after one or more integration tests are run. The `mvn install` command runs the tests but shuts down and removes the containers after the integration tests complete. To keep them running after the integration tests complete, use this Maven command:

```
$ mvn integration-test
```

This instructs Maven to run the normal Maven lifecycle through `integration-test`, and to stop before the `post-integration-test` phase when the containers are normally shut down and removed. Be aware that you will need to manually stop and remove the containers before running the build again:

```
$ mvn docker:stop
```



### Skipping the containers entirely

If you only want to run the unit tests, or you are running against a Milvus cluster you manage yourself, you can disable the container lifecycle:

```
$ mvn install -Ddocker.skip=true -DskipITs
```



## Contributing

The Debezium community welcomes anyone that wants to help out in any way, whether that includes reporting problems, helping with documentation, or contributing code changes to fix bugs, add tests, or implement new features. See [this document](https://github.com/debezium/debezium/blob/main/CONTRIBUTING.md) for details.

### Building just the artifacts, without running tests, CheckStyle, etc.

You can skip all non-essential plug-ins (tests, integration tests, CheckStyle, formatter, API compatibility check, etc.) using the "quick" build profile:

```
$ mvn clean verify -Dquick
```

This provides the fastest way for solely producing the output artifacts, without running any of the QA related Maven plug-ins.
This comes in handy for producing connector JARs and/or archives as quickly as possible, e.g. for manual testing in Kafka Connect.