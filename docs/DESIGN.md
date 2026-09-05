# Rivet: Durable Event Broker Design

## 1. Purpose and learning contract

Rivet is a small durable event broker built to practice Rust and systems programming. It accepts events from producers, persists them to append-only logs, and lets independent consumers read and replay events using durable offsets.

Rivet is not intended to compete with Kafka, NATS, RabbitMQ, or Redpanda. Its purpose is to expose the mechanics of a durable event log while exercising ownership, borrowing, enums, traits, error handling, file I/O, binary encoding, iterators, concurrency, async Rust, networking, synchronization, testing, and performance measurement.

The architecture in this document is intentionally provided. Treat it as a product and architecture specification rather than redesigning the system while learning Rust. Change a fixed decision only when implementation evidence shows that it is unworkable, and record the reason.

### AI learning contract

The learner writes all new production implementation code.

AI may:

- explain Rust concepts and compiler errors;
- ask Socratic questions and challenge a proposed approach;
- review code after the learner writes it;
- suggest tests and edge cases;
- help diagnose bugs;
- compare approaches and explain tradeoffs;
- help maintain architecture, plans, and documentation.

AI must not generate production implementation code unless the learner explicitly requests an exception. Prefer guidance, pseudocode, API-level discussion, and review over completed Rust solutions. Tests should normally be written by the learner too; AI may specify what to test unless explicitly asked to generate test code.

## 2. Product shape

Working name: **Rivet**

Executables:

- `rivetd`: broker server
- `rivet`: command-line client

Examples:

```text
rivetd
rivet topic create orders
rivet publish orders '{"order_id":123}'
rivet consume orders
```

Rivet is a persistent ordered event log. Events are not removed merely because they have been consumed. Each consumer group owns an independent position, so many consumers can process the same stream.

## 3. Non-goals

Do not initially implement:

- distributed clustering, replication, leader election, or consensus;
- authentication, authorization, or TLS;
- schemas or cross-topic transactions;
- exactly-once delivery;
- cloud deployment or Kubernetes;
- a web UI;
- SQL-backed storage;
- plugin systems or dynamic configuration services.

These would distract from the learning objective.

## 4. Fundamental data model

```text
Broker
 └── Topic
      └── Partition
           └── Segment
                └── Record
```

The initial version gives each topic exactly one partition. Multiple partitions arrive later.

### Record

Every persisted event has:

```text
Record {
    offset: u64,
    timestamp: u64,
    key: optional bytes,
    payload: bytes,
}
```

The storage engine treats the key and payload as opaque bytes. The CLI may accept JSON for convenience, but JSON has no special storage semantics.

### Offsets

Offsets are monotonically increasing unsigned 64-bit integers scoped to a partition. The first record is offset `0`; appending returns the assigned offset. Offsets are never reused.

Ordering is guaranteed only within a partition. There is no global ordering guarantee across partitions.

## 5. Storage model

Each partition is an append-only segmented log on disk:

```text
data/
  orders/
    0/
      00000000000000000000.log
      00000000000001000000.log
    1/
      00000000000000000000.log
```

A segment filename is the first offset it contains. Closed segments are immutable; only the active segment may be appended to.

### Binary record format, version 1

All integer fields use big-endian encoding.

| Field | Size |
| --- | ---: |
| magic (`RIVT`) | 4 bytes |
| version (`1`) | 1 byte |
| offset | 8 bytes |
| timestamp | 8 bytes |
| key length | 4 bytes |
| value length | 4 bytes |
| key | variable |
| payload | variable |
| checksum | 4 bytes |

The checksum is CRC32 over `version`, `offset`, `timestamp`, `key_length`, `value_length`, `key`, and `payload`. It excludes the magic bytes and checksum field.

Reading validates:

1. magic bytes;
2. supported version;
3. configured key and payload length limits before allocation;
4. presence of the complete record;
5. CRC32 equality.

An invalid record produces a typed storage error.

### Durability

The first durable implementation acknowledges a publish only after the equivalent of:

```text
write()
flush()
sync_data()
```

Correctness is favored over throughput. Batching is a later experiment.

### Crash recovery

At startup:

1. discover segments and sort them by base offset;
2. validate the ordered, immutable segments;
3. scan every record in the active segment;
4. identify the end of the final valid record;
5. truncate a partial trailing record;
6. determine the next offset.

A partial trailing record is recoverable. A CRC failure or malformed complete record in the middle of a segment is fatal corruption; do not attempt sophisticated repair.

Restarting without new writes must not change the observable log.

### Segment rotation

`max_segment_bytes` defaults to 64 MiB. When the active segment reaches or exceeds the configured threshold, close it and create a segment whose filename is the next event offset.

## 6. Topics and partitions

Topic names are 1–128 characters and contain only ASCII letters, digits, `-`, and `_`. Names such as `../etc/passwd` and `orders/foo` are invalid.

Topics are explicitly created:

```text
rivet topic create orders
rivet topic list
```

Topic deletion is not part of the first implementation.

When multiple partitions are introduced:

```text
rivet topic create orders --partitions 4
```

- an unkeyed publish uses round-robin selection;
- a keyed publish uses `stable_hash(key) % partition_count`;
- the hash algorithm must be explicitly chosen, deterministic across processes and versions, and not a randomized process-local hash.

## 7. Publishing and consuming

Publish:

```text
rivet publish orders '{"order_id":123}'
rivet publish orders --key customer-123 '{"order_id":123}'
```

Successful output includes topic, partition, and assigned offset.

Consume:

```text
rivet consume orders
rivet consume orders --offset 100
rivet consume orders --follow
rivet consume orders --group fraud-detector
```

`--follow` behaves like `tail -f` for an event log.

### Consumer groups

Consumer groups provide durable and independent reader positions. Persist the **next offset to read**, not the last processed offset. If a consumer handles offset `42`, a successful commit stores `43`.

The delivery contract is at least once. A crash after handling an event but before committing may cause redelivery. Exactly-once delivery is not supported.

Offsets are stored on disk:

```text
data/
  __consumer_offsets/
    fraud-detector/
      orders/
        0.offset
```

The file contains the next offset as text, such as `43`. Updates are atomic: write a temporary file, sync it, and atomically rename it. The implementation must also consider the durability of the containing directory entry.

Updating one consumer group must never affect another.

## 8. Logical broker API

Filesystem details are hidden from callers. The logical operations are:

```text
create_topic(name, partition_count)
list_topics()
publish(topic, key, payload) -> PublishResult
read(topic, partition, offset) -> Option<Record>
commit_offset(group, topic, partition, next_offset)
get_committed_offset(group, topic, partition) -> Option<u64>
```

Use meaningful typed errors that retain their causes. Expected categories include:

```text
BrokerError
StorageError
ProtocolError
ConfigurationError
TopicError
ConsumerError
```

Strings are not the primary internal error representation. An error helper library may be adopted after understanding the standard error traits.

## 9. Concurrency model

Introduce concurrency only after single-threaded storage is correct.

Multiple producers may publish concurrently and multiple consumers may read concurrently. Each partition has exactly one logical writer, preserving deterministic partition order and exclusive mutation of its active file.

```text
Producer A ─┐
Producer B ─┼──> bounded partition queue ──> one partition writer ──> disk
Producer C ─┘
```

Use standard threads and channels first where practical to learn `Arc`, `Mutex`, `RwLock`, ownership across threads, `Send`, and `Sync`.

Later, Tokio handles external requests. Network tasks send commands through bounded async channels to partition writers. Synchronous disk I/O may remain behind the writer boundary initially. Never hold a synchronous lock across `.await`.

Bounded queues provide backpressure. When full, publishing waits or returns an explicit timeout/error according to the eventual API; events are never silently dropped.

## 10. TCP protocol

Use a custom length-delimited TCP protocol before considering HTTP:

```text
4-byte big-endian frame length
1-byte operation
operation-specific payload
```

Operations:

| Code | Operation |
| --- | --- |
| `0x01` | `CREATE_TOPIC` |
| `0x02` | `LIST_TOPICS` |
| `0x03` | `PUBLISH` |
| `0x04` | `READ` |
| `0x05` | `COMMIT_OFFSET` |
| `0x06` | `GET_OFFSET` |

Responses use equivalent framing and typed status/error information. The maximum frame is 8 MiB. Reject an oversized length before allocating its payload. Handle EOF in the middle of a frame as a protocol error without affecting the server.

`rivetd` listens on `127.0.0.1:7411` by default. At this stage `rivet` becomes a network client rather than directly opening broker storage.

## 11. Sparse index

Each segment may have a rebuildable sparse index named with the same base offset and `.idx` extension. Every configured N records, store `offset -> byte position`.

To locate offset 253, find the greatest indexed offset no larger than 253, seek to its byte position, then scan forward. The log is authoritative; a missing or corrupt index is rebuilt from it.

## 12. Retention

Support maximum total bytes first and maximum record age later. Retention removes only complete closed segments and never the active segment.

Retention initially ignores consumer positions. A slow consumer may fall behind and receive `OffsetOutOfRange`, which reports the earliest and latest available offsets.

## 13. Observability

Expose metrics such as:

```text
events_published_total
bytes_written_total
publish_latency
active_connections
consumer_lag
segment_count
disk_bytes
```

Log lifecycle events including topic opening, recovery, rotation, corruption, offset commits, and shutdown. Do not log event payloads by default.

## 14. Graceful shutdown

On SIGINT or SIGTERM:

1. stop accepting connections;
2. stop accepting new publishes;
3. drain pending writes within a configured timeout;
4. flush and sync active logs;
5. finish in-flight offset commits;
6. close files and exit.

Shutdown is bounded. If the deadline expires, report the incomplete work clearly and exit without pretending that unacknowledged writes succeeded.

## 15. Expected eventual module shape

Grow into this layout; do not create every module on day one.

```text
src/
  main.rs
  broker/
    mod.rs
    broker.rs
    topic.rs
    partition.rs
  storage/
    mod.rs
    log.rs
    segment.rs
    record.rs
    index.rs
  consumer/
    mod.rs
    group.rs
    offset_store.rs
  protocol/
    mod.rs
    frame.rs
    request.rs
    response.rs
  server/
    mod.rs
    connection.rs
  client/
    mod.rs
  config/
    mod.rs
  error.rs
```

## 16. Dependency philosophy

Keep dependencies deliberately small. Eventual dependencies for Tokio, CLI parsing, CRC32, and structured logging are appropriate. Avoid crates that supply the interesting parts of the exercise: durable logs, embedded queues, actor frameworks, distributed messaging, or storage engines.

## 17. Testing strategy

### Unit tests

Cover record encoding/decoding, CRC validation, topic validation, offset arithmetic, routing, and protocol framing.

### Storage tests

Use temporary directories. Cover append, reopen/restart, recovery, truncation, rotation, cross-segment reads, and index rebuilding.

### Integration tests

Start `rivetd` and use the network client. Cover topic creation, publish, consume, commit, server restart, and consumer resume.

### Optional property tests

Useful invariants include:

```text
decode(encode(record)) == record
restart(append(records)).read_all() == records
```

## 18. Benchmarks and batching experiment

Do not optimize before correctness. Eventually measure:

- single and concurrent producer throughput;
- publish p50, p95, and p99 latency;
- sequential read throughput;
- random offset lookup latency;
- startup recovery time.

The baseline performs one sync per message. Later compare it with a bounded batch such as up to 5 ms or 100 records followed by one sync. Record throughput, latency, and durability tradeoffs; do not retain a faster design without understanding them.

## 19. Correctness invariants

- **Offset uniqueness:** no two records in a partition share an offset.
- **Offset ordering:** each subsequent record has a greater offset.
- **Append-only storage:** existing valid records are never modified.
- **Deterministic recovery:** repeated restart without writes preserves observable contents.
- **Consumer isolation:** commits by one group cannot affect another.
- **Partition ordering:** successful publishes appear in assigned-offset order.
- **Acknowledged durability:** after a publish is acknowledged, it survives a process restart under the documented storage assumptions.
- **Single writer:** only one logical writer mutates a partition's active segment.

## 20. Required failure scenarios

Test termination during publish, sync, segment rotation, and consumer commit. Also test:

- an empty segment;
- missing and corrupt indexes;
- a partial trailing record;
- invalid CRC in the middle of a segment;
- missing topic and unknown consumer;
- invalid and retained-away offsets;
- client disconnect during a request;
- oversized and partial network frames;
- slow consumers and producers;
- a full bounded queue;
- shutdown with pending work.

These failures are part of the project, not optional polish.

## 21. Delivery phases

The implementation backlog is intentionally split into 24 independently testable milestones:

1. project skeleton and test setup;
2. record model;
3. in-memory partition;
4. binary record codec;
5. append-only file;
6. restart and recovery;
7. CRC validation;
8. segmented logs;
9. topics;
10. durable consumer offsets;
11. local CLI;
12. thread-safe broker;
13. partition writer queues;
14. Tokio runtime;
15. TCP framing protocol;
16. network client;
17. multiple partitions;
18. stable key-based routing;
19. backpressure;
20. sparse index;
21. retention;
22. graceful shutdown;
23. metrics and lifecycle logging;
24. benchmarks and batching experiment.

The GitHub issues define scope, acceptance criteria, tests, Rust concepts, and dependencies for each milestone.

## 22. Definition of version 1.0

Rivet 1.0 supports durable topics, multiple partitions, append-only segmented storage, CRC-protected records, deterministic crash recovery, persistent consumer groups, at-least-once consumption, stable keyed routing, concurrent producers and consumers, a TCP server, a CLI client, backpressure, a sparse rebuildable index, size-based retention, bounded graceful shutdown, metrics, and reproducible benchmarks.

It remains a single-node learning broker. Distributed features and exactly-once semantics remain non-goals.
