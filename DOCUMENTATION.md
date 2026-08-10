# A Decoupled Distributed Log Analytics Ecosystem: Design and Evaluation of a "Mini-Splunk" System

> **Format note:** This is a working draft structured to the required outline.
> The final submission must be typeset in the IEEE conference template
> (two-column, 10pt) and must not exceed 6 pages. Sections marked
> **[FILL IN]** require measurements taken from an actual run of the deployed
> system — do not submit with placeholders in place.

**Abstract** — *[FILL IN after results are collected: 150–250 words summarizing
the problem, the architecture, and the headline quantitative results.]*

**Keywords** — distributed systems, message-oriented middleware, log analytics,
containerization, fault tolerance, idempotent processing

---

## I. Introduction

Modern computing environments generate log data at a volume and velocity that
defeats single-machine processing. A conventional log analyzer — one process
that opens a file, parses it, and writes results into a local database —
couples ingestion, parsing, storage, and query into a single failure domain.
When any stage stalls, every stage stalls; when the host dies, the entire
capability dies with it.

This project implements a "Mini-Splunk" log analytics ecosystem that
deliberately abandons that monolithic structure. Standard syslog files are
ingested through a command-line forwarder, transported as discrete units of
work across message-oriented middleware, parsed in parallel by a pool of
interchangeable worker nodes, and indexed into a partitioned, replicated
storage cluster that is queried through a scatter-gather search head.

**Motivation for decoupling.** Three properties drove the architecture:

1. *Temporal decoupling* — the ingestion path must not block on worker
   availability. A client uploading a bulk log file receives acknowledgement
   as soon as the work is durably queued, not when parsing finishes.
2. *Horizontal scalability* — parsing throughput must scale by adding worker
   containers, with no change to configuration or code, and no coordination
   among workers themselves.
3. *Failure independence* — the loss of any single component (a worker, a
   storage node, or the broker) must degrade throughput rather than cause data
   loss or halt the system.

**Objectives, aligned to course learning outcomes.**

- **CLO1** — Analyze architectural patterns and communication models
  appropriate to scalable distributed systems, and justify the selection of a
  message-queue IPC model over synchronous RPC/REST for the ingestion path
  (Section II-B).
- **CLO2** — Implement a functional distributed system using modern
  middleware (RabbitMQ), containerization (Docker Compose), and inter-process
  communication, deployable from a single unified manifest (Sections II-A,
  II-D).
- **CLO3** — Resolve distributed-environment challenges in task
  synchronization, resource coordination, and data consistency — specifically
  exactly-once-effective processing under at-least-once delivery, and mutual
  exclusion during destructive operations (Section II-C), validated by chaos
  testing (Section III-B).

---

## II. Algorithms and Implementation

### A. System Architecture

The ecosystem comprises five decoupled component classes. No component holds
a direct reference to any other component's internal state; all interaction is
mediated by either HTTP or AMQP.

**Figure 1.** *[Insert the professional architecture diagram here. It must show:
the forwarder, the gateway, the RabbitMQ broker with both the `logs.ingest`
queue and the `control.purge` fanout exchange, N worker nodes, and the 3-node
Elasticsearch cluster with shard/replica placement. Annotate each edge with its
protocol (HTTP or AMQP) and indicate which edges are asynchronous.]*

**1) The Forwarder (CLI Client).** A stateless client (`forwarder/forwarder.py`)
that reads a local syslog file, splits it into lines, and transmits them to the
gateway over HTTP in batches of 1,000. It transmits the raw payload only — it
performs no parsing — so that parsing cost is borne by the horizontally
scalable tier rather than the client. The forwarder generates a UUID4 `job_id`
for each ingestion and assigns every line a monotonic sequence number `seq`
computed from its absolute offset in the file. This `(job_id, seq)` pair is the
foundation of the system's deduplication guarantee (Section II-C).

**2) The API Gateway / Search Head.** An asynchronous FastAPI service
(`gateway/main.py`) exposing three operations: `POST /ingest`, `GET /query`,
and `POST /purge`. On ingestion the gateway performs no parsing and no
storage write; it wraps each line in a JSON envelope and publishes it to the
broker, returning to the client immediately. On query it translates the
requested search into Elasticsearch Query DSL and returns aggregated results.
The gateway holds no per-job state, so gateway replicas may be added freely.

**3) Distributed Worker Nodes.** Identical, stateless containers
(`worker/main.py`) that competitively consume from a shared queue. Each worker
parses lines with a regular expression (`common/parser.py`) into the five
required components and writes the resulting document to Elasticsearch. The
extraction pattern accommodates the RFC 3164 BSD syslog shape, with an optional
priority prefix and an optional process identifier:

```
^(?:<(?P<pri>\d{1,3})>)?
 (?P<timestamp>[A-Z][a-z]{2}\s+\d{1,2}\s+\d{2}:\d{2}:\d{2})\s+
 (?P<hostname>\S+)\s+
 (?P<process>[^\[:\s]+)(?:\[(?P<pid>\d+)\])?:\s*
 (?P<message>.*)$
```

Severity requires special handling. RFC 3164 encodes facility and severity in
a `<PRI>` prefix, from which severity is recovered as `PRI mod 8` and mapped to
the standard eight-level scale (EMERG…DEBUG). However, syslog daemons strip
this prefix before writing to disk, so files taken from `/var/log` carry no
explicit severity field. When no prefix is present the parser infers severity
from severity-indicating keywords in the message body, defaulting to `INFO`.
This limitation is inherent to the on-disk format rather than to the
implementation. Lines that fail the pattern entirely are not discarded; they
are indexed with placeholder host/process values so that ingestion is lossless
by construction.

**4) Decoupled & Distributed Data Layer.** A three-node Elasticsearch cluster
forms the storage tier. The `logs` index is configured with **3 primary shards
and 1 replica per shard**, so the six resulting shards distribute across the
three nodes and every document exists on two distinct nodes. This satisfies the
prohibition on a centralized database instance: there is no single node whose
loss makes data unavailable, and query load is spread across all nodes. Field
mappings are chosen for query semantics — `hostname`, `process`, and `severity`
are `keyword` (exact-match term queries), `message` and `raw` are analyzed
`text` (phrase search), and `timestamp` is a true `date` type (range queries).

**5) Scatter-Gather Search.** A query arriving at the gateway is issued against
the cluster, whose receiving node acts as coordinator: it fans the query out to
every relevant shard, each shard executes locally, and the coordinator merges
and ranks the partial result sets before returning them. The gateway then
formats the merged result for the client. `COUNT_KEYWORD` uses the `_count`
API, so per-shard counts are aggregated cluster-side and only a scalar crosses
the network.

**Command mapping.** The eight required CLI operations map to storage
operations as follows:

| CLI command | Query construct | Target field |
|---|---|---|
| `SEARCH_DATE "<date>"` | `range` over a computed 24-hour window | `timestamp` |
| `SEARCH_HOST <host>` | `term` | `hostname` |
| `SEARCH_DAEMON <daemon>` | `term` | `process` |
| `SEARCH_SEVERITY <level>` | `term` (upper-cased) | `severity` |
| `SEARCH_KEYWORD <kw>` | `match_phrase` | `message` |
| `COUNT_KEYWORD <kw>` | `match_phrase` via `_count` | `message` |
| `INGEST <file> <gw>` | batched publish | `logs.ingest` queue |
| `PURGE <gw>` | locked `delete_by_query` | entire index |

Because RFC 3164 timestamps omit the year, `SEARCH_DATE` resolves the year
against the ingestion reference time, rolling back one year if the resulting
instant would lie in the future.

### B. Inter-Process Communication (IPC)

**Selection of middleware.** Three IPC models were considered. Synchronous
REST from gateway to worker would have required the gateway to know worker
addresses, to load-balance explicitly, and — critically — to block while a
worker parsed, coupling client latency to worker throughput. RPC has the same
temporal coupling. A message queue was selected because it inverts the
relationship: workers pull work when they have capacity, so the system
self-balances, and the gateway is decoupled from worker existence entirely.
**RabbitMQ** was chosen as the broker.

**Topology.** Two AMQP structures are declared:

- `logs.ingest` — a durable work queue. The gateway publishes to the default
  exchange with the queue name as routing key; workers consume competitively.
- `control.purge` — a durable **fanout** exchange. Each worker binds its own
  exclusive, auto-deleting queue to it, so a single published control message
  is delivered to *every* worker. This is the broadcast channel used for purge
  coordination (Section II-C).

The two structures deliberately use different exchange semantics: ingestion is
*competitive* (exactly one worker must handle each line), whereas control is
*broadcast* (every worker must observe each signal).

**Workload distribution.** Distribution is achieved by competing consumers plus
a per-worker prefetch limit (`basic_qos(prefetch_count=20)`). Without a
prefetch limit RabbitMQ would round-robin messages to workers regardless of
their progress, allowing a slow worker to accumulate a long private backlog
while a fast worker idles. Bounding unacknowledged messages per worker converts
distribution from round-robin to effectively least-loaded, and simultaneously
bounds the redelivery blast radius when a worker is killed (at most 20 messages
must be reprocessed).

**Minimizing synchronous blocking.** The gateway uses `aio-pika` over asyncio
and the asynchronous Elasticsearch client, so publishing and searching yield
the event loop rather than occupying a thread. The ingestion request path
therefore never waits on a worker, and a slow storage query cannot stall
concurrent ingestion requests. Workers, by contrast, use the synchronous `pika`
client: a worker is a dedicated single-purpose process, so blocking within its
own consume loop costs nothing that parallelism across containers does not
already recover.

**Network handling.** All inter-container traffic traverses a user-defined
Docker bridge network, with services addressed by name through Docker's
embedded DNS — no hard-coded IP addresses appear anywhere in the system. Only
the gateway (8000) and the broker's management UI (15672) are published to the
host. The client-facing `<Gateway_IP>` argument accepts a bare host, a
`host:port` pair, or a full URL, defaulting to port 8000.

### C. State Management and Consistency

This section addresses the two hardest guarantees the specification demands:
zero data loss with zero duplicate processing, and safe destructive operations
under concurrency.

**1) Durability of in-flight work.** Three settings together ensure a message
survives every failure short of total disk loss: the queue is declared
`durable`, messages are published with `delivery_mode=PERSISTENT`, and the
broker's data directory is a named Docker volume. A broker restart therefore
recovers the queue and its contents.

**2) Acknowledgement discipline.** Workers consume with manual acknowledgement
and acknowledge **only after** the Elasticsearch write returns successfully:

```
receive message → parse → index into ES → basic_ack
                                ↓ on exception
                          basic_nack(requeue=True)
```

If a worker is killed between receiving and acknowledging, the broker observes
the dropped connection and redelivers the unacknowledged message to another
consumer. No line can be lost by a worker crash, because no line is
acknowledged until it is durably stored.

**3) Idempotency — resolving at-least-once delivery.** The acknowledgement
discipline above guarantees *at-least-once* delivery, which by itself permits
duplicates: a worker may successfully write to Elasticsearch and then die
before its acknowledgement is transmitted, causing redelivery of already-stored
work. The system resolves this at the storage layer rather than by attempting
distributed transactions. Every document is written with a **deterministic
document identifier**, `{job_id}:{seq}`, derived from the ingestion job and the
line's absolute file offset. Elasticsearch's index operation is an upsert on
document ID, so a redelivered message overwrites its own prior document
byte-for-byte instead of creating a second copy. The composition is
*at-least-once delivery + idempotent write = effectively-once processing*,
achieved without consensus, distributed transactions, or deduplication state.

Note that content hashing would be an incorrect identifier choice here:
legitimately repeated log lines (for example, three identical failed-password
entries) are distinct events and must remain distinct documents. Positional
identity distinguishes them; content identity would silently collapse them.

**4) Distributed mutual exclusion for PURGE.** Deleting all indexed entries
while workers may concurrently be writing is a classic reader-writer hazard.
Coordination uses two mechanisms in combination, both provided by the broker
already present in the system — no additional coordination service (ZooKeeper,
etcd, Redis) is introduced.

*Mutex.* RabbitMQ guarantees that an **exclusive queue** is owned by exactly
one connection, and that a competing declaration of the same queue name from a
different connection fails with `RESOURCE_LOCKED`. The gateway therefore
acquires the lock by opening a dedicated connection and declaring the exclusive
queue `purge.lock`. Success means the lock is held; failure means another purge
is in flight and the request is rejected with HTTP 409. Because ownership is
bound to the connection, a gateway crash mid-purge releases the lock
automatically when the broker reaps the connection — the lock cannot be
orphaned. A deliberately non-robust (non-auto-reconnecting) connection is used
for this purpose, so that a network partition releases the lock rather than
silently reasserting it.

*Reader admission control.* The mutex alone only excludes *other purges*. The
specification requires that no node be reading or writing the shards during the
clear, so purge is modelled as the **writer** of a reader-writer lock in which
ingestion and query are the readers. On acquiring the mutex the gateway raises
a purge flag; every subsequent `INGEST` or `QUERY` is refused with HTTP 409
rather than queued, and the purge then waits (bounded by
`READER_DRAIN_TIMEOUT`) for requests admitted before the flag was raised to
complete. Readers consult the flag through `purge_in_progress()`, which checks
local state first and otherwise performs a *passive* declaration of the lock
queue — a purge started by a different gateway replica is therefore still
observed, because the lock resides in the broker rather than in any single
process. An ambiguous broker error during the check is interpreted as "locked",
so uncertainty is resolved in favour of consistency; the client simply retries.

*Barrier.* Holding the mutex, the gateway broadcasts `pause` on the
`control.purge` fanout exchange. Each worker, on receipt, cancels its ingest
consumer. Because a worker's AMQP event loop is single-threaded, the control
message can only be dispatched *between* ingest callbacks — never in the middle
of one — so a paused worker is guaranteed not to be mid-write; the grace period
(`PURGE_PAUSE_GRACE_SECONDS`, default 3s) covers broadcast propagation rather
than write completion.

*Queue discard.* Pausing the workers stops writes but does not stop the *cause*
of writes: lines already queued remain queued. An early implementation cleared
the index and then resumed the workers, which promptly drained the backlog and
re-populated the index — the purge silently undid itself, and a purge issued
during a 10,000-line ingestion left thousands of documents behind. `PURGE`
therefore also purges the `logs.ingest` queue, discarding pending lines, and
reports the count of discarded lines alongside the count of deleted documents.
This is the coherent reading of "deletes all indexed log entries": work already
admitted for indexing is part of the state being cleared.

The full sequence is:

```
acquire purge.lock (exclusive queue) ─── fails ──> HTTP 409, abort
        │ acquired
        ├─> raise purge flag        ──> new INGEST/QUERY refused with 409
        ├─> drain in-flight readers ──> bounded wait
        ├─> broadcast "pause"       ──> all workers cancel ingest consumers
        ├─> wait grace period       ──> broadcast propagates
        ├─> queue.purge()           ──> discard pending queued lines
        ├─> delete_by_query(match_all, refresh)
        ├─> broadcast "resume"      ──> all workers resume consuming
        └─> close lock connection   ──> releases mutex; flag lowered
```

Exclusivity is therefore enforced at three scopes: against other purges by the
broker-held mutex, against clients by reader admission control, and against
workers by the pause barrier and queue discard.

**5) Data-layer consistency.** Within Elasticsearch, replica shards are kept
consistent by primary-backup replication: a write is routed to the primary
shard, which applies it and forwards it to its replica before acknowledging.
Reads may be served by either copy. The `refresh=true` flag on purge forces
the operation to be visible to subsequent searches immediately rather than at
the next periodic refresh, so a `PURGE` followed instantly by a `QUERY` cannot
observe stale results.

**6) Autonomous recovery.** Recovery from broker loss must be handled
independently on both sides of the queue, because the two sides fail
differently.

*Consumer side.* Workers wrap their entire connection lifecycle in a retry loop
with a fixed backoff, so broker unavailability produces reconnection rather
than termination. Containers additionally carry `restart: unless-stopped`, so a
killed process is restarted by the Docker daemon and rejoins the consumer pool
with no operator action.

*Producer side.* A broker restart force-closes the gateway's AMQP connection.
Because the gateway is a long-lived service that caches its channel, a naive
implementation continues publishing to a dead channel and fails every
subsequent request until manually restarted — an availability bug that
survives the broker's own recovery. The gateway therefore treats its cached
connection as disposable: before each publish it validates the connection and
channel, rebuilds them (re-declaring the queue and control exchange) if either
is closed, and retries the publish with backoff, returning HTTP 503 only after
repeated failure. Publish retry is safe precisely because of the idempotency
property of Section II-C-3 — a line republished after an ambiguous failure
carries the same `(job_id, seq)` and therefore the same document identifier,
so a duplicated publish collapses into an overwrite. The forwarder applies the
same reasoning one level up, retrying an entire failed batch rather than
aborting the ingestion job.

Recovery is therefore autonomous at three independent levels: process restart
by the container runtime, connection retry within each component, and request
retry by the client.

### D. Deployment and Containerization

The entire ecosystem is described by a single manifest,
`docker-compose.yml`, satisfying the one-click deployment requirement:

```
docker compose up -d --build --scale worker=3
```

**Unified manifest logic.** Seven services are declared: the RabbitMQ broker,
three Elasticsearch nodes, the gateway, the worker pool, and the forwarder.
Several manifest techniques carry design intent:

- *YAML anchors* (`x-es-common`) factor the shared Elasticsearch
  configuration into one block merged into all three node definitions, so
  cluster-wide settings cannot drift between nodes. Only `node.name` and the
  data volume differ per node.
- *Health checks and conditional dependencies* — `depends_on` with
  `condition: service_healthy` prevents application containers from starting
  against a broker or cluster that is still initializing, eliminating a class
  of startup race. The broker is probed with `rabbitmq-diagnostics ping`; each
  storage node is probed against its cluster-health endpoint.
- *Scalability by construction* — the worker service declares no
  `container_name` and publishes no host ports, which is precisely what allows
  `--scale worker=N` to instantiate arbitrarily many replicas without
  collision. Scaling requires no code or configuration change.
- *Named volumes* for the broker and each storage node keep state independent
  of container lifetime, so `docker compose restart` does not discard data.
- *Profiles* — the forwarder is placed in the `tools` profile so that it does
  not run as a daemon, and is instead invoked on demand via
  `docker compose run --rm forwarder …`.
- *Build context* — the gateway and worker Dockerfiles take the repository
  root as build context so both images can copy the shared `common/` package,
  keeping the parser and broker topology defined in exactly one place.

Images are built from `python:3.11-slim` with dependencies installed before
source is copied, so that dependency layers remain cached across code changes.

**Host portability.** Elasticsearch imposes host-level requirements that are
not always grantable. Memory locking (`bootstrap.memory_lock`) requires
`RLIMIT_MEMLOCK=unlimited`, which rootless Docker and container-based hosts
refuse; the manifest therefore leaves it disabled by default. Hosts whose
`vm.max_map_count` is below 262144 require either a sysctl adjustment or the
`node.store.allow_mmap: false` fallback documented in the manifest.

---

## III. Test and Result

> **All quantitative results in this section must be replaced with figures
> measured from an actual deployment.** The procedures below are the tests to
> run; the tables are the shapes to fill.

**Test environment.** *[FILL IN: host OS, CPU cores, RAM, Docker version,
worker replica count, Elasticsearch heap per node.]*

**Dataset.** *[FILL IN: source and size of syslog corpus — line count, byte
size, distinct hosts, distinct daemons. A bundled 26-line sample is provided at
`sample_logs/sample.syslog` for functional testing; performance testing
requires a substantially larger corpus, e.g. 100k–1M lines.]*

### A. Core Functionality and Performance

**1) Parsing correctness.** Verify that every line is parsed into the five
required components and that no line is dropped. Compare the ingested line
count against `wc -l` of the source file.

| Metric | Expected | Observed |
|---|---|---|
| Lines in source file | — | *[FILL IN]* |
| Documents indexed | = source lines | *[FILL IN]* |
| Lines matching RFC 3164 pattern | — | *[FILL IN]* |
| Lines requiring fallback record | — | *[FILL IN]* |

**2) Distributed query correctness.** Execute each required CLI command and
verify the returned result set against a ground-truth `grep`/`awk` computation
over the source file.

| Command | Argument | Expected matches | Returned | Latency (ms) |
|---|---|---|---|---|
| `SEARCH_DATE` | *[FILL IN]* | | | |
| `SEARCH_HOST` | *[FILL IN]* | | | |
| `SEARCH_DAEMON` | *[FILL IN]* | | | |
| `SEARCH_SEVERITY` | *[FILL IN]* | | | |
| `SEARCH_KEYWORD` | *[FILL IN]* | | | |
| `COUNT_KEYWORD` | *[FILL IN]* | | | |

**3) Ingestion throughput and horizontal scaling.** Measure end-to-end
ingestion time — from forwarder invocation to the point where the queue depth
returns to zero — while varying worker replica count. This is the central
demonstration of workload distribution: throughput should increase with worker
count until a downstream resource saturates.

| Workers | Lines ingested | Wall-clock time (s) | Throughput (lines/s) | Speedup |
|---|---|---|---|---|
| 1 | | | | 1.00× |
| 2 | | | | |
| 3 | | | | |
| 5 | | | | |

*[Plot throughput against worker count as Figure 2, and discuss where the curve
departs from linear and why — expected causes are Elasticsearch indexing cost
and per-message broker overhead rather than worker CPU.]*

**4) Client-observed ingestion latency.** Record the time for the forwarder's
`INGEST` call to return versus the time for all lines to become searchable.
The gap between these two figures quantifies the temporal decoupling achieved
by the message-queue design — the client is released long before processing
completes.

| Measurement | Value |
|---|---|
| Time for `INGEST` to return (queueing complete) | *[FILL IN]* |
| Time until all documents searchable | *[FILL IN]* |
| Decoupling ratio | *[FILL IN]* |

### B. Fault Tolerance and Chaos Testing

Each experiment below establishes a precondition, injects a failure, and then
verifies an invariant. The two invariants under test throughout are
**zero data loss** (final document count equals source line count) and **zero
duplicate processing** (no two documents share a `(job_id, seq)` pair, and the
count never exceeds the source line count).

**Experiment 1 — Worker killed mid-ingestion.**
Begin ingesting a large corpus; while the queue is draining, forcibly
terminate one worker with `SIGKILL` (not a graceful stop, so unacknowledged
messages are stranded):

```
docker compose run --rm forwarder INGEST /logs/large.syslog gateway:8000 &
docker compose kill -s SIGKILL <one worker container>
```

*Expected:* the broker returns that worker's unacknowledged messages to the
queue; surviving workers absorb them; Docker restarts the killed container,
which rejoins the pool. Final count equals source line count exactly.

| Measurement | Value |
|---|---|
| Source lines | *[FILL IN]* |
| Documents after recovery | *[FILL IN]* |
| Lost lines (must be 0) | *[FILL IN]* |
| Duplicate documents (must be 0) | *[FILL IN]* |
| Messages redelivered | *[FILL IN]* |
| Time to autonomous recovery | *[FILL IN]* |

**Experiment 2 — All workers killed simultaneously.**
Kill every worker while the queue is non-empty. *Expected:* messages remain
durably queued, the queue depth stops decreasing but does not drop, and
processing resumes exactly where it stopped when workers restart. This isolates
durability from redelivery.

| Measurement | Value |
|---|---|
| Queue depth at kill | *[FILL IN]* |
| Queue depth after kill (unchanged?) | *[FILL IN]* |
| Documents after full recovery | *[FILL IN]* |

**Experiment 3 — Broker restart with queued messages.**
Restart the RabbitMQ container while messages are queued, then issue a fresh
`INGEST` once the broker reports healthy. *Expected:* because the queue is
durable and messages persistent, the queue is restored from disk; workers
reconnect through their retry loop; and — critically — the gateway rebuilds its
own publisher connection on the next request, so ingestion succeeds again
**without restarting the gateway container**. The final check is the one that
matters: a system that recovers its consumers but not its producers is not
recovered.

| Measurement | Value |
|---|---|
| Queued messages before restart | *[FILL IN]* |
| Queued messages after restart | *[FILL IN]* |
| Worker reconnection time | *[FILL IN]* |
| `INGEST` succeeds post-restart without gateway restart | *[FILL IN — expect yes]* |
| Documents after post-restart ingest | *[FILL IN]* |

**Experiment 4 — Storage node failure.**
Stop one Elasticsearch node and immediately issue queries. *Expected:* because
every shard has a replica on another node, all data remains queryable; cluster
health transitions to `yellow` (replicas unassigned) but never `red` (primaries
lost). Result sets must be identical to those obtained with a full cluster.

| Measurement | Value |
|---|---|
| Cluster health after node loss | *[FILL IN — expect `yellow`]* |
| Query results vs. full cluster | *[FILL IN — expect identical]* |
| Health after node rejoins | *[FILL IN — expect `green`]* |

**Experiment 5 — Concurrent purge (mutual exclusion).**
Issue two `PURGE` commands simultaneously from separate clients.
*Expected:* exactly one acquires the `purge.lock` exclusive queue and
proceeds; the other is rejected with HTTP 409. This is the direct evidence of
working distributed mutual exclusion.

| Measurement | Value |
|---|---|
| Purges accepted (must be 1) | *[FILL IN]* |
| Purges rejected with 409 | *[FILL IN]* |

**Experiment 6 — Purge concurrent with ingestion.**
Begin a large ingestion and issue `PURGE` while the queue is still draining.
*Expected:* ingest requests arriving during the purge are refused with HTTP
409, workers pause, the pending queue is discarded, the index is cleared, and
the index stays empty afterwards. The decisive check is the count taken
*after* workers resume: an implementation that clears only the index leaves the
backlog to re-populate it, so a non-zero count here indicates the purge undid
itself.

| Measurement | Value |
|---|---|
| Documents immediately after purge (expect 0) | *[FILL IN]* |
| Queued lines discarded (reported by `PURGE`) | *[FILL IN]* |
| Ingest requests refused with 409 during purge | *[FILL IN]* |
| Documents 30s after purge, workers resumed (expect 0) | *[FILL IN]* |

---

## IV. Conclusion

*[FILL IN the quantitative claims below once results are collected.]*

This project delivered a fully decoupled log analytics ecosystem in which
ingestion, parsing, storage, and query occupy independent failure domains
connected exclusively by message-oriented middleware and HTTP. Parsing
throughput scales horizontally by adding worker containers, and the entire
ecosystem deploys from a single manifest.

The principal distributed-computing challenges resolved were:

1. **Exactly-once effect under at-least-once delivery.** Rather than pursuing
   distributed transactions across the broker and the storage cluster, the
   system composes acknowledge-after-write with deterministic, position-derived
   document identifiers. Redelivery becomes an idempotent overwrite. This is
   the system's most important design decision: it converts a hard consensus
   problem into a trivial one by choosing the right identity for a record.
2. **Distributed mutual exclusion without a coordination service.** Purge
   safety is obtained from RabbitMQ's exclusive-queue ownership semantics
   combined with a fanout barrier, avoiding the operational cost of adding
   ZooKeeper or etcd. Connection-bound ownership makes the lock
   crash-safe by construction rather than by lease timeout.
3. **Elimination of the centralized storage bottleneck.** Sharding with
   replication distributes both data and query load, and scatter-gather
   execution means query cost is shared across nodes rather than concentrated.

**Limitations and future improvements.**

- *Broker high availability.* RabbitMQ runs as a single instance. Although its
  durable queues survive restart, the broker remains a single point of
  failure for availability. A three-node RabbitMQ cluster with quorum queues
  would extend the system's failure-independence property to the middleware
  tier itself.
- *Purge barrier rigor.* The barrier relies on a fixed grace period rather
  than positive acknowledgement from every worker. A stronger design would
  have workers publish a pause-acknowledgement and require the gateway to
  collect acknowledgements from all registered workers before deleting, making
  the barrier correct rather than probabilistic under adverse scheduling.
- *Severity fidelity.* Keyword-inferred severity is a heuristic imposed by the
  absence of the field in on-disk syslog. Ingesting over the syslog network
  protocol, where the `<PRI>` prefix is intact, would yield exact severity.
- *Poison-message handling.* Failed messages are requeued indefinitely; a
  dead-letter exchange with a retry bound would prevent a persistently
  malformed message from cycling forever.
- *Back-pressure.* The gateway publishes without regard to queue depth. A
  depth-aware admission control path would prevent unbounded queue growth when
  ingestion durably outpaces parsing capacity.

---

## References

*[Verify every URL and access date before submission; IEEE style requires
access dates for online sources.]*

[1] A. S. Tanenbaum and M. van Steen, *Distributed Systems: Principles and
Paradigms*, 3rd ed. Boston, MA, USA: Pearson, 2017.

[2] M. Kleppmann, *Designing Data-Intensive Applications*. Sebastopol, CA,
USA: O'Reilly Media, 2017.

[3] Broadcom Inc., "RabbitMQ Documentation — Reliability Guide."
[Online]. Available: https://www.rabbitmq.com/docs/reliability
[Accessed: *[FILL IN]*].

[4] Broadcom Inc., "RabbitMQ Documentation — Consumer Acknowledgements and
Publisher Confirms." [Online]. Available:
https://www.rabbitmq.com/docs/confirms [Accessed: *[FILL IN]*].

[5] Elasticsearch B.V., "Elasticsearch Guide — Near Real-Time Search and
Distributed Search Execution." [Online]. Available:
https://www.elastic.co/guide/en/elasticsearch/reference/current/index.html
[Accessed: *[FILL IN]*].

[6] Docker Inc., "Docker Compose Specification." [Online]. Available:
https://docs.docker.com/reference/compose-file/ [Accessed: *[FILL IN]*].

[7] C. Lonvick, "The BSD Syslog Protocol," RFC 3164, Internet Engineering Task
Force, Aug. 2001. [Online]. Available: https://www.rfc-editor.org/rfc/rfc3164

[8] S. Ramírez, "FastAPI Documentation." [Online]. Available:
https://fastapi.tiangolo.com/ [Accessed: *[FILL IN]*].

[9] G. Hohpe and B. Woolf, *Enterprise Integration Patterns: Designing,
Building, and Deploying Messaging Solutions*. Boston, MA, USA:
Addison-Wesley, 2003.
