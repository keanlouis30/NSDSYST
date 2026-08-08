# Mini-Splunk: Distributed Log Analytics Ecosystem

A decoupled, containerized log analytics system: a CLI forwarder ships raw
syslog files to an API gateway, which hands them off asynchronously over
RabbitMQ to a pool of worker nodes; workers parse each line and index it into
a sharded, replicated Elasticsearch cluster; the gateway also serves search
queries by fanning them out to that cluster (scatter-gather) and returns
aggregated results.

## Architecture

```
                 ┌─────────────┐        HTTP        ┌─────────────┐
  syslog file -> │  Forwarder  │ ──────────────────> │   Gateway   │
                 │    (CLI)    │   INGEST / QUERY /   │ (FastAPI,   │
                 └─────────────┘        PURGE         │  async)     │
                                                       └──────┬──────┘
                                     publish (persistent)     │        query / delete_by_query
                                     ┌─────────────────────────┼─────────────────────────┐
                                     v                         │                         v
                            ┌─────────────────┐                │              ┌────────────────────┐
                            │    RabbitMQ      │                │              │  Elasticsearch      │
                            │ logs.ingest queue│                │              │  cluster (3 nodes,   │
                            │ control.purge     │<──────────────┘              │  sharded+replicated) │
                            │ fanout exchange   │  pause/resume broadcast      └──────────▲───────────┘
                            └────────┬──────────┘  during PURGE                            │
                                     │ consume (manual ack, prefetch)                       │
                     ┌───────────────┼───────────────┐                                     │
                     v               v               v                                     │
               ┌──────────┐   ┌──────────┐    ┌──────────┐                                 │
               │ Worker 1 │   │ Worker 2 │... │ Worker N │ ── parse (regex) + index ────────┘
               └──────────┘   └──────────┘    └──────────┘
```

### Components

1. **Forwarder** ([forwarder/forwarder.py](forwarder/forwarder.py)) -- CLI client. Reads a local
   syslog file and batches it over HTTP to the gateway; never talks to
   RabbitMQ or Elasticsearch directly. Also issues `QUERY` and `PURGE`
   commands.
2. **RabbitMQ** -- the IPC layer. A durable `logs.ingest` queue decouples
   ingestion from processing (the gateway returns as soon as messages are
   published, never blocking on worker availability). A `control.purge`
   fanout exchange broadcasts pause/resume signals to every worker during a
   purge. An exclusive queue (`purge.lock`) doubles as a distributed mutex.
3. **Workers** ([worker/main.py](worker/main.py)) -- any number of stateless, identical
   containers. Each parses syslog lines via regex ([common/parser.py](common/parser.py))
   into Timestamp/Hostname/Process/Severity/Message and indexes them into
   Elasticsearch.
4. **Elasticsearch cluster** ([docker-compose.yml](docker-compose.yml)) -- 3 nodes, index
   configured with 3 shards / 1 replica. This *is* the distributed,
   partitioned data layer the spec requires (no single centralized
   database); ES natively handles sharding, replication, and scatter-gather
   search across nodes.
5. **Gateway** ([gateway/main.py](gateway/main.py)) -- the API Gateway / Search Head. Publishes
   ingest messages asynchronously (aio-pika, non-blocking), and translates
   `QUERY`/`PURGE` commands into Elasticsearch requests.

### Fault tolerance / chaos-testing properties

- **Zero data loss**: RabbitMQ messages are persisted (`delivery_mode=2`,
  durable queue) and only ack'd by a worker *after* a successful ES write.
  If a worker is killed mid-message, RabbitMQ redelivers it to another
  worker once the connection times out.
- **Zero duplicate processing**: each ES document id is deterministic
  (`job_id:seq`, assigned by the forwarder/gateway pipeline), so redelivery
  after a crash overwrites the same document rather than creating a
  duplicate -- this gives effectively-once processing on top of
  RabbitMQ's at-least-once delivery.
- **Autonomous recovery**: workers run a reconnect loop with backoff around
  the RabbitMQ connection, and `restart: unless-stopped` in compose brings a
  killed container back up automatically. The ES cluster tolerates a node
  loss because of its replica shard.
- **Distributed coordination for PURGE**: the gateway acquires a RabbitMQ
  exclusive queue as a mutex (only one purge can run at a time), broadcasts
  `pause` on the fanout exchange so workers stop consuming, waits a grace
  period, clears the index, then broadcasts `resume`.

### Known simplifications (documented, not hidden)

- RabbitMQ runs as a single broker instance, not a cluster. The spec's "no
  centralized single point of failure" restriction is scoped to the *data
  layer* (worker `#4`), which explicitly is clustered; RabbitMQ persists to
  disk so a broker restart doesn't lose queued messages, but a broker-node
  cluster with quorum queues would be the natural next step for full broker
  HA.
- Severity is read from the RFC3164 `<PRI>` prefix when a line has one
  (true for raw network syslog); for plain on-disk log lines (which don't
  carry a PRI), severity is inferred from keywords in the message
  (ERROR/WARNING/CRITICAL/etc., default `INFO`) since the field genuinely
  isn't present in typical `/var/log` files.

## Running it

Requires Docker Desktop.

```bash
docker compose up -d --build
docker compose up -d --scale worker=3
```

Check cluster health:

```bash
curl http://localhost:8000/health
curl http://localhost:9200/_cluster/health?pretty   # from a shell inside the compose network, or publish the port temporarily
```

RabbitMQ management UI: http://localhost:15672 (guest/guest).

### Using the forwarder

Either run it locally with Python (`pip install -r forwarder/requirements.txt`)
or through compose (`docker compose run --rm forwarder ...`), which mounts
`./sample_logs` at `/logs`.

```bash
# ingest the bundled sample file
docker compose run --rm forwarder INGEST /logs/sample.syslog gateway:8000

# queries
docker compose run --rm forwarder QUERY gateway:8000 SEARCH_HOST webserver01
docker compose run --rm forwarder QUERY gateway:8000 SEARCH_DAEMON sshd
docker compose run --rm forwarder QUERY gateway:8000 SEARCH_SEVERITY ERR
docker compose run --rm forwarder QUERY gateway:8000 SEARCH_DATE "Jul 27"
docker compose run --rm forwarder QUERY gateway:8000 SEARCH_KEYWORD "Connection refused"
docker compose run --rm forwarder QUERY gateway:8000 COUNT_KEYWORD "Failed password"

# purge
docker compose run --rm forwarder PURGE gateway:8000
```

(From the host machine instead of another container, use `localhost:8000` as
the Gateway_IP since port 8000 is published.)

### Troubleshooting host restrictions

Elasticsearch is the fussiest part of the stack about host limits:

- **`error setting rlimit type 8: operation not permitted`** -- the host won't
  grant unlimited locked memory (`RLIMIT_MEMLOCK`). This compose file already
  avoids it: no `ulimits.memlock` block, and `bootstrap.memory_lock: "false"`.
  Only re-enable both together on a host where you control limits.
- **`max virtual memory areas vm.max_map_count [65530] is too low`** -- run
  `sudo sysctl -w vm.max_map_count=262144` on the host (add it to
  `/etc/sysctl.conf` to persist), or uncomment `node.store.allow_mmap: "false"`
  in [docker-compose.yml](docker-compose.yml) if you can't change host sysctls.
- **ES containers exiting on a small VM** -- three JVMs at 512 MB heap each
  need roughly 3-4 GB of RAM. Lower `ES_JAVA_OPTS` to `-Xms256m -Xmx256m`, or
  cut the cluster to two nodes (also drop the removed node from
  `discovery.seed_hosts` and `cluster.initial_master_nodes`).

### Chaos testing

```bash
# ingest, then immediately kill a worker mid-processing
docker compose run --rm forwarder INGEST /logs/sample.syslog gateway:8000 &
docker compose kill -s SIGKILL $(docker compose ps -q worker | head -n1)
docker compose up -d --scale worker=3   # bring replacement worker back
# then verify no lines were lost or duplicated:
docker compose run --rm forwarder QUERY gateway:8000 SEARCH_HOST webserver01
```
