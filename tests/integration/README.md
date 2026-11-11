# Integration tests

See the [project README](../../README.md) for setup and architecture.

Run against a disposable MongoDB database and Redis >= 6.2:

```sh
MONGO_URI=mongodb://127.0.0.1:27028 \
MONGO_DATABASE=gate_test_pipeline_local \
REDIS_HOST=127.0.0.1 REDIS_PORT=6379 REDIS_PASSWORD='<test-redis-password>' \
npm run test:integration
```

The database name must start with `gate_test_`; its contents are dropped after the
suite. Use a fresh disposable database for each simultaneous run. Redis keys use a
random per-run prefix and cleanup deletes only those exact keys. No existing
application stream or consumer group is modified. An HTTP fixture serves a local
HLS playlist, segment and offline response; no external stream is contacted.

Coverage:

- Real XREADGROUP, health checks, queued writes, MongoDB unique upserts and XACK.
- Online and offline monitoring results; independent ACKs for custom groups.
- Actual MongoDB validation rejection, retained pending entry, successful retry.
- Injected ACK response failures before and after the real Redis XACK executes.
- Repeated delivery without duplicate MongoDB documents.
- SIGKILL of a child consumer after buffering, then replacement XAUTOCLAIM recovery.
- Shutdown waits for a deliberately paused active write to finish and ACK.

The ACK and slow-write cases use narrow fault injection around real database
operations. They do not simulate a full network outage. Recovery idle settings are
shortened only in the tests. In deployment, `pendingMinIdleMs` must exceed expected
health processing, queue backpressure and database latency. Custom callbacks must
be idempotent and persist durably before acknowledging their consumer group.

`npm test` runs the separate unit suite without database connections.

Retention coverage verifies the 7-day ingestion cutoff and the physical TTL index.
Startup creates the TTL or updates an older retention window in place. Reads apply
the cutoff independently of asynchronous TTL deletion. No TTL-monitor changes are
required for normal development.

DLQ integration tests use a separate uniquely named database derived from the
gate_test_ database setting. They cover atomic transfer failures, durable retry
metadata, partial writes, archive-before-trim and controlled replay. Their Redis
cleanup removes only test-owned streams/metadata and registry members.


Recurring-monitoring tests additionally verify competing scheduler instances,
queued/pending lease expiry, same-job crash recovery, a Lua fault after XADD,
shared playlist freshness, and JSON-enabled runtime probes through DB/SSE. Their
inventory, freshness and source keys use exact unique namespaces; the runtime
scenario uses a separate uniquely named MongoDB database. All HLS traffic stays
on the local fixture server.


Stream-management integration adds real MongoDB compare-and-set updates, unique
registration/recreation, DB-first POST while its Redis projection is unavailable,
version-fenced deletion, Redis projection rebuild, and global probe concurrency
through the enabled API runtime. These scenarios use dedicated uniquely named
MongoDB databases and exact Redis cleanup keys.

Configuration transfer and monitoring reset tests use uniquely named MongoDB
databases. The reset test wraps Redis commands with its own random test prefix;
it never runs the real installation-wide reset against existing monitoring keys.
These tests verify durable configuration round-trips and preservation of accounts,
sessions and settings during isolated reset scenarios.

The Compose stack uses MongoDB 9.0; report integration tests require MongoDB
features including `$median` and `$setWindowFields`. Use the supplied supported
MongoDB image rather than an older server lacking those operators.
