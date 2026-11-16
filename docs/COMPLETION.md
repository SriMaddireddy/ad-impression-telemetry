# Local streaming lab completion audit

AI-assisted implementation and verification by ChatGPT Work, 2026-11-16 UTC.

## Demonstrated scope

HTTP ingestion publishes validated synthetic events through Kafka. Flink builds
five-second event-time windows with two-second out-of-order tolerance and
projects complete snapshots into Redis. PostgreSQL stores campaign configuration.
The dashboard and standalone Java demo have reproducible README launch commands.

The final audit adds explicit handling for JSON null on the raw Kafka input.
It now becomes an INVALID_EVENT dead letter. Previously deserialization returned
null and the parser dereferenced it, failing the task. A Java regression and the
actual Kafka poison-input check cover the correction.

Replay and retention verification now executes RedisWindowSink.UPSERT directly
from the checked-out source. Isolated test keys hold 2,000 old windows. A new
window is written twice. Both the hash and sorted set must retain 2,000 entries,
the oldest entry must disappear from both, the next-oldest must survive, and
the replayed snapshot and score must match. Cleanup removes the test keys even
when verification fails. Earlier CI used a simplified replay script, which did
not establish the real sink's eviction behavior.

## Evidence and acceptance

Ten stdlib Python checks pass in the cloud, including benchmark corruption,
source-script extraction and failure cleanup. Java/Maven verification was
attempted here but dependency resolution could not reach Maven Central.
[Audit CI run 43901080001](https://github.com/SriMaddireddy/ad-impression-telemetry/actions/runs/43901080001)
passed on commit `b1246424497fb41920939658bf051d0cd7c1a173` with Java 17 and the
full Docker pipeline. That workflow runs all Java tests, malformed and late dead letters,
completed-checkpoint checks, actual Redis Lua replay/retention, a one-million
event benchmark with exact count and money checks, and two ingestion instances.
Its telemetry-verification artifact records current measured results.

The prior benchmark-integrity commit caaaf061b440982597cf68c58fc068d6f7c7e497 passed
[full CI](https://github.com/SriMaddireddy/ad-impression-telemetry/actions/runs/43451778980).
Historical workload measurements and environment limits remain documented in
VERIFICATION.md. They are observations for isolated synthetic workloads.

## Reviewed operating limits

Configuration reads use cache-aside lookup. Updates commit PostgreSQL before
Redis invalidation. The existing API regression checks update invalidation in
demo mode. Cross-node invalidation failures and read/update races can expose
stale thresholds until TTL expiry. This is eventual configuration consistency.
Replayed historical windows can use newer anomaly thresholds.

Completed checkpoints establish checkpoint activity. They do not prove
whole-cluster failover. Task restarts can recover within a live JobManager.
A complete standalone JobManager restart requires explicit checkpoint/savepoint
restoration. Redis AOF, PostgreSQL, Kafka and checkpoint volumes must be retained.
The external Kafka/Redis sinks remain at least once.

HTTP retries can duplicate previously acknowledged events. Counts describe
synthetic signals, without user attribution or a global deduplication contract.
Redis retains the newest 2,000 completed windows. Large or long benchmarks must
fit that cap. The local unauthenticated stack must bind to loopback.

## Completion boundary

The audit commit passed hosted CI. This local synthetic lab meets its
bounded completion gate. Strict configuration consistency, global event-ID
deduplication, cluster-wide failover, production authentication and new features
are deferred. Continue with autonomous-crm-simulator for the backend review.
