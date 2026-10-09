# Verification status

Local verification on October 8, 2026: all 15 Java tests passed. The standalone HTTP smoke check verified 933 events, two closed windows, one expected anomaly, and 100 cache reads without an additional database read. Client config-read p95 was 1.77 ms for this local synthetic check.

The isolated 10,000-event demo workload verified every event across 24 windows. Ingestion took 0.152 seconds and window completion took 7.377 seconds including watermark delay. See [benchmark-demo.json](benchmark-demo.json) and [smoke-demo.json](smoke-demo.json). These are standalone Java demo results and do not measure Kafka, Flink, Redis, production throughput, or an SLA.

Full-stack GitHub CI passed on October 8, 2026 (03:22 UTC October 9): [run 37878767298](https://github.com/SriMaddireddy/ad-impression-telemetry/actions/runs/37878767298), implementation commit `245c64cc7e2c0be94dde06aaf47b74b920a64312`.

- [Kafka smoke](smoke-kafka.json): 933 exact events, two windows, expected anomaly; 100 config reads with zero additional database reads. Client config-read p95: 5.03 ms.
- [Kafka/Flink checks](kafka-checks.json): invalid and late records reached the dead-letter topic; two completed checkpoints; replay preserved one Redis projection row per key.
- [One-million-event benchmark](benchmark-kafka.json): all 1,000,000 submitted events verified across 96 windows, 17.211 seconds of ingestion, 58,103.63 acknowledged events/second, and 22.768 seconds through final window completion. The shared Redis counters showed 24,175 hits in 24,201 configuration lookups (99.89% avoided reads for this synthetic workload).
- [Two-instance smoke](smoke-scaled.json): repeated exact-event/anomaly/cache checks with two ingestion containers behind Nginx; client config-read p95: 3.99 ms.

These measurements use a single-broker Docker lab on a GitHub Linux runner. They are workload-specific observations, not a production SLA or a failover test.

All events are synthetic. Five-second tumbling windows include two-second out-of-order tolerance. These checks do not establish production database savings or cluster-wide HA.
