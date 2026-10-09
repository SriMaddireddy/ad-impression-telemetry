# Verification status

Local verification on October 8, 2026: all 15 Java tests passed. The standalone HTTP smoke check verified 933 events, two closed windows, one expected anomaly, and 100 cache reads without an additional database read. Client config-read p95 was 1.77 ms for this local synthetic check.

The isolated 10,000-event demo workload verified every event across 24 windows. Ingestion took 0.152 seconds and window completion took 7.377 seconds including watermark delay. See [benchmark-demo.json](benchmark-demo.json) and [smoke-demo.json](smoke-demo.json). These are standalone Java demo results and do not measure Kafka, Flink, Redis, production throughput, or an SLA.

GitHub CI provisions the full Docker stack and checks exact window counts, anomaly detection, real Redis read avoidance, invalid/late Kafka dead letters, completed Flink checkpoints, a one-million-event workload, and two ingestion instances. Full-stack verification is pending the hosted run.

All events are synthetic. Five-second tumbling windows include two-second out-of-order tolerance. These checks do not establish production database savings or cluster-wide HA.
