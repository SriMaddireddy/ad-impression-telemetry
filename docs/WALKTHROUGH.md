# Demo and interview walkthrough

1. Open the dashboard and identify whether it is standalone demo or Kafka/Flink mode.
2. Generate 5,000 events and wait for the watermark to close their windows.
3. Inject a burst. Inspect campaign-001's volume rule and the exact counts.
4. Change a threshold; explain why it applies when a window closes and why cache invalidation is bounded by TTL.
5. In Docker mode open the Flink UI on port 8091 and show completed checkpoints.
6. Run the smoke/load scripts and use their measured reports, with storage/runtime mode attached.

Be able to explain:

- Event time versus processing time, the 2-second lateness/completeness tradeoff, and heartbeat records.
- Why incremental aggregation is preferable to buffering every impression.
- Why Kafka producer idempotence does not deduplicate HTTP retries.
- Why a Redis upsert avoids duplicated projection rows but does not provide end-to-end exactly-once delivery.
- Which counters are global/shared and which are per ingestion node.
- Why two ingestion containers are useful but do not make a single Kafka broker highly available.
- What happens to records arriving after a window has closed.

Report the actual implementation dates and workload measurements. Add real future commits for improvements such as schema validation, deduplication, or replay tests.
