# Technical status

## Benchmark integrity milestone, 2026-11-13 UTC

AI-assisted implementation and verification by ChatGPT Work.

The synthetic load command verifies impressions, clicks, conversions, spend and
revenue for every expected window. Money checks reject nonfinite values and use
relative and absolute tolerances of 1e-9 for floating-point aggregation.
Reports include verifiedFields and retain event-only verifiedEvents totals.
Earlier reports checked counts only and do not retroactively establish money
verification.

Seven stdlib Python tests pass, including multi-batch command execution against
an HTTP response fixture, altered money, nonfinite totals, missing fields,
duplicate/missing count effects and accepted arithmetic roundoff. The fixture
does not exercise Java, Kafka, Flink or Redis.

The current cloud runtime has Java 21. Maven verify was attempted but dependency
resolution failed because repo.maven.apache.org could not resolve. Hosted CI
must verify Java 17, all Java tests, the real Kafka/Flink pipeline, one million
events and the scaled ingestion smoke before this milestone is accepted.

## Finite remaining completion scope

1. Verify this milestone's Java and full-stack CI, inspect measured reports and
   resolve any correctness failures in its demonstrated scope.
2. Audit replay, late-window handling, projection retention and configuration
   invalidation against the existing architecture and tests. Fix critical
   demonstrated defects and document operating limits.
3. Record reproducible demo, current measured evidence and completion decision,
   then review autonomous-crm-simulator for backend coverage.

Global event deduplication, replicated cluster operation and production security
are deferred beyond this local synthetic lab. New projects require a separate
repository-creation capability check.
