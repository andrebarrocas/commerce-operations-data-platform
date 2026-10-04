# Requirements and acceptance criteria

Production-Grade Commerce Operations Data Platform is an independent, synthetic portfolio project for fictional **Meridian Commerce**. No affiliation, customer usage, confidential design, internal schemas, or production performance is claimed.

## Scope and boundaries

Build a reproducible local implementation and AWS deployment assets. Separate a working implementation, validated configuration, and observed cloud behavior. A successful local test is not evidence of AWS availability, partition scaling, Kinesis ordering, or Redshift performance. No infrastructure is provisioned implicitly by local commands.

The design uses international orders, line items, facilities, stock reservations/releases, packages, shipments, carriers, cancellations, delivery failures, and returns. Financial amounts use integer minor units and a currency; amounts in different currencies are never summed without an explicit FX model. Personal data is unnecessary and excluded.

## Acceptance matrix

| ID | Requirement | Evidence required |
|---|---|---|
| R01 | Versioned event and product contracts | Machine-readable schemas, examples, compatibility tests |
| R02 | Deterministic synthetic source | Fixed seed/time, stable IDs, coherent relationships, repeatable output hash |
| R03 | Invalid input isolation | Raw preservation plus quarantine reason and source locator; malformed/schema tests |
| R04 | Duplicate, conflicting identity, late arrival | Distinguish exact retry from conflicting payload; tests in separate batches |
| R05 | Bronze/Silver/Gold Parquet | Actual readable Parquet; declared grains and lineage |
| R06 | Configurable quality | Not-null, uniqueness, accepted-values, ranges, timestamps, FK, lifecycle, freshness, completeness tests |
| R07 | Atomic publication and resume | Failed run cannot advance committed head; recovery and replay tests |
| R08 | Bounded historical correction | Explicit date range and maximum span; deterministic recomputation including entity history |
| R09 | Operational products | Six contracted products; dimensions/facts with explicit grains |
| R10 | Distributed processing | Runnable PySpark job, observed Spark runtime, runtime compatibility documented |
| R11 | Advanced SQL | Executed SQL with windows, CTEs, ranking, temporal joins, rolling calculations; separate Redshift MERGE |
| R12 | AWS ingestion and storage | Kinesis producer retries partial failures; Terraform with scoped roles, encryption, retention, logs |
| R13 | Analytics warehouse | Redshift DDL, load/upsert protocol, benchmark scripts and captured results only when actually run |
| R14 | Reliability and observability | Structured counters/timings/quality/SLO reports, CloudWatch mapping and alarms, recovery runbook |
| R15 | Point-in-time features | Feature cutoff and availability rules; leakage tests, source lineage |
| R16 | CI | Python tests/lint/types/contracts, SQL/data assertions, infrastructure formatting/validation |
| R17 | Cost and security | No secrets, environment configuration, capacity limits, measurable units and explicit unknown prices |
| R18 | Honest evidence | Hardware/software/input/command/result metadata; no fabricated cloud or local numbers |

## Target service levels (design targets, not measured service guarantees)

* Raw durability target: 99.9% of source-acknowledged events durably delivered within 5 minutes over a 30-day window. Requires reconciliation of producer acknowledgements and object inventory in AWS.
* Default curated freshness target (individual products may be stricter): 99% of eligible source records published within 30 minutes. Event-time age and ingestion-to-publication latency are reported separately; historical replays are a separate workload.
* Source completeness target: 99.5% of the producer-declared distinct event IDs accepted by the deadline. Rejected IDs are reconciled separately and count against product completeness. A quiet stream is not evidence of completeness. Unknown expected denominator yields `unknown`, never `pass`.
* Quality target: zero conflicting IDs and zero violated hard product constraints in committed outputs. Rejections remain visible and are excluded from completeness success unless the consumer contract explicitly defines accepted disposition.
* Recovery target: operators can replay a bounded range from retained immutable raw input; RTO is measured during experiments. No unmeasured recovery promise.

## Non-goals

No UI, notebooks as the execution system, model training, private data, universal exactly-once claim, always-on expensive compute, or Rust before core acceptance. Cross-region disaster recovery, multi-tenant IAM, production on-call coverage, and Iceberg concurrent writers require a later deployment decision. Rust is optional and can be omitted without reducing core correctness.

## Milestone gate

Every milestone must include executable behavior, meaningful tests and documentation. Phase 0 is the design gate and has schema/document validation rather than a data processing runtime. Subsequent phases cannot report completion without executing their checks. Cloud deployment/benchmark acceptance remains open until actual cloud evidence exists.
