# Production-Grade Commerce Operations Data Platform

Independent data engineering portfolio for fictional **Meridian Commerce**. All records, business rules, schemas and designs are synthetic. There is no employer affiliation, production usage, proprietary architecture or claimed performance improvement.

The working local platform turns versioned commerce events into immutable Parquet snapshots, six reusable data products, dimensional models and point-in-time feature datasets. It preserves malformed inputs, reconciles late history, detects identity conflicts, applies explicit corrections, and publishes through a single atomic manifest. AWS adapters, infrastructure and warehouse experiments are included; **AWS deployment and Redshift measurements have not been performed**.

## Run locally

Python 3.11 or 3.12; Linux or macOS. The baseline needs no AWS credentials, Docker or Java. Work from this repository directory because contract and SQL artifacts are intentionally part of the source checkout.

```bash
python3.11 -m venv .venv
source .venv/bin/activate
pip install -c requirements.lock -e '.[dev,aws]'
make check
commerce demo --orders 1000 --directory var/demo
```

The demo generates coherent orders, line items, reservations, packages, shipments, delivery attempts, cancellations and returns. It includes out-of-order and >48-hour-late arrivals, duplicate deliveries and malformed input. `var/demo/lake/head.json` identifies the committed release, its checksums, quality results and measured timings. Read artifacts from that manifest; globbing all runs would include failed attempts and old releases.

```bash
commerce generate --orders 5000 --seed 42 --faults --output var/events.jsonl
commerce ingest --input var/events.jsonl --lake var/lake \
  --as-of 2026-02-01T00:00:00Z --control var/events.control.json
# Repeating the same command verifies committed checksums and reuses the release.
commerce backfill --input var/events.jsonl --lake var/lake \
  --as-of 2026-02-01T00:00:00Z --start 2026-01-01 --end 2026-01-03
```

Backfill bounds are half-open UTC dates, limited to 31 days. They filter the three windowed operational products into a separate backfill head; complete entity snapshots, facts and features retain full-history context. A backfill cannot replace the global head. This is an explicit, bounded portfolio implementation, not a general incremental backfill orchestrator.

## What is implemented

| Increment | Working behavior | Verification |
|---|---|---|
| Phase 0 | Requirements, domain, contracts, seven ADRs and critical review | 24 examples across 12 types/two versions, negative cases, schema and semantic contract checks |
| Foundation | Seeded Python generator, static references, strict validation, configurable quality | Unit, contract, calendar, compatibility and business-rule tests |
| Local lake | Raw preservation; Bronze/Silver/Gold Parquet; correction history; quarantine; atomic commit | Duplicate, conflict, late-event, replay, checksum, backfill and failure-recovery tests |
| Products | Six exact product schemas; facts/dimensions; three feature outputs | Executed SQL, grain, quantity, FK, temporal availability and deterministic result tests |
| Spark | Actual PySpark projections of canonical Silver, joins, windows, partitioned writes | JVM integration test; separate measured Spark evidence |
| AWS assets | Kinesis retrying producer, Lambda quarantine signals, Firehose capture bridge, Terraform | Fake-client tests and configuration validation; no managed-service equivalence claim |
| Operations | CLI, benchmark harnesses, runbooks, metric definitions and CI | Real local measurements; cloud metrics explicitly unmeasured |

The product surface comprises `order_fulfillment`, `shipment_lifecycle`, `facility_performance`, `carrier_performance`, `delivery_reliability`, and `inventory_movements`. Each has a machine-readable owner role, schema, grain, quality rules, SLA targets, lineage, usage restrictions and cost metadata. Source completeness, transformation coverage and rejected-record reconciliation are separate measures. Targets are not achieved SLAs.

## Spark

Install Java 17 and set `JAVA_HOME`, then:

```bash
pip install -c requirements.lock -e '.[spark]'
make spark-test
python spark/curate.py --input /absolute/path/from/manifest/silver_events.parquet \
  --output var/spark-attempt-1 --metrics var/spark-attempt-1.json --master 'local[2]'
```

This job performs real Spark processing over a canonical Silver artifact and writes new serving projections. It does not independently implement the Python validation/reconciliation pipeline or publish an AWS catalog transaction. Local Spark and the configured EMR release differ; cloud acceptance must test that runtime.

## Evidence, design and operations

Start with [requirements](requirements.md), [architecture](architecture.md), [domain model](domain-model.md), [data contracts](data-contracts.md), [Phase 0 review](docs/phase-0-review.md), [ADRs](docs/adr/README.md) and [roadmap](roadmap.md). For the executed gates, see [verification](docs/verification.md). For operation, see [reliability](reliability.md), [measurements](benchmarks.md), [cost](cost.md), [warehouse](docs/warehouse.md) and [AWS deployment](docs/aws-deployment.md).

```bash
python scripts/benchmark_local.py --orders 5000 --trials 3 \
  --work var/benchmark-new --output benchmarks/my-local-run.json
```

Use a new work directory. Results record input hashes, scale, software/hardware, generation and processing throughput, measured quality time, replay/recovery durations and process peak memory. Fixture ingestion timestamps model late arrival; they are not observed network latency. Redshift scripts capture actual results only when explicitly connected to a configured warehouse.

## Boundaries that matter

* Local publication uses a process lock and filesystem rename on one host. S3 needs a separate cloud commit coordinator; concurrent cloud publication is not implemented.
* Local processing scans retained history and materializes a bounded snapshot in memory. It is deliberately easy to audit and is not claimed to handle unlimited retention.
* Plain Parquet avoids an unnecessary table-format dependency for the single-writer snapshot scope. Iceberg adoption criteria—concurrent writes, row-level mutations, transaction/time-travel needs and rewrite costs—are documented in an ADR.
* Late incomplete history is withheld and listed in semantic quarantine; contradictory evidence fails publication. Correcting an ID conflict requires an audited source disposition, which remains a manual incident action.
* Warehouse serving, cloud IAM behavior, fault recovery, cost and SLA compliance need real deployment evidence. No Rust producer or ML model is included; neither would compensate for missing core reliability.

CI runs Python tests, lint, typing, contracts, SQL/data assertions, a real Spark test and Terraform/OpenTofu validation. Deployment remains a deliberate, separately configured action with persistent remote state and an explicit budget.
