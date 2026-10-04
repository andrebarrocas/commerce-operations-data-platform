# ADR 0004 — Load curated products into Redshift and measure physical design

- Status: Accepted logical design; physical tuning choices require AWS measurements.
- Date: 2026-10-04

## Context

Reusable operational products and dimensional analytics serve different access patterns. Redshift is a query serving layer, not the source of truth for malformed events or replay. DuckDB is useful for local SQL/data tests but cannot establish Redshift distribution, execution plans, materialized-view refresh cost, or query latency.

## Decision

Publish Gold Parquet plus a committed manifest. A Redshift loader stages one identified product snapshot, validates keys and control totals, then applies transactional updates and records a load audit entry. Corrections must be represented in the affected-key set: a replacement snapshot can require deleting rows no longer present, not merely inserting/updating rows that remain. Every source key is deduplicated before `MERGE`; an ambiguous source fails before changing the target. Redshift `MERGE` is the implementation mechanism, with its engine-specific constraints covered by AWS tests. [AWS MERGE reference](https://docs.aws.amazon.com/redshift/latest/dg/r_MERGE.html).

Maintain operational product tables at their contract grains. Also build a dimensional layer with explicit grains in `domain-model.md`: order line, shipment, inventory movement, and delivery attempt facts where their source events support them. Small date, facility, carrier, product, and geography dimensions provide conformed lookup attributes. An order can have several packages and shipments; fact joins must use the appropriate bridge or first aggregate to a shared grain. Monetary measures remain currency-qualified and are never summed across currencies without an explicit synthetic FX model.

Use `DISTSTYLE AUTO`, `SORTKEY AUTO`, and `ENCODE AUTO` as the initial production-style baseline. Explicit distribution/sort candidates are controlled benchmark variants, not undocumented defaults. AWS recommends automatic distribution as a starting point; manual keys can trade redistribution against skew. [AWS distribution guidance](https://docs.aws.amazon.com/redshift/latest/dg/t_Distributing_data.html), [AWS CREATE TABLE reference](https://docs.aws.amazon.com/redshift/latest/dg/r_CREATE_TABLE_NEW.html).

## Benchmark decision protocol

Compare the AUTO baseline against a workload-motivated candidate: a high-cardinality join key such as `order_id` for order-centric large facts, and a timestamp-leading compound sort key for dominant date filters. Evaluate small dimension replication only when storage/load overhead is justified. Record the actual physical design because AUTO choices can evolve during an experiment. [AWS automatic optimization monitoring](https://docs.aws.amazon.com/redshift/latest/dg/c_ato-enabling-disabling-monitoring.html).

The reproducible script must record dataset seed and skew, rows/bytes, engine/workgroup settings, result-cache setting, concurrency, query text/hash, execution-plan output, query IDs, and multiple timed repetitions. Report warmup policy, median, p95, and sample count, plus redistribution, skew, scanned bytes, spill, and load/refresh time when available. Identify queue time separately from execution time. No comparison is valid if data, workload, or capacity changed unaccounted for.

A carrier/day materialized view is an experiment only after the corresponding aggregate query exists and repeats enough to justify refresh work. Its freshness is reported separately from base-table freshness. Benchmark refresh cost and validate corrected historical data against the base query.

## Consequences and validation

Redshift adds serving cost and an additional failure boundary; local mode remains meaningful without it. The load ledger and target mutation commit together. Retries of a previously committed run return its existing audit entry; incomplete transactions roll back. AWS tests cover duplicate source keys, repeated loads, late updates, deleted/replaced affected rows, rollback after a staged failure, and dimensional fanout. No local timings are labelled Redshift results.
