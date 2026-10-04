# ADR 0005 — Parquet snapshots first; adopt Iceberg when table semantics justify it

- Status: Accepted for the first implementation; explicitly conditional at the AWS scale milestone.
- Date: 2026-10-04

## Context

Historical corrections, deterministic reruns, and safe publication need more than a folder of Parquet files. They do not necessarily need concurrent table writers. The initial platform has a single publisher, bounded synthetic datasets, and controlled consumers that can resolve an explicit manifest. Adding a table format does not by itself implement event deduplication, valid lifecycle transitions, or cross-product transactions.

## Decision

Use Parquet files in immutable run directories and a versioned manifest describing each complete logical snapshot. A manifest includes file paths and checksums, row counts, contract/code/config hashes, the input manifest hash, `as_of`, quality outcomes, parent snapshot, and affected scope. A release manifest references the exact product manifests that belong together. Consumers pin the release ID before reading.

Publish only after all required outputs and blocking quality checks succeed. Locally a single publisher writes and fsyncs a temporary pointer and atomically replaces the pointer on the same filesystem, with an advisory process lock. On S3, immutable manifest creation uses `If-None-Match`; the current-release pointer uses `If-Match` against the previous ETag. A conflict fails publication and requires reconciliation, rather than a blind overwrite. S3 conditional writes protect one object; they are not multi-object or Glue/Redshift transactions. [AWS conditional-write semantics](https://docs.aws.amazon.com/AmazonS3/latest/userguide/conditional-writes.html).

A corrected run references replacement files for impacted partitions and prior immutable files for unaffected partitions. Old manifests remain available for replay comparison and audit until the configured retention policy expires. This provides application-level snapshot selection, not SQL-native time travel or general table ACID semantics. Generic engines must use a manifest-aware reader or a catalog table pinned to one immutable run location. They must never infer committed state by listing every Parquet file under a product prefix.

Glue catalogs immutable versioned schemas/locations for discoverability. Updating multiple catalog entries does not publish an atomic release. The release manifest is authoritative; a failed catalog update can be retried from it. Redshift ingestion has its own transaction and records the consumed release ID.

## Why Iceberg is deferred, not dismissed

Iceberg offers atomic snapshots, optimistic concurrent writes, schema and partition evolution, and time travel. Those are valuable table capabilities distinct from Parquet's file encoding. [Apache Iceberg documentation](https://iceberg.apache.org/docs/latest/).

Adopt Iceberg if any of these requirements become real:

- Multiple independent writers must update the same table safely.
- Frequent corrections make manual partition replacement and manifest planning too expensive.
- Consumers require native snapshot/time-travel queries across engines.
- Schema/partition evolution needs compatible access without custom manifest adapters.
- Incremental consumers need a standard snapshot/change interpretation.

Before adoption, pin compatible Spark/EMR, Iceberg, Glue, and reader versions; prove concurrent commits, schema migration, a late correction, snapshot expiration, orphan cleanup, and identical results from each supported reader. Compare operational cost and maintenance burden. Rewrite this ADR if Iceberg's standard metadata/commit machinery becomes simpler than the custom constrained publisher.

## Limits and required evidence

Plain Parquet is not claimed to provide ACID tables. A filesystem atomic rename test does not prove an S3 commit protocol. Crash injection must cover incomplete files, missing manifest, failed pointer update, publication conflict, and garbage collection of referenced snapshots. If the implementation cannot enforce manifest-only reads and one publisher, this decision is invalid and Iceberg should be adopted before broadening access.
