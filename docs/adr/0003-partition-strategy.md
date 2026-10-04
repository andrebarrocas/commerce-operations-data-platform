# ADR 0003 — Arrival partitions for raw evidence, event partitions for analysis

- Status: Accepted initial strategy; file sizing remains a measured tuning decision.
- Date: 2026-10-04

## Context

Events can arrive days after occurrence, and timestamps can be invalid. Partitioning raw data by parsed business attributes risks making malformed records undiscoverable. Partitioning every analytical table by high-cardinality identifiers creates tiny files and expensive metadata.

## Decision

| Dataset | Partition or physical organization | Reason |
| --- | --- | --- |
| Raw transport objects | UTC delivery year/month/day/hour | Discovery does not depend on valid business JSON |
| Bronze Parquet | `ingestion_date` within immutable run output | Receipt-oriented reconciliation and quarantine investigation |
| Silver canonical events | UTC `event_date` within immutable run output | Time-bounded lifecycle and operational analysis |
| Order and shipment current-state products | Complete run snapshot; no initial partition | Small reviewable datasets and simple key lookup |
| Facility/carrier/delivery aggregate products | UTC observation date where file volume warrants it | Date-range queries and bounded recomputation |
| Feature snapshots | UTC `as_of_date` | Explicit point-in-time features and reproducible training extracts |

Business country and region remain filterable columns. Do not partition by order, shipment, event ID, or every possible country/event-type combination. Start with a limited partition count; compact to fewer files after measuring actual bytes and scan patterns. A 128–256 MiB target may be explored at sufficient scale; it is a tuning hypothesis, not a guarantee or a reason to pad a small dataset.

Do not enable Firehose dynamic partitioning initially. Arrival-based prefixes avoid parsing dependency and per-key buffering behavior on the raw path. Dynamic partitioning groups records by evaluated prefixes; adopt it only if measured retrieval requirements justify that additional behavior. [AWS Firehose prefix documentation](https://docs.aws.amazon.com/firehose/latest/dev/dynamic-partitioning-s3bucketprefix.html).

## Corrections and pruning

The input manifest records immutable raw object identities and checksums. Selection by delivery interval discovers newly arrived input. Parsing then derives the impacted event dates and entity keys. Rebuild all affected entity histories and all dependent windows, including a rolling feature's lookback and forward impact interval. A late order event may change a shipment state in a different date partition; rewriting only the event's date is insufficient.

Timestamp-invalid rows remain in Bronze and quarantine. They never create absurd event-date directories. Future skew, oldest supported event date, permitted backfill interval, and maximum input bytes are explicit run policy values.

Readers consume only the published manifest, never a recursive glob over all run directories. Published manifests contain the complete logical file set for a product snapshot, so unchanged partitions can be referenced from a previous run without mixing uncommitted work. Obsolete files are garbage-collected only when no retained manifest references them.

## Trade-offs and verification

Daily analytical partitions trade fine-grained pruning for fewer small files. Full state snapshots are intentionally simple at portfolio scale but become costly at high entity counts. The benchmark records partition count, file count, byte size, skew, scanned files, Spark shuffle/spill, and runtime before recommending another layout.

Tests cover a late event crossing UTC date boundaries, malformed timestamps, empty intervals, rebuilding a historical partition without losing unaffected data, and feature-window impact propagation. Iceberg partition evolution becomes useful if frequent layout changes or multi-engine consumption exceed this explicit-manifest approach; see ADR 0005.
