# ADR 0007 — Replay immutable evidence through bounded replacement runs

- Status: Accepted.
- Date: 2026-10-04

## Context

Replaying events is needed to recover failed processing, validate a new transformation, incorporate late arrivals, and repair historical defects. Resetting a live consumer checkpoint cannot safely stand in for all of these operations. A backfill must not overwrite unrelated partitions or publish an incomplete replacement.

## Decision

Retain raw transport objects and Bronze provenance within an explicit configured retention period. Replay selects a closed input manifest, original checksums, a code/contract version, and an explicit `as_of`. Sources are never rediscovered halfway through a run. An input object that changes or disappears fails the replay before publication.

Support separate run intents:

| Intent | Inputs and behavior |
| --- | --- |
| Recovery | Same logical run and dependencies; resume valid completed partitions or recompute them |
| Exact replay | Same immutable input, contracts, code, bounds, and cutoff; compare logical results |
| Logic backfill | Old input with new code or contracts; produce a new candidate release with a comparison report |
| Late-data repair | New arrival manifests plus prior affected entity history; rebuild impacted products |
| Historical correction | New correction events referencing prior events; recompute the correction chain and downstream windows |

A request names UTC half-open interval `[start, end)`, event-time versus arrival-time selector, allowed products, maximum days, maximum objects/bytes, and a run identifier. The planner lists the impacted entities, partitions, and feature windows and rejects requests beyond configured limits. Bounds are real resource controls, not comments in a script.

The planner expands dependencies: a shipment's history can predate the requested interval, and a changed outcome can affect rolling features after it. Context rows can be read outside the output interval but must not silently expand the publication scope. Missing context fails a blocking quality check or produces explicitly provisional results, according to the product contract. Replacement manifests preserve all unaffected files from the parent release. Feature backfills enforce both business-time windows and knowledge-time availability: a correction received after a historical cutoff is excluded from that cutoff's point-in-time feature row. Restated operational reports can use it, with their current-knowledge semantics explicit.

## Initial local scope and promotion gate

The first local implementation writes bounded backfills into an isolated range/run namespace and validates them without replacing the current published head. This is a real bounded recomputation, with its source manifest and quality evidence preserved, but it is not a complete incremental partition replacement engine. Whole-input local reruns remain available for publishing a complete replacement snapshot.

Promotion of a bounded result into the current dataset is enabled only after implementation proves dependency expansion, preservation of unaffected files, pinned-parent conflict checks, and reader consistency with automated failure tests. The recovery/concurrency design below is the target for that later milestone, not a claim about the first local CLI.

## Recovery and concurrency

Normal ingestion may continue archiving raw data while a backfill runs. Curated publication remains single-writer. A backfill pins its base release and fails the compare-and-swap publication if another release has advanced it. It must replan against the current release; it may not discard newer data by overwriting the pointer. Failed attempts remain uncommitted and can be inspected, then garbage-collected under the retention policy.

Do not replay historical input into the live Kinesis stream by default: doing so mixes old event times with fresh operational lag and can trigger live effects. The replay path reads S3 directly. Kinesis retention supports a finite outage recovery window and is not the long-term archive. [AWS retention guidance](https://docs.aws.amazon.com/streams/latest/dev/kinesis-extended-retention.html).

## Acceptance evidence and limits

Automated cases cover an empty bounded interval, invalid bounds, a too-large request, a corrupted input checksum, cross-midnight late input, a corrected event affecting a 30-day feature, failure after one partition, resume after restart, and a stale-base publication conflict. Compare old/new row counts, unique keys, quality outcomes, changed rows, and source-to-product lineage. Measure recovery duration with a monotonic timer and retain the failed/recovered run IDs.

Retention expiry is a real replay limit. A run cannot claim historical reproducibility after required raw files, source manifests, contracts, or reference-data versions have been removed. Cost documentation must include retained versions and failed-attempt storage, not only the latest Parquet files.
