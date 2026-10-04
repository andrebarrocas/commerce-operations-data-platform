# ADR 0006 — Stable event identity and deterministic publication

- Status: Accepted.
- Date: 2026-10-04

## Context

Producer retries, consumer retries, backfills, and corrections have different meanings. Dropping every repeated `event_id` can silently hide conflicting data. Regenerating identifiers on retries inflates every downstream metric. A successful Spark job does not guarantee that all published products form a consistent release.

## Decision

An event receives a stable ID before transmission. Transport retries preserve its body and ID. A semantic digest uses deterministic canonical JSON and excludes only `ingestion_timestamp` and optional `trace_id`; producer, schema version, payload, event type, business time, and entity references remain part of identity comparison. The implemented digest uses UTF-8 and canonical object-key ordering; timestamp strings retain their contract-valid exact representation and numbers retain JSON value representation. A producer retry must preserve its timestamp spelling. No cross-language semantic timestamp normalization is claimed. Canonicalization changes require an explicit digest version.

| Observation | Outcome |
| --- | --- |
| New ID and valid body | Candidate canonical event |
| Same ID and same semantic digest | Duplicate occurrence; retain Bronze provenance, count once downstream |
| Same ID and different digest | Identity conflict; quarantine conflicting variants and block the affected release |
| New ID with `correction_of_event_id` | Explicit correction candidate, validated against its referenced predecessor |
| Same entity/version with different event IDs | Causality conflict unless covered by an explicit contract rule |

Deduplication is global over the selected source history and the previously published identity index, not just the current input file. If a late conflicting variant contradicts an already published event, emit a quality incident and block a replacement release until the conflict is resolved. Any incident-reviewed exclusion must be explicit, versioned policy; it is not the default. Bronze evidence is never overwritten. Unknown IDs are not treated as harmless duplicates solely because they resemble earlier payloads.

Per-aggregate `entity_version` establishes lifecycle order. `event_timestamp` establishes analytical windows. Conflicting entity versions fail quality; `event_id` can make audit output deterministic but cannot invent causal truth. Corrections use a new ID and a higher entity version and retain the prior chain; invalid target references, cycles, and cross-entity corrections are rejected. Resolve the correction chain before lifecycle reconstruction: a correction replaces its target at the original causal position, while its own higher version orders the audit trail. It does not append a historical transition to the end of the current lifecycle. The corrected business timestamp determines affected analytical windows. Preserve the root event version as `logical_slot_version`; reject cycles and corrections that change the entity, event type, or parent references. An unresolved correction target remains pending/blocked until the target is available.

Feature datasets also preserve knowledge time. Resolve only observations and corrections whose system-recorded `first_received_at` is at or before the feature cutoff. A later correction may restate an operational aggregate, but it must not silently rewrite what was knowable at a historical prediction time. Keep “as known then” and “restated with current knowledge” extracts distinguishable in their contracts and manifests.

## Run and commit identity

Derive a logical run key from ordered input object/checksum references, normalized bounds, `as_of`, code revision, contract hashes, and configuration hash. Record wall-clock timing separately. An exact rerun yields the same sorted logical rows and row/content digests; Parquet bytes and Spark task file names need not be identical across executions. This distinction prevents false determinism claims.

Use unique attempt directories beneath the logical run. Partition completion records contain validated output metadata. A retry reuses only complete outputs whose input and transformation hashes match. The publisher waits for all required partitions, runs cross-partition checks, and creates one release manifest. Missing or failed partitions prevent publication. A restart after commit observes the published run and does not publish/count it again. See ADR 0005 for the filesystem/S3 pointer mechanisms.

## Alternatives and consequences

Transport sequence numbers identify occurrences, not business events. A last-write-wins merge would make outputs depend on arrival ordering and hide producer defects. Exactly-once delivery is not claimed: the target property is one logical contribution per accepted event under repeated delivery. AWS documents producer/consumer duplication, reinforcing this application responsibility. [AWS duplicate-record guidance](https://docs.aws.amazon.com/streams/latest/dev/kinesis-record-processor-duplicates.html).

Tests must replay identical data in different arrival/file orders; duplicate across separate runs; introduce an identity conflict after an earlier publication; recover after each commit boundary; and compare logical product hashes. Rejected/retried/unique counters use distinct names so observability remains auditable under retries.
