# ADR 0002 — Streaming signals and one authoritative batch path

- Status: Accepted design; Lambda is enabled only in the streaming milestone.
- Date: 2026-10-04

## Context

A low-latency validator and a batch validator can produce conflicting truth if both own curated records. Validation before archival can also destroy the evidence needed to investigate malformed input. The project needs streaming behavior and reliable analytical correction without claiming that every data product is updated in real time.

## Decision

Use Kinesis → Firehose → S3 raw as the authoritative archival path. Firehose performs buffered delivery only: no schema conversion and no business filtering. A wire transport envelope contains the original business-event bytes as base64 plus a transport version and producer identity. Each wire envelope is a single line, and Firehose adds the record delimiter. This preserves malformed JSON, non-UTF-8 payload bytes, and embedded newlines inside the encoded body. Wire framing is distinct from the business event contract. Firehose otherwise concatenates incoming records without delimiters, which cannot safely preserve arbitrary event boundaries. [AWS Firehose delivery behavior](https://docs.aws.amazon.com/firehose/latest/dev/basic-deliver.html).

Spark parses the archived transport envelopes into Bronze Parquet rows, retaining the original body bytes, source object/record position, parse outcome, and receipt metadata. Bronze is lossless and may contain invalid business events. Silver contains contract-valid, canonicalized, deduplicated events and entity state. Gold contains contracted products, dimensional tables, and feature-ready snapshots. The batch validator writes the authoritative quarantine outcomes used in reconciliation.

A Lambda Kinesis consumer supplies prompt contract-rejection signals and an early quarantine copy. It uses the same versioned contract validator as batch. It does not publish Silver or Gold and does not declare final lifecycle validity: predecessor events can arrive later. Metrics distinguish processing attempts from final unique-event quality counts. Early quarantine and batch quarantine are correlated by transport identity/body digest rather than counted twice.

Malformed business input is a successful consumer outcome only after its quarantine copy is durable. A transient S3 failure returns a failed record; bounded retry, partial batch reporting, batch splitting, and a failure destination prevent poison records from blocking a shard indefinitely. A malformed transport envelope is preserved as raw evidence and rejected separately. Lambda stream processing is at least once, so repeated invocations must remain safe. [AWS Lambda/Kinesis semantics](https://docs.aws.amazon.com/lambda/latest/dg/with-kinesis.html).

## Time and state semantics

`event_timestamp` represents business occurrence; `ingestion_timestamp` represents the first receipt recorded by the producer/ingestion boundary defined in the contract. AWS approximate arrival, object delivery, and job processing timestamps remain distinct observable times. A synthetic scheduled arrival time must never be reported as measured AWS arrival latency.

Batch runs have explicit input manifests and an `as_of` cutoff. Late events stay admissible: they cause bounded reprocessing of affected entities and windows. A processing watermark indicates the examined arrival frontier, not proof that all business events have arrived. Completeness uses producer control totals in synthetic tests and reports unknown when no authoritative denominator exists.

## Consequences and acceptance evidence

Firehose buffering and batch cadence impose measurable freshness limits. The low-latency path only promises signals. The additional Lambda consumer is justified by those signals and consumer-failure exercises; it can be disabled for low-cost batch-only runs.

Acceptance tests must demonstrate raw preservation of malformed payloads, out-of-order lifecycle reconstruction, late corrections, duplicate-safe quarantine, transient write recovery, and agreement between streaming and batch contract decisions. A local iterable/fake Kinesis client exercises application logic only. It does not emulate shard ownership, AWS retry scheduling, resharding, or managed throughput.
