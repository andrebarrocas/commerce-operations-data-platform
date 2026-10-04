# ADR 0001 — Kinesis for retained event ingestion

- Status: Accepted for the AWS deployment design; deployment and scale claims require later evidence.
- Date: 2026-10-04
- Scope: Synthetic commerce event ingress.

## Context

The platform needs independently consuming applications, short-term replay, burst absorption, and a practical AWS streaming integration. The portfolio has no demonstrated requirement for operating Kafka brokers, cross-cloud portability, or a subsecond end-to-end analytical SLA. Local files do not prove managed-stream behavior.

## Decision

Use one Kinesis Data Stream for synthetic commerce events, with `order_id` as the producer partition key. Inventory events in this scope reference an order; a future order-independent stock feed requires its own documented key. The producer uses stable event identifiers, bounded exponential backoff with jitter, per-record acknowledgement handling, and retries only failed records where the response identifies them. A timeout can still require retrying an ambiguously acknowledged record.

Partition placement groups related records but does not replace business ordering. `PutRecords` partial failures and retry timing can reorder arrivals. Transformations use per-aggregate `entity_version` for lifecycle order and event time for business windows. Cross-aggregate times do not establish causality; conflicting entity versions remain an explicit quality failure. Kinesis duplicates can result from either producer or consumer retries, so consumers deduplicate on the embedded event identifier rather than transport sequence numbers. [AWS duplicate-record guidance](https://docs.aws.amazon.com/streams/latest/dev/kinesis-record-processor-duplicates.html), [AWS producer API guidance](https://docs.aws.amazon.com/streams/latest/dev/developing-producers-with-sdk.html).

Use a configurable provisioned-shard development profile with an intentionally bounded synthetic producer. A later measured burst workload can justify on-demand capacity. Do not assert a throughput limit or cost advantage without the selected region, mode, record sizes, and measurement. Record the stream ARN, partition key, shard/sequence metadata where available, first receipt time, and producer run identity separately from business attributes.

Set retention explicitly. Start with the service's 24-hour baseline for disposable exercises; require an outage/recovery budget before extending it. S3 raw retention is the historical replay boundary, not Kinesis. Retention beyond 24 hours incurs additional charges. [AWS retention documentation](https://docs.aws.amazon.com/streams/latest/dev/kinesis-extended-retention.html).

## Alternatives and consequences

| Alternative | When it would be preferable | Reason not selected here |
| --- | --- | --- |
| S3 file ingestion only | Daily exports with no streaming consumer | Does not demonstrate live consumption, consumer lag, or retained-stream replay |
| Managed Kafka | Existing Kafka interfaces, ecosystem, or portability requirements | Adds operational and configuration scope without a demonstrated requirement |
| Work queue | One worker owns each task and replay is not an event-log requirement | The design needs independently consuming applications and a retained log |
| Direct Firehose writes | Only durable buffered delivery is needed | Removes the retained stream that the low-latency validation consumer exercises |

The chosen partition key can become hot for very large orders. Monitor throttles and distribution skew; do not silently salt keys and imply preserved ordering. Revisit key strategy if measured contention occurs. Failure exercises must include partial `PutRecords` responses, ambiguous timeouts, throttles, duplicate arrivals, and consumer lag. AWS integration tests are distinct from local fake-client tests.
