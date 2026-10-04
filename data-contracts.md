# Contract specifications

The repository contains executable contract specifications for a synthetic public portfolio platform. They are design artifacts until the corresponding runtime milestone is tested. They do not claim observed production SLAs.

## Event contract

`contracts/event.schema.json` uses JSON Schema Draft 2020-12. Its root validates canonical accepted events. `#/$defs/wire_event` validates producer events and permits a null `ingestion_timestamp`; every other required field is still present. Consumers must enable the JSON Schema date-time format checker, because `format` is otherwise an annotation in common validators. Timestamps are UTC RFC3339 with `Z`, seconds, and optional one-to-six fractional digits. Naive timestamps and timezone offsets are rejected to keep normalization explicit.

| Field | Meaning |
| --- | --- |
| event_id | Immutable event identity, reused on retries. |
| event_type | One of the twelve supported operational event types. |
| event_timestamp | Business occurrence time; may precede trusted ingress substantially. |
| ingestion_timestamp | Null on wire or trusted platform arrival in accepted events. |
| schema_version | Exact supported contract version: 1.0.0 or 1.1.0. |
| producer_id | Synthetic producer identity, retained in semantic conflict detection. |
| entity_type, entity_id, entity_version | Aggregate identity and monotonic source sequence. |
| correction_of_event_id | Null for ordinary events; prior revision ID for a correction. |
| order_id | Required parent order for all supported events. |
| package_id, shipment_id, facility_id, carrier_id | Present and nullable when not applicable. Event-specific requirements are enforced. |
| country, region | Destination country and subdivision, independent of facility location. |
| payload | Event-specific closed object; unknown properties are rejected. |
| trace_id | Optional diagnostic addition available only in version 1.1.0. |

Version 1.0.0 rejects `trace_id`; version 1.1.0 permits it. Payloads are identical between these versions, so a 1.0 semantic consumer can use a deliberate adapter that drops 1.1 diagnostics after validating the declared version. A generic consumer must not silently accept unknown fields or future version numbers. Adding an optional field is an additive contract change only after reader support is deployed; changing meaning, required fields, types or entity identity requires a major version and migration. Existing raw bytes remain replayable with their original schema version.

Supported payloads include order lines and money, cancellation reasons, reservation quantities, package contents and scans, dispatch package membership and original promise, delay reasons and ETA, attempt identifiers and outcomes, and initiated returns. Exact required properties, values, bounds and shapes live in the schema rather than duplicated prose. `ORDER_CREATED` lines use a generated SKU registry. Inventory event payloads identify one line each; package and return payloads contain line arrays.

All identifiers are synthetic strings with bounded length. Country/currency enumerations reflect the generator's documented initial support rather than pretending to cover the entire ISO registries. Region-to-country compatibility and currency minor-unit semantics come from generated reference data. The schema permits all non-applicable IDs as null but does not allow empty strings. JSON Schema cannot express `entity_id == order_id`, line-key uniqueness, money totals, lifecycle order, country/reference consistency, or parent existence; reusable semantic checks perform those validations.

## Raw, accepted and quarantined records

The transport frame and the event are separate contracts. The AWS frame base64-encodes original producer bytes so malformed JSON, non-UTF8 payloads and embedded newlines survive delivery. Opaque Firehose delivery does **not** preserve Kinesis shard/sequence/arrival metadata inside those bytes. Authoritative batch processing uses the immutable S3 object identity, record offset and persisted original object `LastModified` as a conservative first-available observation time. Copies/replays retain this original observation in the source manifest rather than adopting a new object's timestamp. The separate Lambda signal consumer can observe Kinesis stream/shard/sequence and `approximateArrivalTimestamp`; these earlier signal times do not silently become batch availability. Local JSONL fixtures retain file/offset and deterministic simulated ingress; arbitrary binary framing is tested at the AWS transport adapter boundary.

Raw delivery is not proof of semantic acceptance. The batch adapter stamps canonical ingress from the trusted S3 observation, not producer-supplied time. S3-delivery-to-publication latency and Kinesis-arrival-to-signal latency are different measures; Firehose delivery latency must not disappear from the platform freshness accounting. An end-to-end Kinesis-to-product measure requires an independently retained producer/stream acknowledgement observation and reconciliation.

Accepted events include the canonical contract plus sidecar metadata: `semantic_digest`, `first_received_at`, `validation_version`, source locator, and processing run identity. Sidecar fields are not inserted into the closed producer payload. Quarantined records retain original bytes/reference, reason code, violated rule IDs, available identity fields, first-seen time, schema/validator version, and retriable/permanent classification. Sensitive data is not present by design; quarantine still uses the same restricted storage policy as raw data.

Reason classes distinguish parsing/encoding failures, unsupported versions, structural violations, conflicting identities, invalid transitions, expired missing parents, and corrections requiring reconciliation. A quarantine record is never counted as an accepted product record. A pending record is not permanently rejected simply because its parent arrived later.

Identity, correction chain and lifecycle semantics are defined in `domain-model.md`. Semantic equality ignores transport arrival and diagnostics but includes schema version and producer identity. Replays preserve the first observed availability clock and recalculate processing clocks independently.

## Product contracts

`contracts/data-product.schema.json` validates every JSON file under `contracts/products/`. Each contract defines:

- name, business purpose, owner and version;
- lifecycle status (`design`, `local_verified`, or `aws_verified`), layer, grain and primary key;
- explicit SQL-friendly typed columns and nullability;
- freshness objective, evaluation window and measurement definition;
- completeness target, population denominator and allowed lateness;
- declarative quality rules with a typed parameter schema for each rule category;
- sources, transformation and downstream uses;
- usage restrictions, synthetic classification and retention policy;
- nullable measured cost metadata and an allocation method.

The owner is a fictional platform team with repository stewardship, not a real corporate organization. Type `timestamp` means UTC timestamp at microsecond precision; `date` means UTC date, `int64` is signed 64-bit integer, and nullable floating-point measures use null rather than NaN or infinity. Rates use fractions in `[0,1]`. SQL migrations must preserve the declared grain and nullability and cannot add columns merely to make a query convenient without a contract change.

| Product | Grain | Freshness target |
| --- | --- | --- |
| order_fulfillment | order_id | 15 minutes |
| shipment_lifecycle | shipment_id with dispatch evidence | 15 minutes |
| facility_performance | facility_id, UTC hourly window_start | 60 minutes |
| carrier_performance | carrier_id, destination country, UTC metric_date | 60 minutes |
| delivery_reliability | destination country, region, UTC metric_date | 60 minutes |
| inventory_movements | effective event_id, line_item_id | 15 minutes |

Freshness targets mean accepted-input arrival-to-publication lag, plus a scheduler heartbeat. They do not mean that an inactive customer's latest order must have a recent event time. Products are provisionally updated while a window is open and remain correctable after the 48-hour lateness allowance. A watermark is an operational expectation, not permission to discard later evidence. Targets are currently unmeasured, not commitments to real users.

Completeness is published expected-key coverage for an explicitly named effective accepted population at one committed input snapshot. For example, orders use distinct accepted `ORDER_CREATED` IDs, while inventory uses effective inventory event/line pairs. The target is 99.9% after the reconciliation allowance. Empty populations report `not_applicable`; they do not report 100%. The platform additionally measures raw expected event coverage from the synthetic generator manifest, durable disposition counts, quarantines and pending dependencies. These separate measures prevent a high accepted-population score from concealing rejected source records. Late/corrected data can revise both numerator and denominator; retain snapshot/run IDs with every evaluation.

## Quality rule registry

Contracts declare reusable `not_null`, `unique`, `accepted_values`, `numeric_range`, `timestamp_sanity`, `referential_integrity`, `duplicate_event`, `lifecycle_transition`, `freshness`, and `completeness` rule kinds. A parameter schema prevents arbitrary unvalidated executable expressions. Runtime rule implementations are centralized; transformations do not embed ad hoc validation branches. Null measures pass range checks unless an explicit not-null rule prohibits null. Referential checks support a grace interval; mandatory seed references have no grace.

Row-local violations quarantine the offending event. Referential and lifecycle checks first reconcile aggregate histories. Grain uniqueness, snapshot completeness or committed-publication failures fail the product publication atomically; prior committed products remain readable. Warning-only checks emit telemetry and never silently turn an error rule into acceptance. Rules execute against immutable input snapshots and write evidence including row counts, violation samples, duration and outcome.

Contract shape validation must be supplemented by checks that key/rule column names exist, column names and rule IDs are unique, reference arity matches, thresholds are valid, and product dependencies form an acyclic publication graph. Some Silver references are shared base datasets rather than the six public products. Event and product contract tests must exercise both accepted and rejected examples, unknown versions, 1.0/1.1 compatibility, impossible dates, missing required fields and unexpected properties.

## Change process

A contract change is reviewed together with positive/negative fixtures, compatibility classification, migration/replay impact, consumer notes, and updated lineage. The CI contract gate validates schemas and fixtures without accessing AWS. Published data pins contract version, code revision and source snapshot. The supported-version list and decoder registry must change together. No consumer discovers breaking changes by receiving a subtly different Parquet file.
