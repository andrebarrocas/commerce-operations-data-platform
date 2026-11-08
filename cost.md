# Cost model

No AWS resources are provisioned by local development commands. No AWS bill, savings, price or production utilization is claimed. Dollar estimates must specify region, price retrieval date and input assumptions; unknown rates remain null.

## Measure units before applying rates

| Component | Driver | Capturable evidence |
|---|---|---|
| Kinesis | Capacity mode, shard-hours or on-demand ingest/read, retention | Producer bytes/records, retries, cloud usage |
| Firehose | Ingested/processed bytes and buffering | Delivered bytes, delivery errors and freshness |
| S3 | Byte-months, request classes, transitions, retrieval | Run manifest byte sizes; request billing requires AWS |
| Lambda | Requests and GB-seconds | Invocation and duration metrics |
| EMR Serverless | vCPU-, memory- and storage-hours | Job resource utilization and billed duration |
| Redshift | Capacity-time, storage and data transfer | Query/workgroup measurements; client latency alone is not cost |
| Glue | Catalog objects/requests and optional operations | Explicit tables, partitions, API requests |
| CloudWatch | Ingest/storage bytes, custom metrics and alarms | Bounded dimensions and log retention |

Local disk bytes and CPU/wall time are useful sizing observations, not conversions to cloud dollars. Full snapshot rewrite costs grow with retained history. An incremental format is warranted when measured rewrite cost or concurrency needs exceed the simpler design's limits; see the Iceberg ADR.

## Guardrails

Set resource maximum capacity, idle stop, short log retention, raw/snapshot lifecycle, and a budget alarm before cloud experiments. Use a small explicit generation count by default. Keep Redshift optional and start only for a planned benchmark. Infrastructure destruction and state changes require deliberate operator commands. Record start/end time and verify resources actually stopped. A budget alarm does not enforce a hard spending cap.

## Reproducible estimate

For each service: `estimated_cost = measured_units * region_specific_unit_price`. Save units, unit label, source, price date, currency and exclusions alongside the estimate. If any rate is unavailable return `unmeasured`; do not substitute zero. An estimator is not a billing reconciliation.

## Temporary AWS experiment — 2026-11-08

The supervised [cloud acceptance](docs/aws-verification.md) used one provisioned Kinesis shard, EMR without warm workers, and a private Redshift workgroup capped at 8 RPUs, with no NAT gateway. The successful EMR job reported 0.146 vCPU-hours and 0.328 GB-hours; the initial failed permission probe also consumed billable units and remains in the evidence. Local Python execution is not charged as EMR.

All 54 managed resources were destroyed after saving evidence; independent API checks and cleanup of AWS-created service-linked roles are retained in [the teardown report](benchmarks/aws-2026-11-08/cleanup.json). KMS is pending its configured deletion window, not active compute. AWS states scheduled-deletion customer keys have no key-storage charge unless deletion is cancelled: [KMS pricing](https://aws.amazon.com/kms/pricing/).

The current-day Cost Explorer request returned `DataUnavailableException`; the final experiment cost is **pending**, not zero. No savings or free-credit coverage is inferred. The account owner explicitly upgraded the Free plan to access the blocked services; resource teardown does not change the account's billing plan.
