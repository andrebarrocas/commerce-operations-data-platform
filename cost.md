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
