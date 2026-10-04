# Architecture decision records

These records describe independently designed portfolio decisions. “Accepted” means the design has been selected, not that the AWS deployment has run or its performance has been measured. Each implementation milestone must supply its own tests and evidence.

| ADR | Decision |
| --- | --- |
| [0001](0001-kinesis-for-event-ingestion.md) | Kinesis for retained event ingress |
| [0002](0002-streaming-and-batch-responsibilities.md) | Streaming signals and authoritative batch validation |
| [0003](0003-partition-strategy.md) | Arrival and event-time partition strategies |
| [0004](0004-redshift-products-and-dimensional-model.md) | Products, dimensional grains, and measured Redshift tuning |
| [0005](0005-parquet-first-and-iceberg-adoption-threshold.md) | Constrained Parquet snapshots and Iceberg adoption threshold |
| [0006](0006-idempotency-and-publication.md) | Event identity, deterministic reruns, and publication |
| [0007](0007-replay-backfill-and-correction.md) | Bounded replay, recovery, and historical correction |

Official service and format documentation was consulted on 2026-10-04. Pin runtime/provider versions during implementation and verify service-specific assumptions with AWS integration evidence before presenting them as exercised behavior.
