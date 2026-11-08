# Supervised AWS acceptance — 2026-11-08

These are actual account-backed experiments in `eu-west-1`, not fake-client results or production usage. Credentials and private recovery inventories remain outside Git. Resources are temporary; evidence persists in this repository.

## Packaged Lambda and S3

Executed `scripts/verify_lambda_aws.py` with the Linux/x86_64 Python 3.11 ZIP built from repository sources. [Machine-readable results](../benchmarks/aws-2026-11-08/lambda-s3.json) include the artifact SHA256, five passed checks, cleanup results, and independent API absence checks.

- A valid event was acknowledged without quarantine.
- Malformed non-UTF-8 business bytes were retained in durable quarantine evidence.
- Repeated invocation reused the same logical quarantine key; S3 versioning can retain multiple physical versions.
- A second existing bucket outside the runtime role's exact write permission caused a partial-batch failure response and received no objects.
- Restoring the authorized bucket recovered the same record. The measured 13.912-second drill includes configuration propagation and invocation, not a general recovery SLO.

The function, scoped IAM role/policy, two versioned private buckets and log group were deleted. Separate `GetFunction`, `GetRole`, `HeadBucket` and log-group API checks confirmed absence. The fixture uses S3 SSE-S3; the platform's KMS-encrypted lake is a separate acceptance scope. Direct invocations use synthetic Kinesis-shaped envelopes: this test does **not** establish Kinesis delivery, event-source retries, Firehose, EMR, Redshift, or distributed publication.

## Account preflight

`scripts/aws_preflight.py` verifies the exact selected account before read-only service probes, suppresses private inventories in its output, and exits 2 if a service is blocked. The initial Free-plan account returned `SubscriptionRequiredException` for Kinesis, Firehose, EMR Serverless and Redshift. After the account owner upgraded the plan, all six probes succeeded. Both actual reports are retained under [the evidence directory](../benchmarks/aws-2026-11-08/). A successful probe is not deployment acceptance.

## Kinesis, Firehose and stream failure recovery

The isolated OpenTofu deployment sent 1,000 synthetic orders: **14,307 unique events, 10 deliberate duplicate attempts and four malformed payloads**. All 14,321 attempts arrived in three GZIP raw objects. The actual Kinesis Lambda consumer quarantined all four malformed records. The manually supervised capture bridge then reconciled the raw objects locally: complete expected population, zero pending events, ten duplicates, four rejects and an identical logical hash on exact replay. [Streaming results](../benchmarks/aws-2026-11-08/stream.json).

Preparing/sending/acknowledging the normal and duplicate workload took 41.675 seconds. All raw and quarantine controls reconciled 157.990 seconds after the experiment began. This includes producer work, Firehose buffering, polling and Lambda consumption; it is **not** per-event Kinesis latency or a throughput capacity benchmark. Local reconciliation took 11.697 seconds, including 1.947 seconds of quality checks. Historical business timestamps are synthetic; batch availability comes from actual S3 `LastModified`, not a reconstructed Kinesis receipt clock.

The live failure drill added a temporary explicit Deny on only the validator's quarantine writes. Lambda exhausted its three configured retries and retained the full failed batch in the separate S3 destination (`RetryAttemptsExhausted`) in 26.969 seconds. After restoring the exact original bucket policy, replaying the same record durably quarantined it in 9.025 seconds. [Failure/recovery results](../benchmarks/aws-2026-11-08/stream-recovery.json). Offline tests prove the original policy is restored when the producer fails during the drill; a host loss still requires operator recovery.

Cloud acceptance exposed a real configuration defect: the prefix-only failure-destination grant was rejected during event-source mapping creation. A dedicated encrypted private failure bucket now contains the bucket-wide write grant; the validator still cannot write raw/Silver/Gold lake prefixes. See [ADR 0008](adr/0008-isolated-failed-batch-destination.md). The test fixture also leaves Lambda concurrency unreserved because this new account initially had only ten total executions. The reusable module retains its default reservation of two.

The private Redshift acceptance fixture is in `infra/acceptance/`: three private subnets, no inbound security-group rules, no NAT/public IPs, and a bucket-scoped S3 gateway endpoint. Use a unique project prefix, the account guard and a reviewed plan. Its state must remain available until teardown and independent absence checks finish. This is supervised acceptance infrastructure, separate from the CI deployment workflow's persistent remote-state requirement.

## EMR Serverless and Spark parity

The actual `spark/curate.py` entry point ran on **EMR 7.12.0 / Spark 3.5.6-amzn-1**, reading the captured canonical Silver Parquet from KMS-encrypted S3 and writing three Parquet projections. All business-column values matched the independently computed local SQL products: 1,999 order lines, 1,099 shipment summaries and 62 facility-hour rows. The local Java/Spark integration test now runs the same comparison, including fractional-second durations and rolling windows.

The driver reported **17.225 seconds** inside the Spark transformation. AWS reported 70 execution seconds; submission through terminal job state took 126.866 seconds. These have different measurement boundaries and must not be presented as interchangeable. Actual billed-resource units were 0.146 vCPU-hours and 0.328 GB-hours for the successful job; they are not a final dollar invoice. [EMR results](../benchmarks/aws-2026-11-08/emr.json).

An [initial failed job](../benchmarks/aws-2026-11-08/emr-initial-failure.json) exposed missing `logs:DescribeLogGroups`. The runtime role now permits that read action only in the selected region; mutations remain scoped to the pre-created project log group. The retry passed without broadening raw/final-output deletion rights. This is cloud execution of projections over already reconciled Silver, not a distributed raw-to-Silver implementation or an S3/Glue release coordinator.

## Redshift correctness and benchmark

The repository DDL and atomic publish procedure executed on the private 8-RPU Serverless workgroup. All **1,099 shipment rows and every column value** matched the source Parquet, including microsecond timestamps and nulls. Checks passed for exact release replay, conflicting release hashes, wrong control totals, duplicate staging keys, affected-key tombstones, restoration of the original snapshot and two concurrent independent staging sessions publishing one release.

A nested atomic procedure deliberately raised an exception **after** the real procedure performed MERGE and inserted its ledger row. Subsequent queries proved that both target data and ledger rolled back. This is an executed transactional test, beyond SQL text inspection. Initial COPY engine time was 1.414 seconds; the first MERGE/procedure took 3.927 seconds. [Correctness results](../benchmarks/aws-2026-11-08/redshift.json).

The existing benchmark harness then ran AUTO, EVEN and KEY layouts over the identical 1,099-row population, with result caching disabled, randomized trial order, one warmup and three measured trials per query/layout. Carrier aggregations, order joins and time predicates returned identical results across layouts. EXPLAIN plans, encoding/table diagnostics and query history were captured; materialized-view results matched the base aggregation, and refresh engine time was 0.126 seconds. [Benchmark summary](../benchmarks/aws-2026-11-08/redshift-benchmark.json), [statement receipts and plans](../benchmarks/aws-2026-11-08/redshift-statements.json).

This small acceptance population establishes correctness and harness execution. It does **not** establish a production-scale distribution choice, a performance improvement, or a reliable tail-latency SLO. Client times include Data API/network/polling; engine durations are reported separately. AWS's negative duration sentinel is represented as unavailable, never as a negative query latency.

## Reproduction and cleanup

### Observability and verified teardown

CloudWatch received actual `CommercePlatform` EMF attempt metrics. The quarantine-write-failure alarm entered **ALARM** during the denial drill; other observed alarms were OK. There were no notification action ARNs configured, so this establishes alarm evaluation, not notification delivery. [CloudWatch evidence](../benchmarks/aws-2026-11-08/cloudwatch.json). Counts include repeated attempts and must not be interpreted as unique events or achieved freshness/completeness SLOs.

OpenTofu destroyed all **54 managed resources**, and its retained state lists zero resources. Independent API checks confirmed deletion of test buckets and versions, stream, Firehose, validator/mapping, warehouse/namespace/managed admin secret, catalog database, project roles/policy, logs/alarms and private test networking. AWS-created EMR and Redshift service-linked roles were also removed. EMR retains a `TERMINATED` application metadata tombstone, with no listed active application. The original account VPC was preserved. [Teardown evidence](../benchmarks/aws-2026-11-08/cleanup.json).

The customer-managed encryption key is **PendingDeletion**, scheduled for **2026-12-08**, following the configured 30-day safety window. AWS does not permit immediate KMS deletion; [AWS key-deletion documentation](https://docs.aws.amazon.com/kms/latest/developerguide/deleting-keys.html). Scheduled-deletion keys have no key-storage charge unless deletion is cancelled; [AWS KMS pricing](https://aws.amazon.com/kms/pricing/).

Cost Explorer returned `DataUnavailableException` for the current day. [Billing evidence](../benchmarks/aws-2026-11-08/billing.json) therefore remains pending, with no fabricated zero-dollar bill or final cost. Raw private API records, plans, state, identifiers and recovery inventories are not committed.

### Commands

Use browser-based `aws login` and temporary SDK credentials. No access key belongs in the repository. With Python AWS dependencies installed:

```sh
python scripts/aws_preflight.py --expected-account YOUR_12_DIGIT_ACCOUNT \
  --region eu-west-1 --output var/preflight.json
python scripts/package_lambda.py
python scripts/verify_lambda_aws.py --expected-account YOUR_12_DIGIT_ACCOUNT \
  --region eu-west-1 --directory var/lambda-acceptance --allow-create-and-destroy
```

The Lambda fixture creates UUID-named resources with synthetic data and cleans up in `finally`, including versioned objects. If a process is forcibly terminated or an API result is ambiguous, inspect its private `resources.json` inventory and verify every listed resource independently; a local process cannot guarantee cleanup after host loss. Offline tests exercise partial deployment failures and cleanup failures. This experiment does not upgrade account plans or purchase a subscription automatically.
