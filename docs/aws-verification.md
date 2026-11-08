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

## Reproduction and cleanup

Use browser-based `aws login` and temporary SDK credentials. No access key belongs in the repository. With Python AWS dependencies installed:

```sh
python scripts/aws_preflight.py --expected-account YOUR_12_DIGIT_ACCOUNT \
  --region eu-west-1 --output var/preflight.json
python scripts/package_lambda.py
python scripts/verify_lambda_aws.py --expected-account YOUR_12_DIGIT_ACCOUNT \
  --region eu-west-1 --directory var/lambda-acceptance --allow-create-and-destroy
```

The Lambda fixture creates UUID-named resources with synthetic data and cleans up in `finally`, including versioned objects. If a process is forcibly terminated or an API result is ambiguous, inspect its private `resources.json` inventory and verify every listed resource independently; a local process cannot guarantee cleanup after host loss. Offline tests exercise partial deployment failures and cleanup failures. This experiment does not upgrade account plans or purchase a subscription automatically.
