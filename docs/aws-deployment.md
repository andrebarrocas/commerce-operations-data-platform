# AWS assets and deployment boundary

The repository contains runnable Kinesis producer and Lambda validator adapters, a real Lambda packaging script, and provider-validated infrastructure definitions. No AWS resources have been deployed and no cloud performance measurements have been made. Fake-client tests prove application logic; they do not prove IAM, quotas, delivery behavior, or managed-service recovery.

## Reproduce local validation

Run from the repository root after installing Python development dependencies:

```sh
PYTHONPATH=src pytest -q tests/test_aws.py
python scripts/package_lambda.py
cd infra
tofu fmt -check
tofu init -backend=false -input=false
tofu validate
```

The packaging script builds `dist/lambda.zip` from the actual transport, validator, metrics, and event-contract sources. It installs pinned Linux/x86_64 wheels for Python 3.11; Spark, PyArrow and DuckDB are excluded. Boto3 comes from the Lambda runtime and is imported only at the cloud entry point. Dependency updates require tests and a fresh artifact. No dummy handler or empty ZIP is used.

Validation uses OpenTofu 1.13.1 and AWS provider 6.67.0. The official macOS arm64 OpenTofu archive was checked against its published SHA256 (`81aebe6453223bcb3ce3c28424b36d009ebfddd3ad65ad1ec3eed0d0d574a77b`). That checks download integrity; it does not claim independent signature verification. Provider installation verified its signing key. Commit `.terraform.lock.hcl`; keep `.terraform/`, state, plans, local variable files, and credentials out of Git. [OpenTofu verification instructions](https://opentofu.org/docs/intro/install/standalone/).

## Infrastructure purpose and defaults

| Resource | Default and responsibility |
| --- | --- |
| Kinesis | One provisioned shard, 24-hour retention, KMS encryption |
| Firehose | Kinesis source, 60-second/5-MiB buffering hints, gzip raw S3 delivery, newline processor; no filtering or schema conversion |
| Lambda | Python 3.11/x86_64, 256 MiB, 30-second timeout, two reserved concurrent executions; row-level signals and quarantine |
| S3 | Private versioned bucket, public access blocking, bucket-owner enforcement, TLS-only policy, KMS encryption with bucket key |
| KMS | Project key with rotation and 30-day deletion window; scoped workload actions |
| Glue | Explicit database; catalog entries must describe committed snapshots |
| EMR Serverless | No warm workers, 8-vCPU/32-GB maximum, five-minute idle stop, scoped execution role and logs |
| Redshift | Disabled initially; optional private workgroup, managed admin secret, SSL, 8-RPU cap and daily 8-RPU-hour deactivation limit |
| CloudWatch | Fourteen-day pipeline log retention; throttling, lag, validator, quarantine-write and quality alarms |

Raw objects expire after 90 days by default; noncurrent raw versions can remain another 30 days. Curated, quarantine, and failed-attempt data requires deliberate cleanup after checking retained manifests. `force_destroy=false` prevents silent deletion of a populated bucket. KMS, Kinesis, logs, storage, and optional Secrets Manager have costs even at low useful volume. Compute limits are not absolute spending caps.

The Lambda mapping allows three retries and one-hour maximum record age, with batch splitting, partial failure reporting, and S3 on-failure retention. Records following the lowest failed sequence may be replayed. Invalid records count as consumed only after durable quarantine. Failed-batch evidence remains separate from authoritative raw input. [AWS partial batch semantics](https://docs.aws.amazon.com/lambda/latest/dg/services-kinesis-batchfailurereporting.html), [S3 failure destination behavior](https://docs.aws.amazon.com/lambda/latest/dg/kinesis-on-failure-destination.html).

## Wire framing and trusted time

Each Kinesis record holds one transport JSON object:

```json
{"transport_version":"1.0.0","record_id":"stable-event-id","payload_base64":"..."}
```

Base64 preserves original business-event bytes, including malformed JSON, non-UTF-8 bytes, and embedded newlines. Firehose inserts record delimiters. The producer writes null business `ingestion_timestamp`; Lambda overrides it with service-provided `approximateArrivalTimestamp`, normalized to UTC `Z`, before validation. Duplicate JSON keys, nonfinite numbers, unknown frame versions, invalid base64, and transport/business ID mismatch are rejected.

Unmodified Firehose payloads do **not** retain each record's Kinesis sequence/arrival metadata. A batch bridge uses object identity/version and record index for provenance, and S3 `LastModified` as a conservative availability upper bound. That clock must be labelled object delivery time; it cannot establish exact Kinesis latency. Lambda quarantine keeps finer arrival metadata but is never unioned into canonical input. Buffering makes batch availability later than stream observation. [Firehose record concatenation](https://docs.aws.amazon.com/firehose/latest/dev/basic-deliver.html).

`send_events(events, boto3.client("kinesis"), stream_name)` sends at most 500 records per request, with conservative 1-MiB record and 5-MiB request limits. It retains stable `order_id` keys, retries individually failed rows after partial responses, and retries the unresolved subset after ambiguous timeouts. Bounded exponential jitter limits retries. `ProducerFailure.pending_ids` identifies unresolved input. Retrying can reorder arrivals; `entity_version` controls lifecycle order. The caller must retain input if it wants resumable production across process restarts.

Deterministic quarantine keys identify repeated attempts at the same logical evidence; bucket versioning can still retain repeated physical writes. EMF `Stage=stream_attempt` metrics count attempts, not unique events. Final unique quality/completeness belongs to batch reconciliation. Empty `alarm_action_arns` creates visible alarms without notifications. Missing metrics are non-breaching for sparse-workload error alarms; an entirely silent pipeline still needs schedule/freshness monitoring.

## Persistent state and a reviewable cloud plan

Use an isolated AWS sandbox and temporary role/SSO credentials through standard environment/profile configuration. No credentials belong in Git, scripts, variable files or publicly shared plans. Attach the output producer policy only to a deliberately selected execution role.

Before cloud plan/apply, copy `infra/backend.tf.example` to `infra/backend.tf` and prepare a private configuration from `infra/backend.hcl.example`. The administrative state bucket must exist independently, with encryption, versioning, public access blocking, restricted IAM, and recovery retention. OpenTofu 1.13.1 supports `use_lockfile=true` native S3 locking. The operator needs exact state/lock-object permissions, including deletion of the lock object, plus required state-key KMS actions. Deployment CI must never use ephemeral local state. [OpenTofu S3 backend](https://opentofu.org/docs/language/settings/backends/s3/).

```sh
# From infra/, with an existing backend and sandbox credentials:
tofu init -backend-config=/secure/location/commerce-backend.hcl
tofu plan -var-file=dev.tfvars -out=reviewed.tfplan
```

A plan contacts AWS; it was not executed here. Review actual account/region, resources, IAM, quotas, network, and cost before applying the saved plan. All variables have defaults for validation; `lambda_zip_path` defaults to `../dist/lambda.zip` relative to `infra/`. Copy `dev.tfvars.example` for intentional overrides. Use a protected deployment environment and persistent remote state in CI.

Optional Redshift requires supplied private subnets across three AZs and restricted security groups. Enhanced VPC routing also requires S3 connectivity for COPY. The module creates no VPC, NAT gateway, or public ingress. The precondition checks supplied ID counts, not actual AZ diversity, routes or security-group rules; verify those in the target account. Managed admin credentials avoid passing a password through Terraform input. Prefer short-lived IAM/Data API access for routine automation.

EMR 7.12.0 uses Spark 3.5.6-amzn-1, while local PySpark is pinned separately. Its default environment can access supported same-region services without a customer VPC/NAT. The application and role are a foundation: Terraform does not schedule jobs, upload artifacts, implement a cloud release coordinator, or make local filesystem code S3-compatible. Submit a tested cloud entry point with worker settings within the cap and a bounded execution timeout. [EMR 7.12 release](https://docs.aws.amazon.com/emr/latest/ReleaseGuide/emr-7120-release.html), [EMR Serverless service access](https://docs.aws.amazon.com/emr/latest/EMR-Serverless-UserGuide/considerations.html).

## AWS acceptance evidence still required

Retain actual resource/runtime versions, run IDs, control totals, duplicate/malformed scenarios, quarantine-write failure and recovery evidence, S3 raw delivery checks, Lambda lag and billing data. Exercise IAM allow/deny paths, verify the Linux package loads, confirm discarded records reach S3, and prove EMR publication before calling the deployment operational. Redshift needs its own SQL execution, plans and measurements. Unavailable measurements remain unavailable; local timings are not substitutes.

## Explicit raw capture and deployment automation

The read-only capture bridge fetches only the named object with SDK credentials:

```sh
python scripts/import_firehose.py --bucket YOUR_LAKE_BUCKET \
  --key raw/ARRIVAL_PATH/OBJECT.gz --output var/captured-object.jsonl
commerce ingest --input var/captured-object.jsonl --lake var/aws-capture \
  --as-of 2026-11-01T00:00:00Z
```

Choose the cutoff deliberately from your experiment, not from this example date. The bridge handles gzip, stores original object identity/version/checksum/line and preserves malformed business bytes. The local engine overwrites producer ingress with captured object availability. An identical recapture is safe; replacing an existing capture with different content fails. These commands are a manually supervised bridge, not distributed S3 batch orchestration.

`.github/workflows/deploy.yml` supports a deliberate `workflow_dispatch` plan or apply. Configure the protected `portfolio-aws` environment, an OIDC-trusting deployment role in `AWS_DEPLOY_ROLE_ARN`, and a pre-existing private state bucket before using it. Its default operation is plan; apply executes the saved plan from that run under remote state locking. Configure environment reviewers/branch restrictions in GitHub—the YAML cannot assert they already exist. Neither this workflow nor CI has been run in GitHub during local development.

The Redshift role trust includes both documented Redshift service principals. The EMR role permits deletion only under Spark task `_temporary` paths inside curated prefixes; it cannot delete raw input or final curated objects. Cloud acceptance must verify the selected EMR committer's exact temporary-object operations and adjust narrowly if its behavior differs. [Redshift Serverless trust requirements](https://docs.aws.amazon.com/redshift/latest/mgmt/serverless-security-other-services.html), [EMR job runtime roles](https://docs.aws.amazon.com/emr/latest/EMR-Serverless-UserGuide/security-iam-runtime-role.html).
