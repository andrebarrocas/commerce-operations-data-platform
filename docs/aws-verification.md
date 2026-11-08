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
