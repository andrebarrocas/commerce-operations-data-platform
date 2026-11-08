# ADR 0008: Isolate Lambda's failed-batch S3 destination

Status: accepted after actual AWS acceptance on 2026-11-08.

The initial execution role allowed failure writes only under the documented `aws/lambda/` prefix in the lake. Actual `CreateEventSourceMapping` rejected that configuration with `InvalidParameterValueException: ... permissions to call PutObject on S3`, including after IAM propagation. Provider validation and fake-client tests had not exercised that control-plane check.

Use a separate private, versioned, KMS-encrypted bucket for discarded invocations. Grant bucket-wide `s3:PutObject` only there, with `s3:ResourceAccount` restricted to the deployment account. Retain prefix-scoped quarantine writes in the main lake. Apply TLS-only access, public access blocking and bounded retention to the new bucket. Do not grant broad writes to authoritative lake data merely to satisfy destination validation.

This adds one low-volume bucket and its configuration to operate and clean up. The live failure drill confirmed that retry exhaustion retains the full original record and replay succeeds after the exact original policy is restored. Failure evidence remains separate from canonical raw input. Acceptance evidence is linked from [AWS verification](../aws-verification.md).
