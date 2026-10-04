# Reliability and operations

## Processing guarantees

At-least-once input and deterministic canonicalization, not end-to-end exactly-once delivery. Immutable input is the source of truth. Producer retries only failed Kinesis entries; an ambiguous network failure may duplicate the whole request. Logical identity resolves duplicates downstream. Same ID with changed semantic content is an error, not an update.

Local work uses an OS advisory lock held across reading current state, writing an immutable run and replacing the committed manifest pointer. Process exit releases the lock. An incomplete run remains unreferenced; restart derives progress from committed state and retained raw data. The output directory is not the input checkpoint. Readers open the head once, then use paths from that immutable manifest. Never glob all run directories.

## Failure handling

| Failure | Response | Recovery proof |
|---|---|---|
| Malformed/schema-incompatible payload | Preserve exact bytes and reason in Bronze/quarantine | Revalidate retained raw under explicitly changed contract |
| Exact duplicate | Increment duplicate metric, preserve earliest availability | Replay gives identical canonical business hash |
| Same ID different content | Reject conflicting identity; fail closed for publication | Operator identifies bad producer; audited input disposition is required because a new correction alone cannot remove a conflicting immutable ID |
| Missing lifecycle predecessor | Incomplete product until history arrives | Late-event replay repairs lifecycle |
| Impossible lifecycle | Quality issue blocks affected output publication | Correct source event, then rerun |
| Exception before commit | Committed head unchanged | Failpoint/restart test |
| Worker interruption | Unreferenced run; raw remains | Restart, inspect old run, then retention cleanup |
| Bad historical correction | Reject unresolved/cyclic/cross-entity references | Correct chain before replay |
| Unknown expected count | Completeness `unknown` | Supply producer manifest; never infer success from no data |
| AWS delivery/permissions failure | CloudWatch alarms plus raw retention | Requires cloud fault drill; not demonstrated by local mocks |

## Replay and backfill runbook

1. Record incident, source locators, contract versions and affected business dates. Conflicting-ID remediation requires an audited raw-input selection/disposition; until that workflow exists, the run remains blocked. Never silently discard a conflicting variant.
2. Verify all retained inputs and their checksums; preserve original first observation timestamps.
3. Run to an isolated output namespace; use an explicit half-open UTC range and maximum window.
4. Recompute complete entity histories before applying the output range. Do not cut off predecessors at the range boundary.
5. Inspect quarantine, quality, expected-ID reconciliation and old/new product counts.
6. Publish only after gates pass. A bounded backfill has its own head and cannot replace the complete current dataset. Promote a corrected full snapshot by rerunning the normal pipeline.
7. Keep previous immutable snapshots for rollback. Rollback changes the pointer, never edits data files.

## Observability

Structured JSON metrics: input_records, accepted_records, rejected_records, duplicate_records, conflict_records, late_records, processing_seconds, quality_seconds, records_per_second, event_age_seconds, ingestion_to_processing_seconds, retries, failures, quality_state, freshness_state, completeness_state, bytes_read, bytes_written. Report p50/p95/p99 only for observed samples; simulated source lag is labelled simulated. Use monotonic timing for durations and UTC wall time for observation.

CloudWatch namespace `CommercePlatform`, dimensions `Environment`, `Pipeline`, `Stage` only. No order IDs, event IDs or unbounded run IDs as metric dimensions. Log event/run IDs for diagnosis. Alarm on errors/throttles, delivery freshness/iterator age, rejected rate with volume guard, failed quality, missing expected heartbeat and job duration. Use `notBreaching` for sparse error counts and `breaching` only for an expected continuous heartbeat. An alarm definition without exercised routing is not on-call readiness.

## Security and retention

No personal data is generated. Use IAM roles/default credential chain; never embed keys or credentials in examples, state, logs or Git. State storage requires a private encrypted backend with separate access control. S3 blocks public access and denies insecure transport. Lifecycle policies bound raw, snapshots, quarantine and log cost. Do not delete retained raw until replay objectives and retention requirements permit. Production deletion/erasure and cross-region recovery are outside this synthetic project's acceptance claims.

## Verified local implementation details

Raw files, output artifacts and directory entries are fsynced before a head publication; the head itself is replaced and its directory fsynced. This provides the intended durable ordering on a filesystem that honors fsync. Process failpoints cover after Silver, before commit and lost acknowledgement after commit; no physical power-cut experiment has been performed. Exact reruns verify every committed Parquet checksum before reuse.

Canonical Silver removes diagnostic trace IDs and normalizes observation-time spelling to UTC microseconds; raw retains the original bytes. Business timestamp strings are part of semantic identity and must be stable across retries. Known correction chains are resolved after observation-time filtering and before filtering effective business time, preventing a known correction to a future effective time from resurrecting its superseded fact.

Missing evidence is listed in `semantic_quarantine.jsonl`: `pending` inside the 48-hour allowance, `overdue` afterward. Every replay reconsiders it; lateness never deletes evidence. Contradictory semantics fail the entire release and preserve `failure.json`. The initial implementation resumes from retained input and a committed head by recomputing a snapshot; it does not cache/reuse individual completed Spark partitions.
