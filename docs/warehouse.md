# Redshift serving and measurement

Redshift assets are **prepared and tested offline; not deployed or measured in AWS**. Local DuckDB tests do not validate Redshift SQL syntax, transactions, physical execution plans, materialized views or performance. The cloud acceptance gate remains open.

## Serving model and publication

`sql/redshift/ddl.sql` defines the shipment accumulating snapshot at one row per dispatched shipment, matching every column and column order of the `shipment_lifecycle` Parquet contract. The initial table uses distribution, sort and encoding AUTO. These are starting choices, not measured optimal settings. `load_ledger` records release identity, source-manifest hash and staged rows. Redshift primary keys are not used as uniqueness enforcement.

`sql/redshift/load_merge.sql` creates a default atomic, security-invoker procedure. A loader creates session-local staging and affected-key tables, loads only files in one immutable release, then calls the procedure. It locks the ledger and target, checks release identity, duplicate keys, control totals and scope, removes affected target rows absent from staging, performs `MERGE`, and records the release in the same transaction. Exceptions roll everything back. A retry of the same committed release/hash is a no-op. A reused release ID with different input fails. Separate sessions have separate staging tables; target publication is serialized.

The same session must perform these steps:

1. Apply the schema/table DDL and procedure definition explicitly. For deployment tools, send each top-level statement separately; do not split a dollar-quoted procedure body on semicolons.
2. Create the two temporary tables in `ddl.sql` for this session.
3. `COPY` the shipment product using an S3 manifest and the approved attached IAM role. Validate every source object against the committed release before constructing the COPY manifest.
4. Fill `affected_shipments`. For a complete replacement use the union of target and staged shipment IDs. For a bounded correction provide the explicit impacted IDs, including tombstones. Never infer deletion scope from source rows alone.
5. Call `commerce.sp_publish_shipment_release` with bound release ID, source-manifest SHA256 and expected row count. Retain the call's query ID and ledger row.
6. Refresh downstream materialized views separately and record their consumed release/freshness. Their refresh is not atomic with the base-table commit.

The SQL procedure supplies an executable transaction pattern; the repository does not claim an exercised cloud loader/coordinator. Cloud failure drills must test duplicate staged keys, corrupted control totals, corrections deleting a former row, rollback after a staged failure, concurrent callers and a retry after ambiguous client timeout. Inspect the ledger before issuing a new release after a timeout. Default atomic stored-procedure behavior and Redshift MERGE constraints are documented by AWS: [CREATE PROCEDURE](https://docs.aws.amazon.com/redshift/latest/dg/r_CREATE_PROCEDURE.html), [MERGE](https://docs.aws.amazon.com/redshift/latest/dg/r_MERGE.html).

## Benchmark input and safety boundaries

The executable harness is `scripts/benchmark_redshift.py`. It requires an existing Serverless workgroup/database and IAM-authenticated AWS access. It does not provision resources. Running it incurs query and storage charges. It creates a fresh schema prefixed `commerce_bench_` and refuses existing schema names; it never drops user tables. It retains one base copy and three physical variants, so budget for roughly four copies before compression differences and temporary work.

Supply either an existing contracted table via `--base-table schema.table` or a Parquet COPY manifest via `--s3-manifest` plus `--iam-role`. An existing table is copied once into the isolated base; all variants read that fixed base. A COPY manifest must list immutable files with `mandatory: true` and `meta.content_length`, and must share the workgroup's AWS Region. The files must contain exactly the contracted shipment columns, in contract order. Do not point the manifest at all run directories or an uncommitted staging prefix. Columnar COPY does not accept a REGION option. See [columnar COPY restrictions](https://docs.aws.amazon.com/redshift/latest/dg/copy-usage_notes-copy-from-columnar.html) and [Parquet manifest requirements](https://docs.aws.amazon.com/redshift/latest/dg/r_COPY_command_examples.html).

Generate a source scale that exceeds a trivial metadata-only workload using the normal synthetic generator and lake pipeline. Retain source seed, input/control manifest, row count and data bytes. `--dataset-id` links that retained evidence; the harness `--seed` randomizes trial order only. It never claims the source has been regenerated from that trial seed.

Set `REDSHIFT_WORKGROUP`, `REDSHIFT_DATABASE`, `AWS_REGION` and `REDSHIFT_COPY_ROLE_ARN` as appropriate. Use an IAM role/default AWS credential chain; no passwords or access keys belong in commands or Git. With `SHIPMENT_COPY_MANIFEST` and `SOURCE_MANIFEST_SHA256` pointing to actual retained inputs, an invocation is:

```sh
python scripts/benchmark_redshift.py \
  --schema "commerce_bench_$(date -u +%Y%m%d_%H%M%S)" \
  --s3-manifest "$SHIPMENT_COPY_MANIFEST" \
  --iam-role "$REDSHIFT_COPY_ROLE_ARN" \
  --dataset-id "$SOURCE_MANIFEST_SHA256" \
  --from-date 2026-01-01 --to-date 2026-02-01 \
  --warmups 1 --trials 5 --seed 42 \
  --output work/redshift-measurement
```

Choose dates covering the actual input; the runner rejects an empty selected population. Choose a new output directory on every experiment. `python scripts/benchmark_redshift.py --help` is safe offline. Installing the project AWS extra provides boto3.

## Measurement protocol and evidence

The runner disables result caching and verifies the setting, preserving the same Data API `SessionId` for all subsequent work. It executes serially because a reused session supports only one active query. A fresh session per query would silently lose the cache setting. [AWS session reuse](https://docs.aws.amazon.com/redshift/latest/mgmt/data-api.html).

It compares AUTO, EVEN plus timestamp sort, and order-ID KEY plus timestamp/order sort against identical data and query text apart from table names. Each table is analyzed. Workloads cover date filtering, aggregation and an order join. EXPLAIN output is saved before timing. Trial order is randomized reproducibly, warmups are excluded, and every result must match across variants. Result caching is disabled, but storage/buffer caches are not flushed; this is a warmed mixed-order experiment, not a cold-cache claim. AUTO physical choices may evolve, so before/after physical metadata is captured when permissions permit.

`configuration.json` records source identity, selected dates, randomization seed, workgroup/database, warmups, repetitions, concurrency and available workgroup capacity settings. `statements.jsonl` is appended after each completed/failed/timed-out statement and records SQL, SHA256, Data API statement ID, engine query ID, status, EXPLAIN/result rows and timing. Result fetching/pagination is outside the recorded client query wall duration. Failed diagnostics remain explicit unavailable files; they are never replaced with invented zeros.

The API `Duration` field is nanoseconds and is converted to engine seconds. Client wall duration includes submission, queueing, polling and service overhead; it is not interchangeable with engine execution duration. `sys_query_history` is captured for queue/execution details when accessible. Missing engine duration remains null. Summary medians and nearest-rank p95 values include their sample count; five repetitions do not establish tail-latency SLAs. [DescribeStatement duration and status fields](https://docs.aws.amazon.com/redshift-data/latest/APIReference/API_DescribeStatement.html).

The harness also creates and refreshes a carrier materialized view, records refresh separately, and compares its results against the base aggregate. `sql/redshift/materialized_views.sql` provides the serving version; `analytics.sql` supplies calendar-bounded rolling reliability, ranking and an incremental cutoff query. Deletion tombstones still come from release scope metadata, because an upsert extraction cannot represent a vanished row.

`summary.json` exists only after the experiment completes. It includes observed samples and a cleanup statement for the isolated schema. Review evidence, then execute that exact cleanup statement explicitly when storage is no longer needed. No benchmark results are checked in unless the corresponding run actually occurred. No distribution, sort-key, materialized-view or compression improvement is claimed by these assets.
