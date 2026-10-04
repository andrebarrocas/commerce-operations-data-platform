# Measurement protocol

Numbers belong here only after execution. No production usage, AWS latency or performance improvement is inferred from laptop experiments.

## Local protocol

Use a fixed seed, start time, order count and fault rates. Record exact command, input SHA-256, logical output hash, package versions, CPU/OS, wall and process CPU duration, peak resident memory (with platform-specific units), bytes and record counts. Report generation and processing throughput separately. Exclude a correctness smoke test from performance claims. Run warmup and repeated trials for comparative results; a single run is a smoke measurement, not a stable capacity estimate.

Measure baseline, exact replay, a pre-publication injected failure followed by recovery, and data-quality duration. Event-time delay in a synthetic fixture is an input property, not real end-to-end latency. Spark runtime must come from an actual JVM job; DuckDB runtime is never labelled Spark.

## Redshift protocol

Cloud results initially **unmeasured**. Store scripts that load identical seeded inputs into AUTO, EVEN and KEY variants, collect EXPLAIN and query IDs, disable result caching per session, randomize query/variant order, execute warmups and multiple measured runs, and save medians/tails with row counts and system metadata. Keep joins, date predicates, windows and aggregations fixed across variants. Record redistribution, skew, compression and scan behavior only from observed plans/system views. Materialized view refresh cost must accompany query benefit. Redshift does not enforce primary/foreign key integrity; run explicit data tests before query optimization can rely on constraints.

Client elapsed time includes API/network/polling and must be labelled. Engine query duration requires system query-history evidence. Do not manufacture a latency when credentials or a warehouse are unavailable.

## Observed local results — 2026-10-04

Evidence: [full local measurement JSON](benchmarks/local-5000-orders.json). Apple M4, 24 GiB RAM, 10 logical CPUs, macOS 26.6.2, Python 3.11.14, DuckDB 1.5.6, PyArrow 23.0.1. Three measured complete rebuilds followed a smaller 40-order warmup. Normal desktop environment; no isolated-host or capacity claim.

| Measurement | Observed result | Boundary |
|---|---:|---|
| Workload | 5,000 orders / 71,568 unique events | Fixed seed 42, independently generated |
| Input attempts | 72,563 | Includes 709 exact duplicates and 286 malformed records |
| Late events | 2,194 | Simulated observation delay >48h, not measured cloud latency |
| Input bytes | 49,386,964 | Actual JSONL size |
| Median processing time | 29.188 s | Raw read, validation, reconciliation, products and quality; head sync follows |
| Median client completion time | 29.366 s | Full call including durable publication |
| Median throughput | 2486.1 attempts/s | Input attempts, not just accepted events |
| Median unique-event throughput | 2452.0 events/s | Unique accepted population |
| Median quality execution | 4.831 s | Contract-driven product checks, including reads of output artifacts |
| Exact replay | 0.091 s | Source + committed artifact checksum validation, no transformations |
| Failure recovery | 29.410 s | Full rerun after injected pre-commit failure to identical logical output |
| Peak process RSS | 1.527 GiB | Maximum across all trials/warmup/recovery, not a per-stage claim |
| Published Parquet bytes | 14,082,962 | 22 artifacts; no cloud-dollar conversion |

All three rebuilds and recovery produced the same logical result hash. The earlier development attempt was invalidated by code changes during its replay check and is not included as performance evidence. Source completeness was 100% against the generator's independently recorded valid-event IDs; this does not establish a production SLA or make malformed source noise disappear.

## Observed Spark result — 2026-10-04

[Recorded Spark result](benchmarks/spark-71568-events.json): PySpark 3.5.7, Java 17.0.20.1, `local[2]`, 71,568 canonical Silver events (4,760,355 bytes). The measured Spark job took **8.643 seconds**, with **10.824 seconds** of client wall time including JVM startup. It wrote 9,999 order-line facts, 5,500 shipment summaries and 228 facility-hour rows. This is one local run.

The Python and Spark measurements cover different workloads: Python includes raw validation, semantic reconciliation and nineteen product/model outputs, while Spark starts from canonical Silver and builds three projections. These timings do not establish a comparative speedup or EMR runtime. Redshift engine/query latency, EMR runtime, AWS fault recovery and cloud costs remain **not measured**.

To reproduce the Spark experiment after producing a committed local snapshot:

```bash
python scripts/benchmark_spark.py --manifest var/lake/head.json \
  --output var/spark-benchmark-new --result benchmarks/my-spark-run.json
```
