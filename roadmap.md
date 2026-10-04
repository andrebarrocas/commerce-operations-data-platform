# Phased implementation roadmap

Each phase ends with tests, documented evidence and a critical review. Design targets are separate from observed results.

| Phase | Runnable deliverable | Exit gate |
|---|---|---|
| 0 — design | Schemas, product contracts, requirements, domain, architecture, ADRs | Parse/validate contracts; review failure modes and mark limits |
| 1 — local foundation | Deterministic producer, validator, raw/quarantine, CLI | Valid/invalid/versioned contracts; reproducible generation; malformed isolation |
| 2 — reliable lakehouse | Parquet layers, dedup/corrections, manifests, configurable quality | Cross-batch duplicate, conflict, failpoint, restart, replay, bounded-backfill tests |
| 3 — reusable products | Six products, dimensions/facts, advanced SQL, point-in-time features | SQL data tests and leakage/lifecycle/late-history assertions |
| 4 — Spark and AWS assets | Actual PySpark batch, streaming adapters, Terraform and CI | Spark execution; producer retry and Lambda tests; IaC validation |
| 5 — measurement and operations | Repeatable benchmark harnesses, runbooks, cost units, warehouse SQL | Real local measurements; AWS results only with deployed resources and recorded evidence |
| 6 — cloud acceptance | Provisioned integration, failure drills, Redshift plans and costs | Requires AWS account, chosen region, budget, deployment execution; never inferred from local tests |
| Optional | Comparable Rust producer | Only after core and cloud acceptance; equal workloads and measured resource/latency comparison |

## Repository layout

```text
contracts/                  JSON Schemas and product contracts
src/commerce_platform/      Python producer, validation, quality, lake, CLI and adapters
sql/                        Executed local transformations and Redshift dialect SQL
spark/                      Bounded PySpark jobs
infra/                      Terraform deployment assets
scripts/                    Validation, measurements and cloud submission
tests/                     Unit, integration, data and recovery assertions
.github/workflows/          CI checks (no implicit infrastructure apply)
docs/adr/                   Decision records
docs/                      Design review, operations and evidence
benchmarks/                 Small checked-in measurement artifacts; generated data ignored
```

## Deferred functionality is explicit

A stage marked implemented is still not a production service. Multi-writer object-store transactions, 24/7 orchestration, scale-out failure behavior, cloud SLA compliance, and Redshift tuning require cloud acceptance. Prefer a smaller verified implementation to an untested broad claim. No placeholder functions may report success.

## Delivery status after local execution

Phases 0–3 have executed local gates, including historical correction propagation into products and immutable prior releases. Phase 4 has an executed local Spark job, tested source adapters, a real Lambda artifact and provider-validated Terraform. Phase 5 has three measured Python trials plus replay/recovery and a separate measured Spark experiment. Exact evidence and limitations are in `benchmarks.md` and `docs/verification.md`.

Phase 6 remains open: no cloud resources or GitHub-hosted workflows were run. The next deployment work is real IAM/transport verification, EMR runtime and publication coordination, cloud SLA reconciliation, and Redshift SQL/plan/cost experiments. The source capture bridge is manually supervised; there is no implemented fully scheduled raw-to-warehouse AWS orchestration. Rust is intentionally omitted while those core acceptance obligations remain.
