# Executed verification

## Reverification — 2026-10-29

Author and maintainer: [André Barrocas](https://github.com/andrebarrocas). The initial repository commit is attributed to this GitHub account. Project ownership is also recorded in `CODEOWNERS`, the package metadata, and the license.

The project was reinstalled into a new Python 3.11.14 virtual environment using `requirements.lock`. This avoids relying on the original development environment. All **109 tests passed**, including the real Java 17/PySpark integration test and two new reviewer-command tests. Lint, formatting, static types, dependency consistency, event/product contracts, and OpenTofu validation also passed. OpenTofu reported zero errors and zero warnings with AWS provider 6.67.0; no AWS plan or apply was executed.

The separate public CLI verification generated **1,000 orders and 14,307 distinct valid events**. It published 22 Parquet artifacts, checked every artifact checksum and row count, checked six product schemas/types/primary keys, reconciled every expected producer event, observed 142 duplicate deliveries and 57 rejected malformed records, and proved an exact replay preserved the committed head and logical result. The portable report is [checked in](../benchmarks/demo-verification-2026-10-29.json). This is a functional verification run; its timings are not a controlled benchmark.

The [original GitHub Actions run](https://github.com/andrebarrocas/commerce-operations-data-platform/actions/runs/37200806047) was inspected and all three jobs passed on Linux: local tests/SQL/lint/types/contracts, real Spark, and Linux Lambda packaging/import plus OpenTofu validation. The workflow now also runs local verification on Python 3.11 and 3.12, repeats the public CLI demonstration, and uploads JUnit and JSON reports. The README badge links to [current hosted results](https://github.com/andrebarrocas/commerce-operations-data-platform/actions/workflows/ci.yml).

The [showcase commit's hosted run](https://github.com/andrebarrocas/commerce-operations-data-platform/actions/runs/40861898451) passed all four jobs, including both Python versions and the uploaded demonstration reports. Its runner annotations identified deprecated Node 20 Actions; workflow pins were subsequently refreshed to releases declaring Node 24, and runners were fixed to Ubuntu 24.04 for reproducibility. Manual deployment remains unexecuted and requires the documented AWS setup.

Reproduce the public CLI check from the source checkout after installing the documented dependencies:

```bash
python scripts/verify_demo.py --work var/reviewer-check --report var/reviewer-check.json
```

Use new paths for each run; the command refuses to overwrite existing evidence. A failed check exits nonzero and does not produce a passing report. CI reports are available as `local-verification-3.11` and `local-verification-3.12` artifacts on the corresponding successful workflow run.

Live AWS ingestion, EMR execution, Redshift execution/performance, cloud IAM behavior and achieved cloud SLAs remain unverified. Adapter tests use fake clients and infrastructure checks validate configuration. The original benchmark artifacts remain unchanged; the new demonstration uses the same core implementation hash.

## Initial verification — 2026-10-04

All checks below were executed locally during initial development. GitHub Actions had not yet run at the time this initial record was written; the hosted result was subsequently verified above. AWS deployment was not run.

| Gate | Result | Evidence boundary |
|---|---|---|
| Phase 0 design | Passed after critical review | 2 JSON schemas, 6 product contracts, 24 versioned examples and negative payload variants |
| Python + SQL + recovery + adapters | 107 tests passed, including Spark | Python 3.11.14, Java 17; tests run in 10.63 s (not a performance benchmark) |
| Lint | Passed | Ruff configured rules across source, tests, scripts and Spark |
| Formatting | Passed | 31 Python files at final lint run |
| Static types | Passed | 12 source modules under configured mypy checks |
| Terraform/OpenTofu | Passed | Formatting and provider-backed validate; OpenTofu 1.13.1, AWS provider 6.67.0 |
| Local CLI | Passed | Real 100-order demo: 1,427 unique events, 15 duplicates, 6 malformed records; 22 published Parquet artifacts |
| Full rebuild/replay/recovery | Passed | Three 5,000-order/71,568-event trials and injected pre-commit recovery; exact logical hashes match |
| Real Spark | Passed | Integration test plus separate 71,568-event job; 3 projection outputs |
| Redshift harness | Offline tests passed | No Redshift SQL execution, plans, latency or cloud cost observed |
| Lambda package | Built from source | Linux/x86_64 wheels and packaged source/schema parity; actual Lambda invocation remains untested |

## Commands

```bash
python scripts/check_contracts.py
python -m ruff check src tests scripts spark
python -m ruff format --check src tests scripts spark
python -m mypy src/commerce_platform
RUN_SPARK_TESTS=1 SPARK_LOCAL_IP=127.0.0.1 python -m pytest -q
python scripts/package_lambda.py
tofu -chdir=infra fmt -check
tofu -chdir=infra init -backend=false
tofu -chdir=infra validate
```

The implementation review found and fixed source-frame ambiguity, calendar validation, missing-version handling, cross-entity references, temporal reservation accounting, trace-induced nondeterminism, correction cutoff ordering, malformed capture metadata, publication durability ordering, and fractional Spark durations. Tests exercise each relevant boundary. Physical host power loss, distributed worker loss and cloud authorization/service limits still require appropriate deployment experiments.

## Interpreting completion

The repository contains a verified local engineering implementation and AWS deployment assets. It does **not** constitute an operational fully scheduled AWS platform. The remaining work is concrete: supervised AWS deployment with budget, source and IAM failure drills, raw-to-Silver distributed execution/orchestration, transactional S3/Glue publication, actual Redshift SQL/load tests and benchmark capture, and cloud SLA/cost evidence. The roadmap preserves those gates instead of marking unexecuted work complete.
