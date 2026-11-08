# Executed verification — 2026-10-04

This section records the original local checks on 2026-10-04. Later GitHub Actions and supervised AWS execution are recorded in [cloud verification](aws-verification.md); they do not retroactively change the scope of these original measurements.

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

The repository contains a verified local implementation and a supervised AWS acceptance run. It does **not** constitute an operational fully scheduled AWS platform. The 2026-11-08 gates passed for streaming delivery/failure recovery, EMR projection parity and Redshift load/transaction/concurrency checks. The final local suite passed **122 tests**, including the JVM Spark test; Ruff, configured mypy and machine-readable contract validation passed. Remaining work includes distributed raw-to-Silver execution/orchestration, transactional S3/Glue publication, worker/host-loss experiments, production-scale warehouse tuning, sustained SLA evidence and final billing reconciliation.
