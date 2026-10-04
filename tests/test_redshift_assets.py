"""Offline harness behavior; these tests do not execute Redshift SQL or measure AWS."""

import importlib.util
import json
from pathlib import Path
from typing import Any

import pytest

SCRIPT = Path(__file__).resolve().parents[1] / "scripts/benchmark_redshift.py"
spec = importlib.util.spec_from_file_location("benchmark_redshift", SCRIPT)
assert spec and spec.loader
benchmark = importlib.util.module_from_spec(spec)
spec.loader.exec_module(benchmark)


class FakeDataAPI:
    def __init__(self, status: str = "FINISHED") -> None:
        self.requests: list[dict[str, Any]] = []
        self.status = status
        self.cancelled: list[str] = []

    def execute_statement(self, **kwargs: Any) -> dict[str, str]:
        self.requests.append(kwargs)
        return {"Id": f"statement-{len(self.requests)}", "SessionId": "one-session"}

    def describe_statement(self, **kwargs: Any) -> dict[str, Any]:
        return {
            "Id": kwargs["Id"],
            "SessionId": "one-session",
            "Status": self.status,
            "Duration": 125000000,
            "RedshiftQueryId": len(self.requests),
            "HasResultSet": True,
            "Error": "fixture error" if self.status == "FAILED" else "",
        }

    def get_statement_result(self, **kwargs: Any) -> dict[str, Any]:
        if "NextToken" not in kwargs:
            return {"Records": [[{"stringValue": "page-one"}]], "NextToken": "second"}
        return {"Records": [[{"stringValue": "page-two"}]]}

    def cancel_statement(self, **kwargs: Any) -> dict[str, bool]:
        self.cancelled.append(kwargs["Id"])
        return {"Status": True}


def test_session_reuse_cache_boundary_and_pagination(tmp_path: Path) -> None:
    client = FakeDataAPI()
    runner = benchmark.Session(client, "workgroup", "dev", tmp_path / "evidence.jsonl")
    runner.execute("SET enable_result_cache_for_session TO off", phase="setup", label="cache")
    measured = runner.execute("SELECT 1", phase="trial", label="probe")
    assert client.requests[0]["WorkgroupName"] == "workgroup"
    assert client.requests[1]["SessionId"] == "one-session"
    assert "WorkgroupName" not in client.requests[1]
    assert measured["engine_seconds"] == 0.125  # API fixture nanoseconds, not benchmark data.
    assert measured["rows"] == [[{"stringValue": "page-one"}], [{"stringValue": "page-two"}]]
    assert len((tmp_path / "evidence.jsonl").read_text().splitlines()) == 2
    assert measured["sql_sha256"] and measured["result_sha256"]


def test_failure_and_timeout_keep_evidence_and_never_return_success(tmp_path: Path) -> None:
    failed = benchmark.Session(FakeDataAPI("FAILED"), "wg", "db", tmp_path / "failed.jsonl")
    with pytest.raises(RuntimeError, match="FAILED"):
        failed.execute("SELECT broken", phase="trial", label="failure")
    evidence = json.loads((tmp_path / "failed.jsonl").read_text())
    assert evidence["status"] == "FAILED"
    assert "result_sha256" not in evidence
    client = FakeDataAPI("STARTED")
    timeout = benchmark.Session(client, "wg", "db", tmp_path / "timeout.jsonl", timeout_seconds=0)
    with pytest.raises(RuntimeError, match="timeout=True"):
        timeout.execute("SELECT slow", phase="trial", label="timeout")
    assert client.cancelled == ["statement-1"]


@pytest.mark.parametrize(
    "bad", ["public.x; DROP SCHEMA public", "x", "schema.table.extra", 'a."b"', "a.b c"]
)
def test_sql_identifiers_reject_injection(bad: str) -> None:
    with pytest.raises(ValueError):
        benchmark.qualified(bad)


def test_copy_paths_are_quoted_and_role_is_validated() -> None:
    sql = benchmark.copy_sql(
        "commerce_bench_test.base",
        "s3://bucket-name/it's-manifest.json",
        "arn:aws:iam::123456789012:role/redshift-copy",
    )
    assert "it''s-manifest" in sql
    assert "FORMAT AS PARQUET MANIFEST" in sql
    assert "REGION" not in sql  # Unsupported by columnar COPY.
    with pytest.raises(ValueError):
        benchmark.copy_sql(
            "commerce_bench_test.base", "s3://bucket-name/data", "role'; DROP TABLE x"
        )


def test_summary_excludes_warmups_and_missing_engine_duration() -> None:
    samples = [
        {"label": "auto/q", "phase": "warmup", "engine_seconds": 999, "client_wall_seconds": 999},
        {"label": "auto/q", "phase": "trial", "engine_seconds": 2, "client_wall_seconds": 3},
        {"label": "auto/q", "phase": "trial", "engine_seconds": None, "client_wall_seconds": 5},
    ]
    summary = benchmark.sample_summary(samples)["auto/q"]
    assert summary["engine_seconds"] == {"median": 2, "p95_nearest_rank": 2, "samples": 1}
    assert summary["client_wall_seconds"]["median"] == 4


def test_ddl_variants_preserve_contract_columns_and_merge_targets_every_column() -> None:
    root = SCRIPT.parents[1]
    contract = json.loads((root / "contracts/products/shipment_lifecycle.json").read_text())
    for variant in benchmark.VARIANTS:
        ddl = benchmark.table_ddl(f"commerce_bench_test.{variant}", variant)
        for column in contract["columns"]:
            assert f"{column['name']} " in ddl
    merge = (root / "sql/redshift/load_merge.sql").read_text()
    # This checks migration drift only, not SQL parser/transaction behavior.
    for column in contract["columns"]:
        if column["name"] != "shipment_id":
            assert f"{column['name']} = s.{column['name']}" in merge
    assert "Duplicate source shipment keys" in merge
    assert "LOCK TABLE commerce.load_ledger" in merge
    assert "DELETE FROM commerce.shipment_lifecycle" in merge
