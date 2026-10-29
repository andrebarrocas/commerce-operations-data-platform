"""Exercise the public CLI and produce verifiable local demonstration evidence."""

from __future__ import annotations

import argparse
import hashlib
import json
import platform
import subprocess
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq

from commerce_platform.products import ARROW_TYPES, PUBLIC_PRODUCTS, ROOT


def require(condition: bool, message: str) -> None:
    if not condition:
        raise RuntimeError(message)


def invoke_cli(*arguments: str) -> dict[str, Any]:
    result = subprocess.run(
        [sys.executable, "-m", "commerce_platform.cli", *arguments],
        check=True,
        capture_output=True,
        text=True,
        timeout=180,
    )
    return json.loads(result.stdout)


def verify(work: Path, orders: int) -> dict[str, Any]:
    """Run in a new directory so previous output cannot mask a broken command."""
    work.mkdir(parents=True, exist_ok=False)
    initial = invoke_cli("demo", "--orders", str(orders), "--directory", str(work))
    head_path = work / "lake/head.json"
    committed_head = head_path.read_bytes()
    manifest = json.loads(committed_head)
    artifacts = manifest["artifacts"]
    require(len(artifacts) == 22, "Expected all 22 committed Parquet artifacts")
    artifact_rows = {}
    for name, artifact in artifacts.items():
        path = Path(artifact["path"])
        require(
            hashlib.sha256(path.read_bytes()).hexdigest() == artifact["sha256"],
            f"Artifact checksum mismatch: {name}",
        )
        table = pq.read_table(path)
        require(table.num_rows == artifact["row_count"], f"Row-count mismatch: {name}")
        artifact_rows[name] = table.num_rows
        if name in PUBLIC_PRODUCTS:
            contract = json.loads((ROOT / f"contracts/products/{name}.json").read_text())
            require(
                table.column_names == [c["name"] for c in contract["columns"]],
                f"Product schema mismatch: {name}",
            )
            keys = [tuple(row[k] for k in contract["primary_key"]) for row in table.to_pylist()]
            require(len(keys) == len(set(keys)), f"Duplicate product grain: {name}")
            for column in contract["columns"]:
                require(
                    table[column["name"]].type == ARROW_TYPES[column["type"]],
                    f"Product type mismatch: {name}.{column['name']}",
                )
                if not column["nullable"]:
                    require(
                        table[column["name"]].null_count == 0,
                        f"Required column has nulls: {name}.{column['name']}",
                    )
    require(artifact_rows["order_fulfillment"] == orders, "Not every order was published")
    require(all(artifact_rows[name] > 0 for name in PUBLIC_PRODUCTS), "Empty core product")
    metrics = manifest["metrics"]
    require(metrics["source_completeness"]["state"] == "pass", "Incomplete source population")
    require(metrics["source_completeness"]["missing"] == 0, "Missing expected event IDs")
    require(metrics["duplicate_records"] > 0, "Duplicate fixture was not exercised")
    require(metrics["records_rejected"] > 0, "Malformed fixture was not exercised")
    quarantine_rows = len(Path(manifest["quarantine_path"]).read_text().splitlines())
    require(quarantine_rows == metrics["records_rejected"], "Quarantine count mismatch")
    require(
        all(q["state"] in {"pass", "not_applicable"} for q in manifest["quality"]),
        "A product quality check did not pass",
    )
    replay = invoke_cli(
        "ingest",
        "--input",
        str(work / "events.jsonl"),
        "--lake",
        str(work / "lake"),
        "--control",
        str(work / "events.control.json"),
        "--as-of",
        manifest["as_of"],
    )
    require(replay["reused"] is True, "Exact replay recomputed rather than reused the release")
    require(replay["logical_hash"] == initial["logical_hash"], "Replay changed logical output")
    require(head_path.read_bytes() == committed_head, "Replay modified the committed head")
    return {
        "status": "passed",
        "verified_at": datetime.now(UTC).isoformat(),
        "scope": "local CLI, Parquet products, quarantine and exact replay; no AWS execution",
        "python": platform.python_version(),
        "platform": platform.platform(),
        "orders": orders,
        "implementation_hash": manifest["implementation_hash"],
        "logical_hash": manifest["logical_hash"],
        "artifact_rows": artifact_rows,
        "checks": {
            "artifact_checksums": "passed",
            "product_contracts_and_grains": "passed",
            "producer_reconciliation": "passed",
            "quarantine": "passed",
            "exact_replay": "passed",
            "committed_head_unchanged": "passed",
        },
        "metrics": metrics,
        "aws_deployed": False,
        "redshift_measured": False,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--work", type=Path, required=True, help="New scratch directory")
    parser.add_argument("--report", type=Path, required=True, help="New JSON evidence file")
    parser.add_argument("--orders", type=int, default=100)
    args = parser.parse_args()
    if args.orders < 40:
        parser.error("Use at least 40 orders to exercise all lifecycle branches")
    if args.work.exists() or args.report.exists():
        parser.error("Work directory and report must be new; existing evidence is preserved")
    report = verify(args.work, args.orders)
    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
