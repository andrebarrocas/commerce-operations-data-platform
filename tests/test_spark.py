import json
import os
import subprocess
import sys
from datetime import UTC, datetime
from pathlib import Path

import pyarrow.dataset as ds
import pytest

from commerce_platform.generator import generate_events
from commerce_platform.lake import run


@pytest.mark.spark
@pytest.mark.skipif(
    os.environ.get("RUN_SPARK_TESTS") != "1", reason="Set RUN_SPARK_TESTS=1 with Java 17"
)
def test_actual_spark_job_fractional_durations(tmp_path):
    source = tmp_path / "events.jsonl"
    events = generate_events(12)
    delivered = next(e for e in events if e["event_type"] == "DELIVERED")
    delivered["event_timestamp"] = delivered["event_timestamp"].replace(".000000Z", ".500000Z")
    source.write_text("".join(json.dumps(e) + "\n" for e in events))
    snapshot = run(
        source, tmp_path / "lake", as_of=datetime(2026, 2, 1, tzinfo=UTC), products=False
    )
    root = Path(__file__).resolve().parents[1]
    output = tmp_path / "spark"
    command = [
        sys.executable,
        str(root / "spark/curate.py"),
        "--input",
        snapshot["artifacts"]["silver_events"]["path"],
        "--output",
        str(output),
        "--metrics",
        str(tmp_path / "metrics.json"),
        "--master",
        "local[2]",
    ]
    result = subprocess.run(command, capture_output=True, text=True, timeout=180)
    assert result.returncode == 0, result.stderr[-5000:]
    metrics = json.loads((tmp_path / "metrics.json").read_text())
    assert metrics["engine"] == "Apache Spark"
    assert metrics["runtime_seconds"] > 0
    assert metrics["input_records"] == len(events)
    lines = ds.dataset(output / "fact_order_line", format="parquet").to_table().to_pylist()
    expected = sum(
        len(e["payload"]["line_items"]) for e in events if e["event_type"] == "ORDER_CREATED"
    )
    assert len(lines) == expected
    assert all(r["line_total_minor"] == r["quantity"] * r["unit_price_minor"] for r in lines)
    shipments = ds.dataset(output / "shipment_summary", format="parquet").to_table().to_pylist()
    assert len(shipments) == sum(e["event_type"] == "SHIPMENT_DISPATCHED" for e in events)
    assert all(r["dispatch_to_delivery_seconds"] >= 0 for r in shipments)

    assert any(row["dispatch_to_delivery_seconds"] % 1 == 0.5 for row in shipments)
