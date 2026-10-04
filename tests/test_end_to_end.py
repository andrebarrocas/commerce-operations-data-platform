import json
from datetime import UTC, datetime

import pyarrow.parquet as pq

from commerce_platform.cli import write_workload
from commerce_platform.lake import run


def test_complete_product_publication_and_backfill_scope(tmp_path):
    events, control = write_workload(tmp_path / "events.jsonl", 20, 42, faults=True)
    expected = set(json.loads(control.read_text())["expected_event_ids"])
    result = run(
        tmp_path / "events.jsonl",
        tmp_path / "lake",
        as_of=datetime(2026, 2, 1, tzinfo=UTC),
        expected_ids=expected,
    )
    assert len(result["artifacts"]) == 22
    assert result["metrics"]["records_rejected"] > 0
    assert result["metrics"]["duplicate_records"] > 0
    assert result["metrics"]["source_completeness"]["state"] == "pass"
    assert result["metrics"]["accepted_unique"] == len(events)
    assert all(q["state"] in ["pass", "not_applicable"] for q in result["quality"])
    bounded = run(
        tmp_path / "events.jsonl",
        tmp_path / "lake",
        as_of=datetime(2026, 2, 1, tzinfo=UTC),
        start="2026-01-01",
        end="2026-01-02",
    )
    rows = pq.read_table(bounded["artifacts"]["carrier_performance"]["path"]).to_pylist()
    assert all(str(r["metric_date"]) == "2026-01-01" for r in rows)
    assert json.loads((tmp_path / "lake/head.json").read_text())["run_key"] == result["run_key"]


def test_historical_correction_recomputes_products_without_mutating_old_release(tmp_path):
    import copy
    from datetime import timedelta

    events, _ = write_workload(tmp_path / "events.jsonl", 2, 42)
    cutoff = datetime(2026, 2, 1, tzinfo=UTC)
    first = run(tmp_path / "events.jsonl", tmp_path / "lake", as_of=cutoff)
    delivered = next(e for e in events if e["event_type"] == "DELIVERED")
    correction = copy.deepcopy(delivered)
    correction["event_id"] = "delivery-correction"
    correction["entity_version"] = (
        max(e["entity_version"] for e in events if e["entity_id"] == delivered["entity_id"]) + 1
    )
    correction["correction_of_event_id"] = delivered["event_id"]
    corrected_time = datetime.fromisoformat(
        delivered["event_timestamp"].replace("Z", "+00:00")
    ) + timedelta(days=2)
    correction["event_timestamp"] = corrected_time.isoformat().replace("+00:00", "Z")
    correction["ingestion_timestamp"] = "2026-01-20T00:00:00Z"
    (tmp_path / "correction.jsonl").write_text(json.dumps(correction) + "\n")
    changed = run(tmp_path / "correction.jsonl", tmp_path / "lake", as_of=cutoff)
    before = pq.read_table(first["artifacts"]["shipment_lifecycle"]["path"]).to_pylist()[0]
    after = pq.read_table(changed["artifacts"]["shipment_lifecycle"]["path"]).to_pylist()[0]
    assert before["delivered_on_time"] is True
    assert after["delivered_on_time"] is False
    assert after["delivered_at"] == corrected_time
    assert changed["metrics"]["accepted_unique"] == first["metrics"]["accepted_unique"] + 1
    assert changed["metrics"]["effective_records"] == first["metrics"]["effective_records"]
