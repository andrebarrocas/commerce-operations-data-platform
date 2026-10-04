import copy
import json
from datetime import UTC, datetime
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from commerce_platform.generator import generate_events
from commerce_platform.lake import PublicationError, canonicalize, ready_history, run

AS_OF = datetime(2026, 2, 1, tzinfo=UTC)


def write(path, events):
    path.write_text("".join(json.dumps(e) + "\n" for e in events))
    return path


@pytest.fixture
def events():
    return generate_events(12)


def test_replay_and_cross_batch_duplicate(tmp_path, events):
    source = write(tmp_path / "batch.jsonl", events)
    lake = tmp_path / "lake"
    first = run(source, lake, as_of=AS_OF, products=False)
    replay = run(source, lake, as_of=AS_OF, products=False)
    assert replay["reused"]
    assert replay["logical_hash"] == first["logical_hash"]
    duplicate = write(tmp_path / "duplicate.jsonl", events[:5])
    rerun = run(duplicate, lake, as_of=AS_OF, products=False)
    assert rerun["logical_hash"] == first["logical_hash"]
    assert rerun["metrics"]["duplicate_records"] == 5


@pytest.mark.parametrize("failpoint", ["after_silver", "before_commit", "after_commit"])
def test_atomic_failure_and_recovery(tmp_path, events, failpoint):
    source = write(tmp_path / "batch.jsonl", events)
    lake = tmp_path / "lake"
    first = run(source, lake, as_of=AS_OF, products=False)
    more = write(tmp_path / "more.jsonl", generate_events(3, seed=7))
    with pytest.raises(PublicationError):
        run(more, lake, as_of=AS_OF, products=False, failpoint=failpoint)
    head = json.loads((lake / "head.json").read_text())
    assert (head["run_key"] == first["run_key"]) == (failpoint != "after_commit")
    recovered = run(more, lake, as_of=AS_OF, products=False)
    clean = tmp_path / "clean"
    reference = run(
        write(tmp_path / "combined.jsonl", events + generate_events(3, seed=7)),
        clean,
        as_of=AS_OF,
        products=False,
    )
    assert recovered["logical_hash"] == reference["logical_hash"]
    assert recovered["reused"] == (failpoint == "after_commit")


def test_malformed_quarantine_preserves_body(tmp_path, events):
    source = write(tmp_path / "input.jsonl", events)
    with source.open("ab") as stream:
        stream.write(b'{not json}\n{"duplicate":1,"duplicate":2}\n')
    result = run(source, tmp_path / "lake", as_of=AS_OF, products=False)
    assert result["metrics"]["records_rejected"] == 2
    bronze = pq.read_table(result["artifacts"]["bronze"]["path"]).to_pylist()
    assert any(row["body"] == b"{not json}" and not row["valid"] for row in bronze)
    assert len(Path(result["quarantine_path"]).read_text().splitlines()) == 2


def test_conflict_blocks_existing_head(tmp_path, events):
    lake = tmp_path / "lake"
    first = run(write(tmp_path / "first", events), lake, as_of=AS_OF, products=False)
    conflict = copy.deepcopy(next(e for e in events if e["event_type"] == "ORDER_CANCELLED"))
    conflict["payload"]["reason"] = "payment_failure"
    with pytest.raises(PublicationError, match="Identity conflict"):
        run(write(tmp_path / "second", [conflict]), lake, as_of=AS_OF, products=False)
    assert json.loads((lake / "head.json").read_text())["run_key"] == first["run_key"]


def test_correction_availability_and_chain(events):
    original = copy.deepcopy(next(e for e in events if e["event_type"] == "ORDER_CREATED"))
    original["ingestion_timestamp"] = "2026-01-01T01:00:00Z"
    correction = copy.deepcopy(original)
    correction.update(
        event_id="correction-1",
        entity_version=3,
        correction_of_event_id=original["event_id"],
        ingestion_timestamp="2026-01-10T00:00:00Z",
    )
    correction["payload"]["customer_id"] = "corrected-customer"
    before = canonicalize([original, correction], datetime(2026, 1, 5, tzinfo=UTC))[1]
    after = canonicalize([original, correction], AS_OF)[1]
    assert before[0]["event_id"] == original["event_id"]
    assert after[0]["event_id"] == correction["event_id"]
    assert after[0]["effective_version"] == 1
    bad = dict(correction, entity_id="other-order")
    with pytest.raises(PublicationError, match="Invalid correction"):
        canonicalize([original, bad], AS_OF)
    fork = dict(correction, event_id="fork", entity_version=4)
    with pytest.raises(PublicationError, match="Forked"):
        canonicalize([original, correction, fork], AS_OF)


def test_late_parent_repairs_pending_history(events):
    selected = [e for e in events if e["entity_type"] == "order"]
    cancelled = next(e for e in selected if e["event_type"] == "ORDER_CANCELLED")
    created = next(
        e
        for e in selected
        if e["order_id"] == cancelled["order_id"] and e["event_type"] == "ORDER_CREATED"
    )
    before, pending = ready_history(canonicalize([cancelled], AS_OF)[1])
    assert before == [] and pending == 1
    after, pending = ready_history(canonicalize([cancelled, created], AS_OF)[1])
    assert len(after) == 2 and pending == 0


def test_impossible_transition_fails(events):
    history = canonicalize(events, AS_OF)[1]
    shipment = next(e for e in history if e["event_type"] == "SHIPMENT_DISPATCHED")
    invalid = copy.deepcopy(shipment)
    invalid.update(event_id="bad", event_type="DELIVERED", effective_version=2, entity_version=2)
    with pytest.raises(PublicationError, match="Invalid lifecycle"):
        ready_history([shipment, invalid])


def test_bounded_backfill_cannot_replace_global(tmp_path, events):
    source = write(tmp_path / "input", events)
    lake = tmp_path / "lake"
    first = run(source, lake, as_of=AS_OF, products=False)
    backfill = run(source, lake, as_of=AS_OF, start="2026-01-01", end="2026-01-03", products=False)
    assert backfill["bounds"] == ["2026-01-01", "2026-01-03"]
    assert json.loads((lake / "head.json").read_text())["run_key"] == first["run_key"]
    assert len(list((lake / "backfills").glob("*.json"))) == 1
    with pytest.raises(ValueError):
        run(source, lake, as_of=AS_OF, start="2026-01-01", end="2026-03-01")


def test_corrupted_raw_and_output_are_detected(tmp_path, events):
    source = write(tmp_path / "input", events)
    lake = tmp_path / "lake"
    result = run(source, lake, as_of=AS_OF, products=False)
    Path(result["artifacts"]["silver_events"]["path"]).write_bytes(b"corrupt")
    with pytest.raises(PublicationError, match="artifact checksum"):
        run(source, lake, as_of=AS_OF, products=False)


@pytest.mark.parametrize("bad_source", [None, {}, [], 42])
def test_invalid_capture_metadata_is_quarantined(tmp_path, bad_source):
    capture = {
        "_capture_version": "1.0.0",
        "source": bad_source,
        "observed_at": "2026-01-01T00:00:00Z",
        "record_id": "x",
        "payload_base64": "e30=",
        "transport_error": None,
    }
    result = run(
        write(tmp_path / "capture", [capture]), tmp_path / "lake", as_of=AS_OF, products=False
    )
    assert result["metrics"]["records_rejected"] == 1
    assert result["metrics"]["accepted_unique"] == 0


def test_transport_clock_spelling_and_trace_do_not_change_canonical_result(tmp_path, events):
    first = copy.deepcopy(events[0])
    first["schema_version"] = "1.1.0"
    first["trace_id"] = "trace-first"
    first["ingestion_timestamp"] = "2026-01-15T00:00:00Z"
    second = copy.deepcopy(first)
    second["trace_id"] = "trace-second"
    second["ingestion_timestamp"] = "2026-01-15T00:00:00.000000Z"
    a = run(
        write(tmp_path / "a.jsonl", [first, second]), tmp_path / "a", as_of=AS_OF, products=False
    )
    b = run(
        write(tmp_path / "b.jsonl", [second, first]), tmp_path / "b", as_of=AS_OF, products=False
    )
    assert a["logical_hash"] == b["logical_hash"]


def test_known_correction_to_future_time_does_not_restore_superseded_fact(events):
    original = copy.deepcopy(next(e for e in events if e["event_type"] == "ORDER_CREATED"))
    original["event_timestamp"] = "2026-01-01T00:00:00Z"
    original["ingestion_timestamp"] = "2026-01-01T00:00:01Z"
    correction = copy.deepcopy(original)
    correction.update(
        event_id="future-correction",
        entity_version=2,
        correction_of_event_id=original["event_id"],
        event_timestamp="2026-01-01T00:02:00Z",
        ingestion_timestamp="2026-01-01T00:01:00Z",
    )
    history, effective, _ = canonicalize(
        [original, correction], datetime(2026, 1, 1, 0, 1, 30, tzinfo=UTC)
    )
    assert len(history) == 2
    assert effective == []


def test_correction_cannot_reassign_shipment_packages(events):
    dispatch = copy.deepcopy(next(e for e in events if e["event_type"] == "SHIPMENT_DISPATCHED"))
    revised = copy.deepcopy(dispatch)
    revised.update(
        event_id="new-membership", entity_version=20, correction_of_event_id=dispatch["event_id"]
    )
    revised["payload"]["package_ids"] = ["some-other-package"]
    with pytest.raises(PublicationError, match="immutable payload identity"):
        canonicalize([dispatch, revised], AS_OF)
