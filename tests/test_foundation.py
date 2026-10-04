"""Producer and contract boundaries: valid histories and adversarial row cases."""

import copy
import hashlib
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Any

import pytest
from jsonschema import Draft202012Validator, FormatChecker

from commerce_platform.generator import generate_events
from commerce_platform.validation import _event_validator, semantic_hash, validate_event


@pytest.fixture(scope="module")
def workload() -> list[dict[str, Any]]:
    return generate_events(40, seed=42)


def test_seeded_workload_is_reproducible(workload: list[dict[str, Any]]) -> None:
    assert workload == generate_events(40, seed=42)
    assert workload != generate_events(40, seed=43)
    digest = hashlib.sha256("".join(semantic_hash(e) for e in workload).encode()).hexdigest()
    assert digest == "ee8b3d29978a9f6c14f7034c9aed51b1fe4b6be6c84ff2328af126ddc0970221"


def test_every_supported_type_and_version_is_valid(workload: list[dict[str, Any]]) -> None:
    assert {e["event_type"] for e in workload} == {
        "ORDER_CREATED",
        "ORDER_CANCELLED",
        "INVENTORY_RESERVED",
        "INVENTORY_RELEASED",
        "PACKAGE_CREATED",
        "PACKAGE_SCANNED",
        "SHIPMENT_DISPATCHED",
        "SHIPMENT_DELAYED",
        "OUT_FOR_DELIVERY",
        "DELIVERED",
        "DELIVERY_FAILED",
        "RETURN_INITIATED",
    }
    assert {e["schema_version"] for e in workload} == {"1.0.0", "1.1.0"}
    assert len({e["event_id"] for e in workload}) == len(workload)
    for event in workload:
        assert validate_event(event) == [], event


def test_histories_conserve_order_units_and_attempts(workload: list[dict[str, Any]]) -> None:
    by_order: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for event in workload:
        by_order[event["order_id"]].append(event)
    for events in by_order.values():
        created = next(e for e in events if e["event_type"] == "ORDER_CREATED")
        expected_units = sum(line["quantity"] for line in created["payload"]["line_items"])
        reservations = [e for e in events if e["event_type"] == "INVENTORY_RESERVED"]
        assert sum(e["payload"]["quantity"] for e in reservations) == expected_units
        packages = [e for e in events if e["event_type"] == "PACKAGE_CREATED"]
        dispatches = [e for e in events if e["event_type"] == "SHIPMENT_DISPATCHED"]
        if any(e["event_type"] == "ORDER_CANCELLED" for e in events):
            assert not packages and not dispatches
            assert (
                sum(
                    e["payload"]["quantity"]
                    for e in events
                    if e["event_type"] == "INVENTORY_RELEASED"
                )
                == expected_units
            )
            continue
        assert (
            sum(line["quantity"] for e in packages for line in e["payload"]["line_items"])
            == expected_units
        )
        dispatched_ids = [package for e in dispatches for package in e["payload"]["package_ids"]]
        assert set(dispatched_ids) == {e["package_id"] for e in packages}
        assert len(dispatched_ids) == len(set(dispatched_ids))
        for dispatch in dispatches:
            history = sorted(
                (
                    e
                    for e in events
                    if e["entity_type"] == "shipment" and e["entity_id"] == dispatch["entity_id"]
                ),
                key=lambda e: e["entity_version"],
            )
            assert history[0]["event_type"] == "SHIPMENT_DISPATCHED"
            assert history[-1]["event_type"] == "DELIVERED"
            attempts = {
                e["payload"]["delivery_attempt_id"]
                for e in history
                if e["event_type"] == "OUT_FOR_DELIVERY"
            }
            outcomes = [e for e in history if e["event_type"] in {"DELIVERED", "DELIVERY_FAILED"}]
            assert {e["payload"]["delivery_attempt_id"] for e in outcomes} == attempts
            assert len(outcomes) == len(attempts)
        for returned in (e for e in events if e["event_type"] == "RETURN_INITIATED"):
            assert returned["event_timestamp"] > max(
                e["event_timestamp"] for e in events if e["event_type"] == "DELIVERED"
            )


def test_arrival_order_can_be_late_and_out_of_sequence(workload: list[dict[str, Any]]) -> None:
    assert workload == sorted(workload, key=lambda e: (e["ingestion_timestamp"], e["event_id"]))
    histories: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    late = 0
    for event in workload:
        histories[(event["entity_type"], event["entity_id"])].append(event)
        if datetime.fromisoformat(event["ingestion_timestamp"]) - datetime.fromisoformat(
            event["event_timestamp"]
        ) > timedelta(days=2):
            late += 1
    assert late > 0
    assert any(
        [e["entity_version"] for e in rows] != sorted(e["entity_version"] for e in rows)
        for rows in histories.values()
    )
    for rows in histories.values():
        assert sorted(e["entity_version"] for e in rows) == list(range(1, len(rows) + 1))


def test_duplicate_hash_ignores_only_transport_diagnostics(workload: list[dict[str, Any]]) -> None:
    original = copy.deepcopy(workload[0])
    duplicate = copy.deepcopy(original)
    duplicate["ingestion_timestamp"] = "2026-06-01T00:00:00Z"
    duplicate["trace_id"] = "retry-trace"
    assert semantic_hash(original) == semantic_hash(duplicate)
    duplicate["country"] = "DE" if original["country"] != "DE" else "PT"
    assert semantic_hash(original) != semantic_hash(duplicate)
    with pytest.raises(ValueError):
        semantic_hash({"nonfinite": float("nan")})


def test_additive_version_is_deliberate(workload: list[dict[str, Any]]) -> None:
    event = copy.deepcopy(next(e for e in workload if e["schema_version"] == "1.0.0"))
    event["trace_id"] = "new-trace"
    assert validate_event(event)
    event["schema_version"] = "1.1.0"
    assert validate_event(event) == []
    event["schema_version"] = "1.2.0"
    assert validate_event(event)


def test_wire_and_accepted_profiles_are_distinct(workload: list[dict[str, Any]]) -> None:
    event = copy.deepcopy(workload[0])
    event["ingestion_timestamp"] = None
    assert validate_event(event)
    schema = _event_validator().schema
    wire = Draft202012Validator(schema["$defs"]["wire_event"], format_checker=FormatChecker())
    assert wire.is_valid(event)


@pytest.mark.parametrize(
    "field,value",
    [
        ("entity_id", "wrong-aggregate"),
        ("event_timestamp", "2026-02-30T12:00:00Z"),
        ("event_timestamp", "2099-01-01T00:00:00Z"),
        ("region", "US-WA"),
        ("schema_version", "2.0.0"),
        ("payload", {}),
    ],
)
def test_contract_rejects_invalid_fields(
    workload: list[dict[str, Any]], field: str, value: Any
) -> None:
    event = copy.deepcopy(
        next(e for e in workload if e["event_type"] == "ORDER_CREATED" and e["country"] == "PT")
    )
    event[field] = value
    assert validate_event(event)


def test_money_duplicate_lines_and_self_correction_rejected(workload: list[dict[str, Any]]) -> None:
    event = copy.deepcopy(next(e for e in workload if e["event_type"] == "ORDER_CREATED"))
    event["payload"]["order_total_minor"] += 1
    assert any(reason.startswith("money:") for reason in validate_event(event))
    event["payload"]["line_items"].append(copy.deepcopy(event["payload"]["line_items"][0]))
    assert any(reason.startswith("uniqueness:") for reason in validate_event(event))
    event["correction_of_event_id"] = event["event_id"]
    assert any(reason.startswith("correction:") for reason in validate_event(event))


def test_generator_input_boundaries() -> None:
    assert generate_events(0) == []
    with pytest.raises(ValueError):
        generate_events(-1)
    with pytest.raises(ValueError):
        generate_events(1, start=datetime(2026, 1, 1))
    shifted = generate_events(1, start=datetime(2026, 1, 1, tzinfo=timezone(timedelta(hours=1))))
    assert min(e["event_timestamp"] for e in shifted) == "2025-12-31T23:00:00.000000Z"
