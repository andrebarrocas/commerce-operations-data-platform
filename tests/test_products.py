"""SQL behavior tests at declared grains, temporal boundaries and contract types."""

import copy
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq
import pytest

from commerce_platform.generator import generate_events
from commerce_platform.products import PUBLIC_PRODUCTS, ROOT, build_products
from commerce_platform.references import FACILITIES

CUTOFF = datetime(2026, 2, 1, tzinfo=UTC)


def read(result: dict[str, Any], name: str) -> list[dict[str, Any]]:
    return pq.read_table(result[name]["path"]).to_pylist()


def parsed(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def timestamp(value: datetime) -> str:
    return value.isoformat().replace("+00:00", "Z")


def test_six_contract_schemas_and_grains(tmp_path: Path) -> None:
    events = generate_events(16)
    result = build_products(events, tmp_path, CUTOFF)
    assert len(result) == 19
    for name in PUBLIC_PRODUCTS:
        contract = json.loads((ROOT / "contracts" / "products" / f"{name}.json").read_text())
        table = pq.read_table(result[name]["path"])
        assert table.column_names == [column["name"] for column in contract["columns"]]
        rows = table.to_pylist()
        keys = [tuple(row[key] for key in contract["primary_key"]) for row in rows]
        assert len(keys) == len(set(keys))
        assert all(row["updated_at"] == CUTOFF for row in rows)
        for column in contract["columns"]:
            if not column["nullable"]:
                assert table[column["name"]].null_count == 0
    orders = read(result, "order_fulfillment")
    assert len(orders) == 16
    assert all(row["is_complete"] for row in orders)
    assert sum(row["line_total_minor"] for row in read(result, "fact_order_line")) == sum(
        row["order_total_minor"] for row in orders
    )
    locations = {row["facility_id"]: (row["country"], row["region"]) for row in FACILITIES}
    assert all(
        (row["country"], row["region"]) == locations[row["facility_id"]]
        for row in read(result, "facility_performance")
    )


def test_fulfillment_uses_units_not_delivered_shipment_count(tmp_path: Path) -> None:
    events = generate_events(2)
    created = next(
        event
        for event in events
        if event["event_type"] == "ORDER_CREATED" and event["order_id"].endswith("00000001")
    )
    # Only some ordered units have been packaged. Every existing shipment is
    # delivered, which must not falsely complete the whole order.
    created["payload"]["line_items"][0]["quantity"] += 1
    created["payload"]["order_total_minor"] += created["payload"]["line_items"][0][
        "unit_price_minor"
    ]
    result = build_products(events, tmp_path, CUTOFF)
    order = next(
        row for row in read(result, "order_fulfillment") if row["order_id"] == created["order_id"]
    )
    assert order["delivered_shipments"] == order["shipments_dispatched"]
    assert not order["is_complete"]
    assert order["fulfillment_status"] == "partially_delivered"


def test_point_in_time_features_exclude_unavailable_outcomes(tmp_path: Path) -> None:
    events = generate_events(2)
    dispatched = next(event for event in events if event["event_type"] == "SHIPMENT_DISPATCHED")
    cutoff = parsed(dispatched["event_timestamp"]) + timedelta(hours=1)
    # Normalize fixture arrivals so this assertion focuses on the explicit
    # future-availability event rather than generator's independent late tail.
    for event in events:
        event["ingestion_timestamp"] = event["event_timestamp"]
    delivered = next(event for event in events if event["event_type"] == "DELIVERED")
    delivered["event_timestamp"] = timestamp(cutoff - timedelta(minutes=1))
    delivered["ingestion_timestamp"] = timestamp(cutoff + timedelta(days=1))
    result = build_products(events, tmp_path / "before", cutoff)
    shipment = read(result, "shipment_lifecycle")[0]
    assert shipment["delivered_at"] is None
    feature = read(result, "features_delivery")[0]
    assert feature["shipment_id"] == dispatched["shipment_id"]
    assert feature["feature_as_of"] == cutoff
    assert feature["package_scan_gap_minutes"] is not None
    assert feature["carrier_delay_rate_30d"] is None
    # A product-level arrival filter cannot resolve a correction's predecessor;
    # the lake's historical-as_of tests separately prove that chain behavior.
    after = build_products(events, tmp_path / "after", CUTOFF)
    assert read(after, "shipment_lifecycle")[0]["delivered_at"] is not None
    assert read(after, "features_delivery") == []


def test_correction_logical_slot_controls_state_and_rerun_order(tmp_path: Path) -> None:
    events = generate_events(2)
    dispatch = next(event for event in events if event["event_type"] == "SHIPMENT_DISPATCHED")
    dispatch["effective_version"] = dispatch["entity_version"]
    dispatch["entity_version"] = 100
    dispatch["correction_of_event_id"] = "earlier-dispatch-revision"
    dispatch["event_id"] = "corrected-dispatch"
    first = build_products(events, tmp_path / "first", CUTOFF)
    second = build_products(list(reversed(copy.deepcopy(events))), tmp_path / "second", CUTOFF)
    assert read(first, "shipment_lifecycle")[0]["state"] == "delivered"
    for name in first:
        assert pq.read_table(first[name]["path"]).equals(pq.read_table(second[name]["path"]))


def test_failure_rates_use_outcome_cohorts_and_window_keys(tmp_path: Path) -> None:
    events = generate_events(16)
    # Attempt identity is scoped by shipment, so two shipments can share a local token.
    for event in events:
        if "delivery_attempt_id" in event["payload"]:
            event["payload"]["delivery_attempt_id"] = "attempt-" + str(
                event["payload"]["attempt_number"]
            )
    result = build_products(events, tmp_path, CUTOFF)
    outcomes = [
        event for event in events if event["event_type"] in {"DELIVERED", "DELIVERY_FAILED"}
    ]
    assert any(event["event_type"] == "DELIVERY_FAILED" for event in outcomes)
    assert sum(
        row["completed_delivery_attempts"] for row in read(result, "carrier_performance")
    ) == len(outcomes)
    for row in read(result, "delivery_reliability"):
        matching = [
            event
            for event in outcomes
            if event["country"] == row["country"]
            and event["region"] == row["region"]
            and parsed(event["event_timestamp"]).date() == row["metric_date"]
        ]
        failed = sum(event["event_type"] == "DELIVERY_FAILED" for event in matching)
        assert row["completed_delivery_attempts"] == len(matching)
        assert row["failed_delivery_rate"] == pytest.approx(failed / len(matching))
    expected_facility_keys = {
        (
            event["facility_id"],
            parsed(event["event_timestamp"]).replace(minute=0, second=0, microsecond=0),
        )
        for event in events
        if event["event_type"]
        in {"INVENTORY_RESERVED", "INVENTORY_RELEASED", "PACKAGE_CREATED", "SHIPMENT_DISPATCHED"}
    }
    assert {
        (row["facility_id"], row["window_start"]) for row in read(result, "facility_performance")
    } == expected_facility_keys


def test_empty_history_is_typed_and_duplicate_input_fails(tmp_path: Path) -> None:
    result = build_products([], tmp_path / "empty", CUTOFF)
    assert all(result[name]["row_count"] == 0 for name in PUBLIC_PRODUCTS)
    events = generate_events(1)
    with pytest.raises(ValueError, match="unique canonical"):
        build_products(events + [events[0]], tmp_path / "duplicate", CUTOFF)
    with pytest.raises(ValueError, match="timezone-aware"):
        build_products(events, tmp_path / "naive", datetime(2026, 2, 1))


def test_diagnostic_trace_does_not_change_feature_snapshot(tmp_path: Path) -> None:
    events = generate_events(2)
    for event in events:
        event["schema_version"] = "1.1.0"
        event["trace_id"] = "first-delivery-diagnostic"
    replay = copy.deepcopy(events)
    for event in replay:
        event["trace_id"] = "retry-diagnostic"
    first = build_products(events, tmp_path / "first", CUTOFF)
    repeated = build_products(replay, tmp_path / "retry", CUTOFF)
    assert first["features_inventory"]["row_count"] > 0
    for name in first:
        assert pq.read_table(first[name]["path"]).equals(pq.read_table(repeated[name]["path"]))
