"""Evidence-driven history gates: missing evidence vs proven contradiction."""

import copy
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest

from commerce_platform.generator import generate_events
from commerce_platform.lake import canonicalize
from commerce_platform.semantics import SemanticError, reconcile

AS_OF = datetime(2026, 2, 1, tzinfo=UTC)


@pytest.fixture
def events() -> list[dict[str, Any]]:
    return generate_events(40)


def resolved(events: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], int]:
    audit, effective, _ = canonicalize(events, AS_OF)
    return reconcile(audit, effective)


def test_generated_histories_are_coherent(events: list[dict[str, Any]]) -> None:
    ready, pending = resolved(events)
    assert pending == 0
    assert len(ready) == len(events)


def test_missing_interior_version_is_pending_and_recovers(events: list[dict[str, Any]]) -> None:
    missing = next(
        e for e in events if e["event_type"] == "PACKAGE_SCANNED" and e["entity_version"] == 2
    )
    ready, pending = resolved([e for e in events if e["event_id"] != missing["event_id"]])
    assert pending > 0
    assert not any(e["entity_id"] == missing["entity_id"] for e in ready)
    # The dependent shipment cannot be published with an incomplete package.
    dispatches = [
        e
        for e in events
        if e["event_type"] == "SHIPMENT_DISPATCHED"
        and missing["package_id"] in e["payload"]["package_ids"]
    ]
    assert not any(e["entity_id"] in {d["entity_id"] for d in dispatches} for e in ready)
    assert resolved(events)[1] == 0


def test_revision_versions_are_not_false_lifecycle_gaps(events: list[dict[str, Any]]) -> None:
    target = next(
        e for e in events if e["event_type"] == "PACKAGE_SCANNED" and e["entity_version"] == 2
    )
    revision = copy.deepcopy(target)
    revision.update(
        event_id="corrected-scan",
        correction_of_event_id=target["event_id"],
        entity_version=1
        + max(e["entity_version"] for e in events if e["entity_id"] == target["entity_id"]),
        ingestion_timestamp="2026-01-20T00:00:00Z",
    )
    revision["payload"]["scan_location_id"] = "corrected-location"
    ready, pending = resolved(events + [revision])
    assert pending == 0
    assert len(ready) == len(events)
    assert any(e["event_id"] == "corrected-scan" and e["effective_version"] == 2 for e in ready)


def test_unknown_parent_is_pending(events: list[dict[str, Any]]) -> None:
    missing = next(e for e in events if e["event_type"] == "ORDER_CREATED")
    ready, pending = resolved([e for e in events if e["event_id"] != missing["event_id"]])
    assert pending > 0
    assert not any(e["order_id"] == missing["order_id"] for e in ready)


def test_cross_order_package_is_a_contradiction(events: list[dict[str, Any]]) -> None:
    dispatch = next(e for e in events if e["event_type"] == "SHIPMENT_DISPATCHED")
    package = next(
        e
        for e in events
        if e["event_type"] == "PACKAGE_CREATED" and e["order_id"] != dispatch["order_id"]
    )
    dispatch["payload"]["package_ids"] = [package["package_id"]]
    with pytest.raises(SemanticError, match="cross_order_package"):
        resolved(events)


def test_over_release_rejected(events: list[dict[str, Any]]) -> None:
    release = next(e for e in events if e["event_type"] == "INVENTORY_RELEASED")
    release["payload"]["quantity"] += 1
    with pytest.raises(SemanticError, match="inventory_overrelease"):
        resolved(events)


def test_partial_release_can_repeat(events: list[dict[str, Any]]) -> None:
    release = next(
        e
        for e in events
        if e["event_type"] == "INVENTORY_RELEASED" and e["payload"]["quantity"] >= 2
    )
    second = copy.deepcopy(release)
    release["payload"]["quantity"] -= 1
    second.update(event_id="second-partial-release", entity_version=3)
    second["payload"]["quantity"] = 1
    ready, pending = resolved(events + [second])
    assert pending == 0
    assert len(ready) == len(events) + 1


def test_attempt_outcome_must_match_open_attempt(events: list[dict[str, Any]]) -> None:
    delivered = next(e for e in events if e["event_type"] == "DELIVERED")
    delivered["payload"]["delivery_attempt_id"] = "other-attempt"
    with pytest.raises(SemanticError, match="outcome_without_matching_attempt"):
        resolved(events)


def test_unknown_line_is_invalid_when_order_is_known(events: list[dict[str, Any]]) -> None:
    reserve = next(e for e in events if e["event_type"] == "INVENTORY_RESERVED")
    for event in events:
        if event["entity_id"] == reserve["entity_id"]:
            event["payload"]["line_item_id"] = "does-not-exist"
    with pytest.raises(SemanticError, match="unknown_order_line"):
        resolved(events)


def test_delay_can_preserve_an_active_attempt(events: list[dict[str, Any]]) -> None:
    delivered = next(e for e in events if e["event_type"] == "DELIVERED")
    out = next(
        e
        for e in events
        if e["entity_id"] == delivered["entity_id"]
        and e["event_type"] == "OUT_FOR_DELIVERY"
        and e["payload"]["delivery_attempt_id"] == delivered["payload"]["delivery_attempt_id"]
    )
    delay = copy.deepcopy(delivered)
    delay.update(
        event_id="active-attempt-delay",
        event_type="SHIPMENT_DELAYED",
        event_timestamp=out["event_timestamp"],
        entity_version=delivered["entity_version"],
    )
    delay["payload"] = {"reason": "address", "revised_eta": delivered["event_timestamp"]}
    delivered["entity_version"] += 1
    ready, pending = resolved(events + [delay])
    assert pending == 0
    assert len(ready) == len(events) + 1


def test_return_cannot_predate_known_delivery(events: list[dict[str, Any]]) -> None:
    returned = next(e for e in events if e["event_type"] == "RETURN_INITIATED")
    created = next(
        e
        for e in events
        if e["event_type"] == "ORDER_CREATED" and e["order_id"] == returned["order_id"]
    )
    returned["event_timestamp"] = created["event_timestamp"]
    with pytest.raises(SemanticError, match="return_before_delivery"):
        resolved(events)


def test_missing_dispatch_package_is_pending(events: list[dict[str, Any]]) -> None:
    dispatch = next(e for e in events if e["event_type"] == "SHIPMENT_DISPATCHED")
    dispatch["payload"]["package_ids"] = ["not-yet-observed-package"]
    ready, pending = resolved(events)
    assert pending > 0
    assert not any(e["entity_id"] == dispatch["entity_id"] for e in ready)


def allocation_workload() -> list[dict[str, Any]]:
    """Two order lines, incomplete first-line reservation, and created packages."""
    events = generate_events(2)
    target_order = next(e["order_id"] for e in events if e["event_type"] == "PACKAGE_CREATED")
    events = [
        e
        for e in events
        if e["order_id"] == target_order
        and e["event_type"] in {"ORDER_CREATED", "INVENTORY_RESERVED", "PACKAGE_CREATED"}
    ]
    order = next(e for e in events if e["event_type"] == "ORDER_CREATED")
    order["payload"]["line_items"][0]["quantity"] = 10
    order["payload"]["line_items"][1]["quantity"] = 2
    order["payload"]["order_total_minor"] = sum(
        line["quantity"] * line["unit_price_minor"] for line in order["payload"]["line_items"]
    )
    for event in events:
        if event["event_type"] == "INVENTORY_RESERVED":
            event["payload"]["quantity"] = 5 if event["payload"]["line_item_id"] == "line-1" else 2
        if event["event_type"] == "PACKAGE_CREATED":
            line = event["payload"]["line_items"][0]
            line["quantity"] = 4 if line["line_item_id"] == "line-1" else 2
    return events


def test_deficient_package_cohort_has_no_input_order_winner() -> None:
    events = allocation_workload()
    first = next(
        e
        for e in events
        if e["event_type"] == "PACKAGE_CREATED"
        and e["payload"]["line_items"][0]["line_item_id"] == "line-1"
    )
    second = copy.deepcopy(first)
    second.update(
        event_id="other-package-event", entity_id="other-package", package_id="other-package"
    )
    second["payload"]["line_items"][0]["quantity"] = 3
    audit, effective, _ = canonicalize(events + [second], AS_OF)
    ready, pending = reconcile(audit, effective)
    assert pending == 2
    assert not any(e["entity_id"] in {first["entity_id"], second["entity_id"]} for e in ready)
    assert reconcile(list(reversed(audit)), list(reversed(effective))) == (ready, pending)


def test_pending_multiline_package_does_not_consume_other_line_capacity() -> None:
    events = allocation_workload()
    events = [
        e
        for e in events
        if not (
            e["event_type"] == "INVENTORY_RESERVED" and e["payload"]["line_item_id"] == "line-2"
        )
    ]
    first = next(
        e
        for e in events
        if e["event_type"] == "PACKAGE_CREATED"
        and e["payload"]["line_items"][0]["line_item_id"] == "line-1"
    )
    second = next(
        e
        for e in events
        if e["event_type"] == "PACKAGE_CREATED"
        and e["payload"]["line_items"][0]["line_item_id"] == "line-2"
    )
    missing_line = copy.deepcopy(second["payload"]["line_items"][0])
    missing_line["quantity"] = 1
    first["payload"]["line_items"].append(missing_line)
    second["payload"]["line_items"] = [{**first["payload"]["line_items"][0], "quantity": 5}]
    ready, pending = resolved(events)
    assert pending == 1
    assert first["event_id"] not in {e["event_id"] for e in ready}
    assert second["event_id"] in {e["event_id"] for e in ready}
    audit, effective, _ = canonicalize(events, AS_OF)
    assert reconcile(list(reversed(audit)), list(reversed(effective))) == (ready, pending)


def test_release_before_package_reduces_allocation() -> None:
    events = allocation_workload()
    package = next(
        e
        for e in events
        if e["event_type"] == "PACKAGE_CREATED"
        and e["payload"]["line_items"][0]["line_item_id"] == "line-1"
    )
    reserve = next(
        e
        for e in events
        if e["event_type"] == "INVENTORY_RESERVED" and e["payload"]["line_item_id"] == "line-1"
    )
    release = copy.deepcopy(reserve)
    before = datetime.fromisoformat(package["event_timestamp"]) - timedelta(minutes=1)
    release.update(
        event_id="release-before-packing",
        event_type="INVENTORY_RELEASED",
        entity_version=2,
        event_timestamp=before.isoformat().replace("+00:00", "Z"),
        ingestion_timestamp=before.isoformat().replace("+00:00", "Z"),
    )
    release["payload"].update(quantity=5, reason="allocation_changed")
    ready, pending = resolved(events + [release])
    assert pending == 1
    assert package["event_id"] not in {e["event_id"] for e in ready}


def test_later_release_does_not_retroactively_invalidate_packing() -> None:
    events = generate_events(2)
    dispatch = next(e for e in events if e["event_type"] == "SHIPMENT_DISPATCHED")
    reserve = next(
        e
        for e in events
        if e["event_type"] == "INVENTORY_RESERVED" and e["order_id"] == dispatch["order_id"]
    )
    release = copy.deepcopy(reserve)
    after = datetime.fromisoformat(dispatch["event_timestamp"]) + timedelta(minutes=1)
    release.update(
        event_id="release-after-dispatch",
        event_type="INVENTORY_RELEASED",
        entity_version=2,
        event_timestamp=after.isoformat().replace("+00:00", "Z"),
        ingestion_timestamp=after.isoformat().replace("+00:00", "Z"),
    )
    release["payload"]["reason"] = "allocation_changed"
    ready, pending = resolved(events + [release])
    assert pending == 0
    assert len(ready) == len(events) + 1


def test_two_reservations_cannot_overallocate_one_order_line() -> None:
    events = generate_events(2)
    original = next(e for e in events if e["event_type"] == "INVENTORY_RESERVED")
    cloned = copy.deepcopy(original)
    cloned.update(event_id="duplicate-allocation", entity_id="second-reservation")
    cloned["payload"]["reservation_id"] = "second-reservation"
    with pytest.raises(SemanticError, match="inventory_overreservation"):
        resolved(events + [cloned])


def test_order_quantity_contradiction_fails_even_when_reservations_missing() -> None:
    events = allocation_workload()
    first = next(
        e
        for e in events
        if e["event_type"] == "PACKAGE_CREATED"
        and e["payload"]["line_items"][0]["line_item_id"] == "line-1"
    )
    first["payload"]["line_items"][0]["quantity"] = 6
    second = copy.deepcopy(first)
    second.update(
        event_id="excess-package-event", entity_id="excess-package", package_id="excess-package"
    )
    second["payload"]["line_items"][0]["quantity"] = 5
    with pytest.raises(SemanticError, match="packaged_units_exceed_order"):
        resolved(events + [second])
