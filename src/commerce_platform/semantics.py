"""History-level reconciliation after structural validation and identity resolution.

Missing evidence is pending. Evidence that proves a contradiction fails closed.
These checks operate on a retained-history snapshot, not an unbounded stream.
"""

import json
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any

from commerce_platform.quality import utc
from commerce_platform.references import CARRIERS, FACILITIES, SKUS

Event = dict[str, Any]
Group = tuple[str, str]
ROOT = Path(__file__).resolve().parents[2]


class SemanticError(ValueError):
    """A complete available history contains contradictory business evidence."""


def _group(event: Event) -> Group:
    return event["entity_type"], event["entity_id"]


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise SemanticError(message)


def reconcile(history: list[Event], effective: list[Event]) -> tuple[list[Event], int]:
    """Return consistent available aggregate histories and pending event count.

    ``history`` includes superseded revisions. ``effective`` carries the root
    lifecycle slot as effective_version; revisions do not create lifecycle gaps.
    Call canonical identity/correction resolution before this function.
    """
    configuration = json.loads((ROOT / "contracts/lifecycle.json").read_text())
    groups: dict[Group, list[Event]] = defaultdict(list)
    versions: dict[Group, set[int]] = defaultdict(set)
    for event in history:
        versions[_group(event)].add(event["entity_version"])
    for event in effective:
        groups[_group(event)].append(event)
    pending: set[Group] = set()
    for key, events in groups.items():
        observed = versions[key]
        # min/max/count establishes contiguous unique positive source versions
        # without allocating a huge range when a malformed source jumps ahead.
        if not observed or min(observed) != 1 or len(observed) != max(observed):
            pending.add(key)
            continue
        events.sort(
            key=lambda e: int(
                e["effective_version"] if "effective_version" in e else e["entity_version"]
            )
        )
        before = "__start__"
        previous_time: datetime | None = None
        for event in events:
            _require(
                event["event_type"] in configuration[key[0]].get(before, []),
                f"lifecycle:{key}: {before} -> {event['event_type']}",
            )
            timestamp = utc(event["event_timestamp"])
            _require(
                previous_time is None or timestamp >= previous_time,
                f"causal_timestamp_regression:{key}",
            )
            previous_time, before = timestamp, event["event_type"]
        first = events[0]
        for event in events[1:]:
            stable = ("order_id", "country", "region", "facility_id", "carrier_id")
            _require(
                all(event[field] == first[field] for field in stable),
                f"aggregate_identity_changed:{key}",
            )
        if key[0] == "shipment":
            _attempts(events)
        if key[0] == "package":
            stages = configuration["package_scan_order"]
            scan_stages = [
                stages[e["payload"]["scan_type"]]
                for e in events
                if e["event_type"] == "PACKAGE_SCANNED"
            ]
            _require(scan_stages == sorted(scan_stages), f"package_scan_regression:{key}")
        if key[0] == "reservation":
            reserve = first["payload"]
            _require(
                all(
                    e["payload"]["line_item_id"] == reserve["line_item_id"]
                    and e["payload"]["sku_id"] == reserve["sku_id"]
                    for e in events
                ),
                f"reservation_line_changed:{key}",
            )
            released = sum(
                e["payload"]["quantity"] for e in events if e["event_type"] == "INVENTORY_RELEASED"
            )
            _require(released <= reserve["quantity"], f"inventory_overrelease:{key}")

    orders = {e["order_id"]: e for e in effective if e["event_type"] == "ORDER_CREATED"}
    lines = {
        (order, line["line_item_id"]): line
        for order, event in orders.items()
        for line in event["payload"]["line_items"]
    }
    facilities = {row["facility_id"] for row in FACILITIES}
    skus = {row["sku_id"] for row in SKUS}
    carriers = {row["carrier_id"]: row for row in CARRIERS}
    for key, events in groups.items():
        if key in pending:
            continue
        first = events[0]
        order_id = first["order_id"]
        order = orders.get(order_id)
        if order is None or ("order", order_id) in pending:
            pending.add(key)
            continue
        for event in events:
            _require(
                (event["country"], event["region"]) == (order["country"], order["region"]),
                f"destination_differs_from_order:{key}",
            )
            if event["facility_id"] is not None:
                _require(event["facility_id"] in facilities, f"unknown_facility:{key}")
            if event["carrier_id"] is not None:
                _require(event["carrier_id"] in carriers, f"unknown_carrier:{key}")
                _require(
                    event["country"] in carriers[event["carrier_id"]]["supported_countries"],
                    f"unsupported_carrier_destination:{key}",
                )
            payload_lines = event["payload"].get("line_items", [])
            if key[0] == "reservation":
                payload_lines = [event["payload"]]
            for line in payload_lines:
                _require(line["sku_id"] in skus, f"unknown_sku:{key}")
                parent = lines.get((order_id, line["line_item_id"]))
                _require(parent is not None, f"unknown_order_line:{key}:{line['line_item_id']}")
                if parent is not None:
                    _require(line["sku_id"] == parent["sku_id"], f"line_sku_mismatch:{key}")
                    _require(
                        line["quantity"] <= parent["quantity"], f"line_quantity_exceeds_order:{key}"
                    )

    reservations: dict[tuple[str, str, str], list[Group]] = defaultdict(list)
    reservation_changes: dict[tuple[str, str], dict[datetime, int]] = defaultdict(
        lambda: defaultdict(int)
    )
    for key, events in groups.items():
        if key[0] == "reservation" and events[0]["event_type"] == "INVENTORY_RESERVED":
            first = events[0]
            reservations[
                (first["order_id"], first["facility_id"], first["payload"]["line_item_id"])
            ].append(key)
            if key not in pending:
                line_key = (first["order_id"], first["payload"]["line_item_id"])
                for event in events:
                    delta = event["payload"]["quantity"] * (
                        1 if event["event_type"] == "INVENTORY_RESERVED" else -1
                    )
                    reservation_changes[line_key][utc(event["event_timestamp"])] += delta
    for line_key, changes in reservation_changes.items():
        balance = 0
        for _, delta in sorted(changes.items()):
            balance += delta
            _require(
                0 <= balance <= lines[line_key]["quantity"],
                f"inventory_overreservation:{line_key}",
            )
    package_created = {
        e["package_id"]: e for e in effective if e["event_type"] == "PACKAGE_CREATED"
    }
    packaged_units: dict[tuple[str, str], int] = defaultdict(int)
    for event in package_created.values():
        for line in event["payload"]["line_items"]:
            parent_line = (event["order_id"], line["line_item_id"])
            if parent_line in lines:
                packaged_units[parent_line] += line["quantity"]
    for line_key, quantity in packaged_units.items():
        _require(quantity <= lines[line_key]["quantity"], f"packaged_units_exceed_order:{line_key}")

    Allocation = tuple[str, str, str]

    def balance_at(allocation: Allocation, at: datetime) -> int:
        # Releases are evaluated at packing time. Later release evidence cannot
        # retroactively remove the allocation that existed when a package formed.
        return sum(
            event["payload"]["quantity"]
            * (1 if event["event_type"] == "INVENTORY_RESERVED" else -1)
            for reservation in reservations.get(allocation, [])
            for event in groups[reservation]
            if utc(event["event_timestamp"]) <= at
        )

    # Decide whole-package dependencies before consuming any line capacity. A
    # package missing one line's reservation must not block a ready package on a
    # different line merely because dictionary iteration visited that line first.
    for package_id, event in package_created.items():
        key = ("package", package_id)
        if key in pending:
            continue
        created_at = utc(event["event_timestamp"])
        for line in event["payload"]["line_items"]:
            allocation = (event["order_id"], event["facility_id"], line["line_item_id"])
            candidates = reservations.get(allocation, [])
            if (
                not candidates
                or any(group in pending for group in candidates)
                or line["quantity"] > balance_at(allocation, created_at)
            ):
                pending.add(key)
                break

    # A deficient reservation cohort is held together: there is no arbitrary
    # winner selected by UUID or input arrival. Equal-time packages are one demand
    # cohort. Suppression propagates conservatively across multi-line packages.
    allocations: dict[Allocation, list[tuple[datetime, Group, int]]] = defaultdict(list)
    for package_id, event in package_created.items():
        key = ("package", package_id)
        if key in pending:
            continue
        for line in event["payload"]["line_items"]:
            allocation = (event["order_id"], event["facility_id"], line["line_item_id"])
            allocations[allocation].append((utc(event["event_timestamp"]), key, line["quantity"]))
    deficient: set[Group] = set()
    for allocation, demand in allocations.items():
        by_time: dict[datetime, int] = defaultdict(int)
        for timestamp, _, quantity in demand:
            by_time[timestamp] += quantity
        cumulative = 0
        for timestamp, quantity in sorted(by_time.items()):
            cumulative += quantity
            if cumulative > balance_at(allocation, timestamp):
                deficient.update(group for _, group, _ in demand)
                break
    pending.update(deficient)

    dispatches = {
        e["shipment_id"]: e for e in effective if e["event_type"] == "SHIPMENT_DISPATCHED"
    }
    memberships: dict[str, str] = {}
    cancelled = {e["order_id"] for e in effective if e["event_type"] == "ORDER_CANCELLED"}
    for shipment_id, dispatch in dispatches.items():
        key = ("shipment", shipment_id)
        if key in pending:
            continue
        _require(
            dispatch["order_id"] not in cancelled,
            f"cancelled_order_dispatched:{dispatch['order_id']}",
        )
        for package_id in dispatch["payload"]["package_ids"]:
            package = package_created.get(package_id)
            if package is None:
                pending.add(key)
                continue
            _require(
                package["order_id"] == dispatch["order_id"],
                f"cross_order_package:{shipment_id}:{package_id}",
            )
            _require(
                package["facility_id"] == dispatch["facility_id"],
                f"cross_facility_package:{shipment_id}:{package_id}",
            )
            _require(
                utc(package["event_timestamp"]) <= utc(dispatch["event_timestamp"]),
                f"dispatch_before_package:{shipment_id}",
            )
            _require(
                package_id not in memberships or memberships[package_id] == shipment_id,
                f"package_dispatched_twice:{package_id}",
            )
            memberships[package_id] = shipment_id
            if ("package", package_id) in pending:
                pending.add(key)

    deliveries: dict[tuple[str, str], list[tuple[datetime, int]]] = defaultdict(list)
    for key, events in groups.items():
        if key[0] != "shipment" or key in pending:
            continue
        delivered = next((e for e in events if e["event_type"] == "DELIVERED"), None)
        if delivered is None:
            continue
        for package_id in events[0]["payload"]["package_ids"]:
            for line in package_created[package_id]["payload"]["line_items"]:
                deliveries[(delivered["order_id"], line["line_item_id"])].append(
                    (utc(delivered["event_timestamp"]), line["quantity"])
                )
    returned: dict[tuple[str, str], int] = defaultdict(int)
    returns = sorted(
        (e for e in effective if e["event_type"] == "RETURN_INITIATED"),
        key=lambda e: (e["event_timestamp"], e["event_id"]),
    )
    for event in returns:
        key = _group(event)
        if key in pending:
            continue
        _require(
            event["order_id"] not in cancelled, f"cancelled_order_returned:{event['order_id']}"
        )
        for line in event["payload"]["line_items"]:
            line_key = (event["order_id"], line["line_item_id"])
            requested = returned[line_key] + line["quantity"]
            _require(
                requested <= lines[line_key]["quantity"], f"returns_exceed_ordered_units:{line_key}"
            )
            known = deliveries[line_key]
            delivered_before = sum(
                quantity
                for timestamp, quantity in known
                if timestamp <= utc(event["event_timestamp"])
            )
            if requested > delivered_before:
                _require(
                    requested > sum(quantity for _, quantity in known),
                    f"return_before_delivery:{line_key}",
                )
                pending.add(key)
            else:
                returned[line_key] = requested

    ready = [event for event in effective if _group(event) not in pending]
    return sorted(ready, key=lambda e: e["event_id"]), len(effective) - len(ready)


def _attempts(events: list[Event]) -> None:
    active: tuple[str, int] | None = None
    seen: set[str] = set()
    last_number = 0
    for event in events:
        kind = event["event_type"]
        if kind == "OUT_FOR_DELIVERY":
            payload = event["payload"]
            attempt = (payload["delivery_attempt_id"], payload["attempt_number"])
            _require(
                active is None and attempt[0] not in seen and attempt[1] == last_number + 1,
                f"invalid_delivery_attempt:{event['entity_id']}",
            )
            active, last_number = attempt, attempt[1]
            seen.add(attempt[0])
        elif kind in {"DELIVERED", "DELIVERY_FAILED"}:
            attempt = (event["payload"]["delivery_attempt_id"], event["payload"]["attempt_number"])
            _require(active == attempt, f"outcome_without_matching_attempt:{event['entity_id']}")
            active = None
