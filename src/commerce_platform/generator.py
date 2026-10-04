"""Deterministic synthetic commerce histories, with simulated arrival disorder.

No real identifiers or data are used. Arrival clocks here are fixture inputs, not
measurements of managed infrastructure latency. Lists are returned in arrival
order, while entity versions retain the source's business sequence.
"""

import random
import uuid
from datetime import UTC, datetime, timedelta
from typing import Any

from commerce_platform.references import CARRIERS, FACILITIES, SKUS

DEFAULT_START = datetime(2026, 1, 1, tzinfo=UTC)
_CURRENCIES = {"PT": "EUR", "DE": "EUR", "US": "USD", "GB": "GBP", "JP": "JPY"}


def _utc(value: datetime) -> str:
    return value.astimezone(UTC).isoformat(timespec="microseconds").replace("+00:00", "Z")


def generate_events(
    orders: int, seed: int = 42, start: datetime | None = None
) -> list[dict[str, Any]]:
    """Generate independent orders and complete causal histories in arrival order.

    Cancellation, delay, retry and return branches are selected by order index,
    so a 40-order workload covers every supported event type for any seed.
    The seed controls monetary amounts, volumes, route choices and timing.
    """
    if isinstance(orders, bool) or not isinstance(orders, int) or orders < 0:
        raise ValueError("orders must be a non-negative integer")
    origin = DEFAULT_START if start is None else start
    if origin.tzinfo is None or origin.utcoffset() is None:
        raise ValueError("start must be timezone aware")
    origin = origin.astimezone(UTC)
    rng = random.Random(seed)
    events: list[dict[str, Any]] = []
    versions: dict[tuple[str, str], int] = {}

    for order_index in range(orders):
        order_id = f"order-{seed}-{order_index:08d}"
        order_time = origin + timedelta(seconds=order_index * 30)
        facility = FACILITIES[order_index % len(FACILITIES)]
        destination = FACILITIES[
            (order_index + (1 if order_index % 6 == 0 else 0)) % len(FACILITIES)
        ]
        carrier = CARRIERS[order_index % len(CARRIERS)]
        country, region = destination["country"], destination["region"]
        promise = order_time + timedelta(hours=60)
        sku_rows = rng.sample(SKUS, 1 + order_index % 3)
        lines = [
            {
                "line_item_id": f"line-{line_index + 1}",
                "sku_id": sku["sku_id"],
                "quantity": rng.randint(1, 4),
                "unit_price_minor": rng.randint(400, 25000),
            }
            for line_index, sku in enumerate(sku_rows)
        ]

        def emit(
            event_type: str,
            entity_type: str,
            entity_id: str,
            at: datetime,
            payload: dict[str, Any],
            *,
            package_id: str | None = None,
            shipment_id: str | None = None,
            use_facility: bool = False,
            use_carrier: bool = False,
            _context: tuple[str, int, str, str, str, str] = (
                order_id,
                order_index,
                facility["facility_id"],
                carrier["carrier_id"],
                country,
                region,
            ),
        ) -> None:
            active_order, active_index, facility_key, carrier_key, dest_country, dest_region = (
                _context
            )
            key = (entity_type, entity_id)
            version = versions.get(key, 0) + 1
            versions[key] = version
            arrival_delay = rng.randint(1, 120)
            # Deliberate finite tail: sometimes child records arrive before parent
            # records, or a record arrives beyond the 48-hour reconciliation target.
            if rng.random() < 0.03:
                arrival_delay += 3 * 86400
            schema_version = "1.1.0" if (active_index + version) % 5 == 0 else "1.0.0"
            event: dict[str, Any] = {
                "event_id": str(
                    uuid.uuid5(
                        uuid.NAMESPACE_URL,
                        f"synthetic-commerce/{seed}/{entity_type}/{entity_id}/{version}",
                    )
                ),
                "event_type": event_type,
                "event_timestamp": _utc(at),
                "ingestion_timestamp": _utc(at + timedelta(seconds=arrival_delay)),
                "schema_version": schema_version,
                "producer_id": "synthetic-python-v1",
                "entity_type": entity_type,
                "entity_id": entity_id,
                "entity_version": version,
                "correction_of_event_id": None,
                "order_id": active_order,
                "package_id": package_id,
                "shipment_id": shipment_id,
                "facility_id": facility_key if use_facility else None,
                "carrier_id": carrier_key if use_carrier else None,
                "country": dest_country,
                "region": dest_region,
                "payload": payload,
            }
            if schema_version == "1.1.0":
                event["trace_id"] = f"trace-{active_order}"
            events.append(event)

        emit(
            "ORDER_CREATED",
            "order",
            order_id,
            order_time,
            {
                "customer_id": f"customer-{seed}-{order_index // 2:08d}",
                "currency": _CURRENCIES[country],
                "order_total_minor": sum(
                    line["quantity"] * line["unit_price_minor"] for line in lines
                ),
                "promised_delivery_at": _utc(promise),
                "line_items": lines,
            },
        )
        for line_index, line in enumerate(lines):
            reservation_id = f"reservation-{order_id}-{line_index}"
            emit(
                "INVENTORY_RESERVED",
                "reservation",
                reservation_id,
                order_time + timedelta(minutes=2, seconds=line_index),
                {
                    "reservation_id": reservation_id,
                    **{k: v for k, v in line.items() if k != "unit_price_minor"},
                },
                use_facility=True,
            )

        if order_index % 10 == 0:
            emit(
                "ORDER_CANCELLED",
                "order",
                order_id,
                order_time + timedelta(minutes=10),
                {"reason": "customer_request"},
            )
            for line_index, line in enumerate(lines):
                reservation_id = f"reservation-{order_id}-{line_index}"
                emit(
                    "INVENTORY_RELEASED",
                    "reservation",
                    reservation_id,
                    order_time + timedelta(minutes=11, seconds=line_index),
                    {
                        "reservation_id": reservation_id,
                        **{k: v for k, v in line.items() if k != "unit_price_minor"},
                        "reason": "order_cancelled",
                    },
                    use_facility=True,
                )
            continue

        package_ids = [f"package-{order_id}-{line_index}" for line_index in range(len(lines))]
        split = order_index % 4 == 0 and len(lines) > 1
        shipment_groups = [[package_id] for package_id in package_ids] if split else [package_ids]
        for line_index, (line, package_id) in enumerate(zip(lines, package_ids, strict=True)):
            created_at = order_time + timedelta(minutes=30 + line_index * 5)
            emit(
                "PACKAGE_CREATED",
                "package",
                package_id,
                created_at,
                {
                    "line_items": [{k: v for k, v in line.items() if k != "unit_price_minor"}],
                    "weight_grams": sku_rows[line_index]["unit_weight_grams"] * line["quantity"]
                    + 60,
                },
                package_id=package_id,
                use_facility=True,
            )
            for scan_index, scan_type in enumerate(("picked", "packed")):
                emit(
                    "PACKAGE_SCANNED",
                    "package",
                    package_id,
                    created_at + timedelta(minutes=5 + scan_index * 5),
                    {"scan_type": scan_type, "scan_location_id": facility["facility_id"]},
                    package_id=package_id,
                    use_facility=True,
                )

        delivered_times = []
        for shipment_index, group in enumerate(shipment_groups):
            shipment_id = f"shipment-{order_id}-{shipment_index}"
            dispatch_at = order_time + timedelta(
                hours=2, minutes=rng.randint(0, 90) + shipment_index * 5
            )
            shipment_kwargs: dict[str, Any] = {
                "shipment_id": shipment_id,
                "use_facility": True,
                "use_carrier": True,
            }
            emit(
                "SHIPMENT_DISPATCHED",
                "shipment",
                shipment_id,
                dispatch_at,
                {"package_ids": group, "promised_delivery_at": _utc(promise)},
                **shipment_kwargs,
            )
            delivery_day = 4 if order_index % 5 == 0 else 2
            out_at = order_time + timedelta(days=delivery_day, hours=shipment_index)
            if order_index % 5 == 0:
                emit(
                    "SHIPMENT_DELAYED",
                    "shipment",
                    shipment_id,
                    dispatch_at + timedelta(hours=8),
                    {"reason": "weather", "revised_eta": _utc(out_at + timedelta(hours=2))},
                    **shipment_kwargs,
                )
            for package_id in group:
                emit(
                    "PACKAGE_SCANNED",
                    "package",
                    package_id,
                    dispatch_at + timedelta(hours=5),
                    {"scan_type": "hub_arrival", "scan_location_id": f"hub-{country}-01"},
                    package_id=package_id,
                    shipment_id=shipment_id,
                    use_facility=True,
                )
            attempt = 1
            attempt_id = f"attempt-{shipment_id}-{attempt}"
            emit(
                "OUT_FOR_DELIVERY",
                "shipment",
                shipment_id,
                out_at,
                {"attempt_number": attempt, "delivery_attempt_id": attempt_id},
                **shipment_kwargs,
            )
            if order_index % 7 == 0:
                emit(
                    "DELIVERY_FAILED",
                    "shipment",
                    shipment_id,
                    out_at + timedelta(minutes=20),
                    {
                        "attempt_number": attempt,
                        "delivery_attempt_id": attempt_id,
                        "reason": "recipient_unavailable",
                    },
                    **shipment_kwargs,
                )
                attempt = 2
                attempt_id = f"attempt-{shipment_id}-{attempt}"
                out_at += timedelta(days=1)
                emit(
                    "OUT_FOR_DELIVERY",
                    "shipment",
                    shipment_id,
                    out_at,
                    {"attempt_number": attempt, "delivery_attempt_id": attempt_id},
                    **shipment_kwargs,
                )
            delivered_at = out_at + timedelta(minutes=30 + rng.randint(0, 60))
            emit(
                "DELIVERED",
                "shipment",
                shipment_id,
                delivered_at,
                {"attempt_number": attempt, "delivery_attempt_id": attempt_id},
                **shipment_kwargs,
            )
            delivered_times.append(delivered_at)

        if order_index % 8 == 0:
            return_id = f"return-{order_id}"
            emit(
                "RETURN_INITIATED",
                "return",
                return_id,
                max(delivered_times) + timedelta(days=2),
                {
                    "return_id": return_id,
                    "line_items": [
                        {
                            "line_item_id": lines[0]["line_item_id"],
                            "sku_id": lines[0]["sku_id"],
                            "quantity": 1,
                        }
                    ],
                    "reason": "unwanted",
                },
            )

    return sorted(events, key=lambda event: (event["ingestion_timestamp"], event["event_id"]))
