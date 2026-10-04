"""Versioned structural checks and event-local business invariants.

History-dependent lifecycle, quantity conservation and references belong to the
reconciliation step. This module does not pretend row validation can prove them.
"""

import hashlib
import json
from datetime import datetime, timedelta
from functools import lru_cache
from pathlib import Path
from typing import Any

from jsonschema import Draft202012Validator, FormatChecker

CONTRACTS_ROOT = Path(__file__).resolve().parents[2] / "contracts"
FORMAT_CHECKER = FormatChecker()


@FORMAT_CHECKER.checks("date-time", raises=(ValueError, TypeError))
def _valid_timestamp(value: Any) -> bool:
    """Enforce dates without relying on jsonschema's optional format extras."""
    if not isinstance(value, str):
        return True  # The schema type check owns non-string errors.
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return parsed.tzinfo is not None


@lru_cache(maxsize=1)
def _event_validator() -> Draft202012Validator:
    schema = json.loads((CONTRACTS_ROOT / "event.schema.json").read_text())
    Draft202012Validator.check_schema(schema)
    return Draft202012Validator(schema, format_checker=FORMAT_CHECKER)


def semantic_hash(event: dict[str, Any]) -> str:
    """Hash immutable business content; transport clocks and tracing are excluded.

    Unknown fields remain in the digest instead of disappearing. Callers must
    validate before treating any event as accepted. NaN/Infinity fail explicitly.
    """
    semantic = {
        key: value for key, value in event.items() if key not in {"ingestion_timestamp", "trace_id"}
    }
    canonical = json.dumps(
        semantic, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _timestamp(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def validate_event(event: dict[str, Any]) -> list[str]:
    """Return stable readable violations; an empty list means row-local validity."""
    violations = []
    errors = sorted(
        _event_validator().iter_errors(event), key=lambda error: "/".join(map(str, error.path))
    )
    for error in errors:
        path = "/".join(map(str, error.path)) or "$"
        violations.append(f"schema:{path}:{error.message}")
    if violations:
        return violations

    payload = event["payload"]
    identity = {
        "order": event["order_id"],
        "package": event["package_id"],
        "shipment": event["shipment_id"],
        "reservation": payload.get("reservation_id"),
        "return": payload.get("return_id"),
    }[event["entity_type"]]
    if event["entity_id"] != identity:
        violations.append("identity:entity_id:must equal the declared aggregate identifier")
    if event["correction_of_event_id"] == event["event_id"]:
        violations.append("correction:correction_of_event_id:self-reference is invalid")
    if not event["region"].startswith(event["country"] + "-"):
        violations.append("geography:region:must start with the destination country and a hyphen")

    event_time = _timestamp(event["event_timestamp"])
    ingress_time = _timestamp(event["ingestion_timestamp"])
    if event_time > ingress_time + timedelta(minutes=5):
        violations.append("timestamp:event_timestamp:more than five minutes after trusted ingress")

    lines = payload.get("line_items", [])
    line_keys = [line["line_item_id"] for line in lines]
    if len(set(line_keys)) != len(line_keys):
        violations.append(
            "uniqueness:payload.line_items:line_item_id must be unique within the event"
        )
    if event["event_type"] == "ORDER_CREATED":
        total = sum(line["quantity"] * line["unit_price_minor"] for line in lines)
        if total != payload["order_total_minor"]:
            violations.append(
                "money:payload.order_total_minor:does not equal the sum of line amounts"
            )
        if _timestamp(payload["promised_delivery_at"]) < event_time:
            violations.append("timestamp:payload.promised_delivery_at:precedes order creation")
    if (
        event["event_type"] == "SHIPMENT_DELAYED"
        and _timestamp(payload["revised_eta"]) < event_time
    ):
        violations.append("timestamp:payload.revised_eta:precedes the delay observation")
    if event["event_type"] == "SHIPMENT_DISPATCHED":
        package_id = event["package_id"]
        if package_id is not None and package_id not in payload["package_ids"]:
            violations.append("reference:package_id:must be listed in dispatch payload package_ids")
    return violations
