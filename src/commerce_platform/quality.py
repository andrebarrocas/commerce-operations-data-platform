"""Configurable data checks; unknown control totals never count as success."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import UTC, datetime, timedelta
from typing import Any


@dataclass(frozen=True)
class CheckResult:
    rule_id: str
    state: str
    failed: int
    checked: int
    severity: str = "error"
    detail: str = ""

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def utc(value: str | datetime) -> datetime:
    parsed = (
        datetime.fromisoformat(value.replace("Z", "+00:00")) if isinstance(value, str) else value
    )
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def evaluate(
    rows: list[dict[str, Any]],
    rules: list[dict[str, Any]],
    *,
    now: datetime,
    references: dict[str, list[dict[str, Any]]] | None = None,
    expected_keys: set[tuple[Any, ...]] | None = None,
) -> list[CheckResult]:
    """Evaluate all rules, fail closed on unknown rule types or absent required columns.

    Nullable values are handled by not_null; ranges and accepted-values ignore null.
    Referential checks are hard publication gates. Upstream pending entities must be
    withheld until resolved; the engine never silently waives FK checks for a grace period.
    """
    results: list[CheckResult] = []
    references = references or {}
    for rule in rules:
        kind = rule["type"]
        columns = rule["columns"]
        parameters = rule.get("parameters", {})
        severity = rule.get("severity", "error")
        keys = [tuple(row.get(c) for c in columns) for row in rows]
        missing_columns = sum(any(c not in row for c in columns) for row in rows)
        failed = 0
        state = "pass"
        detail = ""
        if missing_columns:
            failed = missing_columns
        elif kind == "not_null":
            failed = sum(any(value is None for value in key) for key in keys)
        elif kind in {"unique", "duplicate_event_detection"}:
            failed = len(keys) - len(set(keys))
        elif kind == "duplicate_event":
            identities: dict[tuple[Any, ...], set[Any]] = {}
            for row in rows:
                identity = tuple(row[c] for c in parameters["identity_columns"])
                identities.setdefault(identity, set()).add(
                    row[parameters["semantic_digest_column"]]
                )
            failed = len(rows) - len(identities)
            conflicts = sum(len(values) > 1 for values in identities.values())
            detail = f"duplicate_occurrences={failed}, conflicting_identities={conflicts}"
        elif kind == "accepted_values":
            allowed = parameters["values"]
            failed = sum(any(v is not None and v not in allowed for v in key) for key in keys)
        elif kind == "numeric_range":
            minimum, maximum = parameters.get("minimum"), parameters.get("maximum")
            failed = sum(
                any(
                    v is not None
                    and (
                        (minimum is not None and v < minimum)
                        or (maximum is not None and v > maximum)
                    )
                    for v in key
                )
                for key in keys
            )
        elif kind == "timestamp_sanity":
            future = timedelta(seconds=parameters.get("max_future_seconds", 300))
            reference = parameters.get("reference_column")
            for row in rows:
                for column in columns:
                    if row[column] is not None:
                        value = utc(row[column])
                        if value > utc(now) + future or (
                            reference
                            and row.get(reference) is not None
                            and value < utc(row[reference])
                        ):
                            failed += 1
                            break
        elif kind == "referential_integrity":
            ref = parameters["reference_product"]
            if ref not in references:
                state, detail = "unknown", f"Missing reference dataset: {ref}"
            else:
                valid = {
                    tuple(row[c] for c in parameters["reference_columns"])
                    for row in references[ref]
                }
                failed = sum(key not in valid for key in keys if all(v is not None for v in key))
        elif kind == "completeness":
            if expected_keys is None:
                state, detail = "unknown", "No independently specified expected population"
            elif not expected_keys:
                state = "not_applicable"
            else:
                missing = len(expected_keys - set(keys))
                ratio = (len(expected_keys) - missing) / len(expected_keys)
                failed = missing if ratio < parameters.get("minimum_ratio", 1.0) else 0
                detail = f"expected={len(expected_keys)}, missing={missing}, ratio={ratio:.8f}"
        elif kind == "freshness":
            values = [utc(v) for key in keys for v in key if v is not None]
            if not values:
                state, detail = "unknown", "No observation or expected heartbeat"
            else:
                lag = (utc(now) - max(values)).total_seconds()
                failed = int(
                    lag > parameters.get("max_age_seconds", parameters.get("max_lag_seconds", 900))
                    or lag < -300
                )
                detail = f"lag_seconds={lag}"
        elif kind in {"lifecycle", "lifecycle_transition"}:
            previous: dict[tuple[Any, ...], str] = {}
            group_columns = parameters.get("entity_columns", parameters.get("group_columns"))
            order_column = parameters.get("sequence_column", parameters.get("order_column"))
            transitions = parameters.get("transitions", {})
            if kind == "lifecycle_transition":
                for before_state, after_state in parameters["allowed_transitions"]:
                    transitions.setdefault(before_state, []).append(after_state)
            if group_columns is None or order_column is None:
                raise ValueError("Lifecycle grouping and ordering must be configured")
            for row in sorted(
                rows, key=lambda r: (tuple(r[c] for c in group_columns), r[order_column])
            ):
                group = tuple(row[c] for c in group_columns)
                before = previous.get(group, "__start__")
                value = row[columns[0]]
                if value not in transitions.get(before, []):
                    failed += 1
                previous[group] = value
        else:
            raise ValueError(f"Unsupported quality rule: {kind}")
        if failed:
            state = "fail"
        results.append(CheckResult(rule["id"], state, failed, len(rows), severity, detail))
    return results


def source_completeness(accepted_ids: set[str], expected_ids: set[str] | None) -> dict[str, Any]:
    if expected_ids is None:
        return {"state": "unknown", "ratio": None, "expected": None, "missing": None}
    if not expected_ids:
        return {"state": "not_applicable", "ratio": None, "expected": 0, "missing": 0}
    missing = expected_ids - accepted_ids
    ratio = 1 - len(missing) / len(expected_ids)
    return {
        "state": "pass" if ratio >= 0.995 else "fail",
        "ratio": ratio,
        "expected": len(expected_ids),
        "missing": len(missing),
    }
