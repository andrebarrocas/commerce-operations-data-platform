"""Bounded local lakehouse with immutable evidence and atomic manifest publication.

This module intentionally does not emulate S3 transactions. fcntl + os.replace
are single-host guarantees. It scans retained raw history, not unbounded streams.
"""

from __future__ import annotations

import base64
import fcntl
import hashlib
import json
import os
import time
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from commerce_platform.quality import evaluate, source_completeness, utc
from commerce_platform.validation import semantic_hash, validate_event

ROOT = Path(__file__).resolve().parents[2]


class PublicationError(RuntimeError):
    """An attempted release failed; the previous committed head remains authoritative."""


def encoded(value: Any) -> bytes:
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), default=str, allow_nan=False
    ).encode()


def digest(value: Any) -> str:
    return hashlib.sha256(encoded(value)).hexdigest()


def atomic_json(path: Path, value: Any) -> None:
    temp = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
    with temp.open("wb") as stream:
        stream.write(encoded(value))
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(temp, path)
    directory = os.open(path.parent, os.O_RDONLY)
    try:
        os.fsync(directory)
    finally:
        os.close(directory)


def sync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def sync_tree(path: Path) -> None:
    """Persist files and directory entries before publishing their manifest."""
    for file in path.rglob("*"):
        if file.is_file():
            with file.open("rb") as stream:
                os.fsync(stream.fileno())
    for directory in sorted(
        (p for p in path.rglob("*") if p.is_dir()), key=lambda p: len(p.parts), reverse=True
    ):
        sync_directory(directory)
    sync_directory(path)
    sync_directory(path.parent)


def _pairs(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"Duplicate JSON field: {key}")
        result[key] = value
    return result


def _canonical_observation(event: dict[str, Any]) -> dict[str, Any]:
    output = {key: value for key, value in event.items() if key != "trace_id"}
    output["ingestion_timestamp"] = (
        utc(event["ingestion_timestamp"]).isoformat(timespec="microseconds").replace("+00:00", "Z")
    )
    return output


def canonicalize(
    events: list[dict[str, Any]], as_of: datetime
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], int]:
    """Filter availability FIRST, then resolve exact retries and linear revisions."""
    identities: dict[str, dict[str, Any]] = {}
    duplicates = 0
    for event in events:
        if utc(event["ingestion_timestamp"]) > as_of:
            continue
        key = event["event_id"]
        previous = identities.get(key)
        if previous is not None:
            if semantic_hash(previous) != semantic_hash(event):
                raise PublicationError(f"Identity conflict: {key}")
            duplicates += 1
            if utc(event["ingestion_timestamp"]) < utc(previous["ingestion_timestamp"]):
                identities[key] = _canonical_observation(event)
        else:
            identities[key] = _canonical_observation(event)
    versions: set[tuple[str, str, int]] = set()
    successors: dict[str, str] = {}
    for key, event in identities.items():
        version = (event["entity_type"], event["entity_id"], event["entity_version"])
        if version in versions:
            raise PublicationError(f"Conflicting aggregate version: {version}")
        versions.add(version)
        target = event.get("correction_of_event_id")
        if target is None:
            continue
        if target not in identities:
            raise PublicationError(f"Unresolved correction target: {target}")
        old = identities[target]
        immutable = [
            "entity_type",
            "entity_id",
            "event_type",
            "order_id",
            "package_id",
            "shipment_id",
            "facility_id",
            "carrier_id",
            "country",
            "region",
        ]
        if (
            any(event[k] != old[k] for k in immutable)
            or event["entity_version"] <= old["entity_version"]
        ):
            raise PublicationError(f"Invalid correction relationship: {key}")
        payload_identity_fields = {
            "SHIPMENT_DISPATCHED": ["package_ids"],
            "INVENTORY_RESERVED": ["reservation_id", "line_item_id", "sku_id"],
            "INVENTORY_RELEASED": ["reservation_id", "line_item_id", "sku_id"],
            "RETURN_INITIATED": ["return_id"],
        }
        for field in payload_identity_fields.get(event["event_type"], []):
            old_value, new_value = old["payload"][field], event["payload"][field]
            if field == "package_ids":
                old_value, new_value = sorted(old_value), sorted(new_value)
            if old_value != new_value:
                raise PublicationError(f"Correction changes immutable payload identity: {field}")
        if "line_items" in old["payload"]:
            old_lines = {
                (line["line_item_id"], line["sku_id"]) for line in old["payload"]["line_items"]
            }
            new_lines = {
                (line["line_item_id"], line["sku_id"]) for line in event["payload"]["line_items"]
            }
            if old_lines != new_lines:
                raise PublicationError("Correction changes immutable line membership")
        if target in successors:
            raise PublicationError(f"Forked correction chain: {target}")
        successors[target] = key
    effective = []
    for key, event in identities.items():
        if event.get("correction_of_event_id") is not None:
            continue
        tail = key
        seen = {tail}
        while tail in successors:
            tail = successors[tail]
            if tail in seen:
                raise PublicationError("Cyclic correction chain")
            seen.add(tail)
        current = dict(identities[tail])
        current["effective_version"] = event["entity_version"]
        current["root_event_id"] = key
        if utc(current["event_timestamp"]) <= as_of:
            effective.append(current)
    return (
        sorted(identities.values(), key=lambda e: e["event_id"]),
        sorted(effective, key=lambda e: e["event_id"]),
        duplicates,
    )


def ready_history(events: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], int]:
    """Convenience for unrevised histories; the pipeline supplies full audit history."""
    from commerce_platform.semantics import SemanticError, reconcile

    try:
        return reconcile(events, events)
    except SemanticError as error:
        raise PublicationError(f"Invalid lifecycle or business evidence: {error}") from error


def _parquet_events(path: Path, events: list[dict[str, Any]]) -> None:
    columns = [
        "event_id",
        "event_type",
        "event_timestamp",
        "ingestion_timestamp",
        "schema_version",
        "producer_id",
        "entity_type",
        "entity_id",
        "order_id",
        "package_id",
        "shipment_id",
        "facility_id",
        "carrier_id",
        "country",
        "region",
        "correction_of_event_id",
        "root_event_id",
    ]
    schema = pa.schema(
        [(c, pa.string()) for c in columns]
        + [
            ("entity_version", pa.int64()),
            ("effective_version", pa.int64()),
            ("payload_json", pa.string()),
        ]
    )
    rows = [
        {
            **{c: e.get(c) for c in columns},
            "entity_version": e["entity_version"],
            "effective_version": e.get("effective_version", e["entity_version"]),
            "payload_json": encoded(e["payload"]).decode(),
        }
        for e in events
    ]
    pq.write_table(pa.Table.from_pylist(rows, schema=schema), path, compression="zstd")


def _fingerprint() -> str:
    paths = (
        sorted((ROOT / "contracts").rglob("*.json"))
        + sorted((ROOT / "src").rglob("*.py"))
        + sorted((ROOT / "sql").rglob("*.sql"))
    )
    return digest(
        [(str(p.relative_to(ROOT)), hashlib.sha256(p.read_bytes()).hexdigest()) for p in paths]
    )


def run(
    input_path: Path,
    lake: Path,
    *,
    as_of: datetime,
    start: str | None = None,
    end: str | None = None,
    expected_ids: set[str] | None = None,
    failpoint: str | None = None,
    products: bool = True,
) -> dict[str, Any]:
    """Retain an input batch, rebuild, validate, then atomically publish one release.

    Bounds select a separate as-of snapshot namespace, never the current global head.
    Full history is still scanned. Output filtering is performed by each artifact's
    declared temporal column after products have seen complete entity history.
    """
    began = time.perf_counter()
    as_of = utc(as_of)
    if (start is None) != (end is None):
        raise ValueError("Both start and end are required")
    if start and end:
        lower, upper = utc(start), utc(end)
        if not timedelta(0) < upper - lower <= timedelta(days=31):
            raise ValueError("Backfills require a positive half-open range of at most 31 days")
    lake.mkdir(parents=True, exist_ok=True)
    (lake / "raw").mkdir(exist_ok=True)
    (lake / "runs").mkdir(exist_ok=True)
    (lake / "backfills").mkdir(exist_ok=True)
    head = lake / "head.json"
    if start and end:
        head = lake / "backfills" / f"{digest([start, end])[:20]}.json"
    with (lake / ".publisher.lock").open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        data = input_path.read_bytes()
        input_hash = hashlib.sha256(data).hexdigest()
        raw_path = lake / "raw" / f"{input_hash}.jsonl"
        if not raw_path.exists():
            # Unique hash name makes repeated copies harmless; raw commits before outputs.
            temp = lake / "raw" / f".{uuid.uuid4().hex}.tmp"
            with temp.open("wb") as stream:
                stream.write(data)
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temp, raw_path)
            sync_directory(raw_path.parent)
        sources = sorted((lake / "raw").glob("*.jsonl"))
        source_hashes = [p.stem for p in sources]
        for path in sources:
            if hashlib.sha256(path.read_bytes()).hexdigest() != path.stem:
                raise PublicationError(f"Raw checksum mismatch: {path.name}")
        implementation_hash = _fingerprint()
        run_key = digest(
            {
                "inputs": source_hashes,
                "as_of": as_of.isoformat(),
                "bounds": [start, end],
                "implementation": implementation_hash,
                "expected": sorted(expected_ids) if expected_ids is not None else None,
                "products": products,
            }
        )
        if head.exists():
            current = json.loads(head.read_text())
            if current["run_key"] == run_key:
                # Content-addressing is only safe while referenced output is intact.
                for artifact in current["artifacts"].values():
                    if (
                        hashlib.sha256(Path(artifact["path"]).read_bytes()).hexdigest()
                        != artifact["sha256"]
                    ):
                        raise PublicationError("Committed artifact checksum mismatch")
                return {**current, "reused": True}
        attempt = lake / "runs" / f"{run_key[:16]}-{uuid.uuid4().hex[:8]}"
        attempt.mkdir()
        accepted: list[dict[str, Any]] = []
        bronze = []
        quarantine = []
        for path in sources:
            for line_number, body in enumerate(path.read_bytes().splitlines(), 1):
                errors: list[str]
                event: Any = None
                locator = f"{path.name}:{line_number}"
                try:
                    event = json.loads(body, object_pairs_hook=_pairs)
                    if isinstance(event, dict) and "_capture_version" in event:
                        capture = event
                        if capture["_capture_version"] != "1.0.0":
                            raise ValueError("Unsupported capture version")
                        if not isinstance(capture.get("source"), str) or not capture["source"]:
                            raise ValueError("Capture source must be a nonempty string")
                        if not isinstance(capture.get("observed_at"), str):
                            raise ValueError("Capture observed_at must be a UTC timestamp string")
                        locator = capture["source"]
                        body = base64.b64decode(capture["payload_base64"], validate=True)
                        if capture["transport_error"] is not None:
                            raise ValueError(f"transport: {capture['transport_error']}")
                        event = json.loads(body, object_pairs_hook=_pairs)
                        if (
                            not isinstance(event, dict)
                            or event.get("event_id") != capture["record_id"]
                        ):
                            raise ValueError("Capture record_id differs from business event_id")
                        event["ingestion_timestamp"] = capture["observed_at"]
                    errors = validate_event(event)
                except (ValueError, UnicodeDecodeError, KeyError, TypeError) as error:
                    errors = [f"malformed_or_transport: {error}"]
                row = {
                    "source": locator,
                    "body": body,
                    "valid": not errors,
                    "reasons": encoded(errors).decode(),
                }
                bronze.append(row)
                if errors:
                    quarantine.append(
                        {
                            "source": locator,
                            "body_base64": base64.b64encode(body).decode(),
                            "reasons": errors,
                        }
                    )
                else:
                    accepted.append(event)
        bronze_schema = pa.schema(
            [
                ("source", pa.string()),
                ("body", pa.binary()),
                ("valid", pa.bool_()),
                ("reasons", pa.string()),
            ]
        )
        pq.write_table(
            pa.Table.from_pylist(bronze, schema=bronze_schema),
            attempt / "bronze.parquet",
            compression="zstd",
        )
        (attempt / "quarantine.jsonl").write_bytes(b"".join(encoded(q) + b"\n" for q in quarantine))
        try:
            history, effective, duplicate_count = canonicalize(accepted, as_of)
            from commerce_platform.semantics import SemanticError, reconcile

            original_effective = effective
            try:
                effective, pending_count = reconcile(history, effective)
            except SemanticError as error:
                raise PublicationError(str(error)) from error
            ready_ids = {e["event_id"] for e in effective}
            pending_events = [e for e in original_effective if e["event_id"] not in ready_ids]
            semantic_quarantine = [
                {
                    "event_id": e["event_id"],
                    "reason": "missing_evidence_or_version",
                    "state": "overdue"
                    if (as_of - utc(e["ingestion_timestamp"])).total_seconds() > 172800
                    else "pending",
                    "first_observed_at": e["ingestion_timestamp"],
                }
                for e in pending_events
            ]
            (attempt / "semantic_quarantine.jsonl").write_bytes(
                b"".join(encoded(q) + b"\n" for q in semantic_quarantine)
            )
            _parquet_events(attempt / "silver_history.parquet", history)
            _parquet_events(attempt / "silver_events.parquet", effective)
            if failpoint == "after_silver":
                raise PublicationError("Injected stage failure after Silver before Gold")
            artifacts: dict[str, dict[str, Any]] = {
                "bronze": {
                    "path": str((attempt / "bronze.parquet").resolve()),
                    "row_count": len(bronze),
                },
                "silver_history": {
                    "path": str((attempt / "silver_history.parquet").resolve()),
                    "row_count": len(history),
                },
                "silver_events": {
                    "path": str((attempt / "silver_events.parquet").resolve()),
                    "row_count": len(effective),
                },
            }
            quality_start = time.perf_counter()
            quality: list[dict[str, Any]] = []
            if products:
                from commerce_platform.products import build_products

                artifacts.update(build_products(effective, attempt / "gold", as_of))
                quality_start = time.perf_counter()
                quality = _product_checks(artifacts, effective, as_of)
                if any(
                    r["state"] in {"fail", "unknown"} and r["severity"] == "error" for r in quality
                ):
                    atomic_json(attempt / "quality.json", quality)
                    raise PublicationError(
                        "Product quality gate failed; inspect attempt quality.json"
                    )
            quality_seconds = time.perf_counter() - quality_start
            if start and end:
                _filter_backfill(artifacts, lower, upper)
            for artifact in artifacts.values():
                path = Path(artifact["path"])
                artifact["sha256"] = hashlib.sha256(path.read_bytes()).hexdigest()
                artifact["bytes"] = path.stat().st_size
                artifact["cost_metadata"] = {
                    "storage_bytes": artifact["bytes"],
                    "compute_usd": None,
                    "currency": "USD",
                    "status": "storage_measured_cost_unmeasured",
                }
            logical_hash = digest(
                {
                    name: pq.read_table(a["path"]).to_pylist()
                    for name, a in artifacts.items()
                    if name != "bronze"
                }
            )
            duration = time.perf_counter() - began
            report = {
                "run_key": run_key,
                "implementation_hash": implementation_hash,
                "logical_hash": logical_hash,
                "as_of": as_of.isoformat(),
                "published_at": datetime.now(UTC).isoformat(),
                "bounds": [start, end],
                "sources": source_hashes,
                "artifacts": artifacts,
                "quality": quality,
                "quarantine_path": str((attempt / "quarantine.jsonl").resolve()),
                "semantic_quarantine_path": str((attempt / "semantic_quarantine.jsonl").resolve()),
                "metrics": {
                    "records_processed": len(bronze),
                    "records_rejected": len(quarantine),
                    "accepted_unique": len(history),
                    "effective_records": len(effective),
                    "duplicate_records": duplicate_count,
                    "pending_records": pending_count,
                    "overdue_pending_records": sum(
                        q["state"] == "overdue" for q in semantic_quarantine
                    ),
                    "late_records": sum(
                        (utc(e["ingestion_timestamp"]) - utc(e["event_timestamp"])).total_seconds()
                        > 172800
                        for e in history
                    ),
                    "processing_seconds": duration,
                    "quality_seconds": quality_seconds,
                    "quality_state": "pass",
                    "freshness_state": "not_evaluated_fixture_clock",
                    "retries": 0,
                    "failures": 0,
                    "records_per_second": len(bronze) / duration if duration else 0,
                    "unique_events_per_second": len(history) / duration if duration else 0,
                    "bytes_read": sum(p.stat().st_size for p in sources),
                    "bytes_written": sum(a["bytes"] for a in artifacts.values()),
                    "source_completeness": source_completeness(
                        {e["event_id"] for e in history}, expected_ids
                    ),
                    "latency_clock": "local simulated observation; duration uses actual monotonic time",
                },
                "reused": False,
            }
            sync_tree(attempt)
            atomic_json(attempt / "manifest.json", report)
            if failpoint == "before_commit":
                raise PublicationError("Injected failure before publication")
            atomic_json(head, report)
            if failpoint == "after_commit":
                raise PublicationError("Injected lost acknowledgement after publication")
            return report
        except Exception as error:
            atomic_json(
                attempt / "failure.json",
                {
                    "error": str(error),
                    "head": str(head),
                    "duration_seconds": time.perf_counter() - began,
                },
            )
            raise


def _product_checks(
    artifacts: dict[str, dict[str, Any]], events: list[dict[str, Any]], now: datetime
) -> list[dict[str, Any]]:
    from commerce_platform.references import CARRIERS, FACILITIES, SKUS

    references = {name: pq.read_table(a["path"]).to_pylist() for name, a in artifacts.items()}
    references.update(
        {
            "silver.orders": [e for e in events if e["event_type"] == "ORDER_CREATED"],
            "silver.facilities": FACILITIES,
            "silver.carriers": CARRIERS,
            "silver.skus": SKUS,
            "silver.order_lines": [
                {"order_id": e["order_id"], **line}
                for e in events
                if e["event_type"] == "ORDER_CREATED"
                for line in e["payload"]["line_items"]
            ],
        }
    )
    references.update({f"gold.{name}": rows for name, rows in list(references.items())})
    checks = []
    for file in sorted((ROOT / "contracts/products").glob("*.json")):
        contract = json.loads(file.read_text())
        name = contract["name"]
        rows = references[name]
        primary = contract["primary_key"]
        # Expected population is independently derived from upstream events, not output rows.
        expected = expected_population(name, events)
        rules = contract["quality_rules"] + [
            {
                "id": "all_required_columns",
                "type": "not_null",
                "columns": [c["name"] for c in contract["columns"] if not c["nullable"]],
            }
        ]
        for result in evaluate(rows, rules, now=now, references=references, expected_keys=expected):
            checks.append({"product": name, **result.to_dict()})
        if any(tuple(row.get(c) for c in primary) not in expected for row in rows):
            checks.append(
                {
                    "product": name,
                    "rule_id": "unexpected_keys",
                    "state": "fail",
                    "severity": "error",
                    "failed": 1,
                    "checked": len(rows),
                }
            )
    return checks


def expected_population(name: str, events: list[dict[str, Any]]) -> set[tuple[Any, ...]]:
    if name == "order_fulfillment":
        return {(e["order_id"],) for e in events if e["event_type"] == "ORDER_CREATED"}
    if name == "shipment_lifecycle":
        return {(e["shipment_id"],) for e in events if e["event_type"] == "SHIPMENT_DISPATCHED"}
    if name == "inventory_movements":
        return {
            (e["event_id"], e["payload"]["line_item_id"])
            for e in events
            if e["event_type"].startswith("INVENTORY_")
        }
    if name == "facility_performance":
        return {
            (e["facility_id"], utc(e["event_timestamp"]).replace(minute=0, second=0, microsecond=0))
            for e in events
            if e["event_type"]
            in {
                "INVENTORY_RESERVED",
                "INVENTORY_RELEASED",
                "PACKAGE_CREATED",
                "SHIPMENT_DISPATCHED",
            }
        }
    if name == "carrier_performance":
        return {
            (e["carrier_id"], e["country"], utc(e["event_timestamp"]).date())
            for e in events
            if e["event_type"]
            in {"SHIPMENT_DISPATCHED", "OUT_FOR_DELIVERY", "DELIVERED", "DELIVERY_FAILED"}
        }
    if name == "delivery_reliability":
        return {
            (e["country"], e["region"], utc(e["event_timestamp"]).date())
            for e in events
            if e["event_type"] in {"DELIVERED", "DELIVERY_FAILED"}
        }
    raise ValueError(f"Unknown product: {name}")


def _filter_backfill(artifacts: dict[str, dict[str, Any]], start: datetime, end: datetime) -> None:
    # Preserve complete entity snapshots and evidence; bounds apply only to windowed
    # products. This is explicitly a window-product backfill, not source truncation.
    for name in ("facility_performance", "carrier_performance", "delivery_reliability"):
        if name not in artifacts:
            continue
        path = Path(artifacts[name]["path"])
        table = pq.read_table(path)
        time_column = "window_start" if name == "facility_performance" else "metric_date"
        rows = [r for r in table.to_pylist() if start <= utc(str(r[time_column])) < end]
        pq.write_table(pa.Table.from_pylist(rows, schema=table.schema), path, compression="zstd")
        artifacts[name]["row_count"] = len(rows)
