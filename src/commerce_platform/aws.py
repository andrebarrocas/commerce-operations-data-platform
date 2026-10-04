"""AWS transport adapters; pure fake-client tests do not emulate managed services.

The Lambda consumer reports row-local contract signals only. Firehose archives
wire envelopes independently and remains the authoritative batch input.
"""

from __future__ import annotations

import base64
import hashlib
import json
import os
import random
import time
from collections.abc import Callable, Iterable
from datetime import UTC, datetime
from typing import Any

from commerce_platform.observability import emf
from commerce_platform.validation import validate_event

MAX_RECORD_BYTES = 1024 * 1024  # Conservative frame + partition-key application limit.
MAX_BATCH_BYTES = 5 * 1024 * 1024


class ProducerFailure(RuntimeError):
    """The producer exhausted its bounded retry budget; pending IDs are recoverable."""

    def __init__(self, pending_ids: list[str], attempts: int) -> None:
        self.pending_ids = pending_ids
        self.attempts = attempts
        super().__init__(f"Kinesis send failed after {attempts} attempts: {pending_ids}")


def _json(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()


def _pairs(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    output: dict[str, Any] = {}
    for key, value in pairs:
        if key in output:
            raise ValueError(f"Duplicate JSON field: {key}")
        output[key] = value
    return output


def _parse(data: bytes) -> dict[str, Any]:
    def nonfinite(value: str) -> None:
        raise ValueError(f"Nonfinite JSON number: {value}")

    value = json.loads(data.decode("utf-8"), object_pairs_hook=_pairs, parse_constant=nonfinite)
    if not isinstance(value, dict):
        raise ValueError("Expected a JSON object")
    return value


def encode_frame(payload: bytes, record_id: str) -> bytes:
    """Encode arbitrary business bytes without losing their record boundary."""
    if not isinstance(record_id, str) or not 1 <= len(record_id) <= 128:
        raise ValueError("record_id must contain 1..128 characters")
    return _json(
        {
            "transport_version": "1.0.0",
            "record_id": record_id,
            "payload_base64": base64.b64encode(payload).decode("ascii"),
        }
    )


def decode_frame(frame: bytes) -> tuple[str, bytes]:
    value = _parse(frame)
    if set(value) != {"transport_version", "record_id", "payload_base64"}:
        raise ValueError("Unexpected transport envelope fields")
    if value["transport_version"] != "1.0.0":
        raise ValueError("Unsupported transport version")
    record_id = value["record_id"]
    if not isinstance(record_id, str) or not 1 <= len(record_id) <= 128:
        raise ValueError("Invalid record_id")
    if not isinstance(value["payload_base64"], str):
        raise ValueError("payload_base64 must be a string")
    payload = base64.b64decode(value["payload_base64"], validate=True)
    return record_id, payload


def send_events(
    events: Iterable[dict[str, Any]],
    client: Any,
    stream_name: str,
    *,
    max_attempts: int = 5,
    sleep: Callable[[float], None] = time.sleep,
    jitter: Callable[[], float] = random.random,
) -> dict[str, int]:
    """Send <=500-record/5MiB batches; retry failed or ambiguously acknowledged rows.

    Stable order_id partition keys do not promise arrival ordering after a
    partial failure. Consumers must use the contract's entity_version.
    """
    if max_attempts < 1 or not stream_name:
        raise ValueError("A stream and positive max_attempts are required")
    counts = {"records_acknowledged": 0, "request_attempts": 0, "record_retries": 0}

    def flush(batch: list[tuple[str, dict[str, Any]]]) -> None:
        pending = batch
        for attempt in range(1, max_attempts + 1):
            counts["request_attempts"] += 1
            try:
                response = client.put_records(
                    StreamName=stream_name, Records=[record for _, record in pending]
                )
            except Exception:
                # An ambiguous transport error cannot prove which records committed.
                response = None
            if response is not None:
                results = response.get("Records", [])
                if len(results) == len(pending):
                    failed = []
                    for item, result in zip(pending, results, strict=True):
                        if result.get("ErrorCode") or not result.get("SequenceNumber"):
                            failed.append(item)
                        else:
                            counts["records_acknowledged"] += 1
                    pending = failed
            if not pending:
                return
            if attempt == max_attempts:
                raise ProducerFailure([identifier for identifier, _ in pending], attempt)
            counts["record_retries"] += len(pending)
            sleep(min(0.1 * 2 ** (attempt - 1), 2.0) * max(0.0, min(1.0, jitter())))

    batch: list[tuple[str, dict[str, Any]]] = []
    batch_bytes = 0
    for original in events:
        event = dict(original)
        identifier, partition = event.get("event_id"), event.get("order_id")
        if (
            not isinstance(identifier, str)
            or not isinstance(partition, str)
            or not 1 <= len(partition) <= 256
        ):
            raise ValueError("event_id and a 1..256-character order_id are required")
        # A producer cannot authoritatively stamp an AWS receipt time.
        event["ingestion_timestamp"] = None
        frame = encode_frame(_json(event), identifier)
        size = len(frame) + len(partition.encode("utf-8"))
        if size > MAX_RECORD_BYTES:
            raise ValueError(f"Encoded record exceeds application limit: {identifier}")
        if batch and (len(batch) == 500 or batch_bytes + size > MAX_BATCH_BYTES):
            flush(batch)
            batch, batch_bytes = [], 0
        batch.append((identifier, {"Data": frame, "PartitionKey": partition}))
        batch_bytes += size
    if batch:
        flush(batch)
    return counts


def process_kinesis_batch(
    event: dict[str, Any], s3: Any, bucket: str, *, environment: str = "dev"
) -> dict[str, Any]:
    """Quarantine invalid records before acknowledging; return sequence failures.

    Metrics count attempts, never distinct business events. No valid event is
    forwarded into a second canonical ingestion path.
    """
    failures = []
    processed = rejected = 0
    started = time.monotonic()
    for record in event["Records"]:
        sequence = record["kinesis"]["sequenceNumber"]
        locator = f"{record['eventSourceARN']}/{record['eventID']}"
        encoded_data = record["kinesis"]["data"]
        # Preserve even an invalid outer base64 representation in the evidence.
        evidence: dict[str, Any] = {
            "source_locator": locator,
            "kinesis_data_base64": encoded_data,
            "sequence_number": sequence,
        }
        try:
            arrival = record["kinesis"]["approximateArrivalTimestamp"]
            if isinstance(arrival, bool) or not isinstance(arrival, (int, float)):
                raise ValueError("Kinesis arrival must be an epoch number")
            received = (
                datetime.fromtimestamp(arrival, UTC)
                .isoformat(timespec="microseconds")
                .replace("+00:00", "Z")
            )
            evidence["first_received_at"] = received
            wire = base64.b64decode(encoded_data, validate=True)
            identifier, payload = decode_frame(wire)
            business = _parse(payload)
            if business.get("event_id") != identifier:
                raise ValueError("Transport record_id differs from event_id")
            # Override the untrusted producer clock before timestamp sanity checks.
            business["ingestion_timestamp"] = received
            violations = validate_event(business)
        except (ValueError, TypeError, OverflowError, KeyError) as error:
            violations = [f"transport_or_parse:{type(error).__name__}:{error}"]
        processed += 1
        if violations:
            rejected += 1
            evidence["violations"] = violations
            evidence["validation_scope"] = "row_local_signal"
            key_hash = hashlib.sha256((locator + "\n" + encoded_data).encode()).hexdigest()
            try:
                s3.put_object(
                    Bucket=bucket,
                    Key=f"quarantine/stream/{key_hash}.json",
                    Body=_json(evidence),
                    ContentType="application/json",
                )
            except Exception:
                # A durable write is required before acknowledging poison input.
                failures.append({"itemIdentifier": sequence})
    print(
        json.dumps(
            emf(
                {
                    "records_processed": processed,
                    "records_rejected": rejected,
                    "failures": len(failures),
                    "processing_seconds": time.monotonic() - started,
                },
                environment=environment,
                stage="stream_attempt",
            )
        )
    )
    return {"batchItemFailures": failures}


def handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    import boto3
    from botocore.config import Config

    client = boto3.client("s3", config=Config(retries={"max_attempts": 3, "mode": "standard"}))
    return process_kinesis_batch(
        event, client, os.environ["LAKE_BUCKET"], environment=os.environ.get("ENVIRONMENT", "dev")
    )
