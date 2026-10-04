"""Application AWS boundaries with fake clients; no network or AWS credentials."""

import base64
import copy
import json
from datetime import UTC, datetime
from typing import Any

import pytest

from commerce_platform.aws import (
    ProducerFailure,
    decode_frame,
    encode_frame,
    process_kinesis_batch,
    send_events,
)
from commerce_platform.generator import generate_events


class Kinesis:
    def __init__(self, fail_first: bool = False, timeout_first: bool = False) -> None:
        self.calls: list[list[dict[str, Any]]] = []
        self.fail_first = fail_first
        self.timeout_first = timeout_first

    def put_records(self, **kwargs: Any) -> dict[str, Any]:
        self.calls.append(kwargs["Records"])
        if len(self.calls) == 1 and self.timeout_first:
            raise TimeoutError("acknowledgement lost")
        results = [
            {"SequenceNumber": str(i), "ShardId": "shard-0"}
            for i, _ in enumerate(kwargs["Records"])
        ]
        if len(self.calls) == 1 and self.fail_first:
            results[0] = {"ErrorCode": "ProvisionedThroughputExceededException"}
        return {"Records": results}


class S3:
    def __init__(self, fail: bool = False) -> None:
        self.objects: dict[str, bytes] = {}
        self.fail = fail

    def put_object(self, **kwargs: Any) -> None:
        if self.fail:
            raise TimeoutError("unavailable")
        self.objects[kwargs["Key"]] = kwargs["Body"]


def record(payload: bytes, identifier: str, sequence: str = "1") -> dict[str, Any]:
    wire = encode_frame(payload, identifier)
    return {
        "eventSourceARN": "arn:aws:kinesis:eu-west-1:123456789012:stream/test",
        "eventID": f"shard-0:{sequence}",
        "kinesis": {
            "sequenceNumber": sequence,
            "data": base64.b64encode(wire).decode(),
            "approximateArrivalTimestamp": datetime(2026, 7, 1, tzinfo=UTC).timestamp(),
        },
    }


def test_frame_preserves_arbitrary_malformed_bytes() -> None:
    payload = b'{\n"broken":\xff\n'
    frame = encode_frame(payload, "synthetic-malformed")
    assert b"\n" not in frame
    assert decode_frame(frame) == ("synthetic-malformed", payload)
    with pytest.raises(ValueError):
        decode_frame(b'{"transport_version":"1.0.0","transport_version":"2.0.0"}')


def test_producer_batches_limits_and_preserves_original_events() -> None:
    events = generate_events(60)
    original = copy.deepcopy(events)
    client = Kinesis()
    counts = send_events(events, client, "test")
    assert len(client.calls) > 1
    assert all(len(batch) <= 500 for batch in client.calls)
    assert counts["records_acknowledged"] == len(events)
    assert events == original
    first = client.calls[0][0]
    identifier, payload = decode_frame(first["Data"])
    assert identifier == events[0]["event_id"]
    assert first["PartitionKey"] == events[0]["order_id"]
    assert json.loads(payload)["ingestion_timestamp"] is None


def test_partial_retry_only_failed_record_with_identical_body() -> None:
    client = Kinesis(fail_first=True)
    counts = send_events(generate_events(1), client, "test", sleep=lambda _: None)
    assert len(client.calls[1]) == 1
    assert client.calls[1][0] == client.calls[0][0]
    assert counts["record_retries"] == 1


def test_ambiguous_timeout_retries_all_with_stable_ids() -> None:
    client = Kinesis(timeout_first=True)
    send_events(generate_events(1), client, "test", sleep=lambda _: None)
    assert client.calls[0] == client.calls[1]


def test_exhausted_budget_retains_pending_identifiers() -> None:
    events = generate_events(1)
    with pytest.raises(ProducerFailure) as failure:
        send_events(events, Kinesis(timeout_first=True), "test", max_attempts=1)
    assert failure.value.pending_ids == [event["event_id"] for event in events]
    with pytest.raises(ValueError):
        send_events(events, Kinesis(), "test", max_attempts=0)
    oversized = {**events[0], "untrusted": "x" * 1024 * 1024}
    with pytest.raises(ValueError):
        send_events([oversized], Kinesis(), "test")


def test_valid_event_trusted_arrival_overwrites_producer_clock(capsys: Any) -> None:
    event = generate_events(1)[0]
    event["ingestion_timestamp"] = "untrusted-and-invalid"
    source = record(json.dumps(event).encode(), event["event_id"])
    s3 = S3()
    assert process_kinesis_batch({"Records": [source]}, s3, "bucket") == {"batchItemFailures": []}
    assert not s3.objects
    assert json.loads(capsys.readouterr().out)["Stage"] == "stream_attempt"


@pytest.mark.parametrize("payload", [b"{broken", b"\xff\n", b"[]", b'{"value":NaN}'])
def test_invalid_payload_quarantine_is_idempotent(payload: bytes) -> None:
    event = {"Records": [record(payload, "bad")]}
    s3 = S3()
    for _ in range(2):
        assert process_kinesis_batch(event, s3, "bucket") == {"batchItemFailures": []}
    assert len(s3.objects) == 1
    evidence = json.loads(next(iter(s3.objects.values())))
    assert evidence["violations"]
    assert evidence["source_locator"].endswith("shard-0:1")


def test_quarantine_write_failure_is_not_acknowledged() -> None:
    event = {"Records": [record(b"bad", "bad", "12")]}
    assert process_kinesis_batch(event, S3(fail=True), "bucket") == {
        "batchItemFailures": [{"itemIdentifier": "12"}]
    }


def test_future_event_and_mismatched_transport_identity_rejected() -> None:
    event = generate_events(1)[0]
    event["event_timestamp"] = "2099-01-01T00:00:00Z"
    s3 = S3()
    process_kinesis_batch(
        {"Records": [record(json.dumps(event).encode(), event["event_id"])]}, s3, "bucket"
    )
    assert len(s3.objects) == 1
    s3 = S3()
    process_kinesis_batch({"Records": [record(json.dumps(event).encode(), "wrong")]}, s3, "bucket")
    assert "record_id differs" in next(iter(s3.objects.values())).decode()
