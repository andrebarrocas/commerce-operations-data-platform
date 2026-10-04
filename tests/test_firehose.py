"""S3 Firehose bridge boundaries, using a fake client rather than AWS calls."""

import base64
import gzip
import hashlib
import io
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq
import pytest

from commerce_platform.aws import encode_frame
from commerce_platform.firehose import capture_object
from commerce_platform.generator import generate_events
from commerce_platform.lake import run

OBSERVED = datetime(2026, 1, 12, 12, 30, 5, tzinfo=UTC)


class FakeS3:
    def __init__(
        self, body: bytes, *, modified: datetime = OBSERVED, version: str | None = "v+1/abc"
    ) -> None:
        self.body = body
        self.modified = modified
        self.version = version
        self.calls: list[dict[str, str]] = []
        self.streams: list[io.BytesIO] = []

    def get_object(self, **request: str) -> dict[str, Any]:
        self.calls.append(request)
        stream = io.BytesIO(self.body)
        self.streams.append(stream)
        response: dict[str, Any] = {"Body": stream, "LastModified": self.modified}
        if self.version is not None:
            response["VersionId"] = self.version
        return response


def read_capture(path: Path) -> list[dict[str, Any]]:
    return [json.loads(line) for line in path.read_bytes().splitlines()]


def test_preserves_arbitrary_payload_bytes_and_clock(tmp_path: Path) -> None:
    payloads = [b"\xff\x00binary\nembedded\r\nnewlines", b'{"value":"synthetic"}', b""]
    raw = b"\n".join(encode_frame(body, f"record-{i}") for i, body in enumerate(payloads)) + b"\n"
    client = FakeS3(raw)
    path = tmp_path / "capture.jsonl"
    manifest = capture_object(client, "synthetic-bucket", "bronze/a file.gz", path)
    rows = read_capture(path)
    assert [base64.b64decode(row["payload_base64"]) for row in rows] == payloads
    assert all(row["observed_at"] == "2026-01-12T12:30:05.000000Z" for row in rows)
    assert all(row["transport_error"] is None for row in rows)
    assert [row["record_id"] for row in rows] == ["record-0", "record-1", "record-2"]
    assert rows[0]["source"].endswith("#line=1")
    assert "versionId=v%2B1%2Fabc" in manifest["source"]
    assert "a%20file.gz" in manifest["source"]
    assert manifest["object_sha256"] == hashlib.sha256(raw).hexdigest()
    assert manifest["records"] == 3
    assert client.calls == [{"Bucket": "synthetic-bucket", "Key": "bronze/a file.gz"}]
    assert all(stream.closed for stream in client.streams)


def test_recapture_is_deterministic_and_does_not_replace_evidence(tmp_path: Path) -> None:
    client = FakeS3(encode_frame(b"payload", "record-1") + b"\n")
    path = tmp_path / "capture.jsonl"
    first = capture_object(client, "bucket", "key", path)
    before = path.read_bytes()
    second = capture_object(client, "bucket", "key", path)
    assert second["reused"] is True
    assert first["sha256"] == second["sha256"]
    assert path.read_bytes() == before
    with pytest.raises(FileExistsError, match="different evidence"):
        capture_object(
            FakeS3(client.body, modified=datetime(2026, 2, 1, tzinfo=UTC)), "bucket", "key", path
        )
    assert path.read_bytes() == before


@pytest.mark.parametrize(
    "frame",
    [
        b'{"transport_version":"2.0.0","record_id":"x","payload_base64":"eA=="}',
        b'{"transport_version":"1.0.0","record_id":"x","payload_base64":"not base64"}',
        b"not-json",
        b"\xff",
        b"",
    ],
)
def test_invalid_transport_preserves_frame_bytes(tmp_path: Path, frame: bytes) -> None:
    path = tmp_path / "capture.jsonl"
    report = capture_object(FakeS3(frame + b"\n"), "bucket", "key", path)
    row = read_capture(path)[0]
    assert report["transport_errors"] == 1
    assert row["record_id"] is None
    assert row["transport_error"].startswith("transport_frame:")
    assert base64.b64decode(row["payload_base64"]) == frame


def test_gzip_firehose_and_unversioned_objects(tmp_path: Path) -> None:
    frame = encode_frame(b"some\npayload", "record-1") + b"\n"
    report = capture_object(
        FakeS3(gzip.compress(frame, mtime=0), version=None),
        "bucket",
        "raw.gz",
        tmp_path / "capture.jsonl",
    )
    assert report["compression"] == "gzip"
    assert report["records"] == 1
    assert report["version_id"] is None
    assert "versionId=unversioned" in report["source"]


def test_bad_object_metadata_and_gzip_do_not_publish(tmp_path: Path) -> None:
    target = tmp_path / "capture.jsonl"
    with pytest.raises(ValueError, match="timezone-aware"):
        capture_object(FakeS3(b"payload", modified=datetime(2026, 1, 1)), "bucket", "key", target)
    assert not target.exists()
    with pytest.raises(ValueError, match="GZIP"):
        capture_object(FakeS3(b"\x1f\x8btruncated"), "bucket", "key", target)
    assert not target.exists()


def test_empty_object_has_no_fabricated_record(tmp_path: Path) -> None:
    target = tmp_path / "capture.jsonl"
    report = capture_object(FakeS3(b""), "bucket", "key", target)
    assert report["records"] == 0
    assert target.read_bytes() == b""


def test_capture_ingest_overrides_wire_time_and_quarantines_bad_frame(tmp_path: Path) -> None:
    events = generate_events(2)
    frames = []
    for event in events:
        event["ingestion_timestamp"] = None
        frames.append(encode_frame(json.dumps(event).encode(), event["event_id"]))
    frames.append(b'{"transport_version":"unsupported"}')
    path = tmp_path / "capture.jsonl"
    capture_object(FakeS3(b"\n".join(frames) + b"\n"), "bucket", "raw-object", path)
    result = run(path, tmp_path / "lake", as_of=datetime(2026, 2, 1, tzinfo=UTC), products=False)
    assert result["metrics"]["accepted_unique"] == len(events)
    assert result["metrics"]["records_rejected"] == 1
    rows = pq.read_table(result["artifacts"]["silver_history"]["path"]).to_pylist()
    assert {row["ingestion_timestamp"] for row in rows} == {"2026-01-12T12:30:05.000000Z"}
    quarantine = [
        json.loads(line) for line in Path(result["quarantine_path"]).read_text().splitlines()
    ]
    assert any("transport" in str(record["reasons"]) for record in quarantine)
