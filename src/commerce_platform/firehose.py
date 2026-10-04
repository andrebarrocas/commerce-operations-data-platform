"""Capture one explicitly named S3 Firehose object as immutable local evidence.

S3 LastModified is the conservative availability clock for this batch path.
It is not a reconstructed Kinesis arrival timestamp. Existing source versions
retain that clock on recapture, even when the import runs much later.
"""

import base64
import gzip
import hashlib
import json
import os
import tempfile
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.parse import quote

from commerce_platform.aws import decode_frame


def _encode(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode("utf-8")


def _publish_immutable(path: Path, content: bytes) -> bool:
    """Install a complete file without overwriting a different earlier capture."""
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary: str | None = None
    try:
        with tempfile.NamedTemporaryFile(
            dir=path.parent, prefix=".capture-", delete=False
        ) as stream:
            temporary = stream.name
            stream.write(content)
            stream.flush()
            os.fsync(stream.fileno())
        try:
            os.link(temporary, path)
        except FileExistsError:
            if path.read_bytes() != content:
                raise FileExistsError(
                    f"Capture output already contains different evidence: {path}"
                ) from None
            return True
        descriptor = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
        return False
    finally:
        if temporary is not None:
            Path(temporary).unlink(missing_ok=True)


def capture_object(client: Any, bucket: str, key: str, output: Path) -> dict[str, Any]:
    """Get one object, decode transport frames, and return capture metadata.

    No list/head calls or account discovery occur. The returned S3 version and
    object checksum identify what was actually read. Invalid transport frames
    become evidence rows whose payload is the original frame bytes; they are
    not mistaken for decoded business payloads.

    Firehose GZIP output is supported. Whole-object decompression failures fail
    the import before writing a capture; the original S3 object remains evidence.
    This bounded local bridge buffers one object in memory, not an entire prefix.
    """
    if not bucket or not key:
        raise ValueError("An explicit bucket and object key are required")
    response = client.get_object(Bucket=bucket, Key=key)
    body = response["Body"]
    try:
        raw = body.read()
    finally:
        body.close()
    if not isinstance(raw, bytes):
        raise TypeError("S3 GetObject body must return bytes")
    observed = response.get("LastModified")
    if (
        not isinstance(observed, datetime)
        or observed.tzinfo is None
        or observed.utcoffset() is None
    ):
        raise ValueError("S3 LastModified must be a timezone-aware datetime")
    observed_at = observed.astimezone(UTC).isoformat(timespec="microseconds").replace("+00:00", "Z")
    version = response.get("VersionId")
    if version is not None and not isinstance(version, str):
        raise ValueError("S3 VersionId must be a string when present")
    object_sha = hashlib.sha256(raw).hexdigest()
    source = (
        f"s3://{bucket}/{quote(key, safe='/')}?versionId={quote(version or 'unversioned', safe='')}"
        f"&objectSha256={object_sha}"
    )
    compressed = raw.startswith(b"\x1f\x8b") or response.get("ContentEncoding") == "gzip"
    try:
        data = gzip.decompress(raw) if compressed else raw
    except (OSError, EOFError) as error:
        raise ValueError(f"Unreadable GZIP Firehose object: {source}") from error
    frames = data.split(b"\n") if data else []
    if frames and frames[-1] == b"":
        frames.pop()  # AppendDelimiterToRecord creates one terminal separator.

    rows: list[dict[str, Any]] = []
    transport_errors = 0
    for line_number, frame in enumerate(frames, 1):
        identifier: str | None = None
        payload = frame
        error_message: str | None = None
        try:
            identifier, payload = decode_frame(frame)
        except (ValueError, TypeError) as error:
            transport_errors += 1
            error_message = f"transport_frame:{type(error).__name__}:{error}"
        rows.append(
            {
                "_capture_version": "1.0.0",
                "source": f"{source}#line={line_number}",
                "observed_at": observed_at,
                "record_id": identifier,
                "payload_base64": base64.b64encode(payload).decode("ascii"),
                "transport_error": error_message,
            }
        )
    content = b"".join(_encode(row) + b"\n" for row in rows)
    output = Path(output)
    reused = _publish_immutable(output, content)
    return {
        "path": str(output.resolve()),
        "source": source,
        "sha256": hashlib.sha256(content).hexdigest(),
        "object_sha256": object_sha,
        "records": len(rows),
        "transport_errors": transport_errors,
        "observed_at": observed_at,
        "version_id": version,
        "compression": "gzip" if compressed else "none",
        "reused": reused,
    }
