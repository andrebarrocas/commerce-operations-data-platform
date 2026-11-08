#!/usr/bin/env python3
"""Paid, opt-in Kinesis/Firehose/Lambda acceptance with a supervised local bridge.

Consumes a fresh isolated test deployment. Does not create or delete resources.
Raw-to-Silver reconciliation runs locally; this is not distributed orchestration.
"""

import argparse
import base64
import json
import time
from collections import Counter
from datetime import UTC, datetime, timedelta
from pathlib import Path

from commerce_platform.aws import decode_frame, encode_frame, send_events
from commerce_platform.firehose import capture_object
from commerce_platform.generator import generate_events
from commerce_platform.lake import run


def list_keys(s3, bucket, prefix):
    return [
        row["Key"]
        for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix)
        for row in page.get("Contents", [])
    ]


def verify(session, account, outputs, directory, orders=1000, timeout=600):
    if session.client("sts").get_caller_identity()["Account"] != account:
        raise ValueError("Wrong AWS account")
    bucket, stream = outputs["lake_bucket"], outputs["stream_name"]
    if (
        not stream.endswith("-test-events")
        or bucket != f"{stream.removesuffix('-events')}-{account}-{session.region_name}"
    ):
        raise ValueError("Use an isolated test deployment in the selected account/region")
    if not 40 <= orders <= 5000 or not 60 <= timeout <= 900:
        raise ValueError("Acceptance is bounded to 40..5000 orders and 60..900 seconds")
    s3, kinesis = session.client("s3"), session.client("kinesis")
    if list_keys(s3, bucket, "raw/") or list_keys(s3, bucket, "quarantine/"):
        raise ValueError("Acceptance requires a fresh lake with no prior raw/quarantine objects")
    directory.mkdir(parents=True, exist_ok=False)
    began = time.monotonic()
    events = generate_events(orders, seed=81108, start=datetime.now(UTC) - timedelta(days=14))
    (directory / "producer-events.jsonl").write_text("".join(json.dumps(e) + "\n" for e in events))
    expected = {e["event_id"] for e in events}
    bad = {f"acceptance-malformed-{i}" for i in range(4)}
    report = {
        "state": "running",
        "region": session.region_name,
        "orders": orders,
        "scope": __doc__.strip(),
        "checks": {},
        "expected_unique_events": len(expected),
    }
    try:
        report["producer"] = send_events(events + events[:10], kinesis, stream)
        result = kinesis.put_records(
            StreamName=stream,
            Records=[
                {"PartitionKey": identifier, "Data": encode_frame(payload, identifier)}
                for identifier, payload in zip(
                    sorted(bad), [b"{broken", b"\xff\n", b"[]", b'{"x":NaN}'], strict=True
                )
            ],
        )
        if result.get("FailedRecordCount", 0):
            raise RuntimeError(
                "Malformed-fixture send failed; retain input and retry in a fresh test"
            )
        report["producer_ack_seconds"] = time.monotonic() - began
        captures, observed, quarantined = {}, Counter(), set()
        while time.monotonic() - began < timeout:
            for key in list_keys(s3, bucket, "raw/"):
                if key in captures:
                    continue
                path = directory / f"capture-{len(captures):04d}.jsonl"
                captures[key] = capture_object(s3, bucket, key, path)
                for line in path.read_text().splitlines():
                    row = json.loads(line)
                    assert not row["transport_error"]
                    observed[row["record_id"]] += 1
            for key in list_keys(s3, bucket, "quarantine/stream/"):
                response = s3.get_object(Bucket=bucket, Key=key)
                try:
                    evidence = json.loads(response["Body"].read())
                finally:
                    response["Body"].close()
                identifier, _ = decode_frame(base64.b64decode(evidence["kinesis_data_base64"]))
                assert evidence["violations"]
                quarantined.add(identifier)
            assert not (set(observed) - expected - bad), "Unexpected data in isolated test bucket"
            assert not (quarantined - bad), (
                "A valid fixture was quarantined by the stream validator"
            )
            if (
                expected | bad <= set(observed)
                and bad <= quarantined
                and all(observed[e["event_id"]] >= 2 for e in events[:10])
            ):
                break
            time.sleep(5)
        else:
            raise TimeoutError(
                "Raw delivery/quarantine controls did not reconcile before the deadline"
            )
        report["delivery_wait_seconds_from_send_start"] = time.monotonic() - began
        report["raw_objects"] = len(captures)
        report["raw_records"] = sum(observed.values())
        report["quarantined_unique"] = len(quarantined)
        report["checks"]["complete_raw_delivery_and_duplicate_preservation"] = "pass"
        report["checks"]["actual_stream_validator_quarantine"] = "pass"
        source = directory / "captured.jsonl"
        source.write_bytes(b"".join(Path(c["path"]).read_bytes() for c in captures.values()))
        cutoff = datetime.now(UTC)
        snapshot = run(source, directory / "lake", as_of=cutoff, expected_ids=expected)
        assert snapshot["metrics"]["accepted_unique"] == len(expected)
        assert snapshot["metrics"]["effective_records"] == len(expected)
        assert snapshot["metrics"]["duplicate_records"] >= 10
        assert snapshot["metrics"]["records_rejected"] == len(bad)
        assert snapshot["metrics"]["source_completeness"]["state"] == "pass"
        replay = run(source, directory / "lake", as_of=cutoff, expected_ids=expected)
        assert replay["logical_hash"] == snapshot["logical_hash"]
        report["checks"]["captured_raw_local_reconciliation_and_replay"] = "pass"
        report["local_bridge_metrics"] = {
            **snapshot["metrics"],
            "latency_clock": "S3 object LastModified availability upper bound; synthetic business time; local monotonic processing duration",
        }
        report["snapshot_logical_hash"] = snapshot["logical_hash"]
        # Keep private absolute paths out of the shareable report.
        (directory / "snapshot.json").write_text(json.dumps(snapshot, indent=2, default=str) + "\n")
        report["state"] = "pass"
    except BaseException as error:
        report["state"] = "fail"
        report["error_type"] = type(error).__name__
        raise
    finally:
        report["elapsed_seconds"] = time.monotonic() - began
        (directory / "report.json").write_text(json.dumps(report, indent=2) + "\n")
    return report


def main():
    import boto3
    from botocore.config import Config

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--expected-account", required=True)
    parser.add_argument("--outputs", required=True, type=Path, help="tofu output -json")
    parser.add_argument("--directory", required=True, type=Path)
    parser.add_argument("--orders", default=1000, type=int)
    parser.add_argument("--region", default="eu-west-1")
    parser.add_argument("--allow-stream-writes", required=True, action="store_true")
    args = parser.parse_args()
    session = boto3.Session(region_name=args.region)
    original = session.client
    session.client = lambda service: original(
        service,
        config=Config(retries={"total_max_attempts": 3}, connect_timeout=10, read_timeout=30),
    )
    outputs = json.loads(args.outputs.read_text())["platform"]["value"]
    print(
        json.dumps(
            verify(session, args.expected_account, outputs, args.directory, args.orders), indent=2
        )
    )


if __name__ == "__main__":
    main()
