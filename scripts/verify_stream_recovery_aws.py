#!/usr/bin/env python3
"""Force a real quarantine-write denial, retain the failed batch, restore and replay.

Temporary Deny is scoped to the selected test validator and quarantine prefix.
The original bucket policy is restored in finally. Requires supervised teardown
if the process or host dies while the failure policy is installed.
"""

import argparse
import base64
import json
import time
import uuid
from pathlib import Path

from verify_stream_aws import list_keys

from commerce_platform.aws import decode_frame, encode_frame


def evidence_id(data):
    evidence = json.loads(data)
    return decode_frame(base64.b64decode(evidence["kinesis_data_base64"]))[0]


def verify(session, account, outputs, directory, timeout=300):
    if session.client("sts").get_caller_identity()["Account"] != account:
        raise ValueError("Wrong AWS account")
    stream, bucket = outputs["stream_name"], outputs["lake_bucket"]
    if (
        not stream.endswith("-test-events")
        or bucket != f"{stream.removesuffix('-events')}-{account}-{session.region_name}"
    ):
        raise ValueError("Use an isolated test deployment")
    directory.mkdir(parents=True, exist_ok=False)
    s3, kinesis = session.client("s3"), session.client("kinesis")
    function = stream.removesuffix("-events") + "-validator"
    role = session.client("lambda").get_function_configuration(FunctionName=function)["Role"]
    original = s3.get_bucket_policy(Bucket=bucket)["Policy"]
    (directory / "original-bucket-policy.json").write_text(original)
    modified = json.loads(original)
    modified["Statement"].append(
        {
            "Sid": "AcceptanceQuarantineFailure",
            "Effect": "Deny",
            "Principal": "*",
            "Action": "s3:PutObject",
            "Resource": f"arn:aws:s3:::{bucket}/quarantine/stream/*",
            "Condition": {"ArnEquals": {"aws:PrincipalArn": role}},
        }
    )
    identifier = f"acceptance-recovery-{uuid.uuid4().hex}"
    frame = encode_frame(b"{broken-during-failure-drill", identifier)
    report = {
        "state": "running",
        "scope": __doc__.strip(),
        "checks": {},
        "region": session.region_name,
    }
    began = time.monotonic()

    def read(target, key):
        response = s3.get_object(Bucket=target, Key=key)
        try:
            return response["Body"].read()
        finally:
            response["Body"].close()

    try:
        try:
            s3.put_bucket_policy(Bucket=bucket, Policy=json.dumps(modified))
            kinesis.put_record(StreamName=stream, PartitionKey=identifier, Data=frame)
            deadline = time.monotonic() + timeout
            while time.monotonic() < deadline:
                retained = False
                for key in list_keys(s3, outputs["failed_batches_bucket"], "aws/lambda/"):
                    evidence = json.loads(read(outputs["failed_batches_bucket"], key))
                    payload = evidence.get("payload")
                    if isinstance(payload, str):
                        payload = json.loads(payload)
                    if isinstance(payload, dict) and any(
                        decode_frame(base64.b64decode(record["kinesis"]["data"]))[0] == identifier
                        for record in payload.get("Records", [])
                    ):
                        retained = True
                        report["failed_batch_condition"] = evidence.get("requestContext", {}).get(
                            "condition"
                        )
                        break
                if retained:
                    break
                time.sleep(3)
            else:
                raise TimeoutError("Failed batch did not reach its S3 destination")
            assert not any(
                evidence_id(read(bucket, key)) == identifier
                for key in list_keys(s3, bucket, "quarantine/stream/")
            )
            report["checks"]["denied_quarantine_retries_retained_full_batch"] = "pass"
            report["failure_detection_and_retention_seconds"] = time.monotonic() - began
        finally:
            s3.put_bucket_policy(Bucket=bucket, Policy=original)
            assert json.loads(s3.get_bucket_policy(Bucket=bucket)["Policy"]) == json.loads(original)
            report["checks"]["original_policy_restored"] = "pass"
        recovery = time.monotonic()
        kinesis.put_record(StreamName=stream, PartitionKey=identifier, Data=frame)
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if any(
                evidence_id(read(bucket, key)) == identifier
                for key in list_keys(s3, bucket, "quarantine/stream/")
            ):
                break
            time.sleep(3)
        else:
            raise TimeoutError("Replayed record did not recover after policy restoration")
        report["checks"]["replayed_record_durably_quarantined"] = "pass"
        report["recovery_seconds_after_replay"] = time.monotonic() - recovery
        report["state"] = "pass"
    except BaseException as error:
        report["state"] = "fail"
        report["error_type"] = type(error).__name__
        raise
    finally:
        (directory / "report.json").write_text(json.dumps(report, indent=2) + "\n")
    return report


def main():
    import boto3

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--expected-account", required=True)
    parser.add_argument("--region", default="eu-west-1")
    parser.add_argument("--outputs", type=Path, required=True)
    parser.add_argument("--directory", type=Path, required=True)
    parser.add_argument("--allow-failure-injection", action="store_true", required=True)
    args = parser.parse_args()
    print(
        json.dumps(
            verify(
                boto3.Session(region_name=args.region),
                args.expected_account,
                json.loads(args.outputs.read_text())["platform"]["value"],
                args.directory,
            ),
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
