#!/usr/bin/env python3
"""Opt-in real Lambda/S3 package acceptance with isolated resources and cleanup.

Direct invocations use synthetic Kinesis-shaped envelopes. This does NOT test
Kinesis delivery, event-source mapping retries, Firehose, EMR or Redshift.
"""

import argparse
import base64
import hashlib
import json
import time
import uuid
from datetime import UTC, datetime
from pathlib import Path

from botocore.exceptions import ClientError

from commerce_platform.aws import encode_frame
from commerce_platform.generator import generate_events


def invoke(client, name: str, record: dict) -> dict:
    response = client.invoke(
        FunctionName=name,
        InvocationType="RequestResponse",
        Payload=json.dumps({"Records": [record]}).encode(),
    )
    stream = response["Payload"]
    try:
        result = json.loads(stream.read())
    finally:
        stream.close()
    if response.get("FunctionError") or response["StatusCode"] != 200:
        raise AssertionError("Packaged Lambda invocation failed")
    return result


def envelope(data: bytes, identifier: str, sequence: str) -> dict:
    return {
        "eventSourceARN": "arn:aws:kinesis:eu-west-1:000000000000:stream/synthetic-direct-invocation",
        "eventID": f"shard-synthetic:{sequence}",
        "kinesis": {
            "sequenceNumber": sequence,
            "data": base64.b64encode(encode_frame(data, identifier)).decode(),
            "approximateArrivalTimestamp": datetime.now(UTC).timestamp(),
        },
    }


def ignore_missing(action, **kwargs) -> None:
    try:
        action(**kwargs)
    except ClientError as error:
        if error.response["Error"]["Code"] not in {
            "NoSuchEntity",
            "NoSuchBucket",
            "ResourceNotFoundException",
            "404",
        }:
            raise


def empty_bucket(s3, name: str) -> None:
    """Only called for fresh UUID-named buckets created by this invocation."""
    try:
        for page in s3.get_paginator("list_object_versions").paginate(Bucket=name):
            objects = [
                {"Key": row["Key"], "VersionId": row["VersionId"]}
                for kind in ("Versions", "DeleteMarkers")
                for row in page.get(kind, [])
            ]
            if objects:
                response = s3.delete_objects(Bucket=name, Delete={"Objects": objects})
                if response.get("Errors"):
                    raise RuntimeError("Not all test object versions were deleted")
        s3.delete_bucket(Bucket=name)
    except ClientError as error:
        if error.response["Error"]["Code"] != "NoSuchBucket":
            raise


def verify(session, expected_account: str, package: Path, directory: Path) -> dict:
    identity = session.client("sts").get_caller_identity()
    if identity["Account"] != expected_account or len(expected_account) != 12:
        raise ValueError("AWS account differs from the explicitly selected account")
    archive = package.read_bytes()
    if not archive.startswith(b"PK"):
        raise ValueError("Build the real Linux Lambda ZIP before running acceptance")
    directory.mkdir(parents=True, exist_ok=False)
    directory.chmod(0o700)
    name = f"commerce-check-{uuid.uuid4().hex[:12]}"
    bucket = f"{name}-{expected_account}"
    denied_bucket = f"{bucket}-deny"
    log_group = f"/aws/lambda/{name}"
    tags = {"Project": "commerce-platform", "Environment": "test", "AcceptanceRun": name}
    report = {
        "state": "running",
        "observed_at": datetime.now(UTC).isoformat(),
        "region": session.region_name,
        "account_verified": True,
        "scope": __doc__.strip(),
        "package_sha256": hashlib.sha256(archive).hexdigest(),
        "checks": {},
        "cleanup": {},
    }
    # Private recovery inventory is written BEFORE mutations, including ambiguous failures.
    journal = directory / "resources.json"
    journal.write_text(
        json.dumps(
            {
                "function": name,
                "role": name,
                "policy": name,
                "buckets": [bucket, denied_bucket],
                "log_group": log_group,
            },
            indent=2,
        )
    )
    journal.chmod(0o600)
    iam, s3 = session.client("iam"), session.client("s3")
    lam, logs = session.client("lambda"), session.client("logs")
    created_buckets = []
    role_created = log_created = function_created = policy_created = False
    began = time.monotonic()
    try:
        for target in (bucket, denied_bucket):
            kwargs = {"Bucket": target}
            if session.region_name != "us-east-1":
                kwargs["CreateBucketConfiguration"] = {"LocationConstraint": session.region_name}
            s3.create_bucket(**kwargs)
            created_buckets.append(target)
            s3.put_public_access_block(
                Bucket=target,
                PublicAccessBlockConfiguration={
                    "BlockPublicAcls": True,
                    "IgnorePublicAcls": True,
                    "BlockPublicPolicy": True,
                    "RestrictPublicBuckets": True,
                },
            )
            s3.put_bucket_tagging(
                Bucket=target,
                Tagging={"TagSet": [{"Key": key, "Value": value} for key, value in tags.items()]},
            )
            s3.put_bucket_encryption(
                Bucket=target,
                ServerSideEncryptionConfiguration={
                    "Rules": [{"ApplyServerSideEncryptionByDefault": {"SSEAlgorithm": "AES256"}}]
                },
            )
            s3.put_bucket_versioning(Bucket=target, VersioningConfiguration={"Status": "Enabled"})
            s3.put_bucket_policy(
                Bucket=target,
                Policy=json.dumps(
                    {
                        "Version": "2012-10-17",
                        "Statement": [
                            {
                                "Effect": "Deny",
                                "Principal": "*",
                                "Action": "s3:*",
                                "Resource": [f"arn:aws:s3:::{target}", f"arn:aws:s3:::{target}/*"],
                                "Condition": {"Bool": {"aws:SecureTransport": "false"}},
                            }
                        ],
                    }
                ),
            )
        role = iam.create_role(
            RoleName=name,
            Tags=[{"Key": k, "Value": v} for k, v in tags.items()],
            AssumeRolePolicyDocument=json.dumps(
                {
                    "Version": "2012-10-17",
                    "Statement": [
                        {
                            "Effect": "Allow",
                            "Principal": {"Service": "lambda.amazonaws.com"},
                            "Action": "sts:AssumeRole",
                        }
                    ],
                }
            ),
        )["Role"]["Arn"]
        role_created = True
        iam.put_role_policy(
            RoleName=name,
            PolicyName=name,
            PolicyDocument=json.dumps(
                {
                    "Version": "2012-10-17",
                    "Statement": [
                        {
                            "Effect": "Allow",
                            "Action": "s3:PutObject",
                            "Resource": f"arn:aws:s3:::{bucket}/quarantine/stream/*",
                        },
                        {
                            "Effect": "Allow",
                            "Action": ["logs:CreateLogStream", "logs:PutLogEvents"],
                            "Resource": f"arn:aws:logs:{session.region_name}:{expected_account}:log-group:{log_group}:*",
                        },
                    ],
                }
            ),
        )
        policy_created = True
        logs.create_log_group(logGroupName=log_group, tags=tags)
        log_created = True
        logs.put_retention_policy(logGroupName=log_group, retentionInDays=1)
        # Retry only IAM propagation, with a hard deadline. No reserved concurrency:
        # new AWS accounts may have only ten total/unreserved executions.
        for attempt in range(12):
            try:
                lam.create_function(
                    FunctionName=name,
                    Role=role,
                    Runtime="python3.11",
                    Handler="lambda_handler.handler",
                    Architectures=["x86_64"],
                    Code={"ZipFile": archive},
                    Timeout=30,
                    MemorySize=256,
                    Tags=tags,
                    Environment={"Variables": {"LAKE_BUCKET": bucket, "ENVIRONMENT": "test"}},
                )
                function_created = True
                break
            except ClientError as error:
                if (
                    error.response["Error"]["Code"] != "InvalidParameterValueException"
                    or attempt == 11
                ):
                    raise
                time.sleep(5)
        lam.get_waiter("function_active_v2").wait(
            FunctionName=name, WaiterConfig={"Delay": 2, "MaxAttempts": 60}
        )
        event = generate_events(1)[0]
        valid = envelope(json.dumps(event).encode(), event["event_id"], "1")
        assert invoke(lam, name, valid) == {"batchItemFailures": []}
        assert s3.list_objects_v2(Bucket=bucket).get("KeyCount", 0) == 0
        report["checks"]["valid_record_no_quarantine"] = "pass"
        malformed = b'{"broken":\xff\n'
        bad = envelope(malformed, "synthetic-malformed", "2")
        for _ in range(2):
            assert invoke(lam, name, bad) == {"batchItemFailures": []}
        objects = s3.list_objects_v2(Bucket=bucket, Prefix="quarantine/stream/")["Contents"]
        assert len(objects) == 1
        response = s3.get_object(Bucket=bucket, Key=objects[0]["Key"])
        try:
            evidence = json.loads(response["Body"].read())
        finally:
            response["Body"].close()
        assert evidence["kinesis_data_base64"] == bad["kinesis"]["data"]
        assert evidence["violations"]
        report["checks"]["malformed_bytes_retained"] = "pass"
        report["checks"]["repeat_invocation_stable_quarantine_key"] = "pass"
        # Existing second bucket is outside the role's exact PutObject grant.
        lam.update_function_configuration(
            FunctionName=name,
            Environment={"Variables": {"LAKE_BUCKET": denied_bucket, "ENVIRONMENT": "test"}},
        )
        lam.get_waiter("function_updated_v2").wait(
            FunctionName=name, WaiterConfig={"Delay": 2, "MaxAttempts": 60}
        )
        recovery_began = time.monotonic()
        assert invoke(lam, name, bad) == {"batchItemFailures": [{"itemIdentifier": "2"}]}
        assert s3.list_objects_v2(Bucket=denied_bucket).get("KeyCount", 0) == 0
        report["checks"]["unauthorized_bucket_not_acknowledged"] = "pass"
        lam.update_function_configuration(
            FunctionName=name,
            Environment={"Variables": {"LAKE_BUCKET": bucket, "ENVIRONMENT": "test"}},
        )
        lam.get_waiter("function_updated_v2").wait(
            FunctionName=name, WaiterConfig={"Delay": 2, "MaxAttempts": 60}
        )
        assert invoke(lam, name, bad) == {"batchItemFailures": []}
        report["checks"]["quarantine_recovery"] = "pass"
        report["recovery_seconds"] = time.monotonic() - recovery_began
        report["state"] = "pass"
    except BaseException as error:
        report["state"] = "fail"
        report["error_type"] = type(error).__name__
        raise
    finally:
        actions = []
        if function_created:
            actions.append(
                ("function", lambda: ignore_missing(lam.delete_function, FunctionName=name))
            )
        if policy_created:
            actions.append(
                (
                    "role_policy",
                    lambda: ignore_missing(iam.delete_role_policy, RoleName=name, PolicyName=name),
                )
            )
        if role_created:
            actions.append(("role", lambda: ignore_missing(iam.delete_role, RoleName=name)))
        for target in created_buckets:
            actions.append((target, lambda target=target: empty_bucket(s3, target)))
        if log_created:
            actions.append(
                ("logs", lambda: ignore_missing(logs.delete_log_group, logGroupName=log_group))
            )
        for label, action in actions:
            try:
                action()
                report["cleanup"][
                    label
                    if not label.startswith(name)
                    else ("denied_bucket" if label.endswith("-deny") else "lake_bucket")
                ] = "deleted"
            except Exception as error:
                report["state"] = "cleanup_failed"
                report["cleanup"][label] = type(error).__name__
        report["elapsed_seconds_including_cleanup"] = time.monotonic() - began
        (directory / "report.json").write_text(json.dumps(report, indent=2) + "\n")
    if report["state"] != "pass":
        raise RuntimeError(
            "Cleanup incomplete; inspect the private resources.json recovery inventory"
        )
    return report


def main() -> None:
    import boto3
    from botocore.config import Config

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--expected-account", required=True)
    parser.add_argument("--region", default="eu-west-1")
    parser.add_argument("--package", type=Path, default=Path("dist/lambda.zip"))
    parser.add_argument("--directory", type=Path, required=True)
    parser.add_argument("--allow-create-and-destroy", action="store_true", required=True)
    args = parser.parse_args()
    session = boto3.Session(region_name=args.region)
    original_client = session.client
    session.client = lambda service: original_client(
        service,
        config=Config(retries={"total_max_attempts": 2}, connect_timeout=10, read_timeout=60),
    )
    print(
        json.dumps(verify(session, args.expected_account, args.package, args.directory), indent=2)
    )


if __name__ == "__main__":
    main()
