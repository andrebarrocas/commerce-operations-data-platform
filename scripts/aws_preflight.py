#!/usr/bin/env python3
"""Read-only, account-guarded deployment preflight. Never activates paid services."""

import argparse
import json
import re
from datetime import UTC, datetime
from pathlib import Path

from botocore.exceptions import BotoCoreError, ClientError

PROBES = {
    "s3": ("list_buckets", {}),
    "lambda": ("get_account_settings", {}),
    "kinesis": ("list_streams", {"Limit": 1}),
    "firehose": ("list_delivery_streams", {"Limit": 1}),
    "emr-serverless": ("list_applications", {"maxResults": 1}),
    "redshift-serverless": ("list_workgroups", {"maxResults": 1}),
}


def preflight(session, expected_account: str) -> dict:
    if not re.fullmatch(r"[0-9]{12}", expected_account):
        raise ValueError("Expected an exact 12-digit AWS account ID")
    identity = session.client("sts").get_caller_identity()
    if identity["Account"] != expected_account:
        raise ValueError("AWS identity does not match the explicitly selected account")
    results = {}
    for service, (operation, arguments) in PROBES.items():
        try:
            getattr(session.client(service), operation)(**arguments)
            results[service] = {"state": "accessible", "operation": operation}
        except ClientError as error:
            # Do not serialize raw responses, resource inventories, IDs or credentials.
            results[service] = {
                "state": "blocked",
                "operation": operation,
                "error_code": error.response["Error"]["Code"],
            }
        except BotoCoreError as error:
            results[service] = {
                "state": "unavailable",
                "operation": operation,
                "error_code": type(error).__name__,
            }
    return {
        "observed_at": datetime.now(UTC).isoformat(),
        "region": session.region_name,
        "account_verified": True,
        "scope": "Read-only service access probes; not deployment acceptance",
        "state": "pass" if all(r["state"] == "accessible" for r in results.values()) else "blocked",
        "services": results,
        "resources_created": 0,
    }


def main() -> None:
    import boto3
    from botocore.config import Config

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--expected-account", required=True)
    parser.add_argument("--region", default="eu-west-1")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    session = boto3.Session(region_name=args.region)
    # Bounded network attempts; subscription failures must not become a retry storm.
    original_client = session.client
    session.client = lambda service: original_client(
        service,
        config=Config(retries={"total_max_attempts": 2}, connect_timeout=10, read_timeout=15),
    )
    report = preflight(session, args.expected_account)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report, indent=2))
    raise SystemExit(0 if report["state"] == "pass" else 2)


if __name__ == "__main__":
    main()
