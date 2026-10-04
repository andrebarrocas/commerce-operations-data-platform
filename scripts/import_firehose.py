#!/usr/bin/env python3
"""Explicit read-only S3 import; uses the normal AWS SDK credential chain."""

import argparse
import json
from pathlib import Path

from commerce_platform.firehose import capture_object


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Capture one Firehose S3 object. No bucket enumeration or cloud writes."
    )
    parser.add_argument("--bucket", required=True, help="Exact source S3 bucket name")
    parser.add_argument("--key", required=True, help="Exact source S3 object key")
    parser.add_argument(
        "--output", type=Path, required=True, help="Immutable output JSONL capture file"
    )
    args = parser.parse_args()
    import boto3
    from botocore.config import Config

    client = boto3.client("s3", config=Config(retries={"max_attempts": 3, "mode": "standard"}))
    print(json.dumps(capture_object(client, args.bucket, args.key, args.output), indent=2))


if __name__ == "__main__":
    main()
