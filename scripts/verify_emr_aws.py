#!/usr/bin/env python3
"""Execute the actual Spark entry point on EMR and compare all projection rows.

Uploads an already reconciled local Silver snapshot. Does not claim distributed
raw validation or transactional S3/Glue product publication.
"""

import argparse
import gzip
import hashlib
import json
import time
import uuid
from datetime import UTC, datetime
from pathlib import Path

import duckdb
import pyarrow.dataset as ds

ROOT = Path(__file__).resolve().parents[1]


def logical_rows(rows):
    def normalize(value):
        if isinstance(value, datetime):
            return (
                value.replace(tzinfo=UTC).isoformat()
                if value.tzinfo is None
                else value.astimezone(UTC).isoformat()
            )
        return value

    return sorted(
        json.dumps({k: normalize(v) for k, v in r.items()}, sort_keys=True, default=str)
        for r in rows
    )


def compare(snapshot, output):
    """Compare EMR's business columns with independently built SQL products."""
    connection = duckdb.connect()
    try:
        for name in (
            "fact_order_line",
            "order_fulfillment",
            "shipment_lifecycle",
            "facility_performance",
        ):
            connection.read_parquet(snapshot["artifacts"][name]["path"]).create_view(name)
        queries = {
            "fact_order_line": """SELECT l.order_id, o.country, o.order_created_at AS ordered_at,
                l.currency, l.line_item_id, l.sku_id, l.quantity, l.unit_price_minor, l.line_total_minor
                FROM fact_order_line l JOIN order_fulfillment o USING (order_id)""",
            "shipment_summary": """SELECT shipment_id, order_id, origin_facility_id AS facility_id,
                carrier_id, country, dispatched_at, delivered_at,
                failed_delivery_attempts AS failed_attempts, dispatch_to_delivery_seconds,
                CASE state WHEN 'dispatched' THEN 'SHIPMENT_DISPATCHED' WHEN 'delayed' THEN 'SHIPMENT_DELAYED'
                WHEN 'out_for_delivery' THEN 'OUT_FOR_DELIVERY' WHEN 'delivery_failed' THEN 'DELIVERY_FAILED'
                WHEN 'delivered' THEN 'DELIVERED' END AS last_event_type
                FROM shipment_lifecycle""",
            "facility_hour": """SELECT facility_id, window_start, facility_volume_1h AS event_count,
                shipments_dispatched, window_start::DATE AS event_date,
                coalesce(sum(shipments_dispatched) OVER (PARTITION BY facility_id ORDER BY window_start
                RANGE BETWEEN INTERVAL 24 HOURS PRECEDING AND INTERVAL 1 MICROSECOND PRECEDING), 0)::BIGINT
                AS preceding_24h_dispatches FROM facility_performance""",
        }
        counts = {}
        for name, query in queries.items():
            expected = connection.execute(query).to_arrow_table().to_pylist()
            actual = (
                ds.dataset(output / name, format="parquet", partitioning="hive")
                .to_table()
                .to_pylist()
            )
            if name == "shipment_summary":
                # Observation availability is separately verified from canonical Silver.
                actual = [
                    {k: v for k, v in row.items() if k != "last_observed_at"} for row in actual
                ]
            assert logical_rows(actual) == logical_rows(expected), (
                f"Spark/SQL business rows differ: {name}"
            )
            counts[name] = len(actual)
        return counts
    finally:
        connection.close()


def verify(session, account, outputs, snapshot, directory):
    if session.client("sts").get_caller_identity()["Account"] != account:
        raise ValueError("Wrong AWS account")
    if not outputs["stream_name"].endswith("-test-events"):
        raise ValueError("Use an isolated test deployment")
    if (
        outputs["lake_bucket"]
        != f"{outputs['stream_name'].removesuffix('-events')}-{account}-{session.region_name}"
    ):
        raise ValueError("Lake bucket does not match the isolated test deployment")
    directory.mkdir(parents=True, exist_ok=False)
    client, s3 = session.client("emr-serverless"), session.client("s3")
    bucket = outputs["lake_bucket"]
    token = uuid.uuid4().hex
    source_key, script_key = (
        f"silver/acceptance/{token}/events.parquet",
        f"artifacts/{token}/curate.py",
    )
    output_key = f"gold/spark-acceptance/{token}"
    script = ROOT / "spark/curate.py"
    s3.upload_file(snapshot["artifacts"]["silver_events"]["path"], bucket, source_key)
    s3.upload_file(str(script), bucket, script_key)
    report = {
        "state": "running",
        "scope": __doc__.strip(),
        "region": session.region_name,
        "entrypoint_sha256": hashlib.sha256(script.read_bytes()).hexdigest(),
    }
    application = outputs["emr_application_id"]
    began, job = time.monotonic(), None
    try:
        job = client.start_job_run(
            applicationId=application,
            clientToken=token,
            executionRoleArn=outputs["emr_execution_role_arn"],
            executionTimeoutMinutes=10,
            jobDriver={
                "sparkSubmit": {
                    "entryPoint": f"s3://{bucket}/{script_key}",
                    "entryPointArguments": [
                        "--input",
                        f"s3://{bucket}/{source_key}",
                        "--output",
                        f"s3://{bucket}/{output_key}",
                        "--metrics",
                        "/tmp/commerce-metrics.json",
                    ],
                    "sparkSubmitParameters": "--conf spark.executor.instances=1 --conf spark.executor.cores=4 --conf spark.executor.memory=8g --conf spark.driver.cores=4 --conf spark.driver.memory=8g --conf spark.dynamicAllocation.enabled=false --conf spark.sql.shuffle.partitions=4",
                }
            },
            name="commerce-acceptance",
        )
        report["job_run_id"] = job["jobRunId"]
        deadline = time.monotonic() + 900
        while time.monotonic() < deadline:
            status = client.get_job_run(applicationId=application, jobRunId=job["jobRunId"])[
                "jobRun"
            ]
            report["job_state"] = status["state"]
            if status["state"] in {"SUCCESS", "FAILED", "CANCELLED"}:
                report["job_details"] = {
                    key: status.get(key)
                    for key in (
                        "releaseLabel",
                        "totalExecutionDurationSeconds",
                        "resourceUtilization",
                        "billedResourceUtilization",
                        "stateDetails",
                    )
                }
                if status["state"] != "SUCCESS":
                    raise RuntimeError(f"EMR {status['state']}: {status.get('stateDetails', '')}")
                break
            time.sleep(5)
        else:
            raise TimeoutError("EMR acceptance exceeded its wall-clock deadline")
        report["job_wall_seconds"] = time.monotonic() - began
        for page in s3.get_paginator("list_objects_v2").paginate(
            Bucket=bucket, Prefix=output_key + "/"
        ):
            for obj in page.get("Contents", []):
                if obj["Key"].endswith(".parquet"):
                    relative = obj["Key"][len(output_key) + 1 :]
                    path = directory / "outputs" / relative
                    path.parent.mkdir(parents=True, exist_ok=True)
                    s3.download_file(bucket, obj["Key"], str(path))
        report["output_rows_equal_to_local_sql"] = compare(snapshot, directory / "outputs")
        # Application S3 monitoring retains the driver's actual printed metrics.
        # Do not substitute EMR wall time for Spark's transformation runtime.
        for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix="emr-logs/"):
            for obj in page.get("Contents", []):
                if job["jobRunId"] not in obj["Key"] or "stdout" not in obj["Key"]:
                    continue
                if obj["Size"] > 5 * 1024 * 1024:
                    continue
                response = s3.get_object(Bucket=bucket, Key=obj["Key"])
                try:
                    raw = response["Body"].read()
                finally:
                    response["Body"].close()
                raw = gzip.decompress(raw) if raw.startswith(b"\x1f\x8b") else raw
                for line in raw.decode(errors="replace").splitlines():
                    if '"engine": "Apache Spark"' in line:
                        metrics = json.loads(line[line.index("{") :])
                        report["spark_metrics"] = {
                            key: metrics[key]
                            for key in (
                                "engine",
                                "spark_version",
                                "input_records",
                                "output_rows",
                                "runtime_seconds",
                                "scope",
                            )
                        }
        report["state"] = "pass"
    except BaseException as error:
        report["state"] = "fail"
        report["error_type"] = type(error).__name__
        if job and report.get("job_state") not in {"SUCCESS", "FAILED", "CANCELLED"}:
            client.cancel_job_run(applicationId=application, jobRunId=job["jobRunId"])
        raise
    finally:
        (directory / "report.json").write_text(json.dumps(report, indent=2, default=str) + "\n")
    return report


def main():
    import boto3

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--expected-account", required=True)
    parser.add_argument("--region", default="eu-west-1")
    parser.add_argument("--outputs", type=Path, required=True)
    parser.add_argument("--snapshot", type=Path, required=True)
    parser.add_argument("--directory", type=Path, required=True)
    parser.add_argument("--allow-job-submission", action="store_true", required=True)
    args = parser.parse_args()
    report = verify(
        boto3.Session(region_name=args.region),
        args.expected_account,
        json.loads(args.outputs.read_text())["platform"]["value"],
        json.loads(args.snapshot.read_text()),
        args.directory,
    )
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
