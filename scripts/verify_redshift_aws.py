#!/usr/bin/env python3
"""Execute repository COPY/MERGE SQL, replay, rollback and tombstones in AWS.

Requires a fresh disposable commerce schema. Does not provision resources.
"""

import argparse
import hashlib
import json
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from benchmark_redshift import Session, copy_sql, literal

ROOT = Path(__file__).resolve().parents[1]


def assert_copy_matches(source, api_rows):
    table = pq.read_table(source)
    decoded = []
    for fields in api_rows:
        assert len(fields) == len(table.schema), "Warehouse column count differs from contract"
        row = {}
        for column, field in zip(table.schema, fields, strict=True):
            value = None if field.get("isNull") else next(iter(field.values()))
            if value is not None and pa.types.is_timestamp(column.type):
                value = datetime.fromisoformat(value)
                value = value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)
            row[column.name] = value
        decoded.append(row)
    assert decoded == sorted(table.to_pylist(), key=lambda row: row["shipment_id"]), (
        "COPY changed source values"
    )


def ddl_statements(text):
    """The repository DDL has no procedural bodies or quoted semicolons."""
    return [
        statement.strip()
        for statement in "\n".join(
            line for line in text.splitlines() if not line.lstrip().startswith("--")
        ).split(";")
        if statement.strip()
    ]


def verify(session, account, outputs, snapshot, directory):
    if session.client("sts").get_caller_identity()["Account"] != account:
        raise ValueError("Wrong AWS account")
    workgroup = outputs["redshift_workgroup"]
    if not workgroup.endswith("-test"):
        raise ValueError("Use a fresh isolated test workgroup")
    if outputs["lake_bucket"] != f"{workgroup}-{account}-{session.region_name}":
        raise ValueError("Lake bucket does not match the isolated test workgroup")
    directory.mkdir(parents=True, exist_ok=False)
    source = Path(snapshot["artifacts"]["shipment_lifecycle"]["path"])
    expected = pq.read_table(source).num_rows
    if expected < 2:
        raise ValueError("Use a nontrivial synthetic shipment population")
    digest = hashlib.sha256(source.read_bytes()).hexdigest()
    bucket = outputs["lake_bucket"]
    key = f"gold/acceptance-redshift/{digest}.parquet"
    manifest_key = f"manifests/acceptance-redshift/{digest}.json"
    s3 = session.client("s3")
    s3.upload_file(str(source), bucket, key)
    manifest = json.dumps(
        {
            "entries": [
                {
                    "url": f"s3://{bucket}/{key}",
                    "mandatory": True,
                    "meta": {"content_length": source.stat().st_size},
                }
            ]
        }
    )
    s3.put_object(Bucket=bucket, Key=manifest_key, Body=manifest.encode())
    namespace = session.client("redshift-serverless").get_namespace(namespaceName=workgroup)[
        "namespace"
    ]
    role = namespace["defaultIamRoleArn"]
    runner = Session(
        session.client("redshift-data"),
        workgroup,
        "commerce",
        directory / "statements.jsonl",
        timeout_seconds=300,
    )
    report = {
        "state": "running",
        "scope": __doc__.strip(),
        "checks": {},
        "source_rows": expected,
        "source_sha256": digest,
        "region": session.region_name,
    }

    def execute(sql, label):
        return runner.execute(sql, phase="acceptance", label=label)

    def scalar(sql, label):
        return next(iter(execute(sql, label)["rows"][0][0].values()))

    def expect_failure(sql, label, message):
        try:
            execute(sql, label)
        except RuntimeError as error:
            assert message in str(error), f"Unexpected failure for {label}: {error}"
            return
        raise AssertionError(f"Expected failure did not occur: {label}")

    def publish(release, source_hash=digest, count=expected):
        return f"CALL commerce.sp_publish_shipment_release({literal(release)}, {literal(source_hash)}, {count})"

    def data_hash(label):
        return execute("SELECT * FROM commerce.shipment_lifecycle ORDER BY shipment_id", label)[
            "result_sha256"
        ]

    try:
        assert (
            scalar(
                "SELECT count(*) FROM information_schema.schemata WHERE schema_name='commerce'",
                "fresh_schema",
            )
            == 0
        )
        for index, statement in enumerate(
            ddl_statements((ROOT / "sql/redshift/ddl.sql").read_text())
        ):
            execute(statement, f"ddl_{index}")
        execute((ROOT / "sql/redshift/load_merge.sql").read_text(), "create_publish_procedure")
        execute(
            copy_sql(
                "stage_shipment_lifecycle", f"s3://{bucket}/{manifest_key}", role, temporary=True
            ),
            "copy",
        )
        execute(
            "INSERT INTO affected_shipments SELECT shipment_id FROM stage_shipment_lifecycle",
            "scope",
        )
        execute(publish("initial"), "initial_publish")
        assert (
            scalar("SELECT count(*) FROM commerce.shipment_lifecycle", "published_count")
            == expected
        )
        original = data_hash("published_data")
        assert_copy_matches(source, runner.records[-1]["rows"])
        report["checks"]["all_copied_values_match_parquet_source"] = "pass"
        execute(publish("initial"), "idempotent_replay")
        assert data_hash("replayed_data") == original
        assert scalar("SELECT count(*) FROM commerce.load_ledger", "replayed_ledger") == 1
        report["checks"]["copy_merge_and_exact_release_replay"] = "pass"
        expect_failure(publish("initial", "f" * 64), "conflicting_release", "Release ID reused")
        expect_failure(
            publish("wrong-count", count=expected + 1), "invalid_count", "Staged row count mismatch"
        )
        execute(
            "INSERT INTO stage_shipment_lifecycle SELECT * FROM stage_shipment_lifecycle LIMIT 1",
            "inject_duplicate",
        )
        expect_failure(
            publish("duplicate", count=expected + 1),
            "duplicate_keys",
            "Duplicate source shipment keys",
        )
        execute("DELETE FROM stage_shipment_lifecycle", "reset_stage")
        execute(
            copy_sql(
                "stage_shipment_lifecycle", f"s3://{bucket}/{manifest_key}", role, temporary=True
            ),
            "restore_stage",
        )
        assert data_hash("after_rejections") == original
        assert scalar("SELECT count(*) FROM commerce.load_ledger", "rejected_ledger") == 1
        report["checks"]["release_conflicts_control_totals_duplicate_keys"] = "pass"
        # Nested atomic failure occurs AFTER the real procedure has merged and
        # inserted its ledger row. Both changes must roll back together.
        execute("UPDATE stage_shipment_lifecycle SET state='delayed'", "inject_update")
        execute(
            """CREATE OR REPLACE PROCEDURE commerce.sp_acceptance_fail_after_publish() AS $$
            BEGIN """
            + publish("rollback")
            + "; RAISE EXCEPTION 'acceptance injected failure after publish'; END; $$ LANGUAGE plpgsql",
            "rollback_wrapper",
        )
        expect_failure(
            "CALL commerce.sp_acceptance_fail_after_publish()",
            "rollback_after_merge",
            "acceptance injected failure",
        )
        assert data_hash("after_rollback") == original
        assert scalar("SELECT count(*) FROM commerce.load_ledger", "rollback_ledger") == 1
        report["checks"]["atomic_rollback_after_merge_and_ledger_write"] = "pass"
        # Scope still contains the full old population; omission is a tombstone.
        execute(
            "DELETE FROM stage_shipment_lifecycle WHERE shipment_id=(SELECT min(shipment_id) FROM stage_shipment_lifecycle)",
            "stage_tombstone",
        )
        execute(publish("tombstone", "e" * 64, expected - 1), "publish_tombstone")
        assert (
            scalar("SELECT count(*) FROM commerce.shipment_lifecycle", "tombstone_count")
            == expected - 1
        )
        report["checks"]["affected_key_tombstone_delete"] = "pass"
        # Restore original data for reproducible benchmark scripts.
        execute("DELETE FROM stage_shipment_lifecycle", "reset_original_stage")
        execute(
            copy_sql(
                "stage_shipment_lifecycle", f"s3://{bucket}/{manifest_key}", role, temporary=True
            ),
            "recopy_original",
        )
        execute(publish("restore"), "restore_release")
        assert data_hash("restored_data") == original
        report["checks"]["historical_release_restoration"] = "pass"

        def concurrent_load(index):
            other = Session(
                session.client("redshift-data"),
                workgroup,
                "commerce",
                directory / f"concurrent-{index}.jsonl",
                timeout_seconds=300,
            )
            for statement in (
                "CREATE TEMP TABLE stage_shipment_lifecycle (LIKE commerce.shipment_lifecycle)",
                "CREATE TEMP TABLE affected_shipments (shipment_id VARCHAR(100) NOT NULL)",
                copy_sql(
                    "stage_shipment_lifecycle",
                    f"s3://{bucket}/{manifest_key}",
                    role,
                    temporary=True,
                ),
                "INSERT INTO affected_shipments SELECT shipment_id FROM stage_shipment_lifecycle",
                publish("concurrent"),
            ):
                other.execute(statement, phase="concurrent", label=f"session_{index}")

        with ThreadPoolExecutor(max_workers=2) as pool:
            list(pool.map(concurrent_load, range(2)))
        assert (
            scalar(
                "SELECT count(*) FROM commerce.load_ledger WHERE release_id='concurrent'",
                "concurrent_ledger",
            )
            == 1
        )
        assert data_hash("after_concurrent_load") == original
        report["checks"]["concurrent_same_release_session_isolation"] = "pass"
        report["state"] = "pass"
        report["copy_engine_seconds"] = next(
            r["engine_seconds"] for r in runner.records if r["label"] == "copy"
        )
        report["initial_merge_engine_seconds"] = next(
            r["engine_seconds"] for r in runner.records if r["label"] == "initial_publish"
        )
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
    parser.add_argument("--snapshot", type=Path, required=True)
    parser.add_argument("--directory", type=Path, required=True)
    parser.add_argument("--allow-warehouse-writes", action="store_true", required=True)
    args = parser.parse_args()
    print(
        json.dumps(
            verify(
                boto3.Session(region_name=args.region),
                args.expected_account,
                json.loads(args.outputs.read_text())["platform"]["value"],
                json.loads(args.snapshot.read_text()),
                args.directory,
            ),
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
