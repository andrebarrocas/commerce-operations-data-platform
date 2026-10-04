#!/usr/bin/env python3
"""Run a paid, explicitly invoked Redshift Serverless experiment; never fabricate output."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import random
import re
import statistics
import time
import uuid
from datetime import date, datetime
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
IDENTIFIER = re.compile(r"^[a-z][a-z0-9_]{0,62}$")
VARIANTS = {
    "auto": "DISTSTYLE AUTO SORTKEY AUTO ENCODE AUTO",
    "even": "DISTSTYLE EVEN SORTKEY (dispatched_at) ENCODE AUTO",
    "key": "DISTSTYLE KEY DISTKEY (order_id) SORTKEY (dispatched_at, order_id) ENCODE AUTO",
}


def identifier(value: str) -> str:
    if not IDENTIFIER.fullmatch(value):
        raise ValueError(f"Unsupported SQL identifier: {value!r}")
    return value


def qualified(value: str) -> str:
    parts = value.split(".")
    if len(parts) != 2:
        raise ValueError("Table must be schema.table")
    return ".".join(identifier(part) for part in parts)


def literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def table_ddl(name: str, variant: str) -> str:
    qualified(name)
    types = {
        "string": "VARCHAR(100)",
        "int64": "BIGINT",
        "float64": "DOUBLE PRECISION",
        "boolean": "BOOLEAN",
        "timestamp": "TIMESTAMPTZ",
        "date": "DATE",
    }
    contract = json.loads((ROOT / "contracts/products/shipment_lifecycle.json").read_text())
    columns = [
        f"{identifier(c['name'])} {types[c['type']]}" + (" NOT NULL" if not c["nullable"] else "")
        for c in contract["columns"]
    ]
    return f"CREATE TABLE {name} ({', '.join(columns)}) {VARIANTS[variant]}"


def copy_sql(table: str, manifest: str, iam_role: str) -> str:
    qualified(table)
    if not re.fullmatch(r"s3://[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]/[^\x00-\x1f]+", manifest):
        raise ValueError("Expected a complete S3 manifest URI")
    if not re.fullmatch(
        r"arn:aws(?:-cn|-us-gov)?:iam::[0-9]{12}:role/[A-Za-z0-9_+=,.@/-]+", iam_role
    ):
        raise ValueError("Expected an IAM role ARN")
    return f"COPY {table} FROM {literal(manifest)} IAM_ROLE {literal(iam_role)} FORMAT AS PARQUET MANIFEST"


def queries(table: str, start: date, end: date) -> dict[str, str]:
    qualified(table)
    bounds = f"dispatched_at >= '{start.isoformat()}' AND dispatched_at < '{end.isoformat()}'"
    return {
        "carrier_daily": f"""SELECT carrier_id, country, delivered_at::DATE AS day,
            count(*) AS shipments, sum(CASE WHEN delivered_on_time THEN 1 ELSE 0 END) AS on_time,
            sum(dispatch_to_delivery_seconds::DECIMAL(24,6)) AS transit_seconds
            FROM {table} WHERE {bounds} AND delivered_at IS NOT NULL
            GROUP BY carrier_id, country, delivered_at::DATE ORDER BY carrier_id, country, day""",
        "order_join": f"""WITH orders AS (SELECT order_id, count(*) AS package_shipments
            FROM {table} GROUP BY order_id)
            SELECT s.state, count(*) AS shipments, sum(o.package_shipments) AS order_fanout
            FROM {table} s JOIN orders o ON s.order_id = o.order_id
            WHERE s.{bounds} GROUP BY s.state ORDER BY s.state""",
        "time_filter": f"""SELECT state, count(*) AS shipments FROM {table}
            WHERE {bounds} GROUP BY state ORDER BY state""",
    }


class Session:
    """Serial session reuse keeps cache and timeout settings on every measured query."""

    def __init__(
        self,
        client: Any,
        workgroup: str,
        database: str,
        evidence: Path,
        timeout_seconds: float = 1800,
        poll_seconds: float = 0.5,
    ) -> None:
        self.client = client
        self.workgroup = workgroup
        self.database = database
        self.evidence = evidence
        self.timeout_seconds = timeout_seconds
        self.poll_seconds = poll_seconds
        self.session_id: str | None = None
        self.records: list[dict[str, Any]] = []

    def execute(self, sql: str, *, phase: str, label: str) -> dict[str, Any]:
        request: dict[str, Any] = {
            "Sql": sql,
            "SessionKeepAliveSeconds": 3600,
            "ClientToken": str(uuid.uuid4()),
        }
        if self.session_id:
            request["SessionId"] = self.session_id
        else:
            request.update(WorkgroupName=self.workgroup, Database=self.database)
        begin = time.monotonic()
        response = self.client.execute_statement(**request)
        self.session_id = response.get("SessionId", self.session_id)
        statement_id = response["Id"]
        status: dict[str, Any] = {}
        timed_out = False
        while True:
            status = self.client.describe_statement(Id=statement_id)
            self.session_id = status.get("SessionId", self.session_id)
            if status["Status"] in {"FINISHED", "FAILED", "ABORTED"}:
                break
            if time.monotonic() - begin > self.timeout_seconds:
                self.client.cancel_statement(Id=statement_id)
                timed_out = True
                break
            time.sleep(self.poll_seconds)
        wall = time.monotonic() - begin
        record: dict[str, Any] = {
            "phase": phase,
            "label": label,
            "sql": sql,
            "sql_sha256": hashlib.sha256(sql.encode()).hexdigest(),
            "statement_id": statement_id,
            "session_id": self.session_id,
            "client_wall_seconds": wall,
            "engine_seconds": status.get("Duration", 0) / 1e9 if "Duration" in status else None,
            "redshift_query_id": status.get("RedshiftQueryId"),
            "status": status["Status"],
            "timed_out": timed_out,
            "describe": status,
        }
        if status["Status"] == "FINISHED" and status.get("HasResultSet"):
            pages = []
            request_result: dict[str, Any] = {"Id": statement_id}
            while True:
                page = self.client.get_statement_result(**request_result)
                pages.extend(page["Records"])
                if "NextToken" not in page:
                    break
                request_result["NextToken"] = page["NextToken"]
            record["rows"] = pages
            record["result_sha256"] = hashlib.sha256(
                json.dumps(pages, sort_keys=True).encode()
            ).hexdigest()
        self.records.append(record)
        with self.evidence.open("a") as handle:
            handle.write(json.dumps(record, default=str, sort_keys=True) + "\n")
        if timed_out or status["Status"] != "FINISHED":
            raise RuntimeError(
                f"{label}: {status['Status']}; timeout={timed_out}; {status.get('Error', '')}"
            )
        if not self.session_id:
            raise RuntimeError(
                "Data API did not return a reusable session; cache control cannot be proven"
            )
        return record


def sample_summary(records: list[dict[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    labels = sorted({row["label"] for row in records if row["phase"] == "trial"})
    for label in labels:
        rows = [row for row in records if row["phase"] == "trial" and row["label"] == label]
        measures = {}
        for key in ("engine_seconds", "client_wall_seconds"):
            values = sorted(row[key] for row in rows if row[key] is not None)
            measures[key] = (
                {
                    "median": statistics.median(values),
                    "p95_nearest_rank": values[max(0, (95 * len(values) + 99) // 100 - 1)],
                    "samples": len(values),
                }
                if values
                else None
            )
        result[label] = measures
    return result


def run(args: argparse.Namespace, client: Any) -> dict[str, Any]:
    schema = identifier(args.schema)
    if not schema.startswith("commerce_bench_"):
        raise ValueError(
            "Use a fresh schema beginning commerce_bench_; existing schemas are never reused"
        )
    if args.trials < 2 or args.warmups < 1:
        raise ValueError("At least two measured trials and one warmup are required")
    start, end = date.fromisoformat(args.from_date), date.fromisoformat(args.to_date)
    if args.base_table:
        qualified(args.base_table)
    else:
        copy_sql(f"{schema}.base", args.s3_manifest, args.iam_role)
    if end <= start:
        raise ValueError("Date interval must be nonempty and half-open")
    args.output.mkdir(parents=True, exist_ok=False)
    metadata = vars(args).copy()
    metadata.update(
        started_at=datetime.now().astimezone().isoformat(),
        cloud_execution_requested=True,
        result_cache="disabled within retained SessionId",
        query_concurrency=1,
        cache_scope="Result cache off; buffer/storage caches are not flushed",
    )
    (args.output / "configuration.json").write_text(json.dumps(metadata, default=str, indent=2))
    session = Session(client, args.workgroup, args.database, args.output / "statements.jsonl")
    session.execute(
        "SET enable_result_cache_for_session TO off", phase="setup", label="disable_cache"
    )
    cache = session.execute(
        "SHOW enable_result_cache_for_session", phase="setup", label="verify_cache"
    )
    value = next(iter(cache["rows"][0][0].values()))
    if str(value).lower() not in {"off", "false", "0"}:
        raise RuntimeError("Result cache setting was not disabled")
    session.execute("SET statement_timeout TO 1800000", phase="setup", label="timeout")
    session.execute("SET timezone TO 'UTC'", phase="setup", label="utc_clock")
    session.execute(f"CREATE SCHEMA {schema}", phase="setup", label="create_isolated_schema")
    session.execute(
        f"SET search_path TO {schema}, public", phase="setup", label="diagnostic_search_path"
    )
    base = f"{schema}.base"
    session.execute(table_ddl(base, "auto"), phase="setup", label="create_base")
    if args.base_table:
        columns = json.loads((ROOT / "contracts/products/shipment_lifecycle.json").read_text())[
            "columns"
        ]
        names = ", ".join(identifier(column["name"]) for column in columns)
        load = f"INSERT INTO {base} SELECT {names} FROM {qualified(args.base_table)}"
    else:
        load = copy_sql(base, args.s3_manifest, args.iam_role)
    session.execute(load, phase="load", label="load_one_immutable_base")
    totals = session.execute(
        f"SELECT count(*), count(DISTINCT shipment_id) FROM {base}",
        phase="validation",
        label="base_counts",
    )
    cells = totals["rows"][0]
    count, distinct_count = (next(iter(cell.values())) for cell in cells)
    if count == 0 or count != distinct_count:
        raise ValueError("Benchmark input must have nonzero rows and unique shipment_id")
    selected = session.execute(
        f"SELECT count(*) FROM {base} WHERE dispatched_at >= '{start.isoformat()}' AND dispatched_at < '{end.isoformat()}'",
        phase="validation",
        label="selected_population",
    )
    if next(iter(selected["rows"][0][0].values())) == 0:
        raise ValueError(
            "Selected date interval contains no shipments; choose a meaningful workload"
        )
    workloads: dict[str, dict[str, str]] = {}
    for variant in VARIANTS:
        table = f"{schema}.{variant}"
        session.execute(table_ddl(table, variant), phase="setup", label=f"create_{variant}")
        session.execute(
            f"INSERT INTO {table} SELECT * FROM {base}", phase="load", label=f"load_{variant}"
        )
        session.execute(f"ANALYZE {table}", phase="setup", label=f"analyze_{variant}")
        workloads[variant] = queries(table, start, end)
        for name, sql in workloads[variant].items():
            session.execute("EXPLAIN " + sql, phase="explain", label=f"{variant}/{name}")
    try:
        session.execute(
            f"SELECT * FROM svv_table_info WHERE \"schema\" = '{schema}' ORDER BY table_id",
            phase="diagnostic",
            label="physical_tables_before_trials",
        )
    except RuntimeError as error:
        (args.output / "physical_tables_before_trials_unavailable.txt").write_text(str(error))
    rng = random.Random(args.seed)
    expected: dict[str, str] = {}
    for phase, repetitions in (("warmup", args.warmups), ("trial", args.trials)):
        for _ in range(repetitions):
            schedule = [(variant, name) for variant in VARIANTS for name in workloads[variant]]
            rng.shuffle(schedule)
            for variant, name in schedule:
                record = session.execute(
                    workloads[variant][name], phase=phase, label=f"{variant}/{name}"
                )
                digest = record["result_sha256"]
                if name in expected and expected[name] != digest:
                    raise RuntimeError(f"Query result mismatch across variants: {name}")
                expected[name] = digest
    # A materialized-view experiment uses the same base data, separately timed.
    mv_select = f"SELECT carrier_id, country, count(*) AS delivered_shipments FROM {schema}.auto WHERE delivered_at IS NOT NULL GROUP BY carrier_id, country"
    session.execute(
        f"CREATE MATERIALIZED VIEW {schema}.carrier_mv AUTO REFRESH NO AS {mv_select}",
        phase="mv",
        label="create_mv",
    )
    session.execute(
        f"REFRESH MATERIALIZED VIEW {schema}.carrier_mv", phase="mv", label="refresh_mv"
    )
    direct = session.execute(
        mv_select + " ORDER BY carrier_id, country", phase="mv", label="base_aggregate"
    )
    materialized = session.execute(
        f"SELECT * FROM {schema}.carrier_mv ORDER BY carrier_id, country",
        phase="mv",
        label="mv_aggregate",
    )
    if direct["result_sha256"] != materialized["result_sha256"]:
        raise RuntimeError("Materialized-view aggregate differs from base")
    diagnostics = {
        "physical_tables": f"SELECT * FROM svv_table_info WHERE \"schema\" = '{schema}' ORDER BY table_id",
        "column_encoding": f"SELECT * FROM pg_table_def WHERE schemaname = '{schema}' ORDER BY tablename, sortkey, \"column\"",
        "query_history": "SELECT * FROM sys_query_history WHERE query_id IN ("
        + ",".join(
            str(int(row["redshift_query_id"]))
            for row in session.records
            if row["phase"] == "trial" and row["redshift_query_id"] is not None
        )
        + ") ORDER BY query_id",
    }
    for name, sql in diagnostics.items():
        try:
            session.execute(sql, phase="diagnostic", label=name)
        except RuntimeError as error:
            # Restricted system-view access is recorded, never replaced with zeros.
            (args.output / f"{name}_unavailable.txt").write_text(str(error))
    result = {
        "row_count": count,
        "schema": schema,
        "summaries": sample_summary(session.records),
        "workload_result_hashes": expected,
        "cloud_experiment_completed": True,
        "cleanup_sql": f"DROP SCHEMA {schema} CASCADE;",
    }
    (args.output / "summary.json").write_text(json.dumps(result, indent=2))
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--workgroup", default=os.getenv("REDSHIFT_WORKGROUP"))
    parser.add_argument("--database", default=os.getenv("REDSHIFT_DATABASE"))
    parser.add_argument("--region", default=os.getenv("AWS_REGION"))
    parser.add_argument("--schema", required=True)
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--base-table")
    source.add_argument("--s3-manifest")
    parser.add_argument("--iam-role", default=os.getenv("REDSHIFT_COPY_ROLE_ARN"))
    parser.add_argument(
        "--dataset-id", required=True, help="Source manifest SHA256 or retained dataset identity"
    )
    parser.add_argument(
        "--seed", type=int, default=42, help="Randomizes trial order only; source data stays fixed"
    )
    parser.add_argument("--from-date", required=True)
    parser.add_argument("--to-date", required=True)
    parser.add_argument("--trials", type=int, default=5)
    parser.add_argument("--warmups", type=int, default=1)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if not args.workgroup or not args.database:
        parser.error("Set workgroup and database via arguments or environment")
    if args.s3_manifest and not args.iam_role:
        parser.error("An S3 manifest requires --iam-role or REDSHIFT_COPY_ROLE_ARN")
    import boto3
    from botocore.config import Config

    client = boto3.client(
        "redshift-data",
        region_name=args.region,
        config=Config(retries={"mode": "standard", "max_attempts": 5}),
    )
    try:
        serverless = boto3.client("redshift-serverless", region_name=args.region)
        group = serverless.get_workgroup(workgroupName=args.workgroup)["workgroup"]
        args.workgroup_configuration = {
            key: group.get(key)
            for key in ("baseCapacity", "maxCapacity", "configParameters", "status")
        }
    except Exception as error:
        args.workgroup_configuration = {"status": "unavailable", "reason": type(error).__name__}
    result = run(args, client)
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
