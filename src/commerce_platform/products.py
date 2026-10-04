"""Bounded DuckDB product builder; publication is owned by the lake release protocol.

Inputs are effective canonical events resolved at ``as_of``. The clock filter here
is defense in depth, not a substitute for resolving correction history at cutoff.
``updated_at`` is the deterministic snapshot cutoff; manifests record wall time.
"""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

ROOT = Path(__file__).resolve().parents[2]
PUBLIC_PRODUCTS = (
    "order_fulfillment",
    "shipment_lifecycle",
    "facility_performance",
    "carrier_performance",
    "delivery_reliability",
    "inventory_movements",
)
EXTRA_TABLES = (
    "fact_order_fulfillment",
    "fact_order_line",
    "fact_shipment_delivery",
    "fact_inventory_movement",
    "fact_facility_hour",
    "fact_delivery_attempt",
    "dim_facility",
    "dim_carrier",
    "dim_sku",
    "dim_date",
    "features_delivery",
    "features_inventory",
    "features_facility_anomaly",
)
EVENT_FIELDS = (
    "event_id",
    "event_type",
    "event_timestamp",
    "ingestion_timestamp",
    "order_id",
    "package_id",
    "shipment_id",
    "facility_id",
    "carrier_id",
    "country",
    "region",
    "entity_type",
    "entity_id",
    "correction_of_event_id",
)
ARROW_TYPES = {
    "string": pa.string(),
    "int64": pa.int64(),
    "float64": pa.float64(),
    "boolean": pa.bool_(),
    "timestamp": pa.timestamp("us", tz="UTC"),
    "date": pa.date32(),
}


def _time(value: Any) -> datetime:
    result = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if result.tzinfo is None:
        raise ValueError("Event times must include a timezone")
    return result.astimezone(UTC)


def _arrow_events(events: list[dict[str, Any]]) -> pa.Table:
    schema = pa.schema(
        [(name, pa.string()) for name in EVENT_FIELDS]
        + [("effective_version", pa.int64()), ("payload_json", pa.string())]
    )
    rows = []
    for event in events:
        row = {name: event.get(name) for name in EVENT_FIELDS}
        row["effective_version"] = event.get(
            "effective_version", event.get("logical_slot_version", event["entity_version"])
        )
        row["payload_json"] = json.dumps(event["payload"], sort_keys=True, allow_nan=False)
        rows.append(row)
    return pa.Table.from_pylist(rows, schema=schema)


def _contract_table(table: pa.Table, name: str) -> pa.Table:
    contract = json.loads((ROOT / "contracts" / "products" / f"{name}.json").read_text())
    columns = contract["columns"]
    expected = [column["name"] for column in columns]
    if table.column_names != expected:
        raise ValueError(f"{name}: SQL columns differ from contract: {table.column_names}")
    schema = pa.schema(
        [
            pa.field(column["name"], ARROW_TYPES[column["type"]], nullable=column["nullable"])
            for column in columns
        ]
    )
    for column in columns:
        if not column["nullable"] and table[column["name"]].null_count:
            raise ValueError(f"{name}.{column['name']}: null violates product contract")
    return table.cast(schema, safe=True)


def build_products(
    events: list[dict[str, Any]],
    output: Path,
    as_of: datetime,
) -> dict[str, Any]:
    """Write six contracted products plus dimensional/feature Parquet artifacts.

    Caller owns canonical identity, cross-record quality and atomic publication.
    Artifact paths are absolute. Empty histories produce typed empty products.
    Feature source_snapshot_id hashes effective input rows excluding trace diagnostics, independent of
    physical Parquet filenames, write attempts and wall-clock publication times.
    """
    from commerce_platform.references import CARRIERS, FACILITIES, SKUS

    if as_of.tzinfo is None:
        raise ValueError("as_of must be timezone-aware")
    as_of = as_of.astimezone(UTC)
    eligible = [
        event
        for event in events
        if _time(event["event_timestamp"]) <= as_of and _time(event["ingestion_timestamp"]) <= as_of
    ]
    if len({event["event_id"] for event in eligible}) != len(eligible):
        raise ValueError("Product builder requires unique canonical event IDs")
    eligible.sort(key=lambda event: event["event_id"])
    source_snapshot = hashlib.sha256(
        json.dumps(
            [
                {key: value for key, value in event.items() if key != "trace_id"}
                for event in eligible
            ],
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode()
    ).hexdigest()
    output = output.resolve()
    output.mkdir(parents=True, exist_ok=True)
    result: dict[str, Any] = {}
    with duckdb.connect(":memory:") as connection:
        connection.execute("SET TimeZone = 'UTC'")
        connection.execute("SET threads = 2")
        connection.register("input_events", _arrow_events(eligible))
        connection.register("reference_facilities", pa.Table.from_pylist(FACILITIES))
        # Keep serving dimensions scalar; carrier coverage stays in source reference data.
        connection.register(
            "reference_carriers",
            pa.Table.from_pylist(
                [
                    {"carrier_id": carrier["carrier_id"], "carrier_name": carrier["carrier_name"]}
                    for carrier in CARRIERS
                ]
            ),
        )
        connection.register("reference_skus", pa.Table.from_pylist(SKUS))
        connection.execute(
            "CREATE TABLE config AS SELECT ?::TIMESTAMPTZ AS as_of, ? AS source_snapshot_id",
            [as_of, source_snapshot],
        )
        connection.execute((ROOT / "sql" / "local" / "products.sql").read_text())
        for name in (*PUBLIC_PRODUCTS, *EXTRA_TABLES):
            table = connection.execute(f'SELECT * FROM "{name}" ORDER BY ALL').to_arrow_table()
            if name in PUBLIC_PRODUCTS:
                table = _contract_table(table, name)
            path = output / f"{name}.parquet"
            pq.write_table(table, path, compression="zstd")
            result[name] = {
                "path": str(path),
                "row_count": table.num_rows,
                "columns": table.column_names,
                "layer": "silver" if name == "inventory_movements" else "gold",
            }
    return result
