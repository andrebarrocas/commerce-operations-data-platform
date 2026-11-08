"""Validate the source comparison used against actual Redshift COPY results."""

import importlib.util
from datetime import UTC, datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest


def test_copy_comparison_normalizes_timestamp_and_rejects_changed_values(tmp_path, monkeypatch):
    scripts = Path(__file__).resolve().parents[1] / "scripts"
    monkeypatch.syspath_prepend(str(scripts))
    spec = importlib.util.spec_from_file_location(
        "verify_redshift_aws", scripts / "verify_redshift_aws.py"
    )
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    source = tmp_path / "source.parquet"
    table = pa.Table.from_pylist(
        [
            {
                "shipment_id": "s-1",
                "delivered_at": datetime(2026, 1, 1, 0, 0, 0, 123456, tzinfo=UTC),
                "duration": 0.5,
            },
            {"shipment_id": "s-2", "delivered_at": None, "duration": None},
        ]
    )
    pq.write_table(table, source)
    rows = [
        [
            {"stringValue": "s-1"},
            {"stringValue": "2026-01-01 00:00:00.123456+00"},
            {"doubleValue": 0.5},
        ],
        [{"stringValue": "s-2"}, {"isNull": True}, {"isNull": True}],
    ]
    module.assert_copy_matches(source, rows)
    rows[0][2] = {"doubleValue": 5.0}
    with pytest.raises(AssertionError, match="COPY changed source values"):
        module.assert_copy_matches(source, rows)
