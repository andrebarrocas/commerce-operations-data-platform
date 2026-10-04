"""Low-cardinality CloudWatch Embedded Metric Format and explicit cost units."""

from __future__ import annotations

import time
from typing import Any

UNITS = {
    "records_processed": "Count",
    "records_rejected": "Count",
    "duplicate_records": "Count",
    "late_records": "Count",
    "retries": "Count",
    "failures": "Count",
    "quality_failures": "Count",
    "processing_seconds": "Seconds",
    "quality_seconds": "Seconds",
    "freshness_seconds": "Seconds",
    "records_per_second": "Count/Second",
    "bytes_read": "Bytes",
    "bytes_written": "Bytes",
}


def emf(
    metrics: dict[str, int | float], environment: str = "local", stage: str = "batch"
) -> dict[str, Any]:
    unknown = set(metrics) - UNITS.keys()
    if unknown:
        raise ValueError(f"Undeclared metrics: {sorted(unknown)}")
    return {
        "_aws": {
            "Timestamp": int(time.time() * 1000),
            "CloudWatchMetrics": [
                {
                    "Namespace": "CommercePlatform",
                    "Dimensions": [["Environment", "Pipeline", "Stage"]],
                    "Metrics": [{"Name": name, "Unit": UNITS[name]} for name in metrics],
                }
            ],
        },
        "Environment": environment,
        "Pipeline": "commerce",
        "Stage": stage,
        **metrics,
    }


def estimate_cost(
    units: float, unit: str, rate: float | None, *, region: str, price_date: str | None
) -> dict[str, Any]:
    if units < 0 or (rate is not None and rate < 0):
        raise ValueError("Units and rates must be nonnegative")
    if rate is not None and not price_date:
        raise ValueError("Measured rates require a price retrieval date")
    return {
        "status": "unmeasured" if rate is None else "estimate",
        "units": units,
        "unit": unit,
        "unit_price_usd": rate,
        "estimated_usd": None if rate is None else units * rate,
        "region": region,
        "price_date": price_date,
        "exclusions": "Taxes, minimum billing units, transfer, and shared overhead unless included in units",
    }
