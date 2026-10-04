"""Distributed serving projections over a committed, contract-validated Silver snapshot.

Input is the specific Silver artifact from a manifest, never a glob over all runs.
Output is a new run-scoped directory. This job does not publish a cloud catalog head.
Supports local paths and s3:// paths on EMR; AWS execution remains an acceptance gate.
"""

from __future__ import annotations

import argparse
import json
import time
from pathlib import Path
from typing import Any

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, LongType, StringType, StructField, StructType


def curate(spark: SparkSession, source: str, output: str) -> dict[str, Any]:
    began = time.perf_counter()
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    events = spark.read.parquet(source).withColumn("event_time", F.to_timestamp("event_timestamp"))
    events = events.withColumn("observed_at", F.to_timestamp("ingestion_timestamp")).cache()
    records = events.count()
    # Defensive uniqueness assertion: the upstream manifest promises canonical IDs.
    if events.groupBy("event_id").count().where("count > 1").limit(1).count():
        raise ValueError("Input violates canonical Silver event identity")
    order_schema = StructType(
        [
            StructField("line_item_id", StringType()),
            StructField("sku_id", StringType()),
            StructField("quantity", LongType()),
            StructField("unit_price_minor", LongType()),
        ]
    )
    lines = (
        events.where("event_type = 'ORDER_CREATED'")
        .withColumn(
            "line",
            F.explode(
                F.from_json(
                    F.get_json_object("payload_json", "$.line_items"), ArrayType(order_schema)
                )
            ),
        )
        .select(
            "order_id",
            "country",
            F.col("event_time").alias("ordered_at"),
            F.get_json_object("payload_json", "$.currency").alias("currency"),
            "line.*",
        )
    )
    lines = lines.withColumn("line_total_minor", F.col("quantity") * F.col("unit_price_minor"))
    shipments = events.where("entity_type = 'shipment'")
    ordering = Window.partitionBy("shipment_id").orderBy(F.col("effective_version").desc())
    last_state = (
        shipments.withColumn("rn", F.row_number().over(ordering))
        .where("rn = 1")
        .select("shipment_id", F.col("event_type").alias("last_event_type"))
    )
    durations = (
        shipments.groupBy("shipment_id", "order_id", "facility_id", "carrier_id", "country")
        .agg(
            F.min(F.when(F.col("event_type") == "SHIPMENT_DISPATCHED", F.col("event_time"))).alias(
                "dispatched_at"
            ),
            F.max(F.when(F.col("event_type") == "DELIVERED", F.col("event_time"))).alias(
                "delivered_at"
            ),
            F.sum(F.when(F.col("event_type") == "DELIVERY_FAILED", 1).otherwise(0)).alias(
                "failed_attempts"
            ),
            F.max("observed_at").alias("last_observed_at"),
        )
        .where("dispatched_at IS NOT NULL")
        .join(last_state, "shipment_id", "inner")
    )
    durations = durations.withColumn(
        "dispatch_to_delivery_seconds",
        F.col("delivered_at").cast("double") - F.col("dispatched_at").cast("double"),
    )
    hours = (
        events.where(
            "event_type IN ('INVENTORY_RESERVED','INVENTORY_RELEASED','PACKAGE_CREATED','SHIPMENT_DISPATCHED')"
        )
        .withColumn("window_start", F.date_trunc("hour", "event_time"))
        .groupBy("facility_id", "window_start")
        .agg(
            F.count("*").alias("event_count"),
            F.sum(F.when(F.col("event_type") == "SHIPMENT_DISPATCHED", 1).otherwise(0)).alias(
                "shipments_dispatched"
            ),
        )
        .withColumn("event_date", F.to_date("window_start"))
    )
    preceding = (
        Window.partitionBy("facility_id")
        .orderBy(F.col("window_start").cast("long"))
        .rangeBetween(-86400, -1)
    )
    hours = hours.withColumn(
        "preceding_24h_dispatches",
        F.coalesce(F.sum("shipments_dispatched").over(preceding), F.lit(0)),
    )
    counts = {}
    for name, frame, partition in (
        ("fact_order_line", lines, None),
        ("shipment_summary", durations, None),
        ("facility_hour", hours, "event_date"),
    ):
        frame = frame.cache()
        counts[name] = frame.count()
        writer = frame.write.mode("errorifexists").option("compression", "zstd")
        if partition:
            writer = writer.partitionBy(partition)
        writer.parquet(f"{output.rstrip('/')}/{name}")
        frame.unpersist()
    events.unpersist()
    return {
        "engine": "Apache Spark",
        "spark_version": spark.version,
        "input_records": records,
        "output_rows": counts,
        "runtime_seconds": time.perf_counter() - began,
        "source": source,
        "output": output,
        "scope": "distributed projections of canonical Silver; excludes source validation and cloud publication",
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--metrics", required=True, help="Driver-local JSON result path")
    parser.add_argument("--master", default=None, help="Use local[2] locally; omit on EMR")
    args = parser.parse_args()
    builder = SparkSession.builder.appName("commerce-curated-projections")
    if args.master:
        builder = builder.master(args.master)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    try:
        result = curate(spark, args.input, args.output)
        Path(args.metrics).write_text(json.dumps(result, indent=2) + "\n")
        print(json.dumps(result))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
