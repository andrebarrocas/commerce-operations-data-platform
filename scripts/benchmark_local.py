"""Actual local measurements. No AWS extrapolation or invented results."""

from __future__ import annotations

import argparse
import hashlib
import importlib.metadata
import json
import os
import platform
import resource
import statistics
import subprocess
import time
from datetime import timedelta
from pathlib import Path

from commerce_platform.cli import write_workload
from commerce_platform.lake import PublicationError, run
from commerce_platform.quality import utc


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--orders", type=int, default=5000)
    parser.add_argument("--trials", type=int, default=3)
    parser.add_argument("--work", type=Path, required=True, help="New, empty scratch directory")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.orders < 1 or args.trials < 1 or args.work.exists():
        parser.error("Positive counts and a new work directory are required")
    args.work.mkdir(parents=True)
    cpu_start, generate_start = time.process_time(), time.perf_counter()
    source = args.work / "events.jsonl"
    events, control = write_workload(source, args.orders, 42, faults=True)
    generation_seconds = time.perf_counter() - generate_start
    cutoff = max(utc(e["ingestion_timestamp"]) for e in events) + timedelta(seconds=1)
    expected = set(json.loads(control.read_text())["expected_event_ids"])
    # Warmup is explicitly smaller and excluded from throughput trials.
    warm_source = args.work / "warmup.jsonl"
    warm, _ = write_workload(warm_source, min(40, args.orders), 42)
    run(warm_source, args.work / "warmup", as_of=cutoff)
    trials = []
    logical_hashes = set()
    for trial in range(args.trials):
        observed = time.perf_counter()
        result = run(source, args.work / f"trial-{trial}", as_of=cutoff, expected_ids=expected)
        wall = time.perf_counter() - observed
        logical_hashes.add(result["logical_hash"])
        trials.append({"trial": trial + 1, "client_wall_seconds": wall, **result["metrics"]})
    assert len(logical_hashes) == 1, "Reruns changed logical results"
    replay_start = time.perf_counter()
    replay = run(source, args.work / "trial-0", as_of=cutoff, expected_ids=expected)
    replay_seconds = time.perf_counter() - replay_start
    assert replay["reused"]
    failure_start = time.perf_counter()
    try:
        run(
            source,
            args.work / "recovery",
            as_of=cutoff,
            expected_ids=expected,
            failpoint="before_commit",
        )
    except PublicationError:
        pass
    else:
        raise AssertionError("Fault injection did not fire")
    failure_seconds = time.perf_counter() - failure_start
    assert not (args.work / "recovery/head.json").exists()
    recovery_start = time.perf_counter()
    recovered = run(source, args.work / "recovery", as_of=cutoff, expected_ids=expected)
    recovery_seconds = time.perf_counter() - recovery_start
    assert recovered["logical_hash"] in logical_hashes
    peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    cpu_model = platform.processor() or platform.machine()
    physical_memory_bytes = None
    if platform.system() == "Darwin":
        cpu_model = subprocess.check_output(
            ["sysctl", "-n", "machdep.cpu.brand_string"], text=True
        ).strip()
        physical_memory_bytes = int(
            subprocess.check_output(["sysctl", "-n", "hw.memsize"], text=True)
        )
    report = {
        "scope": "single-host local synthetic experiment; not production or AWS performance",
        "load_control": "normal desktop environment; no CPU pinning or isolated host guarantee",
        "implementation_hash": recovered["implementation_hash"],
        "command": f"python scripts/benchmark_local.py --orders {args.orders} --trials {args.trials} --work <new-directory> --output <result.json>",
        "environment": {
            "os": platform.platform(),
            "machine": platform.machine(),
            "logical_cpus": os.cpu_count(),
            "cpu_model": cpu_model,
            "physical_memory_bytes": physical_memory_bytes,
            "python": platform.python_version(),
            "packages": {
                p: importlib.metadata.version(p) for p in ["duckdb", "pyarrow", "jsonschema"]
            },
        },
        "seed": 42,
        "orders": args.orders,
        "unique_events": len(events),
        "input_bytes": source.stat().st_size,
        "input_sha256": hashlib.sha256(source.read_bytes()).hexdigest(),
        "logical_hash": next(iter(logical_hashes)),
        "generation_seconds": generation_seconds,
        "generation_events_per_second": len(events) / generation_seconds,
        "trials": trials,
        "median_processing_seconds": statistics.median(t["processing_seconds"] for t in trials),
        "median_records_per_second": statistics.median(t["records_per_second"] for t in trials),
        "replay_seconds": replay_seconds,
        "injected_failure_seconds": failure_seconds,
        "failure_recovery_seconds": recovery_seconds,
        "recovery_boundary": "rerun after pre-commit exception to identical committed logical output",
        "process_cpu_seconds_total": time.process_time() - cpu_start,
        "process_peak_rss_bytes": peak if platform.system() == "Darwin" else peak * 1024,
        "memory_scope": "maximum process RSS across warmup, all trials, replay and recovery; not per-stage or per-trial",
        "spark_runtime_seconds": None,
        "redshift_query_latency_seconds": None,
        "cloud_measurement_status": "not_deployed_not_measured",
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2) + "\n")
    print(
        json.dumps(
            {
                k: report[k]
                for k in [
                    "unique_events",
                    "median_processing_seconds",
                    "median_records_per_second",
                    "failure_recovery_seconds",
                ]
            }
        )
    )


if __name__ == "__main__":
    main()
