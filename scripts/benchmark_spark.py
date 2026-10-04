"""Measure a real local Spark run separately from the Python baseline."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import platform
import subprocess
import sys
import time
from pathlib import Path


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True, help="New Spark output directory")
    parser.add_argument("--result", type=Path, required=True)
    parser.add_argument("--master", default="local[2]")
    args = parser.parse_args()
    if args.output.exists():
        parser.error("Output must be new; Spark writes never overwrite an experiment")
    manifest = json.loads(args.manifest.read_text())
    artifact = manifest["artifacts"]["silver_events"]
    source = Path(artifact["path"])
    checksum = hashlib.sha256(source.read_bytes()).hexdigest()
    if checksum != artifact["sha256"]:
        raise ValueError("Input artifact does not match committed manifest")
    job = Path(__file__).resolve().parents[1] / "spark/curate.py"
    args.result.parent.mkdir(parents=True, exist_ok=True)
    command = [
        sys.executable,
        str(job),
        "--input",
        str(source),
        "--output",
        str(args.output.resolve()),
        "--metrics",
        str(args.result.resolve()),
        "--master",
        args.master,
    ]
    env = {**os.environ, "SPARK_LOCAL_IP": os.environ.get("SPARK_LOCAL_IP", "127.0.0.1")}
    began = time.perf_counter()
    execution = subprocess.run(command, env=env, capture_output=True, text=True, timeout=600)
    elapsed = time.perf_counter() - began
    args.result.with_suffix(".log").write_text(execution.stdout + execution.stderr)
    if execution.returncode:
        raise RuntimeError(f"Spark failed; inspect {args.result.with_suffix('.log')}")
    result = json.loads(args.result.read_text())
    assert result["input_records"] == artifact["row_count"]
    result.update(
        {
            "experiment": "one real local Spark run; no managed EMR measurement",
            "client_wall_seconds_including_jvm_startup": elapsed,
            "master": args.master,
            "input_sha256": checksum,
            "input_bytes": source.stat().st_size,
            "spark_job_sha256": hashlib.sha256(job.read_bytes()).hexdigest(),
            "source_implementation_hash": manifest["implementation_hash"],
            "environment": {
                "python": platform.python_version(),
                "os": platform.platform(),
                "machine": platform.machine(),
            },
            "command": "python scripts/benchmark_spark.py --manifest <committed-head.json> --output <new-directory> --result <result.json> --master local[2]",
            "measurement_boundary": "runtime_seconds starts after SparkSession creation and includes reads, assertions, shuffles and writes",
        }
    )
    args.result.write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
