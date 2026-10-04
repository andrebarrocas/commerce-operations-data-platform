"""Local CLI. Every command is offline unless an explicit AWS adapter is called."""

from __future__ import annotations

import argparse
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from commerce_platform.generator import generate_events
from commerce_platform.lake import run
from commerce_platform.quality import utc


def write_workload(
    output: Path, orders: int, seed: int, start: datetime | None = None, faults: bool = False
) -> tuple[list[dict[str, Any]], Path]:
    events = generate_events(orders, seed, start)
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("w") as stream:
        for index, event in enumerate(events):
            line = json.dumps(event, sort_keys=True) + "\n"
            stream.write(line)
            if faults and index % 101 == 0:
                stream.write(line)  # exact at-least-once retry
            if faults and index % 251 == 0:
                stream.write('{"malformed":\n')
    control = output.with_suffix(".control.json")
    control.write_text(
        json.dumps(
            {
                "generator": "synthetic-python-v1",
                "seed": seed,
                "orders": orders,
                "expected_event_ids": sorted({e["event_id"] for e in events}),
                "description": "Independent producer control total of valid generated business events; injected malformed fixtures counted separately",
            },
            indent=2,
        )
        + "\n"
    )
    return events, control


def main() -> None:
    parser = argparse.ArgumentParser(description="Synthetic commerce operations data platform")
    sub = parser.add_subparsers(dest="command", required=True)
    generate = sub.add_parser(
        "generate", help="Create deterministic JSONL and source control manifest"
    )
    generate.add_argument("--output", type=Path, required=True)
    generate.add_argument("--orders", type=int, default=1000)
    generate.add_argument("--seed", type=int, default=42)
    generate.add_argument("--start", type=utc)
    generate.add_argument("--faults", action="store_true")
    for command in ("ingest", "backfill"):
        ingest = sub.add_parser(
            command, help="Rebuild from retained local history and publish a snapshot"
        )
        ingest.add_argument("--input", type=Path, required=True)
        ingest.add_argument("--lake", type=Path, required=True)
        ingest.add_argument("--as-of", type=utc, required=True)
        ingest.add_argument("--control", type=Path)
        if command == "backfill":
            ingest.add_argument("--start", required=True)
            ingest.add_argument("--end", required=True)
    demo = sub.add_parser(
        "demo",
        help="Generate and process an offline fixture including duplicates and malformed records",
    )
    demo.add_argument("--orders", type=int, default=1000)
    demo.add_argument("--directory", type=Path, default=Path("var/demo"))
    args = parser.parse_args()
    if args.command == "generate":
        events, control = write_workload(
            args.output, args.orders, args.seed, args.start, args.faults
        )
        print(
            json.dumps({"events": len(events), "input": str(args.output), "control": str(control)})
        )
        return
    if args.command == "demo":
        events, control = write_workload(
            args.directory / "events.jsonl", args.orders, 42, faults=True
        )
        cutoff = max(
            (utc(e["ingestion_timestamp"]) for e in events),
            default=datetime(2026, 1, 1, tzinfo=UTC),
        ) + timedelta(seconds=1)
        result = run(
            args.directory / "events.jsonl",
            args.directory / "lake",
            as_of=cutoff,
            expected_ids=set(json.loads(control.read_text())["expected_event_ids"]),
        )
    else:
        expected = (
            set(json.loads(args.control.read_text())["expected_event_ids"])
            if args.control
            else None
        )
        result = run(
            args.input,
            args.lake,
            as_of=args.as_of,
            expected_ids=expected,
            start=getattr(args, "start", None),
            end=getattr(args, "end", None),
        )
    print(
        json.dumps(
            {
                "run_key": result["run_key"],
                "reused": result["reused"],
                "logical_hash": result["logical_hash"],
                "artifacts": len(result["artifacts"]),
                "metrics": result["metrics"],
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
