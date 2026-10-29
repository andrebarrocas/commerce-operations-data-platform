"""Validate the reviewer command through the actual installed CLI boundary."""

import json
import subprocess
import sys
from pathlib import Path


def command(*arguments: str) -> subprocess.CompletedProcess[str]:
    script = Path(__file__).resolve().parents[1] / "scripts/verify_demo.py"
    return subprocess.run(
        [sys.executable, str(script), *arguments], capture_output=True, text=True, timeout=180
    )


def test_reviewer_command_publishes_evidence_and_refuses_to_replace_it(tmp_path):
    work = tmp_path / "reviewer"
    report = tmp_path / "report.json"
    args = ("--work", str(work), "--report", str(report), "--orders", "40")
    first = command(*args)
    assert first.returncode == 0, first.stderr
    evidence = report.read_bytes()
    result = json.loads(evidence)
    assert result["status"] == "passed"
    assert result["artifact_rows"]["order_fulfillment"] == 40
    assert all(state == "passed" for state in result["checks"].values())
    assert result["aws_deployed"] is False
    again = command(*args)
    assert again.returncode != 0
    assert "existing evidence is preserved" in again.stderr
    assert report.read_bytes() == evidence


def test_reviewer_command_rejects_a_fixture_too_small_to_exercise_branches(tmp_path):
    result = command(
        "--work",
        str(tmp_path / "unused"),
        "--report",
        str(tmp_path / "report.json"),
        "--orders",
        "1",
    )
    assert result.returncode != 0
    assert not (tmp_path / "unused").exists()
    assert not (tmp_path / "report.json").exists()
