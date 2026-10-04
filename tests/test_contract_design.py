"""Executable Phase 0 design gate; runtime contract tests extend this later."""

import importlib.util
from pathlib import Path


def test_all_contracts_and_examples() -> None:
    script = Path(__file__).resolve().parents[1] / "scripts/check_contracts.py"
    spec = importlib.util.spec_from_file_location("check_contracts", script)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    module.main()
