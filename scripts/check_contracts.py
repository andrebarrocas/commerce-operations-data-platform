"""Validate the design artifacts without importing the pipeline runtime."""

import json
from pathlib import Path

from jsonschema import Draft202012Validator, FormatChecker

ROOT = Path(__file__).resolve().parents[1]


def main() -> None:
    schemas = list((ROOT / "contracts").glob("*.schema.json"))
    if len(schemas) < 2:
        raise SystemExit("Event and product schemas are required")
    for path in schemas:
        Draft202012Validator.check_schema(json.loads(path.read_text()))
    product_schema = json.loads((ROOT / "contracts/data-product.schema.json").read_text())
    validator = Draft202012Validator(product_schema, format_checker=FormatChecker())
    products = sorted((ROOT / "contracts/products").glob("*.json"))
    assert len(products) == 6, "Six data products are required"
    for path in products:
        contract = json.loads(path.read_text())
        validator.validate(contract)
        names = [column["name"] for column in contract["columns"]]
        assert len(names) == len(set(names)), f"{path}: duplicate columns"
        assert set(contract["primary_key"]) <= set(names), f"{path}: unknown primary key"
        for rule in contract["quality_rules"]:
            assert set(rule["columns"]) <= set(names), f"{path}: unknown rule column"
            if rule["type"] == "referential_integrity":
                assert len(rule["columns"]) == len(rule["parameters"]["reference_columns"])
    examples = ROOT / "contracts/examples/events.jsonl"
    if examples.exists():
        event_validator = Draft202012Validator(
            json.loads((ROOT / "contracts/event.schema.json").read_text()),
            format_checker=FormatChecker(),
        )
        events = [json.loads(line) for line in examples.read_text().splitlines() if line]
        for event in events:
            event_validator.validate(event)
        expected = event_validator.schema["$defs"]["wire_event"]["properties"]["event_type"]["enum"]
        assert {event["event_type"] for event in events} == set(expected)
        assert {event["schema_version"] for event in events} == {"1.0.0", "1.1.0"}
        for event in events:
            invalid = dict(event, payload={})
            assert not event_validator.is_valid(invalid), (
                f"Empty payload accepted: {event['event_type']}"
            )
        print(f"Validated {len(events)} event examples and negative payload variants")
    else:
        raise SystemExit("Event examples are required for the Phase 0 gate")
    print(f"Validated {len(schemas)} schemas and {len(products)} product contracts")


if __name__ == "__main__":
    main()
