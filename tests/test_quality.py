from datetime import UTC, datetime

import pytest

from commerce_platform.quality import evaluate, source_completeness

NOW = datetime(2026, 1, 1, tzinfo=UTC)


def check(kind, rows, parameters=None, **context):
    return evaluate(
        rows,
        [{"id": "r", "type": kind, "columns": ["v"], "parameters": parameters or {}}],
        now=NOW,
        **context,
    )[0]


@pytest.mark.parametrize(
    ("kind", "rows", "parameters"),
    [
        ("not_null", [{"v": None}], {}),
        ("unique", [{"v": "a"}, {"v": "a"}], {}),
        ("duplicate_event_detection", [{"v": "a"}, {"v": "a"}], {}),
        ("accepted_values", [{"v": "bad"}], {"values": ["ok"]}),
        ("numeric_range", [{"v": -1}], {"minimum": 0, "maximum": 1}),
        ("timestamp_sanity", [{"v": "2026-01-02T00:00:00Z"}], {}),
        ("freshness", [{"v": "2025-01-01T00:00:00Z"}], {"max_lag_seconds": 900}),
    ],
)
def test_failures(kind, rows, parameters):
    assert check(kind, rows, parameters).state == "fail"


def test_reference_and_missing_reference():
    params = {"reference_product": "parents", "reference_columns": ["id"]}
    assert check("referential_integrity", [{"v": 1}], params).state == "unknown"
    assert (
        check(
            "referential_integrity", [{"v": 1}], params, references={"parents": [{"id": 2}]}
        ).state
        == "fail"
    )


def test_control_totals_are_not_inferred():
    assert check("completeness", []).state == "unknown"
    assert check("completeness", [], expected_keys=set()).state == "not_applicable"
    assert check("completeness", [{"v": 1}], expected_keys={(1,), (2,)}).state == "fail"
    assert source_completeness(set(), None)["state"] == "unknown"
    assert source_completeness(set(), set())["state"] == "not_applicable"
    assert source_completeness({"a"}, {"a", "b"})["ratio"] == 0.5


def test_lifecycle_and_unknown_rules_fail_closed():
    result = check(
        "lifecycle",
        [{"id": 1, "seq": 1, "v": "end"}],
        {"group_columns": ["id"], "order_column": "seq", "transitions": {"__start__": ["start"]}},
    )
    assert result.state == "fail"
    with pytest.raises(ValueError):
        check("invented_rule", [])


def test_machine_contract_rule_names_and_parameters():
    result = check(
        "duplicate_event",
        [{"v": "a", "hash": "x"}, {"v": "a", "hash": "y"}],
        {"identity_columns": ["v"], "semantic_digest_column": "hash"},
    )
    assert result.state == "fail" and "conflicting_identities=1" in result.detail
    result = check(
        "lifecycle_transition",
        [{"id": 1, "seq": 1, "v": "end"}],
        {
            "entity_columns": ["id"],
            "sequence_column": "seq",
            "allowed_transitions": [["__start__", "start"]],
        },
    )
    assert result.state == "fail"
    assert (
        check(
            "freshness",
            [{"v": "2025-01-01T00:00:00Z"}],
            {"max_age_seconds": 60, "clock": "evaluation_time_utc"},
        ).state
        == "fail"
    )
