"""Offline acceptance safety guards and failure-policy restoration."""

import importlib.util
import json
from pathlib import Path

import pytest

SCRIPTS = Path(__file__).resolve().parents[1] / "scripts"


def load(name):
    spec = importlib.util.spec_from_file_location(name, SCRIPTS / f"{name}.py")
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class Client:
    def __init__(self, session):
        self.session = session

    def get_caller_identity(self):
        return {"Account": "123456789012"}

    def get_paginator(self, operation):
        assert operation == "list_objects_v2"
        return self

    def paginate(self, **kwargs):
        return [{"Contents": [{"Key": "raw/existing.gz"}]}]

    def get_function_configuration(self, **kwargs):
        return {"Role": "arn:aws:iam::123456789012:role/test-validator"}

    def get_bucket_policy(self, **kwargs):
        return {"Policy": self.session.policy}

    def put_bucket_policy(self, **kwargs):
        self.session.policies.append(kwargs["Policy"])
        self.session.policy = kwargs["Policy"]

    def put_record(self, **kwargs):
        raise ConnectionError("Injected ambiguous producer failure")


class Session:
    region_name = "eu-west-1"

    def __init__(self):
        self.policy = json.dumps({"Version": "2012-10-17", "Statement": []})
        self.policies = []

    def client(self, service):
        return Client(self)


OUTPUTS = {
    "stream_name": "commerce-test-events",
    "lake_bucket": "commerce-test-123456789012-eu-west-1",
}


def test_stream_rejects_wrong_account_and_existing_source_before_writes(tmp_path):
    module = load("verify_stream_aws")
    with pytest.raises(ValueError, match="account"):
        module.verify(Session(), "000000000000", OUTPUTS, tmp_path / "wrong")
    with pytest.raises(ValueError, match="fresh lake"):
        module.verify(Session(), "123456789012", OUTPUTS, tmp_path / "existing")
    assert not list(tmp_path.iterdir())


@pytest.mark.parametrize(
    "changes", [{"stream_name": "commerce-prod-events"}, {"lake_bucket": "someone-elses-bucket"}]
)
def test_stream_rejects_non_test_or_mismatched_resources(tmp_path, changes):
    with pytest.raises(ValueError, match="isolated test"):
        load("verify_stream_aws").verify(
            Session(), "123456789012", OUTPUTS | changes, tmp_path / "run"
        )


def test_failure_injection_always_restores_original_policy(tmp_path, monkeypatch):
    monkeypatch.syspath_prepend(str(SCRIPTS))
    module = load("verify_stream_recovery_aws")
    session = Session()
    original = session.policy
    with pytest.raises(ConnectionError):
        module.verify(session, "123456789012", OUTPUTS, tmp_path / "run")
    assert session.policy == original
    assert len(session.policies) == 2
    assert json.loads(session.policies[0])["Statement"][0]["Effect"] == "Deny"
    report = json.loads((tmp_path / "run/report.json").read_text())
    assert report["state"] == "fail"
    assert report["checks"]["original_policy_restored"] == "pass"
