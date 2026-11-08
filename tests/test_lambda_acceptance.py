"""Offline tests of acceptance-run failure paths, not managed-service evidence."""

import importlib.util
import io
import json
from pathlib import Path

import pytest
from botocore.exceptions import ClientError

SCRIPT = Path(__file__).resolve().parents[1] / "scripts/verify_lambda_aws.py"
spec = importlib.util.spec_from_file_location("verify_lambda_aws", SCRIPT)
assert spec and spec.loader
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)


class Client:
    def __init__(self, service, session):
        self.service, self.session = service, session

    def __getattr__(self, operation):
        def call(**kwargs):
            self.session.calls.append((self.service, operation, kwargs))
            if operation == "get_caller_identity":
                return {"Account": "123456789012"}
            if operation == "create_role":
                return {"Role": {"Arn": "arn:aws:iam::123456789012:role/test"}}
            if operation == "create_function":
                raise ClientError({"Error": {"Code": "ServiceException"}}, operation)
            if operation == "get_paginator":
                return self
            if operation == "delete_role" and self.session.cleanup_failure:
                raise ClientError({"Error": {"Code": "DeleteConflict"}}, operation)
            return {}

        return call

    def get_paginator(self, name):
        assert name == "list_object_versions"
        return self

    def paginate(self, **kwargs):
        return [
            {
                "Versions": [{"Key": "evidence", "VersionId": "one"}],
                "DeleteMarkers": [{"Key": "evidence", "VersionId": "two"}],
            }
        ]


class Session:
    region_name = "eu-west-1"

    def __init__(self, cleanup_failure=False):
        self.calls = []
        self.cleanup_failure = cleanup_failure

    def client(self, service):
        return Client(service, self)


@pytest.mark.parametrize("cleanup_failure", [False, True])
def test_partial_deployment_failure_cleans_all_completed_resources(tmp_path, cleanup_failure):
    package = tmp_path / "lambda.zip"
    package.write_bytes(b"PKfixture")
    directory = tmp_path / "run"
    session = Session(cleanup_failure)
    with pytest.raises(ClientError):
        module.verify(session, "123456789012", package, directory)
    report = json.loads((directory / "report.json").read_text())
    assert report["state"] == ("cleanup_failed" if cleanup_failure else "fail")
    assert report["cleanup"]["logs"] == "deleted"
    assert report["cleanup"]["lake_bucket"] == "deleted"
    assert report["cleanup"]["denied_bucket"] == "deleted"
    operations = [operation for _, operation, _ in session.calls]
    assert operations.count("delete_bucket") == 2
    assert "delete_role_policy" in operations
    assert "delete_log_group" in operations
    deletions = [
        kwargs["Delete"]["Objects"] for _, op, kwargs in session.calls if op == "delete_objects"
    ]
    assert all({row["VersionId"] for row in batch} == {"one", "two"} for batch in deletions)
    assert "123456789012" not in json.dumps(report)
    assert (directory / "resources.json").stat().st_mode & 0o777 == 0o600


def test_account_guard_prevents_cloud_mutation(tmp_path):
    session = Session()
    with pytest.raises(ValueError, match="account"):
        module.verify(session, "000000000000", tmp_path / "missing.zip", tmp_path / "run")
    assert [op for _, op, _ in session.calls] == ["get_caller_identity"]
    assert not (tmp_path / "run").exists()


def test_function_error_cannot_be_mistaken_for_success_and_body_closes():
    stream = io.BytesIO(b'{"errorMessage":"failure"}')

    class Lambda:
        def invoke(self, **kwargs):
            return {"StatusCode": 200, "FunctionError": "Unhandled", "Payload": stream}

    with pytest.raises(AssertionError, match="invocation failed"):
        module.invoke(Lambda(), "synthetic", {})
    assert stream.closed
