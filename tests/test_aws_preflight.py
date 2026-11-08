"""Offline safety checks; these fixtures are not AWS execution evidence."""

import importlib.util
from pathlib import Path

import pytest

exceptions = pytest.importorskip(
    "botocore.exceptions", reason="Install the aws extra for SDK acceptance tests"
)
ClientError = exceptions.ClientError
EndpointConnectionError = exceptions.EndpointConnectionError

SCRIPT = Path(__file__).resolve().parents[1] / "scripts/aws_preflight.py"
spec = importlib.util.spec_from_file_location("aws_preflight", SCRIPT)
assert spec and spec.loader
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)


class Session:
    region_name = "eu-west-1"

    def __init__(self, failure=None):
        self.calls = []
        self.failure = failure

    def client(self, service):
        self.calls.append(service)
        session = self

        class Client:
            def get_caller_identity(self):
                return {"Account": "123456789012"}

            def __getattr__(self, operation):
                def probe(**kwargs):
                    assert operation.startswith(("list_", "get_"))
                    if service == "kinesis" and session.failure:
                        raise session.failure
                    return {"sensitive_inventory": "must not appear in the public report"}

                return probe

        return Client()


@pytest.mark.parametrize("account", ["another-account", "000000000000", "1234-5678-9012"])
def test_wrong_or_malformed_account_stops_before_service_probes(account):
    session = Session()
    with pytest.raises(ValueError):
        module.preflight(session, account)
    assert session.calls in [[], ["sts"]]


def test_subscription_failure_is_recorded_and_other_services_still_checked():
    error = ClientError({"Error": {"Code": "SubscriptionRequiredException"}}, "ListStreams")
    session = Session(error)
    report = module.preflight(session, "123456789012")
    assert report["state"] == "blocked"
    assert report["services"]["kinesis"]["error_code"] == "SubscriptionRequiredException"
    assert report["services"]["redshift-serverless"]["state"] == "accessible"
    assert session.calls.count("kinesis") == 1
    assert report["resources_created"] == 0
    assert "sensitive_inventory" not in str(report)
    assert "123456789012" not in str(report)


def test_network_failure_is_unavailable_and_success_is_not_deployment_acceptance():
    report = module.preflight(
        Session(EndpointConnectionError(endpoint_url="offline")), "123456789012"
    )
    assert report["services"]["kinesis"]["state"] == "unavailable"
    passed = module.preflight(Session(), "123456789012")
    assert passed["state"] == "pass"
    assert "not deployment acceptance" in passed["scope"]
