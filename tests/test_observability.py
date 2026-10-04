import pytest

from commerce_platform.observability import emf, estimate_cost


def test_low_cardinality_and_unknown_cost():
    report = emf({"records_processed": 10, "processing_seconds": 1.2})
    dims = report["_aws"]["CloudWatchMetrics"][0]["Dimensions"][0]
    assert dims == ["Environment", "Pipeline", "Stage"]
    assert (
        estimate_cost(100, "byte_month", None, region="eu-west-1", price_date=None)["estimated_usd"]
        is None
    )
    with pytest.raises(ValueError):
        emf({"order_id": 123})
    with pytest.raises(ValueError):
        estimate_cost(1, "hour", 0.2, region="eu-west-1", price_date=None)
