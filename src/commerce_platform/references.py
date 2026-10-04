"""Small, public synthetic registries used by the generator and local products."""

from typing import Any

FACILITIES: list[dict[str, Any]] = [
    {
        "facility_id": "FC-PT-01",
        "facility_name": "Atlantic Fulfillment",
        "country": "PT",
        "region": "PT-11",
    },
    {
        "facility_id": "FC-DE-01",
        "facility_name": "Central Fulfillment",
        "country": "DE",
        "region": "DE-BE",
    },
    {
        "facility_id": "FC-US-01",
        "facility_name": "Pacific Fulfillment",
        "country": "US",
        "region": "US-WA",
    },
    {
        "facility_id": "FC-GB-01",
        "facility_name": "Island Fulfillment",
        "country": "GB",
        "region": "GB-LND",
    },
    {
        "facility_id": "FC-JP-01",
        "facility_name": "Eastern Fulfillment",
        "country": "JP",
        "region": "JP-13",
    },
]

CARRIERS: list[dict[str, Any]] = [
    {
        "carrier_id": "CAR-01",
        "carrier_name": "Synthetic Parcel One",
        "supported_countries": ["PT", "DE", "US", "GB", "JP"],
    },
    {
        "carrier_id": "CAR-02",
        "carrier_name": "Synthetic Parcel Two",
        "supported_countries": ["PT", "DE", "US", "GB", "JP"],
    },
    {
        "carrier_id": "CAR-03",
        "carrier_name": "Synthetic Parcel Three",
        "supported_countries": ["PT", "DE", "US", "GB", "JP"],
    },
]

SKUS: list[dict[str, Any]] = [
    {
        "sku_id": f"SKU-{index:03d}",
        "sku_name": f"Synthetic household item {index}",
        "unit_weight_grams": 100 + index * 75,
    }
    for index in range(1, 13)
]
