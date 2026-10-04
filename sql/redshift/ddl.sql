-- Apply explicitly in a chosen Redshift database. No cloud execution is implied.
-- Grain: one current row per dispatched shipment. Keys are checked by the loader;
-- Redshift uniqueness constraints alone would be informational, not enforcement.
CREATE SCHEMA IF NOT EXISTS commerce;
CREATE TABLE IF NOT EXISTS commerce.shipment_lifecycle (
    shipment_id VARCHAR(100) NOT NULL,
    order_id VARCHAR(100) NOT NULL,
    origin_facility_id VARCHAR(100) NOT NULL,
    carrier_id VARCHAR(100) NOT NULL,
    country VARCHAR(100) NOT NULL,
    region VARCHAR(100) NOT NULL,
    dispatched_at TIMESTAMPTZ NOT NULL,
    promised_delivery_at TIMESTAMPTZ NOT NULL,
    out_for_delivery_at TIMESTAMPTZ,
    delivered_at TIMESTAMPTZ,
    last_failure_at TIMESTAMPTZ,
    delivery_attempts BIGINT NOT NULL,
    failed_delivery_attempts BIGINT NOT NULL,
    delay_events BIGINT NOT NULL,
    package_count BIGINT NOT NULL,
    delivered_on_time BOOLEAN,
    dispatch_to_delivery_seconds DOUBLE PRECISION,
    state VARCHAR(100) NOT NULL,
    last_event_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
) DISTSTYLE AUTO SORTKEY AUTO ENCODE AUTO;
CREATE TABLE IF NOT EXISTS commerce.load_ledger (
    release_id VARCHAR(100) NOT NULL,
    source_manifest_sha256 VARCHAR(64) NOT NULL,
    staged_rows BIGINT NOT NULL,
    committed_at TIMESTAMPTZ NOT NULL
) DISTSTYLE ALL;
-- Staging is SESSION-LOCAL. Create these in the same retained Data API session
-- that loads the immutable manifest and calls the publish procedure.
CREATE TEMP TABLE stage_shipment_lifecycle (LIKE commerce.shipment_lifecycle);
CREATE TEMP TABLE affected_shipments (shipment_id VARCHAR(100) NOT NULL);
