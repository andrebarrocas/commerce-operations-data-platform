-- Atomic by default: no NONATOMIC, COMMIT or TRUNCATE inside this procedure.
-- Each client stages in its own retained session. Explicit affected keys include
-- deleted rows; a full-snapshot load uses the union of old and new shipment IDs.
CREATE OR REPLACE PROCEDURE commerce.sp_publish_shipment_release(
    p_release_id VARCHAR(100), p_source_hash VARCHAR(64), p_expected_rows BIGINT
) AS $$
DECLARE
    n BIGINT;
    existing_hash VARCHAR(64);
BEGIN
    IF p_release_id IS NULL OR p_source_hash IS NULL OR len(p_source_hash) <> 64
       OR p_expected_rows IS NULL OR p_expected_rows < 0 THEN
        RAISE EXCEPTION 'Invalid release identity or control total';
    END IF;
    LOCK TABLE commerce.load_ledger;
    LOCK TABLE commerce.shipment_lifecycle;
    SELECT count(*) INTO n FROM commerce.load_ledger WHERE release_id = p_release_id;
    IF n > 1 THEN RAISE EXCEPTION 'Corrupt duplicate release ledger'; END IF;
    IF n = 1 THEN
        SELECT source_manifest_sha256 INTO existing_hash
        FROM commerce.load_ledger WHERE release_id = p_release_id;
        IF existing_hash <> p_source_hash THEN
            RAISE EXCEPTION 'Release ID reused for a different source';
        END IF;
        RETURN;
    END IF;
    SELECT count(*) INTO n FROM stage_shipment_lifecycle;
    IF n <> p_expected_rows THEN RAISE EXCEPTION 'Staged row count mismatch'; END IF;
    SELECT count(*) INTO n FROM (
        SELECT shipment_id FROM stage_shipment_lifecycle GROUP BY shipment_id HAVING count(*) > 1
    ) d;
    IF n > 0 THEN RAISE EXCEPTION 'Duplicate source shipment keys'; END IF;
    SELECT count(*) INTO n FROM (
        SELECT shipment_id FROM commerce.shipment_lifecycle GROUP BY shipment_id HAVING count(*) > 1
    ) d;
    IF n > 0 THEN RAISE EXCEPTION 'Duplicate target shipment keys'; END IF;
    SELECT count(*) INTO n FROM stage_shipment_lifecycle s
    WHERE NOT EXISTS (SELECT 1 FROM affected_shipments a WHERE a.shipment_id = s.shipment_id);
    IF n > 0 THEN RAISE EXCEPTION 'Source row outside declared replacement scope'; END IF;
    SELECT count(*) INTO n FROM stage_shipment_lifecycle
    WHERE dispatch_to_delivery_seconds < 0 OR failed_delivery_attempts > delivery_attempts;
    IF n > 0 THEN RAISE EXCEPTION 'Staged product constraint violated'; END IF;
    DELETE FROM commerce.shipment_lifecycle
    WHERE shipment_id IN (
        SELECT a.shipment_id FROM affected_shipments a
        LEFT JOIN stage_shipment_lifecycle s ON a.shipment_id = s.shipment_id
        WHERE s.shipment_id IS NULL
    );
    MERGE INTO commerce.shipment_lifecycle USING stage_shipment_lifecycle s
    ON commerce.shipment_lifecycle.shipment_id = s.shipment_id
    WHEN MATCHED THEN UPDATE SET
        order_id = s.order_id,
        origin_facility_id = s.origin_facility_id,
        carrier_id = s.carrier_id,
        country = s.country,
        region = s.region,
        dispatched_at = s.dispatched_at,
        promised_delivery_at = s.promised_delivery_at,
        out_for_delivery_at = s.out_for_delivery_at,
        delivered_at = s.delivered_at,
        last_failure_at = s.last_failure_at,
        delivery_attempts = s.delivery_attempts,
        failed_delivery_attempts = s.failed_delivery_attempts,
        delay_events = s.delay_events,
        package_count = s.package_count,
        delivered_on_time = s.delivered_on_time,
        dispatch_to_delivery_seconds = s.dispatch_to_delivery_seconds,
        state = s.state,
        last_event_at = s.last_event_at,
        updated_at = s.updated_at
    WHEN NOT MATCHED THEN INSERT (shipment_id, order_id, origin_facility_id, carrier_id, country, region, dispatched_at, promised_delivery_at, out_for_delivery_at, delivered_at, last_failure_at, delivery_attempts, failed_delivery_attempts, delay_events, package_count, delivered_on_time, dispatch_to_delivery_seconds, state, last_event_at, updated_at)
    VALUES (s.shipment_id, s.order_id, s.origin_facility_id, s.carrier_id, s.country, s.region, s.dispatched_at, s.promised_delivery_at, s.out_for_delivery_at, s.delivered_at, s.last_failure_at, s.delivery_attempts, s.failed_delivery_attempts, s.delay_events, s.package_count, s.delivered_on_time, s.dispatch_to_delivery_seconds, s.state, s.last_event_at, s.updated_at);
    INSERT INTO commerce.load_ledger
    VALUES (p_release_id, p_source_hash, p_expected_rows, GETDATE());
END;
$$ LANGUAGE plpgsql SECURITY INVOKER;
