-- Effective canonical history is cutoff-filtered by the caller. The authoritative
-- lifecycle order is effective_version (the correction target's original slot).
CREATE TEMP TABLE events AS
SELECT * EXCLUDE (event_timestamp, ingestion_timestamp, payload_json),
       event_timestamp::TIMESTAMPTZ AS event_at,
       ingestion_timestamp::TIMESTAMPTZ AS observed_at,
       payload_json::JSON AS payload
FROM input_events;

CREATE TEMP VIEW orders AS
SELECT order_id, country, region, event_at AS order_created_at,
       json_extract_string(payload, '$.currency') AS currency,
       json_extract_string(payload, '$.promised_delivery_at')::TIMESTAMPTZ AS promised_delivery_at,
       json_extract_string(payload, '$.order_total_minor')::BIGINT AS order_total_minor,
       payload
FROM events WHERE event_type = 'ORDER_CREATED';

CREATE TEMP VIEW order_lines AS
SELECT o.order_id, json_extract_string(j.value, '$.line_item_id') AS line_item_id,
       json_extract_string(j.value, '$.sku_id') AS sku_id,
       json_extract_string(j.value, '$.quantity')::BIGINT AS quantity,
       json_extract_string(j.value, '$.unit_price_minor')::BIGINT AS unit_price_minor
FROM orders o, json_each(o.payload, '$.line_items') j;

CREATE TEMP VIEW packages AS
SELECT package_id, order_id, facility_id, event_at AS created_at, payload
FROM events WHERE event_type = 'PACKAGE_CREATED';

CREATE TEMP VIEW package_lines AS
SELECT p.package_id, p.order_id,
       json_extract_string(j.value, '$.line_item_id') AS line_item_id,
       json_extract_string(j.value, '$.quantity')::BIGINT AS quantity
FROM packages p, json_each(p.payload, '$.line_items') j;

CREATE TEMP VIEW dispatches AS
SELECT shipment_id, order_id, facility_id, carrier_id, country, region,
       event_at AS dispatched_at, payload,
       json_extract_string(payload, '$.promised_delivery_at')::TIMESTAMPTZ AS promised_delivery_at
FROM events WHERE event_type = 'SHIPMENT_DISPATCHED';

CREATE TEMP VIEW shipment_packages AS
SELECT d.shipment_id, d.order_id, json_extract_string(j.value, '$') AS package_id
FROM dispatches d, json_each(d.payload, '$.package_ids') j;

CREATE TEMP VIEW shipment_dispatch_latency AS
SELECT d.shipment_id,
       date_diff('microsecond', min(p.created_at), d.dispatched_at) / 1000000.0 AS seconds
FROM dispatches d
LEFT JOIN shipment_packages sp USING (shipment_id)
LEFT JOIN packages p USING (package_id)
GROUP BY d.shipment_id, d.dispatched_at;

CREATE TABLE shipment_lifecycle AS
WITH ranked AS (
    SELECT *, row_number() OVER (
        PARTITION BY shipment_id ORDER BY effective_version DESC, event_id
    ) AS recency_rank
    FROM events WHERE entity_type = 'shipment'
), history AS (
    SELECT shipment_id,
           min(event_at) FILTER (WHERE event_type = 'OUT_FOR_DELIVERY') AS out_for_delivery_at,
           max(event_at) FILTER (WHERE event_type = 'DELIVERED') AS delivered_at,
           max(event_at) FILTER (WHERE event_type = 'DELIVERY_FAILED') AS last_failure_at,
           count(DISTINCT json_extract_string(payload, '$.delivery_attempt_id'))::BIGINT AS delivery_attempts,
           count(DISTINCT json_extract_string(payload, '$.delivery_attempt_id'))
               FILTER (WHERE event_type = 'DELIVERY_FAILED')::BIGINT AS failed_delivery_attempts,
           count(*) FILTER (WHERE event_type = 'SHIPMENT_DELAYED')::BIGINT AS delay_events,
           max(event_at) AS last_event_at
    FROM events WHERE entity_type = 'shipment' GROUP BY shipment_id
)
SELECT d.shipment_id, d.order_id, d.facility_id AS origin_facility_id,
       d.carrier_id, d.country, d.region, d.dispatched_at, d.promised_delivery_at,
       h.out_for_delivery_at, h.delivered_at, h.last_failure_at,
       h.delivery_attempts, h.failed_delivery_attempts, h.delay_events,
       json_array_length(d.payload, '$.package_ids')::BIGINT AS package_count,
       CASE WHEN h.delivered_at IS NOT NULL THEN h.delivered_at <= d.promised_delivery_at END AS delivered_on_time,
       date_diff('microsecond', d.dispatched_at, h.delivered_at) / 1000000.0 AS dispatch_to_delivery_seconds,
       CASE r.event_type
           WHEN 'SHIPMENT_DISPATCHED' THEN 'dispatched'
           WHEN 'SHIPMENT_DELAYED' THEN 'delayed'
           WHEN 'OUT_FOR_DELIVERY' THEN 'out_for_delivery'
           WHEN 'DELIVERY_FAILED' THEN 'delivery_failed'
           WHEN 'DELIVERED' THEN 'delivered'
       END AS state,
       h.last_event_at, c.as_of AS updated_at
FROM dispatches d JOIN history h USING (shipment_id)
JOIN ranked r ON d.shipment_id = r.shipment_id AND r.recency_rank = 1
CROSS JOIN config c;

CREATE TABLE order_fulfillment AS
WITH line_totals AS (
    SELECT order_id, sum(quantity)::BIGINT AS ordered_units FROM order_lines GROUP BY order_id
), delivered_lines AS (
    SELECT pl.order_id, pl.line_item_id, sum(pl.quantity)::BIGINT AS delivered_units
    FROM package_lines pl JOIN shipment_packages sp USING (package_id, order_id)
    JOIN shipment_lifecycle sl USING (shipment_id)
    WHERE sl.delivered_at IS NOT NULL GROUP BY pl.order_id, pl.line_item_id
), coverage AS (
    SELECT ol.order_id,
           bool_and(coalesce(dl.delivered_units, 0) = ol.quantity) AS fully_delivered
    FROM order_lines ol LEFT JOIN delivered_lines dl USING (order_id, line_item_id)
    GROUP BY ol.order_id
), returns AS (
    SELECT e.order_id, sum(json_extract_string(j.value, '$.quantity')::BIGINT)::BIGINT AS units
    FROM events e, json_each(e.payload, '$.line_items') j
    WHERE e.event_type = 'RETURN_INITIATED' GROUP BY e.order_id
), order_events AS (
    SELECT order_id, max(event_at) AS last_event_at,
           max(event_at) FILTER (WHERE event_type = 'ORDER_CANCELLED') AS cancelled_at,
           count(*) FILTER (WHERE event_type = 'INVENTORY_RESERVED') AS reservations,
           count(DISTINCT package_id) FILTER (WHERE event_type = 'PACKAGE_CREATED')::BIGINT AS packages_created
    FROM events GROUP BY order_id
), shipments AS (
    SELECT order_id, count(*)::BIGINT AS shipments_dispatched,
           count(*) FILTER (WHERE delivered_at IS NOT NULL)::BIGINT AS delivered_shipments
    FROM shipment_lifecycle GROUP BY order_id
)
SELECT o.order_id, o.country, o.region, o.currency, o.order_created_at, o.promised_delivery_at,
       o.order_total_minor, lt.ordered_units, oe.cancelled_at, oe.packages_created,
       coalesce(s.shipments_dispatched, 0)::BIGINT AS shipments_dispatched,
       coalesce(s.delivered_shipments, 0)::BIGINT AS delivered_shipments,
       coalesce(r.units, 0)::BIGINT AS return_initiated_units,
       CASE WHEN oe.cancelled_at IS NOT NULL THEN 'cancelled'
            WHEN cv.fully_delivered THEN 'delivered'
            WHEN coalesce(s.delivered_shipments, 0) > 0 THEN 'partially_delivered'
            WHEN coalesce(s.shipments_dispatched, 0) > 0 THEN 'dispatched'
            WHEN oe.packages_created > 0 THEN 'packaged'
            WHEN oe.reservations > 0 THEN 'reserved' ELSE 'created' END AS fulfillment_status,
       (oe.cancelled_at IS NOT NULL OR cv.fully_delivered) AS is_complete,
       oe.last_event_at, c.as_of AS updated_at
FROM orders o JOIN line_totals lt USING (order_id) JOIN order_events oe USING (order_id)
JOIN coverage cv USING (order_id)
LEFT JOIN shipments s USING (order_id) LEFT JOIN returns r USING (order_id)
CROSS JOIN config c;

CREATE TABLE inventory_movements AS
SELECT e.event_id, json_extract_string(payload, '$.line_item_id') AS line_item_id,
       e.order_id, json_extract_string(payload, '$.reservation_id') AS reservation_id,
       e.effective_version::BIGINT AS entity_version, e.facility_id,
       json_extract_string(payload, '$.sku_id') AS sku_id,
       event_at AS event_timestamp, event_at::DATE AS event_date,
       CASE WHEN event_type = 'INVENTORY_RESERVED' THEN 'reserved' ELSE 'released' END AS movement_type,
       json_extract_string(payload, '$.quantity')::BIGINT AS quantity,
       json_extract_string(payload, '$.quantity')::BIGINT *
           CASE WHEN event_type = 'INVENTORY_RESERVED' THEN 1 ELSE -1 END AS reserved_quantity_delta,
       json_extract_string(payload, '$.quantity')::BIGINT *
           CASE WHEN event_type = 'INVENTORY_RESERVED' THEN -1 ELSE 1 END AS available_quantity_delta,
       correction_of_event_id, c.as_of AS updated_at
FROM events e CROSS JOIN config c
WHERE event_type IN ('INVENTORY_RESERVED', 'INVENTORY_RELEASED');

CREATE TABLE facility_performance AS
SELECT e.facility_id, f.country, f.region,
       date_trunc('hour', e.event_at) AS window_start,
       date_trunc('hour', e.event_at) + INTERVAL 1 HOUR AS window_end,
       count(*)::BIGINT AS facility_volume_1h,
       coalesce(sum(json_extract_string(e.payload, '$.quantity')::BIGINT)
           FILTER (WHERE e.event_type = 'INVENTORY_RESERVED'), 0)::BIGINT AS reserved_units,
       coalesce(sum(json_extract_string(e.payload, '$.quantity')::BIGINT)
           FILTER (WHERE e.event_type = 'INVENTORY_RELEASED'), 0)::BIGINT AS released_units,
       count(*) FILTER (WHERE e.event_type = 'PACKAGE_CREATED')::BIGINT AS packages_created,
       count(*) FILTER (WHERE e.event_type = 'SHIPMENT_DISPATCHED')::BIGINT AS shipments_dispatched,
       avg(l.seconds) FILTER (WHERE e.event_type = 'SHIPMENT_DISPATCHED') AS avg_dispatch_latency_seconds,
       c.as_of AS updated_at
FROM events e JOIN reference_facilities f USING (facility_id)
LEFT JOIN shipment_dispatch_latency l ON e.shipment_id = l.shipment_id
CROSS JOIN config c
WHERE e.event_type IN ('INVENTORY_RESERVED', 'INVENTORY_RELEASED', 'PACKAGE_CREATED', 'SHIPMENT_DISPATCHED')
GROUP BY e.facility_id, f.country, f.region, window_start, window_end, c.as_of;

CREATE TEMP VIEW attempt_events AS
SELECT e.*, json_extract_string(e.payload, '$.delivery_attempt_id') AS delivery_attempt_id,
       s.delivered_on_time, s.dispatch_to_delivery_seconds
FROM events e JOIN shipment_lifecycle s USING (shipment_id)
WHERE e.event_type IN ('OUT_FOR_DELIVERY', 'DELIVERED', 'DELIVERY_FAILED');

CREATE TABLE carrier_performance AS
WITH measured AS (
    SELECT e.carrier_id, e.country, e.event_at::DATE AS metric_date,
           count(DISTINCT e.shipment_id) FILTER (WHERE e.event_type = 'SHIPMENT_DISPATCHED')::BIGINT AS dispatched_shipments,
           count(DISTINCT (e.shipment_id, json_extract_string(e.payload, '$.delivery_attempt_id'))) FILTER (WHERE e.event_type = 'OUT_FOR_DELIVERY')::BIGINT AS delivery_attempts,
           count(DISTINCT (e.shipment_id, json_extract_string(e.payload, '$.delivery_attempt_id'))) FILTER (WHERE e.event_type = 'DELIVERY_FAILED')::BIGINT AS failed_delivery_attempts,
           count(DISTINCT (e.shipment_id, json_extract_string(e.payload, '$.delivery_attempt_id'))) FILTER (WHERE e.event_type IN ('DELIVERED', 'DELIVERY_FAILED'))::BIGINT AS completed_delivery_attempts,
           count(DISTINCT e.shipment_id) FILTER (WHERE e.event_type = 'DELIVERED')::BIGINT AS delivered_shipments,
           count(DISTINCT e.shipment_id) FILTER (WHERE e.event_type = 'DELIVERED' AND s.delivered_on_time)::BIGINT AS on_time_deliveries,
           count(DISTINCT e.shipment_id) FILTER (WHERE e.event_type = 'DELIVERED' AND NOT s.delivered_on_time)::BIGINT AS late_deliveries,
           avg(s.dispatch_to_delivery_seconds) FILTER (WHERE e.event_type = 'DELIVERED') AS avg_transit_seconds
    FROM events e JOIN shipment_lifecycle s USING (shipment_id)
    WHERE e.event_type IN ('SHIPMENT_DISPATCHED', 'OUT_FOR_DELIVERY', 'DELIVERED', 'DELIVERY_FAILED')
    GROUP BY e.carrier_id, e.country, metric_date
)
SELECT m.* EXCLUDE (avg_transit_seconds),
       on_time_deliveries::DOUBLE / nullif(delivered_shipments, 0) AS on_time_delivery_rate,
       avg_transit_seconds, c.as_of AS updated_at FROM measured m CROSS JOIN config c;

CREATE TABLE delivery_reliability AS
WITH measured AS (
    SELECT country, region, event_at::DATE AS metric_date,
           count(DISTINCT shipment_id) FILTER (WHERE event_type = 'DELIVERED')::BIGINT AS delivered_shipments,
           count(DISTINCT shipment_id) FILTER (WHERE event_type = 'DELIVERED' AND delivered_on_time)::BIGINT AS on_time_deliveries,
           count(DISTINCT shipment_id) FILTER (WHERE event_type = 'DELIVERED' AND NOT delivered_on_time)::BIGINT AS late_deliveries,
           count(DISTINCT (shipment_id, delivery_attempt_id))::BIGINT AS completed_delivery_attempts,
           count(DISTINCT (shipment_id, delivery_attempt_id)) FILTER (WHERE event_type = 'DELIVERY_FAILED')::BIGINT AS failed_delivery_attempts,
           quantile_cont(dispatch_to_delivery_seconds, 0.95) FILTER (WHERE event_type = 'DELIVERED') AS p95_transit_seconds
    FROM attempt_events WHERE event_type IN ('DELIVERED', 'DELIVERY_FAILED')
    GROUP BY country, region, metric_date
)
SELECT m.* EXCLUDE (p95_transit_seconds),
       on_time_deliveries::DOUBLE / nullif(delivered_shipments, 0) AS on_time_delivery_rate,
       failed_delivery_attempts::DOUBLE / nullif(completed_delivery_attempts, 0) AS failed_delivery_rate,
       p95_transit_seconds, c.as_of AS updated_at FROM measured m CROSS JOIN config c;

-- Serving models: order and shipment facts are accumulating snapshots; line and
-- attempt facts avoid fanout when consumers need lower-grain operational measures.
CREATE TABLE fact_order_fulfillment AS SELECT * FROM order_fulfillment;
CREATE TABLE fact_order_line AS
SELECT ol.*, o.currency, o.order_created_at::DATE AS order_date,
       (ol.quantity * ol.unit_price_minor)::BIGINT AS line_total_minor
FROM order_lines ol JOIN orders o USING (order_id);
CREATE TABLE fact_shipment_delivery AS SELECT * FROM shipment_lifecycle;
CREATE TABLE fact_inventory_movement AS SELECT * FROM inventory_movements;
CREATE TABLE fact_facility_hour AS
SELECT f.*, f.avg_dispatch_latency_seconds * f.shipments_dispatched AS dispatch_latency_sum_seconds
FROM facility_performance f;
CREATE TABLE fact_delivery_attempt AS
SELECT shipment_id, delivery_attempt_id, min(order_id) AS order_id,
       min(event_at) FILTER (WHERE event_type = 'OUT_FOR_DELIVERY') AS started_at,
       max(event_at) FILTER (WHERE event_type IN ('DELIVERED', 'DELIVERY_FAILED')) AS outcome_at,
       CASE WHEN count(*) FILTER (WHERE event_type = 'DELIVERED') > 0 THEN 'delivered'
            WHEN count(*) FILTER (WHERE event_type = 'DELIVERY_FAILED') > 0 THEN 'failed'
            ELSE 'pending' END AS outcome
FROM attempt_events GROUP BY shipment_id, delivery_attempt_id;
CREATE TABLE dim_facility AS SELECT * FROM reference_facilities;
CREATE TABLE dim_carrier AS SELECT * FROM reference_carriers;
CREATE TABLE dim_sku AS SELECT * FROM reference_skus;
CREATE TABLE dim_date AS
SELECT day::DATE AS calendar_date, year(day)::BIGINT AS calendar_year,
       month(day)::BIGINT AS calendar_month, dayofweek(day)::BIGINT AS day_of_week
FROM generate_series((SELECT min(event_at)::DATE FROM events),
                     (SELECT max(event_at)::DATE FROM events), INTERVAL 1 DAY) d(day);

-- Features are for active shipment scoring at this cutoff. Training extracts must
-- rerun canonicalization at each historical cutoff before executing this SQL.
CREATE TABLE features_delivery AS
WITH carrier_history AS (
    SELECT s.carrier_id, s.country,
           count(*) FILTER (WHERE NOT s.delivered_on_time)::DOUBLE / nullif(count(*), 0) AS carrier_delay_rate_30d
    FROM shipment_lifecycle s CROSS JOIN config c
    WHERE s.delivered_at >= date_trunc('day', c.as_of) - INTERVAL 30 DAY
      AND s.delivered_at < date_trunc('day', c.as_of)
    GROUP BY s.carrier_id, s.country
), failures AS (
    SELECT e.carrier_id, e.country,
           count(*) FILTER (WHERE e.event_type = 'DELIVERY_FAILED')::DOUBLE / nullif(count(*), 0) AS recent_failed_delivery_rate
    FROM attempt_events e CROSS JOIN config c
    WHERE e.event_type IN ('DELIVERY_FAILED', 'DELIVERED')
      AND e.event_at >= c.as_of - INTERVAL 7 DAY AND e.event_at < c.as_of
    GROUP BY e.carrier_id, e.country
), facility AS (
    SELECT e.facility_id,
           count(*) FILTER (WHERE e.event_at >= c.as_of - INTERVAL 1 HOUR AND e.event_at < c.as_of)::BIGINT AS facility_volume_1h
    FROM events e CROSS JOIN config c
    WHERE e.event_type IN ('INVENTORY_RESERVED', 'INVENTORY_RELEASED', 'PACKAGE_CREATED', 'SHIPMENT_DISPATCHED')
    GROUP BY e.facility_id
), latency AS (
    SELECT d.facility_id, avg(l.seconds) AS avg_dispatch_latency_7d
    FROM dispatches d JOIN shipment_dispatch_latency l USING (shipment_id) CROSS JOIN config c
    WHERE d.dispatched_at >= c.as_of - INTERVAL 7 DAY AND d.dispatched_at < c.as_of
    GROUP BY d.facility_id
), scans AS (
    SELECT sp.shipment_id, max(e.event_at) AS last_scan_at
    FROM shipment_packages sp JOIN events e USING (package_id) CROSS JOIN config c
    WHERE e.event_type = 'PACKAGE_SCANNED' AND e.event_at < c.as_of GROUP BY sp.shipment_id
)
SELECT s.shipment_id, s.carrier_id, s.origin_facility_id, s.country,
       c.as_of AS feature_as_of, c.source_snapshot_id, '1.0.0' AS feature_contract_version,
       ch.carrier_delay_rate_30d, coalesce(f.facility_volume_1h, 0)::BIGINT AS facility_volume_1h,
       l.avg_dispatch_latency_7d, fr.recent_failed_delivery_rate,
       date_diff('microsecond', sc.last_scan_at, c.as_of) / 60000000.0 AS package_scan_gap_minutes
FROM shipment_lifecycle s CROSS JOIN config c
LEFT JOIN carrier_history ch USING (carrier_id, country)
LEFT JOIN failures fr USING (carrier_id, country)
LEFT JOIN facility f ON s.origin_facility_id = f.facility_id
LEFT JOIN latency l ON s.origin_facility_id = l.facility_id
LEFT JOIN scans sc USING (shipment_id)
WHERE s.delivered_at IS NULL;

-- This describes reservation pressure only. Dispatch consumption and physical
-- stock receipts are outside the movement ledger; this is NOT on-hand stock.
CREATE TABLE features_inventory AS
SELECT i.facility_id, i.sku_id, c.as_of AS feature_as_of, c.source_snapshot_id,
       '1.0.0' AS feature_contract_version,
       sum(i.reserved_quantity_delta)::BIGINT AS cumulative_reserved_minus_released_units,
       coalesce(sum(i.quantity) FILTER (WHERE i.movement_type = 'reserved'
           AND i.event_timestamp >= c.as_of - INTERVAL 7 DAY AND i.event_timestamp < c.as_of), 0)::BIGINT AS reserved_units_7d
FROM inventory_movements i CROSS JOIN config c
GROUP BY i.facility_id, i.sku_id, c.as_of, c.source_snapshot_id;

CREATE TABLE features_facility_anomaly AS
SELECT f.facility_id, f.window_start, f.facility_volume_1h,
       avg(f.facility_volume_1h) OVER (
           PARTITION BY f.facility_id ORDER BY f.window_start
           RANGE BETWEEN INTERVAL 7 DAY PRECEDING AND INTERVAL 1 MICROSECOND PRECEDING
       ) AS prior_observed_hour_mean_7d,
       rank() OVER (PARTITION BY f.window_start ORDER BY f.facility_volume_1h DESC) AS volume_rank,
       c.as_of AS feature_as_of, c.source_snapshot_id, '1.0.0' AS feature_contract_version
FROM facility_performance f CROSS JOIN config c
WHERE f.window_end <= c.as_of;
