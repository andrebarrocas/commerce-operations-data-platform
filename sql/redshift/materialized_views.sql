-- An experiment for repeated analytical scans; refresh is an explicit operation.
CREATE MATERIALIZED VIEW commerce.mv_carrier_delivery_day
AUTO REFRESH NO AS
SELECT carrier_id, country, delivered_at::DATE AS metric_date,
       count(*) AS delivered_shipments,
       sum(CASE WHEN delivered_on_time THEN 1 ELSE 0 END) AS on_time_deliveries,
       sum(dispatch_to_delivery_seconds) AS transit_seconds
FROM commerce.shipment_lifecycle
WHERE delivered_at IS NOT NULL
GROUP BY carrier_id, country, delivered_at::DATE;

REFRESH MATERIALIZED VIEW commerce.mv_carrier_delivery_day;
-- Measure refresh separately and compare every aggregate to its base query.
-- MV freshness can lag the committed base release; do not imply atomic MV refresh.
