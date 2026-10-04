-- Current snapshot, shipment grain; original promise remains the SLA target.
-- Temporal self-join uses calendar bounds rather than ROWS on sparse dates.
WITH delivered AS (
    SELECT carrier_id, country, delivered_at::DATE AS metric_date,
           count(*) AS delivered_count,
           sum(CASE WHEN delivered_on_time THEN 0 ELSE 1 END) AS late_count,
           sum(dispatch_to_delivery_seconds) AS transit_seconds
    FROM commerce.shipment_lifecycle WHERE delivered_at IS NOT NULL
    GROUP BY carrier_id, country, delivered_at::DATE
), rolling AS (
    SELECT anchor.carrier_id, anchor.country, anchor.metric_date,
           sum(history.late_count)::DECIMAL(24,6) / nullif(sum(history.delivered_count), 0) AS delay_rate_30d,
           sum(history.transit_seconds) / nullif(sum(history.delivered_count), 0) AS average_transit_30d
    FROM delivered anchor JOIN delivered history
      ON anchor.carrier_id = history.carrier_id AND anchor.country = history.country
     AND history.metric_date >= dateadd(day, -30, anchor.metric_date)
     AND history.metric_date < anchor.metric_date
    GROUP BY anchor.carrier_id, anchor.country, anchor.metric_date
)
SELECT *, dense_rank() OVER (PARTITION BY country, metric_date ORDER BY delay_rate_30d DESC) AS delay_rank
FROM rolling ORDER BY metric_date, country, carrier_id;

-- Incremental extraction uses the deterministic snapshot cutoff, not event time.
-- Consumers pin a release and save that release ID with their previous cutoff.
SELECT shipment_id, state, delivered_at, updated_at
FROM commerce.shipment_lifecycle
WHERE updated_at > :previous_snapshot_cutoff AND updated_at <= :current_snapshot_cutoff;
-- This is an upsert feed only. Consume affected-key tombstones from the release
-- manifest as well to remove rows invalidated by corrections.
