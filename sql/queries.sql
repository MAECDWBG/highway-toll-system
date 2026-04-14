-- =============================================================================
-- sql/queries.sql
-- Sample Analytical Queries for Highway Toll Collection System
--
-- Author : Mayukh Ghosh | Roll No : 23052334
-- Batch  : Data Engineering 2025-2026, KIIT University
-- =============================================================================


-- 1. Total revenue today (Grafana KPI panel)
SELECT
    COALESCE(SUM(f.amount), 0) AS total_revenue_today,
    COUNT(*)                   AS total_transactions
FROM mart.fact_transaction f
JOIN mart.dim_date d ON f.date_id = d.date_id
WHERE d.date = CURRENT_DATE;


-- 2. Revenue per plaza — last 7 days
SELECT
    p.name            AS plaza_name,
    p.highway,
    SUM(f.amount)     AS total_revenue,
    COUNT(*)          AS tx_count,
    AVG(f.amount)     AS avg_toll
FROM mart.fact_transaction f
JOIN mart.dim_plaza p ON f.plaza_id = p.plaza_id
JOIN mart.dim_date  d ON f.date_id  = d.date_id
WHERE d.date >= CURRENT_DATE - 7
GROUP BY p.name, p.highway
ORDER BY total_revenue DESC;


-- 3. Hourly transaction volume heatmap (last 24 h)
SELECT
    d.date,
    a.hour,
    a.plaza_id,
    p.name             AS plaza_name,
    a.tx_count,
    a.total_revenue,
    a.flagged_count
FROM mart.agg_hourly_revenue a
JOIN mart.dim_date  d ON a.date_id  = d.date_id
JOIN mart.dim_plaza p ON a.plaza_id = p.plaza_id
WHERE d.date >= CURRENT_DATE - 1
ORDER BY d.date, a.hour, a.plaza_id;


-- 4. Vehicle class breakdown — current month
SELECT
    c.name               AS vehicle_class,
    COUNT(*)             AS tx_count,
    SUM(f.amount)        AS revenue,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct_share
FROM mart.fact_transaction f
JOIN mart.dim_class c ON f.class_id = c.class_id
JOIN mart.dim_date  d ON f.date_id  = d.date_id
WHERE d.year  = EXTRACT(year  FROM CURRENT_DATE)
  AND d.month = EXTRACT(month FROM CURRENT_DATE)
GROUP BY c.name
ORDER BY revenue DESC;


-- 5. Top 10 busiest vehicles (FASTag tags) — all time
SELECT
    v.tag_id,
    v.vehicle_no,
    v.registered_state,
    COUNT(*)      AS total_trips,
    SUM(f.amount) AS total_paid
FROM mart.fact_transaction f
JOIN mart.dim_vehicle v ON f.vehicle_id = v.vehicle_id
GROUP BY v.tag_id, v.vehicle_no, v.registered_state
ORDER BY total_trips DESC
LIMIT 10;


-- 6. Flagged / blacklisted transaction report
SELECT
    f.transaction_id,
    f.entry_time,
    v.tag_id,
    v.vehicle_no,
    p.name     AS plaza,
    f.amount,
    py.status
FROM mart.fact_transaction f
JOIN mart.dim_vehicle v  ON f.vehicle_id = v.vehicle_id
JOIN mart.dim_plaza   p  ON f.plaza_id   = p.plaza_id
JOIN mart.dim_payment py ON f.payment_id = py.payment_id
WHERE py.status = 'FLAGGED'
ORDER BY f.entry_time DESC
LIMIT 100;


-- 7. Weekend vs weekday revenue comparison
SELECT
    CASE WHEN d.is_weekend = 1 THEN 'Weekend' ELSE 'Weekday' END AS day_type,
    COUNT(*)        AS tx_count,
    SUM(f.amount)   AS total_revenue,
    AVG(f.amount)   AS avg_toll
FROM mart.fact_transaction f
JOIN mart.dim_date d ON f.date_id = d.date_id
WHERE d.date >= CURRENT_DATE - 30
GROUP BY day_type;


-- 8. Monthly revenue trend (window function)
SELECT
    d.year,
    d.month,
    SUM(f.amount)                                               AS monthly_revenue,
    SUM(SUM(f.amount)) OVER (
        PARTITION BY d.year ORDER BY d.month
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    )                                                           AS cumulative_revenue,
    LAG(SUM(f.amount)) OVER (ORDER BY d.year, d.month)         AS prev_month_revenue,
    ROUND(
        100.0 * (SUM(f.amount) - LAG(SUM(f.amount)) OVER (ORDER BY d.year, d.month))
              / NULLIF(LAG(SUM(f.amount)) OVER (ORDER BY d.year, d.month), 0),
        2
    )                                                           AS mom_growth_pct
FROM mart.fact_transaction f
JOIN mart.dim_date d ON f.date_id = d.date_id
GROUP BY d.year, d.month
ORDER BY d.year, d.month;


-- 9. Low-balance vehicles in last 24 hours (alert feed)
SELECT
    v.tag_id,
    v.vehicle_no,
    v.owner_name,
    v.registered_state,
    MAX(f.entry_time) AS last_seen,
    MIN(f.amount)     AS last_toll_amount
FROM mart.fact_transaction f
JOIN mart.dim_vehicle v  ON f.vehicle_id = v.vehicle_id
JOIN mart.dim_payment py ON f.payment_id = py.payment_id
WHERE py.status = 'LOW_BALANCE'
  AND f.entry_time >= DATEADD(hour, -24, GETDATE())
GROUP BY v.tag_id, v.vehicle_no, v.owner_name, v.registered_state
ORDER BY last_seen DESC;


-- 10. Per-highway SLA: average processing time (entry → exit)
SELECT
    p.highway,
    COUNT(*)                              AS tx_count,
    AVG(DATEDIFF(second, f.entry_time, f.exit_time)) AS avg_processing_sec,
    PERCENTILE_CONT(0.95) WITHIN GROUP (
        ORDER BY DATEDIFF(second, f.entry_time, f.exit_time)
    )                                     AS p95_processing_sec
FROM mart.fact_transaction f
JOIN mart.dim_plaza p ON f.plaza_id = p.plaza_id
WHERE f.exit_time IS NOT NULL
  AND f.entry_time >= CURRENT_DATE - 7
GROUP BY p.highway
ORDER BY avg_processing_sec DESC;
