-- Host consistency analysis
WITH host_metrics AS (
    SELECT
        host_id,
        host_name,
        COUNT(DISTINCT month_date) AS active_months,
        STDDEV(occupancy_rate) AS occupancy_volatility,
        STDDEV(revenue_growth_pct) AS revenue_volatility,
        MIN(occupancy_rate) AS min_occupancy,
        MAX(occupancy_rate) AS max_occupancy,
        AVG(occupancy_rate) AS avg_occupancy,
        SUM(estimated_revenue) AS total_revenue
    FROM {{ ref('host_performance_monthly') }}
    GROUP BY host_id, host_name
    HAVING COUNT(DISTINCT month_date) >= 3 -- At least 3 months of data
)

SELECT
    host_id,
    host_name,
    active_months,
    ROUND(avg_occupancy, 1) AS avg_occupancy,
    ROUND(occupancy_volatility, 1) AS occupancy_volatility,
    ROUND(revenue_volatility, 1) AS revenue_volatility,
    total_revenue,
    CASE
        WHEN occupancy_volatility < 5 AND avg_occupancy > 70 THEN 'Highly Consistent'
        WHEN occupancy_volatility < 10 AND avg_occupancy > 60 THEN 'Consistent'
        WHEN occupancy_volatility < 15 AND avg_occupancy > 50 THEN 'Moderately Consistent'
        WHEN occupancy_volatility >= 15 THEN 'Volatile'
        ELSE 'Low Performer'
    END AS consistency_rating
FROM host_metrics
ORDER BY
    CASE
        WHEN occupancy_volatility < 5 AND avg_occupancy > 70 THEN 1
        WHEN occupancy_volatility < 10 AND avg_occupancy > 60 THEN 2
        WHEN occupancy_volatility < 15 AND avg_occupancy > 50 THEN 3
        WHEN occupancy_volatility >= 15 THEN 4
        ELSE 5
    END,
    total_revenue DESC
LIMIT 100
