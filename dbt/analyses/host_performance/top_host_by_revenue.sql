-- Top hosts by revenue
SELECT
    host_id,
    host_name,
    COUNT(DISTINCT month_date) AS active_months,
    ROUND(AVG(active_listings), 1) AS avg_listings,
    SUM(estimated_revenue) AS total_revenue,
    ROUND(AVG(occupancy_rate), 1) AS avg_occupancy,
    ROUND(AVG(revenue_growth_pct), 1) AS avg_monthly_growth
FROM {{ ref('host_performance_monthly') }}
GROUP BY host_id, host_name
ORDER BY total_revenue DESC
LIMIT 20
