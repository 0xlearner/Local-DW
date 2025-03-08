-- Month-over-month growth performance
WITH filtered_performance AS (
    SELECT *
    FROM {{ ref('host_performance_monthly') }}
    WHERE month_date <= CURRENT_DATE  -- Only include historical data
)

SELECT
    month_date,
    TO_CHAR(month_date, 'YYYY-MM') AS month,
    COUNT(DISTINCT host_id) AS active_hosts,
    SUM(active_listings) AS total_listings,
    ROUND(AVG(occupancy_rate), 1) AS avg_occupancy,
    SUM(estimated_revenue) AS total_revenue,

    -- Growth metrics
    ROUND(
        (SUM(estimated_revenue) - LAG(SUM(estimated_revenue)) OVER (ORDER BY month_date)) /
        NULLIF(LAG(SUM(estimated_revenue)) OVER (ORDER BY month_date), 0) * 100,
    1) AS platform_revenue_growth,

    ROUND(
        (AVG(occupancy_rate) - LAG(AVG(occupancy_rate)) OVER (ORDER BY month_date)) /
        NULLIF(LAG(AVG(occupancy_rate)) OVER (ORDER BY month_date), 0) * 100,
    1) AS platform_occupancy_growth
FROM filtered_performance
GROUP BY month_date, month
ORDER BY month_date
