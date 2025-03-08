-- Seasonal performance patterns
SELECT
    EXTRACT(MONTH FROM month_date) AS month_num,
    TO_CHAR(month_date, 'Month') AS month_name,
    COUNT(DISTINCT host_id) AS active_hosts,
    ROUND(AVG(occupancy_rate), 1) AS avg_occupancy,
    ROUND(AVG(active_listings), 1) AS avg_listings_per_host,
    ROUND(AVG(estimated_revenue), 2) AS avg_host_revenue,
    SUM(estimated_revenue) AS total_platform_revenue,
    ROUND(AVG(positive_review_percentage), 1) AS avg_positive_review_pct
FROM {{ ref('host_performance_monthly') }}
GROUP BY EXTRACT(MONTH FROM month_date), TO_CHAR(month_date, 'Month')
ORDER BY month_num
