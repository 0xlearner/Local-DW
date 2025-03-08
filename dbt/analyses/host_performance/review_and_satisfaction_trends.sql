-- Review and satisfaction trends
WITH host_review_metrics AS (
    SELECT
        host_id,
        host_name,
        month_date,
        review_count,
        positive_review_percentage,
        avg_review_length,
        occupancy_rate,
        estimated_revenue
    FROM {{ ref('host_performance_monthly') }}
    WHERE review_count > 0
)

SELECT
    host_id,
    host_name,
    COUNT(DISTINCT month_date) AS months_with_reviews,
    SUM(review_count) AS total_reviews,
    ROUND(AVG(positive_review_percentage), 1) AS avg_positive_review_pct,
    ROUND(AVG(avg_review_length), 0) AS avg_review_length,
    ROUND(AVG(occupancy_rate), 1) AS avg_occupancy,

    -- Calculate correlation between reviews and business metrics
    CORR(positive_review_percentage, occupancy_rate) AS review_occupancy_correlation,
    CORR(positive_review_percentage, estimated_revenue) AS review_revenue_correlation,

    -- Satisfaction trend (increasing or decreasing)
    CASE
        WHEN COUNT(DISTINCT month_date) >= 3 AND
             CORR(positive_review_percentage,
                  EXTRACT(EPOCH FROM month_date)) > 0.3
        THEN 'Improving'
        WHEN COUNT(DISTINCT month_date) >= 3 AND
             CORR(positive_review_percentage,
                  EXTRACT(EPOCH FROM month_date)) < -0.3
        THEN 'Declining'
        ELSE 'Stable'
    END AS satisfaction_trend
FROM host_review_metrics
GROUP BY host_id, host_name
HAVING COUNT(DISTINCT month_date) >= 2 -- At least 2 months of review data
ORDER BY total_reviews DESC
LIMIT 50
