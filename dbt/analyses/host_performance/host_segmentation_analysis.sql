-- Host segmentation by performance
WITH host_segments AS (
    SELECT
        host_id,
        host_name,
        COUNT(DISTINCT month_date) AS active_months,
        AVG(active_listings) AS avg_listings,
        AVG(occupancy_rate) AS avg_occupancy,
        AVG(estimated_revenue) AS avg_monthly_revenue,
        SUM(estimated_revenue) AS total_revenue,
        AVG(positive_review_percentage) AS avg_positive_reviews
    FROM {{ ref('host_performance_monthly') }}
    GROUP BY host_id, host_name
    HAVING COUNT(DISTINCT month_date) >= 3 -- At least 3 months of data
)

SELECT
    host_id,
    host_name,
    active_months,
    ROUND(avg_listings, 1) AS avg_listings,
    ROUND(avg_occupancy, 1) AS avg_occupancy,
    ROUND(avg_monthly_revenue, 2) AS avg_monthly_revenue,
    ROUND(total_revenue, 2) AS total_revenue,
    ROUND(avg_positive_reviews, 1) AS avg_positive_reviews,

    -- Host segment classification
    CASE
        WHEN avg_occupancy >= 70 AND avg_positive_reviews >= 80 AND avg_monthly_revenue >= 5000
        THEN 'Premium Performer'

        WHEN avg_occupancy >= 60 AND avg_positive_reviews >= 70 AND avg_monthly_revenue >= 3000
        THEN 'High Performer'

        WHEN avg_occupancy >= 50 AND avg_listings > 3
        THEN 'Professional Multi-listing'

        WHEN avg_occupancy >= 50 AND avg_listings <= 3 AND avg_monthly_revenue >= 2000
        THEN 'Efficient Single-listing'

        WHEN avg_occupancy < 50 AND avg_monthly_revenue >= 3000
        THEN 'High-value Low-occupancy'

        WHEN avg_occupancy < 40 AND avg_positive_reviews < 70
        THEN 'Needs Improvement'

        ELSE 'Average Performer'
    END AS host_segment
FROM host_segments
ORDER BY
    CASE
        WHEN avg_occupancy >= 70 AND avg_positive_reviews >= 80 AND avg_monthly_revenue >= 5000
        THEN 1
        WHEN avg_occupancy >= 60 AND avg_positive_reviews >= 70 AND avg_monthly_revenue >= 3000
        THEN 2
        WHEN avg_occupancy >= 50 AND avg_listings > 3
        THEN 3
        WHEN avg_occupancy >= 50 AND avg_listings <= 3 AND avg_monthly_revenue >= 2000
        THEN 4
        WHEN avg_occupancy < 50 AND avg_monthly_revenue >= 3000
        THEN 5
        WHEN avg_occupancy < 40 AND avg_positive_reviews < 70
        THEN 7
        ELSE 6
    END,
    total_revenue DESC
LIMIT 100
