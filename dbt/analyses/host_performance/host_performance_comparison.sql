-- Host performance compared to platform average
WITH platform_averages AS (
    SELECT
        month_date,
        AVG(occupancy_rate) AS avg_platform_occupancy,
        AVG(estimated_revenue / NULLIF(active_listings, 0)) AS avg_platform_revenue_per_listing,
        AVG(positive_review_percentage) AS avg_platform_positive_reviews
    FROM {{ ref('host_performance_monthly') }}
    GROUP BY month_date
)

SELECT
    h.host_id,
    h.host_name,
    h.month_date,
    h.active_listings,
    h.occupancy_rate,
    p.avg_platform_occupancy,
    ROUND((h.occupancy_rate - p.avg_platform_occupancy), 1) AS occupancy_vs_platform,

    (h.estimated_revenue / NULLIF(h.active_listings, 0)) AS revenue_per_listing,
    p.avg_platform_revenue_per_listing,
    ROUND(((h.estimated_revenue / NULLIF(h.active_listings, 0)) -
           p.avg_platform_revenue_per_listing) /
          NULLIF(p.avg_platform_revenue_per_listing, 0) * 100, 1) AS revenue_pct_vs_platform,

    h.positive_review_percentage,
    p.avg_platform_positive_reviews,
    ROUND((h.positive_review_percentage - p.avg_platform_positive_reviews), 1) AS reviews_vs_platform
FROM {{ ref('host_performance_monthly') }} h
JOIN platform_averages p ON h.month_date = p.month_date
WHERE h.host_id = 123456 -- Replace with specific host ID
ORDER BY h.month_date
