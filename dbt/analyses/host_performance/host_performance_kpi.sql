-- Dashboard KPI summary for host performance
WITH recent_metrics AS (
    SELECT
        COUNT(DISTINCT host_id) AS active_hosts,
        SUM(active_listings) AS total_listings,
        SUM(estimated_revenue) AS total_revenue,
        ROUND(AVG(occupancy_rate), 1) AS avg_occupancy,
        ROUND(AVG(revenue_per_listing), 2) AS avg_revenue_per_listing,
        ROUND(AVG(positive_review_percentage), 1) AS avg_positive_reviews,
        SUM(review_count) AS total_reviews
    FROM {{ ref('host_performance_monthly') }}
    WHERE month_date = (SELECT MAX(month_date) FROM {{ ref('host_performance_monthly') }})
),

previous_metrics AS (
    SELECT
        COUNT(DISTINCT host_id) AS active_hosts,
        SUM(active_listings) AS total_listings,
        SUM(estimated_revenue) AS total_revenue,
        ROUND(AVG(occupancy_rate), 1) AS avg_occupancy,
        ROUND(AVG(revenue_per_listing), 2) AS avg_revenue_per_listing,
        ROUND(AVG(positive_review_percentage), 1) AS avg_positive_reviews,
        SUM(review_count) AS total_reviews
    FROM {{ ref('host_performance_monthly') }}
    WHERE month_date = (
        SELECT MAX(month_date)
        FROM {{ ref('host_performance_monthly') }}
        WHERE month_date < (SELECT MAX(month_date) FROM {{ ref('host_performance_monthly') }})
    )
)

SELECT
    r.active_hosts,
    ROUND((r.active_hosts - p.active_hosts) / NULLIF(p.active_hosts, 0) * 100, 1) AS active_hosts_growth,

    r.total_listings,
    ROUND((r.total_listings - p.total_listings) / NULLIF(p.total_listings, 0) * 100, 1) AS listings_growth,

    r.total_revenue,
    ROUND((r.total_revenue - p.total_revenue) / NULLIF(p.total_revenue, 0) * 100, 1) AS revenue_growth,

    r.avg_occupancy,
    (r.avg_occupancy - p.avg_occupancy) AS occupancy_change,

    r.avg_revenue_per_listing,
    ROUND((r.avg_revenue_per_listing - p.avg_revenue_per_listing) / NULLIF(p.avg_revenue_per_listing, 0) * 100, 1) AS revenue_per_listing_growth,

    r.avg_positive_reviews,
    (r.avg_positive_reviews - p.avg_positive_reviews) AS positive_reviews_change,

    r.total_reviews,
    ROUND((r.total_reviews - p.total_reviews) / NULLIF(p.total_reviews, 0) * 100, 1) AS reviews_growth
FROM recent_metrics r, previous_metrics p
