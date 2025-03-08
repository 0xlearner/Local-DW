-- Host growth trajectory classification
WITH host_trajectory AS (
    SELECT
        host_id,
        host_name,
        COUNT(DISTINCT month_date) AS active_months,
        AVG(revenue_growth_pct) AS avg_growth,
        FIRST_VALUE(active_listings) OVER (
            PARTITION BY host_id
            ORDER BY month_date
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS initial_listings,
        LAST_VALUE(active_listings) OVER (
            PARTITION BY host_id
            ORDER BY month_date
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS latest_listings,
        SUM(estimated_revenue) AS total_revenue
    FROM {{ ref('host_performance_monthly') }}
    GROUP BY host_id, host_name
    HAVING COUNT(DISTINCT month_date) >= 3 -- At least 3 months of data
)

SELECT
    host_id,
    host_name,
    active_months,
    ROUND(avg_growth, 1) AS avg_monthly_growth_pct,
    initial_listings,
    latest_listings,
    (latest_listings - initial_listings) AS listing_growth,
    ROUND(total_revenue, 2) AS total_revenue,
    CASE
        WHEN avg_growth > 10 AND (latest_listings - initial_listings) > 0
        THEN 'High Growth'
        WHEN avg_growth > 5 OR (latest_listings - initial_listings) > 0
        THEN 'Growing'
        WHEN avg_growth BETWEEN -5 AND 5 AND (latest_listings - initial_listings) = 0
        THEN 'Stable'
        WHEN avg_growth < -5 OR (latest_listings - initial_listings) < 0
        THEN 'Declining'
        ELSE 'Inconsistent'
    END AS growth_trajectory
FROM host_trajectory
ORDER BY
    CASE
        WHEN avg_growth > 10 AND (latest_listings - initial_listings) > 0
        THEN 1
        WHEN avg_growth > 5 OR (latest_listings - initial_listings) > 0
        THEN 2
        WHEN avg_growth BETWEEN -5 AND 5 AND (latest_listings - initial_listings) = 0
        THEN 3
        WHEN avg_growth < -5 OR (latest_listings - initial_listings) < 0
        THEN 4
        ELSE 5
    END,
    total_revenue DESC
