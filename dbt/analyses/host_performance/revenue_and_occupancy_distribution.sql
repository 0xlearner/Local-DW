-- Revenue and occupancy distribution
WITH host_averages AS (
    SELECT
        host_id,
        AVG(occupancy_rate) AS avg_occupancy,
        AVG(estimated_revenue) AS avg_monthly_revenue
    FROM {{ ref('host_performance_monthly') }}
    GROUP BY host_id
)

SELECT
    -- Occupancy buckets
    CASE
        WHEN avg_occupancy < 30 THEN '1. Below 30%'
        WHEN avg_occupancy < 50 THEN '2. 30-50%'
        WHEN avg_occupancy < 70 THEN '3. 50-70%'
        WHEN avg_occupancy < 90 THEN '4. 70-90%'
        ELSE '5. 90%+'
    END AS occupancy_bucket,

    -- Revenue buckets
    CASE
        WHEN avg_monthly_revenue < 1000 THEN 'A. Under $1K'
        WHEN avg_monthly_revenue < 3000 THEN 'B. $1K-$3K'
        WHEN avg_monthly_revenue < 5000 THEN 'C. $3K-$5K'
        WHEN avg_monthly_revenue < 10000 THEN 'D. $5K-$10K'
        ELSE 'E. $10K+'
    END AS revenue_bucket,

    COUNT(*) AS host_count,
    ROUND(AVG(avg_occupancy), 1) AS avg_occupancy_in_group,
    ROUND(AVG(avg_monthly_revenue), 2) AS avg_revenue_in_group
FROM host_averages
GROUP BY
    CASE
        WHEN avg_occupancy < 30 THEN '1. Below 30%'
        WHEN avg_occupancy < 50 THEN '2. 30-50%'
        WHEN avg_occupancy < 70 THEN '3. 50-70%'
        WHEN avg_occupancy < 90 THEN '4. 70-90%'
        ELSE '5. 90%+'
    END,
    CASE
        WHEN avg_monthly_revenue < 1000 THEN 'A. Under $1K'
        WHEN avg_monthly_revenue < 3000 THEN 'B. $1K-$3K'
        WHEN avg_monthly_revenue < 5000 THEN 'C. $3K-$5K'
        WHEN avg_monthly_revenue < 10000 THEN 'D. $5K-$10K'
        ELSE 'E. $10K+'
    END
ORDER BY occupancy_bucket, revenue_bucket
