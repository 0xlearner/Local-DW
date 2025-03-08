SELECT
    month_start_date,
    SUM(total_revenue) AS platform_revenue,
    AVG(adr) AS platform_avg_adr,
    AVG(occupancy_rate) AS platform_avg_occupancy
FROM {{ ref('fct_financial_performance_metrics') }}
GROUP BY month_start_date
ORDER BY month_start_date
