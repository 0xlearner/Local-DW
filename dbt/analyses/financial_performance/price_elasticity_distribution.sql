SELECT
    elasticity_category,
    COUNT(*) AS month_listing_count,
    COUNT(DISTINCT listing_id) AS unique_listing_count,
    AVG(price_elasticity) AS avg_elasticity,
    AVG(occupancy_rate) AS avg_occupancy,
    AVG(adr) AS avg_price
FROM {{ ref('fct_financial_performance_metrics') }}
WHERE elasticity_category != 'Insufficient Data'
GROUP BY elasticity_category
ORDER BY avg_elasticity DESC
