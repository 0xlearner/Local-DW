SELECT
    property_type,
    AVG(weekend_pricing_premium_pct) AS avg_weekend_premium,
    COUNT(DISTINCT listing_id) AS listing_count
FROM {{ ref('fct_financial_performance_metrics') }}
GROUP BY property_type
HAVING COUNT(DISTINCT listing_id) > 5
ORDER BY avg_weekend_premium DESC
