-- Neighborhood performance by booking duration
SELECT
    neighbourhood_cleansed,
    ROUND(AVG(one_night_booking_percentage), 2) AS avg_one_night_pct,
    ROUND(AVG(long_stay_revenue_percentage), 2) AS avg_long_stay_revenue_pct,
    COUNT(DISTINCT CASE WHEN most_valuable_stay_duration = '1 Night' THEN listing_id END) AS one_night_optimal_listings,
    COUNT(DISTINCT CASE WHEN most_valuable_stay_duration = '2 Nights' THEN listing_id END) AS two_night_optimal_listings,
    COUNT(DISTINCT CASE WHEN most_valuable_stay_duration = '3-6 Nights' THEN listing_id END) AS medium_stay_optimal_listings,
    COUNT(DISTINCT CASE WHEN most_valuable_stay_duration = '7+ Nights' THEN listing_id END) AS long_stay_optimal_listings
FROM {{ ref('fct_booking_economics') }}
GROUP BY neighbourhood_cleansed
HAVING COUNT(DISTINCT listing_id) > 5
ORDER BY AVG(long_stay_revenue_percentage) DESC
