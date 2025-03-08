-- Most profitable stay duration by property type
SELECT
    property_type,
    most_valuable_stay_duration,
    COUNT(DISTINCT listing_id) AS num_listings,
    ROUND(AVG(avg_booking_value), 2) AS avg_booking_value,
    SUM(total_booking_value) AS total_revenue
FROM {{ ref('fct_booking_economics') }}
GROUP BY property_type, most_valuable_stay_duration
ORDER BY property_type, total_revenue DESC
