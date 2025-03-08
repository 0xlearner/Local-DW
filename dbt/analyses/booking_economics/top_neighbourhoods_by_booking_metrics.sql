-- Top neighborhoods by booking metrics
SELECT
    neighbourhood_cleansed,
    COUNT(DISTINCT listing_id) AS num_listings,
    ROUND(AVG(avg_length_of_stay), 2) AS avg_length_of_stay,
    ROUND(AVG(avg_booking_lead_time), 2) AS avg_lead_time,
    ROUND(AVG(avg_booking_value), 2) AS avg_booking_value,
    ROUND(AVG(cancellation_rate), 2) AS avg_cancellation_rate,
    SUM(total_bookings) AS total_bookings,
    ROUND(SUM(total_booking_value), 2) AS total_revenue
FROM {{ ref('fct_booking_economics') }}
GROUP BY neighbourhood_cleansed
ORDER BY total_revenue DESC
LIMIT 15
