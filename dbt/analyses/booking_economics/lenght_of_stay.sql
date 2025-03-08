-- Length of stay distribution
SELECT
    CASE
        WHEN avg_length_of_stay < 2 THEN '1. Under 2 nights'
        WHEN avg_length_of_stay < 3 THEN '2. 2-3 nights'
        WHEN avg_length_of_stay < 5 THEN '3. 3-5 nights'
        WHEN avg_length_of_stay < 7 THEN '4. 5-7 nights'
        ELSE '5. 7+ nights'
    END AS stay_length_category,

    COUNT(DISTINCT listing_id) AS num_listings,
    SUM(total_bookings) AS num_bookings,
    ROUND(SUM(total_booking_value), 2) AS total_revenue,
    ROUND(AVG(avg_booking_value), 2) AS avg_booking_value,
    ROUND(AVG(cancellation_rate), 2) AS avg_cancellation_rate
FROM {{ ref('fct_booking_economics') }}
GROUP BY 1
ORDER BY 1
