-- Identify listings with optimization opportunities
SELECT
    listing_id,
    listing_name,
    property_type,
    room_type,
    neighbourhood_cleansed,
    avg_length_of_stay,
    avg_booking_value,
    cancellation_rate,
    most_valuable_stay_duration,
    CASE
        WHEN cancellation_rate > 15 AND total_bookings > 10 THEN 'High cancellation rate'
        WHEN long_stay_revenue_percentage > 75 AND avg_length_of_stay < 5 THEN 'Extend min nights requirement'
        WHEN one_night_booking_percentage > 75 AND avg_booking_value < (SELECT AVG(avg_booking_value) FROM {{ ref('fct_booking_economics') }}) THEN 'Increase min nights'
        WHEN avg_length_of_stay > 5 AND long_stay_revenue_percentage < 30 THEN 'Optimize for shorter stays'
        ELSE 'No major optimization needed'
    END AS optimization_opportunity
FROM {{ ref('fct_booking_economics') }}
WHERE total_bookings > 5
ORDER BY
    CASE
        WHEN cancellation_rate > 15 AND total_bookings > 10 THEN 4
        WHEN long_stay_revenue_percentage > 75 AND avg_length_of_stay < 5 THEN 3
        WHEN one_night_booking_percentage > 75 AND avg_booking_value < (SELECT AVG(avg_booking_value) FROM {{ ref('fct_booking_economics') }}) THEN 2
        WHEN avg_length_of_stay > 5 AND long_stay_revenue_percentage < 30 THEN 1
        ELSE 0
    END DESC
