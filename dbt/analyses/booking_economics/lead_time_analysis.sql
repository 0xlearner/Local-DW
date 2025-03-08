-- Lead time analysis by booking value
SELECT
    CASE
        WHEN avg_booking_lead_time < 7 THEN '1. Under 1 week'
        WHEN avg_booking_lead_time < 14 THEN '2. 1-2 weeks'
        WHEN avg_booking_lead_time < 30 THEN '3. 2-4 weeks'
        WHEN avg_booking_lead_time < 60 THEN '4. 1-2 months'
        ELSE '5. 2+ months'
    END AS lead_time_category,

    COUNT(DISTINCT listing_id) AS num_listings,
    ROUND(AVG(avg_booking_value), 2) AS avg_booking_value,
    ROUND(SUM(total_booking_value), 2) AS total_revenue,
    ROUND(AVG(avg_length_of_stay), 2) AS avg_length_of_stay,
    ROUND(AVG(cancellation_rate), 2) AS avg_cancellation_rate
FROM {{ ref('fct_booking_economics') }}
GROUP BY 1
ORDER BY 1
