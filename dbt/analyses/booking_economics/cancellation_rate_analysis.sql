-- Cancellation rate by lead time category
SELECT
    CASE
        WHEN avg_booking_lead_time < 7 THEN '1. Under 1 week'
        WHEN avg_booking_lead_time < 14 THEN '2. 1-2 weeks'
        WHEN avg_booking_lead_time < 30 THEN '3. 2-4 weeks'
        WHEN avg_booking_lead_time < 60 THEN '4. 1-2 months'
        ELSE '5. 2+ months'
    END AS lead_time_category,

    ROUND(AVG(cancellation_rate), 2) AS avg_cancellation_rate,
    SUM(cancelled_bookings) AS total_cancelled_bookings,
    SUM(total_bookings_with_cancellation_data) AS total_bookings,
    ROUND(SUM(cancelled_bookings) * 100.0 / NULLIF(SUM(total_bookings_with_cancellation_data), 0), 2) AS overall_cancellation_rate
FROM {{ ref('fct_booking_economics') }}
GROUP BY 1
ORDER BY 1
