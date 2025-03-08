-- Cancellation impact on revenue by property type
SELECT
    property_type,
    ROUND(AVG(cancellation_rate), 2) AS avg_cancellation_rate,
    ROUND(SUM(cancelled_bookings * avg_booking_value), 2) AS estimated_lost_revenue,
    SUM(total_booking_value) AS actual_revenue,
    ROUND((SUM(cancelled_bookings * avg_booking_value) / NULLIF(SUM(total_booking_value), 0)) * 100, 2) AS pct_revenue_impact
FROM {{ ref('fct_booking_economics') }}
GROUP BY property_type
ORDER BY estimated_lost_revenue DESC
