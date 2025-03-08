-- High-level booking metrics summary
SELECT
    COUNT(DISTINCT listing_id) AS total_active_listings,
    SUM(total_bookings) AS total_bookings,
    SUM(total_booking_value) AS total_revenue,

    -- Average metrics
    ROUND(SUM(total_booking_value) / NULLIF(SUM(total_bookings), 0), 2) AS platform_avg_booking_value,
    ROUND(AVG(avg_length_of_stay), 2) AS platform_avg_length_of_stay,
    ROUND(AVG(avg_booking_lead_time), 2) AS platform_avg_lead_time,
    ROUND(AVG(cancellation_rate), 2) AS platform_avg_cancellation_rate
FROM {{ ref('fct_booking_economics') }}
