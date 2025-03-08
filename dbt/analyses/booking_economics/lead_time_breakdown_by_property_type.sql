-- Lead time breakdown by property type
SELECT
    property_type,
    SUM(last_minute_bookings) AS last_minute_bookings,
    SUM(short_lead_bookings) AS short_lead_bookings,
    SUM(medium_lead_bookings) AS medium_lead_bookings,
    SUM(long_lead_bookings) AS long_lead_bookings,
    ROUND(SUM(last_minute_bookings) * 100.0 / NULLIF(SUM(last_minute_bookings + short_lead_bookings + medium_lead_bookings + long_lead_bookings), 0), 2) AS pct_last_minute,
    ROUND(SUM(long_lead_bookings) * 100.0 / NULLIF(SUM(last_minute_bookings + short_lead_bookings + medium_lead_bookings + long_lead_bookings), 0), 2) AS pct_long_lead
FROM {{ ref('fct_booking_economics') }}
GROUP BY property_type
ORDER BY SUM(total_bookings) DESC
