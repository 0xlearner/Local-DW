-- KPI metrics for dashboard cards
SELECT
    -- General metrics
    COUNT(DISTINCT listing_id) AS total_listings,
    SUM(total_bookings) AS total_bookings,
    ROUND(SUM(total_booking_value), 2) AS total_revenue,

    -- ALOS metrics
    ROUND(AVG(avg_length_of_stay), 2) AS avg_length_of_stay,
    ROUND(AVG(median_length_of_stay), 2) AS median_length_of_stay,
    MAX(max_length_of_stay) AS max_length_of_stay,

    -- Lead time metrics
    ROUND(AVG(avg_booking_lead_time), 2) AS avg_booking_lead_time,
    SUM(last_minute_bookings) AS last_minute_bookings,
    ROUND(SUM(last_minute_bookings) * 100.0 / NULLIF(SUM(total_bookings), 0), 2) AS pct_last_minute,

    -- Value metrics
    ROUND(AVG(avg_booking_value), 2) AS avg_booking_value,
    MAX(max_booking_value) AS max_booking_value,

    -- Cancellation metrics
    ROUND(AVG(cancellation_rate), 2) AS avg_cancellation_rate,
    SUM(cancelled_bookings) AS total_cancellations,

    -- Booking distribution
    SUM(one_night_bookings) AS one_night_bookings,
    SUM(two_night_bookings) AS two_night_bookings,
    SUM(three_to_six_night_bookings) AS three_to_six_night_bookings,
    SUM(seven_plus_night_bookings) AS seven_plus_night_bookings,

    -- Revenue distribution
    ROUND(SUM(one_night_revenue), 2) AS one_night_revenue,
    ROUND(SUM(two_night_revenue), 2) AS two_night_revenue,
    ROUND(SUM(three_to_six_night_revenue), 2) AS three_to_six_night_revenue,
    ROUND(SUM(seven_plus_night_revenue), 2) AS seven_plus_night_revenue
FROM {{ ref('fct_booking_economics') }}
