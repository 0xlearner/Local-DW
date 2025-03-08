{{
    config(
        materialized = 'table',
        schema = 'gold_analytics'
    )
}}

WITH listings AS (
    SELECT * FROM {{ ref('dim_listings_scd_type1') }}
),

calendar AS (
    SELECT * FROM {{ ref('fct_calendar_daily') }}
),

-- First, identify booking sequences
-- A booking sequence is a continuous stretch of unavailable days for a listing
booking_sequences AS (
    SELECT
        listing_id,
        calendar_date,
        price,
        -- Mark the start of a new sequence when the current day is unavailable
        -- and either the previous day was available or it's the first record for this listing
        CASE
            WHEN NOT is_available AND (
                LAG(is_available) OVER (
                    PARTITION BY listing_id
                    ORDER BY calendar_date
                ) = TRUE
                OR LAG(is_available) OVER (
                    PARTITION BY listing_id
                    ORDER BY calendar_date
                ) IS NULL
            )
            THEN 1
            ELSE 0
        END AS new_sequence_flag
    FROM calendar
    WHERE NOT is_available  -- Only consider unavailable days (assumed to be booked)
),

-- Assign a unique sequence ID to each booking
booking_with_sequence_id AS (
    SELECT
        listing_id,
        calendar_date,
        price,
        SUM(new_sequence_flag) OVER (
            PARTITION BY listing_id
            ORDER BY calendar_date
            ROWS UNBOUNDED PRECEDING
        ) AS booking_sequence_id
    FROM booking_sequences
),

-- Aggregate booking details
bookings AS (
    SELECT
        listing_id,
        booking_sequence_id,
        MIN(calendar_date) AS check_in_date,
        MAX(calendar_date) AS check_out_date,
        COUNT(*) AS nights,
        AVG(price) AS avg_nightly_price,
        SUM(price) AS total_booking_value
    FROM booking_with_sequence_id
    GROUP BY listing_id, booking_sequence_id
    HAVING COUNT(*) > 0  -- Ensure we have actual bookings
),

-- Calculate booking metrics by listing
listing_booking_metrics AS (
    SELECT
        listing_id,
        -- Average Length of Stay (ALOS)
        AVG(nights) AS alos,
        -- Range of stay lengths
        MIN(nights) AS min_nights,
        MAX(nights) AS max_nights,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY nights) AS median_nights,
        -- Booking Value metrics
        AVG(total_booking_value) AS avg_booking_value,
        MAX(total_booking_value) AS max_booking_value,
        MIN(total_booking_value) AS min_booking_value,
        SUM(total_booking_value) AS total_booking_value,
        -- Booking counts
        COUNT(*) AS total_bookings,
        -- Booking distribution by length
        SUM(CASE WHEN nights = 1 THEN 1 ELSE 0 END) AS one_night_bookings,
        SUM(CASE WHEN nights = 2 THEN 1 ELSE 0 END) AS two_night_bookings,
        SUM(CASE WHEN nights BETWEEN 3 AND 6 THEN 1 ELSE 0 END) AS three_to_six_night_bookings,
        SUM(CASE WHEN nights >= 7 THEN 1 ELSE 0 END) AS seven_plus_night_bookings,
        -- Revenue distribution by booking length
        SUM(CASE WHEN nights = 1 THEN total_booking_value ELSE 0 END) AS one_night_revenue,
        SUM(CASE WHEN nights = 2 THEN total_booking_value ELSE 0 END) AS two_night_revenue,
        SUM(CASE WHEN nights BETWEEN 3 AND 6 THEN total_booking_value ELSE 0 END) AS three_to_six_night_revenue,
        SUM(CASE WHEN nights >= 7 THEN total_booking_value ELSE 0 END) AS seven_plus_night_revenue
    FROM bookings
    GROUP BY listing_id
),

-- Calculate booking patterns by month
monthly_booking_patterns AS (
    SELECT
        b.listing_id,
        EXTRACT(YEAR FROM b.check_in_date) AS check_in_year,
        EXTRACT(MONTH FROM b.check_in_date) AS check_in_month,
        TO_CHAR(DATE_TRUNC('month', b.check_in_date), 'YYYY-MM-01')::DATE AS month_start_date,
        COUNT(*) AS monthly_bookings,
        AVG(b.nights) AS monthly_alos,
        AVG(b.total_booking_value) AS monthly_avg_booking_value,
        SUM(b.total_booking_value) AS monthly_total_booking_value
    FROM bookings b
    GROUP BY
        b.listing_id,
        EXTRACT(YEAR FROM b.check_in_date),
        EXTRACT(MONTH FROM b.check_in_date),
        TO_CHAR(DATE_TRUNC('month', b.check_in_date), 'YYYY-MM-01')::DATE
),

-- Estimate booking lead time (since we don't have actual booking dates)
-- For this we'll use a random distribution between 1-90 days as a placeholder
-- In a real implementation, you would use actual booking timestamps
lead_time_estimate AS (
    SELECT
        b.listing_id,
        b.booking_sequence_id,
        b.check_in_date,
        -- This is a placeholder. In reality, you would use:
        -- EXTRACT(DAY FROM (b.check_in_date - actual_booking_date)) AS lead_time_days
        -- For simulation, we'll use a random number between 1 and 90
        (RANDOM() * 89 + 1)::INTEGER AS lead_time_days
    FROM bookings b
),

lead_time_by_listing AS (
    SELECT
        listing_id,
        AVG(lead_time_days) AS avg_lead_time_days,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY lead_time_days) AS median_lead_time_days,
        MIN(lead_time_days) AS min_lead_time_days,
        MAX(lead_time_days) AS max_lead_time_days,
        -- Distribution of lead times
        SUM(CASE WHEN lead_time_days <= 7 THEN 1 ELSE 0 END) AS last_minute_bookings,
        SUM(CASE WHEN lead_time_days BETWEEN 8 AND 30 THEN 1 ELSE 0 END) AS short_lead_bookings,
        SUM(CASE WHEN lead_time_days BETWEEN 31 AND 90 THEN 1 ELSE 0 END) AS medium_lead_bookings,
        SUM(CASE WHEN lead_time_days > 90 THEN 1 ELSE 0 END) AS long_lead_bookings,
        COUNT(*) AS total_bookings_with_lead_time
    FROM lead_time_estimate
    GROUP BY listing_id
),

-- Simulate cancellation data (since we don't have actual cancellation data)
-- This is a placeholder. In reality, you would join to an actual cancellations table.
cancellation_simulation AS (
    SELECT
        b.listing_id,
        b.booking_sequence_id,
        -- Simulate a 5% random cancellation rate
        CASE WHEN RANDOM() < 0.05 THEN TRUE ELSE FALSE END AS is_cancelled
    FROM bookings b
),

cancellation_by_listing AS (
    SELECT
        listing_id,
        SUM(CASE WHEN is_cancelled THEN 1 ELSE 0 END) AS cancelled_bookings,
        COUNT(*) AS total_bookings_with_cancellation_data,
        CASE
            WHEN COUNT(*) > 0
            THEN (SUM(CASE WHEN is_cancelled THEN 1 ELSE 0 END)::DECIMAL / COUNT(*)) * 100
            ELSE 0
        END AS cancellation_rate
    FROM cancellation_simulation
    GROUP BY listing_id
)

-- Combine all metrics into final table
SELECT
    -- Listing details
    l.listing_id,
    l.listing_name,
    l.host_id,
    l.host_name,
    l.property_type,
    l.room_type,
    l.accommodates,
    l.neighbourhood_cleansed,

    -- Average Length of Stay (ALOS)
    COALESCE(lbm.alos, 0) AS avg_length_of_stay,
    COALESCE(lbm.median_nights, 0) AS median_length_of_stay,
    COALESCE(lbm.min_nights, 0) AS min_length_of_stay,
    COALESCE(lbm.max_nights, 0) AS max_length_of_stay,

    -- Booking lead time
    COALESCE(ltl.avg_lead_time_days, 0) AS avg_booking_lead_time,
    COALESCE(ltl.median_lead_time_days, 0) AS median_booking_lead_time,
    COALESCE(ltl.last_minute_bookings, 0) AS last_minute_bookings,
    COALESCE(ltl.short_lead_bookings, 0) AS short_lead_bookings,
    COALESCE(ltl.medium_lead_bookings, 0) AS medium_lead_bookings,
    COALESCE(ltl.long_lead_bookings, 0) AS long_lead_bookings,

    -- Booking value
    COALESCE(lbm.avg_booking_value, 0) AS avg_booking_value,
    COALESCE(lbm.min_booking_value, 0) AS min_booking_value,
    COALESCE(lbm.max_booking_value, 0) AS max_booking_value,

    -- Cancellation rate
    COALESCE(cl.cancellation_rate, 0) AS cancellation_rate,
    COALESCE(cl.cancelled_bookings, 0) AS cancelled_bookings,
    COALESCE(cl.total_bookings_with_cancellation_data, 0) AS total_bookings_with_cancellation_data,

    -- Total booking metrics
    COALESCE(lbm.total_bookings, 0) AS total_bookings,
    COALESCE(lbm.total_booking_value, 0) AS total_booking_value,

    -- Revenue distribution metrics
    COALESCE(lbm.one_night_bookings, 0) AS one_night_bookings,
    COALESCE(lbm.two_night_bookings, 0) AS two_night_bookings,
    COALESCE(lbm.three_to_six_night_bookings, 0) AS three_to_six_night_bookings,
    COALESCE(lbm.seven_plus_night_bookings, 0) AS seven_plus_night_bookings,

    COALESCE(lbm.one_night_revenue, 0) AS one_night_revenue,
    COALESCE(lbm.two_night_revenue, 0) AS two_night_revenue,
    COALESCE(lbm.three_to_six_night_revenue, 0) AS three_to_six_night_revenue,
    COALESCE(lbm.seven_plus_night_revenue, 0) AS seven_plus_night_revenue,

    -- Calculated fields
    CASE
        WHEN COALESCE(lbm.total_bookings, 0) > 0
        THEN COALESCE(lbm.one_night_bookings, 0)::DECIMAL / COALESCE(lbm.total_bookings, 1) * 100
        ELSE 0
    END AS one_night_booking_percentage,

    CASE
        WHEN COALESCE(lbm.total_booking_value, 0) > 0
        THEN COALESCE(lbm.seven_plus_night_revenue, 0)::DECIMAL / COALESCE(lbm.total_booking_value, 1) * 100
        ELSE 0
    END AS long_stay_revenue_percentage,

    -- Most valuable booking length category
    CASE
        GREATEST(
            COALESCE(lbm.one_night_revenue, 0),
            COALESCE(lbm.two_night_revenue, 0),
            COALESCE(lbm.three_to_six_night_revenue, 0),
            COALESCE(lbm.seven_plus_night_revenue, 0)
        )
        WHEN COALESCE(lbm.one_night_revenue, 0) THEN '1 Night'
        WHEN COALESCE(lbm.two_night_revenue, 0) THEN '2 Nights'
        WHEN COALESCE(lbm.three_to_six_night_revenue, 0) THEN '3-6 Nights'
        WHEN COALESCE(lbm.seven_plus_night_revenue, 0) THEN '7+ Nights'
        ELSE 'No Revenue'
    END AS most_valuable_stay_duration,

    -- Metadata
    {{ current_timestamp_utc() }} AS dbt_loaded_at
FROM listings l
LEFT JOIN listing_booking_metrics lbm ON l.listing_id = lbm.listing_id
LEFT JOIN lead_time_by_listing ltl ON l.listing_id = ltl.listing_id
LEFT JOIN cancellation_by_listing cl ON l.listing_id = cl.listing_id
