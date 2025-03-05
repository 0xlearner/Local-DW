{{
    config(
        materialized = 'table',
        schema = 'gold_analytics'
    )
}}

WITH calendar_daily AS (
    SELECT * FROM {{ ref('fct_calendar_daily') }}
),

listing_months AS (
    SELECT DISTINCT
        listing_id,
        year_num,
        month_num
    FROM calendar_daily
),

monthly_stats AS (
    SELECT
        l.listing_id,
        l.year_num,
        l.month_num,

        -- Availability metrics
        COUNT(*) AS total_days,
        SUM(CASE WHEN cal.is_available THEN 1 ELSE 0 END) AS available_days,
        SUM(CASE WHEN NOT cal.is_available THEN 1 ELSE 0 END) AS booked_days,

        -- Pricing metrics
        AVG(cal.price) AS avg_daily_price,
        MAX(cal.price) AS max_daily_price,
        MIN(cal.price) AS min_daily_price,

        -- Weekend vs weekday metrics
        AVG(CASE WHEN cal.is_weekend THEN cal.price ELSE NULL END) AS avg_weekend_price,
        AVG(CASE WHEN NOT cal.is_weekend THEN cal.price ELSE NULL END) AS avg_weekday_price,

        -- Occupancy rate
        (SUM(CASE WHEN NOT cal.is_available THEN 1 ELSE 0 END)::DECIMAL / NULLIF(COUNT(*), 0)) * 100 AS occupancy_rate
    FROM listing_months l
    JOIN calendar_daily cal
      ON l.listing_id = cal.listing_id
      AND l.year_num = cal.year_num
      AND l.month_num = cal.month_num
    GROUP BY l.listing_id, l.year_num, l.month_num
)

SELECT
    listing_id,
    year_num,
    month_num,
    -- Fix: Cast to integer for MAKE_DATE function
    MAKE_DATE(year_num::integer, month_num::integer, 1) AS first_day_of_month,
    total_days,
    available_days,
    booked_days,
    avg_daily_price,
    max_daily_price,
    min_daily_price,
    avg_weekend_price,
    avg_weekday_price,
    occupancy_rate,
    {{ current_timestamp_utc() }} AS dbt_loaded_at
FROM monthly_stats
