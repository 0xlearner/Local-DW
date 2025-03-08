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

reviews AS (
    SELECT * FROM {{ ref('fct_reviews') }}
),

-- Monthly booking metrics by host
host_monthly_metrics AS (
    SELECT
        l.host_id,
        l.host_name,
        DATE_TRUNC('month', c.calendar_date)::date AS month_date,

        -- Activity metrics
        COUNT(DISTINCT l.listing_id) AS active_listings,
        SUM(CASE WHEN NOT c.is_available THEN 1 ELSE 0 END) AS booked_days,
        SUM(CASE WHEN c.is_available THEN 1 ELSE 0 END) AS available_days,
        COUNT(*) AS total_days,

        -- Revenue metrics
        SUM(CASE WHEN NOT c.is_available THEN c.price ELSE 0 END) AS estimated_revenue,
        AVG(c.price) AS average_daily_rate,

        -- Quality metrics
        COUNT(DISTINCT r.review_id) AS review_count,
        SUM(CASE WHEN r.sentiment = 'positive' THEN 1 ELSE 0 END) AS positive_reviews,
        AVG(r.review_length) AS avg_review_length
    FROM listings l
    JOIN calendar c ON l.listing_id = c.listing_id
    LEFT JOIN reviews r ON
        l.listing_id = r.listing_id AND
        DATE_TRUNC('month', r.review_date) = DATE_TRUNC('month', c.calendar_date)
    GROUP BY l.host_id, l.host_name, DATE_TRUNC('month', c.calendar_date)::date
),

-- Calculate metrics including occupancy_rate
metrics_with_occupancy AS (
    SELECT
        host_id,
        host_name,
        month_date,
        active_listings,
        booked_days,
        available_days,
        total_days,

        -- Calculate occupancy rate
        ROUND((booked_days::decimal / NULLIF(total_days, 0)) * 100, 2) AS occupancy_rate,

        estimated_revenue,
        ROUND(estimated_revenue::decimal / NULLIF(active_listings, 0), 2) AS revenue_per_listing,
        ROUND(estimated_revenue::decimal / NULLIF(booked_days, 0), 2) AS revenue_per_booked_day,
        average_daily_rate,

        -- Quality metrics
        review_count,
        ROUND((positive_reviews::decimal / NULLIF(review_count, 0)) * 100, 2) AS positive_review_percentage,
        avg_review_length
    FROM host_monthly_metrics
)

-- Now we can safely use LAG on occupancy_rate and other calculated fields
SELECT
    host_id,
    host_name,
    month_date,
    active_listings,
    booked_days,
    available_days,
    total_days,
    occupancy_rate,
    estimated_revenue,
    revenue_per_listing,
    revenue_per_booked_day,
    average_daily_rate,

    -- Quality metrics
    review_count,
    positive_review_percentage,
    avg_review_length,

    -- Month-over-month metrics
    LAG(estimated_revenue) OVER (PARTITION BY host_id ORDER BY month_date) AS previous_month_revenue,
    LAG(occupancy_rate) OVER (PARTITION BY host_id ORDER BY month_date) AS previous_month_occupancy,

    -- Growth calculations
    CASE
        WHEN LAG(estimated_revenue) OVER (PARTITION BY host_id ORDER BY month_date) IS NOT NULL AND
             LAG(estimated_revenue) OVER (PARTITION BY host_id ORDER BY month_date) > 0
        THEN ((estimated_revenue / LAG(estimated_revenue) OVER (PARTITION BY host_id ORDER BY month_date)) - 1) * 100
        ELSE NULL
    END AS revenue_growth_pct,

    {{ current_timestamp_utc() }} AS dbt_loaded_at
FROM metrics_with_occupancy
ORDER BY host_id, month_date
