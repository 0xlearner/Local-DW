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
    WHERE calendar_date <= CURRENT_DATE -- Focus on historical data
),

-- Calculate booked status
calendar_with_booking_status AS (
    SELECT
        *,
        -- A day is considered booked if it's not available
        NOT is_available AS is_booked
    FROM calendar
),

-- Daily metrics by listing
daily_metrics AS (
    SELECT
        listing_id,
        calendar_date,
        is_booked,
        is_weekend,
        price,
        -- We consider revenue as the price when the listing is booked
        CASE WHEN is_booked THEN price ELSE 0 END AS daily_revenue,

        -- Create date components for aggregation
        EXTRACT(YEAR FROM calendar_date) AS year,
        EXTRACT(MONTH FROM calendar_date) AS month,
        EXTRACT(QUARTER FROM calendar_date) AS quarter,
        EXTRACT(DOW FROM calendar_date) AS day_of_week,

        -- Create date buckets for different aggregation levels
        DATE_TRUNC('month', calendar_date)::DATE AS month_start_date,
        DATE_TRUNC('quarter', calendar_date)::DATE AS quarter_start_date
    FROM calendar_with_booking_status
),

-- 1. Listing-Month Level Metrics
listing_month_metrics AS (
    SELECT
        listing_id,
        year,
        month,
        month_start_date,

        -- Counts
        COUNT(*) AS total_days,
        SUM(CASE WHEN is_booked THEN 1 ELSE 0 END) AS booked_days,
        SUM(CASE WHEN NOT is_booked THEN 1 ELSE 0 END) AS available_days,
        SUM(CASE WHEN is_weekend AND is_booked THEN 1 ELSE 0 END) AS booked_weekend_days,
        SUM(CASE WHEN NOT is_weekend AND is_booked THEN 1 ELSE 0 END) AS booked_weekday_days,

        -- Revenue
        SUM(daily_revenue) AS total_revenue,

        -- Average Daily Rate (ADR) - average price per booked night
        CASE
            WHEN SUM(CASE WHEN is_booked THEN 1 ELSE 0 END) > 0
            THEN SUM(CASE WHEN is_booked THEN price ELSE 0 END) / SUM(CASE WHEN is_booked THEN 1 ELSE 0 END)
            ELSE 0
        END AS adr,

        -- Revenue Per Available Day (RevPAD)
        SUM(daily_revenue) / COUNT(*) AS revpad,

        -- Weekend vs. Weekday ADR
        CASE
            WHEN SUM(CASE WHEN is_weekend AND is_booked THEN 1 ELSE 0 END) > 0
            THEN SUM(CASE WHEN is_weekend AND is_booked THEN price ELSE 0 END) /
                 SUM(CASE WHEN is_weekend AND is_booked THEN 1 ELSE 0 END)
            ELSE 0
        END AS weekend_adr,

        CASE
            WHEN SUM(CASE WHEN NOT is_weekend AND is_booked THEN 1 ELSE 0 END) > 0
            THEN SUM(CASE WHEN NOT is_weekend AND is_booked THEN price ELSE 0 END) /
                 SUM(CASE WHEN NOT is_weekend AND is_booked THEN 1 ELSE 0 END)
            ELSE 0
        END AS weekday_adr,

        -- Occupancy rate
        CASE
            WHEN COUNT(*) > 0
            THEN (SUM(CASE WHEN is_booked THEN 1 ELSE 0 END)::DECIMAL / COUNT(*)) * 100
            ELSE 0
        END AS occupancy_rate
    FROM daily_metrics
    GROUP BY listing_id, year, month, month_start_date
),

-- 2. Price elasticity calculation: Need to join with historical price changes
price_change_analysis AS (
    SELECT
        lm1.listing_id,
        lm1.month_start_date,
        lm1.adr AS current_adr,
        lm1.occupancy_rate AS current_occupancy,
        LAG(lm1.adr) OVER (PARTITION BY lm1.listing_id ORDER BY lm1.month_start_date) AS previous_adr,
        LAG(lm1.occupancy_rate) OVER (PARTITION BY lm1.listing_id ORDER BY lm1.month_start_date) AS previous_occupancy,

        -- Calculate price change percentage
        CASE
            WHEN LAG(lm1.adr) OVER (PARTITION BY lm1.listing_id ORDER BY lm1.month_start_date) > 0
            THEN ((lm1.adr / LAG(lm1.adr) OVER (PARTITION BY lm1.listing_id ORDER BY lm1.month_start_date)) - 1) * 100
            ELSE NULL
        END AS price_change_pct,

        -- Calculate occupancy change percentage
        CASE
            WHEN LAG(lm1.occupancy_rate) OVER (PARTITION BY lm1.listing_id ORDER BY lm1.month_start_date) > 0
            THEN ((lm1.occupancy_rate / LAG(lm1.occupancy_rate) OVER (PARTITION BY lm1.listing_id ORDER BY lm1.month_start_date)) - 1) * 100
            ELSE NULL
        END AS occupancy_change_pct
    FROM listing_month_metrics lm1
),

-- Calculate price elasticity
elasticity_calc AS (
    SELECT
        listing_id,
        month_start_date,
        current_adr,
        current_occupancy,
        previous_adr,
        previous_occupancy,
        price_change_pct,
        occupancy_change_pct,

        -- Price elasticity calculation (change in demand / change in price)
        -- Elasticity > 0 means price elastic (price increase leads to bookings decrease)
        -- Elasticity = 0 means price inelastic (price doesn't affect bookings)
        -- We use a NULLIF to avoid division by zero
        CASE
            WHEN price_change_pct != 0
            THEN (occupancy_change_pct / NULLIF(price_change_pct, 0)) * -1
            ELSE 0
        END AS price_elasticity
    FROM price_change_analysis
    WHERE price_change_pct IS NOT NULL AND occupancy_change_pct IS NOT NULL
),

-- 3. Combine with listing details
final_metrics AS (
    SELECT
        -- Listing identifiers
        l.listing_id,
        l.listing_name,
        l.host_id,
        l.host_name,
        l.neighbourhood_cleansed,
        l.property_type,
        l.room_type,

        -- Time dimensions
        lm.month_start_date,
        lm.year,
        lm.month,

        -- Pricing metrics
        lm.adr,
        lm.revpad,
        lm.total_revenue,

        -- Weekend vs Weekday analysis
        lm.weekend_adr,
        lm.weekday_adr,
        CASE
            WHEN lm.weekday_adr > 0
            THEN ((lm.weekend_adr / lm.weekday_adr) - 1) * 100
            ELSE 0
        END AS weekend_pricing_premium_pct,

        -- Occupancy metrics
        lm.booked_days,
        lm.available_days,
        lm.total_days,
        lm.occupancy_rate,

        -- Price elasticity - join to elasticity calculation
        e.price_change_pct,
        e.occupancy_change_pct,
        e.price_elasticity,

        -- Price elasticity interpretation
        CASE
            WHEN e.price_elasticity > 1.5 THEN 'Highly Elastic'
            WHEN e.price_elasticity BETWEEN 0.5 AND 1.5 THEN 'Elastic'
            WHEN e.price_elasticity BETWEEN -0.5 AND 0.5 THEN 'Inelastic'
            WHEN e.price_elasticity < -0.5 THEN 'Abnormal Response'
            ELSE 'Insufficient Data'
        END AS elasticity_category,

        -- Pricing recommendation based on elasticity
        CASE
            WHEN e.price_elasticity > 1.0 THEN 'Consider lowering price to increase occupancy'
            WHEN e.price_elasticity BETWEEN 0.5 AND 1.0 THEN 'Price is sensitive - optimize carefully'
            WHEN e.price_elasticity BETWEEN -0.5 AND 0.5 THEN 'Price is inelastic - may increase price'
            WHEN e.price_elasticity < -0.5 THEN 'Abnormal data - investigate other factors'
            ELSE 'Need more data'
        END AS pricing_recommendation,

        {{ current_timestamp_utc() }} AS dbt_loaded_at
    FROM listing_month_metrics lm
    LEFT JOIN elasticity_calc e ON
        lm.listing_id = e.listing_id AND
        lm.month_start_date = e.month_start_date
    JOIN listings l ON lm.listing_id = l.listing_id
)

-- 4. Add seasonal analysis based on month-over-month changes
SELECT
    fm.*,

    -- Seasonal analysis
    AVG(fm.adr) OVER (PARTITION BY fm.listing_id ORDER BY fm.month_start_date
                      ROWS BETWEEN 11 PRECEDING AND CURRENT ROW) AS trailing_12m_avg_adr,

    -- Seasonal index (current ADR vs trailing 12-month average)
    CASE
        WHEN AVG(fm.adr) OVER (PARTITION BY fm.listing_id ORDER BY fm.month_start_date
                              ROWS BETWEEN 11 PRECEDING AND CURRENT ROW) > 0
        THEN fm.adr / NULLIF(AVG(fm.adr) OVER (PARTITION BY fm.listing_id ORDER BY fm.month_start_date
                                             ROWS BETWEEN 11 PRECEDING AND CURRENT ROW), 0)
        ELSE NULL
    END AS seasonal_index,

    -- Determine if this is high or low season
    CASE
        WHEN fm.adr > (1.1 * AVG(fm.adr) OVER (PARTITION BY fm.listing_id ORDER BY fm.month_start_date
                                            ROWS BETWEEN 11 PRECEDING AND CURRENT ROW))
        THEN 'High Season'
        WHEN fm.adr < (0.9 * AVG(fm.adr) OVER (PARTITION BY fm.listing_id ORDER BY fm.month_start_date
                                            ROWS BETWEEN 11 PRECEDING AND CURRENT ROW))
        THEN 'Low Season'
        ELSE 'Regular Season'
    END AS seasonality
FROM final_metrics fm
ORDER BY listing_id, month_start_date
