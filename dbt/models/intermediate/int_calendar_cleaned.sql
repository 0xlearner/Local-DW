{{
    config(
        materialized = 'incremental',
        unique_key = ['listing_id', 'calendar_date']
    )
}}

WITH stg_calendar AS (
    SELECT * FROM {{ ref('stg_calendar') }}
)

SELECT
    listing_id,
    -- Explicitly cast to date type
    calendar_date::date AS calendar_date,

    -- Standardize availability flag
    CASE
        WHEN available = 't' THEN TRUE
        WHEN available = 'f' THEN FALSE
        ELSE NULL
    END AS is_available,

    -- Clean up price fields (remove $ and convert to numeric)
    CASE
        WHEN price IS NOT NULL AND price != ''
            THEN REPLACE(REPLACE(price, '$', ''), ',', '')::DECIMAL(10,2)
        ELSE NULL
    END AS price,

    CASE
        WHEN adjusted_price IS NOT NULL AND adjusted_price != ''
            THEN REPLACE(REPLACE(adjusted_price, '$', ''), ',', '')::DECIMAL(10,2)
        ELSE NULL
    END AS adjusted_price,

    minimum_nights,
    maximum_nights,

    -- Calculate day of week, month, etc. with explicit cast
    EXTRACT(DOW FROM calendar_date::date) AS day_of_week,
    EXTRACT(MONTH FROM calendar_date::date) AS month_num,
    EXTRACT(YEAR FROM calendar_date::date) AS year_num,

    -- Is this a weekend?
    CASE
        WHEN EXTRACT(DOW FROM calendar_date::date) IN (0, 6) THEN TRUE
        ELSE FALSE
    END AS is_weekend,

    _ingested_at,
    record_hash
FROM stg_calendar
{% if is_incremental() %}
WHERE _ingested_at > (SELECT max(_ingested_at) FROM {{ this }})
{% endif %}
