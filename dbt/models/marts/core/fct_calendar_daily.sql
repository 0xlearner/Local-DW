{{
    config(
        materialized = 'incremental',
        unique_key = ['listing_id', 'calendar_date'],
        on_schema_change = 'append_new_columns'
    )
}}

WITH int_calendar AS (
    SELECT * FROM {{ ref('int_calendar_cleaned') }}
)

SELECT
    -- Natural composite key
    listing_id,
    calendar_date,

    -- Measures
    is_available,
    price,
    adjusted_price,
    minimum_nights,
    maximum_nights,

    -- Time attributes
    day_of_week,
    month_num,
    year_num,
    is_weekend,

    -- Derived metrics
    CASE
        WHEN LAG(price) OVER(PARTITION BY listing_id ORDER BY calendar_date) IS NOT NULL
        THEN price - LAG(price) OVER(PARTITION BY listing_id ORDER BY calendar_date)
        ELSE 0
    END AS price_change_from_previous_day,

    {{ current_timestamp_utc() }} AS dbt_loaded_at,
    _ingested_at,
    record_hash
FROM int_calendar
{% if is_incremental() %}
WHERE _ingested_at > (SELECT max(_ingested_at) FROM {{ this }})
{% endif %}
