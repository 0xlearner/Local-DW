{{
    config(
        materialized = 'incremental',
        unique_key = 'review_sk',
        incremental_strategy = 'merge',
        on_schema_change = 'append_new_columns'
    )
}}

WITH int_reviews AS (
    SELECT * FROM {{ ref('int_reviews_cleaned') }}
    WHERE data_quality_check = 'valid' -- Only include valid records
),

-- Extract time dimensions
reviews_with_time_dims AS (
    SELECT
        *,
        EXTRACT(YEAR FROM review_date) AS review_year,
        EXTRACT(MONTH FROM review_date) AS review_month,
        EXTRACT(DAY FROM review_date) AS review_day,
        TO_CHAR(review_date, 'Day') AS review_day_of_week
    FROM int_reviews
)

SELECT
    -- Surrogate key
    {{ dbt_utils.generate_surrogate_key(['review_id']) }} AS review_sk,

    -- Natural/business keys
    review_id,

    -- Foreign keys for dimensional modeling
    listing_id,
    reviewer_id,

    -- Event details
    reviewer_name,
    review_date,
    review_text,

    -- Time dimensions for analysis
    review_year,
    review_month,
    review_day,
    review_day_of_week,

    -- Metrics (already cleaned in intermediate)
    review_length,
    sentiment,

    -- Metadata
    _ingested_at,
    record_hash,
    {{ current_timestamp_utc() }} AS dbt_loaded_at
FROM reviews_with_time_dims

{% if is_incremental() %}
WHERE _ingested_at > (SELECT max(_ingested_at) FROM {{ this }})
{% endif %}
