{{
    config(
        materialized = 'incremental',
        unique_key = 'review_id'
    )
}}

WITH stg_reviews AS (
    SELECT * FROM {{ ref('stg_reviews') }}
),

cleaned AS (
    SELECT
        -- Clean and convert IDs
        CASE
            WHEN review_id::text ~ '^\d+$' THEN review_id::bigint
            ELSE NULL
        END as review_id,

        CASE
            WHEN listing_id::text ~ '^\d+$' THEN listing_id::bigint
            ELSE NULL
        END as listing_id,

        CASE
            WHEN reviewer_id::text ~ '^\d+$' THEN reviewer_id::bigint
            ELSE NULL
        END as reviewer_id,

        -- Clean reviewer name
        TRIM(reviewer_name) as reviewer_name,

        -- Properly convert date
        CASE
            WHEN review_date ~ '^\d{4}-\d{2}-\d{2}$' THEN review_date::date
            WHEN review_date ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_DATE(review_date, 'MM/DD/YYYY')
            ELSE NULL
        END as review_date,

        -- Clean and prepare review text
        TRIM(review_text) as review_text,

        -- Add derived fields and metrics
        LENGTH(TRIM(review_text)) AS review_length,

        -- Calculate sentiment
        CASE
            WHEN review_text ILIKE '%excellent%' OR
                 review_text ILIKE '%great%' OR
                 review_text ILIKE '%fantastic%' OR
                 review_text ILIKE '%amazing%' OR
                 review_text ILIKE '%wonderful%' OR
                 review_text ILIKE '%perfect%' THEN 'positive'
            WHEN review_text ILIKE '%bad%' OR
                 review_text ILIKE '%terrible%' OR
                 review_text ILIKE '%poor%' OR
                 review_text ILIKE '%awful%' OR
                 review_text ILIKE '%horrible%' THEN 'negative'
            ELSE 'neutral'
        END AS sentiment,

        -- Metadata
        _ingested_at,
        record_hash,

        -- Data quality check
        CASE
            WHEN review_id IS NULL THEN 'missing_review_id'
            WHEN listing_id IS NULL THEN 'missing_listing_id'
            WHEN review_date IS NULL THEN 'invalid_date_format'
            WHEN TRIM(review_text) = '' THEN 'empty_review_text'
            ELSE 'valid'
        END AS data_quality_check
    FROM stg_reviews
)

SELECT * FROM cleaned
{% if is_incremental() %}
WHERE _ingested_at > (SELECT max(_ingested_at) FROM {{ this }})
{% endif %}
