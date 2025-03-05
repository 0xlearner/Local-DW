{{
    config(
        materialized = 'incremental',
        unique_key = 'reviewer_id'
    )
}}

WITH stg_reviews AS (
    SELECT * FROM {{ ref('stg_reviews') }}
),

reviewer_stats AS (
    SELECT
        reviewer_id,
        reviewer_name,
        MIN(review_date) AS first_review_date,
        MAX(review_date) AS last_review_date,
        COUNT(*) AS total_reviews,
        COUNT(DISTINCT listing_id) AS unique_listings_reviewed
    FROM stg_reviews
    GROUP BY reviewer_id, reviewer_name
)

SELECT
    reviewer_id,
    reviewer_name,
    first_review_date,
    last_review_date,
    total_reviews,
    unique_listings_reviewed,

    -- Derived fields
    CASE
        WHEN total_reviews >= 10 THEN 'frequent'
        WHEN total_reviews >= 3 THEN 'regular'
        ELSE 'new'
    END AS reviewer_tier,

    {{ current_timestamp_utc() }} AS dbt_loaded_at
FROM reviewer_stats

{% if is_incremental() %}
WHERE
    reviewer_id NOT IN (SELECT reviewer_id FROM {{ this }})
    OR (
        reviewer_id IN (SELECT reviewer_id FROM {{ this }})
        AND (last_review_date > (SELECT MAX(last_review_date) FROM {{ this }} WHERE reviewer_id = reviewer_stats.reviewer_id)
            OR total_reviews != (SELECT total_reviews FROM {{ this }} WHERE reviewer_id = reviewer_stats.reviewer_id))
    )
{% endif %}
