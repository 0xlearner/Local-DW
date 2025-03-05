{{
    config(
        materialized = 'table',
        schema = 'gold_analytics'
    )
}}

WITH reviews AS (
    SELECT * FROM {{ ref('fct_reviews') }}
),

listings AS (
    SELECT * FROM {{ ref('dim_listings_scd_type1') }}
),

review_metrics AS (
    SELECT
        listing_id,
        COUNT(*) AS review_count,
        AVG(review_length) AS avg_review_length,
        MIN(review_date) AS first_review_date,
        MAX(review_date) AS last_review_date,
        SUM(CASE WHEN sentiment = 'positive' THEN 1 ELSE 0 END) AS positive_reviews,
        SUM(CASE WHEN sentiment = 'negative' THEN 1 ELSE 0 END) AS negative_reviews,
        SUM(CASE WHEN sentiment = 'neutral' THEN 1 ELSE 0 END) AS neutral_reviews
    FROM reviews
    GROUP BY listing_id
)

SELECT
    l.listing_id,
    l.listing_name,
    l.host_id,
    l.host_name,
    l.neighbourhood_cleansed,
    l.property_type,
    l.room_type,
    l.price,

    r.review_count,
    r.avg_review_length,
    r.first_review_date,
    r.last_review_date,
    r.positive_reviews,
    r.negative_reviews,
    r.neutral_reviews,

    -- Derived metrics
    CASE WHEN r.review_count > 0
         THEN (r.positive_reviews::float / r.review_count) * 100
         ELSE 0
    END AS positive_review_percentage,

    CASE
        WHEN r.review_count = 0 THEN 'No Reviews'
        WHEN r.positive_reviews > r.negative_reviews * 3 THEN 'Highly Rated'
        WHEN r.positive_reviews > r.negative_reviews THEN 'Well Rated'
        WHEN r.negative_reviews > r.positive_reviews THEN 'Poorly Rated'
        ELSE 'Mixed Reviews'
    END AS review_rating_category,

    {{ current_timestamp_utc() }} AS dbt_loaded_at
FROM listings l
LEFT JOIN review_metrics r ON l.listing_id = r.listing_id
