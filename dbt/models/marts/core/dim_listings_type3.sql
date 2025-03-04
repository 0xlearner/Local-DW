{{
    config(
        materialized = 'table',
        schema = 'gold_core'
    )
}}

/*
  This is a Type 3 SCD implementation that maintains current and previous values
  for selected important attributes that change over time.

  We're specifically tracking:
  - Price changes
  - Superhost status changes
  - Room type changes
  - Minimum nights requirement changes
*/

WITH current_listings AS (
    SELECT *
    FROM {{ ref('dim_listings_scd') }}
    WHERE is_current = TRUE
),

-- Get the most recent previous version of each listing
previous_versions AS (
    SELECT
        listing_id,
        price,
        host_is_superhost,
        room_type,
        minimum_nights,
        valid_from,
        valid_to,
        ROW_NUMBER() OVER (PARTITION BY listing_id ORDER BY valid_from DESC) AS version_rank
    FROM {{ ref('dim_listings_scd') }}
    WHERE NOT is_current
),

previous_listings AS (
    SELECT
        listing_id,
        price AS previous_price,
        host_is_superhost AS previous_superhost_status,
        room_type AS previous_room_type,
        minimum_nights AS previous_minimum_nights,
        valid_from AS previous_values_from,
        valid_to AS previous_values_to
    FROM previous_versions
    WHERE version_rank = 1
),

-- Find the penultimate version for "original" values where available
original_versions AS (
    SELECT
        listing_id,
        price,
        host_is_superhost,
        room_type,
        minimum_nights,
        valid_from,
        ROW_NUMBER() OVER (PARTITION BY listing_id ORDER BY valid_from ASC) AS version_rank
    FROM {{ ref('dim_listings_scd') }}
),

original_listings AS (
    SELECT
        listing_id,
        price AS original_price,
        host_is_superhost AS original_superhost_status,
        room_type AS original_room_type,
        minimum_nights AS original_minimum_nights,
        valid_from AS original_values_from
    FROM original_versions
    WHERE version_rank = 1
)

SELECT
    -- Primary keys and standard attributes
    c.listing_id,
    c.host_id,
    c.listing_name,
    c.host_name,
    c.host_listings_count,
    c.neighbourhood_cleansed,
    c.latitude,
    c.longitude,
    c.property_type,

    -- Type 3 tracked fields with current, previous, and original values
    -- Price tracking
    c.price AS current_price,
    p.previous_price,
    o.original_price,
    CASE WHEN c.price != COALESCE(p.previous_price, c.price) THEN TRUE ELSE FALSE END AS price_changed,
    CASE
        WHEN p.previous_price IS NOT NULL AND c.price != p.previous_price
        THEN ROUND(((c.price - p.previous_price) / p.previous_price) * 100, 2)
        ELSE 0
    END AS price_percent_change,

    -- Superhost status tracking
    c.host_is_superhost AS current_superhost_status,
    p.previous_superhost_status,
    o.original_superhost_status,
    CASE WHEN c.host_is_superhost != COALESCE(p.previous_superhost_status, c.host_is_superhost) THEN TRUE ELSE FALSE END AS superhost_status_changed,

    -- Room type tracking
    c.room_type AS current_room_type,
    p.previous_room_type,
    o.original_room_type,
    CASE WHEN c.room_type != COALESCE(p.previous_room_type, c.room_type) THEN TRUE ELSE FALSE END AS room_type_changed,

    -- Minimum nights tracking
    c.minimum_nights AS current_minimum_nights,
    p.previous_minimum_nights,
    o.original_minimum_nights,
    CASE WHEN c.minimum_nights != COALESCE(p.previous_minimum_nights, c.minimum_nights) THEN TRUE ELSE FALSE END AS minimum_nights_changed,

    -- Other standard attributes
    c.bedrooms,
    c.beds,
    c.bathrooms_text,
    c.amenities,
    c.maximum_nights,
    c.license,
    c.instant_bookable,

    -- Change timestamps
    c.valid_from AS current_values_from,
    p.previous_values_from,
    p.previous_values_to,
    o.original_values_from,

    -- Metadata fields
    c.last_scraped,
    c._ingested_at,
    c.record_hash,
    {{ current_timestamp_utc() }} AS dbt_updated_at
FROM current_listings c
LEFT JOIN previous_listings p ON c.listing_id = p.listing_id
LEFT JOIN original_listings o ON c.listing_id = o.listing_id
