{{
    config(
        materialized = 'table',
        schema = 'gold_core'
    )
}}

-- This is a Type 1 SCD that only contains the current version of each listing
-- It's simpler and more efficient for queries that only need current data

WITH current_listings AS (
    SELECT
        listing_id,
        host_id,
        listing_name,
        host_name,
        host_city,           -- Include new columns
        host_country,        -- Include new columns
        host_since,          -- Now a date type
        host_response_time_category,
        host_response_time_hours,
        host_response_rate_pct,
        host_acceptance_rate_pct,
        host_quality_score,
        host_is_superhost,
        host_listings_count,
        neighbourhood_cleansed,
        latitude,
        longitude,
        property_type,
        room_type,
        accommodates,
        bathrooms_text,
        bedrooms,
        beds,
        amenities,
        price,
        minimum_nights,
        maximum_nights,
        license,
        instant_bookable,
        calculated_host_listings_count,
        calculated_host_listings_count_entire_homes,
        calculated_host_listings_count_private_rooms,
        calculated_host_listings_count_shared_rooms,
        last_scraped,
        _ingested_at,
        record_hash,
        valid_from,
        dbt_loaded_at
    FROM {{ ref('dim_listings_scd') }}
    WHERE is_current = TRUE
)

SELECT
    -- Primary keys
    listing_id,

    -- Host information
    host_id,
    host_name,
    host_city,           -- Include new columns
    host_country,        -- Include new columns
    host_since,          -- Now a date type
    host_response_time_category,
    host_response_time_hours,
    host_response_rate_pct,
    host_acceptance_rate_pct,
    host_quality_score,
    host_is_superhost,
    host_listings_count,
    calculated_host_listings_count,
    calculated_host_listings_count_entire_homes,
    calculated_host_listings_count_private_rooms,
    calculated_host_listings_count_shared_rooms,

    -- Listing attributes
    listing_name,
    neighbourhood_cleansed,
    latitude,
    longitude,
    property_type,
    room_type,
    accommodates,
    bathrooms_text,
    bedrooms,
    beds,
    amenities,


    -- Booking details
    price,
    minimum_nights,
    maximum_nights,
    license,
    instant_bookable,

    -- Metadata fields
    last_scraped,
    _ingested_at,
    record_hash,
    valid_from as last_updated_at,
    {{ current_timestamp_utc() }} as dbt_updated_at,
    dbt_loaded_at as first_loaded_at
FROM current_listings
