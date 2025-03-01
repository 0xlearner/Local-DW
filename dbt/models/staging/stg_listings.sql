{{
    config(
        materialized = 'view'
    )
}}

WITH source_data AS (
    SELECT
        id as listing_id,
        host_id,
        name as listing_name,
        host_name,
        -- Convert text 't'/'f' to boolean
        CASE
            WHEN host_is_superhost = 't' THEN TRUE
            WHEN host_is_superhost = 'f' THEN FALSE
            ELSE NULL
        END as host_is_superhost,
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
        REPLACE(REPLACE(price, '$', ''), ',', '')::DECIMAL(10,2) as price,
        minimum_nights,
        maximum_nights,
        license,
        -- Convert text 't'/'f' to boolean
        CASE
            WHEN instant_bookable = 't' THEN TRUE
            WHEN instant_bookable = 'f' THEN FALSE
            ELSE NULL
        END as instant_bookable,
        -- Convert date string to timestamp
        CASE
            WHEN last_scraped ~ '^\d{4}-\d{2}-\d{2}$'
            THEN (last_scraped || ' 00:00:00')::timestamp
            ELSE NULL
        END as last_scraped,
        _ingested_at,
        md5(concat_ws('|',
            COALESCE(price::text, ''),
            COALESCE(minimum_nights::text, ''),
            COALESCE(maximum_nights::text, ''),
            COALESCE(host_is_superhost::text, ''),
            COALESCE(host_listings_count::text, ''),
            COALESCE(room_type, ''),
            COALESCE(accommodates::text, ''),
            COALESCE(bathrooms_text, ''),
            COALESCE(bedrooms::text, ''),
            COALESCE(beds::text, ''),
            COALESCE(amenities, ''),
            COALESCE(license, ''),
            COALESCE(instant_bookable::text, '')
        )) as record_hash
    FROM {{ source('public', 'stg_raw_listings_current') }}
)

SELECT *
FROM source_data
