WITH source_data AS (
    SELECT *
    FROM {{ source('bronze', 'raw_listings') }}
)

SELECT
    -- Simple type casting and renaming
    id::bigint as listing_id,
    host_id::bigint as host_id,
    name as listing_name,
    host_name,
    host_since,
    host_location,
    host_response_time,
    host_response_rate,
    host_acceptance_rate,
    (host_is_superhost = 't')::boolean as host_is_superhost,
    host_listings_count::integer,
    neighbourhood_cleansed,
    latitude::decimal(10,6),
    longitude::decimal(10,6),
    TRIM(property_type) as property_type,
    TRIM(room_type) as room_type,
    accommodates::integer,
    bathrooms_text,
    bedrooms::integer,
    beds::integer,
    amenities,
    REPLACE(REPLACE(price, '$', ''), ',', '') as price_raw,
    minimum_nights::integer,
    maximum_nights::integer,
    license,
    (instant_bookable = 't')::boolean as instant_bookable,
    calculated_host_listings_count::integer,
    calculated_host_listings_count_entire_homes::integer,
    calculated_host_listings_count_private_rooms::integer,
    calculated_host_listings_count_shared_rooms::integer,
    last_scraped::timestamp,
    _ingested_at,

    md5(concat_ws('|',
                COALESCE(CASE
                    WHEN price::text != ''
                        AND REPLACE(REPLACE(price, '$', ''), ',', '') ~ '^\d*\.?\d+$'
                    THEN REPLACE(REPLACE(price, '$', ''), ',', '')
                    ELSE NULL END::text, ''),
                COALESCE(CASE
                    WHEN minimum_nights::text ~ '^\d+$'
                    THEN minimum_nights::text
                    ELSE NULL END, ''),
                COALESCE(CASE
                    WHEN maximum_nights::text ~ '^\d+$'
                    THEN maximum_nights::text
                    ELSE NULL END, ''),
                COALESCE(CASE
                    WHEN host_is_superhost = 't' THEN 'true'
                    WHEN host_is_superhost = 'f' THEN 'false'
                    ELSE NULL END, ''),
                COALESCE(CASE
                    WHEN host_listings_count::text ~ '^\d+$'
                    THEN host_listings_count::text
                    ELSE NULL END, ''),
                COALESCE(room_type, ''),
                COALESCE(CASE
                    WHEN accommodates::text ~ '^\d+$'
                    THEN accommodates::text
                    ELSE NULL END, ''),
                COALESCE(bathrooms_text, ''),
                COALESCE(CASE
                    WHEN bedrooms::text ~ '^\d+$'
                    THEN bedrooms::text
                    ELSE NULL END, ''),
                COALESCE(CASE
                    WHEN beds::text ~ '^\d+$'
                    THEN beds::text
                    ELSE NULL END, ''),
                COALESCE(amenities, ''),
                COALESCE(license, ''),
                COALESCE(CASE
                    WHEN instant_bookable = 't' THEN 'true'
                    WHEN instant_bookable = 'f' THEN 'false'
                    ELSE NULL END, '')
            )) as record_hash

        FROM source_data
