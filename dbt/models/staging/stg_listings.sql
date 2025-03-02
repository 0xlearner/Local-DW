WITH source_data AS (
    SELECT *
    FROM {{ source('bronze', 'raw_listings') }}
),

transformed AS (
    SELECT
        -- Primary keys and IDs - Safe conversion
        CASE
            WHEN id::text ~ '^\d+$' THEN id::bigint
            ELSE NULL
        END as listing_id,

        CASE
            WHEN host_id::text ~ '^\d+$' THEN host_id::bigint
            ELSE NULL
        END as host_id,

        name as listing_name,
        host_name,

        CASE
            WHEN host_is_superhost = 't' THEN TRUE
            WHEN host_is_superhost = 'f' THEN FALSE
            ELSE NULL
        END as host_is_superhost,

        CASE
            WHEN host_listings_count::text ~ '^\d+$' THEN host_listings_count::integer
            ELSE NULL
        END as host_listings_count,

        neighbourhood_cleansed,

        CASE
            WHEN latitude::text ~ '^-?\d*\.?\d+$'
                AND latitude::text != ''
                AND latitude::decimal >= -90
                AND latitude::decimal <= 90
            THEN latitude::decimal(10,6)
            ELSE NULL
        END as latitude,

        CASE
            WHEN longitude::text ~ '^-?\d*\.?\d+$'
                AND longitude::text != ''
                AND longitude::decimal >= -180
                AND longitude::decimal <= 180
            THEN longitude::decimal(10,6)
            ELSE NULL
        END as longitude,

        TRIM(property_type) as property_type,
        TRIM(room_type) as room_type,

        CASE
            WHEN accommodates::text ~ '^\d+$' THEN accommodates::integer
            ELSE NULL
        END as accommodates,

        bathrooms_text,

        CASE
            WHEN bedrooms::text ~ '^\d+$' THEN bedrooms::integer
            ELSE NULL
        END as bedrooms,

        CASE
            WHEN beds::text ~ '^\d+$' THEN beds::integer
            ELSE NULL
        END as beds,

        CASE
            WHEN amenities LIKE '[%]' THEN amenities
            ELSE '[]'
        END as amenities,

        CASE
            WHEN price::text != ''
                AND REPLACE(REPLACE(price, '$', ''), ',', '') ~ '^\d*\.?\d+$'
            THEN REPLACE(REPLACE(price, '$', ''), ',', '')::DECIMAL(10,2)
            ELSE NULL
        END as price,

        CASE
            WHEN minimum_nights::text ~ '^\d+$' THEN minimum_nights::integer
            ELSE NULL
        END as minimum_nights,

        CASE
            WHEN maximum_nights::text ~ '^\d+$' THEN maximum_nights::integer
            ELSE NULL
        END as maximum_nights,

        license,

        CASE
            WHEN instant_bookable = 't' THEN TRUE
            WHEN instant_bookable = 'f' THEN FALSE
            ELSE NULL
        END as instant_bookable,

        CASE
            WHEN last_scraped ~ '^\d{4}-\d{2}-\d{2}$'
            THEN (last_scraped || ' 00:00:00')::timestamp
            ELSE NULL
        END as last_scraped,

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
        )

SELECT * FROM transformed
