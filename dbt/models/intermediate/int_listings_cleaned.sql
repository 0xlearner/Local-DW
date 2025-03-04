{{
    config(
        materialized = 'incremental',
        unique_key = 'listing_id',
        on_schema_change = 'append_new_columns'
    )
}}

WITH stg_listings AS (
    SELECT *
    FROM {{ ref('stg_listings') }}
)

SELECT
    -- Primary keys and IDs
    listing_id,
    host_id,

    -- Basic information
    listing_name,
    host_name,

    -- Your existing location parsing
    CASE
        WHEN host_location LIKE '%Berlin, Germany%' THEN 'Berlin'
        WHEN host_location LIKE '%,%' THEN
            TRIM(SPLIT_PART(host_location, ',', 1))
        ELSE NULL
    END AS host_city,

    CASE
        WHEN host_location LIKE '%Berlin, Germany%' THEN 'Germany'
        WHEN host_location LIKE '%, Germany%' THEN 'Germany'
        WHEN host_location LIKE '%, France%' THEN 'France'
        WHEN host_location LIKE '%, United Kingdom%' OR host_location LIKE '%, UK%' THEN 'United Kingdom'
        WHEN host_location LIKE '%, United States%' OR host_location LIKE '%, USA%' THEN 'United States'
        WHEN host_location LIKE '%, Austria%' THEN 'Austria'
        WHEN host_location LIKE '%, Spain%' THEN 'Spain'
        WHEN host_location LIKE '%, Italy%' THEN 'Italy'
        WHEN host_location LIKE '%,%' THEN
            TRIM(SPLIT_PART(host_location, ',', -1))
        ELSE host_location
    END AS host_country,

    -- Convert host_since to date
    CASE
        WHEN host_since IS NOT NULL AND host_since ~ '^\d{4}-\d{2}-\d{2}$' THEN host_since::date
        WHEN host_since IS NOT NULL AND host_since ~ '^\d{2}/\d{2}/\d{4}$' THEN
            TO_DATE(host_since, 'MM/DD/YYYY')
        ELSE NULL
    END AS host_since,

    -- Handle host_response_time as a standardized category
    CASE
        WHEN host_response_time = 'within an hour' OR host_response_time = 'within a hour' THEN 'within_hour'
        WHEN host_response_time = 'within a few hours' OR host_response_time = 'within few hours' THEN 'within_few_hours'
        WHEN host_response_time = 'within a day' THEN 'within_day'
        WHEN host_response_time = 'within a few days' OR host_response_time = 'within few days' THEN 'within_few_days'
        WHEN host_response_time IS NOT NULL THEN 'other'
        ELSE NULL
    END AS host_response_time_category,

    -- Convert response time to approximate hours for analysis
    CASE
        WHEN host_response_time = 'within an hour' OR host_response_time = 'within a hour' THEN 1
        WHEN host_response_time = 'within a few hours' OR host_response_time = 'within few hours' THEN 3
        WHEN host_response_time = 'within a day' THEN 24
        WHEN host_response_time = 'within a few days' OR host_response_time = 'within few days' THEN 72
        ELSE NULL
    END AS host_response_time_hours,

    -- Convert percentage strings to numeric values - FIXED FOR N/A VALUES
    CASE
        WHEN host_response_rate IS NOT NULL
             AND host_response_rate != 'N/A'
             AND host_response_rate != ''
             AND host_response_rate ~ '%'
             AND REPLACE(host_response_rate, '%', '') ~ '^\d+(\.\d+)?$'
            THEN REPLACE(host_response_rate, '%', '')::DECIMAL(5,2)
        ELSE NULL
    END AS host_response_rate_pct,

    CASE
        WHEN host_acceptance_rate IS NOT NULL
             AND host_acceptance_rate != 'N/A'
             AND host_acceptance_rate != ''
             AND host_acceptance_rate ~ '%'
             AND REPLACE(host_acceptance_rate, '%', '') ~ '^\d+(\.\d+)?$'
            THEN REPLACE(host_acceptance_rate, '%', '')::DECIMAL(5,2)
        ELSE NULL
    END AS host_acceptance_rate_pct,

    -- Host quality score - FIXED TO USE PROPER NULL CHECKS
    CASE
        WHEN host_response_rate IS NOT NULL
             AND host_response_rate != 'N/A'
             AND host_response_rate != ''
             AND host_response_rate ~ '%'
             AND REPLACE(host_response_rate, '%', '') ~ '^\d+(\.\d+)?$'
             AND REPLACE(host_response_rate, '%', '')::DECIMAL(5,2) >= 90
             AND host_response_time IN ('within an hour', 'within a hour', 'within a few hours', 'within few hours')
             AND host_acceptance_rate IS NOT NULL
             AND host_acceptance_rate != 'N/A'
             AND host_acceptance_rate != ''
             AND host_acceptance_rate ~ '%'
             AND REPLACE(host_acceptance_rate, '%', '') ~ '^\d+(\.\d+)?$'
             AND REPLACE(host_acceptance_rate, '%', '')::DECIMAL(5,2) >= 80
            THEN 'excellent'
        WHEN host_response_rate IS NOT NULL
             AND host_response_rate != 'N/A'
             AND host_response_rate != ''
             AND host_response_rate ~ '%'
             AND REPLACE(host_response_rate, '%', '') ~ '^\d+(\.\d+)?$'
             AND REPLACE(host_response_rate, '%', '')::DECIMAL(5,2) >= 70
             AND host_response_time IN ('within an hour', 'within a hour', 'within a few hours', 'within few hours', 'within a day')
             AND host_acceptance_rate IS NOT NULL
             AND host_acceptance_rate != 'N/A'
             AND host_acceptance_rate != ''
             AND host_acceptance_rate ~ '%'
             AND REPLACE(host_acceptance_rate, '%', '') ~ '^\d+(\.\d+)?$'
             AND REPLACE(host_acceptance_rate, '%', '')::DECIMAL(5,2) >= 60
            THEN 'good'
        WHEN host_response_rate IS NOT NULL
             AND host_response_rate != 'N/A'
             AND host_response_rate != ''
             AND host_response_rate ~ '%'
             AND REPLACE(host_response_rate, '%', '') ~ '^\d+(\.\d+)?$'
             AND REPLACE(host_response_rate, '%', '')::DECIMAL(5,2) >= 50
            THEN 'average'
        WHEN host_response_rate IS NOT NULL
             AND host_response_rate != 'N/A'
             AND host_response_rate != ''
             AND host_response_rate ~ '%'
             AND REPLACE(host_response_rate, '%', '') ~ '^\d+(\.\d+)?$'
            THEN 'below_average'
        ELSE NULL
    END AS host_quality_score,

    -- Rest of your columns
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

    -- Price conversion
    CASE
        WHEN price_raw IS NOT NULL
             AND TRIM(price_raw) != ''
             AND TRIM(price_raw) ~ '^\d*\.?\d+$'
        THEN TRIM(price_raw)::DECIMAL(10,2)
        ELSE NULL
    END AS price,

    minimum_nights,
    maximum_nights,
    license,
    instant_bookable,
    calculated_host_listings_count::integer,
    calculated_host_listings_count_entire_homes::integer,
    calculated_host_listings_count_private_rooms::integer,
    calculated_host_listings_count_shared_rooms::integer,
    last_scraped,
    _ingested_at,
    record_hash,

    -- Data quality check
    CASE
        WHEN listing_id IS NULL THEN 'missing_listing_id'
        WHEN listing_name IS NULL THEN 'missing_listing_name'
        WHEN price_raw IS NULL OR NOT (TRIM(price_raw) ~ '^\d*\.?\d+$') THEN 'invalid_price'
        WHEN TRIM(price_raw)::DECIMAL(10,2) < 0 THEN 'negative_price'
        WHEN accommodates IS NULL OR accommodates <= 0 OR accommodates >= 50 THEN 'invalid_accommodates'
        WHEN minimum_nights < 0 THEN 'negative_minimum_nights'
        WHEN maximum_nights < minimum_nights THEN 'invalid_maximum_nights'
        ELSE 'valid'
    END AS data_quality_check
FROM stg_listings
{% if is_incremental() %}
WHERE _ingested_at > (SELECT max(_ingested_at) FROM {{ this }})
{% endif %}
