{{
    config(
        materialized = 'incremental',
        unique_key = 'listing_sk',
        incremental_strategy = 'merge',
        on_schema_change='fail'
    )
}}

WITH source AS (
    SELECT
        *,
        {{ current_timestamp_utc() }} as dbt_loaded_at
    FROM {{ ref('stg_listings') }}
),

existing_records AS (
    {% if is_incremental() %}
        SELECT *
        FROM {{ this }}
        WHERE is_current = TRUE
    {% else %}
        -- Include all necessary columns for first run with correct data types
        SELECT
            NULL::varchar as listing_sk,
            NULL::bigint as listing_id,
            NULL::bigint as host_id,
            NULL::text as listing_name,
            NULL::text as host_name,
            NULL::boolean as host_is_superhost,
            NULL::integer as host_listings_count,
            NULL::text as neighbourhood_cleansed,
            NULL::decimal as latitude,
            NULL::decimal as longitude,
            NULL::text as property_type,
            NULL::text as room_type,
            NULL::integer as accommodates,
            NULL::text as bathrooms_text,
            NULL::integer as bedrooms,
            NULL::integer as beds,
            NULL::text as amenities,
            NULL::decimal(10,2) as price,
            NULL::integer as minimum_nights,
            NULL::integer as maximum_nights,
            NULL::text as license,
            NULL::boolean as instant_bookable,
            NULL::timestamp as last_scraped,
            NULL::timestamp as _ingested_at,
            NULL::text as record_hash,
            NULL::timestamp as valid_from,
            NULL::timestamp as valid_to,
            NULL::boolean as is_current,
            NULL::timestamp as dbt_loaded_at
        LIMIT 0
    {% endif %}
),

changes AS (
    SELECT
        source.*,
        COALESCE(
            existing.listing_sk,
            {{ dbt_utils.generate_surrogate_key(['source.listing_id']) }}
        ) as listing_sk,
        CASE
            WHEN existing.listing_sk IS NULL THEN TRUE  -- New record
            WHEN source._ingested_at > existing._ingested_at  -- More recent data
                 AND source.record_hash != existing.record_hash THEN TRUE  -- Actual changes
            ELSE FALSE
        END as requires_change
    FROM source
    LEFT JOIN existing_records existing
        ON source.listing_id = existing.listing_id
),

final_updates AS (
    SELECT
        listing_sk,
        listing_id,
        host_id,
        listing_name,
        host_name,
        host_is_superhost::boolean,
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
        instant_bookable::boolean,
        {{ convert_timezone('last_scraped') }} as last_scraped,  -- Ensure UTC timezone
        _ingested_at,
        record_hash,
        dbt_loaded_at as valid_from,
        '9999-12-31 23:59:59'::timestamp as valid_to,
        TRUE as is_current,
        dbt_loaded_at
    FROM changes
    WHERE requires_change

    UNION ALL

    SELECT
        existing.listing_sk,
        existing.listing_id,
        existing.host_id,
        existing.listing_name,
        existing.host_name,
        existing.host_is_superhost::boolean,
        existing.host_listings_count,
        existing.neighbourhood_cleansed,
        existing.latitude,
        existing.longitude,
        existing.property_type,
        existing.room_type,
        existing.accommodates,
        existing.bathrooms_text,
        existing.bedrooms,
        existing.beds,
        existing.amenities,
        existing.price,
        existing.minimum_nights,
        existing.maximum_nights,
        existing.license,
        existing.instant_bookable::boolean,
        existing.last_scraped,
        existing._ingested_at,
        existing.record_hash,
        existing.valid_from,
        CASE
            WHEN c.requires_change THEN c.dbt_loaded_at
            ELSE existing.valid_to
        END as valid_to,
        CASE
            WHEN c.requires_change THEN FALSE
            ELSE existing.is_current
        END as is_current,
        existing.dbt_loaded_at
    FROM existing_records existing
    LEFT JOIN changes c
        ON existing.listing_id = c.listing_id
    WHERE c.requires_change OR existing.is_current = FALSE
)

SELECT * FROM final_updates
