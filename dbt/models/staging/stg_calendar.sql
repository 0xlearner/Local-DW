WITH source_data AS (
    SELECT *
    FROM {{ source('bronze', 'raw_calendar') }}
)

SELECT
    listing_id,
    date AS calendar_date,
    available,
    price,
    adjusted_price,
    minimum_nights,
    maximum_nights,
    {% if '_ingested_at' in adapter.get_columns_in_relation(source('bronze', 'raw_calendar')) | map(attribute='name') %}
    _ingested_at,
    {% else %}
    {{ current_timestamp_utc() }} as _ingested_at,
    {% endif %}
    md5(concat_ws('|',
        COALESCE(listing_id::text, ''),
        COALESCE(date::text, ''),
        COALESCE(available, ''),
        COALESCE(price, ''),
        COALESCE(minimum_nights::text, ''),
        COALESCE(maximum_nights::text, '')
    )) as record_hash
FROM source_data
