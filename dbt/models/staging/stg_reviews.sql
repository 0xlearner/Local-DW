WITH source_data AS (
    SELECT *
    FROM {{ source('bronze', 'raw_reviews') }}
)

SELECT
    -- Simple type casting and renaming only
    id as review_id,
    listing_id,
    reviewer_id,
    reviewer_name,
    date as review_date,
    comments as review_text,
    {% if '_ingested_at' in adapter.get_columns_in_relation(source('bronze', 'raw_reviews')) | map(attribute='name') %}
    _ingested_at,
    {% else %}
    {{ current_timestamp_utc() }} as _ingested_at,
    {% endif %}
    md5(concat_ws('|',
        COALESCE(id::text, ''),
        COALESCE(listing_id::text, ''),
        COALESCE(date::text, ''),
        COALESCE(comments, '')
    )) as record_hash
FROM source_data
