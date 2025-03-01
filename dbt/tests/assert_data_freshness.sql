WITH freshness_check AS (
    SELECT
        listing_id,
        last_scraped,
        _ingested_at,
        dbt_loaded_at,
        {{ current_timestamp_utc() }} as current_timestamp,
        CASE
            WHEN last_scraped < ({{ current_timestamp_utc() }} - interval '24 hours') THEN 'stale_scrape'
            WHEN _ingested_at < ({{ current_timestamp_utc() }} - interval '1 hour') THEN 'stale_ingest'
            WHEN dbt_loaded_at < ({{ current_timestamp_utc() }} - interval '1 hour') THEN 'stale_load'
            ELSE 'fresh'
        END as freshness_status
    FROM {{ ref('dim_listings_scd') }}
    WHERE is_current = TRUE
)

SELECT *
FROM freshness_check
WHERE freshness_status != 'fresh'
