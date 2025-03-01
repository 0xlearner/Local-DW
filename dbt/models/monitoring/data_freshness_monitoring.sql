{{
    config(
        materialized = 'incremental',
        unique_key = ['table_name', 'batch_id'],
        on_schema_change='fail'
    )
}}

WITH freshness_metrics AS (
    SELECT
        'listings' as table_name,
        {{ current_timestamp_utc() }} as check_timestamp,
        md5({{ current_timestamp_utc() }}::text)::text as batch_id,
        min(last_scraped) as oldest_record_timestamp,
        max(last_scraped) as newest_record_timestamp,
        min(_ingested_at) as oldest_ingestion_timestamp,
        max(_ingested_at) as newest_ingestion_timestamp,
        count(*) as record_count,
        sum(case when _ingested_at >= {{ current_timestamp_utc() }} - interval '24 hours' then 1 else 0 end) as fresh_records_24h,
        min(dbt_loaded_at) as warehouse_load_start,
        max(dbt_loaded_at) as warehouse_load_end
    FROM {{ ref('dim_listings_scd') }}
    WHERE is_current = TRUE
    GROUP BY 1, 2, 3
)

SELECT
    *,
    EXTRACT(EPOCH FROM (newest_record_timestamp - oldest_record_timestamp))/3600 as data_time_span_hours,
    EXTRACT(EPOCH FROM (newest_ingestion_timestamp - oldest_ingestion_timestamp))/3600 as ingestion_time_span_hours,
    EXTRACT(EPOCH FROM (warehouse_load_end - warehouse_load_start))/3600 as load_duration_hours,
    fresh_records_24h::float / NULLIF(record_count, 0) * 100 as fresh_records_percentage
FROM freshness_metrics
