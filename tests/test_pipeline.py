import gzip
import io
import os

import polars as pl
import pytest

from src.config import Config
from src.pipeline.data_pipeline import Pipeline


@pytest.mark.asyncio
async def test_real_data_pipeline(test_config, minio_client, pg_pool):
    """Test pipeline with real data from multiple directories"""
    pipeline = None
    source_files = [
        {
            "s3_path": "listings/listings.csv.gz",  # Removed "data/" prefix
            "table": "listings",
        },
        {
            "s3_path": "reviews/reviews.csv.gz",  # Removed "data/" prefix
            "table": "reviews",
        },
        {
            "s3_path": "calendar/calendar.csv.gz",  # Removed "data/" prefix
            "table": "calendar",
        },
    ]
    batch_ids = {}

    try:
        # Initialize pipeline
        config = Config()
        config.PG_HOST = test_config["postgres"]["host"]
        config.PG_PORT = test_config["postgres"]["port"]
        config.PG_USER = test_config["postgres"]["user"]
        config.PG_PASSWORD = test_config["postgres"]["password"]
        config.PG_DATABASE = test_config["postgres"]["database"]
        config.MINIO_ENDPOINT = test_config["s3"]["endpoint"]
        config.MINIO_ACCESS_KEY = test_config["s3"]["access_key"]
        config.MINIO_SECRET_KEY = test_config["s3"]["secret_key"]
        config.MINIO_BUCKET = test_config["s3"]["bucket"]

        pipeline = Pipeline(config)
        await pipeline.initialize()

        # Process each file
        for file_info in source_files:
            # Verify file exists in MinIO
            try:
                minio_client.stat_object(
                    test_config["s3"]["bucket"], file_info["s3_path"]
                )
            except Exception as e:
                pytest.fail(f"Required file not found in MinIO: {str(e)}")

            # Run pipeline for this file
            batch_id = await pipeline.run(
                file_prefix=file_info["s3_path"],
                primary_key="id",
                target_table=file_info["table"],
            )

            # Get total rows for verification
            async with pg_pool.acquire() as conn:
                total_rows = await conn.fetchval(
                    f"""
                    SELECT COUNT(*)
                    FROM bronze.raw_{file_info['table']}
                    WHERE _batch_id = $1
                    """,
                    batch_id,
                )

            batch_ids[file_info["table"]] = {
                "batch_id": batch_id,
                "total_rows": total_rows,
            }

            # Verify data was loaded correctly
            async with pg_pool.acquire() as conn:
                # Check main table exists
                table_exists = await conn.fetchval(
                    """
                    SELECT EXISTS (
                        SELECT FROM pg_tables
                        WHERE schemaname = 'bronze'
                        AND tablename = $1
                    )
                    """,
                    f"raw_{file_info['table']}",
                )
                assert (
                    table_exists
                ), f"Table bronze.raw_{file_info['table']} does not exist"

                # Verify row count matches
                row_count = await conn.fetchval(
                    f"""
                    SELECT COUNT(*)
                    FROM bronze.raw_{file_info['table']}
                    WHERE _batch_id = $1
                    """,
                    batch_id,
                )
                assert (
                    row_count == total_rows
                ), f"Expected {total_rows} rows, but found {row_count} for {file_info['table']}"

                # Verify metadata columns
                sample_row = await conn.fetchrow(
                    f"""
                    SELECT _ingested_at, _file_name, _batch_id
                    FROM bronze.raw_{file_info['table']}
                    WHERE _batch_id = $1
                    LIMIT 1
                    """,
                    batch_id,
                )
                assert (
                    sample_row["_file_name"] == file_info["s3_path"]
                ), "File name mismatch"
                assert sample_row["_batch_id"] == batch_id, "Batch ID mismatch"
                assert (
                    sample_row["_ingested_at"] is not None
                ), "Ingestion timestamp is missing"

    finally:
        if pipeline:
            await pipeline.shutdown()
