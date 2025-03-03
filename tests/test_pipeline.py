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
            "local_path": "/app/data/listings.csv",
            "s3_path": "data/listings/listings.csv.gz",
            "table": "listings",
        },
        {
            "local_path": "/app/data/reviews.csv",
            "s3_path": "data/reviews/reviews.csv.gz",
            "table": "reviews",
        },
        {
            "local_path": "/app/data/calendar.csv",
            "s3_path": "data/calendar/calendar.csv.gz",
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
            # Read and compress the local CSV file
            df = pl.read_csv(file_info["local_path"])
            total_rows = len(df)

            # Create compressed version in temporary location
            compressed_path = f"{file_info['local_path']}.gz"
            with open(file_info["local_path"], "rb") as f_in:
                with gzip.open(compressed_path, "wb") as f_out:
                    f_out.write(f_in.read())

            # Upload to MinIO
            with open(compressed_path, "rb") as f:
                file_data = f.read()
                # Create directory structure if needed
                directory = os.path.dirname(file_info["s3_path"])
                if directory:
                    try:
                        minio_client.put_object(
                            bucket_name=test_config["s3"]["bucket"],
                            object_name=f"{directory}/",
                            data=io.BytesIO(b""),
                            length=0,
                        )
                    except:
                        pass  # Directory might already exist

                # Upload file
                minio_client.put_object(
                    bucket_name=test_config["s3"]["bucket"],
                    object_name=file_info["s3_path"],
                    data=io.BytesIO(file_data),
                    length=len(file_data),
                )

            # Verify file was uploaded
            try:
                minio_client.stat_object(
                    test_config["s3"]["bucket"], file_info["s3_path"]
                )
            except Exception as e:
                pytest.fail(f"Failed to upload test file to MinIO: {str(e)}")

            # Run pipeline for this file
            batch_id = await pipeline.run(
                file_prefix=file_info["s3_path"],
                primary_key="id",
                target_table=file_info["table"],
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
                ), f"Table bronze.raw_{
                    file_info['table']} does not exist"

                # Verify row count
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
                ), f"Expected {total_rows} rows, but found {
                    row_count} for {file_info['table']}"

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

        # Cleanup
        try:
            for file_info in source_files:
                # Remove from MinIO
                minio_client.remove_object(
                    test_config["s3"]["bucket"], file_info["s3_path"]
                )
                # Remove local compressed file
                compressed_path = f"{file_info['local_path']}.gz"
                if os.path.exists(compressed_path):
                    os.remove(compressed_path)
        except:
            pass


@pytest.mark.asyncio
async def test_pipeline_performance(
    test_config, minio_client, pg_pool, clean_test_db, sample_csv_data
):
    """Test pipeline performance with large dataset"""
    pipeline = None
    target_table = "performance_test"
    s3_file_name = "large_dataset.csv.gz"

    try:
        # Create large dataset
        large_df = pl.concat([sample_csv_data] * 100)  # 10,000 rows

        # Save and compress
        compressed_path = "/app/data/large_dataset.csv.gz"
        with gzip.open(compressed_path, "wb") as f_out:
            f_out.write(large_df.write_csv().encode())

        # Upload to MinIO
        with open(compressed_path, "rb") as f:
            file_data = f.read()
            minio_client.put_object(
                bucket_name=test_config["s3"]["bucket"],
                object_name=s3_file_name,
                data=io.BytesIO(file_data),
                length=len(file_data),
            )

        # Initialize and run pipeline
        config = Config()
        config.MINIO_ENDPOINT = test_config["s3"]["endpoint"]
        config.MINIO_ACCESS_KEY = test_config["s3"]["access_key"]
        config.MINIO_SECRET_KEY = test_config["s3"]["secret_key"]
        config.MINIO_BUCKET = test_config["s3"]["bucket"]
        config.PG_HOST = test_config["postgres"]["host"]
        config.PG_PORT = test_config["postgres"]["port"]
        config.PG_USER = test_config["postgres"]["user"]
        config.PG_PASSWORD = test_config["postgres"]["password"]
        config.PG_DATABASE = test_config["postgres"]["database"]

        pipeline = Pipeline(config)
        await pipeline.initialize()

        # Run pipeline and measure performance
        start_time = pytest.helpers.timer()
        batch_id = await pipeline.run(
            file_prefix=s3_file_name,
            primary_key="id",
            target_table=target_table,
        )
        processing_time = pytest.helpers.timer() - start_time

        # Verify performance metrics
        async with pg_pool.acquire() as conn:
            metrics = await conn.fetchrow(
                """
                SELECT
                    rows_processed,
                    load_duration_seconds,
                    processing_status
                FROM pipeline_metrics
                WHERE batch_id = $1
                """,
                batch_id,
            )

            # Performance assertions
            assert processing_time < 10, "Processing took too long"
            assert metrics["load_duration_seconds"] < 5, "Data loading took too long"
            rows_per_second = (
                metrics["rows_processed"] / metrics["load_duration_seconds"]
            )
            assert rows_per_second > 1000, "Bulk loading performance below threshold"

            print("\nPerformance Metrics:")
            print(f"Total rows processed: {metrics['rows_processed']}")
            print(f"Processing time: {processing_time:.2f} seconds")
            print(f"Rows per second: {rows_per_second:.2f}")

    finally:
        if pipeline:
            await pipeline.shutdown()

        # Cleanup
        try:
            minio_client.remove_object(
                test_config["s3"]["bucket"], s3_file_name)
            os.remove(compressed_path)
        except:
            pass

        async with pg_pool.acquire() as conn:
            await conn.execute(
                f"DROP TABLE IF EXISTS temp_{target_table}_{
                    batch_id.replace('-', '_')}"
            )
