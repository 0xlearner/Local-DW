import gzip
import io
import os

import polars as pl
import pytest

from src.config import Config
from src.pipeline.data_pipeline import Pipeline


@pytest.mark.asyncio
async def test_real_data_pipeline(test_config, minio_client, pg_pool, clean_test_db):
    """Test pipeline with real data from local directory"""
    pipeline = None
    target_table = "raw_listings"
    local_csv_path = "/app/data/listings.csv"
    s3_file_name = "listings.csv.gz"
    batch_id = None
    temp_table = None
    current_view = None

    try:
        # Read and compress the local CSV file
        df = pl.read_csv(local_csv_path)
        total_csv_rows = len(df)

        # Create compressed version
        compressed_path = "/app/data/listings.csv.gz"
        with open(local_csv_path, "rb") as f_in:
            with gzip.open(compressed_path, "wb") as f_out:
                f_out.write(f_in.read())

        # Upload to MinIO
        with open(compressed_path, "rb") as f:
            file_data = f.read()
            minio_client.put_object(
                bucket_name=test_config["s3"]["bucket"],
                object_name=s3_file_name,
                data=io.BytesIO(file_data),
                length=len(file_data),
            )

        # Verify file was uploaded
        try:
            minio_client.stat_object(test_config["s3"]["bucket"], s3_file_name)
        except Exception as e:
            pytest.fail(f"Failed to upload test file to MinIO: {str(e)}")

        # Initialize pipeline
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

        # Run pipeline
        batch_id = await pipeline.run(
            file_prefix=s3_file_name,
            primary_key="id",
            target_table=target_table,
        )

        # Define tables after we have batch_id
        temp_table = f"temp_{target_table}_{batch_id.replace('-', '_')}"
        current_view = f"stg_{target_table}_current"  # Add this line

        # Verify results
        async with pg_pool.acquire() as conn:
            # Check if temp table exists
            temp_table_exists = await conn.fetchval(
                """
                SELECT EXISTS (
                    SELECT FROM information_schema.tables
                    WHERE table_name = $1
                )
                """,
                temp_table,
            )
            assert temp_table_exists, f"Temp table {
                temp_table} was not created"

            # Check total rows
            db_count = await conn.fetchval(f"SELECT COUNT(*) FROM {temp_table}")
            assert (
                db_count == total_csv_rows
            ), f"Row count mismatch. CSV: {total_csv_rows}, DB: {db_count}"

            # Verify metadata columns exist and are populated
            metadata_check = await conn.fetchrow(
                f"""
                SELECT
                    COUNT(*) as total,
                    COUNT(DISTINCT _batch_id) as batch_ids,
                    COUNT(DISTINCT _file_name) as file_names,
                    COUNT(_ingested_at) as ingestion_timestamps
                FROM {temp_table}
                """
            )
            assert metadata_check["batch_ids"] == 1, "Should have exactly one batch_id"
            assert (
                metadata_check["file_names"] == 1
            ), "Should have exactly one file_name"
            assert (
                metadata_check["ingestion_timestamps"] == total_csv_rows
            ), "All rows should have ingestion timestamps"

            # Check processed files status
            processed_file = await conn.fetchrow(
                """
                SELECT status, rows_processed, error_message
                FROM processed_files
                WHERE batch_id = $1
                """,
                batch_id,
            )

            assert (
                processed_file is not None
            ), f"No processed file record found for batch_id: {batch_id}"
            assert (
                processed_file["status"] == "COMPLETED"
            ), f"File processing failed: {processed_file['error_message']}"
            assert processed_file["rows_processed"] == total_csv_rows

            # Check pipeline metrics
            metrics = await conn.fetchrow(
                """
                SELECT
                    rows_processed,
                    processing_status,
                    load_duration_seconds
                FROM pipeline_metrics
                WHERE batch_id = $1
                """,
                batch_id,
            )
            assert metrics["processing_status"] == "COMPLETED"
            assert metrics["rows_processed"] == total_csv_rows
            assert (
                metrics["load_duration_seconds"] > 0
            ), "Load duration should be greater than 0"

            # Sample data verification
            sample_data = await conn.fetch(
                f"""
                SELECT *
                FROM {temp_table}
                LIMIT 5
                """
            )
            assert len(sample_data) > 0, "No data found in temp table"

            # Verify data types
            column_types = await conn.fetch(
                """
                SELECT column_name, data_type
                FROM information_schema.columns
                WHERE table_name = $1
                """,
                temp_table,
            )
            assert len(column_types) > 0, "No columns found in temp table"

            # Get and print load summary
            print("\nLoad Summary:")
            print(f"Total rows processed: {metrics['rows_processed']}")
            print(
                f"Processing duration: {
                    metrics['load_duration_seconds']:.2f} seconds"
            )
            print(f"Processing status: {metrics['processing_status']}")

    finally:
        if pipeline:
            await pipeline.shutdown()

        # Clean up test files
        try:
            minio_client.remove_object(
                test_config["s3"]["bucket"], s3_file_name)
            os.remove(compressed_path)
        except:
            pass

        # Clean up the database objects
        if temp_table:
            async with pg_pool.acquire() as conn:
                # Drop view first, then table
                if current_view:
                    await conn.execute(f"DROP VIEW IF EXISTS {current_view}")
                await conn.execute(f"DROP TABLE IF EXISTS {temp_table}")


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
