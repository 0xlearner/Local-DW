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
    target_table = "listings"  # removed 'raw_' prefix as it's added in the pipeline
    local_csv_path = "/app/data/listings.csv"
    s3_file_name = "listings.csv.gz"
    batch_id = None

    try:
        # Initialize pipeline first
        config = Config()
        config.PG_HOST = test_config["postgres"]["host"]
        config.PG_PORT = test_config["postgres"]["port"]
        config.PG_USER = test_config["postgres"]["user"]
        config.PG_PASSWORD = test_config["postgres"]["password"]
        config.PG_DATABASE = test_config["postgres"]["database"]

        pipeline = Pipeline(config)
        await pipeline.initialize()  # Ensure this is called before any operations

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

        # Initialize pipeline with S3 and DB configs
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

        # Verify data was loaded correctly
        async with pg_pool.acquire() as conn:
            # Check main table exists
            table_exists = await conn.fetchval("""
                SELECT EXISTS (
                    SELECT FROM pg_tables
                    WHERE schemaname = 'bronze'
                    AND tablename = $1
                )
            """, f"raw_{target_table}")
            assert table_exists, f"Table bronze.raw_{
                target_table} does not exist"

            # Check partition exists
            partition_exists = await conn.fetchval("""
                SELECT EXISTS (
                    SELECT FROM pg_tables
                    WHERE schemaname = 'bronze'
                    AND tablename = $1
                )
            """, f"raw_{target_table}_{batch_id.replace('-', '_')}")
            assert partition_exists, f"Partition for batch {
                batch_id} does not exist"

            # Verify row count in partition
            row_count = await conn.fetchval(f"""
                SELECT COUNT(*)
                FROM bronze.raw_{target_table}
                WHERE _batch_id = $1
            """, batch_id)
            assert row_count == total_csv_rows, (
                f"Expected {total_csv_rows} rows, but found {row_count}"
            )

            # Verify metadata columns
            sample_row = await conn.fetchrow(f"""
                SELECT _ingested_at, _file_name, _batch_id
                FROM bronze.raw_{target_table}
                WHERE _batch_id = $1
                LIMIT 1
            """, batch_id)
            assert sample_row["_file_name"] == s3_file_name, "File name mismatch"
            assert sample_row["_batch_id"] == batch_id, "Batch ID mismatch"
            assert sample_row["_ingested_at"] is not None, "Ingestion timestamp is missing"

            # Verify data integrity
            sample_data = await conn.fetch(f"""
                SELECT *
                FROM bronze.raw_{target_table}
                WHERE _batch_id = $1
                LIMIT 5
            """, batch_id)
            assert len(sample_data) > 0, "No data found in the partition"

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

        # Clean up the partition and main table
        # async with pg_pool.acquire() as conn:
        #     if batch_id:
        #         await conn.execute(f"""
        #             DROP TABLE IF EXISTS bronze.raw_{target_table}_{batch_id.replace('-', '_')}
        #         """)
        #         await conn.execute(f"""
        #             DROP TABLE IF EXISTS bronze.raw_{target_table}
        #         """)


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
