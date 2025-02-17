import asyncio
import asyncpg
import pytest
from src.pipeline.pipeline import Pipeline
from src.config import Config


@pytest.mark.asyncio
async def test_pipeline_recovery(
    test_config, minio_client, compressed_csv_file, clean_test_db
):
    # Upload test file
    with open(compressed_csv_file, "rb") as f:
        minio_client.put_object(
            test_config["s3"]["bucket"], "test_data.csv.gz", f, f.seek(0, 2)
        )

    # Configure and initialize pipeline
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
    await pipeline.run(file_prefix="", primary_key="id")

    # Verify results
    async with asyncpg.create_pool(**test_config["postgres"]) as pool:
        async with pool.acquire() as conn:
            # Check if table was created
            result = await conn.fetchval(
                """
                SELECT COUNT(*) FROM raw_test_data
            """
            )
            assert result == 100

            # Check metadata
            meta_result = await conn.fetchrow(
                """
                SELECT * FROM table_metadata 
                WHERE table_name = 'raw_test_data'
            """
            )
            assert meta_result["total_rows"] == 100

            # Check change history
            changes = await conn.fetch(
                """
                SELECT * FROM change_history 
                WHERE table_name = 'raw_test_data'
            """
            )
            assert len(changes) > 0

    await pipeline.shutdown()

    # Simulate failure by corrupting data mid-process
    async def fail_first_attempt():
        raise Exception("Simulated failure")

    pipeline = Pipeline(config)
    pipeline.data_loader.load_data = fail_first_attempt

    # Run pipeline (should fail)
    with pytest.raises(Exception):
        await pipeline.run(file_prefix="", primary_key="id")

    # Verify recovery point was created
    async with asyncpg.create_pool(**test_config["postgres"]) as pool:
        async with pool.acquire() as conn:
            recovery_points = await conn.fetch(
                """
                SELECT * FROM recovery_points 
                WHERE status = 'FAILED'
            """
            )
            assert len(recovery_points) == 1

    # Restore original load_data method and let recovery complete
    pipeline = Pipeline(config)
    await pipeline.initialize()
    await asyncio.sleep(1)  # Allow recovery worker to process

    # Verify recovery succeeded
    async with asyncpg.create_pool(**test_config["postgres"]) as pool:
        async with pool.acquire() as conn:
            result = await conn.fetchval(
                """
                SELECT COUNT(*) FROM raw_test_data
            """
            )
            assert result == 100

    await pipeline.shutdown()
