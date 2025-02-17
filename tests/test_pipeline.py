import asyncio
import asyncpg
import pytest
import json
from src.pipeline.pipeline import Pipeline
from src.config import Config


@pytest.mark.asyncio
async def test_pipeline_execution(
    test_config, minio_client, compressed_csv_file, pg_pool, clean_test_db
):
    """
    Test the complete pipeline execution with a sample CSV file.
    """
    pipeline = None
    try:
        # Upload test file to MinIO
        with open(compressed_csv_file, "rb") as f:
            f.seek(0, 2)
            file_size = f.tell()
            f.seek(0)

            minio_client.put_object(
                test_config["s3"]["bucket"], "test_data.csv.gz", f, file_size
            )

        # Configure pipeline
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

        # Initialize and run pipeline
        pipeline = Pipeline(config)
        await pipeline.initialize()
        await pipeline.run(file_prefix="", primary_key="id")

        # Ensure pipeline is fully shut down
        await pipeline.shutdown()
        await pipeline.cleanup()

        await asyncio.sleep(0.2)

        # Verify results with a fresh connection
        conn = await asyncpg.connect(**test_config["postgres"])
        try:
            # Verify row count
            result = await conn.fetchval("SELECT COUNT(*) FROM raw_test_data")
            assert result == 100, f"Expected 100 rows, but found {result}"

            # Verify table metadata
            table_meta = await conn.fetchrow(
                """
                SELECT * FROM table_metadata 
                WHERE table_name = 'raw_test_data'
            """
            )
            assert table_meta is not None, "Table metadata not found"
            assert table_meta["total_rows"] == 100
            assert table_meta["last_file_processed"] == "test_data.csv.gz"
            assert table_meta["schema_snapshot"] is not None

            # Verify change history
            changes = await conn.fetch(
                """
                SELECT change_type, COUNT(*) as count 
                FROM change_history 
                WHERE table_name = 'raw_test_data'
                GROUP BY change_type
            """
            )
            changes_dict = {row["change_type"]: row["count"] for row in changes}
            assert changes_dict.get("INSERT", 0) == 100, "Expected 100 INSERT records"
            assert changes_dict.get("UPDATE", 0) == 0, "Expected 0 UPDATE records"

            # Verify merge history
            merge_record = await conn.fetchrow(
                """
                SELECT * FROM merge_history 
                WHERE table_name = 'raw_test_data'
                ORDER BY started_at DESC 
                LIMIT 1
            """
            )
            assert merge_record is not None, "Merge history not found"
            assert merge_record["rows_inserted"] == 100
            assert merge_record["rows_updated"] == 0
            assert merge_record["status"] == "COMPLETED"
            assert merge_record["error_message"] is None

            # Run pipeline again with same data to test update scenario
            await pipeline.initialize()
            await pipeline.run(file_prefix="", primary_key="id")
            await pipeline.shutdown()
            await pipeline.cleanup()

            await asyncio.sleep(0.2)

            # Verify update scenario
            merge_record_update = await conn.fetchrow(
                """
                SELECT * FROM merge_history 
                WHERE table_name = 'raw_test_data'
                ORDER BY started_at DESC 
                LIMIT 1
            """
            )
            assert merge_record_update["rows_inserted"] == 0
            assert merge_record_update["rows_updated"] == 100
            assert merge_record_update["status"] == "COMPLETED"

            # Verify final change history counts
            final_changes = await conn.fetch(
                """
                SELECT change_type, COUNT(*) as count 
                FROM change_history 
                WHERE table_name = 'raw_test_data'
                GROUP BY change_type
            """
            )
            final_changes_dict = {
                row["change_type"]: row["count"] for row in final_changes
            }
            assert (
                final_changes_dict.get("INSERT", 0) == 100
            ), "Expected 100 total INSERT records"
            assert (
                final_changes_dict.get("UPDATE", 0) == 100
            ), "Expected 100 UPDATE records after second run"

            # Verify schema consistency
            schema_check = await conn.fetchrow(
                """
                SELECT schema_snapshot 
                FROM table_metadata 
                WHERE table_name = 'raw_test_data'
            """
            )
            assert schema_check["schema_snapshot"] is not None
            schema = json.loads(schema_check["schema_snapshot"])
            assert "id" in schema, "Schema should contain 'id' field"
            assert schema["id"] == "BIGINT", "ID field should be BIGINT"

        finally:
            await conn.close()

    finally:
        if pipeline:
            await pipeline.shutdown()
            await pipeline.cleanup()
