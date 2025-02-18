import asyncio
import asyncpg
import pytest
import json
import io
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
    conn = None
    try:
        # Initial setup and table creation
        conn = await asyncpg.connect(**test_config["postgres"])

        # Create required tables if they don't exist
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS raw_test_data (
                id BIGINT PRIMARY KEY,
                name TEXT,
                email TEXT,
                age INTEGER,
                created_at TIMESTAMP WITH TIME ZONE,
                is_active BOOLEAN,
                tags JSONB,
                scores JSONB
            );

            CREATE TABLE IF NOT EXISTS change_history (
                id SERIAL PRIMARY KEY,
                table_name TEXT NOT NULL,
                primary_key_column TEXT NOT NULL,
                primary_key_value TEXT NOT NULL,
                column_name TEXT NOT NULL,
                old_value TEXT,
                new_value TEXT,
                change_type TEXT NOT NULL,
                file_name TEXT NOT NULL,
                batch_id TEXT NOT NULL,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS merge_history (
                id SERIAL PRIMARY KEY,
                table_name TEXT NOT NULL,
                file_name TEXT NOT NULL,
                started_at TIMESTAMP WITH TIME ZONE NOT NULL,
                completed_at TIMESTAMP WITH TIME ZONE,
                status TEXT NOT NULL,
                batch_id TEXT NOT NULL,
                rows_inserted INTEGER DEFAULT 0,
                rows_updated INTEGER DEFAULT 0
            );

            CREATE TABLE IF NOT EXISTS table_metadata (
                id SERIAL PRIMARY KEY,
                table_name TEXT NOT NULL,
                processed_at TIMESTAMP,
                total_rows INTEGER DEFAULT 0,
                last_file_processed TEXT,
                schema_snapshot JSONB,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(table_name)
            );

            CREATE TABLE IF NOT EXISTS processed_files (
                id SERIAL PRIMARY KEY,
                file_name TEXT NOT NULL,
                file_hash TEXT NOT NULL,
                status TEXT NOT NULL,
                rows_processed INTEGER DEFAULT 0,
                error_message TEXT,
                processed_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
            );
        """
        )

        # Now we can safely truncate the tables
        await conn.execute(
            """
            TRUNCATE TABLE raw_test_data CASCADE;
            TRUNCATE TABLE change_history CASCADE;
            TRUNCATE TABLE merge_history CASCADE;
            TRUNCATE TABLE table_metadata CASCADE;
            TRUNCATE TABLE processed_files CASCADE;
        """
        )

        # Upload test file to MinIO
        test_file_name = "test_data.csv.gz"
        with open(compressed_csv_file, "rb") as f:
            file_data = f.read()
            minio_client.put_object(
                bucket_name=test_config["s3"]["bucket"],
                object_name=test_file_name,
                data=io.BytesIO(file_data),
                length=len(file_data),
            )

        # Verify file was uploaded
        try:
            minio_client.stat_object(test_config["s3"]["bucket"], test_file_name)
        except Exception as e:
            pytest.fail(f"Failed to upload test file to MinIO: {str(e)}")

        # Initialize config
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
        # Use the same file prefix as the uploaded file
        await pipeline.run(file_prefix=test_file_name, primary_key="id")
        await pipeline.shutdown()
        await pipeline.cleanup()

        # Verify results
        conn = await asyncpg.connect(**test_config["postgres"])

        # 1. Verify raw data
        raw_count = await conn.fetchval("SELECT COUNT(*) FROM raw_test_data")
        assert (
            raw_count == 100
        ), f"Expected 100 rows in raw_test_data, found {raw_count}"

        # 2. Check processed files
        processed_files = await conn.fetch(
            """
            SELECT file_name, status, rows_processed, error_message
            FROM processed_files
            ORDER BY processed_at DESC
        """
        )
        assert (
            len(processed_files) == 1
        ), f"Expected 1 processed file, found {len(processed_files)}"
        assert processed_files[0]["status"] == "COMPLETED"
        assert processed_files[0]["rows_processed"] == 100

        # 3. Examine change history in detail
        change_details = await conn.fetch(
            """
            SELECT 
                change_type,
                COUNT(*) as count,
                COUNT(DISTINCT batch_id) as batch_count,
                MIN(created_at) as first_change,
                MAX(created_at) as last_change
            FROM change_history
            WHERE table_name = 'raw_test_data'
            GROUP BY change_type
            ORDER BY change_type
        """
        )

        print("\nChange History Details:")
        for detail in change_details:
            print(f"Change Type: {detail['change_type']}")
            print(f"Total Changes: {detail['count']}")
            print(f"Unique Batches: {detail['batch_count']}")
            print(f"Time Range: {detail['first_change']} to {detail['last_change']}\n")

        # 4. Check for duplicate batch_ids
        duplicate_batches = await conn.fetch(
            """
            SELECT batch_id, COUNT(*) as count
            FROM change_history
            WHERE table_name = 'raw_test_data'
            GROUP BY batch_id
            HAVING COUNT(*) > 100
            ORDER BY count DESC
        """
        )

        if duplicate_batches:
            print("\nFound duplicate batch records:")
            for batch in duplicate_batches:
                print(f"Batch ID: {batch['batch_id']}, Count: {batch['count']}")

        # 5. Verify final counts
        changes = await conn.fetch(
            """
            SELECT change_type, COUNT(*) as count
            FROM change_history
            WHERE table_name = 'raw_test_data'
            GROUP BY change_type
        """
        )
        changes_dict = {row["change_type"]: row["count"] for row in changes}

        assert changes_dict.get("INSERT", 0) == 100, (
            f"Expected 100 INSERT records, found {changes_dict.get('INSERT', 0)}. "
            f"Full change distribution: {changes_dict}"
        )
        assert (
            changes_dict.get("UPDATE", 0) == 0
        ), f"Expected 0 UPDATE records, found {changes_dict.get('UPDATE', 0)}"

    finally:
        # Cleanup
        if pipeline:
            await pipeline.shutdown()
            await pipeline.cleanup()

        # Clean up the test file from MinIO
        try:
            minio_client.remove_object(test_config["s3"]["bucket"], test_file_name)
        except:
            pass

        if conn:
            await conn.close()
