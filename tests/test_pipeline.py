import gzip
import json
import os
import asyncpg
import pytest
import io
import polars as pl
from src.pipeline.data_pipeline import Pipeline
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

        # Verify results using a single connection
        async with pg_pool.acquire() as conn:
            # All database operations should be within this block
            raw_count = await conn.fetchval("SELECT COUNT(*) FROM raw_test_data")
            assert (
                raw_count == 100
            ), f"Expected 100 rows in raw_test_data, found {raw_count}"

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
                print(
                    f"Time Range: {detail['first_change']} to {
                        detail['last_change']}\n"
                )

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
                    print(
                        f"Batch ID: {batch['batch_id']}, Count: {
                            batch['count']}"
                    )

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
                f"Expected 100 INSERT records, found {
                    changes_dict.get('INSERT', 0)}. "
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


@pytest.mark.asyncio
async def test_comprehensive_incremental_load(
    test_config, minio_client, pg_pool, clean_test_db, sample_csv_data, tmp_path
):
    """Test comprehensive incremental load scenarios"""
    target_table = "raw_data"
    pipeline = None
    batch_ids = []

    try:
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

        # Initial load: first 50 rows - add ID offset to ensure unique IDs
        initial_data = sample_csv_data.slice(0, 50).with_columns(pl.col("id") + 1000)
        initial_csv = "initial_load.csv.gz"

        # New records: next 25 rows with different ID offset
        new_data = sample_csv_data.slice(50, 25).with_columns(pl.col("id") + 2000)
        new_records_csv = "new_records.csv.gz"

        # Updates: modify first 25 rows - use SAME ID offset as initial_data
        updates_data = initial_data.slice(0, 25).with_columns([pl.col("age") + 100])
        updates_csv = "updates.csv.gz"

        # Create and upload files
        for data, filename in [
            (initial_data, initial_csv),
            (new_data, new_records_csv),
            (updates_data, updates_csv),
        ]:
            temp_csv = tmp_path / f"{filename}.temp"
            data.write_csv(temp_csv)

            compressed_path = tmp_path / filename
            with open(temp_csv, "rb") as f_in:
                with gzip.open(compressed_path, "wb") as f_out:
                    f_out.write(f_in.read())

            with open(compressed_path, "rb") as f:
                file_data = f.read()
                minio_client.put_object(
                    bucket_name=test_config["s3"]["bucket"],
                    object_name=filename,
                    data=io.BytesIO(file_data),
                    length=len(file_data),
                )

        # Run initial load and capture batch ID
        initial_batch_id = await pipeline.run(
            file_prefix=initial_csv,
            primary_key="id",
            merge_strategy="INSERT",
            target_table=target_table,
        )
        batch_ids.append(initial_batch_id)

        # Run new records and capture batch ID
        new_records_batch_id = await pipeline.run(
            file_prefix=new_records_csv,
            primary_key="id",
            merge_strategy="INSERT",
            target_table=target_table,
        )
        batch_ids.append(new_records_batch_id)

        # Run updates and capture batch ID
        updates_batch_id = await pipeline.run(
            file_prefix=updates_csv,
            primary_key="id",
            merge_strategy="UPDATE",
            target_table=target_table,
        )
        batch_ids.append(updates_batch_id)

        # Get actual metrics from database
        db_summary = await pipeline.get_load_summary(target_table)

        # Generate and save the report
        report_path = "/app/reports/load_report.json"
        await pipeline.save_load_report(
            str(report_path), batch_ids=batch_ids, table_name=target_table
        )

        # Verify report exists and contains expected data
        assert os.path.exists(report_path)
        with open(report_path) as f:
            report = json.load(f)

        # Verify report structure and contents using actual metrics
        assert "report_generated_at" in report
        assert "generated_by" in report
        assert report["generated_by"] == "0xlearner"
        assert report["table_name"] == target_table

        # Compare report summary with database metrics
        assert (
            report["summary"]["total_files_processed"]
            == db_summary["total_files_processed"]
        )
        assert (
            report["summary"]["total_records_processed"]
            == db_summary["total_records_processed"]
        )
        assert report["summary"]["total_inserts"] == db_summary["total_inserts"]
        assert report["summary"]["total_updates"] == db_summary["total_updates"]

        # Additional verification using change history
        assert db_summary["change_history"]["INSERT"] == db_summary["total_inserts"]
        assert db_summary["change_history"]["UPDATE"] == db_summary["total_updates"]

        # Verify expected counts based on our test data
        assert db_summary["total_inserts"] == 75  # 50 initial + 25 new records
        assert db_summary["total_updates"] == 25  # 25 updates

        update_verification = await pipeline.verify_updates(
            table_name=target_table,
            id_range=(1000, 1025),
            verifications=[
                {
                    "column": "age",
                    "condition": "> 100",
                    "message": "Age increased by 100",
                }
            ],
        )

        assert (
            update_verification
        ), "Update verification failed - some ages were not updated correctly"

        # Log the actual numbers for transparency
        print(f"\nActual metrics from database:")
        print(f"Files processed: {db_summary['total_files_processed']}")
        print(f"Records processed: {db_summary['total_records_processed']}")
        print(f"Inserts: {db_summary['total_inserts']}")
        print(f"Updates: {db_summary['total_updates']}")
        print(f"Failures: {db_summary['total_failures']}")

    finally:
        if pipeline:
            await pipeline.shutdown()
            await pipeline.cleanup()

        # Clean up test files from MinIO
        for file_name in [initial_csv, new_records_csv, updates_csv]:
            try:
                minio_client.remove_object(test_config["s3"]["bucket"], file_name)
            except:
                pass

        # Clean up the target table
        async with pg_pool.acquire() as conn:
            await conn.execute(f"DROP TABLE IF EXISTS {target_table}")


@pytest.mark.asyncio
async def test_real_data_pipeline(test_config, minio_client, pg_pool, clean_test_db):
    """Test pipeline with real data from local directory"""
    pipeline = None
    target_table = "raw_real_data"
    # Assuming CSV is mounted in /app/data
    local_csv_path = "/app/data/listings.csv"
    s3_file_name = "listings.csv.gz"

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
            primary_key="id",  # Adjust based on your CSV structure
            merge_strategy="INSERT",
            target_table=target_table,
        )

        # Verify results
        async with pg_pool.acquire() as conn:
            # Check total rows
            db_count = await conn.fetchval(f"SELECT COUNT(*) FROM {target_table}")
            assert (
                db_count == total_csv_rows
            ), f"Row count mismatch. CSV: {total_csv_rows}, DB: {db_count}"

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
                processed_file["status"] == "COMPLETED"
            ), f"File processing failed: {processed_file['error_message']}"
            assert processed_file["rows_processed"] == total_csv_rows

            # Check pipeline metrics
            metrics = await conn.fetchrow(
                """
                SELECT rows_processed, rows_inserted, processing_status
                FROM pipeline_metrics
                WHERE batch_id = $1
            """,
                batch_id,
            )

            assert metrics["processing_status"] == "COMPLETED"
            assert metrics["rows_processed"] == total_csv_rows
            assert metrics["rows_inserted"] == total_csv_rows

            # Sample data verification
            # Adjust column names based on your CSV structure
            sample_data = await conn.fetch(
                f"""
                SELECT *
                FROM {target_table}
                LIMIT 5
            """
            )
            assert len(sample_data) > 0, "No data found in target table"

            # Get and print load summary
            db_summary = await pipeline.get_load_summary(target_table)

            # Generate and save the report
            report_path = "/app/reports/load_report.json"
            await pipeline.save_load_report(
                str(report_path), batch_ids=batch_id, table_name=target_table
            )
            print("\nLoad Summary:")
            print(
                f"Total files processed: {
                  db_summary['total_files_processed']}"
            )
            print(
                f"Total records processed: {
                  db_summary['total_records_processed']}"
            )
            print(f"Total inserts: {db_summary['total_inserts']}")
            print(f"Total updates: {db_summary['total_updates']}")
            print(f"Total failures: {db_summary['total_failures']}")

    finally:
        if pipeline:
            await pipeline.shutdown()

        # Clean up test files
        try:
            minio_client.remove_object(test_config["s3"]["bucket"], s3_file_name)
            os.remove(compressed_path)
        except:
            pass

        # Clean up the target table
        async with pg_pool.acquire() as conn:
            await conn.execute(f"DROP TABLE IF EXISTS {target_table}")
