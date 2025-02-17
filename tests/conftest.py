import asyncio
from typing import AsyncGenerator
import pytest
import pandas as pd
import gzip
import os
from minio import Minio
import asyncpg
from datetime import datetime
import json


@pytest.fixture(scope="session")
def test_config():
    return {
        "postgres": {
            "host": os.getenv("TEST_POSTGRES_HOST", "postgres-test"),
            "port": int(os.getenv("TEST_POSTGRES_PORT", "5432")),
            "user": os.getenv("TEST_POSTGRES_USER", "warehouse_user"),
            "password": os.getenv("TEST_POSTGRES_PASSWORD", "warehouse_password"),
            "database": os.getenv("TEST_POSTGRES_DB", "warehouse_db"),
        },
        "s3": {
            "endpoint": os.getenv("S3_ENDPOINT", "minio-test:9000"),
            "access_key": os.getenv("S3_ACCESS_KEY", "test_access_key"),
            "secret_key": os.getenv("S3_SECRET_KEY", "test_secret_key"),
            "bucket": "test-data-bucket",
        },
    }


@pytest.fixture(scope="function")  # Changed from session to function scope
async def pg_pool(test_config) -> AsyncGenerator[asyncpg.Pool, None]:
    """Create a connection pool for testing"""
    pool = await asyncpg.create_pool(
        host=test_config["postgres"]["host"],
        port=test_config["postgres"]["port"],
        user=test_config["postgres"]["user"],
        password=test_config["postgres"]["password"],
        database=test_config["postgres"]["database"],
        min_size=1,  # Reduced from 2
        max_size=5,  # Reduced from 10
        command_timeout=60,
    )
    try:
        yield pool
    finally:
        await asyncio.sleep(0.1)  # Give time for connections to be returned
        await pool.close()
        await asyncio.sleep(0.1)  # Give time for pool to fully close


@pytest.fixture(scope="session")
def minio_client(test_config):
    client = Minio(
        test_config["s3"]["endpoint"],
        access_key=test_config["s3"]["access_key"],
        secret_key=test_config["s3"]["secret_key"],
        secure=False,
    )

    # Create test bucket if it doesn't exist
    if not client.bucket_exists(test_config["s3"]["bucket"]):
        client.make_bucket(test_config["s3"]["bucket"])

    return client


@pytest.fixture(scope="function")
def sample_csv_data():
    # Create sample data with various data types
    data = {
        "id": range(1, 101),
        "name": [f"User {i}" for i in range(1, 101)],
        "email": [f"user{i}@example.com" for i in range(1, 101)],
        "age": [i % 50 + 20 for i in range(1, 101)],
        "created_at": [datetime.now().isoformat() for _ in range(100)],
        "is_active": [i % 2 == 0 for i in range(1, 101)],
        "tags": [
            json.dumps([f"tag{j}" for j in range(i % 3 + 1)]) for i in range(1, 101)
        ],
        "scores": [f"{{{i},{i+1},{i+2}}}" for i in range(1, 101)],  # Array type
    }
    df = pd.DataFrame(data)
    return df


@pytest.fixture(scope="function")
def compressed_csv_file(sample_csv_data, tmp_path):
    # Create CSV file
    csv_path = tmp_path / "test_data.csv"
    sample_csv_data.to_csv(csv_path, index=False)

    # Compress with gzip
    gz_path = tmp_path / "test_data.csv.gz"
    with open(csv_path, "rb") as f_in:
        with gzip.open(gz_path, "wb") as f_out:
            f_out.write(f_in.read())

    return gz_path


@pytest.fixture(scope="function")
async def clean_test_db(test_config, pg_pool) -> AsyncGenerator[None, None]:
    """Ensure the test database is cleaned before running tests"""
    try:
        async with pg_pool.acquire() as conn:
            async with conn.transaction():
                await conn.execute(
                    """
                    DO $$
                    DECLARE
                        r RECORD;
                    BEGIN
                        FOR r IN (SELECT tablename FROM pg_tables WHERE schemaname = 'public') LOOP
                            EXECUTE 'DROP TABLE IF EXISTS ' || quote_ident(r.tablename) || ' CASCADE';
                        END LOOP;
                    END $$;
                    """
                )
    except Exception as e:
        print(f"Error cleaning database: {e}")
        raise
    yield
