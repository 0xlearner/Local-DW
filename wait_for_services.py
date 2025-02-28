import asyncio
import os
import time

import asyncpg
from minio import Minio

TEST_POSTGRES_CONFIG = {
    "host": os.getenv(
        "TEST_POSTGRES_HOST", "postgres-test"
    ),  # Changed to match service name
    "port": int(os.getenv("TEST_POSTGRES_PORT", "5432")),
    "user": os.getenv("TEST_POSTGRES_USER", "warehouse_user"),
    "password": os.getenv("TEST_POSTGRES_PASSWORD", "warehouse_password"),
    "database": os.getenv("TEST_POSTGRES_DB", "warehouse_db"),
}

TEST_MINIO_CONFIG = {
    "endpoint": os.getenv(
        "S3_ENDPOINT", "minio-test:9000"
    ),  # Changed to match service name
    "access_key": os.getenv("S3_ACCESS_KEY", "test_access_key"),
    "secret_key": os.getenv("S3_SECRET_KEY", "test_secret_key"),
    "secure": False,
}


async def wait_for_postgres():
    max_attempts = 30
    for attempt in range(max_attempts):
        try:
            print(
                f"Attempting to connect to PostgreSQL with config: {TEST_POSTGRES_CONFIG}"
            )
            conn = await asyncpg.connect(**TEST_POSTGRES_CONFIG)
            await conn.close()
            print("PostgreSQL is ready!")
            return
        except Exception as e:
            print(f"Waiting for PostgreSQL... ({attempt + 1}/{max_attempts}): {str(e)}")
            await asyncio.sleep(1)
    raise Exception("PostgreSQL failed to become ready")


def wait_for_minio():
    max_attempts = 30
    client = Minio(**TEST_MINIO_CONFIG)
    for attempt in range(max_attempts):
        try:
            print(
                f"Attempting to connect to MinIO with endpoint: {TEST_MINIO_CONFIG['endpoint']}"
            )
            client.list_buckets()
            print("MinIO is ready!")
            return
        except Exception as e:
            print(f"Waiting for MinIO... ({attempt + 1}/{max_attempts}): {str(e)}")
            time.sleep(1)
    raise Exception("MinIO failed to become ready")


if __name__ == "__main__":
    print("Starting service checks...")
    asyncio.run(wait_for_postgres())
    wait_for_minio()
    print("All services are ready!")
