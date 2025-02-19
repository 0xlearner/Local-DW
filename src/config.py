import os
from dataclasses import dataclass


@dataclass
class Config:
    MINIO_ENDPOINT: str = os.getenv("S3_ENDPOINT", "minio-test:9000")
    MINIO_ACCESS_KEY: str = os.getenv("S3_ACCESS_KEY", "test_access_key")
    MINIO_SECRET_KEY: str = os.getenv("S3_SECRET_KEY", "test_security_key")
    MINIO_BUCKET: str = os.getenv("S3_BUCKET", "test-data-bucket")

    PG_HOST: str = os.getenv("TEST_POSTGRES_HOST", "postgres-test")
    PG_PORT: int = int(os.getenv("TEST_POSTGRES_PORT", "5432"))
    PG_USER: str = os.getenv("TEST_POSTGRES_USER", "postgres")
    PG_PASSWORD: str = os.getenv("TEST_POSTGRES_PASSWORD", "postgres")
    PG_DATABASE: str = os.getenv("TEST_POSTGRES_DB", "test_warehouse_db")

    BATCH_SIZE: int = int(os.getenv("BATCH_SIZE", "1000"))
    MAX_WORKERS: int = int(os.getenv("MAX_WORKERS", "4"))
