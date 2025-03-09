import os
from dataclasses import dataclass


@dataclass
class Config:
    # S3/MinIO Configuration
    MINIO_ENDPOINT: str = os.getenv("S3_ENDPOINT", "localhost:9000")
    MINIO_ACCESS_KEY: str = os.getenv("S3_ACCESS_KEY", "access_key")
    MINIO_SECRET_KEY: str = os.getenv("S3_SECRET_KEY", "security_key")
    MINIO_BUCKET: str = os.getenv("S3_BUCKET", "data-bucket")

    # PostgreSQL Configuration
    PG_HOST: str = os.getenv("TEST_POSTGRES_HOST", "localhost")
    PG_PORT: int = int(os.getenv("TEST_POSTGRES_PORT", "5432"))
    PG_USER: str = os.getenv("TEST_POSTGRES_USER", "postgres")
    PG_PASSWORD: str = os.getenv("TEST_POSTGRES_PASSWORD", "postgres")
    PG_DATABASE: str = os.getenv("TEST_POSTGRES_DB", "warehouse_db")

    # Pipeline Configuration
    CHUNK_SIZE: int = int(os.getenv("CHUNK_SIZE", "1000"))
    MAX_WORKERS: int = int(os.getenv("MAX_WORKERS", "4"))

    def __post_init__(self):
        """Validate configuration after initialization"""
        required_env_vars = [
            ("S3_ENDPOINT", self.MINIO_ENDPOINT),
            ("S3_ACCESS_KEY", self.MINIO_ACCESS_KEY),
            ("S3_SECRET_KEY", self.MINIO_SECRET_KEY),
            ("S3_BUCKET", self.MINIO_BUCKET),
            ("TEST_POSTGRES_HOST", self.PG_HOST),
            ("TEST_POSTGRES_PORT", str(self.PG_PORT)),
            ("TEST_POSTGRES_USER", self.PG_USER),
            ("TEST_POSTGRES_PASSWORD", self.PG_PASSWORD),
            ("TEST_POSTGRES_DB", self.PG_DATABASE),
        ]

        # Log configuration (without sensitive data)
        print("Current configuration:")
        print(f"MinIO Endpoint: {self.MINIO_ENDPOINT}")
        print(f"MinIO Bucket: {self.MINIO_BUCKET}")
        print(f"PostgreSQL Host: {self.PG_HOST}")
        print(f"PostgreSQL Port: {self.PG_PORT}")
        print(f"PostgreSQL Database: {self.PG_DATABASE}")
        print(f"Chunk Size: {self.CHUNK_SIZE}")
        print(f"Max Workers: {self.MAX_WORKERS}")
