import os
from minio import Minio
from typing import List
from src.config import Config


def upload_test_files_to_minio(
    files: List[str],
    bucket_name: str = "test-data-bucket",
    minio_endpoint: str = "minio-test:9000",
    access_key: str = "test_access_key",
    secret_key: str = "test_secret_key",
    secure: bool = False,
) -> None:
    """
    Upload test .gz files to MinIO with organized folder structure.
    Uses test configuration settings.

    Args:
        files: List of .gz file paths
        bucket_name: Name of the MinIO bucket (defaults to test bucket)
        minio_endpoint: MinIO server endpoint (defaults to test endpoint)
        access_key: MinIO access key (defaults to test access key)
        secret_key: MinIO secret key (defaults to test secret key)
        secure: Use HTTPS if True
    """
    # Initialize MinIO client with test configuration
    client = Minio(
        endpoint=minio_endpoint,
        access_key=access_key,
        secret_key=secret_key,
        secure=secure,
    )

    # Create bucket if it doesn't exist
    if not client.bucket_exists(bucket_name):
        client.make_bucket(bucket_name)
        print(f"Created test bucket: {bucket_name}")

    # Upload each file
    for file_path in files:
        if not file_path.endswith(".gz"):
            print(f"Skipping {file_path}: not a .gz file")
            continue

        # Get the base filename without path
        filename = os.path.basename(file_path)

        # Create folder name from filename (e.g., "listings" from "listings.csv.gz")
        folder_name = filename.split(".")[0]

        # Construct the S3 object path (e.g., "listings/listings.csv.gz")
        object_name = f"{folder_name}/{filename}"

        try:
            # Upload the file
            with open(file_path, "rb") as file_data:
                file_stat = os.stat(file_path)
                client.put_object(
                    bucket_name=bucket_name,
                    object_name=object_name,
                    data=file_data,
                    length=file_stat.st_size,
                )
            print(f"Successfully uploaded test file {file_path} to {bucket_name}/{object_name}")
        except Exception as e:
            print(f"Error uploading test file {file_path}: {str(e)}")


def get_test_config_from_env() -> dict:
    """Get MinIO test configuration from environment variables"""
    return {
        "bucket_name": os.getenv("S3_BUCKET", "test-data-bucket"),
        "minio_endpoint": os.getenv("S3_ENDPOINT", "minio-test:9000"),
        "access_key": os.getenv("S3_ACCESS_KEY", "test_access_key"),
        "secret_key": os.getenv("S3_SECRET_KEY", "test_secret_key"),
    }


if __name__ == "__main__":
    # Use paths that match the Docker container test structure
    test_files_to_upload = [
        "/app/data/listings.csv.gz",
        "/app/data/reviews.csv.gz",
        "/app/data/calendar.csv.gz",
    ]

    # Get test configuration from environment variables
    test_config = get_test_config_from_env()

    upload_test_files_to_minio(
        files=test_files_to_upload,
        **test_config
    )