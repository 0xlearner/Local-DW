import os
from minio import Minio
from typing import List


def upload_files_to_minio(
    files: List[str],
    bucket_name: str = "data-bucket",
    minio_endpoint: str = "localhost:9000",
    access_key: str = "access_key",  # Match docker-compose.yml MINIO_ROOT_USER
    secret_key: str = "security_key",  # Match docker-compose.yml MINIO_ROOT_PASSWORD
    secure: bool = False,
) -> None:
    """
    Upload .gz files to MinIO with organized folder structure.

    Args:
        files: List of .gz file paths
        bucket_name: Name of the MinIO bucket
        minio_endpoint: MinIO server endpoint
        access_key: MinIO access key
        secret_key: MinIO secret key
        secure: Use HTTPS if True
    """
    # Initialize MinIO client
    client = Minio(
        endpoint=minio_endpoint,
        access_key=access_key,
        secret_key=secret_key,
        secure=secure,
    )

    # Create bucket if it doesn't exist
    if not client.bucket_exists(bucket_name):
        client.make_bucket(bucket_name)
        print(f"Created bucket: {bucket_name}")

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
            print(f"Successfully uploaded {file_path} to {bucket_name}/{object_name}")
        except Exception as e:
            print(f"Error uploading {file_path}: {str(e)}")


if __name__ == "__main__":
    # Use paths that match the Docker container structure
    files_to_upload = [
        "data/listings.csv.gz",
        "data/reviews.csv.gz",
        "data/calendar.csv.gz",
    ]

    upload_files_to_minio(
        files=files_to_upload,
        bucket_name="data-bucket",
        minio_endpoint="localhost:9000",
        access_key="access_key",  # Match docker-compose.yml MINIO_ROOT_USER
        secret_key="security_key",  # Match docker-compose.yml MINIO_ROOT_PASSWORD
    )
