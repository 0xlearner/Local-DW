import pytest
from src.client.s3_client import S3Client
from src.config import Config


@pytest.mark.asyncio
async def test_s3_client_upload_and_list(
    test_config, minio_client, compressed_csv_file
):
    # Configure client
    config = Config()
    config.MINIO_ENDPOINT = test_config["s3"]["endpoint"]
    config.MINIO_ACCESS_KEY = test_config["s3"]["access_key"]
    config.MINIO_SECRET_KEY = test_config["s3"]["secret_key"]
    config.MINIO_BUCKET = test_config["s3"]["bucket"]

    client = S3Client(config)

    # Upload test file
    with open(compressed_csv_file, "rb") as f:
        minio_client.put_object(
            config.MINIO_BUCKET, "test_data.csv.gz", f, f.seek(0, 2)
        )

    # List files
    files = list(client.list_files())
    assert len(files) == 1
    assert files[0] == "test_data.csv.gz"

    # Read file
    content = client.read_gz_file("test_data.csv.gz")
    assert content.startswith("id,name,email")
