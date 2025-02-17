import gzip
from io import BytesIO
from typing import Generator
import minio
from src.config import Config
from src.logger import setup_logger

logger = setup_logger("s3_client")


class S3Client:
    def __init__(self, config: Config):
        self.client = minio.Minio(
            config.MINIO_ENDPOINT,
            access_key=config.MINIO_ACCESS_KEY,
            secret_key=config.MINIO_SECRET_KEY,
            secure=False,
        )
        self.bucket = config.MINIO_BUCKET

    def list_files(self, prefix: str = "") -> Generator[str, None, None]:
        try:
            objects = self.client.list_objects(self.bucket, prefix=prefix)
            for obj in objects:
                if obj.object_name.endswith(".csv.gz"):
                    yield obj.object_name
        except Exception as e:
            logger.error(f"Error listing files: {str(e)}")
            raise

    def read_gz_file(self, file_name: str) -> BytesIO:
        """
        Read and decompress a gzipped file from S3, returning a BytesIO object
        containing the uncompressed data.
        """
        response = None
        try:
            response = self.client.get_object(self.bucket, file_name)
            gz_data = BytesIO(response.read())
            with gzip.GzipFile(fileobj=gz_data, mode="rb") as gz_file:
                # Create a new BytesIO object with the uncompressed data
                uncompressed_data = BytesIO(gz_file.read())
                uncompressed_data.seek(0)  # Reset position to start
                return uncompressed_data
        except Exception as e:
            logger.error(f"Error reading file {file_name}: {str(e)}")
            raise
        finally:
            if response:
                response.close()
                response.release_conn()