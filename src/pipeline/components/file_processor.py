import hashlib
from typing import Tuple

import polars as pl

from src.client.s3_client import S3Client
from src.logger import setup_logger
from src.schema_inferrer.schema_infer import SchemaInferrer
from src.validator.data_validation import DataValidator


class FileProcessor:
    def __init__(
        self,
        s3_client: S3Client,
        schema_inferrer: SchemaInferrer,
        data_validator: DataValidator,
    ):
        self.s3_client = s3_client
        self.schema_inferrer = schema_inferrer
        self.data_validator = data_validator
        self.logger = setup_logger("file_processor")

    async def process_file(
        self, file_name: str, table_name: str
    ) -> Tuple[pl.DataFrame, str, dict]:
        """Process a file and return the DataFrame, file hash, and schema"""
        csv_data = await self._read_and_validate_file(file_name)
        file_hash = hashlib.md5(csv_data.getvalue()).hexdigest()

        df = self._prepare_dataframe(csv_data)

        # Get schema for validation
        schema = {
            col: self.schema_inferrer.infer_pg_type(df[col].dtype) for col in df.columns
        }

        # Validate schema compatibility with existing table
        is_compatible = await self.schema_inferrer.validate_schema_compatibility(
            table_name, schema
        )
        if not is_compatible:
            raise ValueError(f"Schema incompatibility detected for table {table_name}")

        # Validate the data against the schema
        if not self.data_validator.validate_data(df, schema):
            errors = self.data_validator.validation_errors
            error_msg = "\n".join([f"{e['type']}: {e['message']}" for e in errors])
            raise ValueError(f"Data validation failed:\n{error_msg}")

        return df, file_hash, schema

    async def _read_and_validate_file(self, file_name: str):
        """Read and validate file from S3"""
        csv_data = await self.s3_client.read_file(file_name)
        return csv_data

    def _prepare_dataframe(self, csv_data) -> pl.DataFrame:
        """Convert CSV data to Polars DataFrame"""
        return pl.read_csv(csv_data)
