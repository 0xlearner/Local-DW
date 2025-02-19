from datetime import datetime
import json
import polars as pl
from typing import Dict, Any, List, Optional, Tuple, Union
from io import StringIO, BytesIO
from src.connection_manager import ConnectionManager
from src.logger import setup_logger


class SchemaInferrer:
    def __init__(self):
        self.logger = setup_logger("schema_inferrer")
        self._datetime_formats = [
            "%Y-%m-%d",
            "%Y-%m-%dT%H:%M:%S",
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%dT%H:%M:%S.%f",
            "%Y-%m-%d %H:%M:%S.%f",
        ]

    async def initialize(self):
        """Verify connection pool exists"""
        ConnectionManager.get_pool()

    def _is_json_string(self, s: str) -> bool:
        """Check if a string is valid JSON."""
        try:
            json.loads(s)
            return True
        except (json.JSONDecodeError, TypeError):
            return False

    def _is_array_string(self, s: str) -> bool:
        """Check if string represents a PostgreSQL array."""
        s = s.strip()
        return (s.startswith("{") and s.endswith("}")) or (
            s.startswith("[") and s.endswith("]")
        )

    def _detect_array_type(self, values: List[str]) -> str:
        """
        Detect the type of array elements.

        Args:
            values: List of string values to analyze

        Returns:
            PostgreSQL array type (TEXT[] or NUMERIC[])
        """
        # Sample non-null values
        sample_values = [v for v in values if v is not None and v != ""][:100]

        if not sample_values:
            return "TEXT[]"

        # Check if all values are numeric
        try:
            all(float(v) for v in sample_values)
            return "NUMERIC[]"
        except ValueError:
            return "TEXT[]"

    def _try_parse_datetime(self, value: str) -> Optional[datetime]:
        """
        Attempt to parse a string as datetime using multiple formats.

        Args:
            value: String to parse as datetime

        Returns:
            datetime object if successful, None otherwise
        """
        if not isinstance(value, str):
            return None

        # Handle ISO format with timezone
        if "T" in value:
            try:
                # Replace 'Z' timezone indicator with +00:00
                if value.endswith("Z"):
                    value = value[:-1] + "+00:00"
                return datetime.fromisoformat(value)
            except ValueError:
                pass

        # Try other common formats
        for fmt in self._datetime_formats:
            try:
                return datetime.strptime(value, fmt)
            except ValueError:
                continue
        return None

    def _infer_column_type(
        self, col_name: str, sample_values: List[Any], dtype: Any
    ) -> Tuple[str, bool]:
        """
        Infer PostgreSQL type for a column based on name and sample values.

        Args:
            col_name: Name of the column
            sample_values: List of sample values from the column
            dtype: Polars dtype of the column

        Returns:
            Tuple of (PostgreSQL type, is_nullable)
        """
        # Check for datetime patterns in column name
        is_datetime_column = any(
            suffix in col_name.lower()
            for suffix in ["_at", "_date", "_time", "_timestamp"]
        )

        # Get non-null samples
        valid_samples = [s for s in sample_values if s is not None and s != ""]
        is_nullable = len(valid_samples) < len(sample_values)

        if not valid_samples:
            return "TEXT", is_nullable

        # Check for datetime values
        if is_datetime_column and isinstance(valid_samples[0], str):
            if self._try_parse_datetime(valid_samples[0]):
                return "TIMESTAMP WITH TIME ZONE", is_nullable

        # Check for JSON or Array types
        if isinstance(valid_samples[0], str):
            if all(self._is_json_string(s) for s in valid_samples[:5]):
                return "JSONB", is_nullable
            if all(self._is_array_string(s) for s in valid_samples[:5]):
                return self._detect_array_type(valid_samples), is_nullable

        # Map Polars types to PostgreSQL types
        if dtype == pl.Int64:
            return "BIGINT", is_nullable
        elif dtype == pl.Float64:
            return "NUMERIC", is_nullable
        elif dtype == pl.Boolean:
            return "BOOLEAN", is_nullable
        elif dtype == pl.Datetime:
            return "TIMESTAMP WITH TIME ZONE", is_nullable
        elif dtype == pl.List:
            return "TEXT[]", is_nullable

        return "TEXT", is_nullable

    def infer_schema(self, csv_data: Union[str, BytesIO], file_name: str) -> Dict[str, str]:
        """
        Infer PostgreSQL schema from CSV data.

        Args:
            csv_data: CSV content as string or BytesIO object

        Returns:
            Dictionary mapping column names to PostgreSQL types
        """
        try:
            # Handle BytesIO input
            if isinstance(csv_data, BytesIO):
                csv_data.seek(0)
                csv_content = csv_data.read().decode('utf-8')
                csv_buffer = StringIO(csv_content)
            else:
                csv_buffer = StringIO(csv_data)

            # Read CSV into Polars DataFrame
            df = pl.read_csv(csv_buffer)

            if df.is_empty():
                raise ValueError(f"No data found in file {file_name}")

            # Convert to pandas for additional type detection
            pdf = df.to_pandas()

            schema = {}

            for column in df.schema.items():
                col_name, dtype = column

                # Get sample values for type detection
                sample_values = pdf[col_name].dropna().head(100).tolist()

                # Infer column type
                pg_type, is_nullable = self._infer_column_type(
                    col_name, sample_values, dtype
                )

                schema[col_name] = pg_type

            return schema

        except Exception as e:
            self.logger.error(f"Error inferring schema: {str(e)}")
            raise

    async def create_table_if_not_exists(
        self,
        table_name: str,
        schema: Dict[str, str],
        primary_key: str,
    ) -> None:
        """
        Create table if it doesn't exist with the specified schema.

        Args:
            table_name: Name of the table to create
            schema: Dictionary mapping column names to PostgreSQL types
            primary_key: Name of the primary key column
        """
        try:
            # Properly quote the table name to handle special characters
            quoted_table_name = f'"{table_name}"'

            # Build column definitions
            columns = []

            # Filter out created_at and updated_at if they exist in the schema
            filtered_schema = {k: v for k, v in schema.items()
                               if k not in ('created_at', 'updated_at')}

            for column_name, data_type in filtered_schema.items():
                quoted_column = f'"{column_name}"'
                if column_name == primary_key:
                    columns.append(f"{quoted_column} {data_type} PRIMARY KEY")
                else:
                    columns.append(f"{quoted_column} {data_type}")

            # Add created_at and updated_at columns
            columns.extend([
                "created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP",
                "updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP"
            ])

            # Create table
            create_table_sql = f"""
                CREATE TABLE IF NOT EXISTS {quoted_table_name} (
                    {','.join(columns)}
                );
            """

            # Create updated_at trigger
            trigger_sql = f"""
                CREATE OR REPLACE FUNCTION update_updated_at_column()
                RETURNS TRIGGER AS $$
                BEGIN
                    NEW.updated_at = CURRENT_TIMESTAMP;
                    RETURN NEW;
                END;
                $$ language 'plpgsql';

                DROP TRIGGER IF EXISTS update_{table_name}_updated_at ON {quoted_table_name};

                CREATE TRIGGER update_{table_name}_updated_at
                    BEFORE UPDATE ON {quoted_table_name}
                    FOR EACH ROW
                    EXECUTE FUNCTION update_updated_at_column();
            """

            async with ConnectionManager.get_pool().acquire() as conn:
                await conn.execute(create_table_sql)
                await conn.execute(trigger_sql)

            self.logger.info(f"Successfully created table {table_name}")

        except Exception as e:
            self.logger.error(
                f"Error creating table {table_name}: {str(e)}"
            )
            raise

    async def validate_schema_compatibility(
        self,
        table_name: str,
        new_schema: Dict[str, str],
    ) -> bool:
        """
        Validate if new schema is compatible with existing table schema.

        Args:
            table_name: Name of the table to check
            new_schema: New schema to validate

        Returns:
            True if schemas are compatible, False otherwise
        """
        try:
            async with ConnectionManager.get_pool().acquire() as conn:
                # Get existing schema
                existing_schema = {}
                rows = await conn.fetch(
                    """
                    SELECT column_name, data_type, udt_name
                    FROM information_schema.columns
                    WHERE table_name = $1
                    """,
                    table_name,
                )

                for row in rows:
                    col_type = (
                        row["data_type"]
                        if row["data_type"] != "USER-DEFINED"
                        else row["udt_name"]
                    )
                    existing_schema[row["column_name"]] = col_type.upper()

                # Compare schemas
                for col, new_type in new_schema.items():
                    if col not in existing_schema:
                        self.logger.warning(
                            f"New column {col} not in existing schema for {
                                table_name}"
                        )
                        return False

                    if existing_schema[col].upper() != new_type.upper():
                        self.logger.warning(
                            f"Type mismatch for column {col} in {table_name}: "
                            f"existing={existing_schema[col]}, new={new_type}"
                        )
                        return False

                return True

        except Exception as e:
            self.logger.error(
                f"Error validating schema for {table_name}: {str(e)}"
            )
            raise
