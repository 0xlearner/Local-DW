import json
import polars as pl
from typing import Dict, Any, List
from io import StringIO
import asyncpg
from src.logger import setup_logger

logger = setup_logger("schema_inferrer")


class SchemaInferrer:
    @staticmethod
    def _is_json_string(s: str) -> bool:
        try:
            json.loads(s)
            return True
        except (json.JSONDecodeError, TypeError):
            return False

    @staticmethod
    def _is_array_string(s: str) -> bool:
        # Check if string is in array format like "{item1,item2}" or "[item1,item2]"
        s = s.strip()
        return (s.startswith("{") and s.endswith("}")) or (
            s.startswith("[") and s.endswith("]")
        )

    @staticmethod
    def _detect_array_type(values: List[str]) -> str:
        # Sample non-null values
        sample_values = [v for v in values if v is not None and v != ""][:100]

        if not sample_values:
            return "TEXT[]"  # Default to text array if no samples

        # Check if all values are numeric
        try:
            all(float(v) for v in sample_values)
            return "NUMERIC[]"
        except ValueError:
            pass

        return "TEXT[]"

    @staticmethod
    def infer_schema(csv_data: str) -> Dict[str, Any]:
        try:
            # Convert string data to StringIO object for Polars to read
            csv_buffer = StringIO(csv_data)
            df = pl.read_csv(csv_buffer)
            schema = {}

            # Convert to pandas for easier JSON detection
            pdf = df.to_pandas()

            for column in df.schema.items():
                col_name, dtype = column

                # Get sample of non-null values for type detection
                sample_values = pdf[col_name].dropna().head(100).tolist()

                # Check for JSON or array types first
                if sample_values:
                    # Check if values are JSON
                    if all(
                        isinstance(v, str) and SchemaInferrer._is_json_string(v)
                        for v in sample_values
                    ):
                        pg_type = "JSONB"
                    # Check if values are arrays
                    elif all(
                        isinstance(v, str) and SchemaInferrer._is_array_string(v)
                        for v in sample_values
                    ):
                        pg_type = SchemaInferrer._detect_array_type(sample_values)
                    else:
                        # Map Polars dtypes to PostgreSQL types
                        if dtype in [pl.Int8, pl.Int16, pl.Int32, pl.Int64]:
                            pg_type = "BIGINT"
                        elif dtype in [pl.Float32, pl.Float64]:
                            pg_type = "DOUBLE PRECISION"
                        elif dtype == pl.Boolean:
                            pg_type = "BOOLEAN"
                        elif dtype == pl.Datetime:
                            pg_type = "TIMESTAMP"
                        elif dtype == pl.List:
                            pg_type = "TEXT[]"
                        else:
                            pg_type = "TEXT"
                else:
                    # Default to TEXT for empty columns
                    pg_type = "TEXT"

                schema[col_name] = pg_type

            return schema
        except Exception as e:
            logger.error(f"Error inferring schema: {str(e)}")
            raise

    @staticmethod
    async def create_table_if_not_exists(
        conn: asyncpg.Connection,
        table_name: str,
        schema: Dict[str, Any],
        primary_key: str,
    ) -> None:
        try:
            columns = [f"{col} {dtype}" for col, dtype in schema.items()]
            columns.append(f"CONSTRAINT {table_name}_pkey PRIMARY KEY ({primary_key})")

            create_table_query = f"""
                CREATE TABLE IF NOT EXISTS {table_name} (
                    {', '.join(columns)}
                )
            """
            await conn.execute(create_table_query)
        except Exception as e:
            logger.error(f"Error creating table: {str(e)}")
            raise
