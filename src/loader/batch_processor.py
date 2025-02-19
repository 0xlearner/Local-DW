import asyncpg
from typing import List, Dict, Any
from .value_formatter import ValueFormatter
from src.logger import setup_logger


class BatchProcessor:
    def __init__(self, value_formatter: ValueFormatter):
        self.value_formatter = value_formatter
        self.logger = setup_logger("batch_processor")

    async def process_batch(
        self,
        conn: asyncpg.Connection,
        table_name: str,
        columns: List[str],
        records: List[Dict],
        column_types: Dict[str, str],
        batch_number: int,
        total_batches: int,
    ) -> List[Dict[str, Any]]:
        """
        Process a batch of records.

        Args:
            conn: Database connection
            table_name: Name of the target table
            columns: List of column names
            records: List of records to process
            column_types: Dictionary mapping column names to their PostgreSQL types
            batch_number: Current batch number
            total_batches: Total number of batches

        Returns:
            List of formatted records ready for database insertion
        """
        self.logger.info(
            f"Processing batch {batch_number}/{total_batches} "
            f"with {len(records)} records for {table_name}"
        )
        formatted_records = []

        for record in records:
            formatted_record = {}
            for col in columns:
                try:
                    value = record.get(col)
                    formatted_value = self.value_formatter.format_value(
                        value=value, col_name=col, schema=column_types
                    )
                    formatted_record[col] = formatted_value
                except Exception as e:
                    self.logger.error(
                        f"Error formatting value for column {col}: {str(e)}"
                    )
                    raise
            formatted_records.append(formatted_record)

        self.logger.info(f"Successfully formatted {
                         len(formatted_records)} records")
        return formatted_records

    def generate_sql(
        self,
        table_name: str,
        columns: List[str],
        column_types: Dict[str, str],
        merge_strategy: str,
        primary_key: str,
    ) -> str:
        """Generate SQL for inserting or updating records"""
        placeholders = []
        for i, col in enumerate(columns, start=1):
            pg_type = column_types.get(col, "text")

            # Default to text[] for array types if base type not specified
            if pg_type.upper() == "ARRAY":
                placeholders.append(f"${i}::text[]")
            # Handle explicit array types (e.g., "text[]", "integer[]")
            elif "[]" in pg_type:
                base_type = pg_type.replace("[]", "")
                placeholders.append(f"${i}::{base_type}[]")
            else:
                placeholders.append(f"${i}::{pg_type}")

        if merge_strategy == "INSERT":
            sql = f"""
                INSERT INTO {table_name} ({', '.join(columns)})
                VALUES ({', '.join(placeholders)})
            """
        else:  # MERGE strategy
            set_clause = ", ".join(
                f"{col} = EXCLUDED.{col}" for col in columns if col != primary_key
            )
            sql = f"""
                INSERT INTO {table_name} ({', '.join(columns)})
                VALUES ({', '.join(placeholders)})
                ON CONFLICT ({primary_key})
                DO UPDATE SET {set_clause}
            """

        return sql
