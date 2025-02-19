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
    ) -> List[Dict[str, Any]]:
        """
        Process a batch of records.

        Args:
            conn: Database connection
            table_name: Name of the target table
            columns: List of column names
            records: List of records to process
            column_types: Dictionary mapping column names to their PostgreSQL types

        Returns:
            List of formatted records ready for database insertion
        """
        formatted_records = []

        for record in records:
            formatted_record = {}
            for col in columns:
                try:
                    value = record.get(col)
                    formatted_value = self.value_formatter.format_value(
                        value=value,
                        col_name=col,
                        schema=column_types
                    )
                    formatted_record[col] = formatted_value
                except Exception as e:
                    self.logger.error(
                        f"Error formatting value for column {col}: {str(e)}")
                    raise
            formatted_records.append(formatted_record)

        return formatted_records

    def generate_sql(
        self,
        table_name: str,
        columns: List[str],
        column_types: Dict[str, str],
        merge_strategy: str,
        primary_key: str,
    ) -> str:
        """Generate SQL statement based on merge strategy"""
        if merge_strategy == "INSERT":
            return self._generate_insert_sql(table_name, columns, column_types)
        elif merge_strategy == "UPDATE":
            return self._generate_update_sql(table_name, columns, column_types, primary_key)
        else:  # MERGE
            return self._generate_merge_sql(table_name, columns, column_types, primary_key)

    def _generate_insert_sql(
        self, table_name: str, columns: List[str], column_types: Dict[str, str]
    ) -> str:
        placeholders = [
            f'${i+1}::{column_types.get(col, "text")}'
            for i, col in enumerate(columns)
        ]
        return f"""
            INSERT INTO {table_name} ({', '.join(columns)})
            VALUES ({', '.join(placeholders)})
        """

    def _generate_update_sql(
        self,
        table_name: str,
        columns: List[str],
        column_types: Dict[str, str],
        primary_key: str,
    ) -> str:
        non_pk_columns = [col for col in columns if col != primary_key]
        update_sets = [
            f"{col} = ${i+2}::{column_types.get(col, 'text')}"
            for i, col in enumerate(non_pk_columns)
        ]
        return f"""
            UPDATE {table_name}
            SET {', '.join(update_sets)}
            WHERE {primary_key} = $1::{column_types.get(primary_key, 'text')}
        """

    def _generate_merge_sql(
        self,
        table_name: str,
        columns: List[str],
        column_types: Dict[str, str],
        primary_key: str,
    ) -> str:
        placeholders = [
            f'${i+1}::{column_types.get(col, "text")}'
            for i, col in enumerate(columns)
        ]
        return f"""
            INSERT INTO {table_name} ({', '.join(columns)})
            VALUES ({', '.join(placeholders)})
            ON CONFLICT ({primary_key})
            DO UPDATE SET {
            ', '.join(f"{col} = EXCLUDED.{col}"
                      for col in columns
                      if col != primary_key)
        }
        """
