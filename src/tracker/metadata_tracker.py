import json
from typing import Any, Dict, Optional

from src.connection_manager import ConnectionManager
from src.logger import setup_logger


class MetadataTracker:
    def __init__(self):
        self.logger = setup_logger("metadata_tracker")
        self._data_loader = None

    def set_data_loader(self, data_loader) -> None:
        """Set the data loader instance to use its serialization method"""
        self._data_loader = data_loader

    async def initialize(self):
        """Verify connection pool exists"""
        ConnectionManager.get_pool()  # Will raise if pool not initialized

    async def update_table_metadata(
        self, table_name: str, file_name: str, total_rows: int, schema: Dict[str, Any]
    ) -> None:
        """
        Update metadata for a table including processing timestamp, row count, and schema.

        Args:
            table_name: Name of the table being processed
            file_name: Name of the last processed file
            total_rows: Total number of rows in the table
            schema: Current schema definition of the table
        """
        try:
            async with ConnectionManager.get_pool().acquire() as conn:
                await conn.execute(
                    """
                    INSERT INTO table_metadata (
                        table_name,
                        processed_at,
                        total_rows,
                        last_file_processed,
                        schema_snapshot,
                        updated_at
                    ) VALUES ($1, CURRENT_TIMESTAMP, $2, $3, $4, CURRENT_TIMESTAMP)
                    ON CONFLICT (table_name) DO UPDATE SET
                        processed_at = CURRENT_TIMESTAMP,
                        total_rows = EXCLUDED.total_rows,
                        last_file_processed = EXCLUDED.last_file_processed,
                        schema_snapshot = EXCLUDED.schema_snapshot,
                        updated_at = CURRENT_TIMESTAMP
                    """,
                    table_name,
                    total_rows,
                    file_name,
                    json.dumps(schema),
                )
        except Exception as e:
            self.logger.error(
                f"Error updating table metadata for {
                              table_name}: {str(e)}"
            )
            raise

    async def record_change(
        self,
        conn,
        table_name: str,
        change_type: str,
        new_record: dict,
        batch_id: str,
        primary_key: str,
        file_name: str,
        old_record: dict = None,
    ) -> None:
        """Record a change in the change_history table"""
        try:
            # Serialize records before JSON encoding
            serialized_new = self._data_loader.serialize_record(new_record)
            serialized_old = (
                self._data_loader.serialize_record(old_record) if old_record else None
            )

            await conn.execute(
                """
                    INSERT INTO change_history (
                        table_name,
                        primary_key_column,
                        primary_key_value,
                        column_name,
                        old_value,
                        new_value,
                        change_type,
                        file_name,
                        batch_id
                    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                    """,
                table_name,
                primary_key,
                str(new_record.get(primary_key)),
                "*",  # Use "*" to indicate entire row change
                json.dumps(serialized_old) if serialized_old else None,
                json.dumps(serialized_new),
                change_type,
                file_name,
                batch_id,
            )
        except Exception as e:
            self.logger.error(f"Error recording change: {str(e)}")
            raise

    async def get_table_metadata(self, table_name: str) -> Optional[Dict[str, Any]]:
        """
        Retrieve metadata for a specific table.

        Args:
            table_name: Name of the table to get metadata for

        Returns:
            Dictionary containing table metadata or None if not found
        """
        try:
            async with ConnectionManager.get_pool().acquire() as conn:
                result = await conn.fetchrow(
                    """
                    SELECT
                        table_name,
                        processed_at,
                        total_rows,
                        last_file_processed,
                        schema_snapshot,
                        created_at,
                        updated_at
                    FROM table_metadata
                    WHERE table_name = $1
                    """,
                    table_name,
                )
                return dict(result) if result else None
        except Exception as e:
            self.logger.error(
                f"Error retrieving table metadata for {
                              table_name}: {str(e)}"
            )
            raise

    async def get_table_schema(self, table_name: str) -> Dict[str, Any]:
        """
        Get the schema for a specific table.

        Args:
            table_name: Name of the table

        Returns:
            Dictionary containing table schema or empty dict if not found
        """
        try:
            metadata = await self.get_table_metadata(table_name)
            if metadata and "schema_snapshot" in metadata:
                return metadata["schema_snapshot"]

            # If no schema found in metadata, get it from the database
            async with ConnectionManager.get_pool().acquire() as conn:
                schema = {}
                result = await conn.fetch(
                    """
                    SELECT column_name, data_type, is_nullable
                    FROM information_schema.columns
                    WHERE table_name = $1
                    """,
                    table_name,
                )

                for row in result:
                    schema[row["column_name"]] = {
                        "type": row["data_type"],
                        "nullable": row["is_nullable"] == "YES",
                    }

                return schema

        except Exception as e:
            self.logger.error(
                f"Error retrieving table schema for {table_name}: {str(e)}"
            )
            # Return empty schema if table doesn't exist yet
            return {}
