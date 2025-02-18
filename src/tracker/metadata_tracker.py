from typing import Dict, Any, Optional
import asyncpg
import json


class MetadataTracker:
    def __init__(self, conn_params: Dict[str, Any]):
        self.conn_params = conn_params
        self._pool = None

    def set_pool(self, pool: asyncpg.Pool):
        """Set the connection pool"""
        self._pool = pool

    async def initialize(self):
        """Initialize connection pool"""
        if self._pool is None:
            self._pool = await asyncpg.create_pool(**self.conn_params)

    async def get_pool(self):
        """Get or create connection pool"""
        if self._pool is None:
            await self.initialize()
        return self._pool

    async def close(self):
        """Close the connection pool"""
        if self._pool:
            await self._pool.close()
            self._pool = None

    async def initialize_metadata_tables(self):
        """This method is kept for backward compatibility but no longer creates tables"""
        pass  # Tables are now created by Pipeline._initialize_infrastructure_tables

    async def record_change(
        self,
        table_name: str,
        primary_key_column: str,
        primary_key_value: str,
        column_name: str,
        old_value: Optional[str],
        new_value: Optional[str],
        change_type: str,
        file_name: str,
        batch_id: str,
    ):
        pool = await self.get_pool()
        async with pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO change_history (
                    table_name, primary_key_column, primary_key_value,
                    column_name, old_value, new_value, change_type,
                    file_name, batch_id
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            """,
                table_name,
                primary_key_column,
                primary_key_value,
                column_name,
                old_value,
                new_value,
                change_type,
                file_name,
                batch_id,
            )

    async def update_table_metadata(
        self, table_name: str, file_name: str, total_rows: int, schema: Dict[str, Any]
    ):
        pool = await self.get_pool()
        async with pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO table_metadata (
                    table_name, processed_at, total_rows,
                    last_file_processed, schema_snapshot, updated_at
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
