from typing import Dict, Any, Optional
import asyncpg
import json


class MetadataTracker:
    def __init__(self, conn_params: Dict[str, Any]):
        self.conn_params = conn_params
        self._pool = None

    async def initialize(self):
        """Initialize connection pool"""
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
        pool = await self.get_pool()
        async with pool.acquire() as conn:
            # First create table if it doesn't exist
            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS table_metadata (
                    id SERIAL PRIMARY KEY,
                    table_name TEXT NOT NULL,
                    processed_at TIMESTAMP,
                    total_rows INTEGER DEFAULT 0,
                    last_file_processed TEXT,
                    schema_snapshot JSONB,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(table_name)
                )
            """
            )

            # Remove the ALTER TABLE statement since columns are already defined in CREATE TABLE
            # If you need to add new columns in the future, you can uncomment and modify this block
            """
            # Add schema_snapshot column if it doesn't exist
            await conn.execute(
                DO $$
                BEGIN
                    IF NOT EXISTS (
                        SELECT 1 
                        FROM information_schema.columns 
                        WHERE table_name = 'table_metadata' 
                        AND column_name = 'new_column_name'
                    ) THEN
                        ALTER TABLE table_metadata 
                        ADD COLUMN new_column_name NEW_TYPE;
                    END IF;
                END $$;
            )
            """

            # Table for tracking changes to data
            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS change_history (
                    id SERIAL PRIMARY KEY,
                    table_name TEXT NOT NULL,
                    primary_key_column TEXT NOT NULL,
                    primary_key_value TEXT NOT NULL,
                    column_name TEXT NOT NULL,
                    old_value TEXT,
                    new_value TEXT,
                    change_type TEXT NOT NULL, -- INSERT, UPDATE, DELETE
                    file_name TEXT NOT NULL,
                    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    batch_id TEXT NOT NULL
                )
            """
            )

            # Table for tracking merge operations
            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS merge_history (
                    id SERIAL PRIMARY KEY,
                    table_name TEXT NOT NULL,
                    file_name TEXT NOT NULL,
                    rows_inserted INTEGER DEFAULT 0,
                    rows_updated INTEGER DEFAULT 0,
                    rows_deleted INTEGER DEFAULT 0,
                    started_at TIMESTAMP NOT NULL,
                    completed_at TIMESTAMP,
                    status TEXT NOT NULL, -- IN_PROGRESS, COMPLETED, FAILED
                    error_message TEXT,
                    batch_id TEXT NOT NULL
                )
            """
            )

            # Table for tracking recovery points
            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS recovery_points (
                    id SERIAL PRIMARY KEY,
                    table_name TEXT NOT NULL,
                    file_name TEXT NOT NULL,
                    batch_id TEXT NOT NULL,
                    checkpoint_data JSONB NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    status TEXT NOT NULL, -- ACTIVE, PROCESSED, FAILED
                    retry_count INTEGER DEFAULT 0,
                    last_error TEXT,
                    next_retry_at TIMESTAMP,
                    UNIQUE(table_name, file_name, batch_id)
                )
            """
            )

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
