from typing import Any, Dict, Optional

import asyncpg


class FileTracker:
    def __init__(self, conn_params: Dict[str, Any]):
        self.conn_params = conn_params

    async def initialize_tracking_table(self):
        async with asyncpg.create_pool(**self.conn_params) as pool:
            async with pool.acquire() as conn:
                await conn.execute(
                    """
                    CREATE TABLE IF NOT EXISTS processed_files (
                        id SERIAL PRIMARY KEY,
                        file_name TEXT NOT NULL UNIQUE,
                        processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        file_hash TEXT NOT NULL,
                        status TEXT NOT NULL,
                        error_message TEXT,
                        rows_processed INTEGER DEFAULT 0
                    )
                """
                )

    async def is_file_processed(self, file_name: str, file_hash: str) -> bool:
        async with asyncpg.create_pool(**self.conn_params) as pool:
            async with pool.acquire() as conn:
                result = await conn.fetchrow(
                    """
                    SELECT * FROM processed_files 
                    WHERE file_name = $1 AND file_hash = $2
                    AND status = 'COMPLETED'
                """,
                    file_name,
                    file_hash,
                )
                return result is not None

    async def mark_file_processed(
        self,
        file_name: str,
        file_hash: str,
        status: str,
        rows_processed: int,
        error_message: Optional[str] = None,
    ):
        async with asyncpg.create_pool(**self.conn_params) as pool:
            async with pool.acquire() as conn:
                await conn.execute(
                    """
                    INSERT INTO processed_files (
                        file_name, file_hash, status, rows_processed, error_message
                    ) VALUES ($1, $2, $3, $4, $5)
                    ON CONFLICT (file_name) DO UPDATE SET
                        file_hash = EXCLUDED.file_hash,
                        status = EXCLUDED.status,
                        rows_processed = EXCLUDED.rows_processed,
                        error_message = EXCLUDED.error_message,
                        processed_at = CURRENT_TIMESTAMP
                """,
                    file_name,
                    file_hash,
                    status,
                    rows_processed,
                    error_message,
                )
