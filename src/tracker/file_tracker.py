from typing import Any, Dict, Optional

import asyncpg


class FileTracker:
    def __init__(self, conn_params: Dict[str, Any]):
        self.conn_params = conn_params

    async def initialize_tracking_table(self, conn: asyncpg.Connection) -> None:
        """Initialize the file tracking table."""
        await conn.execute(
            """
            DROP TABLE IF EXISTS processed_files;
            CREATE TABLE processed_files (
                id SERIAL PRIMARY KEY,
                file_name VARCHAR(255) NOT NULL,
                file_hash VARCHAR(64),
                status VARCHAR(20),
                rows_processed INTEGER,
                error_message TEXT,
                processed_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                CONSTRAINT processed_files_file_name_key UNIQUE (file_name)
            );
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
