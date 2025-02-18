from typing import Any, Dict, Optional
import asyncpg


class FileTracker:
    def __init__(self, conn_params: Dict[str, Any]):
        self.conn_params = conn_params
        self._pool = None

    def set_pool(self, pool: asyncpg.Pool):
        """Set the connection pool"""
        self._pool = pool

    async def initialize_tracking_table(self, conn: asyncpg.Connection) -> None:
        """This method is kept for backward compatibility but no longer creates tables"""
        pass  # Tables are now created by Pipeline._initialize_infrastructure_tables

    async def is_file_processed(self, file_name: str, file_hash: str) -> bool:
        if self._pool is None:
            async with asyncpg.create_pool(**self.conn_params) as pool:
                async with pool.acquire() as conn:
                    return await self._check_file_processed(conn, file_name, file_hash)
        else:
            async with self._pool.acquire() as conn:
                return await self._check_file_processed(conn, file_name, file_hash)

    async def _check_file_processed(
        self, conn: asyncpg.Connection, file_name: str, file_hash: str
    ) -> bool:
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
        if self._pool is None:
            async with asyncpg.create_pool(**self.conn_params) as pool:
                async with pool.acquire() as conn:
                    await self._mark_file_processed_in_db(
                        conn,
                        file_name,
                        file_hash,
                        status,
                        rows_processed,
                        error_message,
                    )
        else:
            async with self._pool.acquire() as conn:
                await self._mark_file_processed_in_db(
                    conn, file_name, file_hash, status, rows_processed, error_message
                )

    async def _mark_file_processed_in_db(
        self,
        conn: asyncpg.Connection,
        file_name: str,
        file_hash: str,
        status: str,
        rows_processed: int,
        error_message: Optional[str] = None,
    ):
        await conn.execute(
            """
            INSERT INTO processed_files (
                file_name, file_hash, status, rows_processed, error_message
            ) VALUES ($1, $2, $3, $4, $5)
            ON CONFLICT (file_name) DO UPDATE
            SET file_hash = $2,
                status = $3,
                rows_processed = $4,
                error_message = $5,
                processed_at = CURRENT_TIMESTAMP
            """,
            file_name,
            file_hash,
            status,
            rows_processed,
            error_message,
        )
