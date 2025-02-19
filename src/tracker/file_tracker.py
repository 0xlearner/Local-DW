from typing import Optional
from src.connection_manager import ConnectionManager
from src.logger import setup_logger


class FileTracker:
    def __init__(self):
        self.logger = setup_logger("file_tracker")

    async def initialize(self):
        """Verify connection pool exists"""
        ConnectionManager.get_pool()  # Will raise if pool not initialized

    async def is_file_processed(self, file_name: str, file_hash: str) -> bool:
        async with ConnectionManager.get_pool().acquire() as conn:
            return await self._check_file_processed(conn, file_name, file_hash)

    async def _check_file_processed(
        self, conn, file_name: str, file_hash: str
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
        batch_id: Optional[str] = None,
    ):
        async with ConnectionManager.get_pool().acquire() as conn:
            await conn.execute(
                """
                        INSERT INTO processed_files (
                            file_name, file_hash, status, rows_processed, error_message,
                            batch_id, processed_at, updated_at
                        ) VALUES ($1, $2, $3, $4, $5, $6, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                        ON CONFLICT (file_name)
                        DO UPDATE SET
                            status = EXCLUDED.status,
                            rows_processed = EXCLUDED.rows_processed,
                            error_message = EXCLUDED.error_message,
                            batch_id = EXCLUDED.batch_id,
                            processed_at = CURRENT_TIMESTAMP,
                            updated_at = CURRENT_TIMESTAMP
                        """,
                file_name,
                file_hash,
                status,
                rows_processed,
                error_message,
                batch_id,
            )
