from datetime import datetime
from typing import Optional

from src.connection_manager import ConnectionManager


class FileTracker:
    async def mark_file_processed(
        self,
        file_name: str,
        batch_id: str,
        status: str,
        rows_processed: int,
        file_hash: Optional[str] = None,
        error_message: Optional[str] = None,
    ) -> None:
        """
        Mark a file as processed with the given status and details
        """
        async with ConnectionManager.get_pool().acquire() as conn:
            await conn.execute(
                """
                INSERT INTO bronze.processed_files (
                    file_name, file_hash, status, rows_processed,
                    error_message, batch_id, processed_at
                ) VALUES ($1, $2, $3, $4, $5, $6, $7)
                ON CONFLICT (file_name, file_hash)
                DO UPDATE SET
                    status = EXCLUDED.status,
                    rows_processed = EXCLUDED.rows_processed,
                    error_message = EXCLUDED.error_message,
                    batch_id = EXCLUDED.batch_id,
                    processed_at = EXCLUDED.processed_at
                """,
                file_name,
                file_hash,
                status,
                rows_processed,
                error_message,
                batch_id,
                datetime.now(),
            )

    async def is_file_processed(
        self, file_name: str, file_hash: Optional[str] = None
    ) -> bool:
        """
        Check if a file has been successfully processed before
        """
        async with ConnectionManager.get_pool().acquire() as conn:
            where_clause = "file_name = $1"
            params = [file_name]

            if file_hash:
                where_clause += " AND file_hash = $2"
                params.append(file_hash)

            result = await conn.fetchrow(
                f"""
                SELECT status
                FROM bronze.processed_files
                WHERE {where_clause}
                AND status = 'COMPLETED'
                """,
                *params,
            )
            return result is not None

    async def get_processing_status(self, batch_id: str) -> Optional[dict]:
        """
        Get the processing status for a specific batch
        """
        async with ConnectionManager.get_pool().acquire() as conn:
            return await conn.fetchrow(
                """
                SELECT
                    file_name,
                    status,
                    rows_processed,
                    error_message,
                    processed_at
                FROM bronze.processed_files
                WHERE batch_id = $1
                """,
                batch_id,
            )
