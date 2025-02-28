import json
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List

from src.connection_manager import ConnectionManager
from src.logger import setup_logger


class RecoveryManager:
    def __init__(self):
        self.logger = setup_logger("recovery_manager")
        self.max_retries = 3

    async def initialize(self):
        """Verify connection pool exists"""
        ConnectionManager.get_pool()  # Will raise if pool not initialized

    async def create_recovery_point(
        self,
        table_name: str,
        file_name: str,
        batch_id: str,
        checkpoint_data: Dict[str, Any],
    ) -> None:
        """Create a recovery point for a batch operation"""
        async with ConnectionManager.get_pool().acquire() as conn:
            await conn.execute(
                """
                INSERT INTO recovery_points (
                    table_name, file_name, batch_id, checkpoint_data, status
                ) VALUES ($1, $2, $3, $4, $5)
                """,
                table_name,
                file_name,
                batch_id,
                checkpoint_data,
                "PENDING"
            )

    async def update_recovery_point(
        self,
        batch_id: str,
        status: str,
        error: str = None,
        retry_count: int = None,
    ) -> None:
        """Update the status of a recovery point"""
        async with ConnectionManager.get_pool().acquire() as conn:
            update_fields = ["status = $1"]
            params = [status]
            param_count = 2

            if error is not None:
                update_fields.append(f"last_error = ${param_count}")
                params.append(error)
                param_count += 1

            if retry_count is not None:
                update_fields.append(f"retry_count = ${param_count}")
                params.append(retry_count)
                param_count += 1

            # Add next retry time if status is FAILED
            if status == "FAILED":
                next_retry = datetime.now(timezone.utc) + timedelta(minutes=5)
                update_fields.append(f"next_retry_at = ${param_count}")
                params.append(next_retry)
                param_count += 1

            query = f"""
                UPDATE recovery_points
                SET {', '.join(update_fields)}
                WHERE batch_id = $1
            """
            params.append(batch_id)
            await conn.execute(query, *params)

    async def get_pending_recovery_points(self, limit: int = 10) -> list:
        """Get pending recovery points that need processing"""
        async with ConnectionManager.get_pool().acquire() as conn:
            return await conn.fetch(
                """
                SELECT * FROM recovery_points
                WHERE status = 'PENDING'
                OR (status = 'FAILED'
                    AND retry_count < 3
                    AND next_retry_at <= CURRENT_TIMESTAMP)
                ORDER BY created_at ASC
                LIMIT $1
                """,
                limit
            )

    async def cleanup_old_recovery_points(self, days: int = 7) -> int:
        """Clean up old completed or failed recovery points"""
        async with ConnectionManager.get_pool().acquire() as conn:
            result = await conn.execute(
                """
                DELETE FROM recovery_points
                WHERE (status = 'COMPLETED'
                    AND created_at < CURRENT_TIMESTAMP - interval '1 day' * $1)
                OR (status = 'FAILED'
                    AND retry_count >= 3
                    AND created_at < CURRENT_TIMESTAMP - interval '1 day' * $1)
                """,
                days
            )
            return result.split()[1]  # Extract number of deleted rows

    async def record_failure(
        self,
        file_name: str,
        error_message: str,
        table_name: str = None,
        rows_processed: int = 0
    ) -> None:
        """
        Record a failure in the recovery points table.

        Args:
            file_name: Name of the failed file
            error_message: Error message describing the failure
            table_name: Target table name (optional)
            rows_processed: Number of rows processed before failure (optional)
        """
        try:
            async with ConnectionManager.get_pool().acquire() as conn:
                await conn.execute(
                    """
                    INSERT INTO recovery_points (
                        table_name,
                        file_name,
                        batch_id,
                        checkpoint_data,
                        status,
                        last_error,
                        retry_count
                    ) VALUES ($1, $2, $3, $4, $5, $6, $7)
                    ON CONFLICT (table_name, file_name, batch_id)
                    DO UPDATE SET
                        last_error = EXCLUDED.last_error,
                        retry_count = recovery_points.retry_count + 1,
                        status = 'FAILED',
                        next_retry_at = CASE
                            WHEN recovery_points.retry_count >= $8 THEN NULL
                            ELSE CURRENT_TIMESTAMP + interval '5 minutes'
                        END
                    """,
                    table_name or 'unknown',
                    file_name,
                    # Generate new batch_id for the failure
                    str(uuid.uuid4()),
                    json.dumps({
                        'rows_processed': rows_processed,
                        'failed_at': datetime.now(timezone.utc).isoformat()
                    }),
                    'FAILED',
                    error_message,
                    0,  # Initial retry_count
                    self.max_retries
                )
                self.logger.info(f"Recorded failure for file {file_name}")
        except Exception as e:
            self.logger.error(f"Error recording failure: {str(e)}")
            raise

    async def get_failed_jobs(self) -> List[Dict[str, Any]]:
        """
        Get failed jobs that are eligible for retry.

        Returns:
            List of failed jobs with retry_count < max_retries and ready for next retry
        """
        async with ConnectionManager.get_pool().acquire() as conn:
            failed_jobs = await conn.fetch(
                """
                    SELECT *
                    FROM recovery_points
                    WHERE status = 'FAILED'
                    AND retry_count < $1
                    AND (next_retry_at IS NULL OR next_retry_at <= CURRENT_TIMESTAMP)
                    ORDER BY created_at ASC
                    """,
                self.max_retries,
            )

            # Convert asyncpg.Record objects to regular dictionaries
            return [dict(job) for job in failed_jobs]
