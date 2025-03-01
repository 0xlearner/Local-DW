from datetime import datetime, timezone
from typing import Optional

from src.connection_manager import ConnectionManager
from src.logger import setup_logger


class BatchTracker:
    def __init__(self):
        self.logger = setup_logger("batch_tracker")

    async def initialize(self):
        """Initialize the batch tracker"""
        ConnectionManager.get_pool()  # Will raise if pool not initialized

    async def close(self):
        """Cleanup resources - implemented to satisfy interface requirements"""
        # Currently no resources to clean up, but method exists for consistency
        pass

    async def start_batch(
        self,
        batch_id: str,
        table_name: str,
        file_name: str,
        batch_number: int,
        total_batches: int,
        records_in_batch: int,
    ) -> int:
        """Record the start of a batch processing"""
        async with ConnectionManager.get_pool().acquire() as conn:
            result = await conn.fetchval(
                """
                INSERT INTO bronze.batch_processing (
                    batch_id, table_name, file_name, batch_number, total_batches,
                    records_in_batch, status
                ) VALUES ($1, $2, $3, $4, $5, $6, 'PROCESSING')
                RETURNING id
                """,
                batch_id,
                table_name,
                file_name,
                batch_number,
                total_batches,
                records_in_batch,
            )
            return result

    async def complete_batch(
        self,
        batch_id: str,
        batch_number: int,
        records_processed: int,
        records_inserted: int,
        records_updated: int,
        records_failed: int = 0,
        error_message: Optional[str] = None,
    ):
        """Record the completion of a batch processing"""
        # Ensure end_time is timezone-aware with UTC
        end_time = datetime.now(timezone.utc)

        async with ConnectionManager.get_pool().acquire() as conn:
            await conn.execute(
                """
                UPDATE bronze.batch_processing
                SET status = CASE WHEN $7::text IS NULL THEN 'COMPLETED' ELSE 'FAILED' END,
                    records_processed = $3,
                    records_inserted = $4,
                    records_updated = $5,
                    records_failed = $6,
                    error_message = $7::text,
                    end_time = $8::timestamptz  -- Changed to timestamptz
                WHERE batch_id = $1 AND batch_number = $2
                """,
                batch_id,
                batch_number,
                records_processed,
                records_inserted,
                records_updated,
                records_failed,
                error_message,
                end_time,
            )

    async def get_batch_status(self, batch_id: str) -> list[dict]:
        """Get the status of all batches for a given batch_id"""
        async with ConnectionManager.get_pool().acquire() as conn:
            results = await conn.fetch(
                """
                SELECT
                    batch_id,
                    table_name,
                    file_name,
                    batch_number,
                    total_batches,
                    records_in_batch,
                    records_processed,
                    records_inserted,
                    records_updated,
                    records_failed,
                    status,
                    error_message,
                    start_time,
                    end_time,
                    processing_duration_seconds
                FROM bronze.batch_processing
                WHERE batch_id = $1
                ORDER BY batch_number ASC
                """,
                batch_id,
            )

            # Convert to list of dicts and add summary information
            batches = [dict(row) for row in results]

            if batches:
                total_batches = batches[0]["total_batches"]
                completed = sum(
                    1 for b in batches if b["status"] == "COMPLETED")
                failed = sum(1 for b in batches if b["status"] == "FAILED")
                in_progress = sum(
                    1 for b in batches if b["status"] == "PROCESSING")

                summary = {
                    "total_batches": total_batches,
                    "completed_batches": completed,
                    "failed_batches": failed,
                    "in_progress_batches": in_progress,
                    "remaining_batches": total_batches
                    - (completed + failed + in_progress),
                    "total_records_processed": sum(
                        b["records_processed"] or 0 for b in batches
                    ),
                    "total_records_failed": sum(
                        b["records_failed"] or 0 for b in batches
                    ),
                    "overall_status": (
                        "COMPLETED"
                        if completed == total_batches
                        else "FAILED" if failed > 0 else "IN_PROGRESS"
                    ),
                }

                return {"summary": summary, "batches": batches}

            return {"summary": None, "batches": []}
