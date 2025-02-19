from dataclasses import dataclass
from typing import Dict, Any, Optional
from datetime import datetime
from src.connection_manager import ConnectionManager
from src.logger import setup_logger


@dataclass
class PipelineMetrics:
    file_name: str
    table_name: str
    batch_id: str
    start_time: datetime
    load_duration_seconds: float = 0.0
    end_time: Optional[datetime] = None
    rows_processed: int = 0
    rows_inserted: int = 0
    rows_updated: int = 0
    rows_failed: int = 0
    error_message: Optional[str] = None
    file_size_bytes: int = 0
    processing_status: str = "IN_PROGRESS"  # IN_PROGRESS, COMPLETED, FAILED


class MetricsTracker:
    def __init__(self):
        self.logger = setup_logger("metrics_tracker")

    async def initialize(self):
        """Verify connection pool exists"""
        ConnectionManager.get_pool()  # Will raise if pool not initialized

    async def save_metrics(self, metrics: PipelineMetrics) -> None:
        """Save pipeline metrics to the database"""
        async with ConnectionManager.get_pool().acquire() as conn:
            await conn.execute(
                """
                INSERT INTO pipeline_metrics (
                    file_name, table_name, batch_id, start_time, end_time,
                    processing_status, rows_processed, rows_inserted,
                    rows_updated, rows_failed, file_size_bytes,
                    error_message, load_duration_seconds
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
                """,
                metrics.file_name,
                metrics.table_name,
                metrics.batch_id,
                metrics.start_time,
                metrics.end_time,
                metrics.processing_status,
                metrics.rows_processed or 0,  # Use 0 instead of null
                metrics.rows_inserted or 0,
                metrics.rows_updated or 0,
                metrics.rows_failed or 0,
                metrics.file_size_bytes or 0,
                metrics.error_message,
                metrics.load_duration_seconds or 0,
            )

    async def get_metrics_summary(self, table_name: Optional[str] = None) -> Dict[str, Any]:
        """Get summary of pipeline metrics"""
        async with ConnectionManager.get_pool().acquire() as conn:
            query = """
                SELECT
                    COUNT(DISTINCT file_name) as total_files,
                    SUM(rows_processed) as total_rows_processed,
                    SUM(rows_inserted) as total_rows_inserted,
                    SUM(rows_updated) as total_rows_updated,
                    SUM(rows_failed) as total_rows_failed,
                    COUNT(CASE WHEN processing_status = 'COMPLETED' THEN 1 END) as successful_loads,
                    COUNT(CASE WHEN processing_status = 'FAILED' THEN 1 END) as failed_loads
                FROM pipeline_metrics
                WHERE ($1::text IS NULL OR table_name = $1)
            """
            result = await conn.fetchrow(query, table_name)
            return dict(result)
