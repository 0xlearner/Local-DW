from dataclasses import dataclass
from datetime import datetime
from typing import Dict, Any, Optional
import asyncpg


@dataclass
class PipelineMetrics:
    file_name: str
    table_name: str
    start_time: datetime
    end_time: Optional[datetime] = None
    rows_processed: int = 0
    rows_inserted: int = 0
    rows_updated: int = 0
    rows_failed: int = 0
    error_message: Optional[str] = None
    file_size_bytes: int = 0
    processing_status: str = "IN_PROGRESS"  # IN_PROGRESS, COMPLETED, FAILED


class MetricsTracker:
    def __init__(self, conn_params: Dict[str, Any]):
        self.conn_params = conn_params

    async def initialize_metrics_table(self):
        async with asyncpg.create_pool(**self.conn_params) as pool:
            async with pool.acquire() as conn:
                await conn.execute(
                    """
                    CREATE TABLE IF NOT EXISTS pipeline_metrics (
                        id SERIAL PRIMARY KEY,
                        file_name TEXT NOT NULL,
                        table_name TEXT NOT NULL,
                        start_time TIMESTAMP NOT NULL,
                        end_time TIMESTAMP,
                        rows_processed INTEGER DEFAULT 0,
                        rows_inserted INTEGER DEFAULT 0,
                        rows_updated INTEGER DEFAULT 0,
                        rows_failed INTEGER DEFAULT 0,
                        error_message TEXT,
                        file_size_bytes BIGINT DEFAULT 0,
                        processing_status TEXT NOT NULL,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """
                )

    async def save_metrics(self, metrics: PipelineMetrics):
        async with asyncpg.create_pool(**self.conn_params) as pool:
            async with pool.acquire() as conn:
                await conn.execute(
                    """
                    INSERT INTO pipeline_metrics (
                        file_name, table_name, start_time, end_time,
                        rows_processed, rows_inserted, rows_updated,
                        rows_failed, error_message, file_size_bytes,
                        processing_status
                    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
                """,
                    metrics.file_name,
                    metrics.table_name,
                    metrics.start_time,
                    metrics.end_time,
                    metrics.rows_processed,
                    metrics.rows_inserted,
                    metrics.rows_updated,
                    metrics.rows_failed,
                    metrics.error_message,
                    metrics.file_size_bytes,
                    metrics.processing_status,
                )
