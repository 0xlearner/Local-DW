import json
import os
from datetime import datetime
from typing import List, Optional

from src.connection_manager import ConnectionManager
from src.logger import setup_logger


class LoadReportGenerator:
    def __init__(self):
        self.logger = setup_logger("load_report_generator")

    async def initialize(self):
        """Verify connection pool exists"""
        ConnectionManager.get_pool()

    async def get_load_summary(self, table_name: Optional[str] = None) -> dict:
        async with ConnectionManager.get_pool().acquire() as conn:
            metrics_query = """
                SELECT
                    COUNT(DISTINCT file_name) as total_files_processed,
                    SUM(rows_processed) as total_records_processed,
                    SUM(rows_inserted) as total_inserts,
                    SUM(rows_updated) as total_updates,
                    SUM(rows_failed) as total_failures,
                    MAX(file_size_bytes) as max_file_size,
                    AVG(file_size_bytes) as avg_file_size
                FROM pipeline_metrics
                WHERE ($1::text IS NULL OR table_name = $1)
                AND processing_status = 'COMPLETED'
            """
            metrics = await conn.fetchrow(metrics_query, table_name)

            changes_query = """
                SELECT
                    change_type,
                    COUNT(*) as count
                FROM change_history
                WHERE ($1::text IS NULL OR table_name = $1)
                GROUP BY change_type
            """
            changes = await conn.fetch(changes_query, table_name)
            changes_dict = {row["change_type"]: row["count"]
                            for row in changes}

            return {
                "total_files_processed": metrics["total_files_processed"] or 0,
                "total_records_processed": metrics["total_records_processed"] or 0,
                "total_inserts": changes_dict.get("INSERT", 0),
                "total_updates": changes_dict.get("UPDATE", 0),
                "total_failures": metrics["total_failures"] or 0,
                "file_sizes": {
                    "max": metrics["max_file_size"] or 0,
                    "average": round(metrics["avg_file_size"] or 0, 2)
                },
                "change_history": changes_dict
            }

    async def generate_report(
        self,
        table_name: Optional[str] = None,
        batch_ids: Optional[List[str]] = None
    ) -> dict:
        summary = await self.get_load_summary(table_name)

        async with ConnectionManager.get_pool().acquire() as conn:
            metrics_query = """
                SELECT
                    file_name,
                    table_name,
                    batch_id,
                    start_time,
                    end_time,
                    processing_status,
                    rows_processed,
                    rows_inserted,
                    rows_updated,
                    rows_failed,
                    file_size_bytes,
                    error_message
                FROM pipeline_metrics
                WHERE ($1::text IS NULL OR table_name = $1)
                AND ($2::text[] IS NULL OR batch_id = ANY($2))
                ORDER BY start_time DESC
            """
            metrics_records = await conn.fetch(metrics_query, table_name, batch_ids)

            return {
                "report_generated_at": datetime.utcnow().isoformat(),
                "generated_by": "0xlearner",
                "table_name": table_name,
                "summary": summary,
                "operations": [dict(record) for record in metrics_records]
            }

    async def save_report(
        self,
        report_path: str,
        table_name: Optional[str] = None,
        batch_ids: Optional[List[str]] = None
    ) -> None:
        try:
            report = await self.generate_report(table_name, batch_ids)

            os.makedirs(os.path.dirname(
                os.path.abspath(report_path)), exist_ok=True)

            with open(report_path, "w") as f:
                json.dump(report, f, indent=2, default=str)

            self.logger.info(f"Load report saved to {report_path}")

        except Exception as e:
            self.logger.error(f"Error saving load report: {str(e)}")
            raise
