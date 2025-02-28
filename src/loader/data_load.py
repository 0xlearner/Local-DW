import io
from datetime import datetime
from typing import Any, Dict

import asyncpg
import polars as pl

from src.config import Config
from src.connection_manager import ConnectionManager
from src.logger import setup_logger
from src.metrics.pipeline_metrics import PipelineMetrics
from src.recover.recovery_manager import RecoveryManager
from src.tracker.batch_tracker import BatchTracker
from src.tracker.metadata_tracker import MetadataTracker


class DataLoader:
    def __init__(
        self,
        config: Config,
        metadata_tracker: MetadataTracker,
        recovery_manager: RecoveryManager,
    ):
        self.config = config
        self.metadata_tracker = metadata_tracker
        self.recovery_manager = recovery_manager
        self.logger = setup_logger("data_loader")
        self.batch_tracker = BatchTracker()

    async def close(self):
        """Cleanup resources"""
        try:
            if hasattr(self.batch_tracker, "close"):
                await self.batch_tracker.close()
        except Exception as e:
            self.logger.error(f"Error closing batch tracker: {str(e)}")

    async def initialize(self):
        """Initialize the data loader and its components"""
        await self.batch_tracker.initialize()

    async def load_data(
        self,
        df: pl.DataFrame,
        table_name: str,
        batch_id: str,
        file_name: str,
    ) -> int:
        """
        Bulk load data into a timestamped temp table using COPY command
        """
        # Handle null and NaN values separately for numeric and non-numeric columns
        expressions = []
        for col in df.columns:
            if df.schema[col].is_numeric():
                # For numeric columns, handle both null and NaN
                expr = (
                    pl.when(pl.col(col).is_null() | pl.col(col).is_nan())
                    .then(None)
                    .otherwise(pl.col(col))
                    .alias(col)
                )
            else:
                # For non-numeric columns, only handle null
                expr = (
                    pl.when(pl.col(col).is_null())
                    .then(None)
                    .otherwise(pl.col(col))
                    .alias(col)
                )
            expressions.append(expr)

        df = df.with_columns(expressions)

        temp_table = f"temp_{table_name}_{batch_id.replace('-', '_')}"

        async with ConnectionManager.get_pool().acquire() as conn:
            try:
                # Get schema first
                schema = await self.metadata_tracker.get_table_schema(table_name)
                if not schema:
                    schema = {
                        col: self._infer_pg_type(df[col].dtype) for col in df.columns
                    }

                # Add metadata columns to schema
                schema.update(
                    {
                        "_ingested_at": "TIMESTAMP WITH TIME ZONE",
                        "_file_name": "TEXT",
                        "_batch_id": "TEXT",
                    }
                )

                # Create temp table
                create_table_sql = self._generate_create_table_sql(temp_table, schema)
                await conn.execute(create_table_sql)

                # Register temp table
                await conn.execute(
                    """
                    INSERT INTO temp_tables_registry 
                    (table_name, original_table, batch_id)
                    VALUES ($1, $2, $3)
                    """,
                    temp_table,
                    table_name,
                    batch_id,
                )

                # Add metadata columns
                df = df.with_columns(
                    [
                        pl.lit(datetime.now()).alias("_ingested_at"),
                        pl.lit(file_name).alias("_file_name"),
                        pl.lit(batch_id).alias("_batch_id"),
                    ]
                )

                # Stream data in chunks
                CHUNK_SIZE = 1000
                total_rows = 0

                try:
                    async with conn.transaction():
                        for i in range(0, len(df), CHUNK_SIZE):
                            chunk = df.slice(i, CHUNK_SIZE)
                            # Convert chunk to list of tuples for copy_records_to_table
                            records = chunk.rows()
                            await conn.copy_records_to_table(
                                temp_table, records=records, columns=df.columns
                            )
                            total_rows += len(chunk)

                    self.logger.info(
                        f"Successfully loaded {total_rows} records into {temp_table}"
                    )
                    return total_rows

                except Exception as e:
                    self.logger.error(f"Bulk copy failed: {str(e)}")
                    await conn.execute(f"DROP TABLE IF EXISTS {temp_table}")
                    await conn.execute(
                        """
                        UPDATE temp_tables_registry 
                        SET status = 'FAILED'
                        WHERE table_name = $1
                        """,
                        temp_table,
                    )
                    raise

            except Exception as e:
                self.logger.error(f"Error during bulk load: {str(e)}")
                await conn.execute(f"DROP TABLE IF EXISTS {temp_table}")
                await conn.execute(
                    """
                    UPDATE temp_tables_registry 
                    SET status = 'FAILED'
                    WHERE table_name = $1
                    """,
                    temp_table,
                )
                raise

    def _infer_pg_type(self, dtype: str) -> str:
        """Infer PostgreSQL type from Polars dtype"""
        dtype = str(dtype).lower()
        if "int" in dtype:
            return "BIGINT"
        elif "float" in dtype:
            return "DOUBLE PRECISION"
        elif "bool" in dtype:
            return "BOOLEAN"
        elif "datetime" in dtype:
            return "TIMESTAMP WITH TIME ZONE"
        elif "date" in dtype:
            return "DATE"
        else:
            return "TEXT"

    def _generate_create_table_sql(self, table_name: str, schema: dict) -> str:
        """Generate CREATE TABLE SQL with metadata columns"""
        columns_sql = ",\n    ".join(
            f"{col_name} {col_type}" for col_name, col_type in schema.items()
        )

        return f"""
        CREATE TABLE {table_name} (
            {columns_sql}
        )
        """

    async def load_data_with_tracking(
        self,
        df: pl.DataFrame,
        table_name: str,
        primary_key: str,
        file_name: str,
        batch_id: str,
        metrics: PipelineMetrics,
        schema: dict,
    ) -> None:
        """
        Load data with metrics tracking
        """
        start_time = datetime.now()

        try:
            total_processed = await self.load_data(
                df=df,
                table_name=table_name,
                batch_id=batch_id,
                file_name=file_name,
            )

            metrics.rows_processed = total_processed
            metrics.file_size_bytes = len(df.write_csv().encode("utf-8"))
            metrics.processing_status = "COMPLETED"

            self.logger.info(
                f"Successfully loaded {total_processed} rows into temp table for {table_name}"
            )

        except Exception as e:
            metrics.error_message = str(e)
            metrics.processing_status = "FAILED"
            self.logger.error(
                f"Error loading data into temp table for {table_name}: {str(e)}",
                exc_info=True,
            )
            raise

        finally:
            metrics.end_time = datetime.now()
            metrics.load_duration_seconds = (
                metrics.end_time - start_time
            ).total_seconds()
