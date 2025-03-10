from datetime import datetime

import polars as pl

from src.config import Config
from src.connection_manager import ConnectionManager
from src.infrastructure.table_manager import TableManager
from src.logger import setup_logger
from src.metrics.pipeline_metrics import PipelineMetrics
from src.recover.recovery_manager import RecoveryManager
from src.schema_inferrer.schema_infer import SchemaInferrer
from src.tracker.batch_tracker import BatchTracker
from src.tracker.metadata_tracker import MetadataTracker


class DataLoader:
    def __init__(
        self,
        config: Config,
        metadata_tracker: MetadataTracker,
        recovery_manager: RecoveryManager,
        table_manager: TableManager,
    ):
        self.config = config
        self.metadata_tracker = metadata_tracker
        self.recovery_manager = recovery_manager
        self.table_manager = table_manager
        self.logger = setup_logger("data_loader")
        self.batch_tracker = BatchTracker()
        self.schema_inferrer = SchemaInferrer()

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
        """Bulk load data into a partitioned table using COPY command"""
        # Handle null and NaN values separately for numeric and non-numeric columns
        expressions = []
        for col in df.columns:
            if df.schema[col].is_numeric():
                expr = (
                    pl.when(pl.col(col).is_null() | pl.col(col).is_nan())
                    .then(None)
                    .otherwise(pl.col(col))
                    .alias(col)
                )
            else:
                expr = (
                    pl.when(pl.col(col).is_null())
                    .then(None)
                    .otherwise(pl.col(col))
                    .alias(col)
                )
            expressions.append(expr)

        df = df.with_columns(expressions)

        # Main table name (no batch_id suffix)
        main_table = f"bronze.raw_{table_name}"

        async with ConnectionManager.get_pool().acquire() as conn:
            try:
                # Get or create schema
                schema = await self.metadata_tracker.get_table_schema(table_name)
                if not schema:
                    schema = {
                        col: self.schema_inferrer.infer_pg_type(df[col].dtype)
                        for col in df.columns
                    }

                # Add metadata columns to schema
                schema.update(
                    {
                        "_ingested_at": "TIMESTAMP WITH TIME ZONE",
                        "_file_name": "TEXT",
                        "_batch_id": "TEXT",
                    }
                )

                # Create the main partitioned table if it doesn't exist
                await self.table_manager.create_table(conn, main_table, schema)

                # Create partition for this batch
                await self.table_manager.create_partition(conn, main_table, batch_id)

                # Register batch
                await conn.execute(
                    """
                        INSERT INTO bronze.tables_registry
                        (table_name, original_table, batch_id)
                        VALUES ($1, $2, $3)
                        ON CONFLICT (table_name, batch_id)
                        DO UPDATE SET last_accessed_at = CURRENT_TIMESTAMP
                        """,
                    main_table,
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
                CHUNK_SIZE = self.config.CHUNK_SIZE
                total_rows = 0

                try:
                    async with conn.transaction():
                        for i in range(0, len(df), CHUNK_SIZE):
                            chunk = df.slice(i, CHUNK_SIZE)
                            records = chunk.rows()
                            await conn.copy_records_to_table(
                                # table name without schema
                                f"raw_{table_name}",
                                schema_name="bronze",
                                records=records,
                                columns=df.columns,
                            )
                            total_rows += len(chunk)

                    self.logger.info(
                        f"Successfully loaded {
                            total_rows} records into {main_table} "
                        f"partition for batch {batch_id}"
                    )

                    # Get count from the partition
                    partition_count = await conn.fetchval(
                        f"""
                        SELECT COUNT(*)
                        FROM {main_table}
                        WHERE _batch_id = $1
                        """,
                        batch_id,
                    )

                    self.logger.info(
                        f"Verified {partition_count} records in partition for batch {
                            batch_id}"
                    )
                    return total_rows

                except Exception as e:
                    self.logger.error(f"Bulk copy failed: {str(e)}")
                    # Drop the partition on failure
                    await conn.execute(
                        f"""
                        DROP TABLE IF EXISTS {main_table}_{batch_id.replace('-', '_')}
                        """
                    )
                    await conn.execute(
                        """
                            UPDATE bronze.temp_tables_registry
                            SET status = 'FAILED'
                            WHERE table_name = $1 AND batch_id = $2
                            """,
                        main_table,
                        batch_id,
                    )
                    raise

            except Exception as e:
                self.logger.error(f"Error during bulk load: {str(e)}")
                # Drop the partition on failure
                await conn.execute(
                    f"""
                    DROP TABLE IF EXISTS {main_table}_{batch_id.replace('-', '_')}
                    """
                )
                await conn.execute(
                    """
                        UPDATE bronze.tables_registry
                        SET status = 'FAILED'
                        WHERE table_name = $1 AND batch_id = $2
                        """,
                    main_table,
                    batch_id,
                )
                raise

    async def load_data_with_tracking(
        self,
        df: pl.DataFrame,
        table_name: str,
        primary_key: str,
        file_name: str,
        batch_id: str,
        metrics: PipelineMetrics,
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
