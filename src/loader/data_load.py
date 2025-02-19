from typing import Dict, Any
from datetime import datetime
from io import StringIO

import polars as pl

from src.config import Config
from src.logger import setup_logger
from src.connection_manager import ConnectionManager
from src.metrics.pipeline_metrics import PipelineMetrics
from src.recover.recovery_manager import RecoveryManager
from src.tracker.metadata_tracker import MetadataTracker
from .value_formatter import ValueFormatter
from .batch_processor import BatchProcessor
from .serializer import RecordSerializer


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
        self.batch_size = config.BATCH_SIZE
        self.logger = setup_logger("data_loader")

        # Initialize components
        self.conn_manager = ConnectionManager()
        self.value_formatter = ValueFormatter()
        self.batch_processor = BatchProcessor(self.value_formatter)
        self.serializer = RecordSerializer()

    async def initialize(self):
        """Verify connection pool exists"""
        ConnectionManager.get_pool()  # Will raise if pool not initialized

    async def close(self):
        """Nothing to close as we're using shared connection pool"""
        pass

    async def load_data_with_tracking(
        self,
        df: pl.DataFrame,
        table_name: str,
        primary_key: str,
        file_name: str,
        batch_id: str,
        merge_strategy: str,
        metrics: PipelineMetrics,
        schema: dict,
    ) -> None:
        """
        Load data into the target table while tracking metrics and managing errors.

        Args:
            df: Polars DataFrame containing the data to load
            table_name: Name of the target table
            primary_key: Primary key column name
            file_name: Name of the source file
            batch_id: Unique identifier for this batch
            merge_strategy: One of "INSERT", "UPDATE", or "MERGE"
            metrics: PipelineMetrics instance for tracking
            schema: Dictionary containing column schema information
        """
        start_time = datetime.now()

        try:
            # Load the data using the data loader
            total_processed, inserts, updates = await self.load_data(
                df=df,
                table_name=table_name,
                primary_key=primary_key,
                file_name=file_name,
                batch_id=batch_id,
                merge_strategy=merge_strategy,
            )

            # Update metrics
            metrics.rows_processed = total_processed
            metrics.rows_inserted = inserts
            metrics.rows_updated = updates
            # Calculate file size using StringIO buffer
            buffer = StringIO()
            df.write_csv(buffer)
            metrics.file_size_bytes = len(buffer.getvalue().encode('utf-8'))
            metrics.file_size_bytes = len(df.write_csv().encode('utf-8'))
            metrics.processing_status = "COMPLETED"

            self.logger.info(
                f"Successfully loaded {
                    total_processed} rows into {table_name} "
                f"({inserts} inserts, {updates} updates)"
            )

            self.logger.info(
                f"Successfully loaded {
                    total_processed} rows into {table_name} "
                f"({inserts} inserts, {updates} updates)"
            )

        except Exception as e:
            metrics.error_message = str(e)
            metrics.processing_status = "FAILED"
            self.logger.error(
                f"Error loading data into {table_name}: {str(e)}", exc_info=True
            )
            raise

        finally:
            # Record timing information
            metrics.end_time = datetime.now()
            metrics.load_duration_seconds = (
                metrics.end_time - start_time
            ).total_seconds()

    async def load_data(
        self,
        df: pl.DataFrame,
        table_name: str,
        primary_key: str,
        file_name: str,
        batch_id: str,
        merge_strategy: str = "MERGE",
    ) -> None:
        """Load data into the target table with the specified merge strategy"""
        self.logger.info(f"Starting load_data for table {
                         table_name} with batch_id {batch_id}")

        async with ConnectionManager.get_pool().acquire() as conn:
            columns = df.columns
            schema = await ConnectionManager.get_column_types(table_name)
            records = df.to_dicts()

            formatted_records = await self.batch_processor.process_batch(
                conn=conn,
                table_name=table_name,
                columns=columns,
                records=records,
                column_types=schema
            )
            total_processed = 0
            inserts = 0
            updates = 0
            for record in formatted_records:
                existing_record = await conn.fetchrow(
                    f"SELECT * FROM {table_name} WHERE {primary_key} = ${
                        1}::{schema.get(primary_key, 'text')}",
                    record[primary_key],
                )

                sql = self.batch_processor.generate_sql(
                    table_name, columns, schema, merge_strategy, primary_key
                )

                values = [record[col] for col in columns]
                try:
                    await conn.execute(sql, *values)
                    total_processed += 1
                    if existing_record:
                        updates += 1
                    else:
                        inserts += 1
                except Exception as e:
                    self.logger.error(f"Error executing SQL: {str(e)}")
                    self.logger.error(f"SQL: {sql}")
                    self.logger.error(f"Values: {values}")
                    raise

                # Record the change
                await self.metadata_tracker.record_change(
                    conn,
                    table_name,
                    "UPDATE" if existing_record else "INSERT",
                    record,
                    batch_id,
                    primary_key,
                    file_name,
                    old_record=dict(
                        existing_record) if existing_record else None,
                )

            self.logger.info(f"Processed {total_processed} records")
            return total_processed, inserts, updates

    def serialize_record(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """Delegate serialization to RecordSerializer"""
        return self.serializer.serialize_record(record)
