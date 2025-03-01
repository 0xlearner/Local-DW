import asyncio
import uuid
from datetime import datetime
from typing import List

from src.client.s3_client import S3Client
from src.config import Config
from src.connection_manager import ConnectionManager
from src.loader.data_load import DataLoader
from src.logger import setup_logger
from src.metrics.pipeline_metrics import MetricsTracker, PipelineMetrics
from src.recover.recovery_manager import RecoveryManager
from src.recover.recovery_worker import RecoveryWorker
from src.reporting.load_report import LoadReportGenerator
from src.schema_inferrer.schema_infer import SchemaInferrer
from src.tracker.file_tracker import FileTracker
from src.tracker.metadata_tracker import MetadataTracker
from src.validator.data_validation import DataValidator

from .components.file_processor import FileProcessor
from .components.infrastructure_manager import InfrastructureManager
from .components.recovery_coordinator import RecoveryCoordinator


class Pipeline:
    def __init__(self, config: Config):
        self.config = config
        self.logger = setup_logger("pipeline")
        self._initialized = False
        self.infrastructure_manager = InfrastructureManager()
        self.cleanup_task = None

        # Initialize connection management
        self.conn_params = {
            "host": config.PG_HOST,
            "port": config.PG_PORT,
            "user": config.PG_USER,
            "password": config.PG_PASSWORD,
            "database": config.PG_DATABASE,
        }

        # Initialize core services
        self.schema_inferrer = SchemaInferrer()
        self.data_validator = DataValidator()

        # Initialize components
        self.s3_client = S3Client(config)
        self.schema_inferrer = SchemaInferrer()
        self.metadata_tracker = MetadataTracker()
        self.recovery_manager = RecoveryManager()
        self.metrics_tracker = MetricsTracker()
        self.file_tracker = FileTracker()

        # Initialize specialized components
        self.data_loader = DataLoader(
            config=config,
            metadata_tracker=self.metadata_tracker,
            recovery_manager=self.recovery_manager,
        )
        # Set data_loader in metadata_tracker
        self.metadata_tracker.set_data_loader(self.data_loader)

        self.recovery_worker = RecoveryWorker(
            config, self.recovery_manager, self.data_loader
        )

        # Initialize pipeline-specific components
        self.infrastructure_manager = InfrastructureManager()
        self.file_processor = FileProcessor(
            self.s3_client, self.schema_inferrer, self.data_validator
        )
        self.recovery_coordinator = RecoveryCoordinator(self.recovery_worker)
        self.report_generator = None

    async def initialize(self):
        """Initialize the pipeline and its components"""
        if self._initialized:
            return

        # Initialize central connection pool
        await ConnectionManager.initialize(self.conn_params)
        self.report_generator = LoadReportGenerator()

        # Initialize infrastructure
        async with ConnectionManager.get_pool().acquire() as conn:
            await self.infrastructure_manager.initialize_infrastructure_tables(conn)

        # Initialize components
        await self.data_loader.initialize()
        await self.metadata_tracker.initialize()
        await self.recovery_manager.initialize()
        await self.metrics_tracker.initialize()
        await self.report_generator.initialize()

        # Start recovery process
        await self.recovery_coordinator.start_recovery()

        # Start cleanup task
        self.cleanup_task = asyncio.create_task(self._periodic_cleanup())

        self._initialized = True
        self.logger.info("Pipeline initialization completed successfully")

    async def _periodic_cleanup(self):
        """Run periodic cleanup of temp tables"""
        while True:
            try:
                # Run cleanup every 24 hours
                await asyncio.sleep(24 * 60 * 60)  # 24 hours in seconds
                await self.infrastructure_manager.cleanup_temp_tables(retention_days=90)
            except Exception as e:
                self.logger.error(f"Error in periodic cleanup: {str(e)}")

    async def _handle_processing_error(
        self,
        error: Exception,
        file_name: str,
        metrics: PipelineMetrics,
        file_hash: str = None,
    ) -> None:
        """
        Handle errors that occur during file processing.

        Args:
            error: The exception that occurred
            file_name: Name of the file being processed
            metrics: PipelineMetrics instance for tracking
            file_hash: Hash of the file being processed (if available)
        """
        error_message = str(error)
        self.logger.error(
            f"Error processing file {file_name}: {error_message}", exc_info=True
        )

        # Update metrics
        metrics.end_time = datetime.now()
        metrics.processing_status = "FAILED"
        metrics.error_message = error_message

        # Mark file as failed in the file tracker
        try:
            await self.file_tracker.mark_file_processed(
                file_name=file_name,
                file_hash=file_hash,
                status="FAILED",
                rows_processed=metrics.rows_processed or 0,
                error_message=error_message,
                batch_id=metrics.batch_id,
            )
        except Exception as e:
            # Log but don't raise - we want to preserve the original error
            self.logger.error(
                f"Error updating file tracker for {file_name}: {str(e)}", exc_info=True
            )

        # Attempt to record failure in recovery manager
        try:
            await self.recovery_manager.record_failure(
                file_name=file_name,
                error_message=error_message,
                table_name=metrics.table_name,
                rows_processed=metrics.rows_processed or 0,
            )
        except Exception as e:
            # Log but don't raise - we want to preserve the original error
            self.logger.error(
                f"Error recording failure in recovery manager for {
                    file_name}: {str(e)}",
                exc_info=True,
            )

    async def process_file(
        self,
        file_name: str,
        primary_key: str,
        target_table: str = None,
    ) -> str:
        """Process a single file and load into temp table"""
        batch_id = str(uuid.uuid4())
        table_name = target_table or self._get_table_name(file_name)
        csv_df, file_hash, schema = await self.file_processor.process_file(file_name)

        metrics = PipelineMetrics(
            file_name=file_name,
            table_name=table_name,
            batch_id=batch_id,
            start_time=datetime.now(),
        )

        try:
            # Check for duplicate processing
            if await self.file_tracker.is_file_processed(file_name, file_hash):
                self.logger.info(
                    f"File {file_name} already processed, skipping")
                return batch_id

            # Load the data into temp table
            metrics.rows_processed = len(csv_df)
            await self.data_loader.load_data_with_tracking(
                csv_df,
                table_name,
                primary_key,
                file_name,
                batch_id,
                metrics,
                schema,
            )

            # Mark file as successfully processed
            await self.file_tracker.mark_file_processed(
                file_name=file_name,
                file_hash=file_hash,
                status="COMPLETED",
                rows_processed=metrics.rows_processed,
                batch_id=batch_id,
            )

            self.logger.info(
                f"Successfully processed file {
                    file_name} with batch_id {batch_id}"
            )
            return batch_id

        except Exception as e:
            await self._handle_processing_error(e, file_name, metrics, file_hash)
            raise
        finally:
            metrics.end_time = datetime.now()
            await self.metrics_tracker.save_metrics(metrics)

    async def get_batch_processing_status(self, batch_id: str) -> list[dict]:
        """Get detailed batch processing status for a specific batch"""
        return await self.data_loader.batch_tracker.get_batch_status(batch_id)

    async def monitor_batch_processing(
        self, batch_id: str, polling_interval: int = 5
    ) -> dict:
        """
        Monitor the progress of batch processing until completion or failure.

        Args:
            batch_id: The batch ID to monitor
            polling_interval: Time in seconds between status checks

        Returns:
            Final batch processing status
        """
        while True:
            status = await self.get_batch_processing_status(batch_id)

            if not status["batches"]:
                self.logger.warning(
                    f"No batch information found for batch_id: {batch_id}"
                )
                return status

            summary = status["summary"]

            # Log current status
            self.logger.info(
                f"Batch Processing Status: "
                f"Completed: {summary['completed_batches']
                              }/{summary['total_batches']}, "
                f"Failed: {summary['failed_batches']}, "
                f"In Progress: {summary['in_progress_batches']}, "
                f"Records Processed: {summary['total_records_processed']}"
            )

            # If there are failed batches, log their errors
            failed_batches = [b for b in status["batches"]
                              if b["status"] == "FAILED"]
            for batch in failed_batches:
                self.logger.error(
                    f"Batch {batch['batch_number']} failed: {
                        batch['error_message']}"
                )

            # Check if processing is complete (either all done or any failures)
            if summary["overall_status"] in ["COMPLETED", "FAILED"]:
                return status

            await asyncio.sleep(polling_interval)

    async def run(
        self,
        file_prefix: str,
        primary_key: str,
        target_table: str = None,
    ) -> str:
        """Run the pipeline to load data into temp table"""
        if not self._initialized:
            await self.initialize()

        try:
            batch_id = await self.process_file(
                file_name=file_prefix,
                primary_key=primary_key,
                target_table=target_table,
            )
            self.logger.info("Pipeline completed successfully")
            return batch_id

        except Exception as e:
            self.logger.error(f"Pipeline failed: {str(e)}")
            raise

    async def verify_updates(
        self, table_name: str, id_range: tuple, verifications: List[dict]
    ) -> bool:
        """
        Verify that records in the specified table meet multiple conditions.

        Args:
            table_name: Name of the table to check
            id_range: Tuple of (start_id, end_id) to check
            verifications: List of dictionaries containing:
                - column: Column name to verify
                - condition: SQL condition to check against the column
                - message: Custom message for logging (optional)

        Returns:
            bool: True if all records meet all conditions, False otherwise
        """
        try:
            async with ConnectionManager.get_pool().acquire() as conn:
                # Get all necessary columns
                columns = [v["column"] for v in verifications]
                query = f"""
                    SELECT id, {', '.join(columns)}
                    FROM {table_name}
                    WHERE id >= $1 AND id < $2
                    ORDER BY id
                """
                records = await conn.fetch(query, id_range[0], id_range[1])

                # Verify each condition for each record
                failed_verifications = []
                for verification in verifications:
                    column = verification["column"]
                    condition = verification["condition"]
                    message = verification.get(
                        "message", f"Condition: {column} {condition}"
                    )

                    failed_records = []
                    for record in records:
                        if not eval(f"{record[column]} {condition}"):
                            failed_records.append(
                                {
                                    "id": record["id"],
                                    f"{
                                        column}": record[
                                        column
                                    ],
                                }
                            )

                    if failed_records:
                        error_msg = (
                            f"Verification failed for {message}. "
                            f"Found {len(failed_records)} failing records: {
                                failed_records}"
                        )
                        self.logger.error(error_msg)
                        failed_verifications.append(error_msg)
                    else:
                        self.logger.info(
                            f"Successfully verified {len(records)} records for {
                                message}"
                        )

                return len(failed_verifications) == 0

        except Exception as e:
            self.logger.error(f"Error verifying updates: {str(e)}")
            raise

    async def shutdown(self):
        """Shutdown all components in the correct order"""
        if not self._initialized:
            return

        if self.cleanup_task:
            self.cleanup_task.cancel()
            try:
                await self.cleanup_task
            except asyncio.CancelledError:
                pass

        await self.recovery_coordinator.stop_recovery()
        await self.data_loader.close()
        # Close central connection pool last
        await ConnectionManager.close()
        self._initialized = False

    def _get_table_name(self, file_name: str) -> str:
        """Extract table name from file name"""
        return file_name.split("/")[-1].split(".")[0].lower()

    async def get_load_summary(self, table_name: str = None) -> dict:
        if not self._initialized:
            await self.initialize()
        return await self.report_generator.get_load_summary(table_name)

    async def generate_load_report(
        self, batch_ids: List[str] = None, table_name: str = None
    ) -> dict:
        if not self._initialized:
            await self.initialize()
        return await self.report_generator.generate_report(table_name, batch_ids)

    async def save_load_report(
        self, report_path: str, batch_ids: List[str] = None, table_name: str = None
    ) -> None:
        if not self._initialized:
            await self.initialize()
        await self.report_generator.save_report(report_path, table_name, batch_ids)
