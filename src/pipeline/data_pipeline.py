import uuid
from datetime import datetime
from typing import List


from src.config import Config
from src.client.s3_client import S3Client
from src.schema_inferrer.schema_infer import SchemaInferrer
from src.connection_manager import ConnectionManager
from src.loader.data_load import DataLoader
from src.logger import setup_logger
from src.metrics.pipeline_metrics import MetricsTracker, PipelineMetrics
from src.recover.recovery_manager import RecoveryManager
from src.recover.recovery_worker import RecoveryWorker
from src.tracker.file_tracker import FileTracker
from src.tracker.metadata_tracker import MetadataTracker
from src.validator.data_validation import DataValidator
from src.reporting.load_report import LoadReportGenerator
from .components.infrastructure_manager import InfrastructureManager
from .components.file_processor import FileProcessor
from .components.recovery_coordinator import RecoveryCoordinator


class Pipeline:
    def __init__(self, config: Config):
        self.config = config
        self.logger = setup_logger("pipeline")
        self._initialized = False

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
        await self.file_tracker.initialize()
        await self.report_generator.initialize()

        # Start recovery process
        await self.recovery_coordinator.start_recovery()

        self._initialized = True
        self.logger.info("Pipeline initialization completed successfully")

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
            f"Error processing file {file_name}: {error_message}",
            exc_info=True
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
                error_message=error_message
            )
        except Exception as e:
            # Log but don't raise - we want to preserve the original error
            self.logger.error(
                f"Error updating file tracker for {file_name}: {str(e)}",
                exc_info=True
            )

        # Attempt to record failure in recovery manager
        try:
            await self.recovery_manager.record_failure(
                file_name=file_name,
                error_message=error_message,
                table_name=metrics.table_name,
                rows_processed=metrics.rows_processed or 0
            )
        except Exception as e:
            # Log but don't raise - we want to preserve the original error
            self.logger.error(
                f"Error recording failure in recovery manager for {
                    file_name}: {str(e)}",
                exc_info=True
            )

    async def process_file(
        self,
        file_name: str,
        primary_key: str,
        merge_strategy: str,
        target_table: str = None,
    ) -> str:
        """Process a single file with the specified merge strategy"""
        batch_id = str(uuid.uuid4())
        table_name = target_table or self._get_table_name(file_name)
        csv_df, file_hash, schema = await self.file_processor.process_file(
            file_name)

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

            # Validate schema compatibility if table exists
            async with ConnectionManager.get_pool().acquire() as conn:
                table_exists = await conn.fetchval(
                    """
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables
                        WHERE table_name = $1
                    )
                    """,
                    table_name,
                )

                if table_exists:
                    # Validate schema compatibility
                    is_compatible = await self.schema_inferrer.validate_schema_compatibility(
                        table_name, schema
                    )
                    if not is_compatible:
                        raise ValueError(
                            f"Schema doesn't exist for table {
                                table_name}"
                        )
                else:
                    # Create table with inferred schema
                    await self.schema_inferrer.create_table_if_not_exists(
                        table_name, schema, primary_key
                    )
                    self.logger.info(f"Created table: {table_name}")

            # Validate data before loading
            if not self.data_validator.validate_data(csv_df, schema):
                validation_errors = self.data_validator.validation_errors
                self.logger.error(
                    f"Data validation failed for {
                        file_name}: {validation_errors}"
                )
                raise ValueError(f"Data validation failed: {
                                 validation_errors}")

            # Load the data using the enhanced data loader
            metrics.rows_processed = len(csv_df)
            await self.data_loader.load_data_with_tracking(
                csv_df, table_name, primary_key, file_name, batch_id,
                merge_strategy, metrics, schema
            )

            # Update metrics and mark file as processed
            metrics.end_time = datetime.now()
            metrics.processing_status = "COMPLETED"
            await self.file_tracker.mark_file_processed(
                file_name, file_hash, "COMPLETED", metrics.rows_processed
            )

            return batch_id

        except Exception as e:
            await self._handle_processing_error(e, file_name, metrics, file_hash)
            raise
        finally:
            metrics.end_time = datetime.now()
            await self.metrics_tracker.save_metrics(metrics)

    async def run(
        self,
        file_prefix: str,
        primary_key: str,
        merge_strategy: str = "MERGE",
        target_table: str = None,
    ) -> str:
        """Run the pipeline with the specified merge strategy"""
        if not self._initialized:
            await self.initialize()

        if merge_strategy not in ["INSERT", "UPDATE", "MERGE"]:
            raise ValueError(f"Invalid merge strategy: {merge_strategy}")

        try:
            batch_id = await self.process_file(
                file_name=file_prefix,
                primary_key=primary_key,
                merge_strategy=merge_strategy,
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
                                {"id": record["id"], f"{
                                    column}": record[column]}
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

        await self.recovery_coordinator.stop_recovery()
        await self.data_loader.close()
        # Close central connection pool last
        await ConnectionManager.close()
        self._initialized = False

    async def cleanup(self):
        """Clean up all resources"""
        await self.shutdown()

    def _get_table_name(self, file_name: str) -> str:
        """Extract table name from file name"""
        return file_name.split('/')[-1].split('.')[0].lower()

    async def get_load_summary(self, table_name: str = None) -> dict:
        if not self._initialized:
            await self.initialize()
        return await self.report_generator.get_load_summary(table_name)

    async def generate_load_report(
        self,
        batch_ids: List[str] = None,
        table_name: str = None
    ) -> dict:
        if not self._initialized:
            await self.initialize()
        return await self.report_generator.generate_report(table_name, batch_ids)

    async def save_load_report(
        self,
        report_path: str,
        batch_ids: List[str] = None,
        table_name: str = None
    ) -> None:
        if not self._initialized:
            await self.initialize()
        await self.report_generator.save_report(report_path, table_name, batch_ids)
