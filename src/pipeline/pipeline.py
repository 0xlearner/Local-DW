import asyncio
import uuid
from datetime import datetime
import hashlib
import json
import os
from io import StringIO
import asyncpg
import polars as pl
from src.config import Config
from src.client.s3_client import S3Client
from src.schema_inferrer.schema_infer import SchemaInferrer
from src.loader.data_loader import DataLoader
from src.logger import setup_logger
from src.metrics.pipeline_metrics import MetricsTracker, PipelineMetrics
from src.recover.recovery_manager import RecoveryManager
from src.recover.recovery_worker import RecoveryWorker
from src.tracker.file_tracker import FileTracker
from src.tracker.metadata_tracker import MetadataTracker
from src.validator.data_validation import DataValidator

logger = setup_logger("pipeline")


class Pipeline:
    def __init__(self, config: Config):
        self.config = config
        self.s3_client = S3Client(config)
        self.schema_inferrer = SchemaInferrer()
        self._pool = None
        self._recovery_task = None
        self._initialized = False

        # Store connection params
        self.conn_params = {
            "host": config.PG_HOST,
            "port": config.PG_PORT,
            "user": config.PG_USER,
            "password": config.PG_PASSWORD,
            "database": config.PG_DATABASE,
        }

        # Initialize components without connections
        self.metadata_tracker = MetadataTracker(self.conn_params)
        self.recovery_manager = RecoveryManager(self.conn_params)
        self.data_loader = DataLoader(
            config, self.metadata_tracker, self.recovery_manager
        )
        self.recovery_worker = RecoveryWorker(
            config, self.recovery_manager, self.data_loader
        )
        self.data_validator = DataValidator()
        self.metrics_tracker = MetricsTracker(self.conn_params)
        self.file_tracker = FileTracker(self.conn_params)

    async def initialize(self):
        if self._initialized:
            return

        # Initialize connection pool first
        self._pool = await asyncpg.create_pool(**self.conn_params)

        # Initialize components that need database access
        await self.metadata_tracker.initialize_metadata_tables()
        await self.data_loader.initialize()

        # Start recovery worker
        self._recovery_task = asyncio.create_task(self.recovery_worker.start())
        self._initialized = True

    async def initialize_tracking_tables(self):
        async with self._pool.acquire() as conn:
            await self.metrics_tracker.initialize_metrics_table()
            await self.file_tracker.initialize_tracking_table(conn)

    def _get_table_name(self, file_name: str) -> str:
        # Extract base name from file path and remove extensions
        base_name = os.path.basename(file_name)
        # Remove both .csv and .gz extensions
        base_name = base_name.replace(".csv.gz", "").replace(".csv", "")
        # Add raw_ prefix
        return f"raw_{base_name}"

    def _calculate_file_hash(self, data: str) -> str:
        return hashlib.md5(data.encode()).hexdigest()

    async def process_file(self, file_name: str, primary_key: str) -> None:
        metrics = PipelineMetrics(
            file_name=file_name,
            table_name=self._get_table_name(file_name),
            start_time=datetime.now(),
        )

        try:
            # Read and decompress file
            logger.info(f"Reading file {file_name} from S3")
            csv_data = self.s3_client.read_gz_file(file_name)

            if csv_data.getbuffer().nbytes == 0:
                raise ValueError(
                    f"Empty or invalid data received from file {file_name}"
                )

            # Calculate hash from the binary data
            file_hash = hashlib.md5(csv_data.getvalue()).hexdigest()
            logger.debug(f"File hash: {file_hash}")

            # Check if file was already processed
            if await self.file_tracker.is_file_processed(file_name, file_hash):
                logger.info(f"File {file_name} was already processed, skipping")
                return

            # Update metrics
            metrics.file_size_bytes = csv_data.getbuffer().nbytes
            logger.info(
                f"Processing file {file_name} of size {metrics.file_size_bytes} bytes"
            )

            # Read CSV data for schema inference
            csv_content = csv_data.read().decode("utf-8")
            # Create a new StringIO buffer for each read operation
            csv_buffer = StringIO(csv_content)
            df = pl.read_csv(csv_buffer)

            if df.is_empty():
                raise ValueError(f"No data found in file {file_name}")
            logger.info(f"Successfully read CSV with {len(df)} rows")

            # Create a new buffer for schema inference
            schema_buffer = StringIO(csv_content)
            schema = self.schema_inferrer.infer_schema(schema_buffer.read())
            logger.debug(f"Inferred schema: {schema}")

            # Add debug logging for timestamp columns
            timestamp_cols = [
                col
                for col, type_info in schema.items()
                if "TIMESTAMP" in type_info.upper()
            ]
            logger.debug(f"Timestamp columns: {timestamp_cols}")

            if "created_at" in df.columns:
                sample_dates = df["created_at"].head(5)
                logger.debug(f"Sample created_at values: {sample_dates}")

            if not self.data_validator.validate_data(df, schema):
                validation_errors = json.dumps(self.data_validator.validation_errors)
                logger.error(
                    f"Data validation failed for {file_name}: {validation_errors}"
                )
                raise ValueError(f"Data validation failed: {validation_errors}")

            # Create table and load data
            async with await self.data_loader.connect() as conn:
                await self.schema_inferrer.create_table_if_not_exists(
                    conn, metrics.table_name, schema, primary_key
                )
                logger.info(f"Created Table: {metrics.table_name}")
                metrics.rows_processed = len(df)
                # Create a new buffer for data loading
                load_buffer = StringIO(csv_content)
                logger.info("Loading Data...")
                await self.data_loader.load_data(
                    load_buffer.read(),
                    metrics.table_name,
                    primary_key,
                    file_name,
                    schema,
                )
                logger.info(
                    f"Successfully loaded {metrics.rows_processed} rows into {metrics.table_name}"
                )

            # Update metrics with the number of inserts
            metrics.rows_inserted = metrics.rows_processed

            # Update metrics and mark file as processed
            metrics.end_time = datetime.now()
            metrics.processing_status = "COMPLETED"
            await self.file_tracker.mark_file_processed(
                file_name, file_hash, "COMPLETED", metrics.rows_processed
            )

        except Exception as e:
            logger.error(f"Error processing file {file_name}: {str(e)}", exc_info=True)
            metrics.end_time = datetime.now()
            metrics.processing_status = "FAILED"
            metrics.error_message = str(e)
            await self.file_tracker.mark_file_processed(
                file_name,
                file_hash if "file_hash" in locals() else None,
                "FAILED",
                metrics.rows_processed if "metrics" in locals() else 0,
                str(e),
            )
            raise
        finally:
            await self.metrics_tracker.save_metrics(metrics)

    async def run(self, file_prefix: str = "", primary_key: str = "id") -> None:
        try:
            # Initialize tracking tables
            await self.initialize_tracking_tables()

            files = list(self.s3_client.list_files(file_prefix))
            logger.info(f"Found {len(files)} files to process")

            tasks = []
            semaphore = asyncio.Semaphore(self.config.MAX_WORKERS)

            async def process_with_semaphore(file_name: str) -> None:
                async with semaphore:
                    await self.process_file(file_name, primary_key)

            for file_name in files:
                task = asyncio.create_task(process_with_semaphore(file_name))
                tasks.append(task)

            await asyncio.gather(*tasks)
            logger.info("Pipeline completed successfully")

        except Exception as e:
            logger.error(f"Pipeline failed: {str(e)}")
            raise

    async def shutdown(self):
        """Shutdown all components in the correct order"""
        # Stop the recovery worker first
        if self.recovery_worker:
            self.recovery_worker.stop()

        if self._recovery_task:
            try:
                await asyncio.wait_for(self._recovery_task, timeout=1.0)
            except (asyncio.TimeoutError, Exception):
                if not self._recovery_task.done():
                    self._recovery_task.cancel()
                    try:
                        await self._recovery_task
                    except (asyncio.CancelledError, Exception):
                        pass
            finally:
                self._recovery_task = None

        # Close data loader connections
        if self.data_loader:
            await self.data_loader.close()

    async def cleanup(self):
        """Clean up all resources"""
        await self.shutdown()

        if self._pool:
            await self._pool.close()
            self._pool = None

        self._initialized = False
