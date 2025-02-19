import asyncio
from typing import List
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


class Pipeline:
    def __init__(self, config: Config):
        self.config = config
        self.s3_client = S3Client(config)
        self.schema_inferrer = SchemaInferrer()
        self._pool = None
        self._recovery_task = None
        self._initialized = False
        self.logger = setup_logger("pipeline")

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
        self.data_loader = DataLoader(
            config, self.metadata_tracker, self.recovery_manager
        )
        # Set data_loader in metadata_tracker
        self.metadata_tracker.set_data_loader(self.data_loader)
        self.recovery_worker = RecoveryWorker(
            config, self.recovery_manager, self.data_loader
        )
        self.data_validator = DataValidator()
        self.metrics_tracker = MetricsTracker(self.conn_params)
        self.file_tracker = FileTracker(self.conn_params)

    async def _initialize_infrastructure_tables(self, conn: asyncpg.Connection):
        """Initialize all infrastructure tables required by the pipeline and its components"""
        await conn.execute(
            """
            -- Table metadata tracking
            CREATE TABLE IF NOT EXISTS table_metadata (
                id SERIAL PRIMARY KEY,
                table_name TEXT NOT NULL,
                processed_at TIMESTAMP,
                total_rows INTEGER DEFAULT 0,
                last_file_processed TEXT,
                schema_snapshot JSONB,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(table_name)
            );

            -- Change history tracking
            CREATE TABLE IF NOT EXISTS change_history (
                id SERIAL PRIMARY KEY,
                table_name TEXT NOT NULL,
                primary_key_column TEXT NOT NULL,
                primary_key_value TEXT NOT NULL,
                column_name TEXT NOT NULL,
                old_value TEXT,
                new_value TEXT,
                change_type TEXT NOT NULL,
                file_name TEXT NOT NULL,
                batch_id TEXT NOT NULL,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
            );

            -- Processed files tracking
            CREATE TABLE IF NOT EXISTS processed_files (
                id SERIAL PRIMARY KEY,
                file_name VARCHAR(255) NOT NULL,
                file_hash VARCHAR(64),
                status VARCHAR(20),
                rows_processed INTEGER,
                error_message TEXT,
                processed_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                CONSTRAINT processed_files_file_name_key UNIQUE (file_name)
            );

            -- Pipeline metrics
            CREATE TABLE IF NOT EXISTS pipeline_metrics (
                id SERIAL PRIMARY KEY,
                file_name TEXT NOT NULL,
                table_name TEXT NOT NULL,
                start_time TIMESTAMP WITH TIME ZONE,
                end_time TIMESTAMP WITH TIME ZONE,
                processing_status TEXT,
                rows_processed INTEGER DEFAULT 0,
                rows_inserted INTEGER DEFAULT 0,
                rows_updated INTEGER DEFAULT 0,
                rows_failed INTEGER DEFAULT 0,
                file_size_bytes BIGINT,
                error_message TEXT,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
            );

            -- Merge history
            CREATE TABLE IF NOT EXISTS merge_history (
                id SERIAL PRIMARY KEY,
                table_name TEXT NOT NULL,
                file_name TEXT NOT NULL,
                started_at TIMESTAMP WITH TIME ZONE NOT NULL,
                completed_at TIMESTAMP WITH TIME ZONE,
                status TEXT NOT NULL,
                batch_id TEXT NOT NULL,
                rows_inserted INTEGER DEFAULT 0,
                rows_updated INTEGER DEFAULT 0,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
            );

            -- Recovery points
            CREATE TABLE IF NOT EXISTS recovery_points (
                id SERIAL PRIMARY KEY,
                table_name TEXT NOT NULL,
                file_name TEXT NOT NULL,
                batch_id TEXT NOT NULL,
                checkpoint_data JSONB NOT NULL,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                status TEXT NOT NULL,
                retry_count INTEGER DEFAULT 0,
                last_error TEXT,
                next_retry_at TIMESTAMP WITH TIME ZONE,
                UNIQUE(table_name, file_name, batch_id)
            );
        """
        )
        self.logger.info("Successfully initialized all infrastructure tables")

    async def initialize(self):
        """Initialize the pipeline and its components"""
        if self._initialized:
            return

        # Initialize single connection pool
        self._pool = await asyncpg.create_pool(
            **self.conn_params, min_size=2, max_size=10, command_timeout=60
        )

        # Share pool with components
        self.metadata_tracker.set_pool(self._pool)
        self.recovery_manager.set_pool(self._pool)
        self.data_loader.set_pool(self._pool)
        self.recovery_worker.set_pool(self._pool)
        self.metrics_tracker.set_pool(self._pool)
        self.file_tracker.set_pool(self._pool)

        # Initialize all infrastructure tables first
        async with self._pool.acquire() as conn:
            await self._initialize_infrastructure_tables(conn)

        # Initialize components (they no longer need to create their own tables)
        await self.data_loader.initialize()

        # Start recovery worker
        self._recovery_task = asyncio.create_task(self.recovery_worker.start())
        self._initialized = True
        self.logger.info("Pipeline initialization completed successfully")

    def _get_table_name(self, file_name: str) -> str:
        # Extract base name from file path and remove extensions
        base_name = os.path.basename(file_name)
        # Remove both .csv and .gz extensions
        base_name = base_name.replace(".csv.gz", "").replace(".csv", "")
        # Add raw_ prefix
        return f"raw_{base_name}"

    def _calculate_file_hash(self, data: str) -> str:
        return hashlib.md5(data.encode()).hexdigest()

    async def process_file(
        self,
        file_name: str,
        primary_key: str,
        merge_strategy: str,
        target_table: str = None,
    ) -> str:
        """
        Process a single file with the specified merge strategy.

        Args:
            file_name: Name of the file to process
            primary_key: Primary key column for merge operations
            merge_strategy: One of "INSERT", "UPDATE", or "MERGE"
            target_table: Optional target table name. If not provided, will be derived from file_name

        Returns:
            str: The batch ID for this processing run
        """

        # Generate batch_id at the start of processing
        batch_id = str(uuid.uuid4())

        # Use provided target table or derive from file name
        table_name = target_table or self._get_table_name(file_name)

        # If using a target table, don't create file-specific tables
        if target_table:
            self.logger.info(f"Using target table: {target_table}")
        else:
            self.logger.info(f"Using file-specific table: {table_name}")

        metrics = PipelineMetrics(
            file_name=file_name,
            table_name=table_name,
            start_time=datetime.now(),
        )

        try:
            # Read and decompress file
            self.logger.info(f"Reading file {file_name} from S3")
            csv_data = self.s3_client.read_gz_file(file_name)

            if csv_data.getbuffer().nbytes == 0:
                raise ValueError(
                    f"Empty or invalid data received from file {file_name}"
                )

            # Calculate hash from the binary data
            file_hash = hashlib.md5(csv_data.getvalue()).hexdigest()
            self.logger.debug(f"File hash: {file_hash}")

            # Check if file was already processed with the same target table
            if await self.file_tracker.is_file_processed(file_name, file_hash):
                self.logger.info(
                    f"File {file_name} was already processed for table {table_name}, skipping"
                )
                return

            # Update metrics
            metrics.file_size_bytes = csv_data.getbuffer().nbytes
            self.logger.info(
                f"Processing file {file_name} of size {metrics.file_size_bytes} bytes"
            )

            # Read CSV data for schema inference
            csv_content = csv_data.read().decode("utf-8")
            csv_buffer = StringIO(csv_content)
            df = pl.read_csv(csv_buffer)

            if df.is_empty():
                raise ValueError(f"No data found in file {file_name}")
            self.logger.info(f"Successfully read CSV with {len(df)} rows")

            # Schema inference
            schema_buffer = StringIO(csv_content)
            schema = self.schema_inferrer.infer_schema(schema_buffer.read())
            self.logger.debug(f"Inferred schema: {schema}")

            # Create table and load data
            async with await self.data_loader.connect() as conn:
                # Check if table exists
                table_exists = await conn.fetchval(
                    """
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables 
                        WHERE table_name = $1
                    )
                    """,
                    table_name,
                )

                if not table_exists:
                    # Create table with inferred schema
                    await self.schema_inferrer.create_table_if_not_exists(
                        conn, table_name, schema, primary_key
                    )
                    self.logger.info(f"Created table: {table_name}")

                # Validate data before loading
                if not self.data_validator.validate_data(df, schema):
                    validation_errors = json.dumps(
                        self.data_validator.validation_errors
                    )
                    self.logger.error(
                        f"Data validation failed for {file_name}: {validation_errors}"
                    )
                    raise ValueError(f"Data validation failed: {validation_errors}")

                metrics.rows_processed = len(df)
                self.logger.info(f"Loading data into {table_name}...")
                await self.data_loader.load_data(
                    df=df,
                    table_name=table_name,
                    primary_key=primary_key,
                    file_name=file_name,
                    batch_id=batch_id,
                    merge_strategy=merge_strategy,
                )
                self.logger.info(
                    f"Successfully loaded {metrics.rows_processed} rows into {table_name}"
                )

            # Update metrics with the number of inserts
            metrics.rows_inserted = metrics.rows_processed

            # Update metrics and mark file as processed
            metrics.end_time = datetime.now()
            metrics.processing_status = "COMPLETED"
            await self.file_tracker.mark_file_processed(
                file_name, file_hash, "COMPLETED", metrics.rows_processed
            )

            # Return batch_id here, after successful completion but before exception handling
            return batch_id

        except Exception as e:
            self.logger.error(
                f"Error processing file {file_name}: {str(e)}", exc_info=True
            )
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

    async def run(
        self,
        file_prefix: str,
        primary_key: str,
        merge_strategy: str = "MERGE",
        target_table: str = None,
    ) -> str:
        """
        Run the pipeline with the specified merge strategy.

        Args:
            file_prefix: Name of the file to process
            primary_key: Primary key column for merge operations
            merge_strategy: One of "INSERT", "UPDATE", or "MERGE"
            target_table: Optional target table name. If not provided, will be derived from file_prefix

        Returns:
            str: The batch ID for this pipeline run
        """
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

    async def shutdown(self):
        """Shutdown all components in the correct order"""
        if not self._initialized:
            return

        # Stop the recovery worker first
        if self.recovery_worker:
            await self.recovery_worker.stop()

        if self._recovery_task:
            try:
                await asyncio.wait_for(self._recovery_task, timeout=5.0)
            except (asyncio.TimeoutError, Exception):
                if not self._recovery_task.done():
                    self._recovery_task.cancel()
                    try:
                        await self._recovery_task
                    except (asyncio.CancelledError, Exception):
                        pass
            finally:
                self._recovery_task = None

        # Close component connections
        if self.data_loader:
            await self.data_loader.close()

        # Close the pool last
        if self._pool:
            await self._pool.close()
            self._pool = None

        self._initialized = False

    async def cleanup(self):
        """Clean up all resources"""
        await self.shutdown()

    async def get_load_summary(self, table_name: str = None) -> dict:
        """
        Get summary metrics for data loading operations from the database.

        Args:
            table_name: Optional table name to filter the metrics

        Returns:
            dict: Summary of loading operations including total files, records, inserts, and updates
        """
        async with self._pool.acquire() as conn:
            # Get metrics from pipeline_metrics table
            metrics_query = """
                SELECT 
                    COUNT(DISTINCT file_name) as total_files_processed,
                    SUM(rows_processed) as total_records_processed,
                    SUM(rows_inserted) as total_inserts,
                    SUM(rows_updated) as total_updates,
                    SUM(rows_failed) as total_failures
                FROM pipeline_metrics
                WHERE ($1::text IS NULL OR table_name = $1)
                AND processing_status = 'COMPLETED'
            """
            metrics = await conn.fetchrow(metrics_query, table_name)

            # Get metrics from change_history
            changes_query = """
                SELECT 
                    change_type,
                    COUNT(*) as count
                FROM change_history
                WHERE ($1::text IS NULL OR table_name = $1)
                GROUP BY change_type
            """
            changes = await conn.fetch(changes_query, table_name)
            changes_dict = {row["change_type"]: row["count"] for row in changes}

            # Use change_history counts as the source of truth
            total_inserts = changes_dict.get("INSERT", 0)
            total_updates = changes_dict.get("UPDATE", 0)

            return {
                "total_files_processed": metrics["total_files_processed"] or 0,
                "total_records_processed": metrics["total_records_processed"] or 0,
                "total_inserts": total_inserts,  # Use change_history count
                "total_updates": total_updates,  # Use change_history count
                "total_failures": metrics["total_failures"] or 0,
                "change_history": {"inserts": total_inserts, "updates": total_updates},
            }

    async def generate_load_report(
        self, batch_ids: List[str] = None, table_name: str = None
    ) -> dict:
        """Generate a detailed JSON report of data loading operations."""
        try:
            # Get summary metrics from database
            summary = await self.get_load_summary(table_name)

            report = {
                "report_generated_at": "2025-02-18 17:27:57",  # Using the provided UTC time
                "generated_by": "0xlearner",  # Using the provided user login
                "table_name": table_name,
                "summary": summary,  # Use the complete summary from get_load_summary
                "operations": [],
            }

            async with self._pool.acquire() as conn:
                # Get pipeline metrics
                metrics_query = """
                    SELECT 
                        file_name,
                        table_name,
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
                    ORDER BY start_time DESC
                """
                metrics_records = await conn.fetch(metrics_query, table_name)

                # Get change history
                changes_query = """
                    SELECT 
                        table_name,
                        primary_key_column,
                        primary_key_value,
                        column_name,
                        old_value,
                        new_value,
                        change_type,
                        file_name,
                        batch_id,
                        created_at
                    FROM change_history
                    WHERE ($1::text IS NULL OR table_name = $1)
                        AND ($2::text[] IS NULL OR batch_id = ANY($2))
                    ORDER BY created_at DESC
                """
                changes_records = await conn.fetch(changes_query, table_name, batch_ids)

                # Process metrics records
                file_operations = {}
                for record in metrics_records:
                    file_name = record["file_name"]
                    if file_name not in file_operations:
                        file_operations[file_name] = {
                            "file_name": file_name,
                            "table_name": record["table_name"],
                            "started_at": record["start_time"].strftime(
                                "%Y-%m-%d %H:%M:%S"
                            ),
                            "completed_at": (
                                record["end_time"].strftime("%Y-%m-%d %H:%M:%S")
                                if record["end_time"]
                                else None
                            ),
                            "status": record["processing_status"],
                            "statistics": {
                                "records_processed": record["rows_processed"],
                                "records_inserted": record["rows_inserted"],
                                "records_updated": record["rows_updated"],
                                "records_failed": record["rows_failed"],
                                "file_size_bytes": record["file_size_bytes"],
                            },
                            "changes": [],
                        }

                # Process change history
                for change in changes_records:
                    file_name = change["file_name"]
                    if file_name in file_operations:
                        change_detail = {
                            "operation_type": change["change_type"],
                            "primary_key": {
                                "column": change["primary_key_column"],
                                "value": change["primary_key_value"],
                            },
                            "batch_id": change["batch_id"],
                            "timestamp": change["created_at"].strftime(
                                "%Y-%m-%d %H:%M:%S"
                            ),
                        }

                        # Include value changes for updates
                        if (
                            change["change_type"] == "UPDATE"
                            and change["old_value"]
                            and change["new_value"]
                        ):
                            try:
                                old_data = json.loads(change["old_value"])
                                new_data = json.loads(change["new_value"])

                                # Compare and show only changed values
                                changes = {}
                                for key in old_data:
                                    if (
                                        key in new_data
                                        and old_data[key] != new_data[key]
                                    ):
                                        changes[key] = {
                                            "old": old_data[key],
                                            "new": new_data[key],
                                        }

                                if changes:
                                    change_detail["value_changes"] = changes
                            except json.JSONDecodeError:
                                # If not JSON, store raw values
                                change_detail["value_changes"] = {
                                    "old": change["old_value"],
                                    "new": change["new_value"],
                                }

                        file_operations[file_name]["changes"].append(change_detail)

                # Add all operations to the report
                report["operations"] = list(file_operations.values())

                return report

        except Exception as e:
            self.logger.error(f"Error generating load report: {str(e)}")
            raise

    async def save_load_report(
        self, report_path: str, batch_ids: List[str] = None, table_name: str = None
    ) -> None:
        """
        Generate and save a load report to a file.

        Args:
            report_path: Path where to save the report
            batch_ids: Optional list of batch IDs to filter the report
            table_name: Optional table name to filter the report
        """
        try:
            report = await self.generate_load_report(batch_ids, table_name)

            # Ensure directory exists
            os.makedirs(os.path.dirname(os.path.abspath(report_path)), exist_ok=True)

            # Save report with pretty formatting
            with open(report_path, "w") as f:
                json.dump(report, f, indent=2)

            self.logger.info(f"Load report saved to {report_path}")

        except Exception as e:
            self.logger.error(f"Error saving load report: {str(e)}")
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
            async with self._pool.acquire() as conn:
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
                                {"id": record["id"], f"{column}": record[column]}
                            )

                    if failed_records:
                        error_msg = (
                            f"Verification failed for {message}. "
                            f"Found {len(failed_records)} failing records: {failed_records}"
                        )
                        self.logger.error(error_msg)
                        failed_verifications.append(error_msg)
                    else:
                        self.logger.info(
                            f"Successfully verified {len(records)} records for {message}"
                        )

                return len(failed_verifications) == 0

        except Exception as e:
            self.logger.error(f"Error verifying updates: {str(e)}")
            raise
