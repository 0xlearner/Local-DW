import asyncio
import asyncpg
from datetime import datetime, timedelta
from src.logger import setup_logger


class InfrastructureManager:
    _instance = None
    _lock = asyncio.Lock()

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(InfrastructureManager, cls).__new__(cls)
            cls._instance._initialized = False
        return cls._instance

    def __init__(self):
        if hasattr(self, "logger"):
            return
        self.logger = setup_logger("infrastructure_manager")
        self._initialized = False
        self.pool = None

    async def initialize_infrastructure_tables(self, conn: asyncpg.Connection):
        """Initialize all infrastructure tables required by the pipeline and its components"""
        async with self._lock:
            try:
                # Create schema first
                await conn.execute("CREATE SCHEMA IF NOT EXISTS bronze")

                # Create tables registry
                await conn.execute(
                    """
                    CREATE TABLE IF NOT EXISTS bronze.tables_registry (
                        id SERIAL PRIMARY KEY,
                        table_name TEXT NOT NULL,
                        original_table TEXT NOT NULL,
                        batch_id TEXT NOT NULL,
                        created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                        last_accessed_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                        status TEXT DEFAULT 'ACTIVE',
                        UNIQUE(table_name, batch_id)
                    );

                    CREATE INDEX IF NOT EXISTS idx_tables_registry_batch_id
                    ON bronze.tables_registry(batch_id);

                    CREATE INDEX IF NOT EXISTS idx_tables_registry_status
                    ON bronze.tables_registry(status);
                    """
                )

                # Create table metadata tracking
                await conn.execute(
                    """
                    CREATE TABLE IF NOT EXISTS bronze.table_metadata (
                        id SERIAL PRIMARY KEY,
                        table_name TEXT NOT NULL,
                        processed_at TIMESTAMP WITH TIME ZONE,
                        total_rows INTEGER DEFAULT 0,
                        last_file_processed TEXT,
                        schema_snapshot JSONB,
                        created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                        UNIQUE(table_name)
                    );
                    """
                )

                # Create pipeline metrics tracking
                await conn.execute(
                    """
                    CREATE TABLE IF NOT EXISTS bronze.pipeline_metrics (
                        id SERIAL PRIMARY KEY,
                        file_name TEXT NOT NULL,
                        table_name TEXT NOT NULL,
                        batch_id TEXT NOT NULL,
                        start_time TIMESTAMP WITH TIME ZONE,
                        end_time TIMESTAMP WITH TIME ZONE,
                        processing_status TEXT,
                        rows_processed INTEGER DEFAULT 0,
                        rows_inserted INTEGER DEFAULT 0,
                        rows_updated INTEGER DEFAULT 0,
                        rows_failed INTEGER DEFAULT 0,
                        file_size_bytes BIGINT,
                        error_message TEXT,
                        load_duration_seconds FLOAT DEFAULT 0.0,
                        created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
                    );
                    """
                )

                # Create processed files tracking
                await conn.execute(
                    """
                    CREATE TABLE IF NOT EXISTS bronze.processed_files (
                        id SERIAL PRIMARY KEY,
                        file_name TEXT NOT NULL,
                        file_hash TEXT,
                        status TEXT NOT NULL,
                        rows_processed INTEGER NOT NULL DEFAULT 0,
                        error_message TEXT,
                        batch_id TEXT NOT NULL,
                        processed_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                        UNIQUE(file_name, file_hash)
                    );

                    CREATE INDEX IF NOT EXISTS idx_processed_files_batch_id
                    ON bronze.processed_files(batch_id);
                    """
                )

                # Create recovery points table
                await conn.execute(
                    """
                    CREATE TABLE IF NOT EXISTS bronze.recovery_points (
                        id SERIAL PRIMARY KEY,
                        table_name TEXT,
                        file_name TEXT NOT NULL,
                        batch_id TEXT,
                        checkpoint_data JSONB,
                        created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                        status TEXT DEFAULT 'PENDING',
                        last_error TEXT,
                        retry_count INTEGER DEFAULT 0,
                        next_retry_at TIMESTAMP WITH TIME ZONE,
                        UNIQUE(file_name, batch_id)
                    );

                    CREATE INDEX IF NOT EXISTS idx_recovery_points_status
                    ON bronze.recovery_points(status);
                    """
                )

                # Then verify if ALL required tables exist
                required_tables = [
                    'tables_registry',
                    'table_metadata',
                    'pipeline_metrics',
                    'processed_files',
                    'recovery_points'
                ]

                tables_exist = await conn.fetchval("""
                    SELECT bool_and(EXISTS (
                        SELECT FROM information_schema.tables
                        WHERE table_schema = 'bronze'
                        AND table_name = ANY($1::text[])
                    ))
                """, required_tables)

                if tables_exist:
                    self._initialized = True
                    self.logger.info(
                        "All infrastructure tables verified to exist")
                    return

                if not tables_exist:
                    raise Exception(
                        "Failed to verify all required tables were created")

                self._initialized = True
                self.logger.info(
                    "Successfully initialized all infrastructure tables")

            except Exception as e:
                self.logger.error(
                    f"Failed to initialize infrastructure: {str(e)}")
                raise

    async def cleanup_temp_tables(self, retention_days: int = 90) -> None:
        """Clean up temporary tables older than retention period"""
        cutoff_date = datetime.now() - timedelta(days=retention_days)

        try:
            async with self.pool.acquire() as conn:
                # Get list of inactive tables
                tables = await conn.fetch(
                    """
                    SELECT table_name
                    FROM bronze.tables_registry
                    WHERE status = 'INACTIVE'
                    AND last_accessed_at < $1
                    """,
                    cutoff_date
                )

                for table in tables:
                    try:
                        await conn.execute(
                            f"DROP TABLE IF EXISTS {table['table_name']}")
                        self.logger.info(
                            f"Dropped table {table['table_name']}")
                    except Exception as e:
                        self.logger.error(
                            f"Failed to drop table {table['table_name']}: {str(e)}")

        except Exception as e:
            self.logger.error(f"Error during temp table cleanup: {str(e)}")
