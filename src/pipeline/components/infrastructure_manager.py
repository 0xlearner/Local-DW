import asyncpg
import asyncio

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
        # Only initialize if not already initialized
        if hasattr(self, "logger"):
            return
        self.logger = setup_logger("infrastructure_manager")
        self._initialized = False

    async def initialize_infrastructure_tables(self, conn: asyncpg.Connection):
        """Initialize all infrastructure tables required by the pipeline and its components"""
        async with self._lock:
            if self._initialized:
                return

            # Create schema if it doesn't exist
            await conn.execute(
                """
                CREATE SCHEMA IF NOT EXISTS bronze;
                """
            )

            await conn.execute(
                """
                -- Table metadata tracking
                CREATE TABLE IF NOT EXISTS bronze.table_metadata (
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

                -- File processing tracking
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

                -- Create index on batch_id for faster lookups
                CREATE INDEX IF NOT EXISTS idx_processed_files_batch_id
                ON bronze.processed_files(batch_id);

                -- Pipeline metrics
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

                -- Merge history
                CREATE TABLE IF NOT EXISTS bronze.merge_history (
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
                CREATE TABLE IF NOT EXISTS bronze.recovery_points (
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

                 -- Batch processing tracking
                    CREATE TABLE IF NOT EXISTS bronze.batch_processing (
                        id SERIAL PRIMARY KEY,
                        batch_id TEXT NOT NULL,
                        table_name TEXT NOT NULL,
                        file_name TEXT NOT NULL,
                        batch_number INTEGER NOT NULL,
                        total_batches INTEGER NOT NULL,
                        records_in_batch INTEGER NOT NULL,
                        records_processed INTEGER DEFAULT 0,
                        records_inserted INTEGER DEFAULT 0,
                        records_updated INTEGER DEFAULT 0,
                        records_failed INTEGER DEFAULT 0,
                        start_time TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                        end_time TIMESTAMP WITH TIME ZONE,
                        status TEXT NOT NULL,
                        error_message TEXT,
                        processing_duration_seconds FLOAT,
                        created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                        UNIQUE(batch_id, batch_number)
                    );

                -- Create temp tables registry in bronze schema
                CREATE TABLE IF NOT EXISTS bronze.temp_tables_registry (
                    id SERIAL PRIMARY KEY,
                    table_name TEXT NOT NULL,
                    original_table TEXT NOT NULL,
                    batch_id TEXT NOT NULL,
                    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                    last_accessed_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                    status TEXT DEFAULT 'ACTIVE',
                    UNIQUE(table_name)
                );

                -- Create index for cleanup queries
                CREATE INDEX IF NOT EXISTS idx_temp_tables_created_at
                ON bronze.temp_tables_registry(created_at);
            """
            )
            self._initialized = True
            self.logger.info(
                "Successfully initialized all infrastructure tables in bronze schema"
            )
