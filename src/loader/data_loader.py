import uuid
import asyncpg
import polars as pl
from typing import List, Dict, Any
from io import StringIO
import json
from src.config import Config
from src.logger import setup_logger
from src.recover.recovery_manager import RecoveryManager
from src.tracker.metadata_tracker import MetadataTracker

logger = setup_logger("data_loader")


class DataLoader:
    def __init__(
        self,
        config: Config,
        metadata_tracker: MetadataTracker,
        recovery_manager: RecoveryManager,
    ):
        self.config = config
        self.batch_size = config.BATCH_SIZE
        self.metadata_tracker = metadata_tracker
        self.recovery_manager = recovery_manager
        self._pool = None

    async def initialize(self):
        """Initialize connection pool"""
        if self._pool is None:
            self._pool = await asyncpg.create_pool(
                host=self.config.PG_HOST,
                port=self.config.PG_PORT,
                user=self.config.PG_USER,
                password=self.config.PG_PASSWORD,
                database=self.config.PG_DATABASE,
            )

    async def connect(self):
        """Get a connection from the pool"""
        if not self._pool:
            await self.initialize()
        return self._pool.acquire()

    async def close(self):
        """Close the connection pool"""
        if self._pool:
            await self._pool.close()
            self._pool = None

    async def upsert_batch(
        self,
        conn: asyncpg.Connection,
        table_name: str,
        data: List[Dict[str, Any]],
        primary_key: str,
    ) -> None:
        try:
            columns = list(data[0].keys())
            values = [tuple(row[col] for col in columns) for row in data]

            # Generate the upsert query
            set_statements = [
                f"{col} = EXCLUDED.{col}" for col in columns if col != primary_key
            ]

            query = f"""
                INSERT INTO {table_name} ({', '.join(columns)})
                VALUES ($1{', '.join(f'${i}' for i in range(2, len(columns) + 1))})
                ON CONFLICT ({primary_key})
                DO UPDATE SET {', '.join(set_statements)}
                """

            await conn.executemany(query, values)
        except Exception as e:
            logger.error(f"Error upserting batch: {str(e)}")
            raise

    async def _compare_and_track_changes(
        self,
        conn: asyncpg.Connection,
        table_name: str,
        primary_key: str,
        new_data: Dict[str, Any],
        batch_id: str,
        file_name: str,
    ):
        # Fetch existing record
        existing_record = await conn.fetchrow(
            f"""
            SELECT * FROM {table_name}
            WHERE {primary_key} = $1
        """,
            new_data[primary_key],
        )

        if existing_record is None:
            # Track insert
            for column, value in new_data.items():
                await self.metadata_tracker.record_change(
                    table_name,
                    primary_key,
                    str(new_data[primary_key]),
                    column,
                    None,
                    str(value),
                    "INSERT",
                    file_name,
                    batch_id,
                )
        else:
            # Track updates
            existing_dict = dict(existing_record)
            for column, new_value in new_data.items():
                old_value = existing_dict.get(column)
                if old_value != new_value:
                    await self.metadata_tracker.record_change(
                        table_name,
                        primary_key,
                        str(new_data[primary_key]),
                        column,
                        str(old_value),
                        str(new_value),
                        "UPDATE",
                        file_name,
                        batch_id,
                    )

    async def load_data(
        self,
        csv_data: str,
        table_name: str,
        primary_key: str,
        file_name: str,
        schema: Dict[str, Any],
    ) -> None:
        batch_id = str(uuid.uuid4())
        logger.info(
            f"Starting load_data for table {table_name} with batch_id {batch_id}"
        )

        try:
            # Convert string data to StringIO for Polars to read
            csv_buffer = StringIO(csv_data)
            df = pl.read_csv(csv_buffer)

            if df.is_empty():
                raise ValueError(f"No data found in CSV content from {file_name}")

            records = df.to_dicts()
            logger.info(f"Successfully read {len(records)} records from CSV data")

            # Create recovery point
            checkpoint_data = {
                "total_records": len(records),
                "schema": schema,
                "processed_records": 0,
            }
            await self.recovery_manager.create_recovery_point(
                table_name, file_name, checkpoint_data
            )

            async with await self.connect() as conn:
                async with conn.transaction():
                    # Start merge history record
                    await conn.execute(
                        """
                        INSERT INTO merge_history (
                            table_name, file_name, started_at,
                            status, batch_id
                        ) VALUES ($1, $2, CURRENT_TIMESTAMP, 'IN_PROGRESS', $3)
                    """,
                        table_name,
                        file_name,
                        batch_id,
                    )

                    rows_inserted = 0
                    rows_updated = 0

                    # Process data in batches
                    logger.info(
                        f"Processing {len(records)} records in batches of {self.batch_size}"
                    )
                    for i in range(0, len(records), self.batch_size):
                        batch = records[i : i + self.batch_size]
                        logger.debug(
                            f"Processing batch {i//self.batch_size + 1} with {len(batch)} records"
                        )

                        # Process each record to handle array types
                        processed_batch = []
                        for record in batch:
                            processed_record = {}
                            for col, value in record.items():
                                if schema[col].endswith("[]"):
                                    # Handle array string representations
                                    if isinstance(value, str):
                                        if value.startswith("{") and value.endswith(
                                            "}"
                                        ):
                                            # PostgreSQL array format
                                            value = (
                                                value[1:-1].split(",")
                                                if value != "{}"
                                                else []
                                            )
                                        elif value.startswith("[") and value.endswith(
                                            "]"
                                        ):
                                            # JSON array format
                                            value = json.loads(value)
                                        else:
                                            # Single value to array
                                            value = [value] if value else []
                                    elif not isinstance(value, list):
                                        value = [value] if value is not None else []
                                processed_record[col] = value
                            processed_batch.append(processed_record)

                        # Track changes before update
                        for record in processed_batch:
                            await self._compare_and_track_changes(
                                conn,
                                table_name,
                                primary_key,
                                record,
                                batch_id,
                                file_name,
                            )

                        # Prepare the upsert query
                        columns = list(schema.keys())
                        placeholders = [f"${i+1}" for i in range(len(columns))]
                        non_pk_columns = [col for col in columns if col != primary_key]
                        update_stmt = ", ".join(
                            f"{col} = EXCLUDED.{col}" for col in non_pk_columns
                        )

                        query = f"""
                            INSERT INTO {table_name} ({', '.join(columns)})
                            VALUES ({', '.join(placeholders)})
                            ON CONFLICT ({primary_key})
                            DO UPDATE SET {update_stmt}
                            RETURNING 
                                CASE WHEN xmax::text::int > 0 THEN 1 ELSE 0 END as updated,
                                CASE WHEN xmax = 0 THEN 1 ELSE 0 END as inserted
                        """

                        # Execute for each record in the batch
                        for record in processed_batch:
                            values = []
                            for col in columns:
                                value = record[col]
                                if schema[col].endswith("[]"):
                                    # Convert Python list to PostgreSQL array
                                    value = (
                                        value
                                        if isinstance(value, list)
                                        else [value] if value is not None else []
                                    )
                                values.append(value)

                            result = await conn.fetch(query, *values)
                            for row in result:
                                rows_updated += row["updated"]
                                rows_inserted += row["inserted"]

                        # Update recovery point
                        checkpoint_data["processed_records"] = i + len(batch)
                        await self.recovery_manager.update_recovery_status(
                            batch_id, "PROCESSED"
                        )
                        logger.debug(
                            f"Completed batch with {len(batch)} records. Total: {rows_inserted} inserted, {rows_updated} updated"
                        )

                    # Update merge history
                    await conn.execute(
                        """
                        UPDATE merge_history
                        SET completed_at = CURRENT_TIMESTAMP,
                            status = 'COMPLETED',
                            rows_inserted = $1,
                            rows_updated = $2
                        WHERE batch_id = $3
                    """,
                        rows_inserted,
                        rows_updated,
                        batch_id,
                    )

                    # Update table metadata
                    await self.metadata_tracker.update_table_metadata(
                        table_name, file_name, len(records), schema
                    )

                    logger.info(
                        f"Successfully loaded data: {rows_inserted} rows inserted, {rows_updated} rows updated"
                    )

        except Exception as e:
            logger.error(f"Error loading data: {str(e)}", exc_info=True)
            await self.recovery_manager.update_recovery_status(
                batch_id, "FAILED", str(e)
            )
            raise
