import ast
import uuid
import asyncpg
import polars as pl
from typing import List, Dict, Any
from io import StringIO
import json
from datetime import datetime
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
        self.schema = {}  # Will be populated during load_data

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

    async def _compare_and_track_changes(
        self,
        conn: asyncpg.Connection,
        table_name: str,
        primary_key: str,
        new_data: Dict[str, Any],
        batch_id: str,
        file_name: str,
    ):
        logger.info(
            f"Starting change tracking for {table_name}, primary_key={new_data[primary_key]}, batch_id={batch_id}"
        )
        # Fetch existing record
        existing_record = await conn.fetchrow(
            f"""
            SELECT * FROM {table_name}
            WHERE {primary_key} = $1
            FOR UPDATE
            """,
            new_data[primary_key],
        )

        if existing_record is None:
            logger.info(
                f"Recording INSERT for {table_name}, primary_key={new_data[primary_key]}"
            )
            # Track insert - do it for each column
            for column, value in new_data.items():
                await conn.execute(
                    """
                    INSERT INTO change_history (
                        table_name, primary_key_column, primary_key_value,
                        column_name, old_value, new_value, change_type,
                        file_name, batch_id
                    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                    """,
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
            # Convert record to dictionary for comparison
            existing_dict = dict(existing_record)
            updates_found = False

            for column, new_value in new_data.items():
                old_value = existing_dict.get(column)
                if str(old_value) != str(
                    new_value
                ):  # Convert both to string for comparison
                    updates_found = True
                    await conn.execute(
                        """
                        INSERT INTO change_history (
                            table_name, primary_key_column, primary_key_value,
                            column_name, old_value, new_value, change_type,
                            file_name, batch_id
                        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                        """,
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

            if updates_found:
                logger.info(
                    f"Recording UPDATE for {table_name}, primary_key={new_data[primary_key]}"
                )

    @staticmethod
    def _format_value(value: Any, col_name: str, schema: Dict[str, str]) -> Any:
        """Format value according to column type"""
        if value is None:
            return None

        pg_type = schema[col_name].upper()

        # Handle timestamp/date types
        if any(type_name in pg_type for type_name in ["TIMESTAMP", "DATE", "TIME"]):
            if isinstance(value, str):
                try:
                    return datetime.fromisoformat(value.replace("Z", "+00:00"))
                except ValueError as e:
                    logger.error(
                        f"Error parsing timestamp for {col_name}: {value} - {str(e)}"
                    )
                    raise

        # Handle JSON/JSONB type
        if any(json_type in pg_type for json_type in ["JSON", "JSONB"]):
            if isinstance(value, str):
                try:
                    # Validate JSON string
                    json.loads(value)
                    return value
                except json.JSONDecodeError:
                    return json.dumps(value)
            return json.dumps(value)

        return value

    def _serialize_row(self, row: Dict[str, Any]) -> Dict[str, str]:
        """Convert row values to JSON-serializable format"""
        serialized = {}
        for key, value in row.items():
            if isinstance(value, datetime):
                serialized[key] = value.isoformat()
            else:
                serialized[key] = value
        return serialized

    async def load_batch(
        self,
        conn,
        table_name: str,
        data: List[Dict],
        primary_key: str,
        batch_id: str,
        file_name: str,
    ):
        try:
            columns = list(data[0].keys())
            processed_data = []

            # Process the data first
            for row in data:
                processed_row = {}
                for col, val in row.items():
                    if col not in self.schema:
                        logger.warning(f"Column {col} not found in schema")
                        processed_row[col] = val
                        continue

                    col_type = self.schema[col].upper()
                    logger.debug(f"Processing column {col} with type {col_type}")

                    # Handle timestamp/datetime columns
                    if "TIMESTAMP" in col_type:
                        if isinstance(val, str):
                            try:
                                processed_row[col] = datetime.fromisoformat(
                                    val.replace("Z", "+00:00")
                                )
                            except ValueError as e:
                                logger.error(f"Failed to parse datetime: {val}")
                                raise ValueError(
                                    f"Invalid datetime format: {val}"
                                ) from e
                        else:
                            processed_row[col] = val
                    # Handle JSON/JSONB columns
                    elif "JSON" in col_type:
                        if isinstance(val, str):
                            try:
                                json.loads(val)
                                processed_row[col] = val
                            except json.JSONDecodeError:
                                processed_row[col] = json.dumps(val)
                        else:
                            processed_row[col] = json.dumps(val)
                    else:
                        processed_row[col] = val

                processed_data.append(processed_row)

            # Track changes before UPSERT
            for row in processed_data:
                # Check if record exists
                existing = await conn.fetchrow(
                    f"""
                    SELECT * FROM {table_name}
                    WHERE {primary_key} = $1
                    """,
                    row[primary_key],
                )

                if existing is None:
                    logger.info(
                        f"Recording INSERT for {table_name}, primary_key={row[primary_key]}"
                    )
                    # Single INSERT record for the whole row
                    await conn.execute(
                        """
                        INSERT INTO change_history (
                            table_name, primary_key_column, primary_key_value,
                            column_name, old_value, new_value, change_type,
                            file_name, batch_id
                        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                        """,
                        table_name,
                        primary_key,
                        str(row[primary_key]),
                        "*",  # Use '*' to indicate all columns
                        None,
                        json.dumps(
                            self._serialize_row(row)
                        ),  # Serialize row before JSON conversion
                        "INSERT",
                        file_name,
                        batch_id,
                    )
                else:
                    # Handle updates
                    existing_dict = dict(existing)
                    updates = {}

                    for column, new_value in row.items():
                        old_value = existing_dict.get(column)
                        if str(old_value) != str(new_value):
                            updates[column] = {
                                "old": str(old_value),
                                "new": str(new_value),
                            }

                    if updates:
                        logger.info(
                            f"Recording UPDATE for {table_name}, primary_key={row[primary_key]}"
                        )
                        await conn.execute(
                            """
                            INSERT INTO change_history (
                                table_name, primary_key_column, primary_key_value,
                                column_name, old_value, new_value, change_type,
                                file_name, batch_id
                            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                            """,
                            table_name,
                            primary_key,
                            str(row[primary_key]),
                            "*",  # Use '*' to indicate all columns
                            json.dumps(
                                self._serialize_row(existing_dict)
                            ),  # Serialize existing row
                            json.dumps(updates),  # Updates are already strings
                            "UPDATE",
                            file_name,
                            batch_id,
                        )

            # Now perform the UPSERT
            values = [tuple(row[col] for col in columns) for row in processed_data]
            param_placeholders = [f"${i+1}" for i in range(len(columns))]
            set_statements = [
                f"{col} = EXCLUDED.{col}" for col in columns if col != primary_key
            ]

            query = f"""
                INSERT INTO {table_name} ({', '.join(columns)})
                VALUES ({', '.join(param_placeholders)})
                ON CONFLICT ({primary_key})
                DO UPDATE SET {', '.join(set_statements)}
            """

            logger.debug(f"Executing query with first row: {values[0]}")
            await conn.executemany(query, values)

        except Exception as e:
            logger.error(f"Error upserting batch: {str(e)}")
            if "values" in locals():
                logger.error(f"Failed values (first row): {values[0]}")
                logger.error(f"Schema types for failed row:")
                for col, val in zip(columns, values[0]):
                    logger.error(
                        f"Column: {col}, Value: {val}, Type: {type(val)}, Schema Type: {self.schema.get(col, 'UNKNOWN')}"
                    )
            raise

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

        # Store schema for use in load_batch
        self.schema = schema

        try:
            # Convert string data to StringIO for Polars to read
            csv_buffer = StringIO(csv_data)
            df = pl.read_csv(csv_buffer)

            if df.is_empty():
                raise ValueError(f"No data found in CSV content from {file_name}")

            # Convert DataFrame to list of dictionaries
            records = df.to_dicts()

            # Format values according to schema
            formatted_records = []
            for record in records:
                formatted_record = {}
                for col_name, value in record.items():
                    if col_name in schema:
                        pg_type = schema[col_name].upper()
                        if "TIMESTAMP" in pg_type and isinstance(value, str):
                            # Convert timestamp strings to datetime objects
                            formatted_record[col_name] = datetime.fromisoformat(
                                value.replace("Z", "+00:00")
                            )
                        else:
                            formatted_record[col_name] = self._format_value(
                                value, col_name, schema
                            )
                    else:
                        formatted_record[col_name] = value
                formatted_records.append(formatted_record)

            # Process in batches
            batch_size = self.batch_size
            for i in range(0, len(formatted_records), batch_size):
                batch = formatted_records[i : i + batch_size]
                async with self._pool.acquire() as conn:
                    await self.load_batch(
                        conn,
                        table_name,
                        batch,
                        primary_key,
                        batch_id,
                        file_name,
                    )

            logger.info(
                f"Successfully loaded {len(formatted_records)} records into {table_name}"
            )

        except Exception as e:
            logger.error(f"Error loading data: {str(e)}")
            raise
