import asyncpg
import polars as pl
from typing import List, Dict, Any, Optional
import json
from datetime import datetime
from src.config import Config
from src.logger import setup_logger
from src.recover.recovery_manager import RecoveryManager
from src.tracker.metadata_tracker import MetadataTracker


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
        self._pool = None
        self.batch_size = config.BATCH_SIZE
        self.logger = setup_logger("data_loader")

    def set_pool(self, pool: asyncpg.Pool):
        self._pool = pool

    async def initialize(self):
        """No need to create pool, just verify we have one"""
        if not self._pool:
            raise RuntimeError("Pool not set before initialization")

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
        new_data: Dict,
        batch_id: str,
        file_name: str,
    ) -> None:
        """Compare and track changes between old and new data."""
        # Serialize the new data before JSON encoding
        serialized_new_data = self._serialize_record(new_data)

        # Fetch the old record
        old_record = await conn.fetchrow(
            f"SELECT * FROM {table_name} WHERE {primary_key} = $1",
            new_data[primary_key],
        )

        if old_record:
            # Convert old record to dict and serialize it
            old_data = dict(old_record)
            serialized_old_data = self._serialize_record(old_data)

            await conn.execute(
                """
                INSERT INTO change_history (
                    table_name,
                    primary_key_column,
                    primary_key_value,
                    column_name,
                    old_value,
                    new_value,
                    change_type,
                    file_name,
                    batch_id
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                """,
                table_name,
                primary_key,
                str(new_data[primary_key]),
                "*",  # Use "*" to indicate entire row change
                json.dumps(serialized_old_data),
                json.dumps(serialized_new_data),
                "UPDATE",
                file_name,
                batch_id,
            )

    @staticmethod
    def _format_value(value: Any, col_name: str, schema: Dict[str, str]) -> Any:
        """Format value according to column type"""
        logger = setup_logger("data_loader")
        if value is None:
            return None

        pg_type = schema[col_name].upper() if col_name in schema else "TEXT"

        # Handle timestamp/date types
        if any(type_name in pg_type for type_name in ["TIMESTAMP", "DATE", "TIME"]):
            if isinstance(value, str):
                try:
                    # Handle ISO format with timezone
                    if "T" in value or "-" in value or ":" in value:
                        # If it has 'Z' timezone indicator, replace with +00:00
                        if value.endswith("Z"):
                            value = value[:-1] + "+00:00"
                        # Try parsing as ISO format
                        try:
                            return datetime.fromisoformat(value)
                        except ValueError:
                            # Fall back to other formats if ISO parsing fails
                            pass

                    # Handle other common formats
                    for fmt in [
                        "%Y-%m-%d %H:%M:%S",
                        "%Y-%m-%d",
                        "%Y-%m-%dT%H:%M:%S",
                        "%Y-%m-%dT%H:%M:%S.%f",
                    ]:
                        try:
                            return datetime.strptime(value, fmt)
                        except ValueError:
                            continue

                    # Last resort - try again with ISO format but handle exceptions better
                    try:
                        if "+" in value:
                            # Try to handle timezone in a simpler way
                            parts = value.split("+")
                            base_dt = parts[0]
                            return datetime.fromisoformat(base_dt)
                        else:
                            raise ValueError(f"Could not parse timestamp: {value}")
                    except Exception:
                        logger.error(f"Failed to parse timestamp value: {value}")
                        raise ValueError(f"Could not parse timestamp: {value}")

                except ValueError as e:
                    logger.error(
                        f"Error parsing timestamp for {
                            col_name}: {value} - {str(e)}"
                    )
                    raise
            elif isinstance(value, datetime):
                return value
            else:
                raise ValueError(f"Unexpected type for timestamp value: {type(value)}")

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

    def _serialize_record(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """Convert record values to JSON-serializable format"""
        serialized = {}
        for key, value in record.items():
            if isinstance(value, datetime):
                serialized[key] = value.isoformat()
            else:
                serialized[key] = value
        return serialized

    async def _record_change(
        self,
        conn,
        table_name: str,
        change_type: str,
        new_record: dict,
        batch_id: str,
        primary_key: str,
        file_name: str,
        old_record: dict = None,
    ) -> None:
        """Record a change in the change_history table"""
        try:
            # Serialize records before JSON encoding
            serialized_new = self._serialize_record(new_record)
            serialized_old = self._serialize_record(old_record) if old_record else None

            await conn.execute(
                """
                INSERT INTO change_history (
                    table_name, 
                    primary_key_column, 
                    primary_key_value,
                    column_name,
                    old_value,
                    new_value,
                    change_type,
                    file_name,
                    batch_id
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                """,
                table_name,
                primary_key,
                str(serialized_new[primary_key]),
                "*",  # Use "*" to indicate entire row change
                json.dumps(serialized_old) if serialized_old else None,
                json.dumps(serialized_new),
                change_type,
                file_name,
                batch_id,
            )
        except Exception as e:
            self.logger.error(
                f"Error recording change history for {table_name}: {str(e)}"
            )
            raise

    def get_type_cast(self, col: str) -> str:
        """Get PostgreSQL type cast for a column based on its schema type."""
        if col not in self.schema:
            return ""

        pg_type = self.schema[col].upper()
        if "TIMESTAMP" in pg_type:
            return "::timestamp"
        elif "JSON" in pg_type or "JSONB" in pg_type:
            return "::jsonb"
        elif "ARRAY" in pg_type or pg_type.endswith("[]"):
            return "::text[]"
        elif "NUMERIC" in pg_type:
            return "::numeric"
        elif "INTEGER" in pg_type or "BIGINT" in pg_type:
            return "::bigint"
        elif "BOOLEAN" in pg_type:
            return "::boolean"
        else:
            return ""  # Default to no type cast for TEXT and other types

    async def _process_batch(
        self,
        conn: asyncpg.Connection,
        table_name: str,
        columns: List[str],
        records: List[Dict],
        batch_id: str,
        primary_key: str,
        file_name: str,
        merge_strategy: str = "MERGE",
    ):
        """Process a batch of records with the specified merge strategy"""
        column_types = await self._get_column_types(conn, table_name)

        # Format values for each record
        formatted_records = []
        for record in records:
            formatted_record = {}
            for col in columns:
                try:
                    value = record.get(col)
                    formatted_value = self._format_value(value, col, column_types)
                    formatted_record[col] = formatted_value
                except Exception as e:
                    self.logger.error(
                        f"Error formatting value for column {col}: {str(e)}"
                    )
                    raise
            formatted_records.append(formatted_record)

        # Generate placeholders for SQL query
        placeholders = [f"${i+1}" for i in range(len(columns))]

        # Create INSERT statement
        insert_sql = f"""
            INSERT INTO {table_name} ({', '.join(columns)})
            VALUES ({', '.join(placeholders)})
        """

        # Execute for each formatted record
        for record in formatted_records:
            values = [record[col] for col in columns]
            try:
                await conn.execute(insert_sql, *values)
            except Exception as e:
                self.logger.error(f"Error executing insert: {str(e)}")
                self.logger.error(f"Values: {values}")
                raise

    async def _get_column_types(
        self, conn: asyncpg.Connection, table_name: str
    ) -> Dict[str, str]:
        """Get PostgreSQL column types for a given table."""
        types = {}
        query = """
            SELECT column_name, data_type, udt_name
            FROM information_schema.columns
            WHERE table_name = $1
        """
        rows = await conn.fetch(query, table_name)
        for row in rows:
            # Use udt_name for more specific type information
            types[row["column_name"]] = (
                f"{row['data_type']}"
                if row["data_type"] != "USER-DEFINED"
                else row["udt_name"]
            )
        return types

    async def load_batch(
        self,
        conn: asyncpg.Connection,
        table_name: str,
        formatted_batch: List[Dict],
        schema: Dict[str, str],
        primary_key: str,
        merge_strategy: str,
        file_name: str,
        batch_id: str,
    ) -> None:
        """Load a batch of records into the database."""
        if not formatted_batch:
            return

        columns = list(formatted_batch[0].keys())
        column_types = {col: schema[col] for col in columns if col in schema}

        for record in formatted_batch:
            # First, format the values based on their types
            formatted_record = {}
            for col, value in record.items():
                if (
                    col in schema
                    and "TIMESTAMP" in schema[col].upper()
                    and isinstance(value, str)
                ):
                    try:
                        # Convert string timestamps to datetime objects
                        formatted_record[col] = self._format_value(value, col, schema)
                    except Exception as e:
                        self.logger.error(
                            f"Error formatting timestamp {
                                          value}: {str(e)}"
                        )
                        raise
                else:
                    formatted_record[col] = value

            # Fetch the existing record (if any) BEFORE making any changes
            existing_record = await conn.fetchrow(
                f"SELECT * FROM {table_name} WHERE {primary_key} = ${
                    1}::{column_types.get(primary_key, 'text')}",
                formatted_record[primary_key],
            )

            if merge_strategy == "UPDATE":
                if existing_record:
                    # Store the old record before updating
                    old_record = dict(existing_record)

                    # Perform the update
                    non_pk_columns = [col for col in columns if col != primary_key]
                    update_sets = [
                        f"{col} = ${i+2}::{column_types.get(col, 'text')}"
                        for i, col in enumerate(non_pk_columns)
                    ]
                    update_sql = f"""
                        UPDATE {table_name}
                        SET {', '.join(update_sets)}
                        WHERE {primary_key} = $1::{column_types.get(primary_key, 'text')}
                    """
                    values = [formatted_record[primary_key]]
                    values.extend(formatted_record[col] for col in non_pk_columns)
                    await conn.execute(update_sql, *values)

                    # Record the change with correct old and new values
                    await self._record_change(
                        conn,
                        table_name,
                        "UPDATE",
                        formatted_record,
                        batch_id,
                        primary_key,
                        file_name,
                        old_record=old_record,
                    )
                # Remove the else clause that was inserting new records
                # When using UPDATE strategy, we should only update existing records
            elif merge_strategy == "INSERT":
                # For INSERT strategy, just insert the record
                placeholders = [
                    f'${i+1}::{column_types.get(col, "text")}'
                    for i, col in enumerate(columns)
                ]
                insert_sql = f"""
                    INSERT INTO {table_name} ({', '.join(columns)})
                    VALUES ({', '.join(placeholders)})
                """
                await conn.execute(
                    insert_sql, *[formatted_record[col] for col in columns]
                )

            else:  # MERGE
                # For MERGE strategy, use INSERT ON CONFLICT
                placeholders = [
                    f'${i+1}::{column_types.get(col, "text")}'
                    for i, col in enumerate(columns)
                ]
                merge_sql = f"""
                    INSERT INTO {table_name} ({', '.join(columns)})
                    VALUES ({', '.join(placeholders)})
                    ON CONFLICT ({primary_key})
                    DO UPDATE SET {', '.join(
                    f"{col} = EXCLUDED.{col}"
                    for col in columns
                    if col != primary_key
                )}
                """
                await conn.execute(
                    merge_sql, *[formatted_record[col] for col in columns]
                )

            # Record the change in change_history
            if merge_strategy == "MERGE":
                change_type = "UPDATE" if existing_record else "INSERT"
            else:
                change_type = merge_strategy

            await self._record_change(
                conn,
                table_name,
                change_type,
                formatted_record,
                batch_id,
                primary_key,
                file_name,
            )

    async def load_data(
        self,
        df: pl.DataFrame,
        table_name: str,
        primary_key: str,
        file_name: str,
        batch_id: str,
        merge_strategy: str = "MERGE",
    ) -> int:
        """Load data into the target table with the specified merge strategy

        Args:
            df: Polars DataFrame containing the data to load
            table_name: Name of the target table
            primary_key: Name of the primary key column
            file_name: Name of the source file
            batch_id: Unique identifier for this batch
            merge_strategy: Strategy to use for loading ("INSERT", "UPDATE", or "MERGE")

        Returns:
            int: Number of records processed
        """
        self.logger.info(
            f"Starting load_data for table {
                table_name} with batch_id {batch_id}"
        )

        async with self._pool.acquire() as conn:
            columns = df.columns

            # Get column types from the database
            schema = await self._get_column_types(conn, table_name)

            # Convert from Polars DataFrame to Python dicts
            records = df.to_dicts()

            # Adjust any datetime strings to be proper datetime objects
            formatted_records = []
            for record in records:
                formatted_record = {}
                for col, value in record.items():
                    if (
                        col in schema
                        and "TIMESTAMP" in schema[col].upper()
                        and isinstance(value, str)
                    ):
                        try:
                            formatted_record[col] = self._format_value(
                                value, col, schema
                            )
                        except Exception as e:
                            self.logger.error(
                                f"Error formatting timestamp {
                                              value}: {str(e)}"
                            )
                            # Instead of completely failing, try to provide the original value
                            self.logger.warning(
                                f"Using original value for {col}: {value}"
                            )
                            formatted_record[col] = value
                    else:
                        formatted_record[col] = value
                formatted_records.append(formatted_record)

            records = formatted_records

            if merge_strategy == "UPDATE":
                # For UPDATE strategy, filter out records that don't exist
                existing_records = []
                missing_records = []

                for record in records:
                    exists = await conn.fetchval(
                        f"SELECT EXISTS(SELECT 1 FROM {table_name} WHERE {
                            primary_key} = $1)",
                        record[primary_key],
                    )
                    if exists:
                        existing_records.append(record)
                    else:
                        missing_records.append(record[primary_key])

                if missing_records:
                    self.logger.warning(
                        f"Skipping {len(missing_records)} records that don't exist in {
                            table_name}. "
                        f"Primary keys: {missing_records}"
                    )

                records = existing_records

                if not records:
                    self.logger.warning(
                        f"No existing records found to update in {table_name}"
                    )
                    return 0

                # Process updates
                for record in records:
                    # Get existing record
                    old_record = await conn.fetchrow(
                        f"SELECT * FROM {table_name} WHERE {primary_key} = $1",
                        record[primary_key],
                    )

                    # Prepare update statement
                    non_pk_columns = [col for col in columns if col != primary_key]
                    update_sets = [
                        f"{col} = ${i+2}" for i, col in enumerate(non_pk_columns)
                    ]
                    update_sql = f"""
                        UPDATE {table_name}
                        SET {', '.join(update_sets)}
                        WHERE {primary_key} = $1
                    """

                    # Execute update
                    values = [record[primary_key]]
                    values.extend(record[col] for col in non_pk_columns)
                    await conn.execute(update_sql, *values)

                    # Record the change
                    await self._record_change(
                        conn,
                        table_name,
                        "UPDATE",
                        record,
                        batch_id,
                        primary_key,
                        file_name,
                        old_record=dict(old_record) if old_record else None,
                    )

                return len(records)

            elif merge_strategy == "INSERT":
                for record in records:
                    placeholders = [f"${i+1}" for i in range(len(columns))]
                    insert_sql = f"""
                        INSERT INTO {table_name} ({', '.join(columns)})
                        VALUES ({', '.join(placeholders)})
                    """
                    values = [record[col] for col in columns]
                    await conn.execute(insert_sql, *values)

                    await self._record_change(
                        conn,
                        table_name,
                        "INSERT",
                        record,
                        batch_id,
                        primary_key,
                        file_name,
                    )

                return len(records)

            else:  # MERGE
                for record in records:
                    # Check if record exists
                    exists = await conn.fetchval(
                        f"SELECT EXISTS(SELECT 1 FROM {table_name} WHERE {
                            primary_key} = $1)",
                        record[primary_key],
                    )

                    if exists:
                        # Update existing record
                        old_record = await conn.fetchrow(
                            f"SELECT * FROM {table_name} WHERE {primary_key} = $1",
                            record[primary_key],
                        )

                        non_pk_columns = [col for col in columns if col != primary_key]
                        update_sets = [
                            f"{col} = ${i+2}" for i, col in enumerate(non_pk_columns)
                        ]
                        update_sql = f"""
                            UPDATE {table_name}
                            SET {', '.join(update_sets)}
                            WHERE {primary_key} = $1
                        """
                        values = [record[primary_key]]
                        values.extend(record[col] for col in non_pk_columns)
                        await conn.execute(update_sql, *values)

                        await self._record_change(
                            conn,
                            table_name,
                            "UPDATE",
                            record,
                            batch_id,
                            primary_key,
                            file_name,
                            old_record=dict(old_record) if old_record else None,
                        )
                    else:
                        # Insert new record
                        placeholders = [f"${i+1}" for i in range(len(columns))]
                        insert_sql = f"""
                            INSERT INTO {table_name} ({', '.join(columns)})
                            VALUES ({', '.join(placeholders)})
                        """
                        values = [record[col] for col in columns]
                        await conn.execute(insert_sql, *values)

                        await self._record_change(
                            conn,
                            table_name,
                            "INSERT",
                            record,
                            batch_id,
                            primary_key,
                            file_name,
                        )

                return len(records)
