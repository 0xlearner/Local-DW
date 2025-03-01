from typing import Dict

from src.logger import setup_logger


class SchemaInferrer:
    def __init__(self):
        self.logger = setup_logger("schema_inferrer")
        self._datetime_formats = [
            "%Y-%m-%d",
            "%Y-%m-%dT%H:%M:%S",
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%dT%H:%M:%S.%f",
            "%Y-%m-%d %H:%M:%S.%f",
        ]

    def infer_pg_type(self, dtype: str) -> str:
        """Infer PostgreSQL type from Polars dtype"""
        dtype = str(dtype).lower()
        if "int" in dtype:
            return "BIGINT"
        elif "float" in dtype:
            return "DOUBLE PRECISION"
        elif "bool" in dtype:
            return "BOOLEAN"
        elif "datetime" in dtype:
            return "TIMESTAMP WITH TIME ZONE"
        elif "date" in dtype:
            return "DATE"
        else:
            return "TEXT"

    async def validate_schema_compatibility(
        self,
        table_name: str,
        new_schema: Dict[str, str],
        default_schema: str = 'bronze'
    ) -> bool:
        """
        Validate if the new schema is compatible with the existing table schema.

        Args:
            table_name: Table name (either 'table' or 'schema.table')
            new_schema: Dictionary of column names and their PostgreSQL types
            default_schema: Default schema name if not provided in table_name

        Returns:
            bool: True if schemas are compatible, False otherwise
        """
        try:
            from src.connection_manager import ConnectionManager

            # Handle both schema-qualified and unqualified table names
            if '.' in table_name:
                schema_name, table = table_name.split('.')
            else:
                schema_name, table = default_schema, table_name

            async with ConnectionManager.get_pool().acquire() as conn:
                # Check if table exists
                table_exists = await conn.fetchval(
                    """
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables
                        WHERE table_schema = $1
                        AND table_name = $2
                    )
                    """,
                    schema_name,
                    table,
                )

                if not table_exists:
                    # If table doesn't exist, schema is compatible
                    self.logger.info(f"Table {schema_name}.{
                                     table} does not exist yet")
                    return True

                # Get existing table schema
                result = await conn.fetch(
                    """
                    SELECT column_name, data_type
                    FROM information_schema.columns
                    WHERE table_schema = $1
                    AND table_name = $2
                    """,
                    schema_name,
                    table,
                )

                existing_schema = {row['column_name']: row['data_type'].upper()
                                   for row in result}

                # Compare schemas
                for col, new_type in new_schema.items():
                    if col in existing_schema:
                        # Convert types to uppercase for comparison
                        new_type = new_type.upper()
                        existing_type = existing_schema[col]

                        if not self._are_types_compatible(existing_type, new_type):
                            self.logger.warning(
                                f"Type mismatch for column {
                                    col} in {schema_name}.{table}: "
                                f"existing={existing_type}, new={new_type}"
                            )
                            return False

                return True

        except Exception as e:
            self.logger.error(f"Error validating schema for {
                              table_name}: {str(e)}")
            raise

    def _are_types_compatible(self, existing_type: str, new_type: str) -> bool:
        """
        Check if two PostgreSQL types are compatible.

        Args:
            existing_type: Existing column type in uppercase
            new_type: New column type in uppercase

        Returns:
            bool: True if types are compatible, False otherwise
        """
        # Define type compatibility rules
        compatible_types = {
            'INTEGER': {'BIGINT', 'INTEGER', 'SMALLINT'},
            'BIGINT': {'BIGINT'},
            'NUMERIC': {'NUMERIC', 'INTEGER', 'BIGINT', 'SMALLINT', 'DOUBLE PRECISION'},
            'TEXT': {'TEXT', 'VARCHAR', 'CHAR'},
            'TIMESTAMP': {'TIMESTAMP', 'TIMESTAMPTZ'},
            'BOOLEAN': {'BOOLEAN'},
            'JSONB': {'JSONB', 'JSON'},
            'JSON': {'JSON', 'JSONB'}
        }

        # If types are exactly the same, they're compatible
        if existing_type == new_type:
            return True

        # Check if the new type is in the set of compatible types
        for base_type, compatible_set in compatible_types.items():
            if existing_type.startswith(base_type):
                return any(new_type.startswith(t) for t in compatible_set)

        return False
