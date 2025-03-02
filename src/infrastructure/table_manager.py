import asyncpg

from src.logger import setup_logger


class TableManager:
    def __init__(self):
        self.logger = setup_logger("table_manager")

    async def create_table(self, conn: asyncpg.Connection, table_name: str, schema: dict) -> str:
        """Generate CREATE TABLE SQL with metadata columns"""
        # Split the table_name into schema and table parts
        schema_name, table = table_name.split('.')

        columns_sql = ",\n    ".join(
            f"\"{col_name}\" {col_type}" for col_name, col_type in schema.items()
        )

        # Create the partitioned table with explicit schema
        sql = f"""
        CREATE TABLE IF NOT EXISTS {schema_name}.{table} (
            {columns_sql}
        ) PARTITION BY LIST (_batch_id)
        """
        await conn.execute(sql)

        # Verify table creation
        exists = await conn.fetchval(
            """
                SELECT EXISTS (
                    SELECT FROM pg_tables
                    WHERE schemaname = $1
                    AND tablename = $2
                )
                """,
            schema_name,
            table
        )

        if not exists:
            raise RuntimeError(f"Failed to create table {table_name}")

        self.logger.info(f"Successfully created table {table_name}")
        return table

    async def create_partition(
        self,
        conn: asyncpg.Connection,
        table_name: str,
        batch_id: str
    ) -> None:
        """Create a partition for a specific batch_id"""
        schema_name, table = table_name.split('.')
        partition_name = f"{table}_{batch_id.replace('-', '_')}"

        sql = f"""
        CREATE TABLE IF NOT EXISTS {schema_name}.{partition_name}
        PARTITION OF {schema_name}.{table}
        FOR VALUES IN ('{batch_id}')
        """
        await conn.execute(sql)
        self.logger.info(f"Successfully created partition {
            schema_name}.{partition_name}")
