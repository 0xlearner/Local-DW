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

        # Create the table with explicit schema
        sql = f"""
        CREATE TABLE IF NOT EXISTS {schema_name}.{table} (
            {columns_sql}
        )
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
        return table  # Return the table name without schema for copy_records_to_table

    async def create_view(
        self, conn: asyncpg.Connection, view_name: str, source_table: str, batch_id: str
    ) -> None:
        """Create or replace view after dropping if exists"""
        schema_name, view = view_name.split('.')

        # First drop the view if it exists
        drop_view_sql = f"""
            DROP VIEW IF EXISTS {schema_name}.{view} CASCADE
        """
        await conn.execute(drop_view_sql)

        # Then create the new view - note we're directly interpolating batch_id since parameters
        # aren't supported in CREATE VIEW statements
        create_view_sql = f"""
            CREATE VIEW {schema_name}.{view} AS
            SELECT * FROM {source_table}
            WHERE _batch_id = '{batch_id}'
        """

        await conn.execute(create_view_sql)

        # Verify view creation
        exists = await conn.fetchval(
            """
                SELECT EXISTS (
                    SELECT FROM pg_views
                    WHERE schemaname = $1
                    AND viewname = $2
                )
                """,
            schema_name,
            view
        )

        if not exists:
            raise RuntimeError(f"Failed to create view {view_name}")

        self.logger.info(f"Successfully created view {view_name}")
