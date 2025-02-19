import asyncpg
from typing import Dict, Optional
from src.logger import setup_logger


class ConnectionManager:
    _instance = None
    _pool: Optional[asyncpg.Pool] = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(ConnectionManager, cls).__new__(cls)
            cls._instance.logger = setup_logger("connection_manager")
        return cls._instance

    @classmethod
    async def initialize(
        cls,
        conn_params: Dict[str, str],
        min_size: int = 2,
        max_size: int = 10,
        command_timeout: int = 60
    ) -> None:
        """Initialize the connection pool with given parameters"""
        if cls._pool is not None:
            cls._instance.logger.warning("Pool already initialized")
            return

        cls._pool = await asyncpg.create_pool(
            **conn_params,
            min_size=min_size,
            max_size=max_size,
            command_timeout=command_timeout
        )
        cls._instance.logger.info("Connection pool initialized successfully")

    @classmethod
    def get_pool(cls) -> asyncpg.Pool:
        """Get the connection pool instance"""
        if cls._pool is None:
            raise RuntimeError(
                "Connection pool not initialized. Call initialize() first")
        return cls._pool

    @classmethod
    async def close(cls) -> None:
        """Close the connection pool"""
        if cls._pool:
            await cls._pool.close()
            cls._pool = None
            cls._instance.logger.info("Connection pool closed")

    @classmethod
    async def get_connection(cls) -> asyncpg.Connection:
        """Get a connection from the pool"""
        if cls._pool is None:
            raise RuntimeError(
                "Connection pool not initialized. Call initialize() first")
        return await cls._pool.acquire()

    @classmethod
    async def get_column_types(cls, table_name: str) -> dict:
        """Get PostgreSQL column types for a given table"""
        async with cls.get_pool().acquire() as conn:
            types = {}
            query = """
                SELECT column_name, data_type, udt_name
                FROM information_schema.columns
                WHERE table_name = $1
            """
            rows = await conn.fetch(query, table_name)
            for row in rows:
                types[row["column_name"]] = (
                    f"{row['data_type']}"
                    if row["data_type"] != "USER-DEFINED"
                    else row["udt_name"]
                )
            return types
