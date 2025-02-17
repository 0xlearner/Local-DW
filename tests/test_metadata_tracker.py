import pytest
from src.tracker.metadata_tracker import MetadataTracker


@pytest.mark.asyncio
async def test_metadata_tracking(test_config, pg_pool, clean_test_db):
    tracker = MetadataTracker(test_config["postgres"])

    # Initialize tables
    await tracker.initialize_metadata_tables()

    # Test table metadata tracking
    await tracker.update_table_metadata(
        "test_table", "test_file.csv.gz", 100, {"id": "BIGINT", "name": "TEXT"}
    )

    async with pg_pool.acquire() as conn:
        result = await conn.fetchrow(
            """
            SELECT * FROM table_metadata 
            WHERE table_name = 'test_table'
        """
        )

        assert result is not None
        assert result["total_rows"] == 100
        assert result["last_file_processed"] == "test_file.csv.gz"
