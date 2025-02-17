import pytest
from src.schema_inferrer.schema_infer import SchemaInferrer


@pytest.mark.asyncio
async def test_schema_inference(sample_csv_data, pg_pool):
    # Convert sample data to CSV string
    csv_data = sample_csv_data.to_csv(index=False)

    inferrer = SchemaInferrer()
    schema = inferrer.infer_schema(csv_data)

    # Verify schema types
    assert schema["id"] == "BIGINT"
    assert schema["name"] == "TEXT"
    assert schema["email"] == "TEXT"
    assert schema["age"] == "BIGINT"
    assert schema["created_at"] == "TIMESTAMP"
    assert schema["is_active"] == "BOOLEAN"
    assert schema["tags"] == "JSONB"
    assert schema["scores"] == "TEXT[]"

    # Test table creation
    async with pg_pool.acquire() as conn:
        await inferrer.create_table_if_not_exists(conn, "test_table", schema, "id")

        # Verify table exists
        result = await conn.fetchval(
            """
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_name = 'test_table'
            )
        """
        )
        assert result is True
