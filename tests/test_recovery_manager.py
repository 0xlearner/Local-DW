import pytest
from src.recover.recovery_manager import RecoveryManager


@pytest.mark.asyncio
async def test_recovery_management(test_config, pg_pool, clean_test_db):
    manager = RecoveryManager(test_config["postgres"])

    # Create recovery point
    batch_id = await manager.create_recovery_point(
        "test_table",
        "test_file.csv.gz",
        {"processed_records": 50, "total_records": 100},
    )

    # Test failed status
    await manager.update_recovery_status(batch_id, "FAILED", "Test error")

    # Get failed jobs
    failed_jobs = await manager.get_failed_jobs()
    assert len(failed_jobs) == 1
    assert failed_jobs[0]["batch_id"] == batch_id
    assert failed_jobs[0]["status"] == "FAILED"
