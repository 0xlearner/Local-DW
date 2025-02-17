import json
import uuid
from typing import List, Optional, Dict, Any

import asyncpg


class RecoveryManager:
    def __init__(self, conn_params: Dict[str, Any], max_retries: int = 3):
        self.conn_params = conn_params
        self.max_retries = max_retries

    async def create_recovery_point(
        self, table_name: str, file_name: str, checkpoint_data: Dict[str, Any]
    ) -> str:
        batch_id = str(uuid.uuid4())

        async with asyncpg.create_pool(**self.conn_params) as pool:
            async with pool.acquire() as conn:
                await conn.execute(
                    """
                    INSERT INTO recovery_points (
                        table_name, file_name, batch_id, checkpoint_data,
                        status, next_retry_at
                    ) VALUES ($1, $2, $3, $4, 'ACTIVE', NULL)
                """,
                    table_name,
                    file_name,
                    batch_id,
                    json.dumps(checkpoint_data),
                )

        return batch_id

    async def update_recovery_status(
        self, batch_id: str, status: str, error: Optional[str] = None
    ):
        async with asyncpg.create_pool(**self.conn_params) as pool:
            async with pool.acquire() as conn:
                if status == "FAILED":
                    await conn.execute(
                        """
                        UPDATE recovery_points
                        SET status = $1,
                            retry_count = retry_count + 1,
                            last_error = $2,
                            next_retry_at = CASE
                                WHEN retry_count < $3 THEN 
                                    CURRENT_TIMESTAMP + 
                                    (INTERVAL '5 minutes' * retry_count)
                                ELSE NULL
                            END
                        WHERE batch_id = $4
                    """,
                        status,
                        error,
                        self.max_retries,
                        batch_id,
                    )
                else:
                    await conn.execute(
                        """
                        UPDATE recovery_points
                        SET status = $1,
                        next_retry_at = NULL
                        WHERE batch_id = $2
                    """,
                        status,
                        batch_id,
                    )

    async def get_failed_jobs(self) -> List[Dict[str, Any]]:
        async with asyncpg.create_pool(**self.conn_params) as pool:
            async with pool.acquire() as conn:
                return await conn.fetch(
                    """
                    SELECT *
                    FROM recovery_points
                    WHERE status = 'FAILED'
                    AND retry_count < $1
                    AND (next_retry_at IS NULL OR next_retry_at <= CURRENT_TIMESTAMP)
                """,
                    self.max_retries,
                )
