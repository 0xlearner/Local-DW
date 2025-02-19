import asyncio

import asyncpg

from src.config import Config
from src.loader.data_load import DataLoader
from src.recover.recovery_manager import RecoveryManager

from src.logger import setup_logger

logger = setup_logger("recovery_worker")


class RecoveryWorker:
    def __init__(
        self, config: Config, recovery_manager: RecoveryManager, data_loader: DataLoader
    ):
        self.config = config
        self.recovery_manager = recovery_manager
        self.data_loader = data_loader
        self.running = False
        self._pool = None

    def set_pool(self, pool: asyncpg.Pool):
        self._pool = pool

    async def start(self):
        self.running = True

        while self.running:
            try:
                # Get failed jobs that need retry
                failed_jobs = await self.recovery_manager.get_failed_jobs()

                for job in failed_jobs:
                    if not self.running:  # Check if we should stop
                        break
                    try:
                        # Reload the data
                        await self.data_loader.load_data(
                            job["checkpoint_data"]["csv_data"],
                            job["table_name"],
                            job["checkpoint_data"]["primary_key"],
                            job["file_name"],
                            job["checkpoint_data"]["schema"],
                        )

                        # Update recovery status
                        await self.recovery_manager.update_recovery_status(
                            job["batch_id"], "PROCESSED"
                        )

                    except Exception as e:
                        await self.recovery_manager.update_recovery_status(
                            job["batch_id"], "FAILED", str(e)
                        )

                if not self.running:
                    break

                # Wait before next check, but make it interruptible
                try:
                    await asyncio.sleep(5)  # Reduced sleep time for testing
                except asyncio.CancelledError:
                    break

            except asyncio.CancelledError:
                break
            except Exception as e:
                if self.running:
                    logger.error(f"Error in recovery worker: {str(e)}")
                    try:
                        # Reduced sleep time for testing
                        await asyncio.sleep(5)
                    except asyncio.CancelledError:
                        break

        await self.cleanup()
        self.running = False

    async def stop(self):
        """Stop the worker and cleanup resources"""
        self.running = False
        await self.cleanup()

    async def cleanup(self):
        """Cleanup any resources used by the recovery worker"""
        logger.debug("Cleaning up recovery worker resources")
        # Currently no resources to clean up, but method needs to exist
        # for future resource cleanup implementation
        pass
