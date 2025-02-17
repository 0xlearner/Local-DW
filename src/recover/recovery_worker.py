import asyncio

from src.config import Config
from src.loader.data_loader import DataLoader
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

                # Wait before next check, but make it interruptible
                try:
                    await asyncio.sleep(300)  # 5 minutes
                except asyncio.CancelledError:
                    self.running = False
                    break

            except asyncio.CancelledError:
                self.running = False
                break
            except Exception as e:
                logger.error(f"Error in recovery worker: {str(e)}")
                try:
                    await asyncio.sleep(60)  # Wait 1 minute on error
                except asyncio.CancelledError:
                    self.running = False
                    break

    def stop(self):
        self.running = False
