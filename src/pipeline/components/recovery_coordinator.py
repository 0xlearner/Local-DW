import asyncio
from src.logger import setup_logger
from src.recover.recovery_worker import RecoveryWorker


class RecoveryCoordinator:
    def __init__(self, recovery_worker: RecoveryWorker):
        self.recovery_worker = recovery_worker
        self.logger = setup_logger("recovery_coordinator")
        self._recovery_task = None

    async def start_recovery(self):
        """Start the recovery worker"""
        self._recovery_task = asyncio.create_task(self.recovery_worker.start())

    async def stop_recovery(self):
        """Stop the recovery worker"""
        if self.recovery_worker:
            await self.recovery_worker.stop()

        if self._recovery_task:
            try:
                await asyncio.wait_for(self._recovery_task, timeout=5.0)
            except (asyncio.TimeoutError, Exception):
                if not self._recovery_task.done():
                    self._recovery_task.cancel()
                    try:
                        await self._recovery_task
                    except (asyncio.CancelledError, Exception):
                        pass
            finally:
                self._recovery_task = None
