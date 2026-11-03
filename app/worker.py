import asyncio
import logging
import signal
import uuid

from app import repository
from app.config import settings
from app.db import close_pool, get_pool, init_pool

logger = logging.getLogger("worker")


class Worker:
    def __init__(self) -> None:
        self.worker_id = f"worker-{uuid.uuid4().hex[:12]}"
        self._semaphore = asyncio.Semaphore(settings.worker_concurrency)
        self._in_flight: set[asyncio.Task] = set()
        self._stopping = False

    async def run(self) -> None:
        logger.info("worker %s started", self.worker_id)
        try:
            while not self._stopping:
                try:
                    await self._claim_and_dispatch()
                except Exception as exc:
                    logger.warning("claim failed, retrying next poll: %r", exc)
                await asyncio.sleep(settings.poll_interval_seconds)
        finally:
            if self._in_flight:
                await asyncio.gather(*self._in_flight, return_exceptions=True)
            await self._release_leases()
            logger.info("worker %s stopped", self.worker_id)

    async def stop(self) -> None:
        self._stopping = True

    async def _claim_and_dispatch(self) -> None:
        free_slots = settings.worker_concurrency - len(self._in_flight)
        if free_slots <= 0:
            return
        pool = get_pool()
        async with pool.acquire() as conn:
            async with conn.transaction():
                ids = await repository.claim_pending(
                    conn, self.worker_id, free_slots, settings.lease_seconds
                )
        for transaction_id in ids:
            task = asyncio.create_task(self._process(transaction_id))
            self._in_flight.add(task)
            task.add_done_callback(self._in_flight.discard)

    async def _process(self, transaction_id: str) -> None:
        async with self._semaphore:
            pool = get_pool()
            try:
                await asyncio.sleep(settings.processing_delay_seconds)
                async with pool.acquire() as conn:
                    await repository.complete_transaction(conn, transaction_id, self.worker_id)
            except Exception as exc:
                logger.warning("processing failed for %s: %r", transaction_id, exc)
                try:
                    async with pool.acquire() as conn:
                        await repository.release_on_error(conn, transaction_id, str(exc), self.worker_id)
                except Exception as release_exc:
                    logger.warning(
                        "could not release lease for %s, it will expire on its own: %r",
                        transaction_id,
                        release_exc,
                    )

    async def _release_leases(self) -> None:
        try:
            pool = get_pool()
            async with pool.acquire() as conn:
                await repository.release_worker_leases(conn, self.worker_id)
        except Exception as exc:
            logger.warning("could not release leases on shutdown, they will expire on their own: %r", exc)


async def _main() -> None:
    logging.basicConfig(level=logging.INFO)
    await init_pool()
    worker = Worker()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, lambda: asyncio.create_task(worker.stop()))
        except NotImplementedError:
            pass
    try:
        await worker.run()
    finally:
        await close_pool()


if __name__ == "__main__":
    asyncio.run(_main())
