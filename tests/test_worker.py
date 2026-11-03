import asyncio

from app import repository
from app.worker import Worker


async def test_claim_process_complete(pool):
    async with pool.acquire() as conn:
        await repository.insert_transaction(conn, "txn_w1", "a", "b", 10, "INR")

    worker = Worker()
    worker_task = asyncio.create_task(worker.run())

    for _ in range(50):
        async with pool.acquire() as conn:
            row = await repository.get_transaction(conn, "txn_w1")
        if row["status"] == "PROCESSED":
            break
        await asyncio.sleep(0.2)

    await worker.stop()
    await worker_task

    assert row["status"] == "PROCESSED"
    assert row["processed_at"] is not None


async def test_crash_recovery_lease_reclaimed(pool):
    async with pool.acquire() as conn:
        await repository.insert_transaction(conn, "txn_w2", "a", "b", 10, "INR")

    stale_worker = "worker-dead"
    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE transactions SET locked_by = $1, locked_until = now() - interval '1 second', attempts = 1 "
            "WHERE transaction_id = 'txn_w2'",
            stale_worker,
        )

    worker = Worker()
    worker_task = asyncio.create_task(worker.run())

    for _ in range(50):
        async with pool.acquire() as conn:
            row = await repository.get_transaction(conn, "txn_w2")
        if row["status"] == "PROCESSED":
            break
        await asyncio.sleep(0.2)

    await worker.stop()
    await worker_task

    assert row["status"] == "PROCESSED"

    async with pool.acquire() as conn:
        final = await conn.fetchrow("SELECT attempts FROM transactions WHERE transaction_id = 'txn_w2'")
    assert final["attempts"] == 2
