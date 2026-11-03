import asyncio
from contextlib import asynccontextmanager

from httpx import ASGITransport, AsyncClient

from app import repository, routes
from app.main import app
from app.worker import Worker

BODY = {
    "transaction_id": "txn_dbdown_unit",
    "source_account": "a",
    "destination_account": "b",
    "amount": 10,
    "currency": "INR",
}


class RefusingPool:
    @asynccontextmanager
    async def acquire(self):
        raise ConnectionRefusedError("database is down")
        yield


class HangingPool:
    @asynccontextmanager
    async def acquire(self):
        await asyncio.sleep(10)
        yield


def client():
    return AsyncClient(transport=ASGITransport(app=app), base_url="http://test")


async def test_webhook_returns_503_when_db_refuses(monkeypatch):
    monkeypatch.setattr(routes, "get_pool", lambda: RefusingPool())
    async with client() as c:
        resp = await c.post("/v1/webhooks/transactions", json=BODY)
    assert resp.status_code == 503


async def test_webhook_returns_503_fast_when_db_hangs(monkeypatch):
    monkeypatch.setattr(routes, "get_pool", lambda: HangingPool())
    loop = asyncio.get_running_loop()
    async with client() as c:
        start = loop.time()
        resp = await c.post("/v1/webhooks/transactions", json=BODY)
        elapsed_ms = (loop.time() - start) * 1000
    assert resp.status_code == 503
    assert elapsed_ms < 500


async def test_lookup_returns_503_when_db_refuses(monkeypatch):
    monkeypatch.setattr(routes, "get_pool", lambda: RefusingPool())
    async with client() as c:
        resp = await c.get("/v1/transactions/anything")
    assert resp.status_code == 503


async def test_health_ignores_db_outage(monkeypatch):
    monkeypatch.setattr(routes, "get_pool", lambda: RefusingPool())
    async with client() as c:
        resp = await c.get("/")
    assert resp.status_code == 200


async def test_worker_survives_claim_failures_and_recovers(pool, monkeypatch):
    async with pool.acquire() as conn:
        await repository.insert_transaction(conn, "txn_worker_survives", "a", "b", 10, "INR")

    real_claim = repository.claim_pending
    failures = {"left": 3}

    async def flaky_claim(*args, **kwargs):
        if failures["left"] > 0:
            failures["left"] -= 1
            raise ConnectionRefusedError("database is down")
        return await real_claim(*args, **kwargs)

    monkeypatch.setattr(repository, "claim_pending", flaky_claim)

    worker = Worker()
    task = asyncio.create_task(worker.run())

    status = None
    for _ in range(50):
        async with pool.acquire() as conn:
            status = (await repository.get_transaction(conn, "txn_worker_survives"))["status"]
        if status == "PROCESSED":
            break
        await asyncio.sleep(0.2)

    await worker.stop()
    await task

    assert failures["left"] == 0
    assert status == "PROCESSED"
