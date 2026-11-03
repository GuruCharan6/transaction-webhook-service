import asyncio

from httpx import ASGITransport, AsyncClient

from app.main import app

BODY = {
    "transaction_id": "txn_dup_001",
    "source_account": "acc_a",
    "destination_account": "acc_b",
    "amount": 42.00,
    "currency": "INR",
}


async def test_sequential_duplicates_one_row(pool):
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
        for _ in range(5):
            resp = await c.post("/v1/webhooks/transactions", json=BODY)
            assert resp.status_code == 202

    row = await pool.fetchrow("SELECT count(*) AS n FROM transactions WHERE transaction_id = $1", BODY["transaction_id"])
    assert row["n"] == 1


async def test_concurrent_duplicates_one_row(pool):
    body = {**BODY, "transaction_id": "txn_dup_concurrent"}

    async def send():
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
            return await c.post("/v1/webhooks/transactions", json=body)

    results = await asyncio.gather(*[send() for _ in range(50)])
    assert all(r.status_code == 202 for r in results)

    row = await pool.fetchrow(
        "SELECT count(*) AS n FROM transactions WHERE transaction_id = $1", body["transaction_id"]
    )
    assert row["n"] == 1
