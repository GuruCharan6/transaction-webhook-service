from httpx import ASGITransport, AsyncClient

from app.main import app


async def test_health(pool):
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
        resp = await c.get("/")
        assert resp.status_code == 200
        assert resp.json()["status"] == "HEALTHY"


async def test_webhook_accepted(pool):
    body = {
        "transaction_id": "txn_test_001",
        "source_account": "acc_a",
        "destination_account": "acc_b",
        "amount": 150.50,
        "currency": "usd",
    }
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
        resp = await c.post("/v1/webhooks/transactions", json=body)
        assert resp.status_code == 202
        assert resp.json()["transaction_id"] == "txn_test_001"


async def test_webhook_validation_error(pool):
    body = {
        "transaction_id": "",
        "source_account": "acc_a",
        "destination_account": "acc_b",
        "amount": -5,
        "currency": "US",
    }
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
        resp = await c.post("/v1/webhooks/transactions", json=body)
        assert resp.status_code == 422


async def test_get_unknown_transaction_404(pool):
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
        resp = await c.get("/v1/transactions/does_not_exist")
        assert resp.status_code == 404


async def test_get_transaction_returns_array(pool):
    body = {
        "transaction_id": "txn_test_002",
        "source_account": "acc_a",
        "destination_account": "acc_b",
        "amount": 10,
        "currency": "INR",
    }
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
        await c.post("/v1/webhooks/transactions", json=body)
        resp = await c.get("/v1/transactions/txn_test_002")
        assert resp.status_code == 200
        data = resp.json()
        assert isinstance(data, list) and len(data) == 1
        assert data[0]["status"] == "PROCESSING"
        assert data[0]["processed_at"] is None
