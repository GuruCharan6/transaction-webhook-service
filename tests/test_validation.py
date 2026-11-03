import pytest
from httpx import ASGITransport, AsyncClient

from app.main import app

VALID = {
    "transaction_id": "txn_valid",
    "source_account": "acc_a",
    "destination_account": "acc_b",
    "amount": 899.75,
    "currency": "EUR",
}


def client():
    return AsyncClient(transport=ASGITransport(app=app), base_url="http://test")


async def row_count(pool, transaction_id):
    return await pool.fetchval("SELECT count(*) FROM transactions WHERE transaction_id = $1", transaction_id)


@pytest.mark.parametrize("missing", list(VALID.keys()))
async def test_missing_field_rejected(pool, missing):
    txn_id = f"txn_missing_{missing}"
    body = {k: v for k, v in {**VALID, "transaction_id": txn_id}.items() if k != missing}
    async with client() as c:
        resp = await c.post("/v1/webhooks/transactions", json=body)
    assert resp.status_code == 422
    assert await pool.fetchval("SELECT count(*) FROM transactions") == 0


@pytest.mark.parametrize(
    "field,value",
    [
        ("transaction_id", ""),
        ("source_account", ""),
        ("destination_account", ""),
        ("amount", 0),
        ("amount", -5),
        ("amount", 10.123),
        ("amount", 1e20),
        ("amount", "not-a-number"),
        ("currency", "EU"),
        ("currency", "EURO"),
        ("currency", "E1R"),
    ],
)
async def test_invalid_value_rejected(pool, field, value):
    txn_id = f"txn_invalid_{field}"
    body = {**VALID, "transaction_id": txn_id, field: value}
    async with client() as c:
        resp = await c.post("/v1/webhooks/transactions", json=body)
    assert resp.status_code == 422
    assert await row_count(pool, body["transaction_id"] or txn_id) == 0


@pytest.mark.parametrize("amount", [2499, 899.7, 899.75, 0.01, 99999999.99])
async def test_valid_amounts_accepted(pool, amount):
    txn_id = f"txn_amount_{str(amount).replace('.', '_')}"
    async with client() as c:
        resp = await c.post("/v1/webhooks/transactions", json={**VALID, "transaction_id": txn_id, "amount": amount})
    assert resp.status_code == 202
    assert await row_count(pool, txn_id) == 1


async def test_currency_stored_uppercase(pool):
    async with client() as c:
        await c.post("/v1/webhooks/transactions", json={**VALID, "transaction_id": "txn_lower", "currency": "inr"})
        resp = await c.get("/v1/transactions/txn_lower")
    assert resp.json()[0]["currency"] == "INR"


async def test_duplicate_with_different_details_keeps_first(pool):
    async with client() as c:
        await c.post("/v1/webhooks/transactions", json={**VALID, "transaction_id": "txn_first_wins", "amount": 10})
        resp = await c.post("/v1/webhooks/transactions", json={**VALID, "transaction_id": "txn_first_wins", "amount": 999})
        assert resp.status_code == 202
        stored = (await c.get("/v1/transactions/txn_first_wins")).json()[0]
    assert float(stored["amount"]) == 10
