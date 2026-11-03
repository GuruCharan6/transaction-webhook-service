import asyncio
import logging
from datetime import UTC, datetime

import asyncpg
from fastapi import APIRouter, HTTPException, Response

from app import repository
from app.config import settings
from app.db import get_pool
from app.schemas import HealthOut, TransactionOut, WebhookIn

logger = logging.getLogger("routes")

router = APIRouter()


@router.get("/", response_model=HealthOut)
async def health() -> HealthOut:
    return HealthOut(current_time=datetime.now(UTC))


@router.post("/v1/webhooks/transactions", status_code=202)
async def receive_webhook(payload: WebhookIn, response: Response) -> dict:
    pool = get_pool()
    try:
        async def _store():
            async with pool.acquire() as conn:
                return await repository.insert_transaction(
                    conn,
                    payload.transaction_id,
                    payload.source_account,
                    payload.destination_account,
                    payload.amount,
                    payload.currency,
                )

        inserted = await asyncio.wait_for(_store(), timeout=settings.db_query_timeout_ms / 1000)
    except (asyncpg.PostgresError, OSError, TimeoutError) as exc:
        logger.warning("database unavailable, returning 503 for %s: %r", payload.transaction_id, exc)
        raise HTTPException(status_code=503, detail="database unavailable, please retry")

    if not inserted:
        logger.warning("duplicate webhook received for %s", payload.transaction_id)

    return {"status": "accepted", "transaction_id": payload.transaction_id}


@router.get("/v1/transactions/{transaction_id}", response_model=list[TransactionOut])
async def get_transaction(transaction_id: str) -> list[TransactionOut]:
    pool = get_pool()
    try:
        async with pool.acquire() as conn:
            row = await repository.get_transaction(conn, transaction_id)
    except (asyncpg.PostgresError, OSError, TimeoutError) as exc:
        logger.warning("database unavailable, returning 503 for lookup %s: %r", transaction_id, exc)
        raise HTTPException(status_code=503, detail="database unavailable, please retry")

    if row is None:
        raise HTTPException(status_code=404, detail="transaction not found")

    return [TransactionOut(**dict(row))]
