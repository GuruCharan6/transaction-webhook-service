import asyncpg

INSERT_TRANSACTION = """
INSERT INTO transactions
    (transaction_id, source_account, destination_account, amount, currency)
VALUES ($1, $2, $3, $4, $5)
ON CONFLICT (transaction_id) DO NOTHING
RETURNING transaction_id;
"""

GET_TRANSACTION = """
SELECT transaction_id, source_account, destination_account, amount, currency,
       status, created_at, processed_at
FROM transactions
WHERE transaction_id = $1;
"""

CLAIM_PENDING = """
UPDATE transactions
SET locked_by = $1,
    locked_until = now() + ($3 || ' seconds')::interval,
    attempts = attempts + 1
WHERE transaction_id IN (
    SELECT transaction_id
    FROM transactions
    WHERE status = 'PROCESSING'
      AND (locked_until IS NULL OR locked_until < now())
    ORDER BY created_at
    LIMIT $2
    FOR UPDATE SKIP LOCKED
)
RETURNING transaction_id;
"""

COMPLETE_TRANSACTION = """
UPDATE transactions
SET status = 'PROCESSED',
    processed_at = now(),
    locked_by = NULL,
    locked_until = NULL
WHERE transaction_id = $1
  AND status = 'PROCESSING'
  AND locked_by = $2;
"""

RELEASE_ON_ERROR = """
UPDATE transactions
SET locked_by = NULL,
    locked_until = NULL,
    last_error = $2
WHERE transaction_id = $1
  AND locked_by = $3;
"""

RELEASE_WORKER_LEASES = """
UPDATE transactions
SET locked_by = NULL,
    locked_until = NULL
WHERE locked_by = $1;
"""


async def insert_transaction(
    conn: asyncpg.Connection,
    transaction_id: str,
    source_account: str,
    destination_account: str,
    amount,
    currency: str,
) -> bool:
    row = await conn.fetchrow(
        INSERT_TRANSACTION, transaction_id, source_account, destination_account, amount, currency
    )
    return row is not None


async def get_transaction(conn: asyncpg.Connection, transaction_id: str):
    return await conn.fetchrow(GET_TRANSACTION, transaction_id)


async def claim_pending(conn: asyncpg.Connection, worker_id: str, limit: int, lease_seconds: int):
    rows = await conn.fetch(CLAIM_PENDING, worker_id, limit, str(lease_seconds))
    return [r["transaction_id"] for r in rows]


async def complete_transaction(conn: asyncpg.Connection, transaction_id: str, worker_id: str) -> None:
    await conn.execute(COMPLETE_TRANSACTION, transaction_id, worker_id)


async def release_on_error(conn: asyncpg.Connection, transaction_id: str, error: str, worker_id: str) -> None:
    await conn.execute(RELEASE_ON_ERROR, transaction_id, error[:1000], worker_id)


async def release_worker_leases(conn: asyncpg.Connection, worker_id: str) -> None:
    await conn.execute(RELEASE_WORKER_LEASES, worker_id)
