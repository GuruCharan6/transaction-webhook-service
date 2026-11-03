import os

import asyncpg
import pytest_asyncio

os.environ.setdefault("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/webhooks")
os.environ.setdefault("PROCESSING_DELAY_SECONDS", "1")
os.environ.setdefault("LEASE_SECONDS", "5")
os.environ.setdefault("POLL_INTERVAL_SECONDS", "0.2")

from app import db  # noqa: E402
from app.config import settings  # noqa: E402

MIGRATION_PATH = os.path.join(os.path.dirname(__file__), "..", "migrations", "001_init.sql")


@pytest_asyncio.fixture
async def pool():
    conn = await asyncpg.connect(settings.database_url)
    await conn.execute("DROP TABLE IF EXISTS transactions")
    with open(MIGRATION_PATH) as f:
        await conn.execute(f.read())
    await conn.close()

    await db.init_pool()
    yield db.get_pool()
    await db.close_pool()
