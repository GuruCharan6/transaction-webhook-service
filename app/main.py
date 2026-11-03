import asyncio
import logging
import os
from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.db import close_pool, init_pool
from app.routes import router
from app.worker import Worker

logging.basicConfig(level=logging.INFO)

RUN_EMBEDDED_WORKER = os.environ.get("RUN_EMBEDDED_WORKER", "true").lower() != "false"


@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_pool()
    worker_task = None
    worker = None
    if RUN_EMBEDDED_WORKER:
        worker = Worker()
        worker_task = asyncio.create_task(worker.run())
    try:
        yield
    finally:
        if worker is not None:
            await worker.stop()
            await worker_task
        await close_pool()


app = FastAPI(title="Transaction Webhook Service", lifespan=lifespan)
app.include_router(router)
