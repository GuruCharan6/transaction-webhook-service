import os


class Settings:
    database_url: str = os.environ["DATABASE_URL"]
    processing_delay_seconds: float = float(os.environ.get("PROCESSING_DELAY_SECONDS", 30))
    lease_seconds: int = int(os.environ.get("LEASE_SECONDS", 60))
    poll_interval_seconds: float = float(os.environ.get("POLL_INTERVAL_SECONDS", 1))
    worker_concurrency: int = int(os.environ.get("WORKER_CONCURRENCY", 100))
    db_pool_size: int = int(os.environ.get("DB_POOL_SIZE", 10))
    db_query_timeout_ms: int = int(os.environ.get("DB_QUERY_TIMEOUT_MS", 300))
    statement_cache_size: int = int(os.environ.get("STATEMENT_CACHE_SIZE", 100))


settings = Settings()
