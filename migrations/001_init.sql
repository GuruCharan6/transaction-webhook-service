CREATE TABLE IF NOT EXISTS transactions (
    transaction_id      TEXT PRIMARY KEY,
    source_account      TEXT NOT NULL,
    destination_account TEXT NOT NULL,
    amount              NUMERIC(18, 2) NOT NULL CHECK (amount > 0),
    currency             CHAR(3) NOT NULL,
    status               TEXT NOT NULL DEFAULT 'PROCESSING'
                          CHECK (status IN ('PROCESSING', 'PROCESSED')),
    created_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
    processed_at         TIMESTAMPTZ,
    locked_by            TEXT,
    locked_until         TIMESTAMPTZ,
    attempts             INT NOT NULL DEFAULT 0,
    last_error           TEXT
);

CREATE INDEX IF NOT EXISTS idx_transactions_pending
    ON transactions (created_at)
    WHERE status = 'PROCESSING';
