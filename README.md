# Transaction Webhook Service

A service that accepts payment-processor webhooks, acknowledges them in under
500ms, and processes each one reliably in the background ~30 seconds later.
Nothing is acknowledged until it is durably stored, and everything stored is
eventually processed — even across a crash.

> **Live URL:** _not yet deployed — see [Deployment](#deployment)_
> ```bash
> curl -X POST https://<deployed-url>/v1/webhooks/transactions \
>   -H "Content-Type: application/json" \
>   -d '{"transaction_id":"txn_demo_001","source_account":"acc_user_789","destination_account":"acc_merchant_456","amount":150.50,"currency":"USD"}'
> ```

## Architecture

```
Payment processor --POST webhook--> FastAPI service --insert once--> Postgres
     (retries on failure)           (validate, store,        (system of record
                                      reply 202)                + queue state)
                                                                      |
                                                     claims rows with a lease,
                                                     then marks them PROCESSED
                                                                      |
                                                              Background worker
                                                     (claim -> process 30s -> complete)

If a worker crashes mid-processing: its lease expires after 60s and the
next poll from any worker reclaims the row. No separate cleanup process.
```

The API and worker never talk to each other directly. The API writes a row,
the worker finds it, and that row survives a restart of either process.
Postgres is the only handoff — no Celery, no Redis, no message broker.

**Request flow:** webhook arrives -> row inserted with `ON CONFLICT DO
NOTHING` (idempotency via primary key) -> 202 returned the instant the
insert commits -> worker polls once a second, claims up to its free
concurrency slots with `FOR UPDATE SKIP LOCKED` under a 60s lease -> waits
30s (simulating the real external call) -> marks the row `PROCESSED` only if
it still holds the lease.

## Technical choices and trade-offs

| Decision | Choice | Rejected | Why |
|---|---|---|---|
| Queue | Postgres with `SKIP LOCKED` | Celery + Redis | One atomic write, no gap between saving and enqueueing, one less service |
| Background work | Dedicated `asyncio` worker loop | FastAPI `BackgroundTasks` | In-memory tasks vanish on restart |
| Idempotency | Primary key + `ON CONFLICT DO NOTHING` | Check-then-insert | The database stops races a check can't |
| Crash recovery | Time-limited lease (`locked_until`) | Separate cleanup job | Abandoned rows become reclaimable with no extra process |
| Concurrency | `asyncio` tasks per claimed row, bounded by a semaphore | One row at a time | The wait is pure I/O, so many rows can wait together |
| DB access | `asyncpg` with plain SQL | Full ORM | The key queries (`SKIP LOCKED`, guarded updates) are Postgres-specific and should stay visible |
| Money | `NUMERIC(18,2)` | `float` | Exact decimal storage, no rounding drift |

**The trade-off to flag:** polling adds up to ~1s of delay, and Postgres
won't scale like a dedicated broker. At much higher volume, keep the same
insert-and-reply API and move delivery to a real queue (SQS/Kafka) behind an
outbox table — the schema here already supports that migration since the row
*is* the outbox entry.

## Spec assumptions

The brief leaves some points open; decisions made, and why:

| Open point | Decision |
|---|---|
| Query response shape | Returns a one-item array (`[{...}]`), matching the brief exactly |
| `amount` as both `1500` and `150.50` | Accept any decimal, store as `NUMERIC(18,2)`, reject >2 decimal places and anything too large for the column (`422`, not a confusing DB error) |
| `currency` as both `INR` and `USD` | Accept any 3-letter alphabetic code, stored uppercase |
| Health response's trailing comma in the brief | Treated as a typo; service returns valid JSON |
| "Handle duplicates gracefully" | Always `202`, never `409` — a processor should never read an error as a reason to keep retrying |
| `status` values | Only `PROCESSING` / `PROCESSED` are ever returned |

## Project structure

```
app/
  main.py        # FastAPI app, lifespan (starts/stops the worker)
  routes.py       # The three endpoints
  schemas.py      # Pydantic request/response models + validation
  repository.py   # All SQL lives here
  worker.py       # Claim -> process -> complete, with lease recovery
  db.py           # asyncpg connection pool
  config.py       # Environment-driven settings
migrations/
  001_init.sql    # Schema
tests/
  test_api.py         # Endpoint contracts, 404, array response shape
  test_validation.py  # Every missing field, bad amounts/currencies, first-version-wins duplicates
  test_idempotency.py # Sequential + 50-concurrent duplicate webhooks
  test_worker.py      # Claim/process/complete, crash-lease reclaim
  test_failures.py    # DB refused/hanging -> fast 503, health unaffected, worker survives outages
scripts/
  run_all.py          # One command: all four success criteria end to end, prints PASS/FAIL
  smoke_test.sh       # Single webhook end to end against a running server
  load_test.py        # Concurrent webhook response-time check
  crash_test.py       # Kills and restarts the worker mid-processing
  run_dev_windows.py  # Local-dev-only launcher (see note below)
docker-compose.yml  # Local Postgres
Dockerfile
requirements.txt
.env.example
```

## Configuration

| Variable | Default | Purpose |
|---|---|---|
| `DATABASE_URL` | — (required) | Postgres connection string |
| `PROCESSING_DELAY_SECONDS` | 30 | Simulated external-call delay |
| `LEASE_SECONDS` | 60 | How long a claim is held before another worker can reclaim it |
| `POLL_INTERVAL_SECONDS` | 1 | How often the worker polls for pending rows |
| `WORKER_CONCURRENCY` | 100 | Max rows processing at once |
| `DB_POOL_SIZE` | 10 | Postgres connections per process |
| `DB_QUERY_TIMEOUT_MS` | 300 | Webhook-insert query budget before returning 503 |
| `STATEMENT_CACHE_SIZE` | 100 | Set to `0` only if deploying behind a transaction-mode pgbouncer (e.g. Supabase's pooler) — otherwise leave it on, it's a large perf win |
| `RUN_EMBEDDED_WORKER` | true | Set `false` to run the API without its own worker (used by the crash test, so the worker process can be killed independently) |

For fast local tests, set `PROCESSING_DELAY_SECONDS=1`.

## Run it locally

Prerequisites: Python 3.11+, Docker Desktop.

```bash
cp .env.example .env
docker compose up -d
python -m venv .venv
source .venv/Scripts/activate  # Git Bash on Windows; use .venv/bin/activate on macOS/Linux
pip install -r requirements.txt
set -a; source .env; set +a
uvicorn app.main:app --reload
```

> **Windows-only note:** `scripts/run_dev_windows.py` switches asyncio to the
> selector event loop before starting uvicorn. Plain `uvicorn app.main:app`
> works fine on Windows too; this script exists only because the default
> `ProactorEventLoop` handles very high simultaneous-connection counts
> noticeably worse than Linux in local testing. The Docker image doesn't need
> it — Linux containers use the standard loop.

The worker starts automatically inside the API process. To run it as a
separate process instead (used by the crash test):

```bash
RUN_EMBEDDED_WORKER=false uvicorn app.main:app     # terminal 1
python -m app.worker                                # terminal 2
```

## Run the tests

```bash
docker compose up -d
pip install -r requirements.txt
DATABASE_URL=postgresql://postgres:postgres@localhost:5432/webhooks \
PROCESSING_DELAY_SECONDS=1 LEASE_SECONDS=5 POLL_INTERVAL_SECONDS=0.2 \
  pytest tests/ -v
```

37 tests, all passing:

- API contracts: health, 202 on accept, 404 on unknown ID, one-item array response
- Validation: each missing field, empty strings, amount `0` / negative / 3
  decimals / too large / non-numeric, bad currency codes, uppercase storage,
  and a duplicate with different details keeping the first version
- Idempotency: 5 sequential and 50 concurrent identical webhooks leave one row
- Worker: claim, process, complete, and reclaiming a row whose lease expired
- Failures: a refused or hanging database returns `503` in under 500ms,
  lookups return `503` too, the health check stays `200`, and the worker keeps
  polling through repeated claim failures and finishes the row afterwards

## Verify all four success criteria in one command

```bash
docker compose up -d
python scripts/run_all.py
```

`run_all.py` takes about 5 minutes and uses the real 30s delay and 60s lease.
It starts from an empty table, starts and stops the server and worker itself,
stops and restarts Postgres for the outage check, and ends with a PASS/FAIL
summary (exit code 0 only if everything passes). Unset any fast-test
variables (`PROCESSING_DELAY_SECONDS` etc.) before running it.

Individual scripts for checking one thing at a time:

```bash
# Single webhook end to end against a running server (~35s)
bash scripts/smoke_test.sh http://localhost:8000

# Concurrent-load response-time check against a running server
python scripts/load_test.py http://localhost:8000 100

# Worker crash-recovery (kills the worker at second 10, restarts it)
RUN_EMBEDDED_WORKER=false uvicorn app.main:app     # terminal 1
python scripts/crash_test.py http://localhost:8000  # terminal 2
```

### Results (local, Windows dev machine, Docker Desktop Postgres)

From `run_all.py`:

| Success criterion | Result |
|---|---|
| Single transaction | `PROCESSED`, `processed_at - created_at` = 30.7s (own target 30-35s) |
| Duplicate prevention | 5 sequential + 50 concurrent identical webhooks: every response `202`, one row each |
| Performance, 100 concurrent webhooks | p50 137ms / p95 180ms / max 185ms (hard limit 500ms) |
| Reliability: worker killed mid-processing | Row still reached `PROCESSED` with `attempts = 2`, one claim per worker |
| Reliability: database stopped | Webhook got `503` (no false ack), health stayed `200`, nothing half-saved (`404` after recovery) |

The performance row is measured in-process (the app and real Postgres, but no
TCP sockets). Running the same 100 requests through real sockets against a
local server on this Windows machine measured roughly 550-800ms. Isolated
benchmarks traced that gap to Windows loopback and Docker Desktop connection
setup, not to the application: the raw `asyncpg` pool alone handles the same
100 inserts at p95 92ms. Linux containers, the deploy target, don't carry that
cost. **The load test will be re-run against the deployed URL once live**, as
the brief notes that network distance affects these numbers.

## Failure modes

| Failure | What happens | Result |
|---|---|---|
| Duplicate webhook, incl. simultaneous | `ON CONFLICT DO NOTHING` skips the insert | One row, `202` every time |
| Database down during a webhook | Handler returns `503` (refused or timed-out connection alike) | Processor retries, no false ack |
| Slow database | Query times out at 300ms, inside the 500ms budget | `503`, response stays bounded |
| Database down during processing | Worker logs a one-line warning and keeps polling | Work resumes by itself when the database returns |
| Database down during a lookup | `GET /v1/transactions/{id}` returns `503` | Clear retry signal instead of a `500` |
| Worker crash mid-processing | Lease expires after 60s | Another worker finishes the row |
| API restart after `202` | Row already in Postgres | Worker picks it up |
| Processing exception | Error saved, lease cleared | Row retried, `attempts` records it |
| Worker loses its lease | Guarded `UPDATE` affects 0 rows | Only the current lease holder completes the row |

Unlimited retry is acceptable for this assessment. In production, cap
`attempts` and move repeat failures to a dead-letter table.

## What I'd add for production

- Cap `attempts` and a dead-letter table for permanently failing rows
- Webhook signature verification (HMAC) — explicitly out of scope here per
  the brief, so the assessor's plain test requests aren't rejected
- Structured logging / tracing with transaction_id correlation
- Move the queue off Postgres polling to SQS/Kafka via an outbox table once
  volume outgrows a single table scan
- Metrics (queue depth, processing latency, retry counts) and alerting
- Multiple worker processes/replicas — already safe today since `SKIP
  LOCKED` coordinates them, just not provisioned here
- Rate limiting and auth on the webhook endpoint

## Deployment

Not yet deployed. Plan: one always-on container (no scale-to-zero — a cold
start would fail the 500ms test on the first request) + managed Postgres,
same region. `DATABASE_URL` set via the host's environment settings, never
committed to the repo.
