# Transaction Webhook Service

Receives payment webhooks, replies `202` immediately, and processes each
transaction exactly once about 30 seconds later, even if the server crashes
mid-processing or the same webhook arrives 50 times at once.

**Live:** https://transaction-webhook-service-twfw.onrender.com

```bash
URL=https://transaction-webhook-service-twfw.onrender.com

curl -X POST $URL/v1/webhooks/transactions \
  -H "Content-Type: application/json" \
  -d '{"transaction_id":"txn_demo_001","source_account":"acc_user_789","destination_account":"acc_merchant_456","amount":150.50,"currency":"USD"}'

curl $URL/v1/transactions/txn_demo_001   # PROCESSING now, PROCESSED ~30s later
```

## Results

| Success criterion | Live (Render, Singapore) | Local (`scripts/run_all.py`) |
|---|---|---|
| Single transaction processed after ~30s | ✅ 30.2s from `created_at` to `processed_at` | ✅ 30.7s |
| Duplicate prevention | ✅ 5 sequential + 50 concurrent identical webhooks: every response `202`, one row | ✅ same, `attempts = 1` |
| Fast response under load | ✅ 120-160ms per request (from India) · ⚠️ bursts of 10+ simultaneous requests are slow on the free 0.1-CPU instance ([details](#performance)) | ✅ 100 concurrent: p50 137ms, p95 180ms, max 185ms |
| No lost transactions | Verified locally (right) | ✅ worker killed mid-processing → still `PROCESSED`, `attempts = 2` · ✅ database stopped → `503`, nothing half-saved, worker resumes on its own |

Plus 37 automated tests (`pytest`), all passing.

## How I approached it

I wrote a short PRD before writing any code. The brief looks like "build
three endpoints", but the hard part is one rule:

> Nothing is acknowledged until it is durably stored, and everything stored is
> eventually processed, exactly once.

Every design choice follows from that rule:

```mermaid
flowchart LR
    P[Payment processor] -->|POST webhook| A[FastAPI API]
    A -->|INSERT ... ON CONFLICT DO NOTHING| D[(Postgres<br/>transactions table)]
    A -->|202 after the insert commits| P
    W[Background worker] -->|claim with FOR UPDATE SKIP LOCKED<br/>+ 60s lease| D
    W -->|after 30s: mark PROCESSED<br/>only if it still holds the lease| D
```

- **Postgres is both the database and the queue.** One atomic `INSERT` saves
  the transaction and queues it. With Celery + Redis there would be a gap
  between "saved" and "queued" where a crash loses the job.
- **Idempotency comes from the primary key.** `ON CONFLICT DO NOTHING` lets the
  database reject duplicates, so 50 simultaneous copies can never create two
  rows. A "check, then insert" approach has a race window.
- **Workers claim rows with `FOR UPDATE SKIP LOCKED`.** Several workers can
  run at once and never grab the same row.
- **A 60-second lease handles crashes.** A claimed row records which worker
  holds it and until when. If that worker dies, the lease expires and any
  worker reclaims the row. No watchdog process needed.
- **Completion is guarded.** `UPDATE ... WHERE locked_by = me`, so a worker
  whose lease expired can't mark a row someone else now owns.

| Decision | Chose | Over | Why |
|---|---|---|---|
| Queue | Postgres + `SKIP LOCKED` | Celery + Redis | No gap between saving and queueing; one less service |
| Background work | Dedicated `asyncio` worker | FastAPI `BackgroundTasks` | In-memory tasks vanish on restart |
| Crash recovery | Time-limited lease | Cleanup cron job | Abandoned rows become claimable automatically |
| DB access | `asyncpg` + plain SQL in one file | ORM | The locking queries are Postgres-specific and should be visible |
| Money | `NUMERIC(18,2)` | `float` | Exact decimals |

Trade-off: polling adds up to 1s of delay, and Postgres won't scale like a
dedicated broker. At much higher volume I'd keep the same insert-and-reply
API and move delivery to SQS/Kafka. The table already works as an outbox.

## What testing caught

Running the tests against real failures found bugs that a happy-path demo
would have hidden:

1. **~45ms wasted on every query.** I had set `statement_cache_size=0`,
   which is only needed behind a pgbouncer pooler. With a direct connection it
   made Postgres re-parse every statement. Removing it cut single inserts from
   ~48ms to ~4ms.
2. **The worker died permanently when the database went down.** One failed
   claim query escaped the polling loop. Now it logs a warning and keeps
   polling, and `test_worker_survives_claim_failures_and_recovers` plus the
   end-to-end outage test prove it resumes.
3. **A refused database connection returned `500` instead of `503`.** The
   handler caught timeouts but not `OSError`. A `500` tells the payment
   processor nothing useful; a `503` makes it retry.
4. **An amount too large for the column came back as `503`.** It passed
   validation, then failed in Postgres. It now returns `422`.
5. **My own crash test was flaky.** It killed the worker after a fixed 10s,
   which was sometimes before the worker had even claimed the row. It now
   waits for a confirmed claim before killing.

## Performance

Live, from India to Render's Singapore region, free instance (0.1 CPU):

| Load | Result |
|---|---|
| Single requests, one at a time | 120-160ms each (most of it is network round-trip) |
| 10 simultaneous | p50 497ms, p95 1.75s |
| 25 / 50 / 100 simultaneous | p95 3.2-4.5s |

The slowdown under bursts comes from the free instance's CPU, not the
application. Each request needs a few milliseconds of CPU for JSON parsing,
validation and the insert. On a tenth of a core, 25 simultaneous requests
queue for about 2 seconds. On a normal machine, the same code handles 100
simultaneous requests at p95 180ms (measured in-process against real
Postgres), and the database layer alone at p95 92ms. A paid instance with a
full CPU, or a second instance, would remove the gap; `SKIP LOCKED` already
makes multiple instances safe.

## Spec assumptions

| The brief says | I decided | Why |
|---|---|---|
| Query response is an array `[{...}]` | Return a one-item array | Automated tests may parse this exact shape |
| Amount is `1500` in one example, `150.50` in another | Accept decimals, max 2 places, store as `NUMERIC(18,2)` | Floats cause rounding errors with money |
| Currency is `INR` in one example, `USD` in another | Accept any 3-letter code, store uppercase | They're examples, not fixed values |
| Health response has a trailing comma | Return valid JSON | Typo |
| Handle duplicates "gracefully" | Always `202`, never `409` | A processor may treat an error as a reason to retry |
| Status is `PROCESSED` or `PROCESSING` | Only ever return those two | Extra states could fail a strict test |

## FAQ

**What happens if the worker dies at second 29?**
The row stays `PROCESSING` with a lease that expires 60s after the claim.
Once it expires, the next poll from any worker claims it again (`attempts`
becomes 2), waits the 30s, and completes it. It finishes later, but it
finishes, and exactly once.

**What if the API restarts right after replying `202`?**
Nothing is lost. The `202` is only sent after the insert commits, so the row
is already in Postgres and the worker picks it up.

**Why is the lease 60s when processing takes 30s?**
The lease must outlast the work. If it were shorter, a slow but healthy
worker could lose its row to a second worker mid-processing.

**Could two workers process the same row?**
No. `SKIP LOCKED` stops them claiming it at the same moment, the lease
filter stops a second claim while the first is valid, and the guarded
`UPDATE ... WHERE locked_by = me` stops a late worker from completing it.

**What if the database is down?**
Webhooks get `503` within 300ms, so the processor retries and nothing is
falsely acknowledged. `GET /` still returns `200` because it never touches
the database. The worker logs a warning each poll and resumes when the
database comes back.

## How I used AI

I used Claude Code as a coding assistant. I wrote the PRD and made the design
decisions (Postgres as the queue, the lease, the guarded completion). The AI
wrote much of the code from that spec. I ran every test and benchmark myself,
and when results looked wrong I dug into the cause rather than accepting them;
the "What testing caught" list above came out of that.

## What I'd add for production

- Cap `attempts` and move repeat failures to a dead-letter table
- Webhook signature verification (HMAC), left out here so plain test requests aren't rejected
- Metrics and alerts: queue depth, processing latency, retry counts
- Structured logs with `transaction_id` on every line
- At higher volume, delivery via SQS/Kafka from the same table used as an outbox

<details>
<summary><b>Run it locally</b></summary>

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

The table is created automatically on startup. The worker runs inside the
API process. To run it as a separate process instead:

```bash
RUN_EMBEDDED_WORKER=false uvicorn app.main:app     # terminal 1
python -m app.worker                                # terminal 2
```

On Windows, `python scripts/run_dev_windows.py` starts the server with the
selector event loop, which handles many simultaneous local connections better
than the default. Linux doesn't need it.

</details>

<details>
<summary><b>Run the tests</b></summary>

Unit and integration tests (37), using a fast 1s processing delay:

```bash
docker compose up -d
export DATABASE_URL=postgresql://postgres:postgres@localhost:5432/webhooks
export PROCESSING_DELAY_SECONDS=1 LEASE_SECONDS=5 POLL_INTERVAL_SECONDS=0.2
pytest tests/ -v
```

All four success criteria end to end, with the real 30s delay and 60s lease
(about 5 minutes). It starts from an empty table, manages the server and
worker itself, stops and restarts Postgres for the outage check, and exits
with code 0 only if everything passes:

```bash
unset PROCESSING_DELAY_SECONDS LEASE_SECONDS POLL_INTERVAL_SECONDS
python scripts/run_all.py
```

Individual checks against any running server, local or live:

```bash
bash scripts/smoke_test.sh $URL          # one webhook end to end (~35s)
python scripts/load_test.py $URL 100     # concurrent response times
```

| Test file | Covers |
|---|---|
| `test_api.py` | Endpoint contracts, 404, array response shape |
| `test_validation.py` | Every missing field, bad amounts and currencies, first-version-wins duplicates |
| `test_idempotency.py` | Sequential and 50-concurrent duplicate webhooks |
| `test_worker.py` | Claim, process, complete; reclaiming an expired lease |
| `test_failures.py` | Database refused or hanging → fast `503`; health unaffected; worker survives outages |

</details>

<details>
<summary><b>Configuration</b></summary>

| Variable | Default | Purpose |
|---|---|---|
| `DATABASE_URL` | required | Postgres connection string |
| `PORT` | 8000 | Port the server listens on (set by the host) |
| `PROCESSING_DELAY_SECONDS` | 30 | Simulated external call |
| `LEASE_SECONDS` | 60 | How long a claim lasts before another worker can take the row |
| `POLL_INTERVAL_SECONDS` | 1 | Time between claim attempts |
| `WORKER_CONCURRENCY` | 100 | Max rows processing at once |
| `DB_POOL_SIZE` | 10 | Database connections per process |
| `DB_QUERY_TIMEOUT_MS` | 300 | Webhook insert time limit before returning `503` |
| `STATEMENT_CACHE_SIZE` | 100 | Set to `0` only behind a transaction-mode pgbouncer (e.g. Supabase's pooler) |
| `RUN_EMBEDDED_WORKER` | true | `false` runs the API without its worker |

</details>

<details>
<summary><b>Project structure</b></summary>

```
app/
  main.py         FastAPI app; startup runs migrations, starts the worker
  routes.py       The three endpoints
  schemas.py      Request/response models and validation
  repository.py   Every SQL query
  worker.py       Claim -> process -> complete, with lease recovery
  db.py           Connection pool and startup migrations
  config.py       Settings from environment variables
migrations/
  001_init.sql    Schema (safe to re-run)
tests/            37 tests, see "Run the tests"
scripts/
  run_all.py      All four success criteria end to end
  smoke_test.sh   One webhook end to end
  load_test.py    Concurrent response times
  crash_test.py   Kill and restart the worker mid-processing
Dockerfile
docker-compose.yml   Local Postgres
```

</details>

<details>
<summary><b>Deployment</b></summary>

- **Render** free web service (Docker) + free Postgres, both in Singapore.
- Render's free tier sleeps after 15 minutes without traffic, which would make
  the first request slow. An UptimeRobot monitor pings `GET /` every 5 minutes
  to keep it awake. That endpoint doesn't touch the database.
- The table is created on startup (`migrations/*.sql`, safe to re-run, guarded
  by a Postgres advisory lock so concurrent starts can't race).
- `DATABASE_URL` uses Render's internal connection string, set in the Render
  dashboard and never committed.
- Render's free Postgres expires after 30 days.

</details>
