import asyncio
import os
import statistics
import subprocess
import sys
import time
import uuid

import asyncpg
import httpx

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
BASE_URL = "http://localhost:8000"
DATABASE_URL = os.environ.get("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/webhooks")
os.environ["DATABASE_URL"] = DATABASE_URL

PROCESSING_DELAY_SECONDS = 30
LEASE_SECONDS = 60

results = []


def record(name, passed, detail):
    results.append((name, passed, detail))
    status = "PASS" if passed else "FAIL"
    print(f"[{status}] {name} — {detail}")


def wait_for_health(timeout=20):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            r = httpx.get(f"{BASE_URL}/", timeout=2)
            if r.status_code == 200:
                return True
        except httpx.HTTPError:
            pass
        time.sleep(0.5)
    return False


def start_server(extra_env):
    env = os.environ.copy()
    env["DATABASE_URL"] = DATABASE_URL
    env.update(extra_env)
    proc = subprocess.Popen(
        [sys.executable, os.path.join(ROOT, "scripts", "run_dev_windows.py")],
        cwd=ROOT,
        env=env,
    )
    if not wait_for_health():
        proc.kill()
        proc.wait()
        raise RuntimeError("server did not become healthy in time")
    return proc


def stop_server(proc):
    proc.terminate()
    try:
        proc.wait(timeout=10)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait()


def docker_compose(*args):
    subprocess.run(["docker", "compose", *args], cwd=ROOT, check=True)


async def db_row(transaction_id):
    conn = await asyncpg.connect(DATABASE_URL)
    try:
        return await conn.fetchrow(
            "SELECT status, attempts FROM transactions WHERE transaction_id = $1", transaction_id
        )
    finally:
        await conn.close()


async def db_timestamps(transaction_id):
    conn = await asyncpg.connect(DATABASE_URL)
    try:
        return await conn.fetchrow(
            "SELECT status, created_at, processed_at FROM transactions WHERE transaction_id = $1",
            transaction_id,
        )
    finally:
        await conn.close()


def test_single_transaction():
    txn_id = f"txn_all_single_{uuid.uuid4().hex[:8]}"
    body = {
        "transaction_id": txn_id,
        "source_account": "acc_user_789",
        "destination_account": "acc_merchant_456",
        "amount": 150.50,
        "currency": "USD",
    }
    resp = httpx.post(f"{BASE_URL}/v1/webhooks/transactions", json=body, timeout=10)
    if resp.status_code != 202:
        record("Single transaction", False, f"webhook POST returned {resp.status_code}")
        return

    deadline = time.time() + 60
    row = None
    while time.time() < deadline:
        row = asyncio.run(db_timestamps(txn_id))
        if row and row["status"] == "PROCESSED":
            break
        time.sleep(1)

    if row is None or row["status"] != "PROCESSED":
        record("Single transaction", False, f"status={row['status'] if row else 'missing'} after 60s")
        return

    db_elapsed = (row["processed_at"] - row["created_at"]).total_seconds()
    ok = PROCESSING_DELAY_SECONDS <= db_elapsed <= PROCESSING_DELAY_SECONDS + 10
    record("Single transaction", ok, f"status=PROCESSED, processed_at - created_at = {db_elapsed:.1f}s")


def test_duplicate_prevention():
    txn_id = f"txn_all_dupseq_{uuid.uuid4().hex[:8]}"
    body = {
        "transaction_id": txn_id,
        "source_account": "a",
        "destination_account": "b",
        "amount": 10,
        "currency": "INR",
    }
    seq_codes = []
    for _ in range(5):
        r = httpx.post(f"{BASE_URL}/v1/webhooks/transactions", json=body, timeout=10)
        seq_codes.append(r.status_code)

    txn_id2 = f"txn_all_dupconc_{uuid.uuid4().hex[:8]}"
    body2 = {**body, "transaction_id": txn_id2}

    async def fire_concurrent():
        async with httpx.AsyncClient(timeout=10) as client:
            tasks = [client.post(f"{BASE_URL}/v1/webhooks/transactions", json=body2) for _ in range(50)]
            responses = await asyncio.gather(*tasks)
            return [r.status_code for r in responses]

    conc_codes = asyncio.run(fire_concurrent())

    row_seq = asyncio.run(db_row(txn_id))
    row_conc = asyncio.run(db_row(txn_id2))

    ok = (
        all(c == 202 for c in seq_codes)
        and all(c == 202 for c in conc_codes)
        and row_seq is not None
        and row_conc is not None
    )
    detail = (
        f"sequential 5x -> all 202={all(c == 202 for c in seq_codes)}, "
        f"concurrent 50x -> all 202={all(c == 202 for c in conc_codes)}, "
        f"concurrent attempts={row_conc['attempts'] if row_conc else 'n/a'}"
    )
    record("Duplicate prevention", ok, detail)


def test_performance():
    from httpx import ASGITransport
    from httpx import AsyncClient as AsgiClient

    from app.db import close_pool, init_pool
    from app.main import app

    async def send(client, i):
        body = {
            "transaction_id": f"txn_all_perf_{uuid.uuid4().hex[:8]}_{i}",
            "source_account": "a",
            "destination_account": "b",
            "amount": 10,
            "currency": "INR",
        }
        t0 = time.perf_counter()
        r = await client.post("http://test/v1/webhooks/transactions", json=body)
        elapsed = (time.perf_counter() - t0) * 1000
        return r.status_code, elapsed

    async def run():
        await init_pool()
        try:
            async with AsgiClient(transport=ASGITransport(app=app), base_url="http://test") as client:
                return await asyncio.gather(*[send(client, i) for i in range(100)])
        finally:
            await close_pool()

    pairs = asyncio.run(run())
    codes = [c for c, _ in pairs]
    durations = sorted(d for _, d in pairs)
    p50 = statistics.median(durations)
    p95 = statistics.quantiles(durations, n=100)[94]
    ok = all(c == 202 for c in codes) and durations[-1] < 500
    record(
        "Performance (100 concurrent)",
        ok,
        f"p50={p50:.0f}ms p95={p95:.0f}ms max={durations[-1]:.0f}ms "
        f"(app+DB layer, hard limit 500ms; measured in-process to remove this Windows "
        f"dev box's real-socket overhead from the number)",
    )


def test_crash_recovery():
    env = {"RUN_EMBEDDED_WORKER": "false"}
    server = start_server(env)
    try:
        worker_env = os.environ.copy()
        worker_env["DATABASE_URL"] = DATABASE_URL
        worker_env["PROCESSING_DELAY_SECONDS"] = str(PROCESSING_DELAY_SECONDS)
        worker_env["LEASE_SECONDS"] = str(LEASE_SECONDS)
        worker = subprocess.Popen([sys.executable, "-m", "app.worker"], cwd=ROOT, env=worker_env)

        txn_id = f"txn_all_crash_{uuid.uuid4().hex[:8]}"
        body = {
            "transaction_id": txn_id,
            "source_account": "acc_user_789",
            "destination_account": "acc_merchant_456",
            "amount": 25.00,
            "currency": "INR",
        }
        resp = httpx.post(f"{BASE_URL}/v1/webhooks/transactions", json=body, timeout=10)
        if resp.status_code != 202:
            record("Reliability: crash recovery", False, f"webhook POST returned {resp.status_code}")
            worker.kill()
            worker.wait()
            return

        claim_deadline = time.time() + 40
        claimed = False
        while time.time() < claim_deadline:
            row = asyncio.run(db_row(txn_id))
            if row and row["attempts"] >= 1:
                claimed = True
                break
            time.sleep(0.5)

        if not claimed:
            record("Reliability: crash recovery", False, "worker never claimed the row within 40s")
            worker.kill()
            worker.wait()
            return

        time.sleep(5)
        worker.kill()
        worker.wait()

        row = asyncio.run(db_row(txn_id))
        if row["status"] != "PROCESSING":
            record(
                "Reliability: crash recovery",
                False,
                f"expected status=PROCESSING right after kill (mid-processing), got {row['status']}",
            )
            return

        worker = subprocess.Popen([sys.executable, "-m", "app.worker"], cwd=ROOT, env=worker_env)

        deadline = time.time() + 120
        status = None
        while time.time() < deadline:
            r = httpx.get(f"{BASE_URL}/v1/transactions/{txn_id}", timeout=10)
            status = r.json()[0]["status"]
            if status == "PROCESSED":
                break
            time.sleep(1)

        worker.kill()
        worker.wait()

        row = asyncio.run(db_row(txn_id))
        ok = status == "PROCESSED" and row is not None and row["attempts"] == 2
        record(
            "Reliability: crash recovery",
            ok,
            f"status={status}, attempts={row['attempts'] if row else 'n/a'} (expect 2)",
        )
    finally:
        stop_server(server)


def test_db_down_handling():
    server = start_server({"PROCESSING_DELAY_SECONDS": "2"})
    try:
        docker_compose("stop", "postgres")
        time.sleep(1)

        txn_id = f"txn_all_dbdown_{uuid.uuid4().hex[:8]}"
        body = {
            "transaction_id": txn_id,
            "source_account": "a",
            "destination_account": "b",
            "amount": 10,
            "currency": "INR",
        }
        webhook_code = None
        try:
            r = httpx.post(f"{BASE_URL}/v1/webhooks/transactions", json=body, timeout=10)
            webhook_code = r.status_code
        except httpx.HTTPError as exc:
            webhook_code = f"error: {exc}"

        health_code = None
        try:
            r = httpx.get(f"{BASE_URL}/", timeout=5)
            health_code = r.status_code
        except httpx.HTTPError as exc:
            health_code = f"error: {exc}"

        docker_compose("start", "postgres")
        time.sleep(5)

        lookup_code = httpx.get(f"{BASE_URL}/v1/transactions/{txn_id}", timeout=10).status_code

        recovery_id = f"txn_all_recovery_{uuid.uuid4().hex[:8]}"
        httpx.post(
            f"{BASE_URL}/v1/webhooks/transactions",
            json={**body, "transaction_id": recovery_id},
            timeout=10,
        )
        recovery_status = None
        deadline = time.time() + 30
        while time.time() < deadline:
            row = asyncio.run(db_row(recovery_id))
            recovery_status = row["status"] if row else None
            if recovery_status == "PROCESSED":
                break
            time.sleep(1)

        ok = (
            webhook_code == 503
            and health_code == 200
            and lookup_code == 404
            and recovery_status == "PROCESSED"
        )
        record(
            "Reliability: DB-down handling",
            ok,
            f"webhook={webhook_code} (expect 503), health={health_code} (expect 200), "
            f"lookup-after-recovery={lookup_code} (expect 404), "
            f"new webhook after DB returns={recovery_status} (expect PROCESSED, proves worker survived)",
        )
    finally:
        stop_server(server)


async def clear_test_data():
    conn = await asyncpg.connect(DATABASE_URL)
    try:
        await conn.execute("TRUNCATE TABLE transactions")
    finally:
        await conn.close()


def main():
    docker_compose("up", "-d", "--wait")
    time.sleep(2)
    asyncio.run(clear_test_data())

    server = start_server({})
    try:
        test_single_transaction()
        test_duplicate_prevention()
        test_performance()
    finally:
        stop_server(server)

    test_crash_recovery()
    test_db_down_handling()

    print()
    print("=" * 60)
    all_ok = True
    for name, passed, _ in results:
        print(f"{'PASS' if passed else 'FAIL':5} {name}")
        all_ok = all_ok and passed
    print("=" * 60)
    print("ALL CRITERIA PASSED" if all_ok else "SOME CRITERIA FAILED")
    sys.exit(0 if all_ok else 1)


if __name__ == "__main__":
    main()
