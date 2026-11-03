import asyncio
import statistics
import sys
import time
import uuid

import httpx

BASE_URL = sys.argv[1] if len(sys.argv) > 1 else "http://localhost:8000"
CONCURRENCY = int(sys.argv[2]) if len(sys.argv) > 2 else 100


async def send_one(client: httpx.AsyncClient, i: int) -> float:
    body = {
        "transaction_id": f"txn_load_{uuid.uuid4().hex[:10]}_{i}",
        "source_account": "acc_cust_4821",
        "destination_account": "acc_store_1093",
        "amount": 10.00,
        "currency": "INR",
    }
    start = time.perf_counter()
    resp = await client.post(f"{BASE_URL}/v1/webhooks/transactions", json=body)
    elapsed = (time.perf_counter() - start) * 1000
    assert resp.status_code == 202, f"unexpected status {resp.status_code}: {resp.text}"
    return elapsed


async def main() -> None:
    async with httpx.AsyncClient(timeout=10) as client:
        durations = await asyncio.gather(*[send_one(client, i) for i in range(CONCURRENCY)])

    durations.sort()
    p95 = statistics.quantiles(durations, n=100)[94]
    print(f"requests: {len(durations)}")
    print(f"min: {durations[0]:.1f}ms  max: {durations[-1]:.1f}ms")
    print(f"p50: {statistics.median(durations):.1f}ms  p95: {p95:.1f}ms")
    if p95 >= 200:
        print("WARN: p95 at or above 200ms target")
    if durations[-1] >= 500:
        print("FAIL: a request exceeded the 500ms hard limit")
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
