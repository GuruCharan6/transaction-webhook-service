import subprocess
import sys
import time

import httpx

BASE_URL = sys.argv[1] if len(sys.argv) > 1 else "http://localhost:8000"
TXN_ID = f"txn_crash_{int(time.time())}"


def main() -> None:
    body = {
        "transaction_id": TXN_ID,
        "source_account": "acc_user_789",
        "destination_account": "acc_merchant_456",
        "amount": 25.00,
        "currency": "INR",
    }

    print("starting worker process...")
    worker_proc = subprocess.Popen([sys.executable, "-m", "app.worker"])

    time.sleep(1)

    resp = httpx.post(f"{BASE_URL}/v1/webhooks/transactions", json=body)
    assert resp.status_code == 202, resp.text
    print(f"webhook sent: {TXN_ID}")

    print("waiting 10s before killing worker mid-processing...")
    time.sleep(10)

    print("killing worker...")
    worker_proc.kill()
    worker_proc.wait()

    print("restarting worker...")
    worker_proc = subprocess.Popen([sys.executable, "-m", "app.worker"])

    deadline = time.time() + 120
    status = None
    while time.time() < deadline:
        r = httpx.get(f"{BASE_URL}/v1/transactions/{TXN_ID}")
        status = r.json()[0]["status"]
        if status == "PROCESSED":
            break
        time.sleep(1)

    worker_proc.kill()
    worker_proc.wait()

    if status != "PROCESSED":
        print(f"FAIL: transaction ended in status={status}, expected PROCESSED")
        sys.exit(1)

    print(f"PASS: transaction reached PROCESSED after worker crash and restart")


if __name__ == "__main__":
    main()
