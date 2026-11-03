#!/usr/bin/env bash
set -euo pipefail

BASE_URL="${1:-http://localhost:8000}"
TXN_ID="txn_smoke_$(date +%s)"

echo "== Health check =="
curl -sf "$BASE_URL/" | tee /dev/stderr
echo

echo "== Send webhook =="
STATUS=$(curl -s -o /tmp/smoke_resp.json -w "%{http_code}" -X POST "$BASE_URL/v1/webhooks/transactions" \
  -H "Content-Type: application/json" \
  -d "{\"transaction_id\":\"$TXN_ID\",\"source_account\":\"acc_cust_4821\",\"destination_account\":\"acc_store_1093\",\"amount\":899.75,\"currency\":\"EUR\"}")
cat /tmp/smoke_resp.json
echo
if [ "$STATUS" != "202" ]; then
  echo "FAIL: expected 202, got $STATUS"
  exit 1
fi

echo "== Immediate query (should be PROCESSING) =="
curl -sf "$BASE_URL/v1/transactions/$TXN_ID"
echo

echo "== Duplicate webhook (should still be 202, no error) =="
curl -s -o /dev/null -w "status=%{http_code}\n" -X POST "$BASE_URL/v1/webhooks/transactions" \
  -H "Content-Type: application/json" \
  -d "{\"transaction_id\":\"$TXN_ID\",\"source_account\":\"acc_cust_4821\",\"destination_account\":\"acc_store_1093\",\"amount\":899.75,\"currency\":\"EUR\"}"

echo "== Waiting ~30s for processing =="
sleep 32

echo "== Final query (should be PROCESSED) =="
curl -sf "$BASE_URL/v1/transactions/$TXN_ID"
echo

echo "== Unknown transaction (should 404) =="
curl -s -o /dev/null -w "status=%{http_code}\n" "$BASE_URL/v1/transactions/does_not_exist"

echo "Smoke test complete."
