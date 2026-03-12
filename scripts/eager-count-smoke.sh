#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

if ! command -v python3 >/dev/null 2>&1; then
  echo "python3 is required" >&2
  exit 1
fi

DEV_STACK_BUILD="${DEV_STACK_BUILD:-0}" scripts/dev-stack.sh up

ENV_FILE="${DEV_STACK_DIR:-target/dev-stack}/environment.sh"
if [[ -f "$ENV_FILE" ]]; then
  # shellcheck disable=SC1090
  source "$ENV_FILE"
fi

TENANT_ID="${DEV_STACK_TENANT_ID:-dev}"
TASK_QUEUE="${DEV_STACK_TASK_QUEUE:-bulk}"
API_GATEWAY_URL="${API_GATEWAY_URL:-http://127.0.0.1:3000}"
REPORT_PATH="${EAGER_COUNT_REPORT_PATH:-target/dev-stack/reports/eager-count-smoke.json}"
TIMEOUT_SECS="${EAGER_COUNT_TIMEOUT_SECS:-90}"
mkdir -p "$(dirname "$REPORT_PATH")"

echo "[eager-count-smoke] configuring task queue throughput policy via api-gateway"
curl -fsS \
  -X PUT \
  -H 'content-type: application/json' \
  -d '{"backend":"stream-v2"}' \
  "${API_GATEWAY_URL}/admin/tenants/${TENANT_ID}/task-queues/activity/${TASK_QUEUE}/throughput-policy" \
  >/dev/null

echo "[eager-count-smoke] running benchmark-runner on eager/count"
API_GATEWAY_URL="$API_GATEWAY_URL" cargo run -p benchmark-runner -- \
  --profile smoke \
  --workflow-count 1 \
  --activities-per-workflow 16 \
  --worker-count 2 \
  --tenant-id "$TENANT_ID" \
  --task-queue "$TASK_QUEUE" \
  --execution-mode throughput \
  --throughput-backend stream-v2 \
  --chunk-size 4 \
  --bulk-reducer count \
  --timeout-secs "$TIMEOUT_SECS" \
  --output "$REPORT_PATH"

INSTANCE_ID="$(python3 - "$REPORT_PATH" <<'PY'
import json
import sys

with open(sys.argv[1], "r", encoding="utf-8") as handle:
    report = json.load(handle)
print(f"{report['instance_prefix']}-0000")
PY
)"

RUNS_JSON="$(curl -fsS "${API_GATEWAY_URL}/tenants/${TENANT_ID}/workflows/${INSTANCE_ID}/runs")"
RUN_ID="$(python3 - <<'PY' "$RUNS_JSON"
import json
import sys

payload = json.loads(sys.argv[1])
run_id = payload.get("current_run_id")
if not run_id and payload.get("runs"):
    run_id = payload["runs"][0]["run_id"]
if not run_id:
    raise SystemExit("missing run_id in workflow runs response")
print(run_id)
PY
)"

EVENTUAL_BATCHES_JSON="$(curl -fsS "${API_GATEWAY_URL}/tenants/${TENANT_ID}/workflows/${INSTANCE_ID}/runs/${RUN_ID}/bulk-batches?consistency=eventual")"
STRONG_BATCHES_JSON="$(curl -fsS "${API_GATEWAY_URL}/tenants/${TENANT_ID}/workflows/${INSTANCE_ID}/runs/${RUN_ID}/bulk-batches?consistency=strong")"

BATCH_ID="$(python3 - <<'PY' "$EVENTUAL_BATCHES_JSON" "$STRONG_BATCHES_JSON"
import json
import sys

eventual = json.loads(sys.argv[1])
strong = json.loads(sys.argv[2])
if eventual.get("batch_count") != 1 or strong.get("batch_count") != 1:
    raise SystemExit(
        f"expected exactly one batch; eventual={eventual.get('batch_count')} strong={strong.get('batch_count')}"
    )
eventual_batch = eventual["batches"][0]
strong_batch = strong["batches"][0]
expected = {
    "status": "completed",
    "throughput_backend": "stream-v2",
    "succeeded_items": 16,
    "failed_items": 0,
    "cancelled_items": 0,
}
for field, value in expected.items():
    lhs = eventual_batch[field]
    rhs = strong_batch[field]
    if lhs != value or rhs != value:
        raise SystemExit(f"unexpected {field}: eventual={lhs!r} strong={rhs!r} expected={value!r}")
if eventual_batch["batch_id"] != strong_batch["batch_id"]:
    raise SystemExit("batch ids differ between eventual and strong reads")
print(eventual_batch["batch_id"])
PY
)"

EVENTUAL_BATCH_JSON="$(curl -fsS "${API_GATEWAY_URL}/tenants/${TENANT_ID}/workflows/${INSTANCE_ID}/runs/${RUN_ID}/bulk-batches/${BATCH_ID}?consistency=eventual")"
STRONG_BATCH_JSON="$(curl -fsS "${API_GATEWAY_URL}/tenants/${TENANT_ID}/workflows/${INSTANCE_ID}/runs/${RUN_ID}/bulk-batches/${BATCH_ID}?consistency=strong")"
EVENTUAL_CHUNKS_JSON="$(curl -fsS "${API_GATEWAY_URL}/tenants/${TENANT_ID}/workflows/${INSTANCE_ID}/runs/${RUN_ID}/bulk-batches/${BATCH_ID}/chunks?consistency=eventual")"
STRONG_CHUNKS_JSON="$(curl -fsS "${API_GATEWAY_URL}/tenants/${TENANT_ID}/workflows/${INSTANCE_ID}/runs/${RUN_ID}/bulk-batches/${BATCH_ID}/chunks?consistency=strong")"

python3 - <<'PY' \
"$EVENTUAL_BATCH_JSON" \
"$STRONG_BATCH_JSON" \
"$EVENTUAL_CHUNKS_JSON" \
"$STRONG_CHUNKS_JSON"
import json
import sys

eventual_batch = json.loads(sys.argv[1])
strong_batch = json.loads(sys.argv[2])
eventual_chunks = json.loads(sys.argv[3])
strong_chunks = json.loads(sys.argv[4])

for payload, expected_consistency in (
    (eventual_batch, "eventual"),
    (strong_batch, "strong"),
    (eventual_chunks, "eventual"),
    (strong_chunks, "strong"),
):
    if payload["consistency"] != expected_consistency:
        raise SystemExit(
            f"expected consistency={expected_consistency}, got {payload['consistency']}"
        )

eventual_summary = eventual_batch["batch"]
strong_summary = strong_batch["batch"]
for field in ("status", "succeeded_items", "failed_items", "cancelled_items", "chunk_count"):
    if eventual_summary[field] != strong_summary[field]:
        raise SystemExit(
            f"batch mismatch for {field}: eventual={eventual_summary[field]!r} strong={strong_summary[field]!r}"
        )

if eventual_chunks["chunk_count"] != strong_chunks["chunk_count"]:
    raise SystemExit("chunk counts differ between eventual and strong reads")
if eventual_chunks["chunk_count"] == 0:
    raise SystemExit("expected at least one chunk")
PY

echo "[eager-count-smoke] success"
echo "tenant_id=$TENANT_ID"
echo "task_queue=$TASK_QUEUE"
echo "instance_id=$INSTANCE_ID"
echo "run_id=$RUN_ID"
echo "batch_id=$BATCH_ID"
echo "report_path=$REPORT_PATH"
