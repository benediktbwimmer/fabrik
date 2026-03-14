#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

require_bin() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "$1 is required" >&2
    exit 1
  fi
}

wait_for_http() {
  local url=$1
  local deadline=$((SECONDS + 120))
  while (( SECONDS < deadline )); do
    if curl -fsS "$url" >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done
  echo "timed out waiting for $url" >&2
  return 1
}

json_request() {
  local method=$1
  local path=$2
  local body_file=${3:-}
  local output_file=$4
  local url="${API_GATEWAY_URL}${path}"
  if [[ -n "$body_file" ]]; then
    curl -fsS -X "$method" "$url" \
      -H 'content-type: application/json' \
      --data-binary @"$body_file" \
      >"$output_file"
  else
    curl -fsS -X "$method" "$url" >"$output_file"
  fi
}

poll_json_path() {
  local path=$1
  local expression=$2
  local output_file=$3
  local timeout_seconds=${4:-60}
  python3 - "$API_GATEWAY_URL" "$path" "$expression" "$output_file" "$timeout_seconds" <<'PY'
import json
import sys
import time
import urllib.request

base, path, expression, output_path, timeout_seconds = sys.argv[1:6]
deadline = time.time() + int(timeout_seconds)
last = None
while time.time() < deadline:
    with urllib.request.urlopen(base + path, timeout=5) as response:
        payload = json.load(response)
    last = payload
    data = payload
    if eval(expression, {"__builtins__": {}}, {"data": data}):
        with open(output_path, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=2)
            handle.write("\n")
        sys.exit(0)
    time.sleep(1)

if last is not None:
    with open(output_path, "w", encoding="utf-8") as handle:
        json.dump(last, handle, indent=2)
        handle.write("\n")
print(f"timed out waiting for {path} to satisfy {expression}", file=sys.stderr)
sys.exit(1)
PY
}

require_bin curl
require_bin docker
require_bin node
require_bin python3

if [[ -f target/dev-stack/environment.sh ]]; then
  # shellcheck disable=SC1091
  source target/dev-stack/environment.sh
fi

API_GATEWAY_URL="${API_GATEWAY_URL:-http://127.0.0.1:3000}"
API_READY_PATH="${STREAMING_QUICKSTART_READY_PATH:-/admin/tenants}"
TENANT_ID="${DEV_STACK_TENANT_ID:-dev}"
TASK_QUEUE="${DEV_STACK_TASK_QUEUE:-bulk}"
REDPANDA_BROKERS="${REDPANDA_BROKERS:-localhost:29092}"
TOPIC_NAME="${STREAMING_QUICKSTART_TOPIC:-streaming.quickstart.events}"
ADAPTER_ID="${STREAMING_QUICKSTART_ADAPTER_ID:-local-streaming-quickstart}"
DEFINITION_ID="${STREAMING_QUICKSTART_DEFINITION_ID:-local-streaming-quickstart-workflow}"
OUTPUT_DIR="${STREAMING_QUICKSTART_OUTPUT_DIR:-target/quickstarts/streaming-local-quickstart}"
ARTIFACT_PATH="$OUTPUT_DIR/${DEFINITION_ID}.artifact.json"
ADAPTER_PATH="$OUTPUT_DIR/${ADAPTER_ID}.adapter.json"
EVENT_PATH="$OUTPUT_DIR/${ADAPTER_ID}.event.json"
PRODUCE_VALUE_PATH="$OUTPUT_DIR/${ADAPTER_ID}.produce.jsonl"
PREVIEW_PATH="$OUTPUT_DIR/${ADAPTER_ID}.preview.json"
ARTIFACT_RESPONSE_PATH="$OUTPUT_DIR/artifact-response.json"
POLICY_RESPONSE_PATH="$OUTPUT_DIR/throughput-policy-response.json"
ADAPTER_RESPONSE_PATH="$OUTPUT_DIR/adapter-response.json"
WORKFLOW_RESPONSE_PATH="$OUTPUT_DIR/workflow.json"
BATCHES_RESPONSE_PATH="$OUTPUT_DIR/bulk-batches.json"
INSTANCE_ID="wf-local-quickstart-$(python3 - <<'PY'
import uuid
print(str(uuid.uuid4())[:8])
PY
)"
REQUEST_ID="req-local-quickstart-${INSTANCE_ID}"
EVENT_ID="evt-local-quickstart-${INSTANCE_ID}"

mkdir -p "$OUTPUT_DIR"

if ! curl -fsS "${API_GATEWAY_URL}${API_READY_PATH}" >/dev/null 2>&1; then
  echo "[streaming-quickstart] api gateway unavailable, starting dev stack"
  scripts/dev-stack.sh up
  if [[ -f target/dev-stack/environment.sh ]]; then
    # shellcheck disable=SC1091
    source target/dev-stack/environment.sh
    API_GATEWAY_URL="${API_GATEWAY_URL:-http://127.0.0.1:3000}"
    TENANT_ID="${DEV_STACK_TENANT_ID:-dev}"
    TASK_QUEUE="${DEV_STACK_TASK_QUEUE:-bulk}"
    REDPANDA_BROKERS="${REDPANDA_BROKERS:-localhost:29092}"
  fi
fi

wait_for_http "${API_GATEWAY_URL}${API_READY_PATH}"

echo "[streaming-quickstart] compiling workflow artifact"
node sdk/typescript-compiler/compiler.mjs \
  --entry examples/typescript-workflows/local-streaming-quickstart-workflow.ts \
  --export localStreamingQuickstartWorkflow \
  --definition-id "$DEFINITION_ID" \
  --version 1 \
  --out "$ARTIFACT_PATH"
cargo run -q -p fabrik-cli --bin rehash-artifact -- "$ARTIFACT_PATH" >/dev/null

echo "[streaming-quickstart] rendering adapter and sample event"
python3 - \
  /Users/bene/code/fabrik/examples/topic-adapters/local-streaming-quickstart.json \
  "$ADAPTER_PATH" \
  "$REDPANDA_BROKERS" \
  "$TOPIC_NAME" \
  "$TASK_QUEUE" \
  "$DEFINITION_ID" \
  <<'PY'
import json
import sys

source_path, output_path, brokers, topic_name, task_queue, definition_id = sys.argv[1:]
payload = json.load(open(source_path, "r", encoding="utf-8"))
payload["brokers"] = brokers
payload["topic_name"] = topic_name
payload["workflow_task_queue"] = task_queue
payload["definition_id"] = definition_id
with open(output_path, "w", encoding="utf-8") as handle:
    json.dump(payload, handle, indent=2)
    handle.write("\n")
PY

python3 - \
  /Users/bene/code/fabrik/examples/topic-adapters/local-streaming-quickstart-event.json \
  "$EVENT_PATH" \
  "$PRODUCE_VALUE_PATH" \
  "$INSTANCE_ID" \
  "$REQUEST_ID" \
  "$EVENT_ID" \
  <<'PY'
import json
import sys

source_path, output_path, produce_value_path, instance_id, request_id, event_id = sys.argv[1:]
payload = json.load(open(source_path, "r", encoding="utf-8"))
payload["instance_id"] = instance_id
payload["request_id"] = request_id
payload["event_id"] = event_id
with open(output_path, "w", encoding="utf-8") as handle:
    json.dump(payload, handle, indent=2)
    handle.write("\n")
with open(produce_value_path, "w", encoding="utf-8") as handle:
    json.dump(payload, handle, separators=(",", ":"))
    handle.write("\n")
PY

echo "[streaming-quickstart] publishing workflow artifact"
json_request POST \
  "/tenants/${TENANT_ID}/workflow-artifacts?validate_existing_runs=false" \
  "$ARTIFACT_PATH" \
  "$ARTIFACT_RESPONSE_PATH"

echo "[streaming-quickstart] pinning the activity queue to stream-v2"
python3 - "$OUTPUT_DIR/throughput-policy-request.json" <<'PY'
import json
import sys
with open(sys.argv[1], "w", encoding="utf-8") as handle:
    json.dump({"backend": "stream-v2"}, handle, indent=2)
    handle.write("\n")
PY
json_request PUT \
  "/admin/tenants/${TENANT_ID}/task-queues/activity/${TASK_QUEUE}/throughput-policy" \
  "$OUTPUT_DIR/throughput-policy-request.json" \
  "$POLICY_RESPONSE_PATH"

echo "[streaming-quickstart] upserting topic adapter"
json_request PUT \
  "/admin/tenants/${TENANT_ID}/topic-adapters/${ADAPTER_ID}" \
  "$ADAPTER_PATH" \
  "$ADAPTER_RESPONSE_PATH"

BASE_PROCESSED_COUNT="$(python3 - "$ADAPTER_RESPONSE_PATH" <<'PY'
import json
import sys
payload = json.load(open(sys.argv[1], "r", encoding="utf-8"))
print((payload.get("adapter") or {}).get("processed_count", 0))
PY
)"

echo "[streaming-quickstart] previewing adapter dispatch"
python3 - "$EVENT_PATH" "$OUTPUT_DIR/preview-request.json" <<'PY'
import json
import sys
payload = json.load(open(sys.argv[1], "r", encoding="utf-8"))
with open(sys.argv[2], "w", encoding="utf-8") as handle:
    json.dump({"payload": payload, "partition_id": 0, "log_offset": 0}, handle, indent=2)
    handle.write("\n")
PY
json_request POST \
  "/admin/tenants/${TENANT_ID}/topic-adapters/${ADAPTER_ID}/preview" \
  "$OUTPUT_DIR/preview-request.json" \
  "$PREVIEW_PATH"

echo "[streaming-quickstart] ensuring Redpanda topic ${TOPIC_NAME} exists"
docker exec fabrik-redpanda-1 rpk topic create "$TOPIC_NAME" -p 1 >/dev/null 2>&1 || true

echo "[streaming-quickstart] producing one topic record"
docker exec -i fabrik-redpanda-1 rpk topic produce "$TOPIC_NAME" -f '%v\n' <"$PRODUCE_VALUE_PATH" >/dev/null

echo "[streaming-quickstart] waiting for adapter to process the record"
poll_json_path \
  "/admin/tenants/${TENANT_ID}/topic-adapters/${ADAPTER_ID}" \
  "data.get('adapter', {}).get('processed_count', 0) > ${BASE_PROCESSED_COUNT}" \
  "$OUTPUT_DIR/adapter-detail.json" \
  60

echo "[streaming-quickstart] waiting for workflow ${INSTANCE_ID} to materialize"
poll_json_path \
  "/tenants/${TENANT_ID}/workflows/${INSTANCE_ID}/runs" \
  "data.get('run_count', 0) >= 1 and (data.get('runs') or [{}])[0].get('run_id') is not None" \
  "$OUTPUT_DIR/workflow-runs.json" \
  120

RUN_ID="$(python3 - "$OUTPUT_DIR/workflow-runs.json" <<'PY'
import json
import sys
payload = json.load(open(sys.argv[1], "r", encoding="utf-8"))
runs = payload.get("runs") or []
if not runs:
    raise SystemExit("workflow runs query returned no runs")
print(runs[0]["run_id"])
PY
)"

echo "[streaming-quickstart] waiting for completed bulk batch details"
poll_json_path \
  "/tenants/${TENANT_ID}/workflows/${INSTANCE_ID}/runs/${RUN_ID}/bulk-batches" \
  "data.get('batch_count', 0) >= 1 and (data.get('batches') or [{}])[0].get('status') == 'completed'" \
  "$BATCHES_RESPONSE_PATH" \
  120

echo "[streaming-quickstart] fetching final workflow state"
if ! poll_json_path \
  "/tenants/${TENANT_ID}/workflows/${INSTANCE_ID}" \
  "data.get('status') == 'completed'" \
  "$WORKFLOW_RESPONSE_PATH" \
  30; then
  echo "[streaming-quickstart] workflow completion projection lagged; capturing latest workflow state" >&2
  json_request GET \
    "/tenants/${TENANT_ID}/workflows/${INSTANCE_ID}" \
    "" \
    "$WORKFLOW_RESPONSE_PATH"
fi

python3 - \
  "$ARTIFACT_RESPONSE_PATH" \
  "$PREVIEW_PATH" \
  "$WORKFLOW_RESPONSE_PATH" \
  "$BATCHES_RESPONSE_PATH" \
  "$OUTPUT_DIR/summary.md" \
  "$INSTANCE_ID" \
  "$RUN_ID" \
  "$TENANT_ID" \
  <<'PY'
import json
import sys

artifact_path, preview_path, workflow_path, batches_path, summary_path, instance_id, run_id, tenant_id = sys.argv[1:]
artifact = json.load(open(artifact_path, "r", encoding="utf-8"))
preview = json.load(open(preview_path, "r", encoding="utf-8"))
workflow = json.load(open(workflow_path, "r", encoding="utf-8"))
batches = json.load(open(batches_path, "r", encoding="utf-8"))
batch = (batches.get("batches") or [{}])[0]
lines = [
    "# Streaming Local Quickstart",
    "",
    f"- Tenant: `{tenant_id}`",
    f"- Instance: `{instance_id}`",
    f"- Run: `{run_id}`",
    f"- Artifact hash: `{artifact.get('artifact_hash', '-')}`",
    f"- Workflow status: `{workflow.get('status', '-')}`",
    f"- Batch backend: `{batch.get('selected_backend', '-')}`",
    f"- Reducer: `{batch.get('reducer_kind', '-')}`",
    f"- Reducer output: `{json.dumps(batch.get('reducer_output'))}`",
    f"- Preview ok: `{preview.get('ok', False)}`",
]
with open(summary_path, "w", encoding="utf-8") as handle:
    handle.write("\n".join(lines))
    handle.write("\n")
PY

echo "[streaming-quickstart] complete"
echo "  artifact response: $ARTIFACT_RESPONSE_PATH"
echo "  adapter preview:   $PREVIEW_PATH"
echo "  workflow record:   $WORKFLOW_RESPONSE_PATH"
echo "  bulk batches:      $BATCHES_RESPONSE_PATH"
echo "  summary:           $OUTPUT_DIR/summary.md"
