#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

REPO_PATH="crates/fabrik-cli/test-fixtures/temporal-converter-trust-pressure"
OUTPUT_DIR="target/alpha-drills/temporal-converter-trust-pressure"
TENANT_ID="converter-pressure-$(python3 - <<'PY'
import uuid
print(str(uuid.uuid4())[:8])
PY
)"
DEFAULT_API_PORT=3000
API_URL="http://127.0.0.1:${DEFAULT_API_PORT}"
BUILD_ID="converter-pressure-v1"
SKIP_STACK=0

while (($# > 0)); do
  case "$1" in
    --skip-stack) SKIP_STACK=1; shift ;;
    --api-url) API_URL="$2"; shift 2 ;;
    --tenant) TENANT_ID="$2"; shift 2 ;;
    *) echo "unknown arg: $1" >&2; exit 2 ;;
  esac
done

require_bin() {
  command -v "$1" >/dev/null 2>&1 || { echo "missing required command: $1" >&2; exit 1; }
}

require_bin cargo
require_bin curl
require_bin python3

mkdir -p "$OUTPUT_DIR"
OUTPUT_DIR_ABS="$(cd "$OUTPUT_DIR" && pwd)"
RUN_LOG="$OUTPUT_DIR_ABS/drill.log"
exec > >(tee "$RUN_LOG") 2>&1

timestamp() { date -u +"%Y-%m-%dT%H:%M:%SZ"; }

wait_for_http() {
  local url=$1
  local label=$2
  python3 - "$url" "$label" <<'PY'
import sys, time, urllib.error, urllib.request
url, label = sys.argv[1], sys.argv[2]
deadline = time.time() + 90
while time.time() < deadline:
    try:
        with urllib.request.urlopen(url, timeout=2):
            sys.exit(0)
    except urllib.error.HTTPError as exc:
        if exc.code < 500:
            sys.exit(0)
    except Exception:
        time.sleep(1)
        continue
    time.sleep(1)
print(f"timed out waiting for {label}: {url}", file=sys.stderr)
sys.exit(1)
PY
}

json_get() {
  local path=$1
  local dest=$2
  curl -fsS "${API_URL}${path}" -o "$dest"
}

json_post() {
  local path=$1
  local payload=$2
  local dest=$3
  curl -fsS -X POST "${API_URL}${path}" -H "content-type: application/json" --data-binary "$payload" -o "$dest"
}

relaunch_managed_workers() {
  local workspace_dir=$1
  local runtime_endpoint=${FABRIK_UNIFIED_RUNTIME_ENDPOINT:-http://127.0.0.1:50054}
  python3 - "$workspace_dir" "$TENANT_ID" "$runtime_endpoint" <<'PY'
import json, os, pathlib, subprocess, sys
workspace_dir = pathlib.Path(sys.argv[1])
tenant_id = sys.argv[2]
runtime_endpoint = sys.argv[3]
for package_path in sorted(workspace_dir.glob("workers/*/worker-package.json")):
    package = json.loads(package_path.read_text(encoding="utf-8"))
    pid_path = pathlib.Path(package["pid_path"])
    log_path = pathlib.Path(package["log_path"])
    bootstrap_path = package["bootstrap_path"]
    task_queue = package["task_queue"]
    build_id = package["build_id"]
    env = os.environ.copy()
    env.update({
        "ACTIVITY_WORKER_SERVICE_PORT": "0",
        "UNIFIED_RUNTIME_ENDPOINT": runtime_endpoint,
        "ACTIVITY_TASK_QUEUE": task_queue,
        "ACTIVITY_WORKER_TENANT_ID": tenant_id,
        "ACTIVITY_WORKER_BUILD_ID": build_id,
        "ACTIVITY_ENABLE_BULK_LANES": "false",
        "ACTIVITY_WORKER_CONCURRENCY": "1",
        "ACTIVITY_RESULT_FLUSHER_CONCURRENCY": "1",
        "ACTIVITY_NODE_BOOTSTRAP": bootstrap_path,
        "ACTIVITY_NODE_EXECUTABLE": "node",
    })
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with open(log_path, "ab", buffering=0) as log_handle:
        proc = subprocess.Popen(
            ["/Users/bene/code/fabrik/target/debug/activity-worker-service"],
            stdin=subprocess.DEVNULL,
            stdout=log_handle,
            stderr=subprocess.STDOUT,
            env=env,
            start_new_session=True,
            close_fds=True,
        )
    pid_path.write_text(f"{proc.pid}\n", encoding="utf-8")
PY
}

poll_json_path() {
  local path=$1
  local python_expr=$2
  local dest=$3
  local timeout_seconds=${4:-120}
  python3 - "$API_URL" "$path" "$python_expr" "$dest" "$timeout_seconds" <<'PY'
import json, sys, time, urllib.error, urllib.request
api_url, path, expr, dest, timeout_seconds = sys.argv[1:6]
deadline = time.time() + float(timeout_seconds)
url = f"{api_url}{path}"
while time.time() < deadline:
    try:
        with urllib.request.urlopen(url, timeout=5) as response:
            payload = response.read()
        data = json.loads(payload)
        if eval(expr, {"__builtins__": {}}, {"data": data}):
            with open(dest, "wb") as handle:
                handle.write(payload)
            sys.exit(0)
    except urllib.error.HTTPError:
        pass
    time.sleep(1)
print(f"timed out waiting for {url} matching: {expr}", file=sys.stderr)
sys.exit(1)
PY
}

extract_report_fields() {
  local report_path=$1
  python3 - "$report_path" <<'PY'
import json, sys
report = json.load(open(sys.argv[1], "r", encoding="utf-8"))
workflow = report["compiled_workflows"][0]
worker = report["deployment"]["workers"][0] if report.get("deployment", {}).get("workers") else report["worker_packages"][0]
print(json.dumps({
    "definition_id": workflow["definition_id"],
    "definition_version": workflow["definition_version"],
    "artifact_hash": workflow["artifact_hash"],
    "workflow_task_queue": worker.get("task_queue"),
}, indent=2))
PY
}

extract_field() {
  local path=$1
  local field=$2
  python3 - "$path" "$field" <<'PY'
import json, sys
print(json.load(open(sys.argv[1], "r", encoding="utf-8"))[sys.argv[2]])
PY
}

trigger_workflow() {
  local definition_id=$1
  local instance_id=$2
  local queue=$3
  local out_prefix=$4
  local response_path="$OUTPUT_DIR_ABS/${out_prefix}-trigger-response.json"
  local payload
  payload="$(python3 - "$TENANT_ID" "$instance_id" "$queue" "$out_prefix" <<'PY'
import json, sys
tenant_id, instance_id, queue, out_prefix = sys.argv[1:5]
print(json.dumps({
    "tenant_id": tenant_id,
    "instance_id": instance_id,
    "workflow_task_queue": queue,
    "input": [{"id": "order-7", "tags": ["vip", "alpha"]}],
    "memo": {"converter": "path-factory"},
    "searchAttributes": {"PressureGroup": "converter"},
    "request_id": f"{out_prefix}-trigger",
}))
PY
)"
  local attempt
  for attempt in 1 2 3 4 5; do
    local status
    status="$(
      curl -sS -o "$response_path" -w '%{http_code}' \
        -X POST "${API_URL}/workflows/${definition_id}/trigger" \
        -H "content-type: application/json" \
        --data-binary "$payload"
    )"
    if [[ "$status" == "200" || "$status" == "202" ]]; then
      return 0
    fi
    if [[ "$attempt" -lt 5 ]]; then
      echo "[converter-pressure-drill] trigger attempt $attempt failed with status $status; retrying" >&2
      sleep 2
      continue
    fi
    echo "[converter-pressure-drill] trigger failed with status $status" >&2
    cat "$response_path" >&2
    return 1
  done
}

query_workflow() {
  local instance_id=$1
  local out_prefix=$2
  json_post \
    "/tenants/${TENANT_ID}/workflows/${instance_id}/queries/status" \
    '{"args":[]}' \
    "$OUTPUT_DIR_ABS/${out_prefix}.json"
}

write_meta() {
  local definition_id=$1
  local definition_version=$2
  local artifact_hash=$3
  local queue=$4
  local instance_id=$5
  local run_id=$6
  python3 - "$OUTPUT_DIR_ABS/drill-meta.json" "$REPO_PATH" "$TENANT_ID" "$definition_id" "$definition_version" "$artifact_hash" "$queue" "$instance_id" "$run_id" <<'PY'
import json, sys
output_path, repo_path, tenant_id, definition_id, definition_version, artifact_hash, queue, instance_id, run_id = sys.argv[1:]
payload = {
    "repo_path": repo_path,
    "tenant_id": tenant_id,
    "definition_id": definition_id,
    "definition_version": int(definition_version),
    "artifact_hash": artifact_hash,
    "workflow_task_queue": queue,
    "instance_id": instance_id,
    "run_id": run_id,
}
with open(output_path, "w", encoding="utf-8") as handle:
    json.dump(payload, handle, indent=2)
PY
}

build_report() {
  python3 - "$OUTPUT_DIR_ABS" <<'PY'
import json, pathlib, sys
out_dir = pathlib.Path(sys.argv[1])
workflow_post_restart = json.loads((out_dir / "workflow-post-restart.json").read_text(encoding="utf-8"))
workflow_post_complete = json.loads((out_dir / "workflow-post-complete.json").read_text(encoding="utf-8"))
replay_post_restart = json.loads((out_dir / "replay-post-restart.json").read_text(encoding="utf-8"))
replay_post_complete = json.loads((out_dir / "replay-post-complete.json").read_text(encoding="utf-8"))
query_pre_signal = json.loads((out_dir / "query-pre-signal.json").read_text(encoding="utf-8"))
query_post_restart = json.loads((out_dir / "query-post-restart.json").read_text(encoding="utf-8"))
query_post_complete = json.loads((out_dir / "query-post-complete.json").read_text(encoding="utf-8"))
meta = json.loads((out_dir / "drill-meta.json").read_text(encoding="utf-8"))
activity_queue = json.loads((out_dir / "task-queue-activity-post-restart.json").read_text(encoding="utf-8"))

def qvalue(payload):
    return payload.get("result", payload)

pre_value = qvalue(query_pre_signal)
restart_value = qvalue(query_post_restart)
complete_value = qvalue(query_post_complete)
expected_waiting = {"phase": "waiting", "id": "order-7", "tags": ["vip", "alpha"]}
checks = {
    "deploy_succeeded": True,
    "workflow_running_before_normalize": True,
    "query_before_normalize_shows_waiting_state": pre_value == expected_waiting,
    "workflow_preserved_artifact_after_restart": workflow_post_restart.get("artifact_hash") == meta["artifact_hash"],
    "query_after_restart_preserves_waiting_state": restart_value == expected_waiting,
    "activity_pollers_present_after_restart": len(activity_queue.get("pollers", [])) > 0,
    "replay_clean_after_restart": replay_post_restart.get("projection_matches_store") is True and replay_post_restart.get("divergence_count") == 0,
    "workflow_completed_with_normalized_output": workflow_post_complete.get("status") == "completed" and workflow_post_complete.get("output") == "order-7:VIP,ALPHA",
    "query_after_complete_shows_normalized_state": complete_value == {"phase": "completed", "id": "order-7", "tags": ["VIP", "ALPHA"]},
    "replay_clean_after_complete": replay_post_complete.get("projection_matches_store") is True and replay_post_complete.get("divergence_count") == 0,
}
status = "passed" if all(checks.values()) else "failed"
report = {
    "schema_version": 1,
    "status": status,
    "tenant_id": meta["tenant_id"],
    "workflow_task_queue": meta["workflow_task_queue"],
    "artifact_hash": meta["artifact_hash"],
    "definition_id": meta["definition_id"],
    "instance_id": meta["instance_id"],
    "run_id": meta["run_id"],
    "checks": checks,
    "evidence": {
        "query_values": {
            "pre_normalize": pre_value,
            "post_restart": restart_value,
            "post_complete": complete_value,
        },
        "replay_divergence_counts": {
            "post_restart": replay_post_restart.get("divergence_count"),
            "post_complete": replay_post_complete.get("divergence_count"),
        },
        "output": workflow_post_complete.get("output"),
        "activity_poller_count_post_restart": len(activity_queue.get("pollers", [])),
    },
}
(out_dir / "converter-pressure-drill-report.json").write_text(json.dumps(report, indent=2), encoding="utf-8")
lines = [
    "# Converter Pressure Drill Report",
    "",
    f"- Status: `{status}`",
    f"- Tenant: `{meta['tenant_id']}`",
    f"- Queue: `{meta['workflow_task_queue']}`",
    f"- Artifact: `{meta['artifact_hash']}`",
    "",
    "## Checks",
]
for key, value in checks.items():
    lines.append(f"- `{key}`: `{'passed' if value else 'failed'}`")
lines.extend([
    "",
    "## Evidence",
    f"- Query values: pre-normalize=`{pre_value}` post-restart=`{restart_value}` post-complete=`{complete_value}`",
    f"- Replay divergence counts: post-restart=`{replay_post_restart.get('divergence_count')}` post-complete=`{replay_post_complete.get('divergence_count')}`",
    f"- Output: `{workflow_post_complete.get('output')}`",
    f"- Activity pollers after restart: `{len(activity_queue.get('pollers', []))}`",
])
(out_dir / "converter-pressure-drill-report.md").write_text("\n".join(lines) + "\n", encoding="utf-8")
print(json.dumps(report, indent=2))
PY
}

echo "[converter-pressure-drill] started $(timestamp)"
echo "[converter-pressure-drill] repo=$REPO_PATH tenant=$TENANT_ID api=$API_URL output=$OUTPUT_DIR_ABS"

if [[ "$SKIP_STACK" != "1" ]]; then
  if [[ -x ./scripts/dev-stack.sh ]]; then
    echo "[converter-pressure-drill] restarting dev stack"
    ./scripts/dev-stack.sh down
    DEV_STACK_BUILD="${DEV_STACK_BUILD:-0}" ./scripts/dev-stack.sh up
  else
    echo "[converter-pressure-drill] dev-stack.sh missing" >&2
    exit 1
  fi
fi

wait_for_http "$API_URL/health" "api gateway"

echo "[converter-pressure-drill] running migration and deployment"
mkdir -p "$OUTPUT_DIR_ABS"
FABRIK_WORKFLOW_BUILD_ID="$BUILD_ID" cargo run -p fabrik-cli --bin fabrik -- migrate temporal "$REPO_PATH" --deploy --output-dir "$OUTPUT_DIR_ABS" --api-url "$API_URL" --tenant "$TENANT_ID"
extract_report_fields "$OUTPUT_DIR_ABS/migration-report.json" > "$OUTPUT_DIR_ABS/report-fields.json"

DEFINITION_ID="$(extract_field "$OUTPUT_DIR_ABS/report-fields.json" definition_id)"
DEFINITION_VERSION="$(extract_field "$OUTPUT_DIR_ABS/report-fields.json" definition_version)"
ARTIFACT_HASH="$(extract_field "$OUTPUT_DIR_ABS/report-fields.json" artifact_hash)"
WORKFLOW_TASK_QUEUE="$(extract_field "$OUTPUT_DIR_ABS/report-fields.json" workflow_task_queue)"

INSTANCE_ID="converter-pressure-$(python3 - <<'PY'
import uuid
print(str(uuid.uuid4())[:8])
PY
)"

echo "[converter-pressure-drill] triggering workflow definition=$DEFINITION_ID instance=$INSTANCE_ID queue=$WORKFLOW_TASK_QUEUE"
trigger_workflow "$DEFINITION_ID" "$INSTANCE_ID" "$WORKFLOW_TASK_QUEUE" "workflow"
RUN_ID="$(python3 - "$OUTPUT_DIR_ABS/workflow-trigger-response.json" <<'PY'
import json, sys
print(json.load(open(sys.argv[1], "r", encoding="utf-8"))["run_id"])
PY
)"

poll_json_path "/tenants/${TENANT_ID}/workflows/${INSTANCE_ID}" "data.get('run_id') == '${RUN_ID}' and data.get('status') == 'running'" "$OUTPUT_DIR_ABS/workflow-pre-signal.json"
query_workflow "$INSTANCE_ID" "query-pre-signal"

echo "[converter-pressure-drill] restarting dev stack"
./scripts/dev-stack.sh down
DEV_STACK_BUILD="${DEV_STACK_BUILD:-0}" ./scripts/dev-stack.sh up
wait_for_http "$API_URL/health" "api gateway after restart"
echo "[converter-pressure-drill] relaunching managed migrated workers"
relaunch_managed_workers "$OUTPUT_DIR_ABS"
sleep 3

poll_json_path "/tenants/${TENANT_ID}/workflows/${INSTANCE_ID}" "data.get('run_id') == '${RUN_ID}' and data.get('status') == 'running'" "$OUTPUT_DIR_ABS/workflow-post-restart.json"
json_get "/tenants/${TENANT_ID}/workflows/${INSTANCE_ID}/runs/${RUN_ID}/replay" "$OUTPUT_DIR_ABS/replay-post-restart.json"
query_workflow "$INSTANCE_ID" "query-post-restart"
json_get "/admin/tenants/${TENANT_ID}/task-queues/activity/${WORKFLOW_TASK_QUEUE}" "$OUTPUT_DIR_ABS/task-queue-activity-post-restart.json"

echo "[converter-pressure-drill] signaling normalize"
json_post "/tenants/${TENANT_ID}/workflows/${INSTANCE_ID}/signals/normalize" '{"payload":true,"request_id":"signal-normalize"}' "$OUTPUT_DIR_ABS/signal-response.json"

poll_json_path "/tenants/${TENANT_ID}/workflows/${INSTANCE_ID}" "data.get('run_id') == '${RUN_ID}' and data.get('status') == 'completed'" "$OUTPUT_DIR_ABS/workflow-post-complete.json"
json_get "/tenants/${TENANT_ID}/workflows/${INSTANCE_ID}/runs/${RUN_ID}/replay" "$OUTPUT_DIR_ABS/replay-post-complete.json"
query_workflow "$INSTANCE_ID" "query-post-complete"

write_meta "$DEFINITION_ID" "$DEFINITION_VERSION" "$ARTIFACT_HASH" "$WORKFLOW_TASK_QUEUE" "$INSTANCE_ID" "$RUN_ID"
build_report > /dev/null

echo "[converter-pressure-drill] completed $(timestamp)"
echo "[converter-pressure-drill] report: $OUTPUT_DIR_ABS/converter-pressure-drill-report.md"
