#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

usage() {
  cat >&2 <<'EOF'
usage: scripts/run-async-external-pressure-drill.sh [options]

options:
  --repo <path>
  --output-dir <path>
  --tenant <tenant_id>
  --api-url <url>
  --skip-stack
  --skip-restart
EOF
  exit 1
}

REPO_PATH="crates/fabrik-cli/test-fixtures/temporal-async-external-pressure"
OUTPUT_DIR="target/alpha-drills/temporal-async-external-pressure"
TENANT_ID="async-external-pressure-$(python3 - <<'PY'
import uuid
print(str(uuid.uuid4())[:8])
PY
)"
DEFAULT_API_PORT="${API_GATEWAY_PORT:-3000}"
API_URL="http://127.0.0.1:${DEFAULT_API_PORT}"
SKIP_STACK=0
SKIP_RESTART=0

while (($#)); do
  case "$1" in
    --repo) REPO_PATH="$2"; shift 2 ;;
    --output-dir) OUTPUT_DIR="$2"; shift 2 ;;
    --tenant) TENANT_ID="$2"; shift 2 ;;
    --api-url) API_URL="$2"; shift 2 ;;
    --skip-stack) SKIP_STACK=1; shift ;;
    --skip-restart) SKIP_RESTART=1; shift ;;
    *) echo "unknown argument: $1" >&2; usage ;;
  esac
done

require_bin() {
  command -v "$1" >/dev/null 2>&1 || { echo "$1 is required" >&2; exit 1; }
}

require_bin cargo
require_bin curl
require_bin python3

mkdir -p "$OUTPUT_DIR"
OUTPUT_DIR_ABS="$(cd "$OUTPUT_DIR" && pwd)"
RUN_LOG="$OUTPUT_DIR_ABS/drill.log"
exec > >(tee "$RUN_LOG") 2>&1

timestamp() { date -u +"%Y-%m-%dT%H:%M:%SZ"; }

echo "[async-external-pressure-drill] started $(timestamp)"
echo "[async-external-pressure-drill] repo=$REPO_PATH tenant=$TENANT_ID api=$API_URL output=$OUTPUT_DIR_ABS"

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
  curl -fsS "${API_URL}$1" -o "$2"
}

json_post() {
  curl -fsS -X POST "${API_URL}$1" -H "content-type: application/json" --data-binary "$2" -o "$3"
}

relaunch_managed_workers() {
  local workspace_dir=$1
  local runtime_endpoint=${FABRIK_UNIFIED_RUNTIME_ENDPOINT:-http://127.0.0.1:50054}
  python3 - "$workspace_dir" "$TENANT_ID" "$runtime_endpoint" <<'PY'
import json
import os
import pathlib
import subprocess
import sys

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
  local expr=$2
  local dest=$3
  local timeout_seconds=${4:-120}
  python3 - "$API_URL" "$path" "$expr" "$dest" "$timeout_seconds" <<'PY'
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
    except Exception:
        pass
    time.sleep(1)
print(f"timed out waiting for condition on {url}: {expr}", file=sys.stderr)
sys.exit(1)
PY
}

poll_query_result() {
  local instance_id=$1
  local expr=$2
  local dest=$3
  local timeout_seconds=${4:-120}
  python3 - "$API_URL" "$TENANT_ID" "$instance_id" "$expr" "$dest" "$timeout_seconds" <<'PY'
import json, sys, time, urllib.error, urllib.request
api_url, tenant_id, instance_id, expr, dest, timeout_seconds = sys.argv[1:7]
deadline = time.time() + float(timeout_seconds)
url = f"{api_url}/tenants/{tenant_id}/workflows/{instance_id}/queries/status"
payload = json.dumps({"args": []}).encode("utf-8")
request = urllib.request.Request(url, data=payload, headers={"content-type": "application/json"}, method="POST")
while time.time() < deadline:
    try:
        with urllib.request.urlopen(request, timeout=5) as response:
            body = response.read()
        raw = json.loads(body)
        data = raw.get("result", raw)
        if eval(expr, {"__builtins__": {}}, {"data": data, "raw": raw}):
            with open(dest, "wb") as handle:
                handle.write(body)
            sys.exit(0)
    except urllib.error.HTTPError:
        pass
    except Exception:
        pass
    time.sleep(1)
print(f"timed out waiting for query condition on {url}: {expr}", file=sys.stderr)
sys.exit(1)
PY
}

extract_report_fields() {
  local report_path=$1
  python3 - "$report_path" <<'PY'
import json, sys

report = json.load(open(sys.argv[1], "r", encoding="utf-8"))
payload = {"target": {}, "controller": {}}

for workflow in report["compiled_workflows"]:
    definition_id = workflow["definition_id"]
    if "asyncsignaltargetworkflow" in definition_id:
        payload["target"].update({
            "definition_id": definition_id,
            "definition_version": workflow["definition_version"],
            "artifact_hash": workflow["artifact_hash"],
        })
    elif "externalcontrollerworkflow" in definition_id:
        payload["controller"].update({
            "definition_id": definition_id,
            "definition_version": workflow["definition_version"],
            "artifact_hash": workflow["artifact_hash"],
        })

for worker in report.get("deployment", {}).get("workers", []):
    payload["target"]["workflow_task_queue"] = worker.get("task_queue")
    payload["controller"]["workflow_task_queue"] = worker.get("task_queue")
    payload["target"]["build_id"] = worker.get("build_id")
    payload["controller"]["build_id"] = worker.get("build_id")

print(json.dumps(payload, indent=2))
PY
}

extract_nested_field() {
  python3 - "$1" "$2" "$3" <<'PY'
import json, sys
payload = json.load(open(sys.argv[1], "r", encoding="utf-8"))
value = payload[sys.argv[2]][sys.argv[3]]
if value is None:
    value = ""
print(value)
PY
}

trigger_workflow() {
  local definition_id=$1
  local instance_id=$2
  local queue=$3
  local input_json=$4
  local lane=$5
  local out_prefix=$6
  local payload
  payload="$(python3 - "$TENANT_ID" "$instance_id" "$queue" "$input_json" "$lane" "$out_prefix" <<'PY'
import json, sys
tenant_id, instance_id, queue, input_json, lane, out_prefix = sys.argv[1:7]
print(json.dumps({
    "tenant_id": tenant_id,
    "instance_id": instance_id,
    "workflow_task_queue": queue,
    "input": [json.loads(input_json)],
    "memo": {"lane": lane},
    "searchAttributes": {"PressureGroup": lane},
    "request_id": f"{out_prefix}-trigger",
}))
PY
)"
  local response_path="$OUTPUT_DIR_ABS/${out_prefix}-trigger-response.json"
  curl -fsS -X POST "${API_URL}/workflows/${definition_id}/trigger" -H "content-type: application/json" --data-binary "$payload" -o "$response_path"
}

query_status() {
  local instance_id=$1
  local out_prefix=$2
  json_post \
    "/tenants/${TENANT_ID}/workflows/${instance_id}/queries/status" \
    '{"args":[]}' \
    "$OUTPUT_DIR_ABS/${out_prefix}.json"
}

write_meta() {
  local target_definition_id=$1
  local controller_definition_id=$2
  local queue=$3
  local target_instance_id=$4
  local target_run_id=$5
  local controller_instance_id=$6
  local controller_run_id=$7
  python3 - "$OUTPUT_DIR_ABS/drill-meta.json" "$REPO_PATH" "$TENANT_ID" "$target_definition_id" "$controller_definition_id" "$queue" "$target_instance_id" "$target_run_id" "$controller_instance_id" "$controller_run_id" <<'PY'
import json, sys
output_path, repo_path, tenant_id, target_definition_id, controller_definition_id, queue, target_instance_id, target_run_id, controller_instance_id, controller_run_id = sys.argv[1:]
payload = {
    "repo_path": repo_path,
    "tenant_id": tenant_id,
    "target_definition_id": target_definition_id,
    "controller_definition_id": controller_definition_id,
    "workflow_task_queue": queue,
    "target_instance_id": target_instance_id,
    "target_run_id": target_run_id,
    "controller_instance_id": controller_instance_id,
    "controller_run_id": controller_run_id,
}
with open(output_path, "w", encoding="utf-8") as handle:
    json.dump(payload, handle, indent=2)
PY
}

build_report() {
  python3 - "$OUTPUT_DIR_ABS" <<'PY'
import json, pathlib, sys
out_dir = pathlib.Path(sys.argv[1])
target_pre_raw = json.loads((out_dir / "target-query-pre-restart.json").read_text(encoding="utf-8"))
target_post_raw = json.loads((out_dir / "target-query-post-restart.json").read_text(encoding="utf-8"))
controller_pre_raw = json.loads((out_dir / "controller-query-pre-restart.json").read_text(encoding="utf-8"))
controller_post_raw = json.loads((out_dir / "controller-query-post-restart.json").read_text(encoding="utf-8"))
target_pre = target_pre_raw.get("result", target_pre_raw)
target_post = target_post_raw.get("result", target_post_raw)
controller_pre = controller_pre_raw.get("result", controller_pre_raw)
controller_post = controller_post_raw.get("result", controller_post_raw)
target_status_final = json.loads((out_dir / "target-workflow-post-cancel.json").read_text(encoding="utf-8"))
controller_status_final = json.loads((out_dir / "controller-workflow-post-complete.json").read_text(encoding="utf-8"))
target_replay_restart = json.loads((out_dir / "target-replay-post-restart.json").read_text(encoding="utf-8"))
controller_replay_restart = json.loads((out_dir / "controller-replay-post-restart.json").read_text(encoding="utf-8"))
target_replay_complete = json.loads((out_dir / "target-replay-post-cancel.json").read_text(encoding="utf-8"))
controller_replay_complete = json.loads((out_dir / "controller-replay-post-complete.json").read_text(encoding="utf-8"))
meta = json.loads((out_dir / "drill-meta.json").read_text(encoding="utf-8"))

checks = {
    "target_async_handler_processed_before_restart": target_pre.get("phase") == "processed" and target_pre.get("echoed") == "echo:payload-7",
    "controller_waiting_before_restart": controller_pre.get("phase") == "signalled" and controller_pre.get("released") is False,
    "target_state_preserved_after_restart": target_post == target_pre,
    "controller_state_preserved_after_restart": controller_post == controller_pre,
    "target_cancelled_after_release": target_status_final.get("status") == "cancelled",
    "controller_completed_after_release": controller_status_final.get("status") == "completed",
    "replay_clean_post_restart": target_replay_restart.get("divergence_count") == 0 and controller_replay_restart.get("divergence_count") == 0,
    "replay_clean_post_complete": target_replay_complete.get("divergence_count") == 0 and controller_replay_complete.get("divergence_count") == 0,
}

overall = all(checks.values())
report = {
    "status": "passed" if overall else "failed",
    "tenant_id": meta["tenant_id"],
    "workflow_task_queue": meta["workflow_task_queue"],
    "target_definition_id": meta["target_definition_id"],
    "controller_definition_id": meta["controller_definition_id"],
    "checks": checks,
    "evidence": {
        "target_query_values": {
            "pre_restart": target_pre,
            "post_restart": target_post,
        },
        "controller_query_values": {
            "pre_restart": controller_pre,
            "post_restart": controller_post,
        },
        "terminal_statuses": {
            "target": target_status_final.get("status"),
            "controller": controller_status_final.get("status"),
        },
        "replay_divergence_counts": {
            "target_post_restart": target_replay_restart.get("divergence_count"),
            "controller_post_restart": controller_replay_restart.get("divergence_count"),
            "target_post_complete": target_replay_complete.get("divergence_count"),
            "controller_post_complete": controller_replay_complete.get("divergence_count"),
        },
    },
}
markdown = "\n".join([
    "# Async External Pressure Drill Report",
    "",
    f"- Status: `{report['status']}`",
    f"- Tenant: `{report['tenant_id']}`",
    f"- Queue: `{report['workflow_task_queue']}`",
    "",
    "## Checks",
    *[f"- `{name}`: `{'passed' if ok else 'failed'}`" for name, ok in checks.items()],
    "",
    "## Evidence",
    f"- Target query pre/post restart: `{target_pre}` / `{target_post}`",
    f"- Controller query pre/post restart: `{controller_pre}` / `{controller_post}`",
    f"- Terminal statuses: target=`{target_status_final.get('status')}` controller=`{controller_status_final.get('status')}`",
    f"- Replay divergence counts: target restart=`{target_replay_restart.get('divergence_count')}` controller restart=`{controller_replay_restart.get('divergence_count')}` target complete=`{target_replay_complete.get('divergence_count')}` controller complete=`{controller_replay_complete.get('divergence_count')}`",
])
(out_dir / "async-external-pressure-drill-report.json").write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
(out_dir / "async-external-pressure-drill-report.md").write_text(markdown + "\n", encoding="utf-8")
sys.exit(0 if overall else 1)
PY
}

if [[ "$SKIP_STACK" != "1" ]]; then
  if ! curl -fsS "$API_URL/health" >/dev/null 2>&1; then
    echo "[async-external-pressure-drill] api gateway unavailable, starting dev stack"
    DEV_STACK_BUILD="${DEV_STACK_BUILD:-0}" ./scripts/dev-stack.sh up
  else
    echo "[async-external-pressure-drill] dev stack already reachable"
  fi
fi

wait_for_http "$API_URL/health" "api gateway"

echo "[async-external-pressure-drill] running migration and deployment"
mkdir -p "$OUTPUT_DIR_ABS"
cargo run -p fabrik-cli --bin fabrik -- migrate temporal "$REPO_PATH" --deploy --output-dir "$OUTPUT_DIR_ABS" --api-url "$API_URL" --tenant "$TENANT_ID"
extract_report_fields "$OUTPUT_DIR_ABS/migration-report.json" > "$OUTPUT_DIR_ABS/report-fields.json"

TARGET_DEFINITION_ID="$(extract_nested_field "$OUTPUT_DIR_ABS/report-fields.json" target definition_id)"
CONTROLLER_DEFINITION_ID="$(extract_nested_field "$OUTPUT_DIR_ABS/report-fields.json" controller definition_id)"
WORKFLOW_TASK_QUEUE="$(extract_nested_field "$OUTPUT_DIR_ABS/report-fields.json" target workflow_task_queue)"

TARGET_INSTANCE_ID="async-target-$(python3 - <<'PY'
import uuid
print(str(uuid.uuid4())[:8])
PY
)"
CONTROLLER_INSTANCE_ID="async-controller-$(python3 - <<'PY'
import uuid
print(str(uuid.uuid4())[:8])
PY
)"

echo "[async-external-pressure-drill] triggering target workflow definition=$TARGET_DEFINITION_ID instance=$TARGET_INSTANCE_ID"
trigger_workflow "$TARGET_DEFINITION_ID" "$TARGET_INSTANCE_ID" "$WORKFLOW_TASK_QUEUE" '"seed"' "async-external-target" "target"
TARGET_RUN_ID="$(python3 - "$OUTPUT_DIR_ABS/target-trigger-response.json" <<'PY'
import json, sys
print(json.load(open(sys.argv[1], "r", encoding="utf-8"))["run_id"])
PY
)"
poll_json_path "/tenants/${TENANT_ID}/workflows/${TARGET_INSTANCE_ID}" "data.get('run_id') == '${TARGET_RUN_ID}' and data.get('status') == 'running'" "$OUTPUT_DIR_ABS/target-workflow-running.json"

CONTROLLER_INPUT="$(python3 - "$TARGET_INSTANCE_ID" "$TARGET_RUN_ID" <<'PY'
import json, sys
print(json.dumps({
    "targetInstanceId": sys.argv[1],
    "targetRunId": sys.argv[2],
    "payload": "payload-7",
}))
PY
)"

echo "[async-external-pressure-drill] triggering controller workflow definition=$CONTROLLER_DEFINITION_ID instance=$CONTROLLER_INSTANCE_ID"
trigger_workflow "$CONTROLLER_DEFINITION_ID" "$CONTROLLER_INSTANCE_ID" "$WORKFLOW_TASK_QUEUE" "$CONTROLLER_INPUT" "async-external-controller" "controller"
CONTROLLER_RUN_ID="$(python3 - "$OUTPUT_DIR_ABS/controller-trigger-response.json" <<'PY'
import json, sys
print(json.load(open(sys.argv[1], "r", encoding="utf-8"))["run_id"])
PY
)"
poll_json_path "/tenants/${TENANT_ID}/workflows/${CONTROLLER_INSTANCE_ID}" "data.get('run_id') == '${CONTROLLER_RUN_ID}' and data.get('status') == 'running'" "$OUTPUT_DIR_ABS/controller-workflow-running.json"

poll_query_result "$TARGET_INSTANCE_ID" "data.get('phase') == 'processed' and data.get('echoed') == 'echo:payload-7'" "$OUTPUT_DIR_ABS/target-query-pre-restart.json"
poll_query_result "$CONTROLLER_INSTANCE_ID" "data.get('phase') == 'signalled' and data.get('released') is False" "$OUTPUT_DIR_ABS/controller-query-pre-restart.json"

if [[ "$SKIP_RESTART" != "1" ]]; then
  echo "[async-external-pressure-drill] restarting dev stack"
  ./scripts/dev-stack.sh down
  DEV_STACK_BUILD=0 ./scripts/dev-stack.sh up
  echo "[async-external-pressure-drill] relaunching managed migrated workers"
  relaunch_managed_workers "$OUTPUT_DIR_ABS"
fi

wait_for_http "$API_URL/health" "api gateway after restart"

poll_json_path "/tenants/${TENANT_ID}/workflows/${TARGET_INSTANCE_ID}" "data.get('run_id') == '${TARGET_RUN_ID}' and data.get('status') == 'running'" "$OUTPUT_DIR_ABS/target-workflow-post-restart.json"
poll_json_path "/tenants/${TENANT_ID}/workflows/${CONTROLLER_INSTANCE_ID}" "data.get('run_id') == '${CONTROLLER_RUN_ID}' and data.get('status') == 'running'" "$OUTPUT_DIR_ABS/controller-workflow-post-restart.json"
json_get "/tenants/${TENANT_ID}/workflows/${TARGET_INSTANCE_ID}/runs/${TARGET_RUN_ID}/replay" "$OUTPUT_DIR_ABS/target-replay-post-restart.json"
json_get "/tenants/${TENANT_ID}/workflows/${CONTROLLER_INSTANCE_ID}/runs/${CONTROLLER_RUN_ID}/replay" "$OUTPUT_DIR_ABS/controller-replay-post-restart.json"
poll_query_result "$TARGET_INSTANCE_ID" "data.get('phase') == 'processed' and data.get('echoed') == 'echo:payload-7'" "$OUTPUT_DIR_ABS/target-query-post-restart.json"
poll_query_result "$CONTROLLER_INSTANCE_ID" "data.get('phase') == 'signalled' and data.get('released') is False" "$OUTPUT_DIR_ABS/controller-query-post-restart.json"

echo "[async-external-pressure-drill] signaling controller release"
json_post \
  "/tenants/${TENANT_ID}/workflows/${CONTROLLER_INSTANCE_ID}/signals/release" \
  '{"payload":true,"request_id":"signal-release-controller"}' \
  "$OUTPUT_DIR_ABS/controller-release-response.json"

poll_json_path "/tenants/${TENANT_ID}/workflows/${CONTROLLER_INSTANCE_ID}" "data.get('run_id') == '${CONTROLLER_RUN_ID}' and data.get('status') == 'completed'" "$OUTPUT_DIR_ABS/controller-workflow-post-complete.json"
poll_json_path "/tenants/${TENANT_ID}/workflows/${TARGET_INSTANCE_ID}" "data.get('run_id') == '${TARGET_RUN_ID}' and data.get('status') == 'cancelled'" "$OUTPUT_DIR_ABS/target-workflow-post-cancel.json"
json_get "/tenants/${TENANT_ID}/workflows/${CONTROLLER_INSTANCE_ID}/runs/${CONTROLLER_RUN_ID}/replay" "$OUTPUT_DIR_ABS/controller-replay-post-complete.json"
json_get "/tenants/${TENANT_ID}/workflows/${TARGET_INSTANCE_ID}/runs/${TARGET_RUN_ID}/replay" "$OUTPUT_DIR_ABS/target-replay-post-cancel.json"

write_meta \
  "$TARGET_DEFINITION_ID" \
  "$CONTROLLER_DEFINITION_ID" \
  "$WORKFLOW_TASK_QUEUE" \
  "$TARGET_INSTANCE_ID" \
  "$TARGET_RUN_ID" \
  "$CONTROLLER_INSTANCE_ID" \
  "$CONTROLLER_RUN_ID"
build_report >/dev/null

echo "[async-external-pressure-drill] completed $(timestamp)"
echo "[async-external-pressure-drill] report: $OUTPUT_DIR_ABS/async-external-pressure-drill-report.md"
