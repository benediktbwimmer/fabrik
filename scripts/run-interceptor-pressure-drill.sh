#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

usage() {
  cat >&2 <<'EOF'
usage: scripts/run-interceptor-pressure-drill.sh [options]

options:
  --repo <path>
  --output-dir <path>
  --tenant <tenant_id>
  --api-url <url>
  --build-id <build_id>
  --skip-stack
  --skip-restart
EOF
  exit 1
}

REPO_PATH="crates/fabrik-cli/test-fixtures/temporal-interceptor-pressure"
OUTPUT_DIR="target/alpha-drills/temporal-interceptor-pressure"
TENANT_ID="interceptor-pressure-$(python3 - <<'PY'
import uuid
print(str(uuid.uuid4())[:8])
PY
)"
DEFAULT_API_PORT="${API_GATEWAY_PORT:-3000}"
API_URL="http://127.0.0.1:${DEFAULT_API_PORT}"
BUILD_ID="interceptor-pressure-v1"
SKIP_STACK=0
SKIP_RESTART=0

while (($#)); do
  case "$1" in
    --repo) REPO_PATH="$2"; shift 2 ;;
    --output-dir) OUTPUT_DIR="$2"; shift 2 ;;
    --tenant) TENANT_ID="$2"; shift 2 ;;
    --api-url) API_URL="$2"; shift 2 ;;
    --build-id) BUILD_ID="$2"; shift 2 ;;
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

echo "[interceptor-pressure-drill] started $(timestamp)"
echo "[interceptor-pressure-drill] repo=$REPO_PATH tenant=$TENANT_ID api=$API_URL output=$OUTPUT_DIR_ABS"

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
  local matching_endpoint=${FABRIK_UNIFIED_RUNTIME_ENDPOINT:-http://127.0.0.1:50054}
  python3 - "$workspace_dir" "$TENANT_ID" "$matching_endpoint" <<'PY'
import json
import os
import pathlib
import subprocess
import sys

workspace_dir = pathlib.Path(sys.argv[1])
tenant_id = sys.argv[2]
matching_endpoint = sys.argv[3]

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
        "MATCHING_SERVICE_ENDPOINT": matching_endpoint,
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

extract_report_fields() {
  local report_path=$1
  python3 - "$report_path" <<'PY'
import json, sys
report = json.load(open(sys.argv[1], "r", encoding="utf-8"))
workflow = report["compiled_workflows"][0]
deployment = report["deployment"]
worker = deployment["workers"][0] if deployment["workers"] else {}
artifact = deployment["published_artifacts"][0] if deployment["published_artifacts"] else {}
print(json.dumps({
    "status": report["status"],
    "qualification": report["alpha_qualification"]["verdict"],
    "definition_id": workflow["definition_id"],
    "definition_version": workflow["definition_version"],
    "artifact_hash": artifact.get("artifact_hash") or workflow.get("artifact_hash"),
    "workflow_task_queue": worker.get("task_queue") or workflow.get("workflow_task_queue") or "default",
}, indent=2))
PY
}

extract_field() {
  python3 - "$1" "$2" <<'PY'
import json, sys
payload = json.load(open(sys.argv[1], "r", encoding="utf-8"))
value = payload.get(sys.argv[2], "")
if value is None:
    value = ""
print(value)
PY
}

trigger_workflow() {
  local definition_id=$1
  local instance_id=$2
  local queue=$3
  local initial_value=$4
  local out_prefix=$5
  local payload
  payload="$(python3 - "$TENANT_ID" "$instance_id" "$queue" "$initial_value" "$out_prefix" <<'PY'
import json, sys
tenant_id, instance_id, queue, initial_value, out_prefix = sys.argv[1:6]
print(json.dumps({
    "tenant_id": tenant_id,
    "instance_id": instance_id,
    "workflow_task_queue": queue,
    "input": [initial_value],
    "memo": {"lane": "interceptor"},
    "searchAttributes": {"PressureGroup": "interceptor"},
    "request_id": f"{out_prefix}-trigger",
}))
PY
)"
  json_post "/workflows/${definition_id}/trigger" "$payload" "$OUTPUT_DIR_ABS/${out_prefix}-trigger-response.json"
}

query_workflow() {
  local instance_id=$1
  local out_prefix=$2
  json_post \
    "/tenants/${TENANT_ID}/workflows/${instance_id}/queries/trackedValue" \
    '{"args":[]}' \
    "$OUTPUT_DIR_ABS/${out_prefix}.json"
}

write_meta() {
  python3 - "$OUTPUT_DIR_ABS/meta.json" "$REPO_PATH" "$TENANT_ID" "$@" <<'PY'
import json, sys
output_path, repo_path, tenant_id, definition_id, definition_version, artifact_hash, queue, instance_id, run_id = sys.argv[1:]
payload = {
    "repo": repo_path,
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
    handle.write("\n")
PY
}

summarize_evidence() {
  python3 - "$OUTPUT_DIR_ABS" <<'PY'
import json
import pathlib
import sys

out_dir = pathlib.Path(sys.argv[1])
meta = json.loads((out_dir / "meta.json").read_text(encoding="utf-8"))
report = json.loads((out_dir / "migration-report.json").read_text(encoding="utf-8"))
workflow_running = json.loads((out_dir / "workflow-pre-signal.json").read_text(encoding="utf-8"))
workflow_post_restart = json.loads((out_dir / "workflow-post-restart.json").read_text(encoding="utf-8"))
workflow_post_complete = json.loads((out_dir / "workflow-post-complete.json").read_text(encoding="utf-8"))
query_pre_signal = json.loads((out_dir / "query-pre-signal.json").read_text(encoding="utf-8"))
query_post_restart = json.loads((out_dir / "query-post-restart.json").read_text(encoding="utf-8"))
query_post_complete = json.loads((out_dir / "query-post-complete.json").read_text(encoding="utf-8"))
replay_post_restart = json.loads((out_dir / "replay-post-restart.json").read_text(encoding="utf-8"))
replay_post_complete = json.loads((out_dir / "replay-post-complete.json").read_text(encoding="utf-8"))
activity_queue = json.loads((out_dir / "task-queue-activity-post-restart.json").read_text(encoding="utf-8"))

def qvalue(doc):
    return doc.get("result", {})

pre_value = qvalue(query_pre_signal)
restart_value = qvalue(query_post_restart)
complete_value = qvalue(query_post_complete)

checks = {
    "deploy_succeeded": report["deployment"]["status"] == "deployed",
    "workflow_running_before_signal": workflow_running.get("status") == "running",
    "query_before_signal_shows_initial_state": pre_value == {"version": 0, "value": "draft"},
    "workflow_preserved_artifact_after_restart": workflow_post_restart.get("artifact_hash") == meta["artifact_hash"],
    "query_after_restart_preserves_initial_state": restart_value == {"version": 0, "value": "draft"},
    "activity_pollers_present_after_restart": len(activity_queue.get("pollers", [])) > 0,
    "replay_clean_after_restart": replay_post_restart.get("divergence_count") == 0 and replay_post_restart.get("projection_matches_store") is True,
    "workflow_completed_with_interceptor_output": workflow_post_complete.get("status") == "completed" and workflow_post_complete.get("output") == "approved",
    "query_after_complete_shows_interceptor_commit": complete_value == {"version": 1, "value": "approved"},
    "replay_clean_after_complete": replay_post_complete.get("divergence_count") == 0 and replay_post_complete.get("projection_matches_store") is True,
  }
overall = all(checks.values())
summary = {
    "schema_version": 1,
    "status": "passed" if overall else "failed",
    "tenant_id": meta["tenant_id"],
    "workflow_task_queue": meta["workflow_task_queue"],
    "artifact_hash": meta["artifact_hash"],
    "definition_id": meta["definition_id"],
    "instance_id": meta["instance_id"],
    "run_id": meta["run_id"],
    "checks": checks,
    "evidence": {
        "qualification_verdict": report["alpha_qualification"]["verdict"],
        "query_values": {
            "pre_signal": pre_value,
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
markdown = "\n".join([
    "# Interceptor Pressure Drill Report",
    "",
    f"- Status: `{summary['status']}`",
    f"- Tenant: `{summary['tenant_id']}`",
    f"- Queue: `{summary['workflow_task_queue']}`",
    f"- Artifact: `{summary['artifact_hash']}`",
    "",
    "## Checks",
    *[f"- `{name}`: `{'passed' if ok else 'failed'}`" for name, ok in checks.items()],
    "",
    "## Evidence",
    f"- Query values: pre-signal=`{pre_value}` post-restart=`{restart_value}` post-complete=`{complete_value}`",
    f"- Replay divergence counts: post-restart=`{replay_post_restart.get('divergence_count')}` post-complete=`{replay_post_complete.get('divergence_count')}`",
    f"- Output: `{workflow_post_complete.get('output')}`",
    f"- Activity pollers after restart: `{len(activity_queue.get('pollers', []))}`",
])
(out_dir / "interceptor-pressure-drill-report.json").write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
(out_dir / "interceptor-pressure-drill-report.md").write_text(markdown + "\n", encoding="utf-8")
sys.exit(0 if overall else 1)
PY
}

if [[ "$SKIP_STACK" != "1" ]]; then
  if ! curl -fsS "$API_URL/health" >/dev/null 2>&1; then
    echo "[interceptor-pressure-drill] api gateway unavailable, starting dev stack"
    ./scripts/dev-stack.sh up
  else
    echo "[interceptor-pressure-drill] dev stack already reachable"
  fi
fi

wait_for_http "$API_URL/health" "api gateway"

echo "[interceptor-pressure-drill] running migration and deployment"
mkdir -p "$OUTPUT_DIR_ABS"
FABRIK_WORKFLOW_BUILD_ID="$BUILD_ID" cargo run -p fabrik-cli --bin fabrik -- migrate temporal "$REPO_PATH" --deploy --output-dir "$OUTPUT_DIR_ABS" --api-url "$API_URL" --tenant "$TENANT_ID"
extract_report_fields "$OUTPUT_DIR_ABS/migration-report.json" > "$OUTPUT_DIR_ABS/report-fields.json"

DEFINITION_ID="$(extract_field "$OUTPUT_DIR_ABS/report-fields.json" definition_id)"
DEFINITION_VERSION="$(extract_field "$OUTPUT_DIR_ABS/report-fields.json" definition_version)"
ARTIFACT_HASH="$(extract_field "$OUTPUT_DIR_ABS/report-fields.json" artifact_hash)"
WORKFLOW_TASK_QUEUE="$(extract_field "$OUTPUT_DIR_ABS/report-fields.json" workflow_task_queue)"

INSTANCE_ID="interceptor-pressure-$(python3 - <<'PY'
import uuid
print(str(uuid.uuid4())[:8])
PY
)"

echo "[interceptor-pressure-drill] triggering workflow definition=$DEFINITION_ID instance=$INSTANCE_ID queue=$WORKFLOW_TASK_QUEUE"
trigger_workflow "$DEFINITION_ID" "$INSTANCE_ID" "$WORKFLOW_TASK_QUEUE" "draft" "workflow"
RUN_ID="$(python3 - "$OUTPUT_DIR_ABS/workflow-trigger-response.json" <<'PY'
import json, sys
print(json.load(open(sys.argv[1], "r", encoding="utf-8"))["run_id"])
PY
)"

poll_json_path "/tenants/${TENANT_ID}/workflows/${INSTANCE_ID}" "data.get('run_id') == '${RUN_ID}' and data.get('status') == 'running'" "$OUTPUT_DIR_ABS/workflow-pre-signal.json"
query_workflow "$INSTANCE_ID" "query-pre-signal"

if [[ "$SKIP_RESTART" != "1" ]]; then
  echo "[interceptor-pressure-drill] restarting dev stack"
  ./scripts/dev-stack.sh down
  DEV_STACK_BUILD=0 ./scripts/dev-stack.sh up
  echo "[interceptor-pressure-drill] relaunching managed migrated workers"
  relaunch_managed_workers "$OUTPUT_DIR_ABS"
fi

wait_for_http "$API_URL/health" "api gateway after restart"
poll_json_path \
  "/admin/tenants/${TENANT_ID}/task-queues/activity/${WORKFLOW_TASK_QUEUE}" \
  "data.get('pollers')" \
  "$OUTPUT_DIR_ABS/task-queue-activity-post-restart.json" \
  300
poll_json_path "/tenants/${TENANT_ID}/workflows/${INSTANCE_ID}" "data.get('run_id') == '${RUN_ID}' and data.get('status') == 'running'" "$OUTPUT_DIR_ABS/workflow-post-restart.json"
json_get "/tenants/${TENANT_ID}/workflows/${INSTANCE_ID}/runs/${RUN_ID}/replay" "$OUTPUT_DIR_ABS/replay-post-restart.json"
query_workflow "$INSTANCE_ID" "query-post-restart"

echo "[interceptor-pressure-drill] signaling approval"
json_post \
  "/tenants/${TENANT_ID}/workflows/${INSTANCE_ID}/signals/approve" \
  '{"payload":"approved","request_id":"signal-approve"}' \
  "$OUTPUT_DIR_ABS/signal-response.json"

poll_json_path "/tenants/${TENANT_ID}/workflows/${INSTANCE_ID}" "data.get('run_id') == '${RUN_ID}' and data.get('status') == 'completed'" "$OUTPUT_DIR_ABS/workflow-post-complete.json"
json_get "/tenants/${TENANT_ID}/workflows/${INSTANCE_ID}/runs/${RUN_ID}/replay" "$OUTPUT_DIR_ABS/replay-post-complete.json"
query_workflow "$INSTANCE_ID" "query-post-complete"

write_meta "$DEFINITION_ID" "$DEFINITION_VERSION" "$ARTIFACT_HASH" "$WORKFLOW_TASK_QUEUE" "$INSTANCE_ID" "$RUN_ID"
summarize_evidence

echo "[interceptor-pressure-drill] completed $(timestamp)"
echo "[interceptor-pressure-drill] report: $OUTPUT_DIR_ABS/interceptor-pressure-drill-report.md"
