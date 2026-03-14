#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

usage() {
  cat >&2 <<'EOF'
usage: scripts/run-monorepo-multiworker-pressure-drill.sh [options]

options:
  --repo <path>
  --output-dir <path>
  --tenant <tenant_id>
  --api-url <url>
  --skip-stack
EOF
  exit 1
}

REPO_PATH="crates/fabrik-cli/test-fixtures/temporal-monorepo-multiworker-pressure"
OUTPUT_DIR="target/alpha-drills/temporal-monorepo-multiworker-pressure"
TENANT_ID="monorepo-multiworker-$(python3 - <<'PY'
import uuid
print(str(uuid.uuid4())[:8])
PY
)"
DEFAULT_API_PORT="${API_GATEWAY_PORT:-3000}"
API_URL="http://127.0.0.1:${DEFAULT_API_PORT}"
SKIP_STACK=0

while (($#)); do
  case "$1" in
    --repo) REPO_PATH="$2"; shift 2 ;;
    --output-dir) OUTPUT_DIR="$2"; shift 2 ;;
    --tenant) TENANT_ID="$2"; shift 2 ;;
    --api-url) API_URL="$2"; shift 2 ;;
    --skip-stack) SKIP_STACK=1; shift ;;
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

echo "[monorepo-multiworker-drill] started $(timestamp)"
echo "[monorepo-multiworker-drill] repo=$REPO_PATH tenant=$TENANT_ID api=$API_URL output=$OUTPUT_DIR_ABS"

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

extract_report_fields() {
  local report_path=$1
  python3 - "$report_path" <<'PY'
import json, sys

report = json.load(open(sys.argv[1], "r", encoding="utf-8"))
payload = {"orders": {}, "reports": {}}

for workflow in report["compiled_workflows"]:
    definition_id = workflow["definition_id"]
    if "orders" in definition_id:
        payload["orders"].update({
            "definition_id": definition_id,
            "definition_version": workflow["definition_version"],
            "artifact_hash": workflow["artifact_hash"],
        })
    elif "reports" in definition_id:
        payload["reports"].update({
            "definition_id": definition_id,
            "definition_version": workflow["definition_version"],
            "artifact_hash": workflow["artifact_hash"],
        })

for worker in report.get("deployment", {}).get("workers", []):
    queue = worker.get("task_queue")
    build_id = worker.get("build_id")
    if queue == "monorepo-orders":
        payload["orders"].update({
            "workflow_task_queue": queue,
            "build_id": build_id,
        })
    elif queue == "monorepo-reports":
        payload["reports"].update({
            "workflow_task_queue": queue,
            "build_id": build_id,
        })

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
  local workflow_input=$4
  local out_prefix=$5
  local payload
  payload="$(python3 - "$TENANT_ID" "$instance_id" "$queue" "$workflow_input" "$out_prefix" <<'PY'
import json, sys
tenant_id, instance_id, queue, workflow_input, out_prefix = sys.argv[1:6]
print(json.dumps({
    "tenant_id": tenant_id,
    "instance_id": instance_id,
    "workflow_task_queue": queue,
    "input": [workflow_input],
    "memo": {"lane": "monorepo"},
    "searchAttributes": {"PressureGroup": "monorepo"},
    "request_id": f"{out_prefix}-trigger",
}))
PY
)"
  local response_path="$OUTPUT_DIR_ABS/${out_prefix}-trigger-response.json"
  curl -fsS -X POST "${API_URL}/workflows/${definition_id}/trigger" -H "content-type: application/json" --data-binary "$payload" -o "$response_path"
}

query_workflow() {
  local instance_id=$1
  local out_prefix=$2
  json_post \
    "/tenants/${TENANT_ID}/workflows/${instance_id}/queries/status" \
    '{"args":[]}' \
    "$OUTPUT_DIR_ABS/${out_prefix}.json"
}

poll_query_result() {
  local instance_id=$1
  local expr=$2
  local dest=$3
  local timeout_seconds=${4:-120}
  python3 - "$API_URL" "$TENANT_ID" "$instance_id" "$expr" "$dest" "$timeout_seconds" <<'PY'
import json, sys, time, urllib.request

api_url, tenant_id, instance_id, expr, dest, timeout_seconds = sys.argv[1:7]
deadline = time.time() + float(timeout_seconds)
url = f"{api_url}/tenants/{tenant_id}/workflows/{instance_id}/queries/status"
payload = b'{"args":[]}'

while time.time() < deadline:
    request = urllib.request.Request(
        url,
        data=payload,
        headers={"content-type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=5) as response:
            body = response.read()
        data = json.loads(body)
        value = data.get("result", data)
        if eval(expr, {"__builtins__": {}}, {"value": value}):
            with open(dest, "wb") as handle:
                handle.write(body)
            sys.exit(0)
    except Exception:
        pass
    time.sleep(1)

print(f"timed out waiting for query condition on {url}: {expr}", file=sys.stderr)
sys.exit(1)
PY
}

write_meta() {
  python3 - "$OUTPUT_DIR_ABS/drill-meta.json" "$REPO_PATH" "$TENANT_ID" "$@" <<'PY'
import json, sys
(
    output_path,
    repo_path,
    tenant_id,
    orders_definition_id,
    orders_definition_version,
    orders_artifact_hash,
    orders_queue,
    orders_instance_id,
    orders_run_id,
    reports_definition_id,
    reports_definition_version,
    reports_artifact_hash,
    reports_queue,
    reports_instance_id,
    reports_run_id,
) = sys.argv[1:]

payload = {
    "repo_path": repo_path,
    "tenant_id": tenant_id,
    "orders": {
        "definition_id": orders_definition_id,
        "definition_version": int(orders_definition_version),
        "artifact_hash": orders_artifact_hash,
        "workflow_task_queue": orders_queue,
        "instance_id": orders_instance_id,
        "run_id": orders_run_id,
    },
    "reports": {
        "definition_id": reports_definition_id,
        "definition_version": int(reports_definition_version),
        "artifact_hash": reports_artifact_hash,
        "workflow_task_queue": reports_queue,
        "instance_id": reports_instance_id,
        "run_id": reports_run_id,
    },
}
with open(output_path, "w", encoding="utf-8") as handle:
    json.dump(payload, handle, indent=2)
PY
}

build_report() {
  python3 - "$OUTPUT_DIR_ABS" <<'PY'
import json, pathlib, sys

out_dir = pathlib.Path(sys.argv[1])
meta = json.loads((out_dir / "drill-meta.json").read_text(encoding="utf-8"))

def load(name):
    return json.loads((out_dir / name).read_text(encoding="utf-8"))

def qvalue(payload):
    return payload.get("result", payload)

orders_pre = qvalue(load("orders-query-pre-signal.json"))
orders_restart = qvalue(load("orders-query-post-restart.json"))
orders_complete = qvalue(load("orders-query-post-complete.json"))
reports_pre = qvalue(load("reports-query-pre-signal.json"))
reports_restart = qvalue(load("reports-query-post-restart.json"))
reports_complete = qvalue(load("reports-query-post-complete.json"))

orders_workflow_restart = load("orders-workflow-post-restart.json")
orders_workflow_complete = load("orders-workflow-post-complete.json")
reports_workflow_restart = load("reports-workflow-post-restart.json")
reports_workflow_complete = load("reports-workflow-post-complete.json")

orders_replay_restart = load("orders-replay-post-restart.json")
orders_replay_complete = load("orders-replay-post-complete.json")
reports_replay_restart = load("reports-replay-post-restart.json")
reports_replay_complete = load("reports-replay-post-complete.json")

orders_queue = load("orders-task-queue-activity-post-restart.json")
reports_queue = load("reports-task-queue-activity-post-restart.json")

checks = {
    "deploy_succeeded": True,
    "orders_running_before_release": True,
    "reports_running_before_release": True,
    "orders_query_before_release_matches": orders_pre == {"prepared": "prepared:order-42", "released": False},
    "reports_query_before_release_matches": reports_pre == {"built": "report:weekly-sales", "released": False},
    "orders_preserved_artifact_after_restart": orders_workflow_restart.get("artifact_hash") == meta["orders"]["artifact_hash"],
    "reports_preserved_artifact_after_restart": reports_workflow_restart.get("artifact_hash") == meta["reports"]["artifact_hash"],
    "orders_query_after_restart_matches": orders_restart == {"prepared": "prepared:order-42", "released": False},
    "reports_query_after_restart_matches": reports_restart == {"built": "report:weekly-sales", "released": False},
    "orders_activity_pollers_present_after_restart": len(orders_queue.get("pollers", [])) > 0,
    "reports_activity_pollers_present_after_restart": len(reports_queue.get("pollers", [])) > 0,
    "orders_replay_clean_after_restart": orders_replay_restart.get("projection_matches_store") is True and orders_replay_restart.get("divergence_count") == 0,
    "reports_replay_clean_after_restart": reports_replay_restart.get("projection_matches_store") is True and reports_replay_restart.get("divergence_count") == 0,
    "orders_completed_with_expected_output": orders_workflow_complete.get("status") == "completed" and orders_workflow_complete.get("output") == "prepared:order-42",
    "reports_completed_with_expected_output": reports_workflow_complete.get("status") == "completed" and reports_workflow_complete.get("output") == "report:weekly-sales",
    "orders_query_after_complete_matches": orders_complete == {"prepared": "prepared:order-42", "released": True},
    "reports_query_after_complete_matches": reports_complete == {"built": "report:weekly-sales", "released": True},
    "orders_replay_clean_after_complete": orders_replay_complete.get("projection_matches_store") is True and orders_replay_complete.get("divergence_count") == 0,
    "reports_replay_clean_after_complete": reports_replay_complete.get("projection_matches_store") is True and reports_replay_complete.get("divergence_count") == 0,
}

status = "passed" if all(checks.values()) else "failed"
report = {
    "schema_version": 1,
    "status": status,
    "tenant_id": meta["tenant_id"],
    "checks": checks,
    "orders": {
        **meta["orders"],
        "query_values": {
            "pre_release": orders_pre,
            "post_restart": orders_restart,
            "post_complete": orders_complete,
        },
        "replay_divergence_counts": {
            "post_restart": orders_replay_restart.get("divergence_count"),
            "post_complete": orders_replay_complete.get("divergence_count"),
        },
        "activity_poller_count_post_restart": len(orders_queue.get("pollers", [])),
        "output": orders_workflow_complete.get("output"),
    },
    "reports": {
        **meta["reports"],
        "query_values": {
            "pre_release": reports_pre,
            "post_restart": reports_restart,
            "post_complete": reports_complete,
        },
        "replay_divergence_counts": {
            "post_restart": reports_replay_restart.get("divergence_count"),
            "post_complete": reports_replay_complete.get("divergence_count"),
        },
        "activity_poller_count_post_restart": len(reports_queue.get("pollers", [])),
        "output": reports_workflow_complete.get("output"),
    },
}

(out_dir / "monorepo-multiworker-pressure-drill-report.json").write_text(json.dumps(report, indent=2), encoding="utf-8")
lines = [
    "# Monorepo Multiworker Pressure Drill Report",
    "",
    f"- Status: `{status}`",
    f"- Tenant: `{meta['tenant_id']}`",
    f"- Orders queue: `{meta['orders']['workflow_task_queue']}`",
    f"- Reports queue: `{meta['reports']['workflow_task_queue']}`",
    "",
    "## Checks",
]
for key, value in checks.items():
    lines.append(f"- `{key}`: `{'passed' if value else 'failed'}`")
lines.extend([
    "",
    "## Evidence",
    f"- Orders query values: pre-release=`{orders_pre}` post-restart=`{orders_restart}` post-complete=`{orders_complete}`",
    f"- Reports query values: pre-release=`{reports_pre}` post-restart=`{reports_restart}` post-complete=`{reports_complete}`",
    f"- Orders replay divergence: post-restart=`{orders_replay_restart.get('divergence_count')}` post-complete=`{orders_replay_complete.get('divergence_count')}`",
    f"- Reports replay divergence: post-restart=`{reports_replay_restart.get('divergence_count')}` post-complete=`{reports_replay_complete.get('divergence_count')}`",
    f"- Orders output: `{orders_workflow_complete.get('output')}`",
    f"- Reports output: `{reports_workflow_complete.get('output')}`",
    f"- Orders activity pollers after restart: `{len(orders_queue.get('pollers', []))}`",
    f"- Reports activity pollers after restart: `{len(reports_queue.get('pollers', []))}`",
])
(out_dir / "monorepo-multiworker-pressure-drill-report.md").write_text("\n".join(lines) + "\n", encoding="utf-8")
print(json.dumps(report, indent=2))
PY
}

if [[ "$SKIP_STACK" != "1" ]]; then
  if [[ -x ./scripts/dev-stack.sh ]]; then
    echo "[monorepo-multiworker-drill] restarting dev stack"
    ./scripts/dev-stack.sh down
    DEV_STACK_BUILD="${DEV_STACK_BUILD:-0}" ./scripts/dev-stack.sh up
  else
    echo "[monorepo-multiworker-drill] dev-stack.sh missing" >&2
    exit 1
  fi
fi

wait_for_http "$API_URL/health" "api gateway"

echo "[monorepo-multiworker-drill] running migration and deployment"
FABRIK_WORKFLOW_BUILD_ID="monorepo-multiworker-v1" cargo run -p fabrik-cli --bin fabrik -- migrate temporal "$REPO_PATH" --deploy --output-dir "$OUTPUT_DIR_ABS" --api-url "$API_URL" --tenant "$TENANT_ID"
extract_report_fields "$OUTPUT_DIR_ABS/migration-report.json" > "$OUTPUT_DIR_ABS/report-fields.json"

ORDERS_DEFINITION_ID="$(extract_nested_field "$OUTPUT_DIR_ABS/report-fields.json" orders definition_id)"
ORDERS_DEFINITION_VERSION="$(extract_nested_field "$OUTPUT_DIR_ABS/report-fields.json" orders definition_version)"
ORDERS_ARTIFACT_HASH="$(extract_nested_field "$OUTPUT_DIR_ABS/report-fields.json" orders artifact_hash)"
ORDERS_TASK_QUEUE="$(extract_nested_field "$OUTPUT_DIR_ABS/report-fields.json" orders workflow_task_queue)"

REPORTS_DEFINITION_ID="$(extract_nested_field "$OUTPUT_DIR_ABS/report-fields.json" reports definition_id)"
REPORTS_DEFINITION_VERSION="$(extract_nested_field "$OUTPUT_DIR_ABS/report-fields.json" reports definition_version)"
REPORTS_ARTIFACT_HASH="$(extract_nested_field "$OUTPUT_DIR_ABS/report-fields.json" reports artifact_hash)"
REPORTS_TASK_QUEUE="$(extract_nested_field "$OUTPUT_DIR_ABS/report-fields.json" reports workflow_task_queue)"

ORDERS_INSTANCE_ID="monorepo-orders-$(python3 - <<'PY'
import uuid
print(str(uuid.uuid4())[:8])
PY
)"
REPORTS_INSTANCE_ID="monorepo-reports-$(python3 - <<'PY'
import uuid
print(str(uuid.uuid4())[:8])
PY
)"

echo "[monorepo-multiworker-drill] triggering orders workflow definition=$ORDERS_DEFINITION_ID instance=$ORDERS_INSTANCE_ID queue=$ORDERS_TASK_QUEUE"
trigger_workflow "$ORDERS_DEFINITION_ID" "$ORDERS_INSTANCE_ID" "$ORDERS_TASK_QUEUE" "order-42" "orders"
ORDERS_RUN_ID="$(python3 - "$OUTPUT_DIR_ABS/orders-trigger-response.json" <<'PY'
import json, sys
print(json.load(open(sys.argv[1], "r", encoding="utf-8"))["run_id"])
PY
)"

echo "[monorepo-multiworker-drill] triggering reports workflow definition=$REPORTS_DEFINITION_ID instance=$REPORTS_INSTANCE_ID queue=$REPORTS_TASK_QUEUE"
trigger_workflow "$REPORTS_DEFINITION_ID" "$REPORTS_INSTANCE_ID" "$REPORTS_TASK_QUEUE" "weekly-sales" "reports"
REPORTS_RUN_ID="$(python3 - "$OUTPUT_DIR_ABS/reports-trigger-response.json" <<'PY'
import json, sys
print(json.load(open(sys.argv[1], "r", encoding="utf-8"))["run_id"])
PY
)"

poll_json_path "/tenants/${TENANT_ID}/workflows/${ORDERS_INSTANCE_ID}" "data.get('run_id') == '${ORDERS_RUN_ID}' and data.get('status') == 'running'" "$OUTPUT_DIR_ABS/orders-workflow-pre-release.json"
poll_json_path "/tenants/${TENANT_ID}/workflows/${REPORTS_INSTANCE_ID}" "data.get('run_id') == '${REPORTS_RUN_ID}' and data.get('status') == 'running'" "$OUTPUT_DIR_ABS/reports-workflow-pre-release.json"
poll_query_result "$ORDERS_INSTANCE_ID" "value == {'prepared': 'prepared:order-42', 'released': False}" "$OUTPUT_DIR_ABS/orders-query-pre-signal.json"
poll_query_result "$REPORTS_INSTANCE_ID" "value == {'built': 'report:weekly-sales', 'released': False}" "$OUTPUT_DIR_ABS/reports-query-pre-signal.json"

echo "[monorepo-multiworker-drill] restarting dev stack"
./scripts/dev-stack.sh down
DEV_STACK_BUILD="${DEV_STACK_BUILD:-0}" ./scripts/dev-stack.sh up
wait_for_http "$API_URL/health" "api gateway after restart"
echo "[monorepo-multiworker-drill] relaunching managed migrated workers"
relaunch_managed_workers "$OUTPUT_DIR_ABS"
sleep 3

poll_json_path "/tenants/${TENANT_ID}/workflows/${ORDERS_INSTANCE_ID}" "data.get('run_id') == '${ORDERS_RUN_ID}' and data.get('status') == 'running'" "$OUTPUT_DIR_ABS/orders-workflow-post-restart.json"
poll_json_path "/tenants/${TENANT_ID}/workflows/${REPORTS_INSTANCE_ID}" "data.get('run_id') == '${REPORTS_RUN_ID}' and data.get('status') == 'running'" "$OUTPUT_DIR_ABS/reports-workflow-post-restart.json"
json_get "/tenants/${TENANT_ID}/workflows/${ORDERS_INSTANCE_ID}/runs/${ORDERS_RUN_ID}/replay" "$OUTPUT_DIR_ABS/orders-replay-post-restart.json"
json_get "/tenants/${TENANT_ID}/workflows/${REPORTS_INSTANCE_ID}/runs/${REPORTS_RUN_ID}/replay" "$OUTPUT_DIR_ABS/reports-replay-post-restart.json"
poll_query_result "$ORDERS_INSTANCE_ID" "value == {'prepared': 'prepared:order-42', 'released': False}" "$OUTPUT_DIR_ABS/orders-query-post-restart.json"
poll_query_result "$REPORTS_INSTANCE_ID" "value == {'built': 'report:weekly-sales', 'released': False}" "$OUTPUT_DIR_ABS/reports-query-post-restart.json"
json_get "/admin/tenants/${TENANT_ID}/task-queues/activity/${ORDERS_TASK_QUEUE}" "$OUTPUT_DIR_ABS/orders-task-queue-activity-post-restart.json"
json_get "/admin/tenants/${TENANT_ID}/task-queues/activity/${REPORTS_TASK_QUEUE}" "$OUTPUT_DIR_ABS/reports-task-queue-activity-post-restart.json"

echo "[monorepo-multiworker-drill] signaling release for both workflows"
json_post "/tenants/${TENANT_ID}/workflows/${ORDERS_INSTANCE_ID}/signals/release" '{"payload":true,"request_id":"orders-signal-release"}' "$OUTPUT_DIR_ABS/orders-signal-response.json"
json_post "/tenants/${TENANT_ID}/workflows/${REPORTS_INSTANCE_ID}/signals/release" '{"payload":true,"request_id":"reports-signal-release"}' "$OUTPUT_DIR_ABS/reports-signal-response.json"

poll_json_path "/tenants/${TENANT_ID}/workflows/${ORDERS_INSTANCE_ID}" "data.get('run_id') == '${ORDERS_RUN_ID}' and data.get('status') == 'completed'" "$OUTPUT_DIR_ABS/orders-workflow-post-complete.json"
poll_json_path "/tenants/${TENANT_ID}/workflows/${REPORTS_INSTANCE_ID}" "data.get('run_id') == '${REPORTS_RUN_ID}' and data.get('status') == 'completed'" "$OUTPUT_DIR_ABS/reports-workflow-post-complete.json"
json_get "/tenants/${TENANT_ID}/workflows/${ORDERS_INSTANCE_ID}/runs/${ORDERS_RUN_ID}/replay" "$OUTPUT_DIR_ABS/orders-replay-post-complete.json"
json_get "/tenants/${TENANT_ID}/workflows/${REPORTS_INSTANCE_ID}/runs/${REPORTS_RUN_ID}/replay" "$OUTPUT_DIR_ABS/reports-replay-post-complete.json"
query_workflow "$ORDERS_INSTANCE_ID" "orders-query-post-complete"
query_workflow "$REPORTS_INSTANCE_ID" "reports-query-post-complete"

write_meta \
  "$ORDERS_DEFINITION_ID" "$ORDERS_DEFINITION_VERSION" "$ORDERS_ARTIFACT_HASH" "$ORDERS_TASK_QUEUE" "$ORDERS_INSTANCE_ID" "$ORDERS_RUN_ID" \
  "$REPORTS_DEFINITION_ID" "$REPORTS_DEFINITION_VERSION" "$REPORTS_ARTIFACT_HASH" "$REPORTS_TASK_QUEUE" "$REPORTS_INSTANCE_ID" "$REPORTS_RUN_ID"
build_report > /dev/null

echo "[monorepo-multiworker-drill] completed $(timestamp)"
echo "[monorepo-multiworker-drill] report: $OUTPUT_DIR_ABS/monorepo-multiworker-pressure-drill-report.md"
