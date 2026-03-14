#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

usage() {
  cat >&2 <<'EOF'
usage: scripts/run-versioning-pressure-mixed-build-drill.sh [options]

options:
  --repo-v1 <path>
  --repo-v2 <path>
  --output-dir <path>
  --tenant <tenant_id>
  --api-url <url>
  --build-v1 <build_id>
  --build-v2 <build_id>
  --skip-stack
  --skip-restart
EOF
  exit 1
}

REPO_V1="crates/fabrik-cli/test-fixtures/temporal-versioning-upgrade-pressure"
REPO_V2="crates/fabrik-cli/test-fixtures/temporal-versioning-upgrade-pressure-v2"
OUTPUT_DIR="target/alpha-drills/temporal-versioning-upgrade-pressure"
TENANT_ID="versioning-pressure-$(python3 - <<'PY'
import uuid
print(str(uuid.uuid4())[:8])
PY
)"
DEFAULT_API_PORT="${API_GATEWAY_PORT:-3000}"
API_URL="http://127.0.0.1:${DEFAULT_API_PORT}"
BUILD_V1="versioning-pressure-v1"
BUILD_V2="versioning-pressure-v2"
SKIP_STACK=0
SKIP_RESTART=0

while (($#)); do
  case "$1" in
    --repo-v1) REPO_V1="$2"; shift 2 ;;
    --repo-v2) REPO_V2="$2"; shift 2 ;;
    --output-dir) OUTPUT_DIR="$2"; shift 2 ;;
    --tenant) TENANT_ID="$2"; shift 2 ;;
    --api-url) API_URL="$2"; shift 2 ;;
    --build-v1) BUILD_V1="$2"; shift 2 ;;
    --build-v2) BUILD_V2="$2"; shift 2 ;;
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

echo "[versioning-pressure-drill] started $(timestamp)"
echo "[versioning-pressure-drill] repo_v1=$REPO_V1 repo_v2=$REPO_V2 tenant=$TENANT_ID api=$API_URL output=$OUTPUT_DIR_ABS"

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

worker_packages = sorted(workspace_dir.glob("workers/*/worker-package.json"))
for package_path in worker_packages:
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
  local name_arg=$4
  local out_prefix=$5
  local payload
  payload="$(python3 - "$TENANT_ID" "$instance_id" "$queue" "$name_arg" "$out_prefix" <<'PY'
import json, sys
tenant_id, instance_id, queue, name_arg, out_prefix = sys.argv[1:6]
print(json.dumps({
    "tenant_id": tenant_id,
    "instance_id": instance_id,
    "workflow_task_queue": queue,
    "input": [name_arg],
    "memo": {"lane": "upgrade"},
    "searchAttributes": {"PressureGroup": "versioning"},
    "request_id": f"{out_prefix}-trigger",
}))
PY
)"
  json_post "/workflows/${definition_id}/trigger" "$payload" "$OUTPUT_DIR_ABS/${out_prefix}-trigger-response.json"
}

write_meta() {
  python3 - "$OUTPUT_DIR_ABS/meta.json" "$REPO_V1" "$REPO_V2" "$TENANT_ID" "$@" <<'PY'
import json, sys
output_path, repo_v1, repo_v2, tenant_id, v1_definition_id, v1_version, v1_artifact_hash, v2_definition_id, v2_version, v2_artifact_hash, queue, old_instance_id, old_run_id, new_instance_id, new_run_id, rollback_instance_id, rollback_run_id = sys.argv[1:]
payload = {
    "repo_v1": repo_v1,
    "repo_v2": repo_v2,
    "tenant_id": tenant_id,
    "workflow_task_queue": queue,
    "v1": {
        "definition_id": v1_definition_id,
        "definition_version": int(v1_version),
        "artifact_hash": v1_artifact_hash,
        "instance_id": old_instance_id,
        "run_id": old_run_id,
    },
    "v2": {
        "definition_id": v2_definition_id,
        "definition_version": int(v2_version),
        "artifact_hash": v2_artifact_hash,
        "instance_id": new_instance_id,
        "run_id": new_run_id,
    },
    "rollback": {
        "instance_id": rollback_instance_id,
        "run_id": rollback_run_id,
    },
}
with open(output_path, "w", encoding="utf-8") as handle:
    json.dump(payload, handle, indent=2)
    handle.write("\n")
PY
}

summarize_evidence() {
  local out_dir=$1
  python3 - "$out_dir" "$BUILD_V1" "$BUILD_V2" <<'PY'
import json, pathlib, sys
out_dir = pathlib.Path(sys.argv[1])
build_v1 = sys.argv[2]
build_v2 = sys.argv[3]
def load(name):
    return json.loads((out_dir / name).read_text(encoding="utf-8"))
meta = load("meta.json")
v1_report = load("migration-v1/migration-report.json")
v2_report = load("migration-v2/migration-report.json")
old_post_upgrade = load("old-run-routing-post-upgrade.json")
new_post_upgrade = load("new-run-routing-post-upgrade.json")
old_post_restart = load("old-run-routing-post-restart.json")
new_post_restart = load("new-run-routing-post-restart.json")
old_replay_post_restart = load("old-run-replay-post-restart.json")
new_replay_post_restart = load("new-run-replay-post-restart.json")
old_workflow_post_complete = load("old-run-workflow-post-complete.json")
new_workflow_post_complete = load("new-run-workflow-post-complete.json")
rollback_workflow = load("rollback-run-workflow-post-complete.json")
old_replay_post_complete = load("old-run-replay-post-complete.json")
new_replay_post_complete = load("new-run-replay-post-complete.json")
rollback_replay_post_complete = load("rollback-run-replay-post-complete.json")
task_queue_candidate = load("task-queue-workflow-candidate.json")
task_queue_rollback = load("task-queue-workflow-rollback.json")
rollback_routing = load("rollback-run-routing-post-complete.json")

def is_v2_output(value):
    return isinstance(value, str) and value.endswith(":build-v2")

checks = {
    "v1_deploy_succeeded": v1_report["deployment"]["status"] == "deployed",
    "v2_deploy_succeeded": v2_report["deployment"]["status"] == "deployed",
    "old_run_stayed_on_v1_artifact_after_upgrade": old_post_upgrade["artifact_hash"] == meta["v1"]["artifact_hash"],
    "new_run_uses_v2_artifact": new_post_upgrade["artifact_hash"] == meta["v2"]["artifact_hash"],
    "old_run_preserved_v1_artifact_after_restart": old_post_restart["artifact_hash"] == meta["v1"]["artifact_hash"],
    "new_run_preserved_v2_artifact_after_restart": new_post_restart["artifact_hash"] == meta["v2"]["artifact_hash"],
    "old_run_replay_clean_after_restart": old_replay_post_restart["divergence_count"] == 0,
    "new_run_replay_clean_after_restart": new_replay_post_restart["divergence_count"] == 0,
    "old_run_completed_with_v1_output": not is_v2_output(old_workflow_post_complete["output"]),
    "new_run_completed_with_v2_output": is_v2_output(new_workflow_post_complete["output"]),
    "old_run_replay_clean_after_complete": old_replay_post_complete["divergence_count"] == 0,
    "new_run_replay_clean_after_complete": new_replay_post_complete["divergence_count"] == 0,
    "candidate_build_promoted": task_queue_candidate.get("default_set_id") == f"default-{build_v2}",
    "rollback_restored_default_set": task_queue_rollback.get("default_set_id") == f"default-{build_v1}",
    "rollback_run_uses_v1_artifact": rollback_routing["artifact_hash"] == meta["v1"]["artifact_hash"],
    "rollback_run_completed_with_v1_output": not is_v2_output(rollback_workflow["output"]),
    "rollback_run_replay_clean": rollback_replay_post_complete["divergence_count"] == 0,
}
overall = all(checks.values())
summary = {
    "schema_version": 1,
    "status": "passed" if overall else "failed",
    "tenant_id": meta["tenant_id"],
    "workflow_task_queue": meta["workflow_task_queue"],
    "builds": {"initial": build_v1, "candidate": build_v2},
    "artifacts": {"v1": meta["v1"]["artifact_hash"], "v2": meta["v2"]["artifact_hash"]},
    "instances": {"old": meta["v1"]["instance_id"], "new": meta["v2"]["instance_id"], "rollback": meta["rollback"]["instance_id"]},
    "checks": checks,
    "evidence": {
        "qualification_verdicts": {"v1": v1_report["alpha_qualification"]["verdict"], "v2": v2_report["alpha_qualification"]["verdict"]},
        "routing_artifact_hashes": {
            "old_after_upgrade": old_post_upgrade["artifact_hash"],
            "new_after_upgrade": new_post_upgrade["artifact_hash"],
            "old_after_restart": old_post_restart["artifact_hash"],
            "new_after_restart": new_post_restart["artifact_hash"],
            "rollback_after_rollback": rollback_routing["artifact_hash"],
        },
        "replay_divergence_counts": {
            "old_post_restart": old_replay_post_restart["divergence_count"],
            "new_post_restart": new_replay_post_restart["divergence_count"],
            "old_post_complete": old_replay_post_complete["divergence_count"],
            "new_post_complete": new_replay_post_complete["divergence_count"],
            "rollback_post_complete": rollback_replay_post_complete["divergence_count"],
        },
        "outputs": {
            "old": old_workflow_post_complete["output"],
            "new": new_workflow_post_complete["output"],
            "rollback": rollback_workflow["output"],
        },
        "default_sets": {
            "candidate": task_queue_candidate.get("default_set_id"),
            "rollback": task_queue_rollback.get("default_set_id"),
        },
    },
}
markdown = "\n".join([
    "# Versioning Pressure Mixed-Build Drill Report",
    "",
    f"- Status: `{summary['status']}`",
    f"- Tenant: `{summary['tenant_id']}`",
    f"- Queue: `{summary['workflow_task_queue']}`",
    f"- V1 artifact: `{summary['artifacts']['v1']}`",
    f"- V2 artifact: `{summary['artifacts']['v2']}`",
    "",
    "## Checks",
    *[f"- `{name}`: `{'passed' if ok else 'failed'}`" for name, ok in checks.items()],
    "",
    "## Evidence",
    f"- Old run artifact after upgrade/restart: `{old_post_upgrade['artifact_hash']}` / `{old_post_restart['artifact_hash']}`",
    f"- New run artifact after upgrade/restart: `{new_post_upgrade['artifact_hash']}` / `{new_post_restart['artifact_hash']}`",
    f"- Replay divergence counts: old-post-restart=`{old_replay_post_restart['divergence_count']}` new-post-restart=`{new_replay_post_restart['divergence_count']}` old-post-complete=`{old_replay_post_complete['divergence_count']}` new-post-complete=`{new_replay_post_complete['divergence_count']}` rollback=`{rollback_replay_post_complete['divergence_count']}`",
    f"- Outputs: old=`{old_workflow_post_complete['output']}` new=`{new_workflow_post_complete['output']}` rollback=`{rollback_workflow['output']}`",
    f"- Candidate default set: `{task_queue_candidate.get('default_set_id')}`",
    f"- Rollback default set: `{task_queue_rollback.get('default_set_id')}`",
])
(out_dir / "versioning-pressure-mixed-build-drill-report.json").write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
(out_dir / "versioning-pressure-mixed-build-drill-report.md").write_text(markdown + "\n", encoding="utf-8")
sys.exit(0 if overall else 1)
PY
}

if [[ "$SKIP_STACK" != "1" ]]; then
  if ! curl -fsS "$API_URL/health" >/dev/null 2>&1; then
    echo "[versioning-pressure-drill] api gateway unavailable, starting dev stack"
    scripts/dev-stack.sh up
  else
    echo "[versioning-pressure-drill] dev stack already reachable"
  fi
fi

wait_for_http "$API_URL/health" "api gateway"

echo "[versioning-pressure-drill] running v1 migration and deployment"
mkdir -p "$OUTPUT_DIR_ABS/migration-v1"
FABRIK_WORKFLOW_BUILD_ID="$BUILD_V1" cargo run -p fabrik-cli --bin fabrik -- migrate temporal "$REPO_V1" --deploy --output-dir "$OUTPUT_DIR_ABS/migration-v1" --api-url "$API_URL" --tenant "$TENANT_ID"
extract_report_fields "$OUTPUT_DIR_ABS/migration-v1/migration-report.json" > "$OUTPUT_DIR_ABS/report-fields-v1.json"

DEFINITION_ID_V1="$(extract_field "$OUTPUT_DIR_ABS/report-fields-v1.json" definition_id)"
DEFINITION_VERSION_V1="$(extract_field "$OUTPUT_DIR_ABS/report-fields-v1.json" definition_version)"
ARTIFACT_HASH_V1="$(extract_field "$OUTPUT_DIR_ABS/report-fields-v1.json" artifact_hash)"
WORKFLOW_TASK_QUEUE="$(extract_field "$OUTPUT_DIR_ABS/report-fields-v1.json" workflow_task_queue)"

OLD_INSTANCE_ID="versioning-pressure-old-$(python3 - <<'PY'
import uuid
print(str(uuid.uuid4())[:8])
PY
)"

echo "[versioning-pressure-drill] triggering old run definition=$DEFINITION_ID_V1 instance=$OLD_INSTANCE_ID queue=$WORKFLOW_TASK_QUEUE"
trigger_workflow "$DEFINITION_ID_V1" "$OLD_INSTANCE_ID" "$WORKFLOW_TASK_QUEUE" "alice" "old-run"
OLD_RUN_ID="$(python3 - "$OUTPUT_DIR_ABS/old-run-trigger-response.json" <<'PY'
import json, sys
print(json.load(open(sys.argv[1], "r", encoding="utf-8"))["run_id"])
PY
)"
poll_json_path "/tenants/${TENANT_ID}/workflows/${OLD_INSTANCE_ID}" "data.get('run_id') == '${OLD_RUN_ID}' and data.get('status') == 'running'" "$OUTPUT_DIR_ABS/old-run-workflow-pre-upgrade.json"
json_get "/tenants/${TENANT_ID}/workflows/${OLD_INSTANCE_ID}/routing" "$OUTPUT_DIR_ABS/old-run-routing-pre-upgrade.json"

echo "[versioning-pressure-drill] running v2 migration and deployment"
mkdir -p "$OUTPUT_DIR_ABS/migration-v2"
FABRIK_WORKFLOW_BUILD_ID="$BUILD_V2" cargo run -p fabrik-cli --bin fabrik -- migrate temporal "$REPO_V2" --deploy --output-dir "$OUTPUT_DIR_ABS/migration-v2" --api-url "$API_URL" --tenant "$TENANT_ID"
extract_report_fields "$OUTPUT_DIR_ABS/migration-v2/migration-report.json" > "$OUTPUT_DIR_ABS/report-fields-v2.json"

DEFINITION_ID_V2="$(extract_field "$OUTPUT_DIR_ABS/report-fields-v2.json" definition_id)"
DEFINITION_VERSION_V2="$(extract_field "$OUTPUT_DIR_ABS/report-fields-v2.json" definition_version)"
ARTIFACT_HASH_V2="$(extract_field "$OUTPUT_DIR_ABS/report-fields-v2.json" artifact_hash)"

json_get "/tenants/${TENANT_ID}/workflows/${OLD_INSTANCE_ID}/routing" "$OUTPUT_DIR_ABS/old-run-routing-post-upgrade.json"
json_get "/admin/tenants/${TENANT_ID}/task-queues/workflow/${WORKFLOW_TASK_QUEUE}" "$OUTPUT_DIR_ABS/task-queue-workflow-candidate.json"

NEW_INSTANCE_ID="versioning-pressure-new-$(python3 - <<'PY'
import uuid
print(str(uuid.uuid4())[:8])
PY
)"

echo "[versioning-pressure-drill] triggering new run definition=$DEFINITION_ID_V2 instance=$NEW_INSTANCE_ID"
trigger_workflow "$DEFINITION_ID_V2" "$NEW_INSTANCE_ID" "$WORKFLOW_TASK_QUEUE" "bob" "new-run"
NEW_RUN_ID="$(python3 - "$OUTPUT_DIR_ABS/new-run-trigger-response.json" <<'PY'
import json, sys
print(json.load(open(sys.argv[1], "r", encoding="utf-8"))["run_id"])
PY
)"
poll_json_path "/tenants/${TENANT_ID}/workflows/${NEW_INSTANCE_ID}" "data.get('run_id') == '${NEW_RUN_ID}' and data.get('status') == 'running'" "$OUTPUT_DIR_ABS/new-run-workflow-post-upgrade.json"
json_get "/tenants/${TENANT_ID}/workflows/${NEW_INSTANCE_ID}/routing" "$OUTPUT_DIR_ABS/new-run-routing-post-upgrade.json"

if [[ "$SKIP_RESTART" != "1" ]]; then
  echo "[versioning-pressure-drill] restarting dev stack"
  scripts/dev-stack.sh down
  DEV_STACK_BUILD=0 scripts/dev-stack.sh up
  echo "[versioning-pressure-drill] relaunching managed migrated workers"
  relaunch_managed_workers "$OUTPUT_DIR_ABS/migration-v1"
  relaunch_managed_workers "$OUTPUT_DIR_ABS/migration-v2"
fi

wait_for_http "$API_URL/health" "api gateway after restart"
poll_json_path \
  "/admin/tenants/${TENANT_ID}/task-queues/activity/${WORKFLOW_TASK_QUEUE}" \
  "data.get('pollers')" \
  "$OUTPUT_DIR_ABS/task-queue-activity-post-restart.json" \
  300
poll_json_path "/tenants/${TENANT_ID}/workflows/${OLD_INSTANCE_ID}" "data.get('run_id') == '${OLD_RUN_ID}' and data.get('status') == 'running'" "$OUTPUT_DIR_ABS/old-run-workflow-post-restart.json"
poll_json_path "/tenants/${TENANT_ID}/workflows/${NEW_INSTANCE_ID}" "data.get('run_id') == '${NEW_RUN_ID}' and data.get('status') == 'running'" "$OUTPUT_DIR_ABS/new-run-workflow-post-restart.json"
json_get "/tenants/${TENANT_ID}/workflows/${OLD_INSTANCE_ID}/routing" "$OUTPUT_DIR_ABS/old-run-routing-post-restart.json"
json_get "/tenants/${TENANT_ID}/workflows/${NEW_INSTANCE_ID}/routing" "$OUTPUT_DIR_ABS/new-run-routing-post-restart.json"
json_get "/tenants/${TENANT_ID}/workflows/${OLD_INSTANCE_ID}/runs/${OLD_RUN_ID}/replay" "$OUTPUT_DIR_ABS/old-run-replay-post-restart.json"
json_get "/tenants/${TENANT_ID}/workflows/${NEW_INSTANCE_ID}/runs/${NEW_RUN_ID}/replay" "$OUTPUT_DIR_ABS/new-run-replay-post-restart.json"

echo "[versioning-pressure-drill] signaling old and new runs"
json_post "/tenants/${TENANT_ID}/workflows/${OLD_INSTANCE_ID}/signals/proceed" '{"payload":true,"request_id":"signal-proceed-old"}' "$OUTPUT_DIR_ABS/old-run-signal-response.json"
json_post "/tenants/${TENANT_ID}/workflows/${NEW_INSTANCE_ID}/signals/proceed" '{"payload":true,"request_id":"signal-proceed-new"}' "$OUTPUT_DIR_ABS/new-run-signal-response.json"
poll_json_path "/tenants/${TENANT_ID}/workflows/${OLD_INSTANCE_ID}" "data.get('run_id') == '${OLD_RUN_ID}' and data.get('status') == 'completed'" "$OUTPUT_DIR_ABS/old-run-workflow-post-complete.json"
poll_json_path "/tenants/${TENANT_ID}/workflows/${NEW_INSTANCE_ID}" "data.get('run_id') == '${NEW_RUN_ID}' and data.get('status') == 'completed'" "$OUTPUT_DIR_ABS/new-run-workflow-post-complete.json"
json_get "/tenants/${TENANT_ID}/workflows/${OLD_INSTANCE_ID}/runs/${OLD_RUN_ID}/replay" "$OUTPUT_DIR_ABS/old-run-replay-post-complete.json"
json_get "/tenants/${TENANT_ID}/workflows/${NEW_INSTANCE_ID}/runs/${NEW_RUN_ID}/replay" "$OUTPUT_DIR_ABS/new-run-replay-post-complete.json"

echo "[versioning-pressure-drill] rolling back default workflow build to $BUILD_V1"
json_post "/admin/tenants/${TENANT_ID}/task-queues/workflow/${WORKFLOW_TASK_QUEUE}/default-set" "{\"set_id\":\"default-${BUILD_V1}\"}" "$OUTPUT_DIR_ABS/rollback-default-set.json"
json_get "/admin/tenants/${TENANT_ID}/task-queues/workflow/${WORKFLOW_TASK_QUEUE}" "$OUTPUT_DIR_ABS/task-queue-workflow-rollback.json"

ROLLBACK_INSTANCE_ID="versioning-pressure-rollback-$(python3 - <<'PY'
import uuid
print(str(uuid.uuid4())[:8])
PY
)"
echo "[versioning-pressure-drill] triggering rollback run definition=$DEFINITION_ID_V1 instance=$ROLLBACK_INSTANCE_ID"
trigger_workflow "$DEFINITION_ID_V1" "$ROLLBACK_INSTANCE_ID" "$WORKFLOW_TASK_QUEUE" "carol" "rollback-run"
ROLLBACK_RUN_ID="$(python3 - "$OUTPUT_DIR_ABS/rollback-run-trigger-response.json" <<'PY'
import json, sys
print(json.load(open(sys.argv[1], "r", encoding="utf-8"))["run_id"])
PY
)"
poll_json_path "/tenants/${TENANT_ID}/workflows/${ROLLBACK_INSTANCE_ID}" "data.get('run_id') == '${ROLLBACK_RUN_ID}' and data.get('status') == 'running'" "$OUTPUT_DIR_ABS/rollback-run-workflow-post-trigger.json"
json_get "/tenants/${TENANT_ID}/workflows/${ROLLBACK_INSTANCE_ID}/routing" "$OUTPUT_DIR_ABS/rollback-run-routing-post-trigger.json"
json_post "/tenants/${TENANT_ID}/workflows/${ROLLBACK_INSTANCE_ID}/signals/proceed" '{"payload":true,"request_id":"signal-proceed-rollback"}' "$OUTPUT_DIR_ABS/rollback-run-signal-response.json"
poll_json_path "/tenants/${TENANT_ID}/workflows/${ROLLBACK_INSTANCE_ID}" "data.get('run_id') == '${ROLLBACK_RUN_ID}' and data.get('status') == 'completed'" "$OUTPUT_DIR_ABS/rollback-run-workflow-post-complete.json"
json_get "/tenants/${TENANT_ID}/workflows/${ROLLBACK_INSTANCE_ID}/routing" "$OUTPUT_DIR_ABS/rollback-run-routing-post-complete.json"
json_get "/tenants/${TENANT_ID}/workflows/${ROLLBACK_INSTANCE_ID}/runs/${ROLLBACK_RUN_ID}/replay" "$OUTPUT_DIR_ABS/rollback-run-replay-post-complete.json"

write_meta "$DEFINITION_ID_V1" "$DEFINITION_VERSION_V1" "$ARTIFACT_HASH_V1" "$DEFINITION_ID_V2" "$DEFINITION_VERSION_V2" "$ARTIFACT_HASH_V2" "$WORKFLOW_TASK_QUEUE" "$OLD_INSTANCE_ID" "$OLD_RUN_ID" "$NEW_INSTANCE_ID" "$NEW_RUN_ID" "$ROLLBACK_INSTANCE_ID" "$ROLLBACK_RUN_ID"
summarize_evidence "$OUTPUT_DIR_ABS"

echo "[versioning-pressure-drill] completed $(timestamp)"
echo "[versioning-pressure-drill] report: $OUTPUT_DIR_ABS/versioning-pressure-mixed-build-drill-report.md"
