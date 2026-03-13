#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

usage() {
  cat >&2 <<'EOF'
usage: scripts/run-alpha-primary-drill.sh [options]

options:
  --repo <path>
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

REPO_PATH="crates/fabrik-cli/test-fixtures/temporal-supported"
OUTPUT_DIR="target/alpha-drills/temporal-supported"
TENANT_ID="dev"
DEFAULT_API_PORT="${API_GATEWAY_PORT:-3000}"
API_URL="http://127.0.0.1:${DEFAULT_API_PORT}"
BUILD_V1="alpha-primary-v1"
BUILD_V2="alpha-primary-v2"
SKIP_STACK=0
SKIP_RESTART=0

while (($#)); do
  case "$1" in
    --repo)
      REPO_PATH="$2"
      shift 2
      ;;
    --output-dir)
      OUTPUT_DIR="$2"
      shift 2
      ;;
    --tenant)
      TENANT_ID="$2"
      shift 2
      ;;
    --api-url)
      API_URL="$2"
      shift 2
      ;;
    --build-v1)
      BUILD_V1="$2"
      shift 2
      ;;
    --build-v2)
      BUILD_V2="$2"
      shift 2
      ;;
    --skip-stack)
      SKIP_STACK=1
      shift
      ;;
    --skip-restart)
      SKIP_RESTART=1
      shift
      ;;
    *)
      echo "unknown argument: $1" >&2
      usage
      ;;
  esac
done

require_bin() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "$1 is required" >&2
    exit 1
  fi
}

require_bin cargo
require_bin curl
require_bin python3

mkdir -p "$OUTPUT_DIR"
OUTPUT_DIR_ABS="$(cd "$OUTPUT_DIR" && pwd)"
RUN_LOG="$OUTPUT_DIR_ABS/drill.log"
exec > >(tee "$RUN_LOG") 2>&1

timestamp() {
  date -u +"%Y-%m-%dT%H:%M:%SZ"
}

echo "[alpha-drill] started $(timestamp)"
echo "[alpha-drill] repo=$REPO_PATH tenant=$TENANT_ID api=$API_URL output=$OUTPUT_DIR_ABS"

wait_for_http() {
  local url=$1
  local label=$2
  python3 - "$url" "$label" <<'PY'
import sys
import time
import urllib.error
import urllib.request

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
  curl -fsS -X POST "${API_URL}${path}" \
    -H "content-type: application/json" \
    --data-binary "$payload" \
    -o "$dest"
}

poll_json_path() {
  local path=$1
  local python_expr=$2
  local dest=$3
  python3 - "$API_URL" "$path" "$python_expr" "$dest" <<'PY'
import json
import sys
import time
import urllib.error
import urllib.request

api_url, path, expr, dest = sys.argv[1:5]
deadline = time.time() + 120
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
import json
import sys

report = json.load(open(sys.argv[1], "r", encoding="utf-8"))
workflow = report["compiled_workflows"][0]
deployment = report["deployment"]
worker = deployment["workers"][0] if deployment["workers"] else {}
artifact = deployment["published_artifacts"][0] if deployment["published_artifacts"] else {}
print(json.dumps({
    "status": report["status"],
    "qualification": report["alpha_qualification"]["verdict"],
    "definition_id": workflow["definition_id"],
    "artifact_hash": artifact.get("artifact_hash") or workflow.get("artifact_hash"),
    "workflow_task_queue": worker.get("task_queue") or workflow.get("workflow_task_queue") or "default",
    "activity_task_queue": worker.get("task_queue"),
    "activity_build_id": worker.get("build_id"),
}, indent=2))
PY
}

summarize_evidence() {
  local out_dir=$1
  python3 - "$out_dir" "$BUILD_V1" "$BUILD_V2" <<'PY'
import json
import pathlib
import sys

out_dir = pathlib.Path(sys.argv[1])
build_v1 = sys.argv[2]
build_v2 = sys.argv[3]

def load(name):
    return json.loads((out_dir / name).read_text(encoding="utf-8"))

meta = load("meta.json")
report = load("migration-v1/migration-report.json")
pre_routing = load("routing-pre-restart.json")
post_restart_routing = load("routing-post-restart.json")
post_complete_routing = load("routing-post-complete.json")
replay_pre = load("replay-pre-restart.json")
replay_post_restart = load("replay-post-restart.json")
replay_post_complete = load("replay-post-complete.json")
task_queue_candidate = load("task-queue-workflow-candidate.json")
task_queue_rollback = load("task-queue-workflow-rollback.json")
workflow_after_complete = load("workflow-post-complete.json")

checks = {
    "qualified_or_caveated": report["alpha_qualification"]["verdict"] in {"qualified", "qualified_with_caveats"},
    "initial_deploy_succeeded": report["deployment"]["status"] == "deployed",
    "pre_restart_replay_clean": replay_pre["divergence_count"] == 0,
    "post_restart_replay_clean": replay_post_restart["divergence_count"] == 0,
    "post_complete_replay_clean": replay_post_complete["divergence_count"] == 0,
    "queue_preserved_post_restart": post_restart_routing["workflow_task_queue"] == replay_post_restart.get("replayed_state", {}).get("workflow_task_queue"),
    "workflow_completed": workflow_after_complete["status"] == "completed",
    "candidate_build_promoted": task_queue_candidate.get("default_set_id") == f"default-{build_v2}",
    "rollback_restored_default_set": task_queue_rollback.get("default_set_id") == f"default-{build_v1}",
}
overall = all(checks.values())

summary = {
    "schema_version": 1,
    "status": "passed" if overall else "failed",
    "repo_path": meta["repo_path"],
    "tenant_id": meta["tenant_id"],
    "definition_id": meta["definition_id"],
    "instance_id": meta["instance_id"],
    "run_id": meta["run_id"],
    "builds": {
        "initial": build_v1,
        "candidate": build_v2,
    },
    "checks": checks,
    "evidence": {
        "qualification_verdict": report["alpha_qualification"]["verdict"],
        "deployment_status": report["deployment"]["status"],
        "routing_status_pre_restart": pre_routing["routing_status"],
        "routing_status_post_restart": post_restart_routing["routing_status"],
        "routing_status_post_complete": post_complete_routing["routing_status"],
        "workflow_status_post_complete": workflow_after_complete["status"],
        "replay_divergence_counts": {
            "pre_restart": replay_pre["divergence_count"],
            "post_restart": replay_post_restart["divergence_count"],
            "post_complete": replay_post_complete["divergence_count"],
        },
        "default_sets": {
            "candidate": task_queue_candidate.get("default_set_id"),
            "rollback": task_queue_rollback.get("default_set_id"),
        },
    },
}
markdown = [
    "# Alpha Primary Drill Report",
    "",
    f"- Status: `{summary['status']}`",
    f"- Repo: `{meta['repo_path']}`",
    f"- Definition: `{meta['definition_id']}`",
    f"- Instance: `{meta['instance_id']}`",
    f"- Run: `{meta['run_id']}`",
    f"- Qualification: `{report['alpha_qualification']['verdict']}`",
    f"- Deployment: `{report['deployment']['status']}`",
    "",
    "## Checks",
]
for name, passed in checks.items():
    markdown.append(f"- `{name}`: `{'passed' if passed else 'failed'}`")
markdown.extend([
    "",
    "## Evidence",
    f"- Replay divergence counts: pre=`{replay_pre['divergence_count']}` post-restart=`{replay_post_restart['divergence_count']}` post-complete=`{replay_post_complete['divergence_count']}`",
    f"- Queue preserved after restart: expected `{post_restart_routing['workflow_task_queue']}` replayed `{replay_post_restart.get('replayed_state', {}).get('workflow_task_queue')}`",
    f"- Workflow status after signal: `{workflow_after_complete['status']}`",
    f"- Candidate default set: `{task_queue_candidate.get('default_set_id')}`",
    f"- Rollback default set: `{task_queue_rollback.get('default_set_id')}`",
])

(out_dir / "alpha-primary-drill-report.json").write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
(out_dir / "alpha-primary-drill-report.md").write_text("\n".join(markdown) + "\n", encoding="utf-8")
PY
}

if [[ "$SKIP_STACK" != "1" ]]; then
  if ! curl -fsS "$API_URL/health" >/dev/null 2>&1; then
    echo "[alpha-drill] api gateway unavailable, starting dev stack"
    scripts/dev-stack.sh up
  else
    echo "[alpha-drill] dev stack already reachable"
  fi
fi

wait_for_http "$API_URL/health" "api gateway"

echo "[alpha-drill] running v1 migration and deployment"
mkdir -p "$OUTPUT_DIR_ABS/migration-v1"
FABRIK_WORKFLOW_BUILD_ID="$BUILD_V1" \
  cargo run -p fabrik-cli -- migrate temporal "$REPO_PATH" \
  --deploy \
  --output-dir "$OUTPUT_DIR_ABS/migration-v1" \
  --api-url "$API_URL" \
  --tenant "$TENANT_ID"

extract_report_fields "$OUTPUT_DIR_ABS/migration-v1/migration-report.json" > "$OUTPUT_DIR_ABS/report-fields.json"

extract_field() {
  local key=$1
  python3 - "$OUTPUT_DIR_ABS/report-fields.json" "$key" <<'PY'
import json
import sys
payload = json.load(open(sys.argv[1], "r", encoding="utf-8"))
value = payload.get(sys.argv[2], "")
if value is None:
    value = ""
print(value)
PY
}

REPORT_STATUS="$(extract_field status)"
QUALIFICATION="$(extract_field qualification)"
DEFINITION_ID="$(extract_field definition_id)"
ARTIFACT_HASH="$(extract_field artifact_hash)"
WORKFLOW_TASK_QUEUE="$(extract_field workflow_task_queue)"
ACTIVITY_TASK_QUEUE="$(extract_field activity_task_queue)"

if [[ "$REPORT_STATUS" == "incompatible_blocked" ]]; then
  echo "[alpha-drill] migration blocked" >&2
  exit 1
fi

INSTANCE_ID="alpha-primary-$(python3 - <<'PY'
import uuid
print(str(uuid.uuid4())[:8])
PY
)"

echo "[alpha-drill] triggering workflow definition=$DEFINITION_ID instance=$INSTANCE_ID queue=$WORKFLOW_TASK_QUEUE"
TRIGGER_PAYLOAD="$(python3 - "$TENANT_ID" "$INSTANCE_ID" "$WORKFLOW_TASK_QUEUE" <<'PY'
import json
import sys
tenant_id, instance_id, workflow_task_queue = sys.argv[1:4]
print(json.dumps({
    "tenant_id": tenant_id,
    "instance_id": instance_id,
    "workflow_task_queue": workflow_task_queue,
    "input": ["alice", False],
    "memo": {"region": "eu", "priority": 1},
    "searchAttributes": {"CustomKeywordField": ["vip", "alpha"], "Region": "eu"},
    "request_id": f"trigger-{instance_id}",
}))
PY
)"
json_post "/workflows/${DEFINITION_ID}/trigger" "$TRIGGER_PAYLOAD" "$OUTPUT_DIR_ABS/trigger-response.json"

RUN_ID="$(python3 - "$OUTPUT_DIR_ABS/trigger-response.json" <<'PY'
import json
import sys
print(json.load(open(sys.argv[1], "r", encoding="utf-8"))["run_id"])
PY
)"

python3 - "$OUTPUT_DIR_ABS/meta.json" "$REPO_PATH" "$TENANT_ID" "$DEFINITION_ID" "$INSTANCE_ID" "$RUN_ID" "$WORKFLOW_TASK_QUEUE" "$ARTIFACT_HASH" "$QUALIFICATION" <<'PY'
import json
import sys
_, output_path, path, tenant, definition, instance, run_id, queue, artifact_hash, qualification = sys.argv
with open(output_path, "w", encoding="utf-8") as handle:
    json.dump({
        "repo_path": path,
        "tenant_id": tenant,
        "definition_id": definition,
        "instance_id": instance,
        "run_id": run_id,
        "workflow_task_queue": queue,
        "artifact_hash": artifact_hash,
        "qualification": qualification,
    }, handle, indent=2)
    handle.write("\n")
PY

poll_json_path "/tenants/${TENANT_ID}/workflows/${INSTANCE_ID}" \
  "data.get('run_id') == '${RUN_ID}' and data.get('status') == 'running'" \
  "$OUTPUT_DIR_ABS/workflow-pre-restart.json"

json_get "/tenants/${TENANT_ID}/workflows/${INSTANCE_ID}/routing" "$OUTPUT_DIR_ABS/routing-pre-restart.json"
json_get "/tenants/${TENANT_ID}/workflows/${INSTANCE_ID}/runs/${RUN_ID}/replay" "$OUTPUT_DIR_ABS/replay-pre-restart.json"
json_get "/admin/tenants/${TENANT_ID}/task-queues/workflow/${WORKFLOW_TASK_QUEUE}" "$OUTPUT_DIR_ABS/task-queue-workflow-pre-restart.json"
if [[ -n "$ACTIVITY_TASK_QUEUE" ]]; then
  json_get "/admin/tenants/${TENANT_ID}/task-queues/activity/${ACTIVITY_TASK_QUEUE}" "$OUTPUT_DIR_ABS/task-queue-activity-pre-restart.json"
fi

if [[ "$SKIP_RESTART" != "1" ]]; then
  echo "[alpha-drill] restarting dev stack"
  scripts/dev-stack.sh down
  DEV_STACK_BUILD=0 scripts/dev-stack.sh up
fi

wait_for_http "$API_URL/health" "api gateway after restart"

poll_json_path "/tenants/${TENANT_ID}/workflows/${INSTANCE_ID}" \
  "data.get('run_id') == '${RUN_ID}' and data.get('status') == 'running'" \
  "$OUTPUT_DIR_ABS/workflow-post-restart.json"

json_get "/tenants/${TENANT_ID}/workflows/${INSTANCE_ID}/routing" "$OUTPUT_DIR_ABS/routing-post-restart.json"
json_get "/tenants/${TENANT_ID}/workflows/${INSTANCE_ID}/runs/${RUN_ID}/replay" "$OUTPUT_DIR_ABS/replay-post-restart.json"
if [[ -n "$ACTIVITY_TASK_QUEUE" ]]; then
  json_get "/admin/tenants/${TENANT_ID}/task-queues/activity/${ACTIVITY_TASK_QUEUE}" "$OUTPUT_DIR_ABS/task-queue-activity-post-restart.json"
  ACTIVITY_POLLER_COUNT="$(python3 - "$OUTPUT_DIR_ABS/task-queue-activity-post-restart.json" <<'PY'
import json
import sys
print(len(json.load(open(sys.argv[1], "r", encoding="utf-8")).get("pollers", [])))
PY
)"
  if [[ "$ACTIVITY_POLLER_COUNT" == "0" ]]; then
    echo "[alpha-drill] no activity pollers detected after restart; redeploying v1 worker"
    FABRIK_WORKFLOW_BUILD_ID="$BUILD_V1" \
      cargo run -p fabrik-cli -- migrate temporal "$REPO_PATH" \
      --deploy \
      --output-dir "$OUTPUT_DIR_ABS/migration-v1-redeploy" \
      --api-url "$API_URL" \
      --tenant "$TENANT_ID"
    json_get "/admin/tenants/${TENANT_ID}/task-queues/activity/${ACTIVITY_TASK_QUEUE}" "$OUTPUT_DIR_ABS/task-queue-activity-post-redeploy.json"
  fi
fi

echo "[alpha-drill] signaling approval"
json_post \
  "/tenants/${TENANT_ID}/workflows/${INSTANCE_ID}/signals/approved" \
  '{"payload":true,"request_id":"signal-approved"}' \
  "$OUTPUT_DIR_ABS/signal-response.json"

poll_json_path "/tenants/${TENANT_ID}/workflows/${INSTANCE_ID}" \
  "data.get('run_id') == '${RUN_ID}' and data.get('status') == 'completed'" \
  "$OUTPUT_DIR_ABS/workflow-post-complete.json"

json_get "/tenants/${TENANT_ID}/workflows/${INSTANCE_ID}/routing" "$OUTPUT_DIR_ABS/routing-post-complete.json"
json_get "/tenants/${TENANT_ID}/workflows/${INSTANCE_ID}/runs/${RUN_ID}/replay" "$OUTPUT_DIR_ABS/replay-post-complete.json"
json_get "/tenants/${TENANT_ID}/workflows/${INSTANCE_ID}/runs/${RUN_ID}/history" "$OUTPUT_DIR_ABS/history-post-complete.json"

echo "[alpha-drill] registering candidate workflow build $BUILD_V2"
json_post \
  "/admin/tenants/${TENANT_ID}/task-queues/workflow/${WORKFLOW_TASK_QUEUE}/builds" \
  "$(python3 - "$BUILD_V2" "$ARTIFACT_HASH" <<'PY'
import json
import sys
build_id, artifact_hash = sys.argv[1:3]
print(json.dumps({
    "build_id": build_id,
    "artifact_hashes": [artifact_hash],
    "promote_default": True,
}))
PY
)" \
  "$OUTPUT_DIR_ABS/register-build-v2.json"

json_get "/admin/tenants/${TENANT_ID}/task-queues/workflow/${WORKFLOW_TASK_QUEUE}" "$OUTPUT_DIR_ABS/task-queue-workflow-candidate.json"

echo "[alpha-drill] rolling back default workflow build to $BUILD_V1"
json_post \
  "/admin/tenants/${TENANT_ID}/task-queues/workflow/${WORKFLOW_TASK_QUEUE}/default-set" \
  "{\"set_id\":\"default-${BUILD_V1}\"}" \
  "$OUTPUT_DIR_ABS/rollback-default-set.json"

json_get "/admin/tenants/${TENANT_ID}/task-queues/workflow/${WORKFLOW_TASK_QUEUE}" "$OUTPUT_DIR_ABS/task-queue-workflow-rollback.json"
json_get "/tenants/${TENANT_ID}/workflows/${INSTANCE_ID}/routing" "$OUTPUT_DIR_ABS/routing-post-rollback.json"

summarize_evidence "$OUTPUT_DIR_ABS"

echo "[alpha-drill] completed $(timestamp)"
echo "[alpha-drill] report: $OUTPUT_DIR_ABS/alpha-primary-drill-report.md"
