#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

if ! command -v docker >/dev/null 2>&1; then
  echo "docker is required" >&2
  exit 1
fi

if ! command -v python3 >/dev/null 2>&1; then
  echo "python3 is required" >&2
  exit 1
fi

NAMESPACE="${BENCHMARK_NAMESPACE:-bench-$(date +%Y%m%dt%H%M%S)-$RANDOM}"
NAMESPACE="$(printf '%s' "$NAMESPACE" | tr '[:upper:]' '[:lower:]')"
normalize_db_name() {
  python3 - "$1" <<'PY'
import hashlib
import re
import sys

raw = sys.argv[1].lower()
sanitized = re.sub(r"[^a-z0-9_]+", "_", raw).strip("_") or "fabrik_benchmark"
if len(sanitized) <= 63:
    print(sanitized)
else:
    digest = hashlib.sha1(sanitized.encode("utf-8")).hexdigest()[:12]
    prefix = sanitized[: 63 - 1 - len(digest)].rstrip("_") or "fabrik"
    print(f"{prefix}_{digest}")
PY
}

DB_NAME_RAW="${BENCHMARK_DB_NAME:-fabrik_${NAMESPACE//-/_}}"
DB_NAME="$(normalize_db_name "$DB_NAME_RAW")"
TENANT_ID="${BENCHMARK_TENANT_ID:-$NAMESPACE}"
TASK_QUEUE="${BENCHMARK_TASK_QUEUE:-default}"
WORKFLOW_EVENTS_PARTITION_COUNT="${WORKFLOW_EVENTS_PARTITION_COUNT:-8}"
THROUGHPUT_TOPIC_PARTITION_COUNT="${THROUGHPUT_TOPIC_PARTITION_COUNT:-$WORKFLOW_EVENTS_PARTITION_COUNT}"

partition_csv() {
  python3 - "$1" <<'PY'
import sys
count = int(sys.argv[1])
print(",".join(str(i) for i in range(count)))
PY
}

configure_redpanda_limits() {
  local max_bytes=${1:-8388608}
  docker exec fabrik-redpanda-1 rpk cluster config set kafka_batch_max_bytes "$max_bytes" >/dev/null
}

WORKFLOW_PARTITIONS="${WORKFLOW_PARTITIONS:-$(partition_csv "$WORKFLOW_EVENTS_PARTITION_COUNT")}"
THROUGHPUT_PARTITIONS="${THROUGHPUT_OWNERSHIP_PARTITIONS:-$(partition_csv "$THROUGHPUT_TOPIC_PARTITION_COUNT")}"
THROUGHPUT_OWNERSHIP_PARTITION_ID_OFFSET="${THROUGHPUT_OWNERSHIP_PARTITION_ID_OFFSET:-2000000}"
POSTGRES_URL="postgres://fabrik:fabrik@127.0.0.1:${POSTGRES_HOST_PORT:-55433}/${DB_NAME}"
WORKFLOW_EVENTS_TOPIC="${WORKFLOW_EVENTS_TOPIC:-workflow-events-$NAMESPACE}"
THROUGHPUT_COMMANDS_TOPIC="${THROUGHPUT_COMMANDS_TOPIC:-throughput-commands-$NAMESPACE}"
THROUGHPUT_REPORTS_TOPIC="${THROUGHPUT_REPORTS_TOPIC:-throughput-reports-$NAMESPACE}"
THROUGHPUT_CHANGELOG_TOPIC="${THROUGHPUT_CHANGELOG_TOPIC:-throughput-changelog-$NAMESPACE}"
THROUGHPUT_PROJECTIONS_TOPIC="${THROUGHPUT_PROJECTIONS_TOPIC:-throughput-projections-$NAMESPACE}"
THROUGHPUT_BUCKET="${THROUGHPUT_PAYLOAD_S3_BUCKET:-fabrik-throughput}"
THROUGHPUT_KEY_PREFIX="${THROUGHPUT_PAYLOAD_S3_KEY_PREFIX:-throughput/$NAMESPACE}"
CHECKPOINT_KEY_PREFIX="${THROUGHPUT_CHECKPOINT_KEY_PREFIX:-checkpoints/$NAMESPACE}"
RUN_DIR="${BENCHMARK_RUN_DIR:-target/benchmark-runs/$NAMESPACE}"
LOG_DIR="$RUN_DIR/logs"
STATE_DIR="$RUN_DIR/state"
CHECKPOINT_DIR="$RUN_DIR/checkpoints"
PID_DIR="$RUN_DIR/pids"
FAILOVER_INJECTION_PATH="$RUN_DIR/failover-injection.json"
REPORT_PATH_DEFAULT="target/benchmark-reports/${NAMESPACE}.json"
BUILD_RELEASE="${BUILD_RELEASE_BINARIES:-1}"
KEEP_DATABASE="${KEEP_BENCHMARK_DATABASE:-0}"
KEEP_TOPICS="${KEEP_BENCHMARK_TOPICS:-0}"
KILL_LOCAL_SERVICES="${BENCHMARK_KILL_LOCAL_SERVICES:-1}"

mkdir -p "$LOG_DIR" "$STATE_DIR" "$CHECKPOINT_DIR" "$PID_DIR" target/benchmark-reports

PORTS=()
while IFS= read -r port; do
  PORTS+=("$port")
done < <(python3 - <<'PY'
import socket

def reserve():
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port

for _ in range(11):
    print(reserve())
PY
)

INGEST_PORT="${INGEST_SERVICE_PORT:-${PORTS[0]}}"
MATCHING_PORT="${MATCHING_SERVICE_PORT:-${PORTS[1]}}"
MATCHING_DEBUG_PORT="${MATCHING_DEBUG_PORT:-${PORTS[2]}}"
THROUGHPUT_RUNTIME_PORT="${THROUGHPUT_RUNTIME_PORT:-${PORTS[3]}}"
THROUGHPUT_DEBUG_PORT="${THROUGHPUT_DEBUG_PORT:-${PORTS[4]}}"
THROUGHPUT_PROJECTOR_PORT="${THROUGHPUT_PROJECTOR_PORT:-${PORTS[5]}}"
ACTIVITY_WORKER_SERVICE_PORT="${ACTIVITY_WORKER_SERVICE_PORT:-${PORTS[6]}}"
STREAM_ACTIVITY_WORKER_SERVICE_PORT="${STREAM_ACTIVITY_WORKER_SERVICE_PORT:-${PORTS[7]}}"
TIMER_SERVICE_PORT="${TIMER_SERVICE_PORT:-${PORTS[8]}}"
UNIFIED_RUNTIME_PORT="${UNIFIED_RUNTIME_PORT:-${PORTS[9]}}"
UNIFIED_DEBUG_PORT="${UNIFIED_DEBUG_PORT:-${PORTS[10]}}"

PIDS=()
REPORT_PATH=""
FAILOVER_INJECTOR_PID=""

pid_file_for() {
  printf '%s/%s.pid\n' "$PID_DIR" "$1"
}

collect_service_pids() {
  {
    printf '%s\n' "${PIDS[@]:-}"
    local pid_file
    for pid_file in "$PID_DIR"/*.pid; do
      [[ -f "$pid_file" ]] || continue
      cat "$pid_file"
    done
  } | awk 'NF {print $1}' | sort -u
}

stop_pid() {
  local pid=$1
  if kill -0 "$pid" >/dev/null 2>&1; then
    kill "$pid" >/dev/null 2>&1 || true
  fi
}

stop_services() {
  local pids=()
  while IFS= read -r pid; do
    [[ -n "$pid" ]] || continue
    pids+=("$pid")
  done < <(collect_service_pids)

  local pid
  for pid in "${pids[@]:-}"; do
    stop_pid "$pid"
  done

  local deadline=$((SECONDS + 5))
  while (( SECONDS < deadline )); do
    local any_running=0
    for pid in "${pids[@]:-}"; do
      if kill -0 "$pid" >/dev/null 2>&1; then
        any_running=1
        break
      fi
    done
    if [[ "$any_running" == "0" ]]; then
      break
    fi
    sleep 1
  done

  for pid in "${pids[@]:-}"; do
    if kill -0 "$pid" >/dev/null 2>&1; then
      kill -9 "$pid" >/dev/null 2>&1 || true
    fi
    wait "$pid" >/dev/null 2>&1 || true
  done
  rm -f "$PID_DIR"/*.pid >/dev/null 2>&1 || true
}

cleanup() {
  local exit_code=$?
  if [[ -n "${FAILOVER_INJECTOR_PID:-}" ]] && kill -0 "$FAILOVER_INJECTOR_PID" >/dev/null 2>&1; then
    kill "$FAILOVER_INJECTOR_PID" >/dev/null 2>&1 || true
    wait "$FAILOVER_INJECTOR_PID" >/dev/null 2>&1 || true
  fi
  stop_services

  if [[ $exit_code -ne 0 ]]; then
    echo >&2
    echo "benchmark stack failed; recent logs:" >&2
    for log in "$LOG_DIR"/*.log; do
      [[ -f "$log" ]] || continue
      echo >&2
      echo "==> $log <==" >&2
      tail -n 40 "$log" >&2 || true
    done
  fi

  if [[ $exit_code -eq 0 ]]; then
    echo
    echo "report_path=$REPORT_PATH"
  fi
}
trap cleanup EXIT

wait_for_port() {
  local host=$1
  local port=$2
  local label=$3
  python3 - "$host" "$port" "$label" <<'PY'
import socket
import sys
import time

host, port, label = sys.argv[1], int(sys.argv[2]), sys.argv[3]
deadline = time.time() + 60
while time.time() < deadline:
    sock = socket.socket()
    sock.settimeout(0.25)
    try:
        sock.connect((host, port))
        sock.close()
        sys.exit(0)
    except OSError:
        time.sleep(0.25)
    finally:
        sock.close()

print(f"timed out waiting for {label} on {host}:{port}", file=sys.stderr)
sys.exit(1)
PY
}

wait_for_container_health() {
  local container=$1
  local expected=${2:-healthy}
  local timeout=${3:-60}
  local deadline=$((SECONDS + timeout))
  while (( SECONDS < deadline )); do
    local status
    status="$(docker inspect -f '{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}' "$container" 2>/dev/null || true)"
    if [[ "$status" == "$expected" ]]; then
      return 0
    fi
    sleep 1
  done
  echo "timed out waiting for container $container to become $expected" >&2
  return 1
}

wait_for_postgres_host_ready() {
  local host=${1:-127.0.0.1}
  local port=${2:-${POSTGRES_HOST_PORT:-55433}}
  local timeout=${3:-60}
  python3 - "$host" "$port" "$timeout" <<'PY'
import socket
import struct
import sys
import time

host = sys.argv[1]
port = int(sys.argv[2])
timeout = int(sys.argv[3])
deadline = time.time() + timeout

while time.time() < deadline:
    sock = socket.socket()
    sock.settimeout(1.0)
    try:
        sock.connect((host, port))
        sock.sendall(struct.pack("!II", 8, 80877103))
        response = sock.recv(1)
        if response in (b"S", b"N"):
            sys.exit(0)
    except OSError:
        pass
    finally:
        sock.close()
    time.sleep(1)

print(f"timed out waiting for postgres handshake on {host}:{port}", file=sys.stderr)
sys.exit(1)
PY
}

wait_for_redpanda_ready() {
  local timeout=${1:-60}
  local deadline=$((SECONDS + timeout))
  while (( SECONDS < deadline )); do
    local output
    output="$(docker exec fabrik-redpanda-1 rpk cluster health 2>/dev/null || true)"
    if [[ "$output" == *"Healthy:"*"true"* ]]; then
      return 0
    fi
    sleep 1
  done
  echo "timed out waiting for redpanda cluster health" >&2
  return 1
}

wait_for_topic_partitions() {
  local topic=$1
  local expected_partitions=$2
  local timeout=${3:-60}
  local deadline=$((SECONDS + timeout))
  while (( SECONDS < deadline )); do
    local actual
    actual="$(
      docker exec fabrik-redpanda-1 rpk topic list 2>/dev/null \
        | awk -v topic="$topic" '
            $1 == topic {
              print $2
              found = 1
            }
            END {
              if (!found) {
                exit 1
              }
            }
          '
    )"
    if [[ -n "${actual:-}" && "$actual" == "$expected_partitions" ]]; then
      return 0
    fi
    sleep 1
  done
  echo "timed out waiting for topic $topic to report $expected_partitions partitions" >&2
  return 1
}

purge_stale_benchmark_topics() {
  local topics=()
  while IFS= read -r topic; do
    [[ -n "$topic" ]] || continue
    topics+=("$topic")
  done < <(
      docker exec fabrik-redpanda-1 rpk topic list 2>/dev/null \
        | awk 'NR > 1 {print $1}' \
        | grep -E '^(workflow-events|throughput-(commands|reports|changelog|projections))-(bench-|temporal-compare-)' \
        || true
  )
  if [[ ${#topics[@]} -eq 0 ]]; then
    return 0
  fi
  docker exec fabrik-redpanda-1 sh -lc "rpk topic delete ${topics[*]}" >/dev/null 2>&1 || true
  local deadline=$((SECONDS + 60))
  while (( SECONDS < deadline )); do
    local remaining
    remaining="$(
      docker exec fabrik-redpanda-1 rpk topic list 2>/dev/null \
        | awk 'NR > 1 {print $1}' \
        | grep -Ec '^(workflow-events|throughput-(commands|reports|changelog|projections))-(bench-|temporal-compare-)' \
        || true
    )"
    if [[ "${remaining:-0}" == "0" ]]; then
      return 0
    fi
    sleep 2
  done
  echo "timed out waiting for stale benchmark topics to be purged" >&2
  return 1
}

start_service() {
  local name=$1
  local log_file=$2
  shift 2
  local args=("$log_file" "$@")
  local pid
  pid="$(
    python3 - "${args[@]}" <<'PY'
import os
import subprocess
import sys

log_file = sys.argv[1]
argv = sys.argv[2:]
separator = argv.index("--")
env_pairs = argv[:separator]
command = argv[separator + 1 :]

child_env = os.environ.copy()
for pair in env_pairs:
    key, value = pair.split("=", 1)
    child_env[key] = value

with open(log_file, "ab", buffering=0) as log:
    proc = subprocess.Popen(
        command,
        stdin=subprocess.DEVNULL,
        stdout=log,
        stderr=subprocess.STDOUT,
        env=child_env,
        start_new_session=True,
        close_fds=True,
    )

print(proc.pid)
PY
  )"
  PIDS+=("$pid")
  printf '%s\n' "$pid" >"$(pid_file_for "$name")"
}

stop_service_by_name() {
  local name=$1
  local grace_seconds=${2:-10}
  local pid_file
  pid_file="$(pid_file_for "$name")"
  if [[ ! -f "$pid_file" ]]; then
    return 0
  fi
  local pid
  pid="$(cat "$pid_file")"
  if (( grace_seconds > 0 )); then
    stop_pid "$pid"
    local deadline=$((SECONDS + grace_seconds))
    while kill -0 "$pid" >/dev/null 2>&1 && (( SECONDS < deadline )); do
      sleep 1
    done
  fi
  if kill -0 "$pid" >/dev/null 2>&1; then
    kill -9 "$pid" >/dev/null 2>&1 || true
  fi
  wait "$pid" >/dev/null 2>&1 || true
  rm -f "$pid_file"
}

now_ms() {
  python3 - <<'PY'
import time
print(int(time.time() * 1000))
PY
}

write_failover_record() {
  local status=$1
  local delay_ms=${2:-0}
  local stop_requested_at_ms=${3:-}
  local stop_completed_at_ms=${4:-}
  local restart_started_at_ms=${5:-}
  local restart_ready_at_ms=${6:-}
  local error_message=${7:-}
  python3 - "$FAILOVER_INJECTION_PATH" \
    "$status" \
    "$delay_ms" \
    "$stop_requested_at_ms" \
    "$stop_completed_at_ms" \
    "$restart_started_at_ms" \
    "$restart_ready_at_ms" \
    "$error_message" <<'PY'
import json
import sys

path, status, delay_ms, stop_requested_at_ms, stop_completed_at_ms, restart_started_at_ms, restart_ready_at_ms, error_message = sys.argv[1:]

def nullable_int(value: str):
    return None if value == "" else int(value)

stop_requested = nullable_int(stop_requested_at_ms)
stop_completed = nullable_int(stop_completed_at_ms)
restart_started = nullable_int(restart_started_at_ms)
restart_ready = nullable_int(restart_ready_at_ms)
downtime_ms = None
if stop_requested is not None and restart_ready is not None:
    downtime_ms = max(0, restart_ready - stop_requested)

payload = {
    "status": status,
    "delay_ms": int(delay_ms),
    "stop_requested_at_ms": stop_requested,
    "stop_completed_at_ms": stop_completed,
    "restart_started_at_ms": restart_started,
    "restart_ready_at_ms": restart_ready,
    "downtime_ms": downtime_ms,
    "error": error_message or None,
}

with open(path, "w", encoding="utf-8") as handle:
    json.dump(payload, handle, indent=2, sort_keys=True)
PY
}

refresh_failover_reports() {
  [[ -f "$FAILOVER_INJECTION_PATH" ]] || return 0
  [[ -n "${REPORT_PATH:-}" ]] || return 0
  python3 - "$REPORT_PATH" "$FAILOVER_INJECTION_PATH" <<'PY'
import json
import sys
from pathlib import Path

report_path = Path(sys.argv[1])
failover = json.loads(Path(sys.argv[2]).read_text(encoding="utf-8"))

def fmt(value):
    return "n/a" if value is None else str(value)

def update_json(path: Path):
    if not path.exists():
        return
    payload = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(payload, dict) and "scenarios" in payload:
        for scenario in payload.get("scenarios", []):
            scenario["failover_injection"] = failover
    elif isinstance(payload, dict):
        payload["failover_injection"] = failover
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")

def update_txt(path: Path):
    if not path.exists():
        return
    values = {
        "failover_status": failover.get("status"),
        "failover_delay_ms": failover.get("delay_ms"),
        "failover_stop_requested_at_ms": failover.get("stop_requested_at_ms"),
        "failover_restart_ready_at_ms": failover.get("restart_ready_at_ms"),
        "failover_downtime_ms": failover.get("downtime_ms"),
        "failover_error": failover.get("error") or "none",
    }
    lines = path.read_text(encoding="utf-8").splitlines()
    out = []
    seen = set()
    for line in lines:
        if "=" in line:
            key = line.split("=", 1)[0]
            if key in values:
                out.append(f"{key}={fmt(values[key])}")
                seen.add(key)
                continue
        out.append(line)
    for key, value in values.items():
        if key not in seen:
            out.append(f"{key}={fmt(value)}")
    path.write_text("\n".join(out) + "\n", encoding="utf-8")

paths = [report_path]
if report_path.exists():
    stem = report_path.stem
    for candidate in report_path.parent.glob(f"{stem}-*.json"):
        paths.append(candidate)
    for path in paths:
        update_json(path)
    txt_paths = [report_path.with_suffix(".txt")]
    txt_paths.extend(report_path.parent.glob(f"{stem}-*.txt"))
    for path in txt_paths:
        update_txt(path)
PY
}

stop_existing_local_services() {
  if [[ "$KILL_LOCAL_SERVICES" != "1" ]]; then
    return 0
  fi
  local patterns=(
    'target/release/matching-service'
    'target/release/ingest-service'
    'target/release/throughput-runtime'
    'target/release/throughput-projector'
    'target/release/timer-service'
    'target/release/unified-runtime'
    'target/release/activity-worker-service'
    'target/release/benchmark-runner'
  )
  for pattern in "${patterns[@]}"; do
    pkill -9 -f "$pattern" >/dev/null 2>&1 || true
  done
}

has_runner_arg() {
  local flag=$1
  shift
  for arg in "$@"; do
    if [[ "$arg" == "$flag" ]]; then
      return 0
    fi
  done
  return 1
}

runner_arg_value() {
  local flag=$1
  shift
  local args=("$@")
  local i
  for ((i = 0; i < ${#args[@]}; i++)); do
    if [[ "${args[$i]}" == "$flag" ]] && (( i + 1 < ${#args[@]} )); then
      printf '%s\n' "${args[$((i + 1))]}"
      return 0
    fi
  done
  return 1
}

echo "namespace=$NAMESPACE"
echo "db_name=$DB_NAME"
echo "run_dir=$RUN_DIR"
echo "logs_dir=$LOG_DIR"

echo "[isolated-benchmark] stopping existing local fabrik services"
stop_existing_local_services

echo "[isolated-benchmark] starting docker infra"
docker compose up -d redpanda postgres minio minio-init >/dev/null

echo "[isolated-benchmark] waiting for redpanda"
wait_for_redpanda_ready 90
echo "[isolated-benchmark] configuring redpanda limits"
configure_redpanda_limits "${REDPANDA_KAFKA_BATCH_MAX_BYTES:-8388608}"
echo "[isolated-benchmark] purging stale benchmark topics"
purge_stale_benchmark_topics
echo "[isolated-benchmark] waiting for postgres"
wait_for_container_health fabrik-postgres-1 healthy 90
wait_for_postgres_host_ready 127.0.0.1 "${POSTGRES_HOST_PORT:-55433}" 90

echo "[isolated-benchmark] waiting for minio"
until curl -fsS "http://127.0.0.1:${MINIO_API_PORT:-9000}/minio/health/live" >/dev/null; do
  sleep 1
done

echo "[isolated-benchmark] preparing database and topics"
docker exec fabrik-postgres-1 psql -U postgres -Atc \
  "SELECT 1 FROM pg_database WHERE datname = '$DB_NAME'" | grep -q 1 \
  || docker exec fabrik-postgres-1 psql -U postgres -c "CREATE DATABASE $DB_NAME OWNER fabrik;" >/dev/null
wait_for_postgres_host_ready 127.0.0.1 "${POSTGRES_HOST_PORT:-55433}" 30

for topic in \
  "$WORKFLOW_EVENTS_TOPIC:$WORKFLOW_EVENTS_PARTITION_COUNT" \
  "$THROUGHPUT_COMMANDS_TOPIC:$THROUGHPUT_TOPIC_PARTITION_COUNT" \
  "$THROUGHPUT_REPORTS_TOPIC:$THROUGHPUT_TOPIC_PARTITION_COUNT" \
  "$THROUGHPUT_CHANGELOG_TOPIC:$THROUGHPUT_TOPIC_PARTITION_COUNT" \
  "$THROUGHPUT_PROJECTIONS_TOPIC:$THROUGHPUT_TOPIC_PARTITION_COUNT"
do
  name="${topic%%:*}"
  partitions="${topic##*:}"
  topic_ready=0
  for _attempt in 1 2 3 4 5; do
    docker exec fabrik-redpanda-1 rpk topic create "$name" -p "$partitions" -r 1 >/dev/null 2>&1 || true
    if wait_for_topic_partitions "$name" "$partitions" 15; then
      topic_ready=1
      break
    fi
    sleep 2
  done
  if [[ "$topic_ready" != "1" ]]; then
    echo "failed to provision topic $name with $partitions partitions" >&2
    exit 1
  fi
done

if [[ "$BUILD_RELEASE" == "1" ]]; then
  echo "[isolated-benchmark] building release binaries"
  cargo build --release \
    -p benchmark-runner \
    -p ingest-service \
    -p matching-service \
    -p unified-runtime \
    -p throughput-runtime \
    -p throughput-projector \
    -p timer-service \
    -p activity-worker-service >/dev/null
fi

RUNNER_ARGS=("$@")
if [[ ${#RUNNER_ARGS[@]} -eq 0 ]]; then
  RUNNER_ARGS=(--suite streaming --profile target --worker-count 8)
fi
RUNNER_SUITE="$(runner_arg_value --suite "${RUNNER_ARGS[@]}" || true)"
RUNNER_SCENARIO_TAG="$(runner_arg_value --scenario-tag "${RUNNER_ARGS[@]}" || true)"
FAILOVER_INJECTION_DELAY_SECS="${FAILOVER_INJECTION_DELAY_SECS:-1}"
FAILOVER_INJECTION_DELAY_MS="$(
  python3 - "$FAILOVER_INJECTION_DELAY_SECS" <<'PY'
import sys
print(int(float(sys.argv[1]) * 1000))
PY
)"

COMMON_ENV=(
  "POSTGRES_URL=$POSTGRES_URL"
  "REDPANDA_BROKERS=localhost:${REDPANDA_HOST_PORT:-29092}"
  "WORKFLOW_EVENTS_TOPIC=$WORKFLOW_EVENTS_TOPIC"
  "WORKFLOW_EVENTS_PARTITIONS=$WORKFLOW_EVENTS_PARTITION_COUNT"
  "THROUGHPUT_COMMANDS_TOPIC=$THROUGHPUT_COMMANDS_TOPIC"
  "THROUGHPUT_REPORTS_TOPIC=$THROUGHPUT_REPORTS_TOPIC"
  "THROUGHPUT_CHANGELOG_TOPIC=$THROUGHPUT_CHANGELOG_TOPIC"
  "THROUGHPUT_PROJECTIONS_TOPIC=$THROUGHPUT_PROJECTIONS_TOPIC"
  "THROUGHPUT_PARTITIONS=$THROUGHPUT_TOPIC_PARTITION_COUNT"
  "THROUGHPUT_PAYLOAD_STORE=s3"
  "THROUGHPUT_PAYLOAD_S3_BUCKET=$THROUGHPUT_BUCKET"
  "THROUGHPUT_PAYLOAD_S3_REGION=us-east-1"
  "THROUGHPUT_PAYLOAD_S3_ENDPOINT=http://127.0.0.1:${MINIO_API_PORT:-9000}"
  "THROUGHPUT_PAYLOAD_S3_ACCESS_KEY_ID=${MINIO_ROOT_USER:-minioadmin}"
  "THROUGHPUT_PAYLOAD_S3_SECRET_ACCESS_KEY=${MINIO_ROOT_PASSWORD:-minioadmin}"
  "THROUGHPUT_PAYLOAD_S3_FORCE_PATH_STYLE=true"
  "THROUGHPUT_PAYLOAD_S3_KEY_PREFIX=$THROUGHPUT_KEY_PREFIX"
  "THROUGHPUT_CHECKPOINT_KEY_PREFIX=$CHECKPOINT_KEY_PREFIX"
  "THROUGHPUT_LOCAL_STATE_DIR=$STATE_DIR"
  "THROUGHPUT_CHECKPOINT_DIR=$CHECKPOINT_DIR"
  "UNIFIED_RUNTIME_STATE_DIR=$RUN_DIR/unified-runtime-state"
  "WORKFLOW_PARTITIONS=$WORKFLOW_PARTITIONS"
  "THROUGHPUT_OWNERSHIP_PARTITION_ID_OFFSET=$THROUGHPUT_OWNERSHIP_PARTITION_ID_OFFSET"
  "THROUGHPUT_MAX_AGGREGATION_GROUPS=${THROUGHPUT_MAX_AGGREGATION_GROUPS:-32}"
  "THROUGHPUT_POLL_MAX_TASKS=${THROUGHPUT_POLL_MAX_TASKS:-32}"
  "THROUGHPUT_REPORT_APPLY_BATCH_SIZE=${THROUGHPUT_REPORT_APPLY_BATCH_SIZE:-64}"
  "THROUGHPUT_CHANGELOG_PUBLISH_BATCH_SIZE=${THROUGHPUT_CHANGELOG_PUBLISH_BATCH_SIZE:-128}"
  "THROUGHPUT_PROJECTION_PUBLISH_BATCH_SIZE=${THROUGHPUT_PROJECTION_PUBLISH_BATCH_SIZE:-128}"
  "THROUGHPUT_NATIVE_STREAM_V2_ENGINE=${THROUGHPUT_NATIVE_STREAM_V2_ENGINE:-true}"
  "THROUGHPUT_MAX_ACTIVE_CHUNKS_PER_BATCH=${THROUGHPUT_MAX_ACTIVE_CHUNKS_PER_BATCH:-1024}"
  "THROUGHPUT_GROUPING_CHUNK_THRESHOLD=${THROUGHPUT_GROUPING_CHUNK_THRESHOLD:-16}"
  "THROUGHPUT_TARGET_CHUNKS_PER_GROUP=${THROUGHPUT_TARGET_CHUNKS_PER_GROUP:-16}"
  "RUST_LOG=${RUST_LOG:-warn}"
)
OWNERSHIP_ENV=(
  "THROUGHPUT_OWNERSHIP_PARTITIONS=$THROUGHPUT_PARTITIONS"
)
if [[ "$RUNNER_SUITE" == "stream-v2-failover" ]]; then
  OWNERSHIP_ENV=(
    "THROUGHPUT_RUNTIME_CAPACITY=$THROUGHPUT_TOPIC_PARTITION_COUNT"
    "THROUGHPUT_OWNERSHIP_MEMBER_HEARTBEAT_TTL_SECONDS=${THROUGHPUT_OWNERSHIP_MEMBER_HEARTBEAT_TTL_SECONDS:-2}"
    "THROUGHPUT_OWNERSHIP_ASSIGNMENT_POLL_INTERVAL_SECONDS=${THROUGHPUT_OWNERSHIP_ASSIGNMENT_POLL_INTERVAL_SECONDS:-1}"
    "THROUGHPUT_OWNERSHIP_REBALANCE_INTERVAL_SECONDS=${THROUGHPUT_OWNERSHIP_REBALANCE_INTERVAL_SECONDS:-1}"
    "THROUGHPUT_OWNERSHIP_LEASE_TTL_SECONDS=${THROUGHPUT_OWNERSHIP_LEASE_TTL_SECONDS:-2}"
    "THROUGHPUT_OWNERSHIP_RENEW_INTERVAL_SECONDS=${THROUGHPUT_OWNERSHIP_RENEW_INTERVAL_SECONDS:-1}"
  )
fi

start_throughput_runtime_service() {
  echo "[isolated-benchmark] starting throughput-runtime"
  start_service throughput-runtime "$LOG_DIR/throughput-runtime.log" \
    "${COMMON_ENV[@]}" \
    "${OWNERSHIP_ENV[@]}" \
    "THROUGHPUT_RUNTIME_PORT=$THROUGHPUT_RUNTIME_PORT" \
    "THROUGHPUT_DEBUG_PORT=$THROUGHPUT_DEBUG_PORT" \
    -- \
    target/release/throughput-runtime
  wait_for_port 127.0.0.1 "$THROUGHPUT_RUNTIME_PORT" "throughput-runtime"
  wait_for_port 127.0.0.1 "$THROUGHPUT_DEBUG_PORT" "throughput-runtime-debug"
}

start_failover_injector() {
  write_failover_record scheduled "$FAILOVER_INJECTION_DELAY_MS"
  (
    local stop_requested_at_ms=""
    local stop_completed_at_ms=""
    local restart_started_at_ms=""
    local restart_ready_at_ms=""
    local error_message=""
    sleep "$FAILOVER_INJECTION_DELAY_SECS"
    stop_requested_at_ms="$(now_ms)"
    write_failover_record in_progress \
      "$FAILOVER_INJECTION_DELAY_MS" \
      "$stop_requested_at_ms"
    if ! stop_service_by_name throughput-runtime 0; then
      error_message="failed to stop throughput-runtime"
      write_failover_record failed \
        "$FAILOVER_INJECTION_DELAY_MS" \
        "$stop_requested_at_ms" \
        "$stop_completed_at_ms" \
        "$restart_started_at_ms" \
        "$restart_ready_at_ms" \
        "$error_message"
      exit 1
    fi
    stop_completed_at_ms="$(now_ms)"
    restart_started_at_ms="$stop_completed_at_ms"
    if ! start_throughput_runtime_service; then
      error_message="failed to restart throughput-runtime"
      write_failover_record failed \
        "$FAILOVER_INJECTION_DELAY_MS" \
        "$stop_requested_at_ms" \
        "$stop_completed_at_ms" \
        "$restart_started_at_ms" \
        "$restart_ready_at_ms" \
        "$error_message"
      exit 1
    fi
    restart_ready_at_ms="$(now_ms)"
    write_failover_record completed \
      "$FAILOVER_INJECTION_DELAY_MS" \
      "$stop_requested_at_ms" \
      "$stop_completed_at_ms" \
      "$restart_started_at_ms" \
      "$restart_ready_at_ms"
  ) &
  FAILOVER_INJECTOR_PID=$!
}

runner_args_without_output_and_scenario() {
  FILTERED_RUNNER_ARGS=()
  local index=0
  while (( index < ${#RUNNER_ARGS[@]} )); do
    case "${RUNNER_ARGS[$index]}" in
      --output|--scenario-tag)
        ((index += 2))
        ;;
      *)
        FILTERED_RUNNER_ARGS+=("${RUNNER_ARGS[$index]}")
        ((index += 1))
        ;;
    esac
  done
}

run_benchmark_runner() {
  local -a extra_env=()
  while (( $# > 0 )); do
    if [[ "$1" == "--" ]]; then
      shift
      break
    fi
    extra_env+=("$1")
    shift
  done
  local -a args=("$@")
  echo "[isolated-benchmark] running benchmark-runner"
  if (( ${#extra_env[@]} > 0 )); then
    env \
      "${COMMON_ENV[@]}" \
      "INGEST_SERVICE_URL=http://127.0.0.1:$INGEST_PORT" \
      "THROUGHPUT_DEBUG_URL=http://127.0.0.1:$THROUGHPUT_DEBUG_PORT" \
      "THROUGHPUT_PROJECTOR_URL=http://127.0.0.1:$THROUGHPUT_PROJECTOR_PORT" \
      "UNIFIED_RUNTIME_DEBUG_URL=http://127.0.0.1:$UNIFIED_DEBUG_PORT" \
      "${extra_env[@]}" \
      target/release/benchmark-runner "${args[@]}"
  else
    env \
      "${COMMON_ENV[@]}" \
      "INGEST_SERVICE_URL=http://127.0.0.1:$INGEST_PORT" \
      "THROUGHPUT_DEBUG_URL=http://127.0.0.1:$THROUGHPUT_DEBUG_PORT" \
      "THROUGHPUT_PROJECTOR_URL=http://127.0.0.1:$THROUGHPUT_PROJECTOR_PORT" \
      "UNIFIED_RUNTIME_DEBUG_URL=http://127.0.0.1:$UNIFIED_DEBUG_PORT" \
      target/release/benchmark-runner "${args[@]}"
  fi
}

materialize_isolated_scenario_report() {
  local temp_suite_output=$1
  local final_suite_output=$2
  python3 - "$temp_suite_output" "$final_suite_output" <<'PY'
import json
import shutil
import sys
from pathlib import Path

temp_suite_output = Path(sys.argv[1])
final_suite_output = Path(sys.argv[2])
suite = json.loads(temp_suite_output.read_text(encoding="utf-8"))
scenario = suite["scenarios"][0]["scenario"]
extension = temp_suite_output.suffix.lstrip(".") or "json"
source_report = temp_suite_output.parent / f"{temp_suite_output.stem}-{scenario}.{extension}"
dest_report = final_suite_output.parent / f"{final_suite_output.stem}-{scenario}.{extension}"
dest_report.parent.mkdir(parents=True, exist_ok=True)
shutil.copy2(source_report, dest_report)
source_summary = source_report.with_suffix(".txt")
if source_summary.exists():
    shutil.copy2(source_summary, dest_report.with_suffix(".txt"))
print(dest_report)
PY
}

write_failover_suite_report() {
  local suite_output=$1
  shift
  python3 - "$suite_output" "$@" <<'PY'
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

suite_output = Path(sys.argv[1])
scenario_paths = [Path(value) for value in sys.argv[2:]]
scenarios = [
    json.loads(path.read_text(encoding="utf-8"))
    for path in scenario_paths
]
payload = {
    "suite": "stream-v2-failover",
    "profile": scenarios[0]["profile"] if scenarios else "unknown",
    "generated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
    "scenarios": scenarios,
}
suite_output.parent.mkdir(parents=True, exist_ok=True)
suite_output.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
PY
}

run_failover_suite_isolated() {
  local final_output=$1
  local requested_tag="${RUNNER_SCENARIO_TAG:-}"
  runner_args_without_output_and_scenario
  local -a scenario_tags=()
  if [[ -n "$requested_tag" ]]; then
    scenario_tags=("$requested_tag")
  else
    scenario_tags=("owner-restart" "owner-restart-retry-cancel")
  fi
  local -a scenario_report_paths=()
  local scenario_tag temp_output scenario_report
  for scenario_tag in "${scenario_tags[@]}"; do
    rm -f "$FAILOVER_INJECTION_PATH"
    temp_output="$RUN_DIR/${scenario_tag}.json"
    REPORT_PATH="$temp_output"
    echo "[isolated-benchmark] scheduling throughput-runtime failover injection for $scenario_tag"
    start_failover_injector
    run_benchmark_runner \
      "BENCHMARK_FAILOVER_INJECTION_PATH=$FAILOVER_INJECTION_PATH" \
      -- \
      "${FILTERED_RUNNER_ARGS[@]}" \
      --output "$temp_output" \
      --scenario-tag "$scenario_tag"
    if [[ -n "${FAILOVER_INJECTOR_PID:-}" ]]; then
      wait "$FAILOVER_INJECTOR_PID"
      refresh_failover_reports
      FAILOVER_INJECTOR_PID=""
    fi
    scenario_report="$(materialize_isolated_scenario_report "$temp_output" "$final_output")"
    echo "scenario_report_path=$scenario_report"
    scenario_report_paths+=("$scenario_report")
  done
  write_failover_suite_report "$final_output" "${scenario_report_paths[@]}"
  REPORT_PATH="$final_output"
  echo "suite_report_path=$final_output"
}

echo "[isolated-benchmark] starting ingest-service"
start_service ingest-service "$LOG_DIR/ingest-service.log" \
  "${COMMON_ENV[@]}" \
  "INGEST_SERVICE_PORT=$INGEST_PORT" \
  -- \
  target/release/ingest-service
wait_for_port 127.0.0.1 "$INGEST_PORT" "ingest-service"

EXECUTION_MODE="durable"
for ((i = 0; i < ${#RUNNER_ARGS[@]}; i++)); do
  if [[ "${RUNNER_ARGS[$i]}" == "--execution-mode" ]] && (( i + 1 < ${#RUNNER_ARGS[@]} )); then
    EXECUTION_MODE="${RUNNER_ARGS[$((i + 1))]}"
    break
  fi
done
WORKER_CONCURRENCY_DEFAULT="$(runner_arg_value --worker-count "${RUNNER_ARGS[@]}" || true)"
if [[ -z "$WORKER_CONCURRENCY_DEFAULT" ]]; then
  WORKER_CONCURRENCY_DEFAULT=8
fi

echo "[isolated-benchmark] starting unified-runtime"
start_service unified-runtime "$LOG_DIR/unified-runtime.log" \
  "${COMMON_ENV[@]}" \
  "UNIFIED_RUNTIME_PORT=$UNIFIED_RUNTIME_PORT" \
  "UNIFIED_DEBUG_PORT=$UNIFIED_DEBUG_PORT" \
  -- \
  target/release/unified-runtime
wait_for_port 127.0.0.1 "$UNIFIED_RUNTIME_PORT" "unified-runtime"
wait_for_port 127.0.0.1 "$UNIFIED_DEBUG_PORT" "unified-runtime-debug"

echo "[isolated-benchmark] starting timer-service"
start_service timer-service "$LOG_DIR/timer-service.log" \
  "${COMMON_ENV[@]}" \
  "TIMER_SERVICE_PORT=$TIMER_SERVICE_PORT" \
  -- \
  target/release/timer-service
wait_for_port 127.0.0.1 "$TIMER_SERVICE_PORT" "timer-service"

if [[ "$EXECUTION_MODE" == "unified" ]]; then
  echo "[isolated-benchmark] starting activity-worker-service (unified)"
  start_service activity-worker-service-unified "$LOG_DIR/activity-worker-service-unified.log" \
    "${COMMON_ENV[@]}" \
    "ACTIVITY_WORKER_SERVICE_PORT=$ACTIVITY_WORKER_SERVICE_PORT" \
    "UNIFIED_RUNTIME_ENDPOINT=http://127.0.0.1:$UNIFIED_RUNTIME_PORT" \
    "MATCHING_SERVICE_ENDPOINT=http://127.0.0.1:$UNIFIED_RUNTIME_PORT" \
    "BULK_ACTIVITY_ENDPOINT=http://127.0.0.1:$UNIFIED_RUNTIME_PORT" \
    "ACTIVITY_WORKER_TENANT_ID=$TENANT_ID" \
    "ACTIVITY_TASK_QUEUE=$TASK_QUEUE" \
    "ACTIVITY_WORKER_CONCURRENCY=${ACTIVITY_WORKER_CONCURRENCY:-$WORKER_CONCURRENCY_DEFAULT}" \
    "ACTIVITY_POLL_MAX_TASKS=${ACTIVITY_POLL_MAX_TASKS:-32}" \
    "ACTIVITY_BULK_POLL_MAX_TASKS=${ACTIVITY_BULK_POLL_MAX_TASKS:-32}" \
    "ACTIVITY_ENABLE_BULK_LANES=${ACTIVITY_ENABLE_BULK_LANES:-false}" \
    -- \
    target/release/activity-worker-service
elif [[ "$EXECUTION_MODE" == "throughput" ]]; then
  start_throughput_runtime_service

  echo "[isolated-benchmark] starting throughput-projector"
  start_service throughput-projector "$LOG_DIR/throughput-projector.log" \
    "${COMMON_ENV[@]}" \
    "THROUGHPUT_PROJECTOR_PORT=$THROUGHPUT_PROJECTOR_PORT" \
    -- \
    target/release/throughput-projector
  wait_for_port 127.0.0.1 "$THROUGHPUT_PROJECTOR_PORT" "throughput-projector"

  echo "[isolated-benchmark] starting activity-worker-service (pg-v1/unified)"
  start_service activity-worker-service "$LOG_DIR/activity-worker-service-pg-v1.log" \
    "${COMMON_ENV[@]}" \
    "ACTIVITY_WORKER_SERVICE_PORT=$ACTIVITY_WORKER_SERVICE_PORT" \
    "UNIFIED_RUNTIME_ENDPOINT=http://127.0.0.1:$UNIFIED_RUNTIME_PORT" \
    "MATCHING_SERVICE_ENDPOINT=http://127.0.0.1:$UNIFIED_RUNTIME_PORT" \
    "BULK_ACTIVITY_ENDPOINT=http://127.0.0.1:$UNIFIED_RUNTIME_PORT" \
    "ACTIVITY_WORKER_TENANT_ID=$TENANT_ID" \
    "ACTIVITY_TASK_QUEUE=$TASK_QUEUE" \
    "ACTIVITY_WORKER_CONCURRENCY=${ACTIVITY_WORKER_CONCURRENCY:-$WORKER_CONCURRENCY_DEFAULT}" \
    "ACTIVITY_BULK_POLL_MAX_TASKS=${ACTIVITY_BULK_POLL_MAX_TASKS:-32}" \
    -- \
    target/release/activity-worker-service

  echo "[isolated-benchmark] starting activity-worker-service (stream-v2)"
  start_service activity-worker-service-stream-v2 "$LOG_DIR/activity-worker-service-stream-v2.log" \
    "${COMMON_ENV[@]}" \
    "ACTIVITY_WORKER_SERVICE_PORT=$STREAM_ACTIVITY_WORKER_SERVICE_PORT" \
    "UNIFIED_RUNTIME_ENDPOINT=http://127.0.0.1:$UNIFIED_RUNTIME_PORT" \
    "MATCHING_SERVICE_ENDPOINT=http://127.0.0.1:$UNIFIED_RUNTIME_PORT" \
    "BULK_ACTIVITY_ENDPOINT=http://127.0.0.1:$THROUGHPUT_RUNTIME_PORT" \
    "ACTIVITY_WORKER_TENANT_ID=$TENANT_ID" \
    "ACTIVITY_TASK_QUEUE=$TASK_QUEUE" \
    "ACTIVITY_WORKER_CONCURRENCY=${STREAM_ACTIVITY_WORKER_CONCURRENCY:-$WORKER_CONCURRENCY_DEFAULT}" \
    "ACTIVITY_BULK_POLL_MAX_TASKS=${STREAM_ACTIVITY_BULK_POLL_MAX_TASKS:-32}" \
    -- \
    target/release/activity-worker-service
else
  echo "[isolated-benchmark] starting matching-service"
  start_service matching-service "$LOG_DIR/matching-service.log" \
    "${COMMON_ENV[@]}" \
    "MATCHING_SERVICE_PORT=$MATCHING_PORT" \
    "MATCHING_DEBUG_PORT=$MATCHING_DEBUG_PORT" \
    -- \
    target/release/matching-service
  wait_for_port 127.0.0.1 "$MATCHING_PORT" "matching-service"

  start_throughput_runtime_service

  echo "[isolated-benchmark] starting throughput-projector"
  start_service throughput-projector "$LOG_DIR/throughput-projector.log" \
    "${COMMON_ENV[@]}" \
    "THROUGHPUT_PROJECTOR_PORT=$THROUGHPUT_PROJECTOR_PORT" \
    -- \
    target/release/throughput-projector
  wait_for_port 127.0.0.1 "$THROUGHPUT_PROJECTOR_PORT" "throughput-projector"

  echo "[isolated-benchmark] starting activity-worker-service (pg-v1/matching)"
  start_service activity-worker-service "$LOG_DIR/activity-worker-service-pg-v1.log" \
    "${COMMON_ENV[@]}" \
    "ACTIVITY_WORKER_SERVICE_PORT=$ACTIVITY_WORKER_SERVICE_PORT" \
    "MATCHING_SERVICE_ENDPOINT=http://127.0.0.1:$MATCHING_PORT" \
    "BULK_ACTIVITY_ENDPOINT=http://127.0.0.1:$MATCHING_PORT" \
    "ACTIVITY_WORKER_TENANT_ID=$TENANT_ID" \
    "ACTIVITY_TASK_QUEUE=$TASK_QUEUE" \
    "ACTIVITY_WORKER_CONCURRENCY=${ACTIVITY_WORKER_CONCURRENCY:-$WORKER_CONCURRENCY_DEFAULT}" \
    "ACTIVITY_BULK_POLL_MAX_TASKS=${ACTIVITY_BULK_POLL_MAX_TASKS:-32}" \
    -- \
    target/release/activity-worker-service

  echo "[isolated-benchmark] starting activity-worker-service (stream-v2)"
  start_service activity-worker-service-stream-v2 "$LOG_DIR/activity-worker-service-stream-v2.log" \
    "${COMMON_ENV[@]}" \
    "ACTIVITY_WORKER_SERVICE_PORT=$STREAM_ACTIVITY_WORKER_SERVICE_PORT" \
    "BULK_ACTIVITY_ENDPOINT=http://127.0.0.1:$THROUGHPUT_RUNTIME_PORT" \
    "ACTIVITY_WORKER_TENANT_ID=$TENANT_ID" \
    "ACTIVITY_TASK_QUEUE=$TASK_QUEUE" \
    "ACTIVITY_WORKER_CONCURRENCY=${STREAM_ACTIVITY_WORKER_CONCURRENCY:-$WORKER_CONCURRENCY_DEFAULT}" \
    "ACTIVITY_BULK_POLL_MAX_TASKS=${STREAM_ACTIVITY_BULK_POLL_MAX_TASKS:-32}" \
    "ACTIVITY_ENABLE_NORMAL_LANES=false" \
    -- \
    target/release/activity-worker-service
fi
sleep 2

if ! has_runner_arg --tenant-id "${RUNNER_ARGS[@]}"; then
  RUNNER_ARGS+=(--tenant-id "$TENANT_ID")
fi

if ! has_runner_arg --task-queue "${RUNNER_ARGS[@]}"; then
  RUNNER_ARGS+=(--task-queue "$TASK_QUEUE")
fi

if ! has_runner_arg --output "${RUNNER_ARGS[@]}"; then
  RUNNER_ARGS+=(--output "$REPORT_PATH_DEFAULT")
fi

REPORT_PATH="$REPORT_PATH_DEFAULT"
for ((i = 0; i < ${#RUNNER_ARGS[@]}; i++)); do
  if [[ "${RUNNER_ARGS[$i]}" == "--output" ]] && (( i + 1 < ${#RUNNER_ARGS[@]} )); then
    REPORT_PATH="${RUNNER_ARGS[$((i + 1))]}"
    break
  fi
done

cat >"$RUN_DIR/environment.txt" <<EOF
NAMESPACE=$NAMESPACE
DB_NAME=$DB_NAME
TENANT_ID=$TENANT_ID
TASK_QUEUE=$TASK_QUEUE
POSTGRES_URL=$POSTGRES_URL
WORKFLOW_EVENTS_TOPIC=$WORKFLOW_EVENTS_TOPIC
THROUGHPUT_COMMANDS_TOPIC=$THROUGHPUT_COMMANDS_TOPIC
THROUGHPUT_REPORTS_TOPIC=$THROUGHPUT_REPORTS_TOPIC
THROUGHPUT_CHANGELOG_TOPIC=$THROUGHPUT_CHANGELOG_TOPIC
THROUGHPUT_PROJECTIONS_TOPIC=$THROUGHPUT_PROJECTIONS_TOPIC
INGEST_SERVICE_URL=http://127.0.0.1:$INGEST_PORT
THROUGHPUT_DEBUG_URL=http://127.0.0.1:$THROUGHPUT_DEBUG_PORT
THROUGHPUT_PROJECTOR_URL=http://127.0.0.1:$THROUGHPUT_PROJECTOR_PORT
TIMER_SERVICE_URL=http://127.0.0.1:$TIMER_SERVICE_PORT
UNIFIED_RUNTIME_DEBUG_URL=http://127.0.0.1:$UNIFIED_DEBUG_PORT
EOF

if [[ "$RUNNER_SUITE" == "stream-v2-failover" ]]; then
  run_failover_suite_isolated "$REPORT_PATH"
else
  run_benchmark_runner -- "${RUNNER_ARGS[@]}"
fi

if [[ -n "${FAILOVER_INJECTOR_PID:-}" ]]; then
  wait "$FAILOVER_INJECTOR_PID"
  refresh_failover_reports
fi

stop_services

if [[ "$KEEP_DATABASE" != "1" ]]; then
  docker exec fabrik-postgres-1 psql -U postgres -c "DROP DATABASE IF EXISTS $DB_NAME WITH (FORCE);" >/dev/null || true
fi

if [[ "$KEEP_TOPICS" != "1" ]]; then
  for topic in \
    "$WORKFLOW_EVENTS_TOPIC" \
    "$THROUGHPUT_COMMANDS_TOPIC" \
    "$THROUGHPUT_REPORTS_TOPIC" \
    "$THROUGHPUT_CHANGELOG_TOPIC" \
    "$THROUGHPUT_PROJECTIONS_TOPIC"
  do
    docker exec fabrik-redpanda-1 rpk topic delete "$topic" >/dev/null 2>&1 || true
  done
fi
