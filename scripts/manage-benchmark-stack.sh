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

usage() {
  cat >&2 <<'EOF'
usage:
  scripts/manage-benchmark-stack.sh start [options]
  scripts/manage-benchmark-stack.sh stop --run-dir <dir>

options:
  --execution-mode <durable|throughput|unified>
  --namespace <value>
  --run-dir <dir>
  --tenant-id <value>
  --task-queue <value>
EOF
  exit 1
}

COMMAND="${1:-}"
if [[ -z "$COMMAND" ]]; then
  usage
fi
shift

EXECUTION_MODE="durable"
NAMESPACE_OVERRIDE=""
RUN_DIR_OVERRIDE=""
TENANT_ID_OVERRIDE=""
TASK_QUEUE_OVERRIDE=""

while (($#)); do
  case "$1" in
    --execution-mode)
      EXECUTION_MODE="$2"
      shift 2
      ;;
    --namespace)
      NAMESPACE_OVERRIDE="$2"
      shift 2
      ;;
    --run-dir)
      RUN_DIR_OVERRIDE="$2"
      shift 2
      ;;
    --tenant-id)
      TENANT_ID_OVERRIDE="$2"
      shift 2
      ;;
    --task-queue)
      TASK_QUEUE_OVERRIDE="$2"
      shift 2
      ;;
    *)
      echo "unknown argument $1" >&2
      usage
      ;;
  esac
done

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

reserve_ports() {
  python3 - <<'PY'
import socket

def reserve():
    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    sock.close()
    return port

for _ in range(12):
    print(reserve())
PY
}

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

stop_existing_local_services() {
  if [[ "${BENCHMARK_KILL_LOCAL_SERVICES:-0}" != "1" ]]; then
    return 0
  fi
  local patterns=(
    'target/release/matching-service'
    'target/release/executor-service'
    'target/release/ingest-service'
    'target/release/throughput-runtime'
    'target/release/throughput-projector'
    'target/release/timer-service'
    'target/release/unified-runtime'
    'target/release/activity-worker-service'
    'target/release/benchmark-runner'
  )
  local pattern
  for pattern in "${patterns[@]}"; do
    pkill -9 -f "$pattern" >/dev/null 2>&1 || true
  done
}

load_env_file() {
  local env_file=$1
  if [[ ! -f "$env_file" ]]; then
    echo "missing environment file: $env_file" >&2
    exit 1
  fi
  set -a
  # shellcheck disable=SC1090
  source "$env_file"
  set +a
}

stop_service_pids() {
  local pid_file=$1
  local pids=()
  if [[ -f "$pid_file" ]]; then
    while IFS= read -r pid; do
      [[ -n "$pid" ]] || continue
      pids+=("$pid")
    done <"$pid_file"
  fi

  local pid
  for pid in "${pids[@]:-}"; do
    if kill -0 "$pid" >/dev/null 2>&1; then
      kill "$pid" >/dev/null 2>&1 || true
    fi
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
}

if [[ "$COMMAND" == "stop" ]]; then
  if [[ -z "$RUN_DIR_OVERRIDE" ]]; then
    echo "--run-dir is required for stop" >&2
    exit 1
  fi
  RUN_DIR="$RUN_DIR_OVERRIDE"
  ENV_FILE="$RUN_DIR/environment.txt"
  PID_FILE="$RUN_DIR/pids.txt"
  load_env_file "$ENV_FILE"
  stop_service_pids "$PID_FILE"

  if [[ "${KEEP_BENCHMARK_DATABASE:-0}" != "1" ]]; then
    docker exec fabrik-postgres-1 psql -U postgres -c "DROP DATABASE IF EXISTS $DB_NAME WITH (FORCE);" >/dev/null || true
  fi

  if [[ "${KEEP_BENCHMARK_TOPICS:-0}" != "1" ]]; then
    local_topics=(
      "$WORKFLOW_EVENTS_TOPIC"
      "$THROUGHPUT_COMMANDS_TOPIC"
      "$THROUGHPUT_REPORTS_TOPIC"
      "$THROUGHPUT_CHANGELOG_TOPIC"
      "$THROUGHPUT_PROJECTIONS_TOPIC"
    )
    docker exec fabrik-redpanda-1 sh -lc "rpk topic delete ${local_topics[*]}" >/dev/null 2>&1 || true
  fi

  echo "stopped_run_dir=$RUN_DIR"
  exit 0
fi

if [[ "$COMMAND" != "start" ]]; then
  usage
fi

NAMESPACE="${NAMESPACE_OVERRIDE:-${BENCHMARK_NAMESPACE:-bench-$(date +%Y%m%dt%H%M%S)-$RANDOM}}"
NAMESPACE="$(printf '%s' "$NAMESPACE" | tr '[:upper:]' '[:lower:]')"
DB_NAME_RAW="${BENCHMARK_DB_NAME:-fabrik_${NAMESPACE//-/_}}"
DB_NAME="$(normalize_db_name "$DB_NAME_RAW")"
TENANT_ID="${TENANT_ID_OVERRIDE:-${BENCHMARK_TENANT_ID:-$NAMESPACE}}"
TASK_QUEUE="${TASK_QUEUE_OVERRIDE:-${BENCHMARK_TASK_QUEUE:-default}}"
WORKFLOW_EVENTS_PARTITION_COUNT="${WORKFLOW_EVENTS_PARTITION_COUNT:-8}"
THROUGHPUT_TOPIC_PARTITION_COUNT="${THROUGHPUT_TOPIC_PARTITION_COUNT:-$WORKFLOW_EVENTS_PARTITION_COUNT}"
WORKFLOW_PARTITIONS="${WORKFLOW_PARTITIONS:-$(partition_csv "$WORKFLOW_EVENTS_PARTITION_COUNT")}"
THROUGHPUT_PARTITIONS="${THROUGHPUT_OWNERSHIP_PARTITIONS:-$(partition_csv "$THROUGHPUT_TOPIC_PARTITION_COUNT")}"
POSTGRES_URL="postgres://fabrik:fabrik@127.0.0.1:${POSTGRES_HOST_PORT:-55433}/${DB_NAME}"
WORKFLOW_EVENTS_TOPIC="${WORKFLOW_EVENTS_TOPIC:-workflow-events-$NAMESPACE}"
THROUGHPUT_COMMANDS_TOPIC="${THROUGHPUT_COMMANDS_TOPIC:-throughput-commands-$NAMESPACE}"
THROUGHPUT_REPORTS_TOPIC="${THROUGHPUT_REPORTS_TOPIC:-throughput-reports-$NAMESPACE}"
THROUGHPUT_CHANGELOG_TOPIC="${THROUGHPUT_CHANGELOG_TOPIC:-throughput-changelog-$NAMESPACE}"
THROUGHPUT_PROJECTIONS_TOPIC="${THROUGHPUT_PROJECTIONS_TOPIC:-throughput-projections-$NAMESPACE}"
THROUGHPUT_BUCKET="${THROUGHPUT_PAYLOAD_S3_BUCKET:-fabrik-throughput}"
THROUGHPUT_KEY_PREFIX="${THROUGHPUT_PAYLOAD_S3_KEY_PREFIX:-throughput/$NAMESPACE}"
CHECKPOINT_KEY_PREFIX="${THROUGHPUT_CHECKPOINT_KEY_PREFIX:-checkpoints/$NAMESPACE}"
RUN_DIR="${RUN_DIR_OVERRIDE:-${BENCHMARK_RUN_DIR:-target/benchmark-stacks/$NAMESPACE}}"
LOG_DIR="$RUN_DIR/logs"
STATE_DIR="$RUN_DIR/state"
CHECKPOINT_DIR="$RUN_DIR/checkpoints"
ENV_FILE="$RUN_DIR/environment.txt"
PID_FILE="$RUN_DIR/pids.txt"
BUILD_RELEASE="${BUILD_RELEASE_BINARIES:-1}"

mkdir -p "$LOG_DIR" "$STATE_DIR" "$CHECKPOINT_DIR"
: >"$PID_FILE"

PORTS=()
while IFS= read -r port; do
  PORTS+=("$port")
done < <(reserve_ports)
INGEST_PORT="${INGEST_SERVICE_PORT:-${PORTS[0]}}"
EXECUTOR_PORT="${EXECUTOR_SERVICE_PORT:-${PORTS[1]}}"
MATCHING_PORT="${MATCHING_SERVICE_PORT:-${PORTS[2]}}"
MATCHING_DEBUG_PORT="${MATCHING_DEBUG_PORT:-${PORTS[3]}}"
THROUGHPUT_RUNTIME_PORT="${THROUGHPUT_RUNTIME_PORT:-${PORTS[4]}}"
THROUGHPUT_DEBUG_PORT="${THROUGHPUT_DEBUG_PORT:-${PORTS[5]}}"
THROUGHPUT_PROJECTOR_PORT="${THROUGHPUT_PROJECTOR_PORT:-${PORTS[6]}}"
ACTIVITY_WORKER_SERVICE_PORT="${ACTIVITY_WORKER_SERVICE_PORT:-${PORTS[7]}}"
STREAM_ACTIVITY_WORKER_SERVICE_PORT="${STREAM_ACTIVITY_WORKER_SERVICE_PORT:-${PORTS[8]}}"
TIMER_SERVICE_PORT="${TIMER_SERVICE_PORT:-${PORTS[9]}}"
UNIFIED_RUNTIME_PORT="${UNIFIED_RUNTIME_PORT:-${PORTS[10]}}"
UNIFIED_DEBUG_PORT="${UNIFIED_DEBUG_PORT:-${PORTS[11]}}"

PIDS=()
START_SUCCEEDED=0

cleanup_failed_start() {
  local exit_code=$?
  if [[ "$START_SUCCEEDED" == "1" ]]; then
    return
  fi
  stop_service_pids "$PID_FILE"
  docker exec fabrik-postgres-1 psql -U postgres -c "DROP DATABASE IF EXISTS $DB_NAME WITH (FORCE);" >/dev/null 2>&1 || true
  docker exec fabrik-redpanda-1 sh -lc \
    "rpk topic delete $WORKFLOW_EVENTS_TOPIC $THROUGHPUT_COMMANDS_TOPIC $THROUGHPUT_REPORTS_TOPIC $THROUGHPUT_CHANGELOG_TOPIC $THROUGHPUT_PROJECTIONS_TOPIC" \
    >/dev/null 2>&1 || true
  exit "$exit_code"
}
trap cleanup_failed_start EXIT

start_service() {
  local log_file=$1
  shift
  (
    while (($#)); do
      if [[ "$1" == "--" ]]; then
        shift
        break
      fi
      export "$1"
      shift
    done
    exec nohup "$@"
  ) >"$log_file" 2>&1 &
  local pid=$!
  PIDS+=("$pid")
  echo "$pid" >>"$PID_FILE"
}

stop_existing_local_services

echo "[benchmark-stack] namespace=$NAMESPACE"
echo "[benchmark-stack] run_dir=$RUN_DIR"
echo "[benchmark-stack] execution_mode=$EXECUTION_MODE"

echo "[benchmark-stack] starting docker infra"
docker compose up -d redpanda postgres minio minio-init >/dev/null

echo "[benchmark-stack] waiting for redpanda"
wait_for_redpanda_ready 90
echo "[benchmark-stack] configuring redpanda limits"
configure_redpanda_limits "${REDPANDA_KAFKA_BATCH_MAX_BYTES:-8388608}"
echo "[benchmark-stack] waiting for postgres"
wait_for_container_health fabrik-postgres-1 healthy 90
wait_for_postgres_host_ready 127.0.0.1 "${POSTGRES_HOST_PORT:-55433}" 90
echo "[benchmark-stack] waiting for minio"
until curl -fsS "http://127.0.0.1:${MINIO_API_PORT:-9000}/minio/health/live" >/dev/null; do
  sleep 1
done

echo "[benchmark-stack] preparing database and topics"
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
  echo "[benchmark-stack] building release binaries"
  cargo build --release \
    -p benchmark-runner \
    -p ingest-service \
    -p matching-service \
    -p executor-service \
    -p unified-runtime \
    -p throughput-runtime \
    -p throughput-projector \
    -p timer-service \
    -p activity-worker-service >/dev/null
fi

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
  "THROUGHPUT_OWNERSHIP_PARTITIONS=$THROUGHPUT_PARTITIONS"
  "THROUGHPUT_MAX_AGGREGATION_GROUPS=${THROUGHPUT_MAX_AGGREGATION_GROUPS:-32}"
  "THROUGHPUT_POLL_MAX_TASKS=${THROUGHPUT_POLL_MAX_TASKS:-32}"
  "THROUGHPUT_REPORT_APPLY_BATCH_SIZE=${THROUGHPUT_REPORT_APPLY_BATCH_SIZE:-64}"
  "THROUGHPUT_CHANGELOG_PUBLISH_BATCH_SIZE=${THROUGHPUT_CHANGELOG_PUBLISH_BATCH_SIZE:-128}"
  "THROUGHPUT_PROJECTION_PUBLISH_BATCH_SIZE=${THROUGHPUT_PROJECTION_PUBLISH_BATCH_SIZE:-128}"
  "THROUGHPUT_MAX_ACTIVE_CHUNKS_PER_BATCH=${THROUGHPUT_MAX_ACTIVE_CHUNKS_PER_BATCH:-1024}"
  "THROUGHPUT_GROUPING_CHUNK_THRESHOLD=${THROUGHPUT_GROUPING_CHUNK_THRESHOLD:-16}"
  "THROUGHPUT_TARGET_CHUNKS_PER_GROUP=${THROUGHPUT_TARGET_CHUNKS_PER_GROUP:-16}"
  "RUST_LOG=${RUST_LOG:-warn}"
)

echo "[benchmark-stack] starting ingest-service"
start_service "$LOG_DIR/ingest-service.log" \
  "${COMMON_ENV[@]}" \
  "INGEST_SERVICE_PORT=$INGEST_PORT" \
  -- \
  target/release/ingest-service
wait_for_port 127.0.0.1 "$INGEST_PORT" "ingest-service"

if [[ "$EXECUTION_MODE" == "unified" ]]; then
  echo "[benchmark-stack] starting unified-runtime"
  start_service "$LOG_DIR/unified-runtime.log" \
    "${COMMON_ENV[@]}" \
    "UNIFIED_RUNTIME_PORT=$UNIFIED_RUNTIME_PORT" \
    "UNIFIED_DEBUG_PORT=$UNIFIED_DEBUG_PORT" \
    -- \
    target/release/unified-runtime
  wait_for_port 127.0.0.1 "$UNIFIED_RUNTIME_PORT" "unified-runtime"
  wait_for_port 127.0.0.1 "$UNIFIED_DEBUG_PORT" "unified-runtime-debug"

  echo "[benchmark-stack] starting timer-service"
  start_service "$LOG_DIR/timer-service.log" \
    "${COMMON_ENV[@]}" \
    "TIMER_SERVICE_PORT=$TIMER_SERVICE_PORT" \
    -- \
    target/release/timer-service
  wait_for_port 127.0.0.1 "$TIMER_SERVICE_PORT" "timer-service"

  echo "[benchmark-stack] starting activity-worker-service (unified)"
  start_service "$LOG_DIR/activity-worker-service-unified.log" \
    "${COMMON_ENV[@]}" \
    "ACTIVITY_WORKER_SERVICE_PORT=$ACTIVITY_WORKER_SERVICE_PORT" \
    "MATCHING_SERVICE_ENDPOINT=http://127.0.0.1:$UNIFIED_RUNTIME_PORT" \
    "BULK_ACTIVITY_ENDPOINT=http://127.0.0.1:$UNIFIED_RUNTIME_PORT" \
    "ACTIVITY_WORKER_TENANT_ID=$TENANT_ID" \
    "ACTIVITY_TASK_QUEUE=$TASK_QUEUE" \
    "ACTIVITY_WORKER_CONCURRENCY=${ACTIVITY_WORKER_CONCURRENCY:-8}" \
    "ACTIVITY_POLL_MAX_TASKS=${ACTIVITY_POLL_MAX_TASKS:-32}" \
    "ACTIVITY_BULK_POLL_MAX_TASKS=${ACTIVITY_BULK_POLL_MAX_TASKS:-32}" \
    "ACTIVITY_ENABLE_BULK_LANES=${ACTIVITY_ENABLE_BULK_LANES:-false}" \
    -- \
    target/release/activity-worker-service
else
  echo "[benchmark-stack] starting matching-service"
  start_service "$LOG_DIR/matching-service.log" \
    "${COMMON_ENV[@]}" \
    "MATCHING_SERVICE_PORT=$MATCHING_PORT" \
    "MATCHING_DEBUG_PORT=$MATCHING_DEBUG_PORT" \
    -- \
    target/release/matching-service
  wait_for_port 127.0.0.1 "$MATCHING_PORT" "matching-service"

  echo "[benchmark-stack] starting executor-service"
  start_service "$LOG_DIR/executor-service.log" \
    "${COMMON_ENV[@]}" \
    "EXECUTOR_SERVICE_PORT=$EXECUTOR_PORT" \
    "MATCHING_SERVICE_ENDPOINT=http://127.0.0.1:$MATCHING_PORT" \
    -- \
    target/release/executor-service
  wait_for_port 127.0.0.1 "$EXECUTOR_PORT" "executor-service"

  echo "[benchmark-stack] starting throughput-runtime"
  start_service "$LOG_DIR/throughput-runtime.log" \
    "${COMMON_ENV[@]}" \
    "THROUGHPUT_RUNTIME_PORT=$THROUGHPUT_RUNTIME_PORT" \
    "THROUGHPUT_DEBUG_PORT=$THROUGHPUT_DEBUG_PORT" \
    -- \
    target/release/throughput-runtime
  wait_for_port 127.0.0.1 "$THROUGHPUT_RUNTIME_PORT" "throughput-runtime"
  wait_for_port 127.0.0.1 "$THROUGHPUT_DEBUG_PORT" "throughput-runtime-debug"

  echo "[benchmark-stack] starting throughput-projector"
  start_service "$LOG_DIR/throughput-projector.log" \
    "${COMMON_ENV[@]}" \
    "THROUGHPUT_PROJECTOR_PORT=$THROUGHPUT_PROJECTOR_PORT" \
    -- \
    target/release/throughput-projector
  wait_for_port 127.0.0.1 "$THROUGHPUT_PROJECTOR_PORT" "throughput-projector"

  echo "[benchmark-stack] starting timer-service"
  start_service "$LOG_DIR/timer-service.log" \
    "${COMMON_ENV[@]}" \
    "TIMER_SERVICE_PORT=$TIMER_SERVICE_PORT" \
    -- \
    target/release/timer-service
  wait_for_port 127.0.0.1 "$TIMER_SERVICE_PORT" "timer-service"

  echo "[benchmark-stack] starting activity-worker-service (pg-v1/matching)"
  start_service "$LOG_DIR/activity-worker-service-pg-v1.log" \
    "${COMMON_ENV[@]}" \
    "ACTIVITY_WORKER_SERVICE_PORT=$ACTIVITY_WORKER_SERVICE_PORT" \
    "MATCHING_SERVICE_ENDPOINT=http://127.0.0.1:$MATCHING_PORT" \
    "BULK_ACTIVITY_ENDPOINT=http://127.0.0.1:$MATCHING_PORT" \
    "ACTIVITY_WORKER_TENANT_ID=$TENANT_ID" \
    "ACTIVITY_TASK_QUEUE=$TASK_QUEUE" \
    "ACTIVITY_WORKER_CONCURRENCY=${ACTIVITY_WORKER_CONCURRENCY:-8}" \
    "ACTIVITY_BULK_POLL_MAX_TASKS=${ACTIVITY_BULK_POLL_MAX_TASKS:-32}" \
    -- \
    target/release/activity-worker-service

  echo "[benchmark-stack] starting activity-worker-service (stream-v2)"
  start_service "$LOG_DIR/activity-worker-service-stream-v2.log" \
    "${COMMON_ENV[@]}" \
    "ACTIVITY_WORKER_SERVICE_PORT=$STREAM_ACTIVITY_WORKER_SERVICE_PORT" \
    "MATCHING_SERVICE_ENDPOINT=http://127.0.0.1:$MATCHING_PORT" \
    "BULK_ACTIVITY_ENDPOINT=http://127.0.0.1:$THROUGHPUT_RUNTIME_PORT" \
    "ACTIVITY_WORKER_TENANT_ID=$TENANT_ID" \
    "ACTIVITY_TASK_QUEUE=$TASK_QUEUE" \
    "ACTIVITY_WORKER_CONCURRENCY=${STREAM_ACTIVITY_WORKER_CONCURRENCY:-8}" \
    "ACTIVITY_BULK_POLL_MAX_TASKS=${STREAM_ACTIVITY_BULK_POLL_MAX_TASKS:-32}" \
    -- \
    target/release/activity-worker-service
fi

cat >"$ENV_FILE" <<EOF
NAMESPACE=$NAMESPACE
DB_NAME=$DB_NAME
TENANT_ID=$TENANT_ID
TASK_QUEUE=$TASK_QUEUE
RUN_DIR=$RUN_DIR
LOG_DIR=$LOG_DIR
POSTGRES_URL=$POSTGRES_URL
WORKFLOW_EVENTS_TOPIC=$WORKFLOW_EVENTS_TOPIC
THROUGHPUT_COMMANDS_TOPIC=$THROUGHPUT_COMMANDS_TOPIC
THROUGHPUT_REPORTS_TOPIC=$THROUGHPUT_REPORTS_TOPIC
THROUGHPUT_CHANGELOG_TOPIC=$THROUGHPUT_CHANGELOG_TOPIC
THROUGHPUT_PROJECTIONS_TOPIC=$THROUGHPUT_PROJECTIONS_TOPIC
INGEST_SERVICE_URL=http://127.0.0.1:$INGEST_PORT
EXECUTOR_SERVICE_URL=http://127.0.0.1:$EXECUTOR_PORT
THROUGHPUT_DEBUG_URL=http://127.0.0.1:$THROUGHPUT_DEBUG_PORT
THROUGHPUT_PROJECTOR_URL=http://127.0.0.1:$THROUGHPUT_PROJECTOR_PORT
TIMER_SERVICE_URL=http://127.0.0.1:$TIMER_SERVICE_PORT
UNIFIED_RUNTIME_DEBUG_URL=http://127.0.0.1:$UNIFIED_DEBUG_PORT
KEEP_BENCHMARK_DATABASE=${KEEP_BENCHMARK_DATABASE:-0}
KEEP_BENCHMARK_TOPICS=${KEEP_BENCHMARK_TOPICS:-0}
EOF

START_SUCCEEDED=1
trap - EXIT

echo "run_dir=$RUN_DIR"
echo "env_file=$ENV_FILE"
