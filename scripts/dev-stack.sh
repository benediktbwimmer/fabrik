#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

COMMAND="${1:-}"
if [[ -z "$COMMAND" ]]; then
  echo "usage: scripts/dev-stack.sh <up|down|status>" >&2
  exit 1
fi
shift || true

STATE_ROOT="${DEV_STACK_DIR:-target/dev-stack}"
LOG_DIR="$STATE_ROOT/logs"
PID_DIR="$STATE_ROOT/pids"
STATE_DIR="$STATE_ROOT/state"
CHECKPOINT_DIR="$STATE_ROOT/checkpoints"
ENV_FILE="$STATE_ROOT/environment.sh"
LOCK_DIR="$STATE_ROOT/lock"

PROFILE="${DEV_STACK_PROFILE:-debug}"
if [[ "$PROFILE" == "release" ]]; then
  PROFILE_FLAG="--release"
  BIN_DIR="target/release"
else
  PROFILE_FLAG=""
  BIN_DIR="target/debug"
fi

POSTGRES_HOST_PORT="${POSTGRES_HOST_PORT:-55433}"
REDPANDA_HOST_PORT="${REDPANDA_HOST_PORT:-29092}"
MINIO_API_PORT="${MINIO_API_PORT:-9000}"
MINIO_CONSOLE_PORT="${MINIO_CONSOLE_PORT:-9001}"

API_GATEWAY_PORT="${API_GATEWAY_PORT:-3000}"
INGEST_PORT="${INGEST_SERVICE_PORT:-3001}"
TIMER_PORT="${TIMER_SERVICE_PORT:-3003}"
QUERY_PORT="${QUERY_SERVICE_PORT:-3005}"
THROUGHPUT_DEBUG_PORT="${THROUGHPUT_DEBUG_PORT:-3006}"
THROUGHPUT_PROJECTOR_PORT="${THROUGHPUT_PROJECTOR_PORT:-3007}"
UNIFIED_DEBUG_PORT="${UNIFIED_DEBUG_PORT:-3008}"
ACTIVITY_WORKER_PORT="${ACTIVITY_WORKER_SERVICE_PORT:-50052}"
THROUGHPUT_RUNTIME_PORT="${THROUGHPUT_RUNTIME_PORT:-50053}"
UNIFIED_RUNTIME_PORT="${UNIFIED_RUNTIME_PORT:-50054}"
STREAM_ACTIVITY_WORKER_PORT="${STREAM_ACTIVITY_WORKER_SERVICE_PORT:-50055}"

DEV_STACK_TENANT_ID="${DEV_STACK_TENANT_ID:-dev}"
DEV_STACK_TASK_QUEUE="${DEV_STACK_TASK_QUEUE:-bulk}"
WORKFLOW_PARTITIONS="${WORKFLOW_PARTITIONS:-0,1,2,3}"
THROUGHPUT_PARTITIONS="${THROUGHPUT_OWNERSHIP_PARTITIONS:-}"
THROUGHPUT_OWNERSHIP_PARTITION_ID_OFFSET="${THROUGHPUT_OWNERSHIP_PARTITION_ID_OFFSET:-2000000}"
POSTGRES_URL="${POSTGRES_URL:-postgres://fabrik:fabrik@localhost:${POSTGRES_HOST_PORT}/fabrik}"
DEV_STACK_SERVICE_RETRIES="${DEV_STACK_SERVICE_RETRIES:-3}"
DEV_STACK_SERVICE_RETRY_DELAY_SECONDS="${DEV_STACK_SERVICE_RETRY_DELAY_SECONDS:-2}"

COMMON_ENV=(
  "POSTGRES_URL=$POSTGRES_URL"
  "REDPANDA_BROKERS=localhost:${REDPANDA_HOST_PORT}"
  "THROUGHPUT_PAYLOAD_STORE=s3"
  "THROUGHPUT_PAYLOAD_S3_BUCKET=${MINIO_THROUGHPUT_BUCKET:-fabrik-throughput}"
  "THROUGHPUT_PAYLOAD_S3_REGION=us-east-1"
  "THROUGHPUT_PAYLOAD_S3_ENDPOINT=http://127.0.0.1:${MINIO_API_PORT}"
  "THROUGHPUT_PAYLOAD_S3_ACCESS_KEY_ID=${MINIO_ROOT_USER:-minioadmin}"
  "THROUGHPUT_PAYLOAD_S3_SECRET_ACCESS_KEY=${MINIO_ROOT_PASSWORD:-minioadmin}"
  "THROUGHPUT_PAYLOAD_S3_FORCE_PATH_STYLE=true"
  "THROUGHPUT_PAYLOAD_S3_KEY_PREFIX=${THROUGHPUT_PAYLOAD_S3_KEY_PREFIX:-throughput/dev-stack}"
  "THROUGHPUT_CHECKPOINT_KEY_PREFIX=${THROUGHPUT_CHECKPOINT_KEY_PREFIX:-checkpoints/dev-stack}"
  "THROUGHPUT_LOCAL_STATE_DIR=$STATE_DIR"
  "THROUGHPUT_CHECKPOINT_DIR=$CHECKPOINT_DIR"
  "THROUGHPUT_OWNERSHIP_PARTITION_ID_OFFSET=$THROUGHPUT_OWNERSHIP_PARTITION_ID_OFFSET"
  "WORKFLOW_PARTITIONS=$WORKFLOW_PARTITIONS"
  "RUST_LOG=${RUST_LOG:-info}"
)

if [[ -n "$THROUGHPUT_PARTITIONS" ]]; then
  COMMON_ENV+=("THROUGHPUT_OWNERSHIP_PARTITIONS=$THROUGHPUT_PARTITIONS")
fi

SERVICES=(
  "unified-runtime"
  "ingest-service"
  "query-service"
  "api-gateway"
  "throughput-runtime"
  "throughput-projector"
  "timer-service"
  "activity-worker-service"
  "activity-worker-service-stream-v2"
)

mkdir -p "$LOG_DIR" "$PID_DIR" "$STATE_DIR" "$CHECKPOINT_DIR"

require_bin() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "$1 is required" >&2
    exit 1
  fi
}

acquire_lock() {
  local waited=0
  while ! mkdir "$LOCK_DIR" 2>/dev/null; do
    if (( waited == 0 )); then
      echo "[dev-stack] waiting for existing dev-stack command to finish" >&2
    fi
    waited=1
    sleep 1
  done
  trap release_lock EXIT
}

release_lock() {
  rmdir "$LOCK_DIR" 2>/dev/null || true
}

wait_for_port() {
  local host=$1
  local port=$2
  local label=$3
  local timeout_seconds=${4:-90}
  python3 - "$host" "$port" "$label" "$timeout_seconds" <<'PY'
import socket
import sys
import time

host, port, label, timeout_seconds = sys.argv[1], int(sys.argv[2]), sys.argv[3], float(sys.argv[4])
deadline = time.time() + timeout_seconds
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
  local timeout=${3:-90}
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

wait_for_redpanda_ready() {
  local timeout=${1:-90}
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

wait_for_minio() {
  local timeout=${1:-90}
  local deadline=$((SECONDS + timeout))
  while (( SECONDS < deadline )); do
    if curl -fsS "http://127.0.0.1:${MINIO_API_PORT}/minio/health/live" >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done
  echo "timed out waiting for minio" >&2
  return 1
}

pidfile_for() {
  printf '%s/%s.pid' "$PID_DIR" "$1"
}

service_ports_for() {
  case "$1" in
    unified-runtime)
      printf '%s\n%s\n' "$UNIFIED_RUNTIME_PORT" "$UNIFIED_DEBUG_PORT"
      ;;
    ingest-service)
      printf '%s\n' "$INGEST_PORT"
      ;;
    query-service)
      printf '%s\n' "$QUERY_PORT"
      ;;
    api-gateway)
      printf '%s\n' "$API_GATEWAY_PORT"
      ;;
    throughput-runtime)
      printf '%s\n%s\n' "$THROUGHPUT_RUNTIME_PORT" "$THROUGHPUT_DEBUG_PORT"
      ;;
    throughput-projector)
      printf '%s\n' "$THROUGHPUT_PROJECTOR_PORT"
      ;;
    timer-service)
      printf '%s\n' "$TIMER_PORT"
      ;;
    activity-worker-service)
      printf '%s\n' "$ACTIVITY_WORKER_PORT"
      ;;
    activity-worker-service-stream-v2)
      printf '%s\n' "$STREAM_ACTIVITY_WORKER_PORT"
      ;;
    *)
      ;;
  esac
}

service_binary_for() {
  case "$1" in
    activity-worker-service|activity-worker-service-stream-v2)
      printf '%s/activity-worker-service' "$BIN_DIR"
      ;;
    *)
      printf '%s/%s' "$BIN_DIR" "$1"
      ;;
  esac
}

matching_service_pids() {
  local binary
  binary="$(service_binary_for "$1")"
  pgrep -f "$binary" || true
}

terminate_pid() {
  local pid=$1
  if ! kill -0 "$pid" >/dev/null 2>&1; then
    return 0
  fi
  kill "$pid" >/dev/null 2>&1 || true
  local deadline=$((SECONDS + 5))
  while kill -0 "$pid" >/dev/null 2>&1 && (( SECONDS < deadline )); do
    sleep 1
  done
  if kill -0 "$pid" >/dev/null 2>&1; then
    kill -9 "$pid" >/dev/null 2>&1 || true
  fi
}

stop_service_processes() {
  local name=$1
  local pid
  while read -r pid; do
    [[ -n "$pid" ]] || continue
    terminate_pid "$pid"
  done < <(matching_service_pids "$name")
}

stop_service_ports() {
  local name=$1
  local port
  while read -r port; do
    [[ -n "$port" ]] || continue
    local pid
    while read -r pid; do
      [[ -n "$pid" ]] || continue
      terminate_pid "$pid"
    done < <(lsof -tiTCP:"$port" -sTCP:LISTEN 2>/dev/null || true)
  done < <(service_ports_for "$name")
}

service_running() {
  local name=$1
  local pidfile
  pidfile="$(pidfile_for "$name")"
  [[ -f "$pidfile" ]] || return 1
  local pid
  pid="$(cat "$pidfile" 2>/dev/null || true)"
  [[ -n "$pid" ]] || return 1
  kill -0 "$pid" >/dev/null 2>&1 || return 1
  local binary
  binary="$(service_binary_for "$name")"
  ps -p "$pid" -o command= 2>/dev/null | grep -F "$binary" >/dev/null 2>&1
}

stop_service() {
  local name=$1
  local pidfile
  pidfile="$(pidfile_for "$name")"
  if ! [[ -f "$pidfile" ]]; then
    stop_service_processes "$name"
    stop_service_ports "$name"
    return 0
  fi
  local pid
  pid="$(cat "$pidfile" 2>/dev/null || true)"
  if [[ -n "$pid" ]]; then
    terminate_pid "$pid"
  fi
  stop_service_processes "$name"
  stop_service_ports "$name"
  rm -f "$pidfile"
}

assert_service_alive() {
  local name=$1
  local pidfile=$2
  local log_file=$3
  if ! service_running "$name"; then
    echo "[dev-stack] $name exited during startup" >&2
    if [[ -f "$log_file" ]]; then
      tail -n 40 "$log_file" >&2 || true
    fi
    return 1
  fi
}

retry_service_start() {
  local name=$1
  local log_file=$2
  local wait_port=${3:-}
  local extra_ports_csv=${4:-}
  shift 4
  local attempt
  for attempt in $(seq 1 "$DEV_STACK_SERVICE_RETRIES"); do
    if start_service "$name" "$log_file" "$wait_port" "$@"; then
      local ports_ok=1
      if [[ -n "$extra_ports_csv" ]]; then
        local extra_port
        IFS=, read -r -a extra_ports <<<"$extra_ports_csv"
        for extra_port in "${extra_ports[@]}"; do
          [[ -n "$extra_port" ]] || continue
          if ! wait_for_port 127.0.0.1 "$extra_port" "$name:$extra_port"; then
            ports_ok=0
            break
          fi
        done
      fi
      if (( ports_ok == 1 )); then
        return 0
      fi
    fi
    stop_service "$name"
    if (( attempt < DEV_STACK_SERVICE_RETRIES )); then
      echo "[dev-stack] retrying $name startup ($attempt/$DEV_STACK_SERVICE_RETRIES)" >&2
      sleep "$DEV_STACK_SERVICE_RETRY_DELAY_SECONDS"
    fi
  done
  echo "[dev-stack] $name failed to start after $DEV_STACK_SERVICE_RETRIES attempts" >&2
  return 1
}

start_service() {
  local name=$1
  local log_file=$2
  local wait_port=${3:-}
  shift 3
  if service_running "$name"; then
    echo "[dev-stack] $name already running"
    return 0
  fi
  stop_service_processes "$name"
  stop_service_ports "$name"
  local pidfile
  pidfile="$(pidfile_for "$name")"
  local env_vars=()
  while (($#)); do
    if [[ "$1" == "--" ]]; then
      shift
      break
    fi
    env_vars+=("$1")
    shift
  done
  : >"$log_file"
  python3 - "$pidfile" "$log_file" "${env_vars[@]}" -- "$@" <<'PY'
import os
import subprocess
import sys

pidfile = sys.argv[1]
log_file = sys.argv[2]
argv = sys.argv[3:]
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

with open(pidfile, "w", encoding="utf-8") as handle:
    handle.write(f"{proc.pid}\n")
PY
  sleep 1
  assert_service_alive "$name" "$pidfile" "$log_file"
  if [[ -n "$wait_port" ]]; then
    wait_for_port 127.0.0.1 "$wait_port" "$name"
    assert_service_alive "$name" "$pidfile" "$log_file"
  fi
}

write_env_file() {
  cat >"$ENV_FILE" <<EOF
export API_GATEWAY_URL=http://127.0.0.1:${API_GATEWAY_PORT}
export INGEST_SERVICE_URL=http://127.0.0.1:${INGEST_PORT}
export QUERY_SERVICE_URL=http://127.0.0.1:${QUERY_PORT}
export UNIFIED_RUNTIME_ENDPOINT=http://127.0.0.1:${UNIFIED_RUNTIME_PORT}
export UNIFIED_RUNTIME_DEBUG_URL=http://127.0.0.1:${UNIFIED_DEBUG_PORT}
export THROUGHPUT_RUNTIME_ENDPOINT=http://127.0.0.1:${THROUGHPUT_RUNTIME_PORT}
export THROUGHPUT_DEBUG_URL=http://127.0.0.1:${THROUGHPUT_DEBUG_PORT}
export THROUGHPUT_PROJECTOR_URL=http://127.0.0.1:${THROUGHPUT_PROJECTOR_PORT}
export POSTGRES_URL=${POSTGRES_URL}
export REDPANDA_BROKERS=localhost:${REDPANDA_HOST_PORT}
export DEV_STACK_TENANT_ID=${DEV_STACK_TENANT_ID}
export DEV_STACK_TASK_QUEUE=${DEV_STACK_TASK_QUEUE}
export DEV_STACK_DIR=${STATE_ROOT}
EOF
}

build_services() {
  echo "[dev-stack] building ${PROFILE} binaries"
  cargo build ${PROFILE_FLAG:+$PROFILE_FLAG} \
    -p api-gateway \
    -p ingest-service \
    -p unified-runtime \
    -p query-service \
    -p throughput-runtime \
    -p throughput-projector \
    -p timer-service \
    -p activity-worker-service
}

up() {
  require_bin docker
  require_bin python3
  require_bin curl
  require_bin lsof

  echo "[dev-stack] starting docker infra"
  docker compose up -d redpanda postgres minio minio-init >/dev/null
  wait_for_redpanda_ready 90
  docker exec fabrik-redpanda-1 rpk cluster config set write_caching_default false >/dev/null
  wait_for_container_health fabrik-postgres-1 healthy 90
  wait_for_minio 90

  if [[ "${DEV_STACK_BUILD:-1}" == "1" ]]; then
    build_services
  fi

  write_env_file

  echo "[dev-stack] starting unified-runtime"
  retry_service_start \
    unified-runtime \
    "$LOG_DIR/unified-runtime.log" \
    "$UNIFIED_RUNTIME_PORT" \
    "$UNIFIED_DEBUG_PORT" \
    "${COMMON_ENV[@]}" \
    "UNIFIED_RUNTIME_PORT=$UNIFIED_RUNTIME_PORT" \
    "UNIFIED_DEBUG_PORT=$UNIFIED_DEBUG_PORT" \
    "UNIFIED_RUNTIME_STATE_DIR=$STATE_DIR/unified-runtime" \
    -- \
    "$BIN_DIR/unified-runtime"

  echo "[dev-stack] starting ingest-service"
  retry_service_start \
    ingest-service \
    "$LOG_DIR/ingest-service.log" \
    "$INGEST_PORT" \
    "" \
    "${COMMON_ENV[@]}" \
    "INGEST_SERVICE_PORT=$INGEST_PORT" \
    -- \
    "$BIN_DIR/ingest-service"

  echo "[dev-stack] starting query-service"
  retry_service_start \
    query-service \
    "$LOG_DIR/query-service.log" \
    "$QUERY_PORT" \
    "" \
    "${COMMON_ENV[@]}" \
    "QUERY_SERVICE_PORT=$QUERY_PORT" \
    "QUERY_STRONG_QUERY_UNIFIED_URL=http://127.0.0.1:$UNIFIED_DEBUG_PORT" \
    -- \
    "$BIN_DIR/query-service"

  echo "[dev-stack] starting api-gateway"
  retry_service_start \
    api-gateway \
    "$LOG_DIR/api-gateway.log" \
    "$API_GATEWAY_PORT" \
    "" \
    "${COMMON_ENV[@]}" \
    "API_GATEWAY_PORT=$API_GATEWAY_PORT" \
    "INGEST_SERVICE_URL=http://127.0.0.1:$INGEST_PORT" \
    "QUERY_SERVICE_URL=http://127.0.0.1:$QUERY_PORT" \
    -- \
    "$BIN_DIR/api-gateway"

  echo "[dev-stack] starting throughput-runtime"
  retry_service_start \
    throughput-runtime \
    "$LOG_DIR/throughput-runtime.log" \
    "$THROUGHPUT_RUNTIME_PORT" \
    "$THROUGHPUT_DEBUG_PORT" \
    "${COMMON_ENV[@]}" \
    "THROUGHPUT_RUNTIME_PORT=$THROUGHPUT_RUNTIME_PORT" \
    "THROUGHPUT_DEBUG_PORT=$THROUGHPUT_DEBUG_PORT" \
    -- \
    "$BIN_DIR/throughput-runtime"

  echo "[dev-stack] starting throughput-projector"
  retry_service_start \
    throughput-projector \
    "$LOG_DIR/throughput-projector.log" \
    "$THROUGHPUT_PROJECTOR_PORT" \
    "" \
    "${COMMON_ENV[@]}" \
    "THROUGHPUT_PROJECTOR_PORT=$THROUGHPUT_PROJECTOR_PORT" \
    -- \
    "$BIN_DIR/throughput-projector"

  echo "[dev-stack] starting timer-service"
  retry_service_start \
    timer-service \
    "$LOG_DIR/timer-service.log" \
    "$TIMER_PORT" \
    "" \
    "${COMMON_ENV[@]}" \
    "TIMER_SERVICE_PORT=$TIMER_PORT" \
    -- \
    "$BIN_DIR/timer-service"

  echo "[dev-stack] starting activity-worker-service (unified-runtime)"
  retry_service_start \
    activity-worker-service \
    "$LOG_DIR/activity-worker-service.log" \
    "" \
    "" \
    "${COMMON_ENV[@]}" \
    "ACTIVITY_WORKER_SERVICE_PORT=$ACTIVITY_WORKER_PORT" \
    "UNIFIED_RUNTIME_ENDPOINT=http://127.0.0.1:$UNIFIED_RUNTIME_PORT" \
    "BULK_ACTIVITY_ENDPOINT=http://127.0.0.1:$UNIFIED_RUNTIME_PORT" \
    "ACTIVITY_WORKER_TENANT_ID=$DEV_STACK_TENANT_ID" \
    "ACTIVITY_TASK_QUEUE=$DEV_STACK_TASK_QUEUE" \
    -- \
    "$BIN_DIR/activity-worker-service"

  echo "[dev-stack] starting activity-worker-service (stream-v2)"
  retry_service_start \
    activity-worker-service-stream-v2 \
    "$LOG_DIR/activity-worker-service-stream-v2.log" \
    "" \
    "" \
    "${COMMON_ENV[@]}" \
    "ACTIVITY_WORKER_SERVICE_PORT=$STREAM_ACTIVITY_WORKER_PORT" \
    "UNIFIED_RUNTIME_ENDPOINT=http://127.0.0.1:$UNIFIED_RUNTIME_PORT" \
    "BULK_ACTIVITY_ENDPOINT=http://127.0.0.1:$THROUGHPUT_RUNTIME_PORT" \
    "ACTIVITY_WORKER_TENANT_ID=$DEV_STACK_TENANT_ID" \
    "ACTIVITY_TASK_QUEUE=$DEV_STACK_TASK_QUEUE" \
    -- \
    "$BIN_DIR/activity-worker-service"

  echo "[dev-stack] stack is up"
  echo "[dev-stack] env file: $ENV_FILE"
}

down() {
  require_bin lsof
  for service in "${SERVICES[@]}"; do
    stop_service "$service"
  done
  echo "[dev-stack] stopping docker infra"
  docker compose stop redpanda postgres minio minio-init >/dev/null || true
}

status() {
  echo "infra:"
  docker compose ps || true
  echo
  echo "services:"
  for service in "${SERVICES[@]}"; do
    if service_running "$service"; then
      echo "  $service: running (pid $(cat "$(pidfile_for "$service")"))"
    else
      echo "  $service: stopped"
    fi
  done
  echo
  echo "env_file=$ENV_FILE"
}

case "$COMMAND" in
  up)
    acquire_lock
    up
    ;;
  down)
    acquire_lock
    down
    ;;
  status)
    acquire_lock
    status
    ;;
  *)
    echo "unknown command: $COMMAND" >&2
    exit 1
    ;;
esac
