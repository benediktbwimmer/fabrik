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
DB_NAME="${BENCHMARK_DB_NAME:-fabrik_${NAMESPACE//-/_}}"
DB_NAME="$(printf '%s' "$DB_NAME" | tr '[:upper:]' '[:lower:]')"
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

WORKFLOW_PARTITIONS="${WORKFLOW_PARTITIONS:-$(partition_csv "$WORKFLOW_EVENTS_PARTITION_COUNT")}"
THROUGHPUT_PARTITIONS="${THROUGHPUT_OWNERSHIP_PARTITIONS:-$(partition_csv "$THROUGHPUT_TOPIC_PARTITION_COUNT")}"
POSTGRES_URL="postgres://fabrik:fabrik@localhost:${POSTGRES_HOST_PORT:-55433}/${DB_NAME}"
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
REPORT_PATH_DEFAULT="target/benchmark-reports/${NAMESPACE}.json"
BUILD_RELEASE="${BUILD_RELEASE_BINARIES:-1}"
KEEP_DATABASE="${KEEP_BENCHMARK_DATABASE:-0}"
KEEP_TOPICS="${KEEP_BENCHMARK_TOPICS:-0}"
KILL_LOCAL_SERVICES="${BENCHMARK_KILL_LOCAL_SERVICES:-1}"

mkdir -p "$LOG_DIR" "$STATE_DIR" "$CHECKPOINT_DIR" target/benchmark-reports

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

for _ in range(9):
    print(reserve())
PY
)

INGEST_PORT="${INGEST_SERVICE_PORT:-${PORTS[0]}}"
EXECUTOR_PORT="${EXECUTOR_SERVICE_PORT:-${PORTS[1]}}"
MATCHING_PORT="${MATCHING_SERVICE_PORT:-${PORTS[2]}}"
MATCHING_DEBUG_PORT="${MATCHING_DEBUG_PORT:-${PORTS[3]}}"
THROUGHPUT_RUNTIME_PORT="${THROUGHPUT_RUNTIME_PORT:-${PORTS[4]}}"
THROUGHPUT_DEBUG_PORT="${THROUGHPUT_DEBUG_PORT:-${PORTS[5]}}"
THROUGHPUT_PROJECTOR_PORT="${THROUGHPUT_PROJECTOR_PORT:-${PORTS[6]}}"
ACTIVITY_WORKER_SERVICE_PORT="${ACTIVITY_WORKER_SERVICE_PORT:-${PORTS[7]}}"
STREAM_ACTIVITY_WORKER_SERVICE_PORT="${STREAM_ACTIVITY_WORKER_SERVICE_PORT:-${PORTS[8]}}"

PIDS=()
REPORT_PATH=""

stop_services() {
  local pid
  for pid in "${PIDS[@]:-}"; do
    if kill -0 "$pid" >/dev/null 2>&1; then
      kill "$pid" >/dev/null 2>&1 || true
    fi
  done

  local deadline=$((SECONDS + 5))
  while (( SECONDS < deadline )); do
    local any_running=0
    for pid in "${PIDS[@]:-}"; do
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

  for pid in "${PIDS[@]:-}"; do
    if kill -0 "$pid" >/dev/null 2>&1; then
      kill -9 "$pid" >/dev/null 2>&1 || true
    fi
    wait "$pid" >/dev/null 2>&1 || true
  done
}

cleanup() {
  local exit_code=$?
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
      | grep -E '^(workflow-events|throughput-(commands|reports|changelog|projections))-bench-' \
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
        | grep -Ec '^(workflow-events|throughput-(commands|reports|changelog|projections))-bench-' \
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
  (
    while (($#)); do
      if [[ "$1" == "--" ]]; then
        shift
        break
      fi
      export "$1"
      shift
    done
    exec "$@"
  ) >"$log_file" 2>&1 &
  local pid=$!
  PIDS+=("$pid")
}

stop_existing_local_services() {
  if [[ "$KILL_LOCAL_SERVICES" != "1" ]]; then
    return 0
  fi
  local patterns=(
    'target/release/matching-service'
    'target/release/executor-service'
    'target/release/ingest-service'
    'target/release/throughput-runtime'
    'target/release/throughput-projector'
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
echo "[isolated-benchmark] purging stale benchmark topics"
purge_stale_benchmark_topics
echo "[isolated-benchmark] waiting for postgres"
wait_for_container_health fabrik-postgres-1 healthy 90

echo "[isolated-benchmark] waiting for minio"
until curl -fsS "http://127.0.0.1:${MINIO_API_PORT:-9000}/minio/health/live" >/dev/null; do
  sleep 1
done

echo "[isolated-benchmark] preparing database and topics"
docker exec fabrik-postgres-1 psql -U postgres -Atc \
  "SELECT 1 FROM pg_database WHERE datname = '$DB_NAME'" | grep -q 1 \
  || docker exec fabrik-postgres-1 psql -U postgres -c "CREATE DATABASE $DB_NAME OWNER fabrik;" >/dev/null

for topic in \
  "$WORKFLOW_EVENTS_TOPIC:$WORKFLOW_EVENTS_PARTITION_COUNT" \
  "$THROUGHPUT_COMMANDS_TOPIC:$THROUGHPUT_TOPIC_PARTITION_COUNT" \
  "$THROUGHPUT_REPORTS_TOPIC:$THROUGHPUT_TOPIC_PARTITION_COUNT" \
  "$THROUGHPUT_CHANGELOG_TOPIC:$THROUGHPUT_TOPIC_PARTITION_COUNT" \
  "$THROUGHPUT_PROJECTIONS_TOPIC:$THROUGHPUT_TOPIC_PARTITION_COUNT"
do
  name="${topic%%:*}"
  partitions="${topic##*:}"
  docker exec fabrik-redpanda-1 rpk topic create "$name" -p "$partitions" -r 1 >/dev/null 2>&1 || true
  wait_for_topic_partitions "$name" "$partitions" 60
done

if [[ "$BUILD_RELEASE" == "1" ]]; then
  echo "[isolated-benchmark] building release binaries"
  cargo build --release \
    -p benchmark-runner \
    -p ingest-service \
    -p matching-service \
    -p executor-service \
    -p throughput-runtime \
    -p throughput-projector \
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

echo "[isolated-benchmark] starting matching-service"
start_service matching-service "$LOG_DIR/matching-service.log" \
  "${COMMON_ENV[@]}" \
  "MATCHING_SERVICE_PORT=$MATCHING_PORT" \
  "MATCHING_DEBUG_PORT=$MATCHING_DEBUG_PORT" \
  -- \
  target/release/matching-service
wait_for_port 127.0.0.1 "$MATCHING_PORT" "matching-service"

echo "[isolated-benchmark] starting executor-service"
start_service executor-service "$LOG_DIR/executor-service.log" \
  "${COMMON_ENV[@]}" \
  "EXECUTOR_SERVICE_PORT=$EXECUTOR_PORT" \
  "MATCHING_SERVICE_ENDPOINT=http://127.0.0.1:$MATCHING_PORT" \
  -- \
  target/release/executor-service
wait_for_port 127.0.0.1 "$EXECUTOR_PORT" "executor-service"

echo "[isolated-benchmark] starting ingest-service"
start_service ingest-service "$LOG_DIR/ingest-service.log" \
  "${COMMON_ENV[@]}" \
  "INGEST_SERVICE_PORT=$INGEST_PORT" \
  -- \
  target/release/ingest-service
wait_for_port 127.0.0.1 "$INGEST_PORT" "ingest-service"

echo "[isolated-benchmark] starting throughput-runtime"
start_service throughput-runtime "$LOG_DIR/throughput-runtime.log" \
  "${COMMON_ENV[@]}" \
  "THROUGHPUT_RUNTIME_PORT=$THROUGHPUT_RUNTIME_PORT" \
  "THROUGHPUT_DEBUG_PORT=$THROUGHPUT_DEBUG_PORT" \
  -- \
  target/release/throughput-runtime
wait_for_port 127.0.0.1 "$THROUGHPUT_RUNTIME_PORT" "throughput-runtime"
wait_for_port 127.0.0.1 "$THROUGHPUT_DEBUG_PORT" "throughput-runtime-debug"

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
  "ACTIVITY_WORKER_CONCURRENCY=${ACTIVITY_WORKER_CONCURRENCY:-8}" \
  "ACTIVITY_BULK_POLL_MAX_TASKS=${ACTIVITY_BULK_POLL_MAX_TASKS:-32}" \
  -- \
  target/release/activity-worker-service

echo "[isolated-benchmark] starting activity-worker-service (stream-v2)"
start_service activity-worker-service-stream-v2 "$LOG_DIR/activity-worker-service-stream-v2.log" \
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
sleep 2

RUNNER_ARGS=("$@")
if [[ ${#RUNNER_ARGS[@]} -eq 0 ]]; then
  RUNNER_ARGS=(--suite streaming --profile target --worker-count 8)
fi

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
EXECUTOR_SERVICE_URL=http://127.0.0.1:$EXECUTOR_PORT
THROUGHPUT_DEBUG_URL=http://127.0.0.1:$THROUGHPUT_DEBUG_PORT
THROUGHPUT_PROJECTOR_URL=http://127.0.0.1:$THROUGHPUT_PROJECTOR_PORT
EOF

echo "[isolated-benchmark] running benchmark-runner"
env \
  "${COMMON_ENV[@]}" \
  "INGEST_SERVICE_URL=http://127.0.0.1:$INGEST_PORT" \
  "EXECUTOR_SERVICE_URL=http://127.0.0.1:$EXECUTOR_PORT" \
  "THROUGHPUT_DEBUG_URL=http://127.0.0.1:$THROUGHPUT_DEBUG_PORT" \
  "THROUGHPUT_PROJECTOR_URL=http://127.0.0.1:$THROUGHPUT_PROJECTOR_PORT" \
  target/release/benchmark-runner "${RUNNER_ARGS[@]}"

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
