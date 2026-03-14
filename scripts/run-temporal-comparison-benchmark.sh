#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

if ! command -v docker >/dev/null 2>&1; then
  echo "docker is required" >&2
  exit 1
fi

if ! command -v node >/dev/null 2>&1; then
  echo "node is required" >&2
  exit 1
fi

if ! command -v cargo >/dev/null 2>&1; then
  echo "cargo is required" >&2
  exit 1
fi

PROFILE="smoke"
OUTPUT=""
MANIFEST="benchmarks/temporal-comparison/workloads.json"
REPETITIONS="1"
TEMPORAL_PROJECT="${TEMPORAL_BENCHMARK_PROJECT:-fabrik-temporal-benchmark}"
TEMPORAL_HOST_PORT="${TEMPORAL_HOST_PORT:-7233}"
TEMPORAL_NAMESPACE="${TEMPORAL_NAMESPACE:-default}"
KEEP_TEMPORAL_STACK="${KEEP_TEMPORAL_STACK:-0}"
TEMPORAL_CONTAINER=""

while (($#)); do
  case "$1" in
    --profile)
      PROFILE="$2"
      shift 2
      ;;
    --output)
      OUTPUT="$2"
      shift 2
      ;;
    --manifest)
      MANIFEST="$2"
      shift 2
      ;;
    --repetitions)
      REPETITIONS="$2"
      shift 2
      ;;
    --temporal-host-port)
      TEMPORAL_HOST_PORT="$2"
      shift 2
      ;;
    --temporal-namespace)
      TEMPORAL_NAMESPACE="$2"
      shift 2
      ;;
    --keep-temporal-stack)
      KEEP_TEMPORAL_STACK="1"
      shift
      ;;
    *)
      echo "unknown argument $1" >&2
      exit 1
      ;;
  esac
done

if [[ -z "$OUTPUT" ]]; then
  OUTPUT="target/benchmark-reports/temporal-comparison-${PROFILE}.json"
fi

if [[ ! -d node_modules/@temporalio/worker ]]; then
  echo "[temporal-comparison] installing npm dependencies"
  npm install
fi

cleanup() {
  local exit_code=$?
  if [[ "$KEEP_TEMPORAL_STACK" != "1" ]]; then
    docker compose \
      -p "$TEMPORAL_PROJECT" \
      -f docker/temporal/docker-compose.yml \
      down -v >/dev/null 2>&1 || true
  fi
  exit "$exit_code"
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
deadline = time.time() + 90
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

wait_for_temporal_namespace() {
  local namespace=$1
  local timeout=${2:-120}
  local deadline=$((SECONDS + timeout))
  while (( SECONDS < deadline )); do
    if docker exec "$TEMPORAL_CONTAINER" \
      temporal operator namespace describe \
      --address temporal:7233 \
      --namespace "$namespace" >/dev/null 2>&1; then
      return 0
    fi
    sleep 2
  done
  echo "timed out waiting for Temporal namespace $namespace" >&2
  return 1
}

ensure_temporal_namespace() {
  local namespace=$1
  if ! docker exec "$TEMPORAL_CONTAINER" \
    temporal operator namespace describe \
    --address temporal:7233 \
    --namespace "$namespace" >/dev/null 2>&1; then
    docker exec "$TEMPORAL_CONTAINER" \
      temporal operator namespace create \
      --address temporal:7233 \
      --namespace "$namespace" >/dev/null
  fi

  wait_for_temporal_namespace "$namespace" 120
}

echo "[temporal-comparison] starting Temporal stack"
TEMPORAL_HOST_PORT="$TEMPORAL_HOST_PORT" docker compose \
  -p "$TEMPORAL_PROJECT" \
  -f docker/temporal/docker-compose.yml \
  up -d >/dev/null
TEMPORAL_CONTAINER="${TEMPORAL_PROJECT}-temporal-1"

echo "[temporal-comparison] waiting for Temporal frontend"
wait_for_port 127.0.0.1 "$TEMPORAL_HOST_PORT" "temporal-frontend"
echo "[temporal-comparison] waiting for Temporal namespace ${TEMPORAL_NAMESPACE}"
ensure_temporal_namespace "$TEMPORAL_NAMESPACE"

echo "[temporal-comparison] building Fabrik release binaries"
cargo build --release \
  -p benchmark-runner \
  -p ingest-service \
  -p unified-runtime \
  -p timer-service \
  -p activity-worker-service >/dev/null

echo "[temporal-comparison] running comparison harness"
TEMPORAL_ADDRESS="127.0.0.1:${TEMPORAL_HOST_PORT}" \
TEMPORAL_NAMESPACE="$TEMPORAL_NAMESPACE" \
BUILD_RELEASE_BINARIES=0 \
node benchmarks/temporal-comparison/runner.mjs \
  --profile "$PROFILE" \
  --manifest "$MANIFEST" \
  --output "$OUTPUT" \
  --repetitions "$REPETITIONS" \
  --temporal-address "127.0.0.1:${TEMPORAL_HOST_PORT}" \
  --temporal-namespace "$TEMPORAL_NAMESPACE"
