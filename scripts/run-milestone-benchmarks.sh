#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

items="${BENCHMARK_ITEMS:-25}"
iterations="${BENCHMARK_ITERATIONS:-3}"

cargo run -p executor-service --release -- benchmark-milestone --items "$items" --iterations "$iterations"

mkdir -p target/benchmark-reports

cargo run -p benchmark-runner -- --profile smoke --output target/benchmark-reports/smoke.json
cargo run -p benchmark-runner -- --profile target --output target/benchmark-reports/target.json
cargo run -p benchmark-runner -- --profile stress --output target/benchmark-reports/stress.json
