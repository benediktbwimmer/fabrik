#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

mkdir -p target/benchmark-reports

cargo run -p benchmark-runner -- --profile smoke --output target/benchmark-reports/smoke.json
cargo run -p benchmark-runner -- --profile target --output target/benchmark-reports/target.json
cargo run -p benchmark-runner -- --profile stress --output target/benchmark-reports/stress.json
