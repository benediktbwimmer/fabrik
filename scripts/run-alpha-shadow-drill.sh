#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

scripts/run-alpha-primary-drill.sh \
  --repo "crates/fabrik-cli/test-fixtures/temporal-shadow-qualified" \
  --output-dir "target/alpha-drills/temporal-shadow-qualified" \
  --build-v1 "alpha-shadow-v1" \
  --build-v2 "alpha-shadow-v2" \
  "$@"
