#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

OUTPUT_DIR="${1:-target/pressure-repo-qualification}"

repos=(
  "crates/fabrik-cli/test-fixtures/temporal-async-external-pressure"
  "crates/fabrik-cli/test-fixtures/temporal-interceptor-pressure"
  "crates/fabrik-cli/test-fixtures/temporal-sinks-pressure"
  "crates/fabrik-cli/test-fixtures/temporal-converter-trust-pressure"
  "crates/fabrik-cli/test-fixtures/temporal-monorepo-multiworker-pressure"
  "crates/fabrik-cli/test-fixtures/temporal-versioning-upgrade-pressure"
)

scripts/run-external-repo-qualification.sh --output-dir "$OUTPUT_DIR" "${repos[@]}"
