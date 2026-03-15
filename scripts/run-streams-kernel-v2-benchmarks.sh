#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

timestamp="$(date +%Y%m%dt%H%M%S)"
run_root="${STREAMS_KERNEL_V2_BENCH_ROOT:-target/benchmark-reports/streams-kernel-v2-$timestamp}"
if [[ "$run_root" != /* ]]; then
  run_root="$(pwd)/$run_root"
fi
target_dir="${STREAMS_KERNEL_V2_CARGO_TARGET_DIR:-$run_root/cargo-target}"
container_data_root="${STREAMS_KERNEL_V2_CONTAINER_DATA_ROOT:-$run_root/docker-data}"
log_dir="$run_root/logs"

mkdir -p "$target_dir" "$container_data_root" "$log_dir"

tests=(
  "perf_stream_job_end_to_end_topic_reports_throughput"
  "perf_aggregate_v2_windowed_owner_activation_reports_throughput_and_skew"
  "perf_aggregate_v2_windowed_checkpoint_and_restore_report_costs"
  "perf_aggregate_v2_windowed_owner_strong_reads_report_latency"
)

echo "streams-kernel-v2 benchmark run"
echo "report_root=$run_root"
echo "cargo_target_dir=$target_dir"
echo "container_data_root=$container_data_root"

for test_name in "${tests[@]}"; do
  log_path="$log_dir/${test_name}.log"
  echo
  echo "==> $test_name"
  CARGO_TARGET_DIR="$target_dir" \
  FABRIK_TEST_CONTAINER_DATA_ROOT="$container_data_root" \
  cargo test -p streams-runtime --release "$test_name" -- --ignored --nocapture --test-threads=1 \
    | tee "$log_path"
done

echo
echo "logs_dir=$log_dir"
