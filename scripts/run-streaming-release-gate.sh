#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

timestamp="$(date +%Y%m%dt%H%M%S)"
report_dir="${STREAMING_RELEASE_GATE_REPORT_DIR:-target/benchmark-reports/streaming-release-gate-$timestamp}"

reducers_report="${STREAMING_RELEASE_GATE_STREAMING_REDUCERS_REPORT:-$report_dir/streaming-reducers-gate-w8.json}"
stream_failover_report="${STREAMING_RELEASE_GATE_STREAM_FAILOVER_REPORT:-$report_dir/stream-v2-failover-gate-w8.json}"
adapter_ingress_report="${STREAMING_RELEASE_GATE_ADAPTER_INGRESS_REPORT:-$report_dir/topic-adapters-gate-w8.json}"
adapter_failover_report="${STREAMING_RELEASE_GATE_ADAPTER_FAILOVER_REPORT:-$report_dir/topic-adapters-failover-gate-w8.json}"
temporal_report="${STREAMING_RELEASE_GATE_TEMPORAL_REPORT:-$report_dir/temporal-comparison-gate.json}"
summary_json="${STREAMING_RELEASE_GATE_SUMMARY_JSON:-$report_dir/streaming-release-gate-summary.json}"
summary_md="${STREAMING_RELEASE_GATE_SUMMARY_MD:-$report_dir/streaming-release-gate-summary.md}"

mkdir -p "$report_dir"

if [[ "${STREAMING_RELEASE_GATE_SKIP_RUNS:-0}" != "1" ]]; then
  ./scripts/run-isolated-benchmark.sh \
    --suite streaming-reducers \
    --profile gate \
    --worker-count 8 \
    --output "$reducers_report"

  ./scripts/run-isolated-benchmark.sh \
    --suite stream-v2-failover \
    --profile gate \
    --worker-count 8 \
    --output "$stream_failover_report"

  ./scripts/run-isolated-benchmark.sh \
    --suite topic-adapters \
    --profile gate \
    --worker-count 8 \
    --output "$adapter_ingress_report"

  ./scripts/run-isolated-benchmark.sh \
    --suite topic-adapters-failover \
    --profile gate \
    --worker-count 8 \
    --output "$adapter_failover_report"

  BENCHMARK_KILL_LOCAL_SERVICES=1 \
    ./scripts/run-temporal-comparison-benchmark.sh \
    --profile gate \
    --output "$temporal_report"
fi

python3 - "$reducers_report" "$stream_failover_report" "$adapter_ingress_report" "$adapter_failover_report" "$temporal_report" "$summary_json" "$summary_md" <<'PY'
import json
import pathlib
import sys

reducers_path = pathlib.Path(sys.argv[1])
stream_failover_path = pathlib.Path(sys.argv[2])
adapter_ingress_path = pathlib.Path(sys.argv[3])
adapter_failover_path = pathlib.Path(sys.argv[4])
temporal_path = pathlib.Path(sys.argv[5])
summary_json_path = pathlib.Path(sys.argv[6])
summary_md_path = pathlib.Path(sys.argv[7])

paths = [
    reducers_path,
    stream_failover_path,
    adapter_ingress_path,
    adapter_failover_path,
    temporal_path,
]
for path in paths:
    if not path.exists():
        raise SystemExit(f"missing report {path}")

reducers = json.loads(reducers_path.read_text())
stream_failover = json.loads(stream_failover_path.read_text())
adapter_ingress = json.loads(adapter_ingress_path.read_text())
adapter_failover = json.loads(adapter_failover_path.read_text())
temporal = json.loads(temporal_path.read_text())

checks = []

def add_check(name: str, ok: bool, detail: str, blocking: bool = True) -> None:
    checks.append({
        "name": name,
        "ok": ok,
        "detail": detail,
        "blocking": blocking,
    })

def scenario_by_fragment(report: dict, fragment: str) -> dict:
    for scenario in report.get("scenarios", []):
        if fragment in scenario.get("scenario", ""):
            return scenario
    raise KeyError(f"missing scenario containing {fragment}")

# Reducer checks: stream-v2 should beat pg-v1 on the supported reducers in gate.
reducer_pairs = {
    "sum": ("throughput-pg-v1-sum-pg-v1-sum", "throughput-stream-v2-sum-stream-v2-sum"),
    "min": ("throughput-pg-v1-min-pg-v1-min", "throughput-stream-v2-min-stream-v2-min"),
    "max": ("throughput-pg-v1-max-pg-v1-max", "throughput-stream-v2-max-stream-v2-max"),
    "avg": ("throughput-pg-v1-avg-pg-v1-avg", "throughput-stream-v2-avg-stream-v2-avg"),
    "histogram": ("throughput-pg-v1-histogram-pg-v1-histogram", "throughput-stream-v2-histogram-stream-v2-histogram"),
}
for reducer, (pg_name, stream_name) in reducer_pairs.items():
    pg = scenario_by_fragment(reducers, pg_name)
    stream = scenario_by_fragment(reducers, stream_name)
    pg_tps = pg["activity_metrics"]["throughput_activities_per_second"]
    stream_tps = stream["activity_metrics"]["throughput_activities_per_second"]
    add_check(
        f"Reducer {reducer}",
        stream_tps > pg_tps,
        f"pg-v1={pg_tps:.2f} APS, stream-v2={stream_tps:.2f} APS",
    )

# stream-v2 failover checks.
for fragment in [
    "owner-restart-all-settled",
    "owner-restart-retry-cancel-all-settled",
]:
    scenario = scenario_by_fragment(stream_failover, fragment)
    failover = scenario.get("failover_injection") or {}
    downtime = failover.get("downtime_ms")
    add_check(
        f"Stream failover {fragment}",
        failover.get("status") == "completed" and isinstance(downtime, int) and downtime <= 5000,
        f"status={failover.get('status')}, downtime_ms={downtime}",
    )

# Adapter ingress checks.
for fragment in [
    "adapter-start-start-workflow",
    "adapter-signal-signal-workflow",
]:
    scenario = scenario_by_fragment(adapter_ingress, fragment)
    metrics = scenario.get("adapter_metrics") or {}
    final_lag = metrics.get("final_lag_records")
    add_check(
        f"Adapter ingress {fragment}",
        metrics.get("failed_count") == 0 and isinstance(final_lag, int) and final_lag <= 5,
        f"processed={metrics.get('processed_count')}, failed={metrics.get('failed_count')}, final_lag={final_lag}",
    )

# Adapter failover checks.
for fragment in [
    "adapter-start-owner-crash-all-settled",
    "adapter-start-lag-under-load-owner-crash-all-settled",
]:
    scenario = scenario_by_fragment(adapter_failover, fragment)
    metrics = scenario.get("adapter_metrics") or {}
    latency = metrics.get("last_takeover_latency_ms")
    add_check(
        f"Adapter failover {fragment}",
        metrics.get("ownership_handoff_count", 0) >= 1 and isinstance(latency, int) and latency <= 2000,
        f"handoffs={metrics.get('ownership_handoff_count')}, owner_epoch={metrics.get('owner_epoch')}, takeover_latency_ms={latency}",
    )

# Temporal comparison checks.
for workload in temporal.get("workloads", []):
    name = workload["workload"]["name"]
    comparison = workload["summary"]["comparisons"]["fabrik_unified"]
    duration_ratio = comparison["durationRatioVsTemporal"]
    throughput_ratio = comparison["throughputRatioVsTemporal"]
    add_check(
        f"Temporal comparison {name}",
        duration_ratio < 1.0 and throughput_ratio > 1.0,
        f"duration_ratio={duration_ratio:.3f}, throughput_ratio={throughput_ratio:.2f}x",
    )

blocking_checks = [check for check in checks if check["blocking"]]
passed = all(check["ok"] for check in blocking_checks)

dt = __import__("datetime")
summary = {
    "passed": passed,
    "generated_at": dt.datetime.now(dt.timezone.utc).isoformat().replace("+00:00", "Z"),
    "reports": {
        "streaming_reducers": str(reducers_path),
        "stream_failover": str(stream_failover_path),
        "adapter_ingress": str(adapter_ingress_path),
        "adapter_failover": str(adapter_failover_path),
        "temporal_comparison": str(temporal_path),
    },
    "checks": checks,
}
summary_json_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")

lines = [
    "# Streaming Release Gate",
    "",
    f"Status: {'PASS' if passed else 'FAIL'}",
    "",
    "## Reports",
    "",
    f"- reducers: `{reducers_path}`",
    f"- stream failover: `{stream_failover_path}`",
    f"- adapter ingress: `{adapter_ingress_path}`",
    f"- adapter failover: `{adapter_failover_path}`",
    f"- temporal comparison: `{temporal_path}`",
    "",
    "## Checks",
    "",
]
for check in checks:
    lines.append(f"- [{'PASS' if check['ok'] else 'FAIL'}] {check['name']}: {check['detail']}")
lines.append("")
summary_md_path.write_text("\n".join(lines), encoding="utf-8")

print(summary_md_path)
if not passed:
    raise SystemExit(1)
PY

echo "summary_json=$summary_json"
echo "summary_md=$summary_md"
