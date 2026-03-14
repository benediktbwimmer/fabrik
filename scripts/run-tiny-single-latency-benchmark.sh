#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

if ! command -v python3 >/dev/null 2>&1; then
  echo "python3 is required" >&2
  exit 1
fi

PROFILE="smoke"
REPETITIONS="1"
TEMPORAL_NAMESPACE="${TEMPORAL_NAMESPACE:-fabrik-bench}"
OUTPUT_PREFIX="target/benchmark-reports/tiny-single-latency"
KEEP_TEMPORAL_STACK="0"

while (($#)); do
  case "$1" in
    --profile)
      PROFILE="$2"
      shift 2
      ;;
    --repetitions)
      REPETITIONS="$2"
      shift 2
      ;;
    --output-prefix)
      OUTPUT_PREFIX="$2"
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

TEMPORAL_OUTPUT="${OUTPUT_PREFIX}-temporal-comparison.json"
THROUGHPUT_OUTPUT="${OUTPUT_PREFIX}-throughput-stream-v2.json"
DURABLE_OUTPUT="${OUTPUT_PREFIX}-durable.json"
SUMMARY_OUTPUT="${OUTPUT_PREFIX}-summary.json"
SUMMARY_TEXT_OUTPUT="${OUTPUT_PREFIX}-summary.txt"

echo "[tiny-single-latency] running Temporal low-concurrency baseline"
TEMPORAL_CMD=(
  ./scripts/run-temporal-comparison-benchmark.sh
  --profile "$PROFILE"
  --manifest benchmarks/temporal-comparison/workloads-tiny-single-latency.json
  --output "$TEMPORAL_OUTPUT"
  --repetitions "$REPETITIONS"
  --temporal-namespace "$TEMPORAL_NAMESPACE"
)
if [[ "$KEEP_TEMPORAL_STACK" == "1" ]]; then
  TEMPORAL_CMD+=(--keep-temporal-stack)
fi
"${TEMPORAL_CMD[@]}"

echo "[tiny-single-latency] running Fabrik throughput single-start latency benchmark"
BENCHMARK_DISABLE_TINY_BATCH_API=1 \
BENCHMARK_TRIGGER_SUBMISSION_CONCURRENCY=1 \
./scripts/run-isolated-benchmark.sh \
  --execution-mode throughput \
  --throughput-backend stream-v2 \
  --bulk-reducer all_settled \
  --workflow-count 100 \
  --activities-per-workflow 1 \
  --payload-size 256 \
  --worker-count 16 \
  --workload-kind fanout \
  --timeout-secs 300 \
  --output "$THROUGHPUT_OUTPUT"

echo "[tiny-single-latency] running Fabrik durable single-start latency benchmark"
BENCHMARK_DISABLE_TINY_BATCH_API=1 \
BENCHMARK_TRIGGER_SUBMISSION_CONCURRENCY=1 \
./scripts/run-isolated-benchmark.sh \
  --execution-mode durable \
  --bulk-reducer all_settled \
  --workflow-count 100 \
  --activities-per-workflow 1 \
  --payload-size 256 \
  --worker-count 16 \
  --workload-kind fanout \
  --timeout-secs 300 \
  --output "$DURABLE_OUTPUT"

python3 - "$TEMPORAL_OUTPUT" "$THROUGHPUT_OUTPUT" "$DURABLE_OUTPUT" "$SUMMARY_OUTPUT" "$SUMMARY_TEXT_OUTPUT" <<'PY'
import json
import sys
from pathlib import Path

temporal_path = Path(sys.argv[1])
throughput_path = Path(sys.argv[2])
durable_path = Path(sys.argv[3])
summary_path = Path(sys.argv[4])
summary_text_path = Path(sys.argv[5])

temporal = json.loads(temporal_path.read_text(encoding="utf-8"))
throughput = json.loads(throughput_path.read_text(encoding="utf-8"))
durable = json.loads(durable_path.read_text(encoding="utf-8"))

temporal_run = temporal["workloads"][0]["runs"][0]["temporal"]
fabrik_unified_run = temporal["workloads"][0]["runs"][0]["fabrik"]["scenarios"][0]["rawReport"]

summary = {
    "generated_from": {
        "temporal_comparison_report": str(temporal_path),
        "throughput_report": str(throughput_path),
        "durable_report": str(durable_path),
    },
    "workload": {
        "workflow_count": temporal_run["workflowCount"],
        "activities_per_workflow": temporal_run["activitiesPerWorkflow"],
        "payload_size": temporal_run["payloadSize"],
        "worker_count": temporal_run["workerCount"],
    },
    "temporal": {
        "duration_ms": temporal_run["durationMs"],
        "throughput_activities_per_second": temporal_run["activityMetrics"]["throughputActivitiesPerSecond"],
        "request_latency_metrics": temporal_run.get("requestLatencyMetrics"),
    },
    "fabrik_unified_comparison_run": {
        "duration_ms": fabrik_unified_run["duration_ms"],
        "throughput_activities_per_second": fabrik_unified_run["activity_metrics"]["throughput_activities_per_second"],
        "request_latency_metrics": fabrik_unified_run.get("ingress_request_latency_metrics"),
    },
    "fabrik_throughput_stream_v2": {
        "duration_ms": throughput["duration_ms"],
        "throughput_activities_per_second": throughput["activity_metrics"]["throughput_activities_per_second"],
        "request_latency_metrics": throughput.get("ingress_request_latency_metrics"),
    },
    "fabrik_durable": {
        "duration_ms": durable["duration_ms"],
        "throughput_activities_per_second": durable["activity_metrics"]["throughput_activities_per_second"],
        "request_latency_metrics": durable.get("ingress_request_latency_metrics"),
    },
}

summary_path.write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")

def fmt_latency(metrics):
    if not metrics:
        return "n/a"
    return (
        f"avg={metrics['avg_request_latency_ms' if 'avg_request_latency_ms' in metrics else 'avgRequestLatencyMs']:.2f}ms "
        f"p50={metrics['p50_request_latency_ms' if 'p50_request_latency_ms' in metrics else 'p50RequestLatencyMs']:.2f}ms "
        f"p95={metrics['p95_request_latency_ms' if 'p95_request_latency_ms' in metrics else 'p95RequestLatencyMs']:.2f}ms "
        f"p99={metrics['p99_request_latency_ms' if 'p99_request_latency_ms' in metrics else 'p99RequestLatencyMs']:.2f}ms"
    )

lines = [
    f"workflows={summary['workload']['workflow_count']}",
    f"activities_per_workflow={summary['workload']['activities_per_workflow']}",
    f"payload_size={summary['workload']['payload_size']}",
    "",
    f"temporal: duration_ms={summary['temporal']['duration_ms']} throughput={summary['temporal']['throughput_activities_per_second']:.2f} latency={fmt_latency(summary['temporal']['request_latency_metrics'])}",
    f"fabrik_unified_comparison_run: duration_ms={summary['fabrik_unified_comparison_run']['duration_ms']} throughput={summary['fabrik_unified_comparison_run']['throughput_activities_per_second']:.2f} latency={fmt_latency(summary['fabrik_unified_comparison_run']['request_latency_metrics'])}",
    f"fabrik_throughput_stream_v2: duration_ms={summary['fabrik_throughput_stream_v2']['duration_ms']} throughput={summary['fabrik_throughput_stream_v2']['throughput_activities_per_second']:.2f} latency={fmt_latency(summary['fabrik_throughput_stream_v2']['request_latency_metrics'])}",
    f"fabrik_durable: duration_ms={summary['fabrik_durable']['duration_ms']} throughput={summary['fabrik_durable']['throughput_activities_per_second']:.2f} latency={fmt_latency(summary['fabrik_durable']['request_latency_metrics'])}",
    "",
    f"summary_path={summary_path}",
]
summary_text_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
PY

echo "summary_path=${SUMMARY_OUTPUT}"
echo "summary_text_path=${SUMMARY_TEXT_OUTPUT}"
