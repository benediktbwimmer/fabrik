# Streaming Backend Benchmarks

Use `benchmark-runner` to compare the durable engine and `stream-v2` under the same workload.

For the current benchmark-backed product envelope and workload-shape guidance, see [streaming-performance-envelope.md](streaming-performance-envelope.md).

## Turnkey isolated run

Use the isolated harness when you want trustworthy numbers without reusing your normal local service processes:

```bash
./scripts/run-isolated-benchmark.sh
```

That command will:

- start or reuse the local Docker infra (`redpanda`, `postgres`, `minio`)
- create a fresh benchmark database and unique Redpanda topics
- launch isolated release services on dedicated ports
- run the benchmark runner with a fresh tenant/task queue namespace
- write logs under `target/benchmark-runs/<namespace>/logs`
- write the benchmark report under `target/benchmark-reports/<namespace>.json`

By default it runs the `streaming` suite with the `target` profile. You can pass normal `benchmark-runner` flags through to the script:

```bash
./scripts/run-isolated-benchmark.sh --suite streaming --profile smoke --worker-count 4
./scripts/run-isolated-benchmark.sh --execution-mode throughput --throughput-backend stream-v2 --profile target --chunk-size 100
```

## Prerequisites

- local stack running, including MinIO, `streams-runtime`, and `streams-projector`
- at least one worker polling the benchmark task queue
- `THROUGHPUT_PAYLOAD_STORE=s3` for the MinIO-backed throughput path

## Recommended first pass

Run the built-in streaming suite:

```bash
cargo run -p benchmark-runner -- \
  --suite streaming \
  --profile smoke \
  --task-queue default \
  --worker-count 4 \
  --payload-size 512 \
  --output target/benchmark-reports/streaming-suite.json
```

This executes the baseline streaming scenarios sequentially:

- `durable`
- `throughput-stream-v2`

The suite writes one JSON/TXT pair per scenario plus a suite JSON aggregate.

For the mergeable fast lane specifically:

```bash
cargo run -p benchmark-runner -- \
  --suite stream-v2-fast-lane \
  --profile target \
  --chunk-size 64 \
  --output target/benchmark-reports/stream-v2-fast-lane.json
```

That suite exercises the `count` and `all_settled` reducers on `stream-v2` with smaller chunks so grouped fan-in and owner-first apply show up in the control-plane metrics.

## Single-scenario runs

`stream-v2` throughput:

```bash
cargo run -p benchmark-runner -- \
  --execution-mode throughput \
  --throughput-backend stream-v2 \
  --profile target \
  --chunk-size 256
```

## What to compare

- `duration_ms`
- `activity_throughput_per_second`
- `bulk_*` vs `projection_*` row counts
- `max_aggregation_group_count`
- `grouped_batch_rows`
- `max_activity_backlog`
- `throughput_runtime_debug`
- `throughput_projector_debug`
- `projection_events_skipped`
- `projection_events_applied_directly`

For `stream-v2`, nonzero `grouped_batch_rows` and `max_aggregation_group_count > 1` show that hierarchical aggregation was enabled during the run.

## Current Status

The streaming path is now broader than the original fast-lane experiment:

- mergeable reducers now include `sum`, `min`, `max`, `avg`, and `histogram`
- topic adapters provide a durable ingress path into streaming workloads
- failover now has dedicated benchmark suites for both `stream-v2` and topic-adapter ownership handoff
- the console exposes a unified streaming operations view

The benchmark story should therefore be read in two layers:

- this document explains how to run the benchmark harness
- [streaming-performance-envelope.md](streaming-performance-envelope.md) captures the current benchmark-backed product guidance
