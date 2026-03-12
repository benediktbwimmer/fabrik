# Streaming Backend Benchmarks

Use `benchmark-runner` to compare the durable engine, `pg-v1`, and `stream-v2` under the same workload.

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

- local stack running, including MinIO, `throughput-runtime`, and `throughput-projector`
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

This executes three scenarios sequentially:

- `durable`
- `throughput-pg-v1`
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

`pg-v1` throughput:

```bash
cargo run -p benchmark-runner -- \
  --execution-mode throughput \
  --throughput-backend pg-v1 \
  --profile target \
  --chunk-size 256
```

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

## Experimental fast lane status

The current `stream-v2` fast lane should still be treated as experimental.

What changed in the benchmark harness:

- owner-first benchmark runs now skip the throughput changelog restore/consumer path inside `throughput-runtime`
- the isolated harness now waits for Redpanda topics to report the requested partition counts before services start
- `/debug/throughput` now exposes tree-specific counters so runs can distinguish leaf, parent, and root group terminalization

What the latest controlled runs showed:

- on a forced 2-level grouped workload (`25` workflows, `4096` activities/workflow, `chunk_size=8`), `count` improved throughput by about `2.8%`
- on a forced 3-level grouped workload (`10` workflows, `4096` activities/workflow, `chunk_size=4`), `count` improved throughput by about `1.3%`
- the fast lane now emits visible hierarchy counters in debug output:
  - 2-level case: leaf groups plus root groups
  - 3-level case: leaf groups, parent groups, and root groups

Why this is still experimental:

- the measured gain is real but small
- it is still far below the original success gate for the milestone
- some ownership-renewal warning noise remains during long runs, so small deltas should still be treated cautiously

Recommended use:

- keep the fast lane behind an experiment flag or branch
- use the isolated benchmark harness for evaluation
- rely on `batch_tree_depth_counts`, `group_level_counts`, and the runtime terminal counters when judging whether a run actually exercised the intended tree shape
