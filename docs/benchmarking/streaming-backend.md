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

For `stream-v2`, nonzero `grouped_batch_rows` and `max_aggregation_group_count > 1` show that hierarchical aggregation was enabled during the run.
