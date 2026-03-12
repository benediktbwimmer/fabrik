# Temporal Comparison Benchmarks

Use `run-temporal-comparison-benchmark.sh` to execute the same fan-out activity workloads against:

- Fabrik durable execution
- Fabrik throughput `pg-v1`
- Fabrik throughput `stream-v2`
- Temporal on a real auto-setup + PostgreSQL stack

This harness is intentionally separate from the existing internal streaming benchmark. It keeps the current Fabrik-only report shape and layers a neutral cross-platform report on top.

## What It Benchmarks Today

The initial suite only includes workloads that both platforms can execute with the same observable behavior right now:

- high-fanout activity dispatch
- fan-in completion after all activities settle
- retryable first-attempt failures
- terminal non-retryable activity cancellations
- payload-size variation
- wider concurrency envelopes

Timers, signals, updates, child workflows, and continue-as-new should be added once Fabrik can run those workloads with Temporal-equivalent semantics end to end.

## Run It

```bash
./scripts/run-temporal-comparison-benchmark.sh --profile smoke
```

Useful options:

```bash
./scripts/run-temporal-comparison-benchmark.sh \
  --profile target \
  --repetitions 3 \
  --output target/benchmark-reports/temporal-comparison-target.json
```

That command will:

- start Temporal with `docker/temporal/docker-compose.yml`
- prebuild Fabrik release binaries once
- run each workload against Temporal
- run the matching Fabrik suite through `scripts/run-isolated-benchmark.sh`
- write one combined JSON report and one TXT summary
- write per-run Temporal and Fabrik artifacts next to the combined report

## Workload Manifest

The workload definitions live in [`benchmarks/temporal-comparison/workloads.json`](/Users/bene/code/fabrik/benchmarks/temporal-comparison/workloads.json).

Each profile entry declares:

- `workflowCount`
- `activitiesPerWorkflow`
- `payloadSize`
- `workerCount`
- `retryRate`
- `cancelRate`
- `timeoutSecs`

The harness uses those values on both platforms. For Fabrik it also sets:

- `ACTIVITY_WORKER_CONCURRENCY`
- `STREAM_ACTIVITY_WORKER_CONCURRENCY`

so the reported worker count matches the actual activity-worker concurrency used in the isolated stack.

## Output Shape

The combined report contains, per workload:

- raw Temporal run reports
- raw Fabrik suite reports
- aggregated platform means across repetitions
- ratios versus Temporal for:
  - duration
  - activity throughput
  - schedule-to-start latency
  - start-to-close latency

The TXT summary is intended for quick scans; the JSON report is the source of truth for analysis.

## Caveats

- The Temporal side currently uses the default namespace and isolates runs with unique task queues and workflow IDs.
- The current comparison is workload-semantic, not API-semantic. It compares equivalent orchestration behavior, not SDK surface.
- Throughput-only Fabrik counters such as grouped batch rows remain useful diagnostics, but they are not part of the cross-platform headline numbers.
