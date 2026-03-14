# Streaming Performance Envelope

This document captures the current benchmark-backed operating envelope for Fabrik's streaming story:

- topic adapters for durable ingress
- `stream-v2` for high-throughput fan-out / fan-in
- live reducer progress and operator visibility
- owner handoff and failover under load

These numbers are intended as product guidance, not as a hard SLA.

## Read The Numbers Correctly

The benchmark runner reports `activity_throughput_per_second`.

That is:

- completed activities per second
- not workflow starts per second
- not a single-workflow latency number

For example, when a run starts `100` workflows and each workflow executes `1000` activities, a result near `42K APS` means the system completed about `100,000` activities in about `2.4s`. It does **not** mean Fabrik started `42,000` workflows per second.

## Current Target Benchmarks

### Adapter-triggered fan-out

These runs exercise topic-adapter ingress plus downstream `stream-v2` execution.

| Workload | Report | Result |
|---|---|---|
| topic adapter `start_workflow` | [`topic-adapters-target-w8-throughput-stream-v2-adapter-start-start-workflow-all-settled.json`](/Users/bene/code/fabrik/target/benchmark-reports/topic-adapters-target-w8-throughput-stream-v2-adapter-start-start-workflow-all-settled.json) | about `42,194 APS` |
| topic adapter `signal_workflow` | [`topic-adapters-target-w8-throughput-stream-v2-adapter-signal-signal-workflow-all-settled.json`](/Users/bene/code/fabrik/target/benchmark-reports/topic-adapters-target-w8-throughput-stream-v2-adapter-signal-signal-workflow-all-settled.json) | about `31,309 APS` |

### `stream-v2` failover

These runs measure throughput while forcing owner restart during execution.

| Workload | Report | Result |
|---|---|---|
| clean owner restart | [`stream-v2-failover-target-w8-throughput-stream-v2-owner-restart-all-settled.json`](/Users/bene/code/fabrik/target/benchmark-reports/stream-v2-failover-target-w8-throughput-stream-v2-owner-restart-all-settled.json) | about `57,008 APS`, `failover_downtime_ms=2921` |
| retry/cancel owner restart | [`stream-v2-failover-target-w8-throughput-stream-v2-owner-restart-retry-cancel-all-settled-retry-100bp-cancel-100bp.json`](/Users/bene/code/fabrik/target/benchmark-reports/stream-v2-failover-target-w8-throughput-stream-v2-owner-restart-retry-cancel-all-settled-retry-100bp-cancel-100bp.json) | about `9,768 APS`, `failover_downtime_ms=2675` |

### Reducer target runs

The `stream-v2` reducer path now materially outperforms `pg-v1` on the supported mergeable reducers.

| Reducer | `pg-v1` | `stream-v2` |
|---|---:|---:|
| `sum` | about `4,788 APS` | about `20,382 APS` |
| `min` | about `4,960 APS` | about `21,135 APS` |
| `max` | about `4,910 APS` | about `20,305 APS` |
| `avg` | about `4,781 APS` | about `19,930 APS` |
| `histogram` | about `4,573 APS` | about `17,187 APS` |

Source reports:

- [`streaming-reducers-target-w8-throughput-pg-v1-sum-pg-v1-sum.json`](/Users/bene/code/fabrik/target/benchmark-reports/streaming-reducers-target-w8-throughput-pg-v1-sum-pg-v1-sum.json)
- [`streaming-reducers-target-w8-throughput-stream-v2-sum-stream-v2-sum.json`](/Users/bene/code/fabrik/target/benchmark-reports/streaming-reducers-target-w8-throughput-stream-v2-sum-stream-v2-sum.json)
- [`streaming-reducers-target-w8-throughput-pg-v1-min-pg-v1-min.json`](/Users/bene/code/fabrik/target/benchmark-reports/streaming-reducers-target-w8-throughput-pg-v1-min-pg-v1-min.json)
- [`streaming-reducers-target-w8-throughput-stream-v2-min-stream-v2-min.json`](/Users/bene/code/fabrik/target/benchmark-reports/streaming-reducers-target-w8-throughput-stream-v2-min-stream-v2-min.json)
- [`streaming-reducers-target-w8-throughput-pg-v1-max-pg-v1-max.json`](/Users/bene/code/fabrik/target/benchmark-reports/streaming-reducers-target-w8-throughput-pg-v1-max-pg-v1-max.json)
- [`streaming-reducers-target-w8-throughput-stream-v2-max-stream-v2-max.json`](/Users/bene/code/fabrik/target/benchmark-reports/streaming-reducers-target-w8-throughput-stream-v2-max-stream-v2-max.json)
- [`streaming-reducers-target-w8-throughput-pg-v1-avg-pg-v1-avg.json`](/Users/bene/code/fabrik/target/benchmark-reports/streaming-reducers-target-w8-throughput-pg-v1-avg-pg-v1-avg.json)
- [`streaming-reducers-target-w8-throughput-stream-v2-avg-stream-v2-avg.json`](/Users/bene/code/fabrik/target/benchmark-reports/streaming-reducers-target-w8-throughput-stream-v2-avg-stream-v2-avg.json)
- [`streaming-reducers-target-w8-throughput-pg-v1-histogram-pg-v1-histogram.json`](/Users/bene/code/fabrik/target/benchmark-reports/streaming-reducers-target-w8-throughput-pg-v1-histogram-pg-v1-histogram.json)
- [`streaming-reducers-target-w8-throughput-stream-v2-histogram-stream-v2-histogram.json`](/Users/bene/code/fabrik/target/benchmark-reports/streaming-reducers-target-w8-throughput-stream-v2-histogram-stream-v2-histogram.json)

### Workload-shape sensitivity

The most important throughput lesson so far is that workload shape matters heavily even when total work stays constant.

All three runs below use:

- `execution_mode=throughput`
- `throughput_backend=stream-v2`
- `bulk_reducer=all_settled`
- `worker_count=8`
- `total_activities=100000`

| Shape | Report | Result |
|---|---|---|
| `1 workflow x 100000 activities` | [`custom-shape-stream-v2-1x100000.json`](/Users/bene/code/fabrik/target/benchmark-reports/custom-shape-stream-v2-1x100000.json) | about `120,192 APS` |
| `10 workflows x 10000 activities` | [`custom-shape-stream-v2-10x10000.json`](/Users/bene/code/fabrik/target/benchmark-reports/custom-shape-stream-v2-10x10000.json) | about `106,496 APS` |
| `100 workflows x 1000 activities` | [`custom-shape-stream-v2-100x1000.json`](/Users/bene/code/fabrik/target/benchmark-reports/custom-shape-stream-v2-100x1000.json) | about `41,684 APS` |

Interpretation:

- fewer, wider workflows are the best fit for `stream-v2`
- many smaller workflows pay much more workflow-level overhead
- the streaming backend is strongest when work is concentrated into a smaller number of wide fan-out / fan-in batches

## Product Guidance

Use these rules when deciding whether a workload is a good fit for the streaming path.

### Best fits

- large batch fan-out / fan-in
- mergeable reducers
- topic-driven ingestion into workflow starts or signals
- workloads where batch-level progress is more valuable than per-item durable history
- workloads that benefit from grouping many items behind one workflow barrier

### Good but less ideal fits

- many medium-width workflows
- reducers that still need more output materialization than `count` / `sum` / `avg`
- workloads with some retries or cancellations but still mostly settle in the fast lane

### Poor fits

- many tiny workflows where per-workflow overhead dominates
- workflows that need per-item workflow-visible history
- workflows that depend on intermediate item ordering
- workloads that should really be modeled as independent workflows rather than one wide batch

## Adapter-Specific Guidance

Topic adapters are an ingress mechanism, not a stream processor.

They are strongest when they:

- map records deterministically to `start_workflow` or `signal_workflow`
- preserve idempotency through partition/offset-derived request IDs
- expose lag, ownership, dead letters, and replay as operator-visible state

They are not intended for:

- arbitrary transforms
- windowing
- joins
- stateful streaming compute

## Reproducing The Key Runs

Adapter ingress target suite:

```bash
./scripts/run-isolated-benchmark.sh \
  --suite topic-adapters \
  --profile target \
  --worker-count 8 \
  --output target/benchmark-reports/topic-adapters-target-w8.json
```

`stream-v2` failover target suite:

```bash
./scripts/run-isolated-benchmark.sh \
  --suite stream-v2-failover \
  --profile target \
  --worker-count 8 \
  --output target/benchmark-reports/stream-v2-failover-target-w8.json
```

Custom shape run:

```bash
./scripts/run-isolated-benchmark.sh \
  --profile target \
  --worker-count 8 \
  --execution-mode throughput \
  --throughput-backend stream-v2 \
  --bulk-reducer all_settled \
  --workflow-count 10 \
  --activities-per-workflow 10000 \
  --output target/benchmark-reports/custom-shape-stream-v2-10x10000.json
```

## Caveats

- These numbers are benchmark-task numbers, not arbitrary business-logic numbers.
- The target envelope is strong enough to guide product positioning, but it is still environment-dependent.
- The `sample_errors` reducer path still hit an environment ceiling at `target w=8`, so the target-profile signal for that reducer currently relies on `w=4` isolated runs rather than a full `w=8` suite aggregate.
