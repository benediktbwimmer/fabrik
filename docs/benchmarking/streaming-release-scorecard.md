# Streaming Release Scorecard

This scorecard defines the minimum release signal for Fabrik's streaming product story.

It is intentionally small. The goal is to protect the core claims we now make:

- streams can enter the platform durably through topic adapters
- wide fan-out / fan-in can execute at high throughput on `stream-v2`
- reducer paths remain within the expected `stream-v2` throughput envelope
- ownership handoff remains fast enough to be operationally boring
- Fabrik still compares favorably to Temporal on the gate workload set

## Entry Point

Run the gate with:

```bash
./scripts/run-streaming-release-gate.sh
```

Equivalent repo shortcuts:

```bash
make benchmark-streaming-release-gate
npm run benchmark:streaming-release-gate
```

The script writes:

- one report per suite under `target/benchmark-reports/streaming-release-gate-<timestamp>/`
- a machine-readable summary JSON
- a human-readable summary Markdown

## Blocking Checks

The current gate blocks release if any of the following fail.

### 1. Reducer gate

Suite:

- `streaming-reducers --profile gate --worker-count 8`

Protected claim:

- the supported mergeable reducer path on `stream-v2` still stays within the expected release envelope

Current assertion:

- all listed reducers must complete successfully on `stream-v2`, and their throughput must stay above the current gate floor for:
  - `sum`
  - `min`
  - `max`
  - `avg`
  - `histogram`

### 2. `stream-v2` failover gate

Suite:

- `stream-v2-failover --profile gate --worker-count 8`

Protected claim:

- owner restart under load still converges correctly and quickly enough to support the product story

Current assertions:

- both failover scenarios complete successfully
- `failover_downtime_ms <= 5000`

### 3. Topic-adapter ingress gate

Suite:

- `topic-adapters --profile gate --worker-count 8`

Protected claim:

- adapter ingress still works as a durable control-plane surface, not just as an API shell

Current assertions:

- `failed_count == 0`
- `final_lag_records <= 5`

for both:

- `start_workflow`
- `signal_workflow`

### 4. Topic-adapter failover gate

Suite:

- `topic-adapters-failover --profile gate --worker-count 8`

Protected claim:

- adapter ownership transfer remains fast and duplicate-safe under crash conditions

Current assertions:

- `ownership_handoff_count >= 1`
- `last_takeover_latency_ms <= 2000`

for both:

- baseline owner crash
- lag-under-load owner crash

### 5. Temporal comparison gate

Suite:

- `temporal-comparison --profile gate`

Protected claim:

- Fabrik still wins the current gate workload set against Temporal on the same orchestration behavior

Current assertions:

- `durationRatioVsTemporal < 1.0`
- `throughputRatioVsTemporal > 1.0`

for every workload in the `gate` manifest.

## Informational, Not Blocking

These signals matter, but are currently informational rather than blocking:

- `target` streaming throughput envelope
- custom workload-shape runs like `1 x 100000` vs `100 x 1000`
- exact reducer ratios for `sample_errors`
- cosmetic console or doc regressions

Those signals should still be reviewed before a significant launch or external publication, but they are not the day-to-day release gate.

## Why These Checks

This scorecard deliberately maps to the product story, not just the implementation:

- reducers protect the high-throughput compute claim
- `stream-v2` failover protects the recovery claim
- topic-adapter ingress protects the streaming-ingress claim
- topic-adapter failover protects the operator and ownership claim
- Temporal comparison protects the broader competitive claim

If one of those checks regresses, the product story weakens in a way users will actually feel.

## When To Tighten The Gate

The next reasonable tightening steps are:

1. add explicit APS floor checks once the gate numbers stabilize more across environments
2. promote part of the `target` envelope into release review for externally published performance claims
3. split informational vs blocking thresholds for launch tiers

For now, the current gate is intentionally conservative: it checks correctness and directional performance, not publication-grade ceilings.
