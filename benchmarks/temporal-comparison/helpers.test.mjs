import test from "node:test";
import assert from "node:assert/strict";

import { buildComparisonReport, formatComparisonSummary, sanitizeIdentifier } from "./helpers.mjs";

test("sanitizeIdentifier normalizes workload names", () => {
  assert.equal(sanitizeIdentifier("Fanout Retry / Wide"), "fanout-retry-wide");
});

test("buildComparisonReport aggregates platform metrics", () => {
  const report = buildComparisonReport({
    generatedAt: "2026-03-13T00:00:00.000Z",
    profile: "smoke",
    manifestPath: "/tmp/workloads.json",
    repetitions: 2,
    temporalAddress: "127.0.0.1:7233",
    temporalNamespace: "default",
    workloads: [
      {
        workload: {
          name: "fanout-baseline",
          description: "baseline",
        },
        runs: [
          {
            repetition: 1,
            temporal: demoTemporalRun(100, 2000),
            fabrik: {
              suiteReportPath: "/tmp/fabrik-1.json",
              scenarios: [
                demoFabrikRun("unified-experiment", 30, 6000),
              ],
            },
          },
          {
            repetition: 2,
            temporal: demoTemporalRun(120, 2200),
            fabrik: {
              suiteReportPath: "/tmp/fabrik-2.json",
              scenarios: [
                demoFabrikRun("unified-experiment", 40, 5900),
              ],
            },
          },
        ],
      },
    ],
  });

  const workload = report.workloads[0];
  assert.equal(workload.summary.platforms.temporal.meanDurationMs, 110);
  assert.equal(workload.summary.platforms.fabrik_unified.meanDurationMs, 35);
  assert.equal(workload.summary.comparisons.fabrik_unified.durationRatioVsTemporal, 35 / 110);
});

test("buildComparisonReport groups suffixed Fabrik scenarios under their base platform", () => {
  const report = buildComparisonReport({
    generatedAt: "2026-03-13T00:00:00.000Z",
    profile: "smoke",
    manifestPath: "/tmp/workloads.json",
    repetitions: 1,
    temporalAddress: "127.0.0.1:7233",
    temporalNamespace: "default",
    workloads: [
      {
        workload: {
          name: "fanout-retry",
          description: "retry workload",
        },
        runs: [
          {
            repetition: 1,
            temporal: demoTemporalRun(100, 2000),
            fabrik: {
              scenarios: [
                demoFabrikRun("unified-experiment-retry-500bp", 45, 4200),
              ],
            },
          },
        ],
      },
    ],
  });

  const workload = report.workloads[0];
  assert.equal(workload.summary.platforms.fabrik_unified.meanDurationMs, 45);
});

test("formatComparisonSummary renders workload summaries", () => {
  const report = buildComparisonReport({
    generatedAt: "2026-03-13T00:00:00.000Z",
    profile: "smoke",
    manifestPath: "/tmp/workloads.json",
    repetitions: 1,
    temporalAddress: "127.0.0.1:7233",
    temporalNamespace: "default",
    workloads: [
      {
        workload: {
          name: "fanout-baseline",
          description: "baseline",
        },
        runs: [
          {
            repetition: 1,
            temporal: demoTemporalRun(100, 2000),
            fabrik: {
              suiteReportPath: "/tmp/fabrik.json",
              scenarios: [
                demoFabrikRun("unified-experiment", 35, 6100),
              ],
            },
          },
        ],
      },
    ],
  });

  const summary = formatComparisonSummary(report);
  assert.match(summary, /fanout-baseline/);
  assert.match(summary, /fabrik_unified_vs_temporal/);
  assert.doesNotMatch(summary, /fabrik_durable_vs_temporal/);
});

function demoTemporalRun(durationMs, throughput) {
  return {
    durationMs,
    executionDurationMs: durationMs,
    workflowOutcomes: {
      completed: 10,
      failed: 0,
      cancelled: 0,
    },
    activityMetrics: {
      completed: 100,
      failed: 0,
      cancelled: 0,
      avgScheduleToStartLatencyMs: 1,
      avgStartToCloseLatencyMs: 2,
      throughputActivitiesPerSecond: throughput,
    },
  };
}

function demoFabrikRun(scenario, durationMs, throughput) {
  return {
    scenario,
    durationMs,
    executionDurationMs: durationMs,
    workflowOutcomes: {
      completed: 10,
      failed: 0,
      cancelled: 0,
    },
    activityMetrics: {
      completed: 100,
      failed: 0,
      cancelled: 0,
      avgScheduleToStartLatencyMs: 1.5,
      avgStartToCloseLatencyMs: 2.5,
      throughputActivitiesPerSecond: throughput,
    },
  };
}
