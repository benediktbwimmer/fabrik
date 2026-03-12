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
                demoFabrikRun("durable", 80, 2500),
                demoFabrikRun("throughput-pg-v1", 70, 2800),
                demoFabrikRun("throughput-stream-v2", 60, 3200),
              ],
            },
          },
          {
            repetition: 2,
            temporal: demoTemporalRun(120, 2200),
            fabrik: {
              suiteReportPath: "/tmp/fabrik-2.json",
              scenarios: [
                demoFabrikRun("durable", 100, 2600),
                demoFabrikRun("throughput-pg-v1", 90, 3000),
                demoFabrikRun("throughput-stream-v2", 80, 3300),
              ],
            },
          },
        ],
      },
    ],
  });

  const workload = report.workloads[0];
  assert.equal(workload.summary.platforms.temporal.meanDurationMs, 110);
  assert.equal(workload.summary.platforms.fabrik_durable.meanActivityThroughputPerSecond, 2550);
  assert.equal(
    workload.summary.comparisons.fabrik_throughput_stream_v2.durationRatioVsTemporal,
    70 / 110,
  );
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
                demoFabrikRun("durable", 80, 2500),
                demoFabrikRun("throughput-pg-v1", 70, 2800),
                demoFabrikRun("throughput-stream-v2", 60, 3200),
              ],
            },
          },
        ],
      },
    ],
  });

  const summary = formatComparisonSummary(report);
  assert.match(summary, /fanout-baseline/);
  assert.match(summary, /fabrik_durable_vs_temporal/);
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
