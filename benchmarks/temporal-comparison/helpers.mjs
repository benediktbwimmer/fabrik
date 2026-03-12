import fs from "node:fs/promises";
import path from "node:path";

const FABRIK_SCENARIO_LABELS = {
  durable: "fabrik_durable",
  "throughput-pg-v1": "fabrik_throughput_pg_v1",
  "throughput-stream-v2": "fabrik_throughput_stream_v2",
};

export function sanitizeIdentifier(value) {
  return value.toLowerCase().replace(/[^a-z0-9]+/g, "-").replace(/^-+|-+$/g, "");
}

export async function loadManifest(manifestPath, profile) {
  const contents = await fs.readFile(manifestPath, "utf8");
  const manifest = JSON.parse(contents);
  const workloads = manifest?.profiles?.[profile];
  if (!Array.isArray(workloads) || workloads.length === 0) {
    throw new Error(`profile ${profile} not found in ${manifestPath}`);
  }
  return workloads.map(validateWorkload);
}

function validateWorkload(workload) {
  const requiredFields = [
    "name",
    "description",
    "workflowCount",
    "activitiesPerWorkflow",
    "payloadSize",
    "workerCount",
    "retryRate",
    "cancelRate",
    "timeoutSecs",
  ];
  for (const field of requiredFields) {
    if (!(field in workload)) {
      throw new Error(`workload ${JSON.stringify(workload)} is missing ${field}`);
    }
  }
  return workload;
}

export async function writeJson(outputPath, value) {
  await fs.mkdir(path.dirname(outputPath), { recursive: true });
  await fs.writeFile(outputPath, `${JSON.stringify(value, null, 2)}\n`);
}

export async function writeText(outputPath, value) {
  await fs.mkdir(path.dirname(outputPath), { recursive: true });
  await fs.writeFile(outputPath, value);
}

export function buildComparisonReport({
  generatedAt,
  profile,
  manifestPath,
  repetitions,
  temporalAddress,
  temporalNamespace,
  workloads,
}) {
  return {
    generatedAt,
    profile,
    manifestPath,
    repetitions,
    temporalAddress,
    temporalNamespace,
    workloads: workloads.map((workloadResult) => {
      const platformRuns = collectPlatformRuns(workloadResult.runs);
      const platforms = Object.fromEntries(
        Object.entries(platformRuns).map(([platform, runs]) => [platform, aggregateRuns(runs)]),
      );
      return {
        workload: workloadResult.workload,
        runs: workloadResult.runs,
        summary: {
          platforms,
          comparisons: buildComparisons(platforms),
        },
      };
    }),
  };
}

function collectPlatformRuns(runs) {
  const platforms = {
    temporal: [],
    fabrik_durable: [],
    fabrik_throughput_pg_v1: [],
    fabrik_throughput_stream_v2: [],
  };
  for (const run of runs) {
    platforms.temporal.push(run.temporal);
    for (const fabrikScenario of run.fabrik.scenarios) {
      const label = FABRIK_SCENARIO_LABELS[fabrikScenario.scenario];
      if (label) {
        platforms[label].push(fabrikScenario);
      }
    }
  }
  return platforms;
}

function aggregateRuns(runs) {
  if (!runs.length) {
    return null;
  }
  return {
    runCount: runs.length,
    meanDurationMs: mean(runs.map((run) => run.durationMs)),
    meanExecutionDurationMs: mean(runs.map((run) => run.executionDurationMs ?? run.durationMs)),
    meanActivityThroughputPerSecond: mean(
      runs.map((run) => run.activityMetrics.throughputActivitiesPerSecond),
    ),
    meanScheduleToStartLatencyMs: mean(
      runs.map((run) => run.activityMetrics.avgScheduleToStartLatencyMs),
    ),
    meanStartToCloseLatencyMs: mean(
      runs.map((run) => run.activityMetrics.avgStartToCloseLatencyMs),
    ),
    workflowsCompleted: mean(runs.map((run) => run.workflowOutcomes.completed)),
    workflowsFailed: mean(runs.map((run) => run.workflowOutcomes.failed)),
    workflowsCancelled: mean(runs.map((run) => run.workflowOutcomes.cancelled)),
    activitiesCompleted: mean(runs.map((run) => run.activityMetrics.completed)),
    activitiesFailed: mean(runs.map((run) => run.activityMetrics.failed)),
    activitiesCancelled: mean(runs.map((run) => run.activityMetrics.cancelled)),
  };
}

function buildComparisons(platforms) {
  if (!platforms.temporal) {
    return {};
  }
  const comparisons = {};
  for (const platform of [
    "fabrik_durable",
    "fabrik_throughput_pg_v1",
    "fabrik_throughput_stream_v2",
  ]) {
    if (!platforms[platform]) {
      continue;
    }
    comparisons[platform] = {
      durationRatioVsTemporal: safeRatio(
        platforms[platform].meanDurationMs,
        platforms.temporal.meanDurationMs,
      ),
      throughputRatioVsTemporal: safeRatio(
        platforms[platform].meanActivityThroughputPerSecond,
        platforms.temporal.meanActivityThroughputPerSecond,
      ),
      scheduleToStartDeltaMs:
        platforms[platform].meanScheduleToStartLatencyMs -
        platforms.temporal.meanScheduleToStartLatencyMs,
      startToCloseDeltaMs:
        platforms[platform].meanStartToCloseLatencyMs -
        platforms.temporal.meanStartToCloseLatencyMs,
    };
  }
  return comparisons;
}

function mean(values) {
  if (!values.length) {
    return 0;
  }
  return values.reduce((sum, value) => sum + value, 0) / values.length;
}

function safeRatio(left, right) {
  if (!right) {
    return null;
  }
  return left / right;
}

export function formatComparisonSummary(report) {
  const lines = [
    `profile=${report.profile}`,
    `repetitions=${report.repetitions}`,
    `temporal_address=${report.temporalAddress}`,
    `temporal_namespace=${report.temporalNamespace}`,
  ];
  for (const workload of report.workloads) {
    lines.push("");
    lines.push(`[${workload.workload.name}] ${workload.workload.description}`);
    for (const [platform, summary] of Object.entries(workload.summary.platforms)) {
      if (!summary) {
        continue;
      }
      lines.push(
        `${platform}: duration_ms=${summary.meanDurationMs.toFixed(2)} throughput=${summary.meanActivityThroughputPerSecond.toFixed(2)} avg_schedule_to_start_ms=${summary.meanScheduleToStartLatencyMs.toFixed(2)} avg_start_to_close_ms=${summary.meanStartToCloseLatencyMs.toFixed(2)}`,
      );
    }
    for (const [platform, comparison] of Object.entries(workload.summary.comparisons)) {
      lines.push(
        `${platform}_vs_temporal: duration_ratio=${formatRatio(comparison.durationRatioVsTemporal)} throughput_ratio=${formatRatio(comparison.throughputRatioVsTemporal)} schedule_to_start_delta_ms=${comparison.scheduleToStartDeltaMs.toFixed(2)} start_to_close_delta_ms=${comparison.startToCloseDeltaMs.toFixed(2)}`,
      );
    }
  }
  return `${lines.join("\n")}\n`;
}

function formatRatio(value) {
  return value == null ? "n/a" : value.toFixed(3);
}
