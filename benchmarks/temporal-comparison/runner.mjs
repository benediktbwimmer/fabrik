import fs from "node:fs/promises";
import path from "node:path";
import { spawn } from "node:child_process";
import { fileURLToPath } from "node:url";

import { ApplicationFailure, Context } from "@temporalio/activity";
import { Client } from "@temporalio/client";
import { NativeConnection, Worker } from "@temporalio/worker";

import {
  buildComparisonReport,
  formatComparisonSummary,
  loadManifest,
  sanitizeIdentifier,
  writeJson,
  writeText,
} from "./helpers.mjs";

const BENCHMARK_FAILURE_TYPE = "BenchmarkConfiguredFailure";
const BENCHMARK_CANCELLED_TYPE = "BenchmarkCancelled";

async function main() {
  const args = parseArgs(process.argv.slice(2));
  const repoRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..", "..");
  const manifestPath = path.resolve(repoRoot, args.manifest);
  const outputPath = path.resolve(repoRoot, args.output);
  const outputDir = path.dirname(outputPath);
  const workloads = await loadManifest(manifestPath, args.profile);
  const connection = await connectWithRetry(args.temporalAddress);

  try {
    const workloadResults = [];
    for (const workload of workloads) {
      const runs = [];
      for (let repetition = 1; repetition <= args.repetitions; repetition += 1) {
        console.log(
          `[temporal-comparison] workload=${workload.name} repetition=${repetition}/${args.repetitions}`,
        );
        const temporalReportPath = path.join(
          outputDir,
          `${sanitizeIdentifier(workload.name)}-temporal-r${repetition}.json`,
        );
        const temporal = await runTemporalWorkload({
          connection,
          outputPath: temporalReportPath,
          profile: args.profile,
          repetition,
          temporalAddress: args.temporalAddress,
          temporalNamespace: args.temporalNamespace,
          workload,
        });
        const fabrik = await runFabrikWorkload({
          outputDir,
          profile: args.profile,
          repoRoot,
          repetition,
          workload,
        });
        runs.push({
          repetition,
          temporal,
          fabrik,
        });
      }
      workloadResults.push({
        workload,
        runs,
      });
    }

    const comparisonReport = buildComparisonReport({
      generatedAt: new Date().toISOString(),
      profile: args.profile,
      manifestPath,
      repetitions: args.repetitions,
      temporalAddress: args.temporalAddress,
      temporalNamespace: args.temporalNamespace,
      workloads: workloadResults,
    });
    await writeJson(outputPath, comparisonReport);
    await writeText(summaryPathFor(outputPath), formatComparisonSummary(comparisonReport));
    console.log(`[temporal-comparison] report_path=${outputPath}`);
  } finally {
    await connection.close();
  }
}

function parseArgs(argv) {
  const args = {
    manifest: "benchmarks/temporal-comparison/workloads.json",
    output: "target/benchmark-reports/temporal-comparison-smoke.json",
    profile: "smoke",
    repetitions: 1,
    temporalAddress: process.env.TEMPORAL_ADDRESS ?? "127.0.0.1:7233",
    temporalNamespace: process.env.TEMPORAL_NAMESPACE ?? "default",
  };

  for (let index = 0; index < argv.length; index += 2) {
    const flag = argv[index];
    const value = argv[index + 1];
    if (!value) {
      throw new Error(`missing value for ${flag}`);
    }
    switch (flag) {
      case "--manifest":
        args.manifest = value;
        break;
      case "--output":
        args.output = value;
        break;
      case "--profile":
        args.profile = value;
        if (!argv.includes("--output")) {
          args.output = `target/benchmark-reports/temporal-comparison-${value}.json`;
        }
        break;
      case "--repetitions":
        args.repetitions = Number.parseInt(value, 10);
        break;
      case "--temporal-address":
        args.temporalAddress = value;
        break;
      case "--temporal-namespace":
        args.temporalNamespace = value;
        break;
      default:
        throw new Error(`unknown argument ${flag}`);
    }
  }

  return args;
}

async function connectWithRetry(address) {
  let lastError;
  for (let attempt = 1; attempt <= 20; attempt += 1) {
    try {
      return await NativeConnection.connect({ address });
    } catch (error) {
      lastError = error;
      await sleep(1000);
    }
  }
  throw lastError;
}

async function runTemporalWorkload({
  connection,
  outputPath,
  profile,
  repetition,
  temporalAddress,
  temporalNamespace,
  workload,
}) {
  const workloadKey = sanitizeIdentifier(workload.name);
  const runKey = `${workloadKey}-r${repetition}-${Date.now()}`;
  const taskQueue = `temporal-compare-${runKey}`;
  const workflowIdPrefix = `temporal-compare-${runKey}`;
  const tracker = createActivityTracker();
  const worker = await Worker.create({
    connection,
    namespace: temporalNamespace,
    taskQueue,
    workflowsPath: path.resolve(path.dirname(fileURLToPath(import.meta.url)), "workflows.mjs"),
    activities: createActivities(tracker),
    maxConcurrentActivityTaskExecutions: workload.workerCount,
    maxConcurrentWorkflowTaskExecutions: workload.workerCount,
    maxCachedWorkflows: workload.workflowCount,
    shutdownGraceTime: "10s",
  });
  const client = new Client({ connection, namespace: temporalNamespace });
  const startedAt = new Date();
  const started = Date.now();
  const workflowOutcomes = {
    completed: 0,
    failed: 0,
    cancelled: 0,
    running: 0,
  };

  await worker.runUntil(async () => {
    const handles = await Promise.all(
      Array.from({ length: workload.workflowCount }, (_, workflowIndex) =>
        client.workflow.start("fanoutBenchmarkWorkflow", {
          args: [
            buildWorkflowInput(
              workload.activitiesPerWorkflow,
              workload.payloadSize,
              workload.retryRate,
              workload.cancelRate,
            ),
          ],
          taskQueue,
          workflowId: `${workflowIdPrefix}-${workflowIndex.toString().padStart(4, "0")}`,
          workflowExecutionTimeout: `${workload.timeoutSecs}s`,
        }),
      ),
    );

    await Promise.all(
      handles.map(async (handle) => {
        try {
          await handle.result();
          workflowOutcomes.completed += 1;
        } catch (error) {
          if (isWorkflowCancelled(error)) {
            workflowOutcomes.cancelled += 1;
          } else {
            workflowOutcomes.failed += 1;
          }
        }
      }),
    );
  });

  const completedAt = new Date();
  const durationMs = Date.now() - started;
  const report = {
    platform: "temporal",
    scenario: "temporal",
    profile,
    description: workload.description,
    temporalAddress,
    temporalNamespace,
    taskQueue,
    startedAt: startedAt.toISOString(),
    executionCompletedAt: completedAt.toISOString(),
    completedAt: completedAt.toISOString(),
    executionDurationMs: durationMs,
    projectionConvergenceDurationMs: 0,
    durationMs,
    workflowCount: workload.workflowCount,
    activitiesPerWorkflow: workload.activitiesPerWorkflow,
    totalActivities: workload.workflowCount * workload.activitiesPerWorkflow,
    workerCount: workload.workerCount,
    payloadSize: workload.payloadSize,
    retryRate: workload.retryRate,
    cancelRate: workload.cancelRate,
    workflowOutcomes,
    activityMetrics: tracker.toActivityMetrics(
      durationMs,
      workload.workflowCount * workload.activitiesPerWorkflow,
    ),
    attemptMetrics: tracker.toAttemptMetrics(),
  };
  await writeJson(outputPath, report);
  await writeText(summaryPathFor(outputPath), temporalSummary(report));
  return {
    ...report,
    reportPath: outputPath,
  };
}

function createActivityTracker() {
  const activityState = new Map();
  let totalAttempts = 0;
  let retriedAttempts = 0;

  return {
    start(info) {
      totalAttempts += 1;
      if (info.attempt > 1) {
        retriedAttempts += 1;
      }
      const key = `${info.workflowExecution.workflowId}:${info.activityId}`;
      if (!activityState.has(key)) {
        activityState.set(key, {
          terminalStatus: null,
          scheduleToStartLatencyMs: 0,
          startToCloseLatencyMs: 0,
        });
      }
      return key;
    },
    complete(key, status, scheduleToStartLatencyMs, startToCloseLatencyMs) {
      activityState.set(key, {
        terminalStatus: status,
        scheduleToStartLatencyMs,
        startToCloseLatencyMs,
      });
    },
    toActivityMetrics(durationMs, totalActivities) {
      let completed = 0;
      let failed = 0;
      let cancelled = 0;
      let scheduleToStartSum = 0;
      let startToCloseSum = 0;
      let maxScheduleToStartLatencyMs = 0;
      let maxStartToCloseLatencyMs = 0;
      for (const state of activityState.values()) {
        if (!state.terminalStatus) {
          continue;
        }
        if (state.terminalStatus === "completed") {
          completed += 1;
        } else if (state.terminalStatus === "failed") {
          failed += 1;
        } else if (state.terminalStatus === "cancelled") {
          cancelled += 1;
        }
        scheduleToStartSum += state.scheduleToStartLatencyMs;
        startToCloseSum += state.startToCloseLatencyMs;
        maxScheduleToStartLatencyMs = Math.max(
          maxScheduleToStartLatencyMs,
          state.scheduleToStartLatencyMs,
        );
        maxStartToCloseLatencyMs = Math.max(
          maxStartToCloseLatencyMs,
          state.startToCloseLatencyMs,
        );
      }
      const terminalCount = completed + failed + cancelled;
      return {
        completed,
        failed,
        cancelled,
        timedOut: 0,
        avgScheduleToStartLatencyMs: terminalCount ? scheduleToStartSum / terminalCount : 0,
        maxScheduleToStartLatencyMs,
        avgStartToCloseLatencyMs: terminalCount ? startToCloseSum / terminalCount : 0,
        maxStartToCloseLatencyMs,
        throughputActivitiesPerSecond:
          durationMs === 0 ? 0 : totalActivities / (durationMs / 1000),
      };
    },
    toAttemptMetrics() {
      return {
        totalAttempts,
        retriedAttempts,
      };
    },
  };
}

function createActivities(tracker) {
  return {
    async benchmarkEcho(input) {
      const context = Context.current();
      const info = context.info;
      const activityKey = tracker.start(info);
      const startedAt = Date.now();
      const scheduleToStartLatencyMs = startedAt - info.currentAttemptScheduledTimestampMs;
      const finish = (status) => {
        tracker.complete(activityKey, status, scheduleToStartLatencyMs, Date.now() - startedAt);
      };

      if (input.cancel) {
        finish("cancelled");
        throw ApplicationFailure.nonRetryable("activity cancelled", BENCHMARK_CANCELLED_TYPE);
      }

      if (info.attempt <= input.fail_until_attempt) {
        const maxAttempts = info.retryPolicy?.maximumAttempts ?? Number.POSITIVE_INFINITY;
        if (info.attempt >= maxAttempts) {
          finish("failed");
        }
        throw ApplicationFailure.retryable(
          `benchmark configured failure on attempt ${info.attempt}`,
          BENCHMARK_FAILURE_TYPE,
        );
      }

      finish("completed");
      return input;
    },
  };
}

function isWorkflowCancelled(error) {
  return error?.name === "CancelledFailure";
}

function buildWorkflowInput(activitiesPerWorkflow, payloadSize, retryRate, cancelRate) {
  const retryCount = Math.round(activitiesPerWorkflow * retryRate);
  const cancelCount = Math.round(activitiesPerWorkflow * cancelRate);
  const payload = "x".repeat(payloadSize);
  return {
    items: Array.from({ length: activitiesPerWorkflow }, (_, index) => ({
      index,
      payload,
      fail_until_attempt: index < retryCount ? 1 : 0,
      cancel: index >= retryCount && index < retryCount + cancelCount,
    })),
  };
}

async function runFabrikWorkload({ outputDir, profile, repoRoot, repetition, workload }) {
  const workloadKey = sanitizeIdentifier(workload.name);
  const scriptPath = path.join(repoRoot, "scripts", "run-isolated-benchmark.sh");
  const scenarios = [
    {
      name: "durable",
      args: ["--execution-mode", "durable", "--bulk-reducer", "all_settled"],
    },
    {
      name: "throughput-pg-v1",
      args: ["--execution-mode", "throughput", "--throughput-backend", "pg-v1"],
    },
    {
      name: "throughput-stream-v2",
      args: ["--execution-mode", "throughput", "--throughput-backend", "stream-v2"],
    },
  ];
  const reports = [];

  for (const scenario of scenarios) {
    const namespace = `temporal-compare-${workloadKey}-${scenario.name}-r${repetition}-${Date.now()}`;
    const outputPath = path.join(
      outputDir,
      `${workloadKey}-fabrik-r${repetition}-${scenario.name}.json`,
    );
    const args = [
      ...scenario.args,
      "--profile",
      profile,
      "--workflow-count",
      String(workload.workflowCount),
      "--activities-per-workflow",
      String(workload.activitiesPerWorkflow),
      "--payload-size",
      String(workload.payloadSize),
      "--worker-count",
      String(workload.workerCount),
      "--retry-rate",
      String(workload.retryRate),
      "--cancel-rate",
      String(workload.cancelRate),
      "--timeout-secs",
      String(workload.timeoutSecs),
      "--output",
      outputPath,
    ];
    await runCommand(scriptPath, args, {
      cwd: repoRoot,
      env: {
        ...process.env,
        ACTIVITY_WORKER_CONCURRENCY: String(workload.workerCount),
        BENCHMARK_NAMESPACE: namespace,
        BUILD_RELEASE_BINARIES: "0",
        STREAM_ACTIVITY_WORKER_CONCURRENCY: String(workload.workerCount),
      },
    });
    reports.push(JSON.parse(await fs.readFile(outputPath, "utf8")));
  }

  return {
    scenarios: reports.map(normalizeFabrikScenario),
  };
}

function runCommand(command, args, options) {
  return new Promise((resolve, reject) => {
    const child = spawn(command, args, {
      stdio: "inherit",
      ...options,
    });
    child.on("error", reject);
    child.on("exit", (code) => {
      if (code === 0) {
        resolve();
        return;
      }
      reject(new Error(`${command} exited with code ${code}`));
    });
  });
}

function summaryPathFor(outputPath) {
  return outputPath.replace(/\.json$/u, ".txt");
}

function normalizeFabrikScenario(report) {
  return {
    scenario: report.scenario,
    durationMs: report.duration_ms,
    executionDurationMs: report.execution_duration_ms,
    workflowOutcomes: {
      completed: report.workflow_outcomes.completed,
      failed: report.workflow_outcomes.failed,
      cancelled: report.workflow_outcomes.cancelled,
    },
    activityMetrics: {
      completed: report.activity_metrics.completed,
      failed: report.activity_metrics.failed,
      cancelled: report.activity_metrics.cancelled,
      avgScheduleToStartLatencyMs: report.activity_metrics.avg_schedule_to_start_latency_ms,
      avgStartToCloseLatencyMs: report.activity_metrics.avg_start_to_close_latency_ms,
      throughputActivitiesPerSecond: report.activity_metrics.throughput_activities_per_second,
    },
    rawReport: report,
  };
}

function temporalSummary(report) {
  return [
    `platform=${report.platform}`,
    `scenario=${report.scenario}`,
    `workflows=${report.workflowCount}`,
    `activities_per_workflow=${report.activitiesPerWorkflow}`,
    `duration_ms=${report.durationMs}`,
    `activity_throughput_per_second=${report.activityMetrics.throughputActivitiesPerSecond.toFixed(2)}`,
    `completed_workflows=${report.workflowOutcomes.completed}`,
    `failed_workflows=${report.workflowOutcomes.failed}`,
    `cancelled_workflows=${report.workflowOutcomes.cancelled}`,
    `completed_activities=${report.activityMetrics.completed}`,
    `failed_activities=${report.activityMetrics.failed}`,
    `cancelled_activities=${report.activityMetrics.cancelled}`,
    `avg_schedule_to_start_ms=${report.activityMetrics.avgScheduleToStartLatencyMs.toFixed(2)}`,
    `avg_start_to_close_ms=${report.activityMetrics.avgStartToCloseLatencyMs.toFixed(2)}`,
  ].join("\n") + "\n";
}

function sleep(ms) {
  return new Promise((resolve) => {
    setTimeout(resolve, ms);
  });
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
