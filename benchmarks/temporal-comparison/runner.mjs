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
const FABRIK_SCENARIOS = [
  {
    name: "unified-experiment",
    stackKey: "unified-experiment",
    stackExecutionMode: "unified",
    args: ["--execution-mode", "unified", "--bulk-reducer", "all_settled"],
  },
];

async function main() {
  const args = parseArgs(process.argv.slice(2));
  const repoRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..", "..");
  const manifestPath = path.resolve(repoRoot, args.manifest);
  const outputPath = path.resolve(repoRoot, args.output);
  const outputDir = path.dirname(outputPath);
  const workloads = await loadManifest(manifestPath, args.profile);
  const fabrikResults = await runFabrikMatrix({
    outputDir,
    profile: args.profile,
    repetitions: args.repetitions,
    repoRoot,
    workloads,
  });
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
        const fabrik = loadFabrikResult(fabrikResults, workload, repetition);
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
  const workflowLatenciesMs = [];

  await worker.runUntil(async () => {
    const handles = await mapWithConcurrency(
      Array.from({ length: workload.workflowCount }, (_, workflowIndex) => workflowIndex),
      workload.startConcurrency ?? 256,
      async (workflowIndex) => {
        const requestStarted = Date.now();
        const handle = await client.workflow.start(workflowTypeFor(workload), {
          args: [
            buildWorkflowInput(
              workload,
              workload.activitiesPerWorkflow,
              workload.payloadSize,
              workload.retryRate,
              workload.cancelRate,
              `${workflowIdPrefix}-${workflowIndex.toString().padStart(4, "0")}`,
            ),
          ],
          taskQueue,
          workflowId: `${workflowIdPrefix}-${workflowIndex.toString().padStart(4, "0")}`,
          workflowExecutionTimeout: `${workload.timeoutSecs}s`,
        });
        return { handle, requestStarted };
      },
    );

    if (workload.kind === "signal_gate") {
      await Promise.all(handles.map(({ handle }) => handle.signal("approve", { approved: true })));
    }
    if (workload.kind === "update_gate") {
      await Promise.all(
        handles.map(({ handle }) =>
          handle.executeUpdate("setValue", {
            args: [1],
          }),
        ),
      );
    }

    await mapWithConcurrency(
      handles,
      workload.resultConcurrency ?? workload.startConcurrency ?? 256,
      async ({ handle, requestStarted }) => {
        try {
          await handle.result();
          workflowOutcomes.completed += 1;
        } catch (error) {
          if (isWorkflowCancelled(error)) {
            workflowOutcomes.cancelled += 1;
          } else {
            workflowOutcomes.failed += 1;
          }
        } finally {
          workflowLatenciesMs.push(Date.now() - requestStarted);
        }
      },
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
    requestLatencyMetrics: summarizeLatencies(workflowLatenciesMs),
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

function summarizeLatencies(latenciesMs) {
  if (!latenciesMs.length) {
    return {
      requestCount: 0,
      avgRequestLatencyMs: 0,
      p50RequestLatencyMs: 0,
      p95RequestLatencyMs: 0,
      p99RequestLatencyMs: 0,
      maxRequestLatencyMs: 0,
    };
  }
  const sorted = [...latenciesMs].sort((left, right) => left - right);
  const percentile = (ratio) => {
    const index = Math.min(sorted.length - 1, Math.floor(ratio * sorted.length));
    return sorted[index];
  };
  const avgRequestLatencyMs =
    latenciesMs.reduce((sum, value) => sum + value, 0) / latenciesMs.length;
  return {
    requestCount: latenciesMs.length,
    avgRequestLatencyMs,
    p50RequestLatencyMs: percentile(0.50),
    p95RequestLatencyMs: percentile(0.95),
    p99RequestLatencyMs: percentile(0.99),
    maxRequestLatencyMs: sorted.at(-1) ?? 0,
  };
}

async function mapWithConcurrency(items, concurrency, mapper) {
  const limit = Math.max(1, Math.min(concurrency, items.length || 1));
  const results = new Array(items.length);
  let nextIndex = 0;

  await Promise.all(
    Array.from({ length: limit }, async () => {
      while (true) {
        const currentIndex = nextIndex;
        nextIndex += 1;
        if (currentIndex >= items.length) {
          return;
        }
        results[currentIndex] = await mapper(items[currentIndex], currentIndex);
      }
    }),
  );

  return results;
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

function workflowTypeFor(workload) {
  return workload.kind === "fanout" ? "fanoutBenchmarkWorkflow" : "comparisonBenchmarkWorkflow";
}

function buildWorkflowInput(workload, activitiesPerWorkflow, payloadSize, retryRate, cancelRate, workflowId) {
  const retryCount = Math.round(activitiesPerWorkflow * retryRate);
  const cancelCount = Math.round(activitiesPerWorkflow * cancelRate);
  const payload = "x".repeat(payloadSize);
  const input = {
    kind: workload.kind,
    items: Array.from({ length: activitiesPerWorkflow }, (_, index) => ({
      index,
      payload,
      fail_until_attempt: index < retryCount ? 1 : 0,
      cancel: index >= retryCount && index < retryCount + cancelCount,
    })),
  };
  if (workload.kind === "timer_gate") {
    input.timerSecs = workload.timerSecs ?? 1;
  }
  if (workload.kind === "continue_as_new") {
    input.remaining = workload.continueRounds ?? 1;
  }
  if (workload.kind === "child_workflow") {
    input.childWorkflowId = `${workflowId}/child`;
  }
  return input;
}

function fabrikScenariosForWorkload(workload) {
  return FABRIK_SCENARIOS;
}

function fabrikResultKey(workloadName, repetition) {
  return `${workloadName}::${repetition}`;
}

function loadFabrikResult(results, workload, repetition) {
  const reports = results.get(fabrikResultKey(workload.name, repetition));
  if (!reports) {
    throw new Error(
      `missing Fabrik reports for workload=${workload.name} repetition=${repetition}`,
    );
  }
  const expectedOrder = fabrikScenariosForWorkload(workload).map((scenario) => scenario.name);
  const reportByScenario = new Map(reports.map((report) => [report.scenario, report]));
  return {
    scenarios: expectedOrder.map((scenarioName) => {
      const report = reportByScenario.get(scenarioName);
      if (!report) {
        throw new Error(
          `missing Fabrik scenario=${scenarioName} for workload=${workload.name} repetition=${repetition}`,
        );
      }
      return report;
    }),
  };
}

async function runFabrikMatrix({ outputDir, profile, repetitions, repoRoot, workloads }) {
  const results = new Map();
  for (let repetition = 1; repetition <= repetitions; repetition += 1) {
    for (const workload of workloads) {
      const scenariosByStack = new Map();
      for (const scenario of fabrikScenariosForWorkload(workload)) {
        const current = scenariosByStack.get(scenario.stackKey) ?? [];
        current.push(scenario);
        scenariosByStack.set(scenario.stackKey, current);
      }
      for (const [stackKey, scenarios] of scenariosByStack) {
        const stackPlan = {
          key: stackKey,
          executionMode: scenarios[0].stackExecutionMode,
          maxWorkerCount: workload.workerCount,
        };
        const stack = await startFabrikStack({ repoRoot, stackPlan });
        try {
          for (const scenario of scenarios) {
            const report = await runFabrikScenario({
              outputDir,
              profile,
              repetition,
              repoRoot,
              scenario,
              stack,
              workload,
            });
            const key = fabrikResultKey(workload.name, repetition);
            const current = results.get(key) ?? [];
            current.push(report);
            results.set(key, current);
          }
        } finally {
          await stopFabrikStack({ repoRoot, runDir: stack.runDir });
        }
      }
    }
  }
  return results;
}

async function startFabrikStack({ repoRoot, stackPlan }) {
  const stackName = `temporal-compare-${stackPlan.key}-${Date.now()}`;
  const runDir = path.join(repoRoot, "target", "benchmark-stacks", stackName);
  const scriptPath = path.join(repoRoot, "scripts", "manage-benchmark-stack.sh");
  await runCommand(
    scriptPath,
    [
      "start",
      "--execution-mode",
      stackPlan.executionMode,
      "--namespace",
      stackName,
      "--run-dir",
      runDir,
      "--tenant-id",
      stackName,
      "--task-queue",
      "default",
    ],
    {
      cwd: repoRoot,
      env: {
        ...process.env,
        ACTIVITY_WORKER_CONCURRENCY: String(stackPlan.maxWorkerCount),
        BUILD_RELEASE_BINARIES: process.env.BUILD_RELEASE_BINARIES ?? "1",
        STREAM_ACTIVITY_WORKER_CONCURRENCY: String(stackPlan.maxWorkerCount),
      },
    },
  );
  const env = await readEnvFile(path.join(runDir, "environment.txt"));
  return { env, runDir };
}

async function stopFabrikStack({ repoRoot, runDir }) {
  const scriptPath = path.join(repoRoot, "scripts", "manage-benchmark-stack.sh");
  await runCommand(scriptPath, ["stop", "--run-dir", runDir], {
    cwd: repoRoot,
    env: process.env,
  });
}

async function readEnvFile(envPath) {
  const content = await fs.readFile(envPath, "utf8");
  const env = {};
  for (const line of content.split("\n")) {
    if (!line || line.startsWith("#")) {
      continue;
    }
    const separator = line.indexOf("=");
    if (separator === -1) {
      continue;
    }
    env[line.slice(0, separator)] = line.slice(separator + 1);
  }
  return env;
}

async function runFabrikScenario({ outputDir, profile, repetition, repoRoot, scenario, stack, workload }) {
  const workloadKey = sanitizeIdentifier(workload.name);
  const outputPath = path.join(
    outputDir,
    `${workloadKey}-fabrik-r${repetition}-${scenario.name}.json`,
  );
  const benchmarkRunnerPath = path.join(repoRoot, "target", "release", "benchmark-runner");
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
    "--workload-kind",
    workload.kind,
    "--timer-secs",
    String(workload.timerSecs ?? 1),
    "--continue-rounds",
    String(workload.continueRounds ?? 1),
    "--timeout-secs",
    String(workload.timeoutSecs),
    "--tenant-id",
    stack.env.TENANT_ID,
    "--task-queue",
    stack.env.TASK_QUEUE,
    "--output",
    outputPath,
  ];
  console.log(
    `[temporal-comparison] fabrik_stack=${scenario.stackKey} scenario=${scenario.name} workload=${workload.name} repetition=${repetition}`,
  );
  await runCommand(benchmarkRunnerPath, args, {
    cwd: repoRoot,
    env: {
      ...process.env,
      ...stack.env,
    },
  });
  const report = JSON.parse(await fs.readFile(outputPath, "utf8"));
  return {
    ...normalizeFabrikScenario(report),
    rawScenario: report.scenario,
    scenario: scenario.name,
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
