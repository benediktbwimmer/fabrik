import test from "node:test";
import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import path from "node:path";
import { promisify } from "node:util";

const execFileAsync = promisify(execFile);
const root = "/Users/bene/code/fabrik";
const compilerPath = path.join(root, "sdk/typescript-compiler/compiler.mjs");

async function runCompiler(args) {
  return execFileAsync("node", [compilerPath, ...args], {
    cwd: root,
  });
}

test("compiler emits source maps for compiled workflows", async () => {
  const fixture = path.join(root, "sdk/typescript-compiler/test-fixtures/valid-workflow.ts");
  const { stdout } = await runCompiler([
    "--entry",
    fixture,
    "--export",
    "validWorkflow",
    "--definition-id",
    "valid-workflow",
    "--version",
    "1",
  ]);
  const artifact = JSON.parse(stdout);
  assert.equal(artifact.definition_id, "valid-workflow");
  assert.ok(Object.keys(artifact.source_map).length > 0);
  assert.ok(artifact.source_files.some((file) => file.endsWith("valid-helper.ts")));
});

test("compiler lowers ctx.now and ctx.uuid expressions", async () => {
  const fixture = path.join(root, "sdk/typescript-compiler/test-fixtures/marker-workflow.ts");
  const { stdout } = await runCompiler([
    "--entry",
    fixture,
    "--export",
    "markerWorkflow",
    "--definition-id",
    "marker-workflow",
    "--version",
    "1",
  ]);
  const artifact = JSON.parse(stdout);
  const states = artifact.workflow.states;
  const assignStates = Object.values(states).filter((state) => state.type === "assign");
  const serialized = JSON.stringify(assignStates);

  assert.match(serialized, /"kind":"now"/);
  assert.match(serialized, /"kind":"uuid"/);
});

test("compiler lowers ctx.sideEffect expressions", async () => {
  const fixture = path.join(root, "sdk/typescript-compiler/test-fixtures/marker-workflow.ts");
  const { stdout } = await runCompiler([
    "--entry",
    fixture,
    "--export",
    "markerWorkflow",
    "--definition-id",
    "marker-workflow",
    "--version",
    "1",
  ]);
  const artifact = JSON.parse(stdout);
  const serialized = JSON.stringify(artifact.workflow.states);

  assert.match(serialized, /"kind":"side_effect"/);
  assert.match(serialized, /"marker_id":"marker_/);
});

test("compiler lowers awaited ctx.activity calls into activity states", async () => {
  const fixture = path.join(root, "sdk/typescript-compiler/test-fixtures/activity-workflow.ts");
  const { stdout } = await runCompiler([
    "--entry",
    fixture,
    "--export",
    "activityWorkflow",
    "--definition-id",
    "activity-workflow",
    "--version",
    "1",
  ]);
  const artifact = JSON.parse(stdout);
  const serialized = JSON.stringify(artifact.workflow.states);

  assert.match(serialized, /"type":"step"/);
  assert.match(serialized, /"handler":"core\.echo"/);
  assert.match(serialized, /"output_var":"echoed"/);
});

test("compiler lowers bulk activity handles and waits", async () => {
  const fixture = path.join(root, "sdk/typescript-compiler/test-fixtures/bulk-workflow.ts");
  const { stdout } = await runCompiler([
    "--entry",
    fixture,
    "--export",
    "bulkWorkflow",
    "--definition-id",
    "bulk-workflow",
    "--version",
    "1",
  ]);
  const artifact = JSON.parse(stdout);
  const serialized = JSON.stringify(artifact.workflow.states);

  assert.match(serialized, /"type":"start_bulk_activity"/);
  assert.match(serialized, /"activity_type":"benchmark\.echo"/);
  assert.match(serialized, /"execution_policy":"eager"/);
  assert.match(serialized, /"reducer":"collect_results"/);
  assert.match(serialized, /"chunk_size":128/);
  assert.match(serialized, /"type":"wait_for_bulk_activity"/);
  assert.match(serialized, /"output_var":"summary"/);
});

test("compiler rejects non-static bulk options", async () => {
  const fixture = path.join(root, "sdk/typescript-compiler/test-fixtures/invalid-bulk-workflow.ts");
  await assert.rejects(
    runCompiler([
      "--entry",
      fixture,
      "--export",
      "invalidBulkWorkflow",
      "--definition-id",
      "invalid-bulk",
      "--version",
      "1",
    ]),
    (error) => {
      const output = `${error.stdout ?? ""}${error.stderr ?? ""}`;
      assert.match(output, /chunkSize must be a numeric literal/);
      return true;
    },
  );
});

test("compiler rejects unsupported bulk backend options", async () => {
  const fixture = path.join(
    root,
    "sdk/typescript-compiler/test-fixtures/invalid-bulk-backend-workflow.ts",
  );
  await assert.rejects(
    runCompiler([
      "--entry",
      fixture,
      "--export",
      "invalidBulkBackendWorkflow",
      "--definition-id",
      "invalid-bulk-backend",
      "--version",
      "1",
    ]),
    (error) => {
      const output = `${error.stdout ?? ""}${error.stderr ?? ""}`;
      assert.match(output, /backend selection is server-controlled/);
      return true;
    },
  );
});

test("compiler lowers query, update, and child workflow handlers", async () => {
  const fixture = path.join(
    root,
    "sdk/typescript-compiler/test-fixtures/query-update-child-workflow.ts",
  );
  const { stdout } = await runCompiler([
    "--entry",
    fixture,
    "--export",
    "queryUpdateChildWorkflow",
    "--definition-id",
    "query-update-child-workflow",
    "--version",
    "1",
  ]);
  const artifact = JSON.parse(stdout);
  assert.ok(artifact.queries.summary);
  assert.equal(artifact.queries.summary.arg_name, "args");
  assert.ok(artifact.updates.approve);
  const serializedUpdateStates = JSON.stringify(artifact.updates.approve.states);
  assert.match(serializedUpdateStates, /"type":"start_child"/);
  assert.match(serializedUpdateStates, /"type":"wait_for_child"/);
  assert.match(serializedUpdateStates, /"parent_close_policy":"REQUEST_CANCEL"/);
});

test("compiler lowers Temporal-style proxyActivities, sleep, and plain return", async () => {
  const fixture = path.join(
    root,
    "sdk/typescript-compiler/test-fixtures/temporal-compatible-workflow.ts",
  );
  const { stdout } = await runCompiler([
    "--entry",
    fixture,
    "--export",
    "temporalCompatibleWorkflow",
    "--definition-id",
    "temporal-compatible-workflow",
    "--version",
    "1",
  ]);
  const artifact = JSON.parse(stdout);
  const serialized = JSON.stringify(artifact.workflow.states);

  assert.match(serialized, /"type":"wait_for_timer"/);
  assert.match(serialized, /"timer_ref":"2s"/);
  assert.match(serialized, /"type":"step"/);
  assert.match(serialized, /"handler":"benchmarkEcho"/);
  assert.match(serialized, /"start_to_close_timeout_ms":30000/);
  assert.match(serialized, /"max_attempts":3/);
  assert.match(serialized, /"delay":"5s"/);
  assert.match(serialized, /"non_retryable_error_types":\["ValidationError","FatalError"\]/);
  assert.match(serialized, /"type":"succeed"/);
  assert.ok(!serialized.includes("ctx.activity"));
});

test("compiler lowers Temporal continueAsNew and object proxy activity calls", async () => {
  const fixture = path.join(
    root,
    "sdk/typescript-compiler/test-fixtures/temporal-compatible-workflow.ts",
  );
  const { stdout } = await runCompiler([
    "--entry",
    fixture,
    "--export",
    "temporalContinueAsNewWorkflow",
    "--definition-id",
    "temporal-continue-as-new-workflow",
    "--version",
    "1",
  ]);
  const artifact = JSON.parse(stdout);
  const serialized = JSON.stringify(artifact.workflow.states);

  assert.match(serialized, /"type":"continue_as_new"/);
  assert.match(serialized, /"type":"step"/);
  assert.match(serialized, /"handler":"benchmarkEcho"/);
});

test("compiler lowers Temporal Promise.all proxy activity maps into fan-out plus barrier", async () => {
  const fixture = path.join(
    root,
    "sdk/typescript-compiler/test-fixtures/temporal-promise-all-workflow.ts",
  );
  const { stdout } = await runCompiler([
    "--entry",
    fixture,
    "--export",
    "temporalPromiseAllWorkflow",
    "--definition-id",
    "temporal-promise-all-workflow",
    "--version",
    "1",
  ]);
  const artifact = JSON.parse(stdout);
  const serialized = JSON.stringify(artifact.workflow.states);

  assert.match(serialized, /"type":"fan_out"/);
  assert.match(serialized, /"activity_type":"benchmarkEcho"/);
  assert.match(serialized, /"reducer":"collect_results"/);
  assert.match(serialized, /"type":"wait_for_all_activities"/);
  assert.match(serialized, /"output_var":"results"/);
});

test("compiler lowers return await Promise.all proxy fan-out", async () => {
  const fixture = path.join(
    root,
    "sdk/typescript-compiler/test-fixtures/temporal-promise-all-workflow.ts",
  );
  const { stdout } = await runCompiler([
    "--entry",
    fixture,
    "--export",
    "temporalPromiseAllReturnWorkflow",
    "--definition-id",
    "temporal-promise-all-return-workflow",
    "--version",
    "1",
  ]);
  const artifact = JSON.parse(stdout);
  const serialized = JSON.stringify(artifact.workflow.states);

  assert.match(serialized, /"type":"fan_out"/);
  assert.match(serialized, /"type":"wait_for_all_activities"/);
  assert.match(serialized, /"type":"succeed"/);
  assert.match(serialized, /"return_value_/);
});

test("compiler lowers Temporal Promise.allSettled fan-out and settled-array transforms", async () => {
  const fixture = path.join(
    root,
    "sdk/typescript-compiler/test-fixtures/temporal-all-settled-workflow.ts",
  );
  const { stdout } = await runCompiler([
    "--entry",
    fixture,
    "--export",
    "temporalAllSettledWorkflow",
    "--definition-id",
    "temporal-all-settled-workflow",
    "--version",
    "1",
  ]);
  const artifact = JSON.parse(stdout);
  const serialized = JSON.stringify(artifact.workflow.states);

  assert.match(serialized, /"type":"fan_out"/);
  assert.match(serialized, /"reducer":"collect_settled_results"/);
  assert.match(serialized, /"type":"wait_for_all_activities"/);
  assert.match(serialized, /"kind":"array_find"/);
  assert.match(serialized, /"kind":"array_map"/);
});

test("compiler lowers the real Temporal benchmark workflow module", async () => {
  const fixture = path.join(root, "benchmarks/temporal-comparison/workflows.mjs");
  const { stdout } = await runCompiler([
    "--entry",
    fixture,
    "--export",
    "fanoutBenchmarkWorkflow",
    "--definition-id",
    "temporal-benchmark-workflow",
    "--version",
    "1",
  ]);
  const artifact = JSON.parse(stdout);
  const serialized = JSON.stringify(artifact.workflow.states);

  assert.match(serialized, /"reducer":"collect_settled_results"/);
  assert.match(serialized, /"start_to_close_timeout_ms":30000/);
  assert.match(serialized, /"max_attempts":2/);
  assert.match(serialized, /"delay":"1s"/);
  assert.match(serialized, /"kind":"array_find"/);
  assert.match(serialized, /"kind":"array_map"/);
});

test("compiler lowers Temporal defineQuery/defineUpdate setHandler registrations", async () => {
  const fixture = path.join(
    root,
    "sdk/typescript-compiler/test-fixtures/temporal-query-update-workflow.ts",
  );
  const { stdout } = await runCompiler([
    "--entry",
    fixture,
    "--export",
    "temporalQueryUpdateWorkflow",
    "--definition-id",
    "temporal-query-update-workflow",
    "--version",
    "1",
  ]);
  const artifact = JSON.parse(stdout);
  assert.ok(artifact.queries.summary);
  assert.equal(artifact.queries.summary.arg_name, "args");
  assert.ok(artifact.updates.approve);
  const serializedUpdateStates = JSON.stringify(artifact.updates.approve.states);
  assert.match(serializedUpdateStates, /"type":"start_child"/);
  assert.match(serializedUpdateStates, /"type":"wait_for_child"/);
});

test("compiler lowers narrow Temporal signal handlers plus condition waits", async () => {
  const fixture = path.join(
    root,
    "sdk/typescript-compiler/test-fixtures/temporal-signal-condition-workflow.ts",
  );
  const { stdout } = await runCompiler([
    "--entry",
    fixture,
    "--export",
    "temporalSignalConditionWorkflow",
    "--definition-id",
    "temporal-signal-condition-workflow",
    "--version",
    "1",
  ]);
  const artifact = JSON.parse(stdout);
  const serialized = JSON.stringify(artifact.workflow.states);

  assert.ok(artifact.signals.approved);
  assert.match(serialized, /"type":"wait_for_condition"/);
  assert.match(serialized, /"condition":\{"kind":"identifier","name":"ready"\}/);
  const signalSerialized = JSON.stringify(artifact.signals);
  assert.match(signalSerialized, /"type":"assign"/);
  assert.match(signalSerialized, /"target":"ready"/);
  assert.match(signalSerialized, /"target":"payload"/);
});

test("compiler emits real compiled signal handler graphs for async Temporal signal handlers", async () => {
  const fixture = path.join(
    root,
    "sdk/typescript-compiler/test-fixtures/temporal-async-signal-workflow.ts",
  );
  const { stdout } = await runCompiler([
    "--entry",
    fixture,
    "--export",
    "temporalAsyncSignalWorkflow",
    "--definition-id",
    "temporal-async-signal-workflow",
    "--version",
    "1",
  ]);
  const artifact = JSON.parse(stdout);
  const serializedSignals = JSON.stringify(artifact.signals);

  assert.ok(artifact.signals.approved);
  assert.match(serializedSignals, /"initial_state"/);
  assert.match(serializedSignals, /"type":"step"/);
  assert.match(serializedSignals, /"handler":"benchmarkEcho"/);
  assert.match(serializedSignals, /"output_var":"echoed"/);
});

test("compiler lowers awaited reassignment statements", async () => {
  const fixture = path.join(
    root,
    "sdk/typescript-compiler/test-fixtures/temporal-await-assignment-workflow.ts",
  );
  const { stdout } = await runCompiler([
    "--entry",
    fixture,
    "--export",
    "temporalAwaitAssignmentWorkflow",
    "--definition-id",
    "temporal-await-assignment-workflow",
    "--version",
    "1",
  ]);
  const artifact = JSON.parse(stdout);
  const serialized = JSON.stringify(artifact.workflow.states);

  assert.match(serialized, /"type":"step"/);
  assert.match(serialized, /"handler":"benchmarkEcho"/);
  assert.match(serialized, /"output_var":"echoed"/);
});

test("compiler lowers narrow Temporal child workflow APIs", async () => {
  const fixture = path.join(
    root,
    "sdk/typescript-compiler/test-fixtures/temporal-child-workflow.ts",
  );
  const { stdout } = await runCompiler([
    "--entry",
    fixture,
    "--export",
    "temporalChildWorkflow",
    "--definition-id",
    "temporal-child-workflow",
    "--version",
    "1",
  ]);
  const artifact = JSON.parse(stdout);
  const serialized = JSON.stringify(artifact.workflow.states);

  assert.match(serialized, /"type":"start_child"/);
  assert.match(serialized, /"child_definition_id":"childWorkflow"/);
  assert.match(serialized, /"child_definition_id":"namedChildWorkflow"/);
  assert.match(serialized, /"workflow_id":\{"kind":"literal","value":"child-started"\}/);
  assert.match(serialized, /"task_queue":\{"kind":"literal","value":"payments"\}/);
  assert.match(serialized, /"parent_close_policy":"REQUEST_CANCEL"/);
  assert.match(serialized, /"type":"wait_for_child"/);
  assert.match(serialized, /"output_var":"childResult"/);
  assert.match(serialized, /"output_var":"directResult"/);
});

test("compiler lowers multi-argument Temporal activity and continueAsNew calls", async () => {
  const fixture = path.join(
    root,
    "sdk/typescript-compiler/test-fixtures/temporal-multi-arg-workflow.ts",
  );

  const [{ stdout: activityStdout }, { stdout: continueStdout }] = await Promise.all([
    runCompiler([
      "--entry",
      fixture,
      "--export",
      "temporalMultiArgWorkflow",
      "--definition-id",
      "temporal-multi-arg-workflow",
      "--version",
      "1",
    ]),
    runCompiler([
      "--entry",
      fixture,
      "--export",
      "temporalContinueAsNewArgsWorkflow",
      "--definition-id",
      "temporal-continue-as-new-args-workflow",
      "--version",
      "1",
    ]),
  ]);

  const activityArtifact = JSON.parse(activityStdout);
  const continueArtifact = JSON.parse(continueStdout);
  const activitySerialized = JSON.stringify(activityArtifact.workflow.states);
  const continueSerialized = JSON.stringify(continueArtifact.workflow.states);

  assert.match(activitySerialized, /"type":"step"/);
  assert.match(activitySerialized, /"handler":"joinValues"/);
  assert.match(activitySerialized, /"input":\{"kind":"array"/);
  assert.match(activitySerialized, /"property":"left"/);
  assert.match(activitySerialized, /"property":"right"/);
  assert.match(activitySerialized, /"value":3/);

  assert.match(continueSerialized, /"type":"continue_as_new"/);
  assert.match(continueSerialized, /"input":\{"kind":"array"/);
  assert.match(continueSerialized, /"property":"id"/);
  assert.match(continueSerialized, /"value":false/);
  assert.match(continueSerialized, /"attempt"/);
});

test("compiler rejects forbidden global access with source location", async () => {
  const fixture = path.join(root, "sdk/typescript-compiler/test-fixtures/invalid-global.ts");
  await assert.rejects(
    runCompiler([
      "--entry",
      fixture,
      "--export",
      "invalidGlobalWorkflow",
      "--definition-id",
      "invalid-global",
      "--version",
      "1",
    ]),
    (error) => {
      const output = `${error.stdout ?? ""}${error.stderr ?? ""}`;
      assert.match(output, /sdk\/typescript-compiler\/test-fixtures\/invalid-global\.ts:\d+:\d+/);
      assert.match(output, /unsupported global access process/);
      return true;
    },
  );
});

test("compiler rejects unsupported helper bodies with helper file location", async () => {
  const fixture = path.join(root, "sdk/typescript-compiler/test-fixtures/invalid-helper-workflow.ts");
  await assert.rejects(
    runCompiler([
      "--entry",
      fixture,
      "--export",
      "invalidHelperWorkflow",
      "--definition-id",
      "invalid-helper",
      "--version",
      "1",
    ]),
    (error) => {
      const output = `${error.stdout ?? ""}${error.stderr ?? ""}`;
      assert.match(
        output,
        /sdk\/typescript-compiler\/test-fixtures\/invalid-helper\.ts:\d+:\d+/,
      );
      assert.match(output, /unsupported expression|unsupported global access/);
      return true;
    },
  );
});

test("compiler keeps state ids stable across unrelated statements", async () => {
  const baseFixture = path.join(root, "sdk/typescript-compiler/test-fixtures/stable-ids-base.ts");
  const shiftedFixture = path.join(
    root,
    "sdk/typescript-compiler/test-fixtures/stable-ids-shifted.ts",
  );

  const [{ stdout: baseOutput }, { stdout: shiftedOutput }] = await Promise.all([
    runCompiler([
      "--entry",
      baseFixture,
      "--export",
      "stableIdsWorkflow",
      "--definition-id",
      "stable-ids",
      "--version",
      "1",
    ]),
    runCompiler([
      "--entry",
      shiftedFixture,
      "--export",
      "stableIdsWorkflow",
      "--definition-id",
      "stable-ids",
      "--version",
      "1",
    ]),
  ]);

  const baseArtifact = JSON.parse(baseOutput);
  const shiftedArtifact = JSON.parse(shiftedOutput);
  const baseStateIds = Object.keys(baseArtifact.workflow.states).sort();
  const shiftedStateIds = Object.keys(shiftedArtifact.workflow.states).sort();

  assert.equal(shiftedStateIds.length, baseStateIds.length + 1);
  baseStateIds.forEach((stateId) => {
    assert.ok(shiftedStateIds.includes(stateId), `missing stable state id ${stateId}`);
  });
});

test("compiler explains non-ctx await failures", async () => {
  const fixture = path.join(root, "sdk/typescript-compiler/test-fixtures/invalid-await.ts");
  await assert.rejects(
    runCompiler([
      "--entry",
      fixture,
      "--export",
      "invalidAwaitWorkflow",
      "--definition-id",
      "invalid-await",
      "--version",
      "1",
    ]),
    (error) => {
      const output = `${error.stdout ?? ""}${error.stderr ?? ""}`;
      assert.match(output, /non-deterministic await detected/);
      assert.match(output, /ctx\.\* methods/);
      return true;
    },
  );
});

test("compiler rejects async query handlers", async () => {
  const fixture = path.join(root, "sdk/typescript-compiler/test-fixtures/invalid-query-await.ts");
  await assert.rejects(
    runCompiler([
      "--entry",
      fixture,
      "--export",
      "invalidQueryAwaitWorkflow",
      "--definition-id",
      "invalid-query-await",
      "--version",
      "1",
    ]),
    (error) => {
      const output = `${error.stdout ?? ""}${error.stderr ?? ""}`;
      assert.match(output, /ctx\.query handlers must not be async/);
      return true;
    },
  );
});
