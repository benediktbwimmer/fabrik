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

test("compiler lowers ctx.version expressions", async () => {
  const fixture = path.join(root, "sdk/typescript-compiler/test-fixtures/version-workflow.ts");
  const { stdout } = await runCompiler([
    "--entry",
    fixture,
    "--export",
    "versionWorkflow",
    "--definition-id",
    "version-workflow",
    "--version",
    "1",
  ]);
  const artifact = JSON.parse(stdout);
  const serialized = JSON.stringify(artifact.workflow.states);

  assert.match(serialized, /"kind":"version"/);
  assert.match(serialized, /"change_id":"feature-x"/);
  assert.match(serialized, /"min_supported":1/);
  assert.match(serialized, /"max_supported":3/);
});

test("compiler lowers Temporal patched APIs onto version markers", async () => {
  const fixture = path.join(
    root,
    "crates/fabrik-cli/test-fixtures/temporal-patching-qualified/src/workflows.ts",
  );
  const { stdout } = await runCompiler([
    "--entry",
    fixture,
    "--export",
    "patchedWorkflow",
    "--definition-id",
    "patched-workflow",
    "--version",
    "1",
  ]);
  const artifact = JSON.parse(stdout);
  const serialized = JSON.stringify(artifact.workflow.states);

  assert.match(serialized, /"kind":"version"/);
  assert.match(serialized, /"change_id":"feature-x"/);
  assert.match(serialized, /"max_supported":1/);
});

test("compiler accepts static top-level Temporal setWorkflowOptions annotations", async () => {
  const fixture = path.join(
    root,
    "crates/fabrik-cli/test-fixtures/temporal-worker-versioning-qualified/src/workflows.ts",
  );
  const { stdout } = await runCompiler([
    "--entry",
    fixture,
    "--export",
    "autoUpgradeWorkflow",
    "--definition-id",
    "auto-upgrade-workflow",
    "--version",
    "1",
  ]);
  const artifact = JSON.parse(stdout);
  const serialized = JSON.stringify(artifact.workflow.states);

  assert.match(serialized, /"kind":"version"/);
  assert.match(serialized, /"handler":"greet"/);
});

test("compiler lowers Temporal search attribute upserts onto durable workflow effects", async () => {
  const fixture = path.join(
    root,
    "sdk/typescript-compiler/test-fixtures/temporal-search-attributes-workflow.ts",
  );
  const { stdout } = await runCompiler([
    "--entry",
    fixture,
    "--export",
    "searchAttributesWorkflow",
    "--definition-id",
    "search-attributes-workflow",
    "--version",
    "1",
  ]);
  const artifact = JSON.parse(stdout);
  const serialized = JSON.stringify(artifact.workflow.states);

  assert.match(serialized, /"target":"__search_attributes"/);
  assert.match(serialized, /"callee":"__builtin_search_attributes_upsert"/);
});

test("compiler lowers Temporal ActivityFailure and ApplicationFailure instanceof checks", async () => {
  const fixture = path.join(
    root,
    "crates/fabrik-cli/test-fixtures/temporal-activity-failure-qualified/src/workflows.ts",
  );
  const { stdout } = await runCompiler([
    "--entry",
    fixture,
    "--export",
    "activityFailureWorkflow",
    "--definition-id",
    "activity-failure-workflow",
    "--version",
    "1",
  ]);
  const artifact = JSON.parse(stdout);
  const serialized = JSON.stringify(artifact.workflow.states);

  assert.match(serialized, /"callee":"__temporal_is_activity_failure"/);
  assert.match(serialized, /"callee":"__temporal_is_application_failure"/);
});

test("compiler ignores imported helper factories used only in Temporal ReturnType annotations", async () => {
  const fixture = path.join(
    root,
    "sdk/typescript-compiler/test-fixtures/temporal-imported-helper-type-workflow.ts",
  );
  const { stdout } = await runCompiler([
    "--entry",
    fixture,
    "--export",
    "temporalImportedHelperTypeWorkflow",
    "--definition-id",
    "temporal-imported-helper-type-workflow",
    "--version",
    "1",
  ]);
  const artifact = JSON.parse(stdout);
  const serialized = JSON.stringify(artifact.workflow.states);

  assert.match(serialized, /"handler":"greet"/);
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

test("compiler lowers supported Temporal workflow APIs and namespace imports", async () => {
  const fixture = path.join(
    root,
    "sdk/typescript-compiler/test-fixtures/temporal-supported-api-workflow.ts",
  );
  const { stdout } = await runCompiler([
    "--entry",
    fixture,
    "--export",
    "temporalSupportedApiWorkflow",
    "--definition-id",
    "temporal-supported-api-workflow",
    "--version",
    "1",
  ]);
  const artifact = JSON.parse(stdout);
  const serialized = JSON.stringify(artifact.workflow.states);

  assert.match(serialized, /"kind":"workflow_info"/);
  assert.match(serialized, /"kind":"uuid"/);
  assert.match(serialized, /"parent_close_policy":"ABANDON"/);
  assert.match(serialized, /"type":"fail"/);
  assert.ok(!serialized.includes("wf.log"));
});

test("compiler lowers switch statements and awaited Temporal continueAsNew", async () => {
  const fixture = path.join(
    root,
    "sdk/typescript-compiler/test-fixtures/temporal-switch-workflow.ts",
  );
  const { stdout } = await runCompiler([
    "--entry",
    fixture,
    "--export",
    "temporalSwitchWorkflow",
    "--definition-id",
    "temporal-switch-workflow",
    "--version",
    "1",
  ]);
  const artifact = JSON.parse(stdout);
  const serialized = JSON.stringify(artifact.workflow.states);

  assert.match(serialized, /"type":"choice"/);
  assert.match(serialized, /"op":"equal"/);
  assert.match(serialized, /"type":"continue_as_new"/);
  assert.match(serialized, /"ApplicationFailure"/);
});

test("compiler lowers Date.now, Object.keys, and array join expressions", async () => {
  const fixture = path.join(
    root,
    "sdk/typescript-compiler/test-fixtures/temporal-expression-builtins-workflow.ts",
  );
  const { stdout } = await runCompiler([
    "--entry",
    fixture,
    "--export",
    "temporalExpressionBuiltinsWorkflow",
    "--definition-id",
    "temporal-expression-builtins-workflow",
    "--version",
    "1",
  ]);
  const artifact = JSON.parse(stdout);
  const serialized = JSON.stringify(artifact.workflow.states);

  assert.match(serialized, /"kind":"now"/);
  assert.match(serialized, /"callee":"__builtin_object_keys"/);
  assert.match(serialized, /"callee":"__builtin_array_map_number"/);
  assert.match(serialized, /"callee":"__builtin_array_sort_numeric_asc"/);
  assert.match(serialized, /"callee":"__builtin_array_join"/);
});

test("compiler lowers Math.random and richer Temporal retry options", async () => {
  const fixture = path.join(
    root,
    "sdk/typescript-compiler/test-fixtures/temporal-random-retry-workflow.ts",
  );
  const { stdout } = await runCompiler([
    "--entry",
    fixture,
    "--export",
    "temporalRandomRetryWorkflow",
    "--definition-id",
    "temporal-random-retry-workflow",
    "--version",
    "1",
  ]);
  const artifact = JSON.parse(stdout);
  const serialized = JSON.stringify(artifact.workflow.states);

  assert.match(serialized, /"kind":"random"/);
  assert.match(serialized, /"maximum_interval":"5 seconds"/);
  assert.match(serialized, /"backoff_coefficient_millis":1500/);
});

test("compiler materializes Temporal retry defaults for sparse retry overrides", async () => {
  const fixture = path.join(
    root,
    "sdk/typescript-compiler/test-fixtures/temporal-sparse-retry-workflow.ts",
  );
  const { stdout } = await runCompiler([
    "--entry",
    fixture,
    "--export",
    "temporalSparseRetryWorkflow",
    "--definition-id",
    "temporal-sparse-retry-workflow",
    "--version",
    "1",
  ]);
  const artifact = JSON.parse(stdout);
  const serialized = JSON.stringify(artifact.workflow.states);

  assert.match(serialized, /"maximum_interval":"5 seconds"/);
  assert.match(serialized, /"max_attempts":4294967295/);
  assert.match(serialized, /"delay":"1s"/);
  assert.match(serialized, /"backoff_coefficient_millis":2000/);
});

test("compiler lowers dynamic proxy activity member calls with spread args", async () => {
  const fixture = path.join(
    root,
    "sdk/typescript-compiler/test-fixtures/temporal-dynamic-activity-workflow.ts",
  );
  const { stdout } = await runCompiler([
    "--entry",
    fixture,
    "--export",
    "temporalDynamicActivityWorkflow",
    "--definition-id",
    "temporal-dynamic-activity-workflow",
    "--version",
    "1",
  ]);
  const artifact = JSON.parse(stdout);
  const serialized = JSON.stringify(artifact.workflow.states);

  assert.match(serialized, /"type":"dynamic_step"/);
  assert.match(serialized, /"activity_type":\{"kind":"identifier","name":"activityName"\}/);
  assert.match(serialized, /"input":\{"kind":"identifier","name":"args"\}/);
});

test("compiler lowers dynamic Temporal sleep durations", async () => {
  const fixture = path.join(
    root,
    "sdk/typescript-compiler/test-fixtures/temporal-dynamic-sleep-workflow.ts",
  );
  const { stdout } = await runCompiler([
    "--entry",
    fixture,
    "--export",
    "temporalDynamicSleepWorkflow",
    "--definition-id",
    "temporal-dynamic-sleep-workflow",
    "--version",
    "1",
  ]);
  const artifact = JSON.parse(stdout);
  const serialized = JSON.stringify(artifact.workflow.states);

  assert.match(serialized, /"type":"wait_for_timer"/);
  assert.match(serialized, /"timer_expr"/);
  assert.match(serialized, /"op":"coalesce"/);
});

test("compiler lowers common string case helpers", async () => {
  const fixture = path.join(
    root,
    "sdk/typescript-compiler/test-fixtures/temporal-string-case-workflow.ts",
  );
  const { stdout } = await runCompiler([
    "--entry",
    fixture,
    "--export",
    "temporalStringCaseWorkflow",
    "--definition-id",
    "temporal-string-case-workflow",
    "--version",
    "1",
  ]);
  const artifact = JSON.parse(stdout);
  const serialized = JSON.stringify(artifact.workflow.states);

  assert.match(serialized, /"callee":"__builtin_string_to_uppercase"/);
  assert.match(serialized, /"callee":"__builtin_string_to_lowercase"/);
});

test("compiler accepts ApplicationFailure.create shorthand properties", async () => {
  const fixture = path.join(
    root,
    "sdk/typescript-compiler/test-fixtures/temporal-application-failure-shorthand-workflow.ts",
  );
  const { stdout } = await runCompiler([
    "--entry",
    fixture,
    "--export",
    "temporalApplicationFailureShorthandWorkflow",
    "--definition-id",
    "temporal-application-failure-shorthand-workflow",
    "--version",
    "1",
  ]);
  const artifact = JSON.parse(stdout);
  const serialized = JSON.stringify(artifact.workflow.states);

  assert.match(serialized, /"type":"fail"/);
  assert.match(serialized, /"message"/);
});

test("compiler lowers uninitialized local let declarations", async () => {
  const fixture = path.join(
    root,
    "sdk/typescript-compiler/test-fixtures/temporal-uninitialized-let-workflow.ts",
  );
  const { stdout } = await runCompiler([
    "--entry",
    fixture,
    "--export",
    "temporalUninitializedLetWorkflow",
    "--definition-id",
    "temporal-uninitialized-let-workflow",
    "--version",
    "1",
  ]);
  const artifact = JSON.parse(stdout);
  const serialized = JSON.stringify(artifact.workflow.states);

  assert.match(serialized, /"target":"message"/);
  assert.match(serialized, /"value":null/);
});

test("compiler lowers typeof and instanceof Error in pure helpers", async () => {
  const fixture = path.join(
    root,
    "sdk/typescript-compiler/test-fixtures/temporal-error-helper-workflow.ts",
  );
  const { stdout } = await runCompiler([
    "--entry",
    fixture,
    "--export",
    "temporalErrorHelperWorkflow",
    "--definition-id",
    "temporal-error-helper-workflow",
    "--version",
    "1",
  ]);
  const artifact = JSON.parse(stdout);
  const serializedHelper = JSON.stringify(artifact.helpers.errorMessage);

  assert.match(serializedHelper, /"callee":"__builtin_typeof"/);
  assert.match(serializedHelper, /"callee":"__builtin_is_error"/);
});

test("compiler ignores top-level runtime guards gated by inWorkflowContext", async () => {
  const fixture = path.join(
    root,
    "sdk/typescript-compiler/test-fixtures/temporal-top-level-guard-workflow.ts",
  );
  const { stdout } = await runCompiler([
    "--entry",
    fixture,
    "--export",
    "temporalTopLevelGuardWorkflow",
    "--definition-id",
    "temporal-top-level-guard-workflow",
    "--version",
    "1",
  ]);
  const artifact = JSON.parse(stdout);
  const serialized = JSON.stringify(artifact.workflow.states);

  assert.match(serialized, /"handler":"greet"/);
});

test("compiler lowers awaited Temporal conditions wrapped in pure expressions", async () => {
  const fixture = path.join(
    root,
    "sdk/typescript-compiler/test-fixtures/temporal-negated-condition-workflow.ts",
  );
  const { stdout } = await runCompiler([
    "--entry",
    fixture,
    "--export",
    "temporalNegatedConditionWorkflow",
    "--definition-id",
    "temporal-negated-condition-workflow",
    "--version",
    "1",
  ]);
  const artifact = JSON.parse(stdout);
  const serialized = JSON.stringify(artifact.workflow.states);

  assert.match(serialized, /"type":"wait_for_condition"/);
  assert.match(serialized, /"target":"timedOut"/);
  assert.match(serialized, /"op":"not"/);
});

test("compiler lowers Temporal signal handler increment statements", async () => {
  const fixture = path.join(
    root,
    "sdk/typescript-compiler/test-fixtures/temporal-signal-counter-workflow.ts",
  );
  const { stdout } = await runCompiler([
    "--entry",
    fixture,
    "--export",
    "temporalSignalCounterWorkflow",
    "--definition-id",
    "temporal-signal-counter-workflow",
    "--version",
    "1",
  ]);
  const artifact = JSON.parse(stdout);
  const serializedSignals = JSON.stringify(artifact.signals.tick.states);

  assert.match(serializedSignals, /"target":"progress"/);
  assert.match(serializedSignals, /"op":"add"/);
});

test("compiler lowers local array push mutations in signal handlers", async () => {
  const fixture = path.join(
    root,
    "sdk/typescript-compiler/test-fixtures/temporal-array-push-workflow.ts",
  );
  const { stdout } = await runCompiler([
    "--entry",
    fixture,
    "--export",
    "temporalArrayPushWorkflow",
    "--definition-id",
    "temporal-array-push-workflow",
    "--version",
    "1",
  ]);
  const artifact = JSON.parse(stdout);
  const serializedSignals = JSON.stringify(artifact.signals.push.states);

  assert.match(serializedSignals, /"callee":"__builtin_array_append"/);
  assert.match(serializedSignals, /"target":"items"/);
});

test("compiler lowers local array shift mutations into head and tail assignments", async () => {
  const fixture = path.join(
    root,
    "sdk/typescript-compiler/test-fixtures/temporal-array-shift-workflow.ts",
  );
  const { stdout } = await runCompiler([
    "--entry",
    fixture,
    "--export",
    "temporalArrayShiftWorkflow",
    "--definition-id",
    "temporal-array-shift-workflow",
    "--version",
    "1",
  ]);
  const artifact = JSON.parse(stdout);
  const serializedWorkflow = JSON.stringify(artifact.workflow.states);

  assert.match(serializedWorkflow, /"callee":"__builtin_array_shift_head"/);
  assert.match(serializedWorkflow, /"callee":"__builtin_array_shift_tail"/);
});

test("compiler lowers Temporal delete statements into object-omit assignments", async () => {
  const fixture = path.join(
    root,
    "sdk/typescript-compiler/test-fixtures/temporal-delete-workflow.ts",
  );
  const { stdout } = await runCompiler([
    "--entry",
    fixture,
    "--export",
    "temporalDeleteWorkflow",
    "--definition-id",
    "temporal-delete-workflow",
    "--version",
    "1",
  ]);
  const artifact = JSON.parse(stdout);
  const serializedSignals = JSON.stringify(artifact.signals.clear.states);

  assert.match(serializedSignals, /"op":"in"/);
  assert.match(serializedSignals, /"callee":"__builtin_object_omit"/);
  assert.match(serializedSignals, /"target":"records"/);
});

test("compiler lowers bare awaited Temporal startChild calls", async () => {
  const fixture = path.join(
    root,
    "sdk/typescript-compiler/test-fixtures/temporal-start-child-bare-workflow.ts",
  );
  const { stdout } = await runCompiler([
    "--entry",
    fixture,
    "--export",
    "temporalStartChildBareWorkflow",
    "--definition-id",
    "temporal-start-child-bare-workflow",
    "--version",
    "1",
  ]);
  const artifact = JSON.parse(stdout);
  const serialized = JSON.stringify(artifact.workflow.states);

  assert.match(serialized, /"type":"start_child"/);
  assert.doesNotMatch(serialized, /"handle_var"/);
});

test("compiler lowers indexed object assignments into object-set calls", async () => {
  const fixture = path.join(
    root,
    "sdk/typescript-compiler/test-fixtures/temporal-object-set-workflow.ts",
  );
  const { stdout } = await runCompiler([
    "--entry",
    fixture,
    "--export",
    "temporalObjectSetWorkflow",
    "--definition-id",
    "temporal-object-set-workflow",
    "--version",
    "1",
  ]);
  const artifact = JSON.parse(stdout);
  const serializedSignals = JSON.stringify(artifact.signals.mark.states);

  assert.match(serializedSignals, /"callee":"__builtin_object_set"/);
  assert.match(serializedSignals, /"target":"records"/);
});

test("compiler lowers member object assignments into object-set calls", async () => {
  const fixture = path.join(
    root,
    "sdk/typescript-compiler/test-fixtures/temporal-member-set-workflow.ts",
  );
  const { stdout } = await runCompiler([
    "--entry",
    fixture,
    "--export",
    "temporalMemberSetWorkflow",
    "--definition-id",
    "temporal-member-set-workflow",
    "--version",
    "1",
  ]);
  const artifact = JSON.parse(stdout);
  const serialized = JSON.stringify(artifact.workflow.states);

  assert.match(serialized, /"callee":"__builtin_object_set"/);
  assert.match(serialized, /"value":"count"/);
});

test("compiler lowers block-bodied pure helpers with for-range statements", async () => {
  const fixture = path.join(
    root,
    "sdk/typescript-compiler/test-fixtures/temporal-helper-block-workflow.ts",
  );
  const { stdout } = await runCompiler([
    "--entry",
    fixture,
    "--export",
    "temporalHelperBlockWorkflow",
    "--definition-id",
    "temporal-helper-block-workflow",
    "--version",
    "1",
  ]);
  const artifact = JSON.parse(stdout);
  const helper = artifact.helpers.divideIntoPartitions;
  const serializedHelper = JSON.stringify(helper);

  assert.match(serializedHelper, /"type":"for_range"/);
  assert.match(serializedHelper, /"type":"assign_index"/);
  assert.match(serializedHelper, /"callee":"__builtin_math_floor"/);
  assert.match(serializedHelper, /"callee":"__builtin_array_fill"/);
});

test("compiler lowers block-bodied pure helpers with if statements", async () => {
  const fixture = path.join(
    root,
    "sdk/typescript-compiler/test-fixtures/temporal-helper-if-workflow.ts",
  );
  const { stdout } = await runCompiler([
    "--entry",
    fixture,
    "--export",
    "temporalHelperIfWorkflow",
    "--definition-id",
    "temporal-helper-if-workflow",
    "--version",
    "1",
  ]);
  const artifact = JSON.parse(stdout);
  const helper = artifact.helpers.prettyErrorMessage;
  const serializedHelper = JSON.stringify(helper);

  assert.match(serializedHelper, /"type":"if"/);
  assert.match(serializedHelper, /"target":"errMessage"/);
});

test("compiler lowers awaited local async helpers into workflow states", async () => {
  const fixture = path.join(
    root,
    "sdk/typescript-compiler/test-fixtures/temporal-async-helper-workflow.ts",
  );
  const { stdout } = await runCompiler([
    "--entry",
    fixture,
    "--export",
    "temporalAsyncHelperWorkflow",
    "--definition-id",
    "temporal-async-helper-workflow",
    "--version",
    "1",
  ]);
  const artifact = JSON.parse(stdout);
  const serialized = JSON.stringify(artifact.workflow.states);

  assert.match(serialized, /"handler":"greet"/);
  assert.match(serialized, /__helper_name/);
});

test("compiler accepts static top-level proxyActivities option constants", async () => {
  const fixture = path.join(
    root,
    "sdk/typescript-compiler/test-fixtures/temporal-static-proxy-options-workflow.ts",
  );
  const { stdout } = await runCompiler([
    "--entry",
    fixture,
    "--export",
    "temporalStaticProxyOptionsWorkflow",
    "--definition-id",
    "temporal-static-proxy-options-workflow",
    "--version",
    "1",
  ]);
  const artifact = JSON.parse(stdout);
  const serialized = JSON.stringify(artifact.workflow.states);

  assert.match(serialized, /"handler":"greet"/);
  assert.match(serialized, /"schedule_to_close_timeout_ms":15000/);
  assert.match(serialized, /"start_to_close_timeout_ms":5000/);
});

test("compiler lowers deferred activity thunks and array unshift compensation patterns", async () => {
  const fixture = path.join(
    root,
    "sdk/typescript-compiler/test-fixtures/temporal-deferred-activity-thunk-workflow.ts",
  );
  const { stdout } = await runCompiler([
    "--entry",
    fixture,
    "--export",
    "temporalDeferredActivityThunkWorkflow",
    "--definition-id",
    "temporal-deferred-activity-thunk-workflow",
    "--version",
    "1",
  ]);
  const artifact = JSON.parse(stdout);
  const serialized = JSON.stringify(artifact.workflow.states);

  assert.match(serialized, /"type":"dynamic_step"/);
  assert.match(serialized, /"__kind":\{"kind":"literal","value":"activity_descriptor"\}/);
  assert.match(serialized, /"activity_type":\{"kind":"literal","value":"cleanup"\}/);
  assert.match(serialized, /"schedule_to_close_timeout_ms":\{"kind":"literal","value":15000\}/);
  assert.match(serialized, /"callee":"__builtin_array_prepend"/);
});

test("compiler lowers array reduce expressions", async () => {
  const fixture = path.join(
    root,
    "sdk/typescript-compiler/test-fixtures/temporal-array-reduce-workflow.ts",
  );
  const { stdout } = await runCompiler([
    "--entry",
    fixture,
    "--export",
    "temporalArrayReduceWorkflow",
    "--definition-id",
    "temporal-array-reduce-workflow",
    "--version",
    "1",
  ]);
  const artifact = JSON.parse(stdout);
  const serialized = JSON.stringify(artifact.workflow.states);

  assert.match(serialized, /"kind":"array_reduce"/);
  assert.match(serialized, /"accumulator_name":"sum"/);
  assert.match(serialized, /"item_name":"value"/);
});

test("compiler lowers local child promise arrays awaited with Promise.all", async () => {
  const fixture = path.join(
    root,
    "sdk/typescript-compiler/test-fixtures/temporal-child-promise-all-workflow.ts",
  );
  const { stdout } = await runCompiler([
    "--entry",
    fixture,
    "--export",
    "temporalChildPromiseAllWorkflow",
    "--definition-id",
    "temporal-child-promise-all-workflow",
    "--version",
    "1",
  ]);
  const artifact = JSON.parse(stdout);
  const serialized = JSON.stringify(artifact.workflow.states);

  assert.match(serialized, /"type":"start_child"/);
  assert.match(serialized, /"callee":"__builtin_array_append"/);
  assert.match(serialized, /"type":"wait_for_child"/);
});

test("compiler lowers mapped executeChild Promise.all joins", async () => {
  const fixture = path.join(
    root,
    "sdk/typescript-compiler/test-fixtures/temporal-child-promise-all-map-workflow.ts",
  );
  const { stdout } = await runCompiler([
    "--entry",
    fixture,
    "--export",
    "temporalChildPromiseAllMapWorkflow",
    "--definition-id",
    "temporal-child-promise-all-map-workflow",
    "--version",
    "1",
  ]);
  const artifact = JSON.parse(stdout);
  const serialized = JSON.stringify(artifact.workflow.states);

  assert.match(serialized, /"type":"start_child"/);
  assert.match(serialized, /"callee":"__builtin_array_append"/);
  assert.match(serialized, /"type":"wait_for_child"/);
  assert.match(serialized, /"property":"length"/);
});

test("compiler lowers Promise.all over mapped async local helpers", async () => {
  const fixture = path.join(
    root,
    "sdk/typescript-compiler/test-fixtures/temporal-async-helper-promise-all-workflow.ts",
  );
  const { stdout } = await runCompiler([
    "--entry",
    fixture,
    "--export",
    "temporalAsyncHelperPromiseAllWorkflow",
    "--definition-id",
    "temporal-async-helper-promise-all-workflow",
    "--version",
    "1",
  ]);
  const artifact = JSON.parse(stdout);
  const serialized = JSON.stringify(artifact.workflow.states);

  assert.match(serialized, /"property":"length"/);
  assert.match(serialized, /__helper_item/);
  assert.match(serialized, /"type":"wait_for_timer"/);
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
    "comparisonBenchmarkWorkflow",
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

test("compiler lowers Temporal condition waits driven by updates without signals", async () => {
  const fixture = path.join(
    root,
    "sdk/typescript-compiler/test-fixtures/temporal-update-condition-workflow.ts",
  );
  const { stdout } = await runCompiler([
    "--entry",
    fixture,
    "--export",
    "temporalUpdateConditionWorkflow",
    "--definition-id",
    "temporal-update-condition-workflow",
    "--version",
    "1",
  ]);
  const artifact = JSON.parse(stdout);
  const serializedWorkflowStates = JSON.stringify(artifact.workflow.states);
  const serializedUpdateStates = JSON.stringify(artifact.updates.setValue.states);

  assert.match(serializedWorkflowStates, /"type":"wait_for_condition"/);
  assert.match(serializedWorkflowStates, /"condition":\{"kind":"binary","op":"greater_than"/);
  assert.match(serializedUpdateStates, /"type":"assign"/);
  assert.match(serializedUpdateStates, /"target":"value"/);
  assert.match(serializedUpdateStates, /"type":"succeed"/);
  assert.ok(artifact.queries.currentValue);
});

test("compiler lowers Temporal condition waits with timeout", async () => {
  const fixture = path.join(
    root,
    "sdk/typescript-compiler/test-fixtures/temporal-condition-timeout-workflow.ts",
  );
  const { stdout } = await runCompiler([
    "--entry",
    fixture,
    "--export",
    "temporalConditionTimeoutWorkflow",
    "--definition-id",
    "temporal-condition-timeout-workflow",
    "--version",
    "1",
  ]);
  const artifact = JSON.parse(stdout);
  const serializedWorkflowStates = JSON.stringify(artifact.workflow.states);

  assert.match(serializedWorkflowStates, /"type":"wait_for_condition"/);
  assert.match(serializedWorkflowStates, /"timeout_ref":"5s"/);
  assert.match(serializedWorkflowStates, /"timeout_next":"assign_/);
});

test("compiler lowers Promise.race over Temporal condition and sleep into timed condition waits", async () => {
  const fixture = path.join(
    root,
    "sdk/typescript-compiler/test-fixtures/temporal-promise-race-condition-timeout-workflow.ts",
  );
  const { stdout } = await runCompiler([
    "--entry",
    fixture,
    "--export",
    "temporalPromiseRaceConditionTimeoutWorkflow",
    "--definition-id",
    "temporal-promise-race-condition-timeout-workflow",
    "--version",
    "1",
  ]);
  const artifact = JSON.parse(stdout);
  const serializedWorkflowStates = JSON.stringify(artifact.workflow.states);

  assert.match(serializedWorkflowStates, /"type":"wait_for_condition"/);
  assert.match(serializedWorkflowStates, /"timeout_ref":"30 days"/);
  assert.match(serializedWorkflowStates, /"timeout_ref":"5s"/);
});

test("compiler accepts local predicate identifiers passed to Temporal condition", async () => {
  const fixture = path.join(
    root,
    "sdk/typescript-compiler/test-fixtures/temporal-condition-local-predicate-workflow.ts",
  );
  const { stdout } = await runCompiler([
    "--entry",
    fixture,
    "--export",
    "temporalConditionLocalPredicateWorkflow",
    "--definition-id",
    "temporal-condition-local-predicate-workflow",
    "--version",
    "1",
  ]);
  const artifact = JSON.parse(stdout);
  const serializedWorkflowStates = JSON.stringify(artifact.workflow.states);

  assert.match(serializedWorkflowStates, /"type":"wait_for_condition"/);
});

test("compiler lowers prefix increment in for-loop update expressions", async () => {
  const fixture = path.join(
    root,
    "sdk/typescript-compiler/test-fixtures/temporal-prefix-increment-workflow.ts",
  );
  const { stdout } = await runCompiler([
    "--entry",
    fixture,
    "--export",
    "temporalPrefixIncrementWorkflow",
    "--definition-id",
    "temporal-prefix-increment-workflow",
    "--version",
    "1",
  ]);
  const artifact = JSON.parse(stdout);
  const serializedWorkflowStates = JSON.stringify(artifact.workflow.states);

  assert.match(serializedWorkflowStates, /"op":"add"/);
});

test("compiler lowers stored Temporal sleep handles and later await", async () => {
  const fixture = path.join(
    root,
    "sdk/typescript-compiler/test-fixtures/temporal-sleep-handle-workflow.ts",
  );
  const { stdout } = await runCompiler([
    "--entry",
    fixture,
    "--export",
    "temporalSleepHandleWorkflow",
    "--definition-id",
    "temporal-sleep-handle-workflow",
    "--version",
    "1",
  ]);
  const artifact = JSON.parse(stdout);
  const serializedWorkflowStates = JSON.stringify(artifact.workflow.states);

  assert.match(serializedWorkflowStates, /"type":"start_timer_handle"/);
  assert.match(serializedWorkflowStates, /"handle_var":"timer"/);
  assert.match(serializedWorkflowStates, /"type":"wait_for_condition"/);
  assert.match(serializedWorkflowStates, /"property":"status"/);
});

test("compiler lowers background activity handles raced against timers and awaited later", async () => {
  const fixture = path.join(
    root,
    "sdk/typescript-compiler/test-fixtures/temporal-activity-handle-race-workflow.ts",
  );
  const { stdout } = await runCompiler([
    "--entry",
    fixture,
    "--export",
    "temporalActivityHandleRaceWorkflow",
    "--definition-id",
    "temporal-activity-handle-race-workflow",
    "--version",
    "1",
  ]);
  const artifact = JSON.parse(stdout);
  const serializedWorkflowStates = JSON.stringify(artifact.workflow.states);

  assert.match(serializedWorkflowStates, /"type":"start_step_handle"/);
  assert.match(serializedWorkflowStates, /"type":"start_timer_handle"/);
  assert.match(serializedWorkflowStates, /"completion_actions"/);
  assert.match(serializedWorkflowStates, /"handler":"sendNotificationEmail"/);
});

test("compiler lowers callback-backed Promise waits into timers", async () => {
  const fixture = path.join(
    root,
    "sdk/typescript-compiler/test-fixtures/temporal-callback-promise-workflow.ts",
  );
  const { stdout } = await runCompiler([
    "--entry",
    fixture,
    "--export",
    "temporalCallbackPromiseWorkflow",
    "--definition-id",
    "temporal-callback-promise-workflow",
    "--version",
    "1",
  ]);
  const artifact = JSON.parse(stdout);
  const serializedWorkflowStates = JSON.stringify(artifact.workflow.states);

  assert.match(serializedWorkflowStates, /"type":"wait_for_timer"/);
  assert.match(serializedWorkflowStates, /"timer_ref":"10ms"/);
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

test("compiler lowers Temporal child handle signal and cancel calls", async () => {
  const fixture = path.join(
    root,
    "sdk/typescript-compiler/test-fixtures/temporal-child-handle-control-workflow.ts",
  );
  const { stdout } = await runCompiler([
    "--entry",
    fixture,
    "--export",
    "temporalChildHandleControlWorkflow",
    "--definition-id",
    "temporal-child-handle-control-workflow",
    "--version",
    "1",
  ]);
  const artifact = JSON.parse(stdout);
  const serialized = JSON.stringify(artifact.workflow.states);

  assert.match(serialized, /"type":"start_child"/);
  assert.match(serialized, /"type":"signal_child"/);
  assert.match(serialized, /"signal_name":"approve"/);
  assert.match(serialized, /"type":"cancel_child"/);
  assert.match(serialized, /"reason":\{"kind":"literal","value":"stop"\}/);
});

test("compiler lowers Temporal external workflow handle signal and cancel calls", async () => {
  const fixture = path.join(
    root,
    "sdk/typescript-compiler/test-fixtures/temporal-external-workflow-handle-workflow.ts",
  );
  const { stdout } = await runCompiler([
    "--entry",
    fixture,
    "--export",
    "temporalExternalWorkflowHandleWorkflow",
    "--definition-id",
    "temporal-external-workflow-handle-workflow",
    "--version",
    "1",
  ]);
  const artifact = JSON.parse(stdout);
  const serialized = JSON.stringify(artifact.workflow.states);

  assert.match(serialized, /"type":"signal_external"/);
  assert.match(serialized, /"type":"cancel_external"/);
  assert.match(serialized, /"target_instance_id":\{"kind":"literal","value":"target-workflow"\}/);
  assert.match(serialized, /"target_run_id":\{"kind":"literal","value":"target-run"\}/);
  assert.match(serialized, /"signal_name":"approve"/);
  assert.match(serialized, /"reason":\{"kind":"literal","value":"stop"\}/);
});

test("compiler lowers Temporal cancellation scopes around proxy activities", async () => {
  const fixture = path.join(
    root,
    "sdk/typescript-compiler/test-fixtures/temporal-cancellation-scope-workflow.ts",
  );
  const { stdout } = await runCompiler([
    "--entry",
    fixture,
    "--export",
    "temporalCancellationScopeWorkflow",
    "--definition-id",
    "temporal-cancellation-scope-workflow",
    "--version",
    "1",
  ]);
  const artifact = JSON.parse(stdout);
  const serialized = JSON.stringify(artifact.workflow.states);

  assert.match(serialized, /"type":"step"/);
  assert.match(serialized, /"handler":"benchmarkEcho"/);
  assert.match(serialized, /"on_error":\{"next":/);
  assert.ok(artifact.helpers.isCancellation);
  assert.match(
    JSON.stringify(artifact.helpers.isCancellation),
    /"callee":"__temporal_is_cancellation"/,
  );
});

test("compiler lowers Temporal cancellation scopes around child workflows", async () => {
  const fixture = path.join(
    root,
    "sdk/typescript-compiler/test-fixtures/temporal-cancellation-scope-workflow.ts",
  );
  const { stdout } = await runCompiler([
    "--entry",
    fixture,
    "--export",
    "temporalCancellationScopeChildWorkflow",
    "--definition-id",
    "temporal-cancellation-scope-child-workflow",
    "--version",
    "1",
  ]);
  const artifact = JSON.parse(stdout);
  const serialized = JSON.stringify(artifact.workflow.states);

  assert.match(serialized, /"type":"start_child"/);
  assert.match(serialized, /"type":"wait_for_child"/);
  assert.match(serialized, /"on_error":\{"next":/);
});

test("compiler lowers block-bodied Temporal cancellation scopes with multiple awaited steps", async () => {
  const fixture = path.join(
    root,
    "sdk/typescript-compiler/test-fixtures/temporal-cancellation-scope-block-workflow.ts",
  );
  const { stdout } = await runCompiler([
    "--entry",
    fixture,
    "--export",
    "temporalCancellationScopeBlockWorkflow",
    "--definition-id",
    "temporal-cancellation-scope-block-workflow",
    "--version",
    "1",
  ]);
  const artifact = JSON.parse(stdout);
  const serialized = JSON.stringify(artifact.workflow.states);
  const benchmarkEchoMatches = serialized.match(/"handler":"benchmarkEcho"/g) ?? [];

  assert.ok(benchmarkEchoMatches.length >= 2);
  assert.match(serialized, /"output_var":"first"/);
  assert.match(serialized, /"output_var":"second"/);
  assert.match(serialized, /"on_error":\{"next":/);
});

test("compiler lowers synchronous Temporal nonCancellable scope handlers", async () => {
  const fixture = path.join(
    root,
    "sdk/typescript-compiler/test-fixtures/temporal-cancellation-scope-sync-workflow.ts",
  );
  const { stdout } = await runCompiler([
    "--entry",
    fixture,
    "--export",
    "temporalCancellationScopeSyncWorkflow",
    "--definition-id",
    "temporal-cancellation-scope-sync-workflow",
    "--version",
    "1",
  ]);
  const artifact = JSON.parse(stdout);
  const serialized = JSON.stringify(artifact.workflow.states);

  assert.match(serialized, /"handler":"cleanup"/);
});

test("compiler lowers return CancellationScope.nonCancellable calls", async () => {
  const fixture = path.join(
    root,
    "sdk/typescript-compiler/test-fixtures/temporal-cancellation-scope-sync-workflow.ts",
  );
  const { stdout } = await runCompiler([
    "--entry",
    fixture,
    "--export",
    "temporalCancellationScopeSyncReturnWorkflow",
    "--definition-id",
    "temporal-cancellation-scope-sync-return-workflow",
    "--version",
    "1",
  ]);
  const artifact = JSON.parse(stdout);
  const serialized = JSON.stringify(artifact.workflow.states);

  assert.match(serialized, /"handler":"cleanup"/);
  assert.match(serialized, /"type":"succeed"/);
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

test("compiler emits workflow parameter metadata for Temporal workflow entrypoints", async () => {
  const fixture = path.join(
    root,
    "sdk/typescript-compiler/test-fixtures/temporal-workflow-params.ts",
  );
  const { stdout } = await runCompiler([
    "--entry",
    fixture,
    "--export",
    "temporalWorkflowParamsWorkflow",
    "--definition-id",
    "temporal-workflow-params",
    "--version",
    "1",
  ]);

  const artifact = JSON.parse(stdout);
  assert.deepEqual(artifact.workflow.params, [
    { name: "name" },
    { name: "punctuation", default: { kind: "literal", value: "!" } },
  ]);

  const serialized = JSON.stringify(artifact.workflow.states);
  assert.match(serialized, /"handler":"greet"/);
  assert.match(serialized, /"input":\{"kind":"array"/);
  assert.match(serialized, /"name":"name"/);
  assert.match(serialized, /"name":"punctuation"/);
});

test("compiler emits rest workflow parameter metadata", async () => {
  const fixture = path.join(
    root,
    "sdk/typescript-compiler/test-fixtures/temporal-rest-params-workflow.ts",
  );
  const { stdout } = await runCompiler([
    "--entry",
    fixture,
    "--export",
    "temporalRestParamsWorkflow",
    "--definition-id",
    "temporal-rest-params",
    "--version",
    "1",
  ]);

  const artifact = JSON.parse(stdout);
  assert.deepEqual(artifact.workflow.params, [
    { name: "prefix" },
    { name: "names", rest: true },
  ]);
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
