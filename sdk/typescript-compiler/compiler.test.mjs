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
