import test from "node:test";
import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import path from "node:path";
import { promisify } from "node:util";

const execFileAsync = promisify(execFile);
const root = "/Users/bene/code/fabrik";
const compilerPath = path.join(root, "sdk/typescript-compiler/stream-compiler.mjs");

async function runCompiler(args) {
  return execFileAsync("node", [compilerPath, ...args], {
    cwd: root,
  });
}

test("stream compiler emits keyed-rollup standalone artifacts", async () => {
  const fixture = path.join(root, "sdk/typescript-compiler/test-fixtures/standalone-stream-job.ts");
  const { stdout } = await runCompiler([
    "--entry",
    fixture,
    "--export",
    "keyedRollupStreamJob",
    "--definition-id",
    "payments-rollup",
    "--version",
    "3",
  ]);
  const artifact = JSON.parse(stdout);

  assert.equal(artifact.definition_id, "payments-rollup");
  assert.equal(artifact.definition_version, 3);
  assert.equal(artifact.runtime_contract, "streams_kernel_v1");
  assert.equal(artifact.job.name, "keyed-rollup");
  assert.equal(artifact.job.runtime, "keyed_rollup");
  assert.equal(artifact.job.source.kind, "bounded_input");
  assert.equal(artifact.job.key_by, "accountId");
  assert.equal(artifact.job.operators[0].kind, "reduce");
  assert.equal(artifact.job.views[0].name, "accountTotals");
  assert.equal(artifact.job.views[0].query_mode, "by_key");
  assert.equal(artifact.job.queries[0].view_name, "accountTotals");
  assert.equal(artifact.job.checkpoint_policy.checkpoints[0].name, "hourly-rollup-ready");
  assert.ok(artifact.artifact_hash.length > 10);
  assert.ok(artifact.source_map.job);
  assert.ok(artifact.source_files.some((file) => file.endsWith("standalone-stream-job.ts")));
});

test("stream compiler rejects keyed-rollup jobs that violate the kernel contract", async () => {
  const fixture = path.join(
    root,
    "sdk/typescript-compiler/test-fixtures/invalid-standalone-stream-job.ts",
  );
  await assert.rejects(
    runCompiler([
      "--entry",
      fixture,
      "--export",
      "invalidKeyedRollupStreamJob",
      "--definition-id",
      "payments-rollup",
      "--version",
      "3",
    ]),
    /emit_checkpoint\.name must match checkpointPolicy\.checkpoints\[0\]\.name/,
  );
});

test("stream compiler emits aggregate-v2 artifacts with widened schema", async () => {
  const fixture = path.join(root, "sdk/typescript-compiler/test-fixtures/aggregate-v2-stream-job.ts");
  const { stdout } = await runCompiler([
    "--entry",
    fixture,
    "--export",
    "fraudDetectorStreamJob",
    "--definition-id",
    "fraud-detector",
    "--version",
    "1",
  ]);
  const artifact = JSON.parse(stdout);

  assert.equal(artifact.runtime_contract, "streams_kernel_v2");
  assert.equal(artifact.job.runtime, "aggregate_v2");
  assert.equal(artifact.job.source.kind, "topic");
  assert.equal(artifact.job.source.name, "payments");
  assert.equal(artifact.job.states[0].id, "risk-state");
  assert.equal(artifact.job.operators[0].operator_id, "filter-valid");
  assert.deepEqual(artifact.job.operators[0].inputs, ["source:payments"]);
  assert.equal(artifact.job.operators[1].kind, "window");
  assert.equal(artifact.job.views[0].view_id, "risk-scores");
  assert.deepEqual(artifact.job.views[0].supported_consistencies, ["strong", "eventual"]);
  assert.equal(artifact.job.queries[0].query_id, "risk-scores-by-key");
  assert.deepEqual(artifact.job.queries[0].arg_fields, ["accountId"]);
  assert.equal(artifact.job.classification, "fast_lane");
  assert.equal(artifact.job.metadata.kernel, "aggregate_v2");
  assert.ok(artifact.source_map.states);
});
