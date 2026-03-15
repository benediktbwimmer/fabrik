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

test("stream compiler emits keyed-rollup workflow signal artifacts", async () => {
  const fixture = path.join(
    root,
    "sdk/typescript-compiler/test-fixtures/keyed-rollup-signal-stream-job.ts",
  );
  const { stdout } = await runCompiler([
    "--entry",
    fixture,
    "--export",
    "keyedRollupSignalStreamJob",
    "--definition-id",
    "payments-rollup-signal",
    "--version",
    "1",
  ]);
  const artifact = JSON.parse(stdout);

  assert.equal(artifact.runtime_contract, "streams_kernel_v1");
  assert.equal(artifact.job.runtime, "keyed_rollup");
  assert.equal(artifact.job.operators[2].kind, "signal_workflow");
  assert.equal(artifact.job.operators[2].config.view, "accountTotals");
  assert.equal(artifact.job.operators[2].config.signalType, "account.rollup.ready");
  assert.equal(artifact.job.operators[2].config.whenOutputField, "totalAmount");
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

test("stream compiler emits aggregate-v2 map artifacts", async () => {
  const fixture = path.join(
    root,
    "sdk/typescript-compiler/test-fixtures/aggregate-v2-map-stream-job.ts",
  );
  const { stdout } = await runCompiler([
    "--entry",
    fixture,
    "--export",
    "fraudMapStreamJob",
    "--definition-id",
    "fraud-map",
    "--version",
    "1",
  ]);
  const artifact = JSON.parse(stdout);

  assert.equal(artifact.runtime_contract, "streams_kernel_v2");
  assert.equal(artifact.job.runtime, "aggregate_v2");
  assert.equal(artifact.job.operators[0].kind, "map");
  assert.equal(artifact.job.operators[0].config.inputField, "riskPoints");
  assert.equal(artifact.job.operators[0].config.outputField, "risk");
  assert.equal(artifact.job.operators[0].config.multiplyBy, 0.01);
  assert.equal(artifact.job.operators[1].config.valueField, "risk");
});

test("stream compiler accepts aggregate-v2 filter then map ordering", async () => {
  const fixture = path.join(
    root,
    "sdk/typescript-compiler/test-fixtures/aggregate-v2-filter-map-stream-job.ts",
  );
  const { stdout } = await runCompiler([
    "--entry",
    fixture,
    "--export",
    "fraudFilterMapStreamJob",
    "--definition-id",
    "fraud-filter-map",
    "--version",
    "1",
  ]);
  const artifact = JSON.parse(stdout);

  assert.equal(artifact.runtime_contract, "streams_kernel_v2");
  assert.equal(artifact.job.runtime, "aggregate_v2");
  assert.equal(artifact.job.operators[0].kind, "filter");
  assert.equal(artifact.job.operators[1].kind, "map");
  assert.equal(artifact.job.operators[1].config.outputField, "risk");
});

test("stream compiler emits aggregate-v2 route artifacts", async () => {
  const fixture = path.join(
    root,
    "sdk/typescript-compiler/test-fixtures/aggregate-v2-route-stream-job.ts",
  );
  const { stdout } = await runCompiler([
    "--entry",
    fixture,
    "--export",
    "fraudRouteStreamJob",
    "--definition-id",
    "fraud-route",
    "--version",
    "1",
  ]);
  const artifact = JSON.parse(stdout);

  assert.equal(artifact.runtime_contract, "streams_kernel_v2");
  assert.equal(artifact.job.runtime, "aggregate_v2");
  assert.equal(artifact.job.operators[0].kind, "route");
  assert.equal(artifact.job.operators[0].config.outputField, "riskBucket");
  assert.equal(artifact.job.operators[0].config.branches[0].value, "high");
  assert.equal(artifact.job.operators[1].kind, "filter");
});

test("stream compiler emits threshold aggregate-v2 artifacts", async () => {
  const fixture = path.join(
    root,
    "sdk/typescript-compiler/test-fixtures/aggregate-v2-threshold-stream-job.ts",
  );
  const { stdout } = await runCompiler([
    "--entry",
    fixture,
    "--export",
    "fraudThresholdStreamJob",
    "--definition-id",
    "fraud-threshold",
    "--version",
    "1",
  ]);
  const artifact = JSON.parse(stdout);

  assert.equal(artifact.runtime_contract, "streams_kernel_v2");
  assert.equal(artifact.job.runtime, "aggregate_v2");
  assert.equal(artifact.job.source.kind, "bounded_input");
  assert.equal(artifact.job.operators[0].config.reducer, "threshold");
  assert.equal(artifact.job.operators[0].config.threshold, 0.97);
  assert.equal(artifact.job.operators[0].config.comparison, "gte");
  assert.equal(artifact.job.views[0].value_fields[1], "riskExceeded");
});

test("stream compiler emits aggregate-v2 workflow signal artifacts", async () => {
  const fixture = path.join(
    root,
    "sdk/typescript-compiler/test-fixtures/aggregate-v2-threshold-signal-stream-job.ts",
  );
  const { stdout } = await runCompiler([
    "--entry",
    fixture,
    "--export",
    "fraudThresholdSignalStreamJob",
    "--definition-id",
    "fraud-threshold-signal",
    "--version",
    "1",
  ]);
  const artifact = JSON.parse(stdout);

  assert.equal(artifact.runtime_contract, "streams_kernel_v2");
  assert.equal(artifact.job.runtime, "aggregate_v2");
  assert.equal(artifact.job.operators[2].kind, "signal_workflow");
  assert.equal(artifact.job.operators[2].config.view, "riskThresholds");
  assert.equal(artifact.job.operators[2].config.signalType, "fraud.threshold.crossed");
  assert.equal(artifact.job.operators[2].config.whenOutputField, "riskExceeded");
});
