import test from "node:test";
import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import path from "node:path";
import { promisify } from "node:util";

const execFileAsync = promisify(execFile);
const root = "/Users/bene/code/fabrik";
const analyzerPath = path.join(root, "sdk/typescript-compiler/migration-analyzer.mjs");

async function analyze(project) {
  const { stdout } = await execFileAsync("node", [analyzerPath, "--project", project], {
    cwd: root,
  });
  return JSON.parse(stdout);
}

test("migration analyzer discovers supported workflows and workers", async () => {
  const fixture = path.join(root, "crates/fabrik-cli/test-fixtures/temporal-supported");
  const payload = await analyze(fixture);
  assert.equal(payload.summary.workflow_count, 1);
  assert.equal(payload.summary.worker_count, 1);
  assert.equal(payload.summary.hard_block_count, 0);
  assert.equal(payload.support_matrix_meta.milestone_scope, "temporal_ts_subset_trust");
  assert.equal(payload.workflows[0].export_name, "orderWorkflow");
  assert.equal(payload.workers[0].task_queue, "orders");
  assert.ok(payload.files.some((file) => file.uses.includes("search_attributes_memo")));
  assert.ok(
    payload.support_matrix.some(
      (entry) =>
        entry.feature === "ctx_version_workflow_evolution" &&
        entry.confidence_class === "supported_upgrade_validated",
    ),
  );
});

test("migration analyzer supports default-compatible payload converters and blocks custom payload landmines", async () => {
  const bootstrapQualifiedFixture = path.join(
    root,
    "crates/fabrik-cli/test-fixtures/temporal-bootstrap-qualified",
  );
  const bootstrapEsmQualifiedFixture = path.join(
    root,
    "crates/fabrik-cli/test-fixtures/temporal-bootstrap-esm-qualified",
  );
  const payloadQualifiedFixture = path.join(
    root,
    "crates/fabrik-cli/test-fixtures/temporal-payload-qualified",
  );
  const payloadPathQualifiedFixture = path.join(
    root,
    "crates/fabrik-cli/test-fixtures/temporal-payload-path-qualified",
  );
  const supportedApiQualifiedFixture = path.join(
    root,
    "crates/fabrik-cli/test-fixtures/temporal-supported-api-qualified",
  );
  const payloadPathQualifiedV2Fixture = path.join(
    root,
    "crates/fabrik-cli/test-fixtures/temporal-payload-path-qualified-v2",
  );
  const payloadFixture = path.join(root, "crates/fabrik-cli/test-fixtures/temporal-payload-blocked");
  const payloadQualified = await analyze(payloadQualifiedFixture);
  const bootstrapQualified = await analyze(bootstrapQualifiedFixture);
  const bootstrapEsmQualified = await analyze(bootstrapEsmQualifiedFixture);
  const payloadPathQualified = await analyze(payloadPathQualifiedFixture);
  const supportedApiQualified = await analyze(supportedApiQualifiedFixture);
  const payloadPathQualifiedV2 = await analyze(payloadPathQualifiedV2Fixture);
  const visibilityFixture = path.join(
    root,
    "crates/fabrik-cli/test-fixtures/temporal-visibility-blocked",
  );
  const payload = await analyze(payloadFixture);
  const visibility = await analyze(visibilityFixture);
  assert.equal(bootstrapQualified.summary.hard_block_count, 0);
  assert.equal(bootstrapQualified.workers[0].task_queue, "bootstrap-qualified");
  assert.equal(bootstrapQualified.workers[0].workflows_path, "./workflows");
  assert.equal(bootstrapEsmQualified.summary.hard_block_count, 0);
  assert.equal(bootstrapEsmQualified.workers[0].task_queue, "bootstrap-esm-qualified");
  assert.equal(bootstrapEsmQualified.workers[0].workflows_path, "./workflows.ts");
  assert.equal(payloadQualified.summary.hard_block_count, 0);
  assert.equal(payloadQualified.workers[0].data_converter_mode, "default_temporal");
  assert.equal(payloadPathQualified.summary.hard_block_count, 0);
  assert.equal(payloadPathQualified.workers[0].data_converter_mode, "path_default_temporal");
  assert.equal(
    payloadPathQualified.workers[0].payload_converter_module,
    "./src/custom-payload-converter.ts",
  );
  assert.equal(supportedApiQualified.summary.hard_block_count, 0);
  assert.equal(supportedApiQualified.workers[0].task_queue, "supported-api");
  assert.equal(payloadPathQualifiedV2.summary.hard_block_count, 0);
  assert.equal(payloadPathQualifiedV2.workers[0].data_converter_mode, "path_default_temporal");
  assert.ok(payload.findings.some((finding) => finding.feature === "payload_data_converter_usage"));
  assert.ok(!visibility.findings.some((finding) => finding.feature === "visibility_search_usage"));
  assert.equal(visibility.summary.hard_block_count, 0);
  assert.ok(visibility.files.some((file) => file.uses.includes("search_attributes_memo")));
});
