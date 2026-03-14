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
  const bootstrapDynamicTaskQueueQualifiedFixture = path.join(
    root,
    "crates/fabrik-cli/test-fixtures/temporal-bootstrap-dynamic-taskqueue-qualified",
  );
  const bootstrapEsmQualifiedFixture = path.join(
    root,
    "crates/fabrik-cli/test-fixtures/temporal-bootstrap-esm-qualified",
  );
  const bootstrapSelfFileQualifiedFixture = path.join(
    root,
    "crates/fabrik-cli/test-fixtures/temporal-bootstrap-self-file-qualified",
  );
  const activityFactoryQualifiedFixture = path.join(
    root,
    "crates/fabrik-cli/test-fixtures/temporal-activity-factory-qualified",
  );
  const testWorkerQualifiedFixture = path.join(
    root,
    "crates/fabrik-cli/test-fixtures/temporal-test-worker-qualified",
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
  const activityFailureQualifiedFixture = path.join(
    root,
    "crates/fabrik-cli/test-fixtures/temporal-activity-failure-qualified",
  );
  const workerVersioningQualifiedFixture = path.join(
    root,
    "crates/fabrik-cli/test-fixtures/temporal-worker-versioning-qualified",
  );
  const searchAttributesQualifiedFixture = path.join(
    root,
    "crates/fabrik-cli/test-fixtures/temporal-search-attributes-qualified",
  );
  const patchingQualifiedFixture = path.join(
    root,
    "crates/fabrik-cli/test-fixtures/temporal-patching-qualified",
  );
  const payloadPathQualifiedV2Fixture = path.join(
    root,
    "crates/fabrik-cli/test-fixtures/temporal-payload-path-qualified-v2",
  );
  const dataConverterFactoryQualifiedFixture = path.join(
    root,
    "crates/fabrik-cli/test-fixtures/temporal-data-converter-factory-qualified",
  );
  const payloadFixture = path.join(root, "crates/fabrik-cli/test-fixtures/temporal-payload-blocked");
  const payloadQualified = await analyze(payloadQualifiedFixture);
  const bootstrapQualified = await analyze(bootstrapQualifiedFixture);
  const bootstrapDynamicTaskQueueQualified = await analyze(
    bootstrapDynamicTaskQueueQualifiedFixture,
  );
  const bootstrapEsmQualified = await analyze(bootstrapEsmQualifiedFixture);
  const bootstrapSelfFileQualified = await analyze(bootstrapSelfFileQualifiedFixture);
  const activityFactoryQualified = await analyze(activityFactoryQualifiedFixture);
  const testWorkerQualified = await analyze(testWorkerQualifiedFixture);
  const payloadPathQualified = await analyze(payloadPathQualifiedFixture);
  const supportedApiQualified = await analyze(supportedApiQualifiedFixture);
  const activityFailureQualified = await analyze(activityFailureQualifiedFixture);
  const workerVersioningQualified = await analyze(workerVersioningQualifiedFixture);
  const searchAttributesQualified = await analyze(searchAttributesQualifiedFixture);
  const patchingQualified = await analyze(patchingQualifiedFixture);
  const payloadPathQualifiedV2 = await analyze(payloadPathQualifiedV2Fixture);
  const dataConverterFactoryQualified = await analyze(dataConverterFactoryQualifiedFixture);
  const visibilityFixture = path.join(
    root,
    "crates/fabrik-cli/test-fixtures/temporal-visibility-blocked",
  );
  const payload = await analyze(payloadFixture);
  const visibility = await analyze(visibilityFixture);
  assert.equal(bootstrapQualified.summary.hard_block_count, 0);
  assert.equal(bootstrapQualified.workers[0].task_queue, "bootstrap-qualified");
  assert.equal(bootstrapQualified.workers[0].workflows_path, "./workflows");
  assert.equal(bootstrapDynamicTaskQueueQualified.summary.hard_block_count, 0);
  assert.equal(bootstrapDynamicTaskQueueQualified.workers[0].task_queue, null);
  assert.equal(
    bootstrapDynamicTaskQueueQualified.workers[0].workflows_path,
    "./workflows",
  );
  assert.equal(bootstrapEsmQualified.summary.hard_block_count, 0);
  assert.equal(bootstrapEsmQualified.workers[0].task_queue, "bootstrap-esm-qualified");
  assert.equal(bootstrapEsmQualified.workers[0].workflows_path, "./workflows.ts");
  assert.equal(bootstrapSelfFileQualified.summary.hard_block_count, 0);
  assert.equal(bootstrapSelfFileQualified.workers[0].task_queue, "bootstrap-self-file");
  assert.match(
    bootstrapSelfFileQualified.workers[0].workflows_path,
    /src\/worker\.ts$/,
  );
  assert.equal(activityFactoryQualified.summary.hard_block_count, 0);
  assert.equal(activityFactoryQualified.workers[0].task_queue, "activity-factory-qualified");
  assert.equal(
    activityFactoryQualified.workers[0].activity_factory_export,
    "createActivities",
  );
  assert.equal(activityFactoryQualified.workers[0].activity_factory_args_js.length, 1);
  assert.match(activityFactoryQualified.workers[0].activity_factory_args_js[0], /return "Temporal"/);
  assert.equal(testWorkerQualified.summary.hard_block_count, 0);
  assert.equal(testWorkerQualified.workers[0].task_queue, "test-worker-qualified");
  assert.ok(
    testWorkerQualified.findings.every(
      (finding) =>
        finding.file !== "src/workflows.test.ts" || finding.severity !== "hard_block",
    ),
  );
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
  assert.equal(activityFailureQualified.summary.hard_block_count, 0);
  assert.equal(activityFailureQualified.workers[0].task_queue, "activity-failure-qualified");
  assert.equal(workerVersioningQualified.summary.hard_block_count, 0);
  assert.equal(workerVersioningQualified.workers[0].task_queue, "worker-versioning-qualified");
  assert.equal(workerVersioningQualified.workflows[0].versioning_behavior, "AUTO_UPGRADE");
  assert.equal(workerVersioningQualified.workflows[1].versioning_behavior, "PINNED");
  assert.equal(searchAttributesQualified.summary.hard_block_count, 0);
  assert.equal(searchAttributesQualified.workers[0].task_queue, "search-attributes-qualified");
  assert.ok(searchAttributesQualified.files.some((file) => file.uses.includes("search_attributes_memo")));
  assert.equal(patchingQualified.summary.hard_block_count, 0);
  assert.equal(patchingQualified.workers[0].task_queue, "patching-qualified");
  assert.equal(payloadPathQualifiedV2.summary.hard_block_count, 0);
  assert.equal(payloadPathQualifiedV2.workers[0].data_converter_mode, "path_default_temporal");
  assert.equal(dataConverterFactoryQualified.summary.hard_block_count, 0);
  assert.equal(
    dataConverterFactoryQualified.workers[0].data_converter_mode,
    "static_data_converter_factory",
  );
  assert.equal(payload.summary.hard_block_count, 0);
  assert.equal(payload.workers[0].data_converter_mode, "path_static_payload_converter");
  assert.equal(
    payload.workers[0].payload_converter_module,
    "./src/custom-payload-converter.ts",
  );
  assert.ok(!visibility.findings.some((finding) => finding.feature === "visibility_search_usage"));
  assert.equal(visibility.summary.hard_block_count, 0);
  assert.ok(visibility.files.some((file) => file.uses.includes("search_attributes_memo")));
});
