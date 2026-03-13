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
  assert.equal(payload.workflows[0].export_name, "orderWorkflow");
  assert.equal(payload.workers[0].task_queue, "orders");
});

test("migration analyzer blocks payload and visibility landmines", async () => {
  const payloadFixture = path.join(root, "crates/fabrik-cli/test-fixtures/temporal-payload-blocked");
  const visibilityFixture = path.join(
    root,
    "crates/fabrik-cli/test-fixtures/temporal-visibility-blocked",
  );
  const payload = await analyze(payloadFixture);
  const visibility = await analyze(visibilityFixture);
  assert.ok(payload.findings.some((finding) => finding.feature === "payload_data_converter_usage"));
  assert.ok(visibility.findings.some((finding) => finding.feature === "visibility_search_usage"));
});
