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
