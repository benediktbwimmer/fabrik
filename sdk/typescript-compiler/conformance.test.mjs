import test from "node:test";
import assert from "node:assert/strict";

import { loadManifest, runConformanceCase } from "./conformance-runner.mjs";

const manifests = [
  "sdk/typescript-compiler/conformance/layer-a-support-fixtures.json",
  "sdk/typescript-compiler/conformance/layer-b-semantic-fixtures.json",
  "sdk/typescript-compiler/conformance/layer-c-trust-fixtures.json",
];

for (const manifestPath of manifests) {
  const manifest = await loadManifest(manifestPath);
  for (const testCase of manifest.cases) {
    test(`${manifest.layer_id}:${testCase.id}`, async () => {
      const result = await runConformanceCase(testCase);
      assert.equal(result.status, "passed", result.error ?? `${testCase.id} failed`);
    });
  }
}
