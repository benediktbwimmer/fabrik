import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import fs from "node:fs/promises";
import path from "node:path";
import process from "node:process";
import { pathToFileURL } from "node:url";
import { promisify } from "node:util";

const execFileAsync = promisify(execFile);
const root = "/Users/bene/code/fabrik";
const analyzerPath = path.join(root, "sdk/typescript-compiler/migration-analyzer.mjs");
const compilerPath = path.join(root, "sdk/typescript-compiler/compiler.mjs");
const defaultManifestByLayer = {
  layer_a_support: "sdk/typescript-compiler/conformance/layer-a-support-fixtures.json",
  layer_b_semantic: "sdk/typescript-compiler/conformance/layer-b-semantic-fixtures.json",
  layer_c_trust: "sdk/typescript-compiler/conformance/layer-c-trust-fixtures.json",
};

export async function loadManifest(manifestPath) {
  const resolved = path.isAbsolute(manifestPath) ? manifestPath : path.join(root, manifestPath);
  return JSON.parse(await fs.readFile(resolved, "utf8"));
}

function clipText(text, maxLength = 600) {
  if (!text) return null;
  if (text.length <= maxLength) return text;
  return `${text.slice(0, maxLength)}…`;
}

function tailLines(text, count = 8) {
  if (!text) return null;
  const lines = text
    .split("\n")
    .map((line) => line.trimEnd())
    .filter((line) => line.length > 0);
  if (lines.length === 0) return null;
  return lines.slice(-count).join("\n");
}

function summarizeCommandOutput(text) {
  const lines = text
    .split("\n")
    .map((line) => line.trim())
    .filter((line) => line.length > 0);
  return (
    lines.find((line) => line.startsWith("test result:")) ??
    lines.find((line) => line.startsWith("running ")) ??
    lines.at(-1) ??
    "command completed"
  );
}

function parseRustTestSummary(text) {
  const match = text.match(
    /test result:\s+\w+\.\s+(\d+)\s+passed;\s+(\d+)\s+failed;\s+(\d+)\s+ignored;\s+(\d+)\s+measured;\s+(\d+)\s+filtered out;/,
  );
  if (!match) return null;
  const [, passed, failed, ignored, measured, filteredOut] = match;
  const parsed = {
    passed: Number(passed),
    failed: Number(failed),
    ignored: Number(ignored),
    measured: Number(measured),
    filtered_out: Number(filteredOut),
  };
  return {
    ...parsed,
    executed: parsed.passed + parsed.failed + parsed.ignored + parsed.measured,
  };
}

function parseRustRunningCount(text) {
  const match = text.match(/running\s+(\d+)\s+tests?/);
  return match ? Number(match[1]) : null;
}

function failureDetails(error) {
  const stdout = String(error?.stdout ?? "");
  const stderr = String(error?.stderr ?? "");
  const combined = `${stdout}${stderr}`;
  const rustSummary = parseRustTestSummary(combined);
  const runningCount = parseRustRunningCount(combined);
  return {
    error: String(error?.message ?? error),
    summary: summarizeCommandOutput(combined),
    observed: rustSummary || runningCount != null
      ? {
          rust_test_summary: rustSummary,
          running_tests: runningCount,
        }
      : undefined,
    evidence: {
      stdout_excerpt: clipText(tailLines(stdout)),
      stderr_excerpt: clipText(tailLines(stderr)),
      combined_excerpt: clipText(tailLines(combined)),
    },
  };
}

function deepValue(value, keyPath) {
  return keyPath.split(".").reduce((current, key) => current?.[key], value);
}

async function runAnalyzerCase(testCase) {
  const project = path.join(root, testCase.project);
  const { stdout } = await execFileAsync("node", [analyzerPath, "--project", project], { cwd: root });
  const payload = JSON.parse(stdout);
  const findings = payload.findings ?? [];
  const workflowExports = (payload.workflows ?? []).map((workflow) => workflow.export_name);
  const workerTaskQueues = (payload.workers ?? []).map((worker) => worker.task_queue).filter(Boolean);

  for (const [key, expected] of Object.entries(testCase.expect.summary ?? {})) {
    assert.equal(payload.summary?.[key], expected, `${testCase.id} expected summary.${key}=${expected}`);
  }
  for (const feature of testCase.expect.finding_features_include ?? []) {
    assert.ok(findings.some((finding) => finding.feature === feature), `${testCase.id} expected finding feature ${feature}`);
  }
  for (const feature of testCase.expect.finding_features_exclude ?? []) {
    assert.ok(!findings.some((finding) => finding.feature === feature), `${testCase.id} did not expect finding feature ${feature}`);
  }
  for (const code of testCase.expect.finding_codes_include ?? []) {
    assert.ok(findings.some((finding) => finding.code === code), `${testCase.id} expected finding code ${code}`);
  }
  for (const exportName of testCase.expect.workflow_exports_include ?? []) {
    assert.ok(workflowExports.includes(exportName), `${testCase.id} expected workflow export ${exportName}`);
  }
  for (const taskQueue of testCase.expect.worker_task_queues_include ?? []) {
    assert.ok(workerTaskQueues.includes(taskQueue), `${testCase.id} expected worker task queue ${taskQueue}`);
  }

  return {
    summary: `workflows=${payload.summary?.workflow_count ?? 0} workers=${payload.summary?.worker_count ?? 0} hard_blocks=${payload.summary?.hard_block_count ?? 0}`,
    observed: {
      workflow_count: payload.summary?.workflow_count ?? 0,
      worker_count: payload.summary?.worker_count ?? 0,
      hard_block_count: payload.summary?.hard_block_count ?? 0,
      finding_features: findings.map((finding) => finding.feature),
    },
    evidence: {
      finding_count: findings.length,
      finding_features: findings.map((finding) => finding.feature),
    },
  };
}

async function runCompilerCase(testCase) {
  const entry = path.join(root, testCase.entry);
  const { stdout } = await execFileAsync(
    "node",
    [
      compilerPath,
      "--entry",
      entry,
      "--export",
      testCase.export_name,
      "--definition-id",
      testCase.definition_id,
      "--version",
      String(testCase.version ?? 1),
    ],
    { cwd: root },
  );
  const artifact = JSON.parse(stdout);
  const serialized = JSON.stringify(artifact);

  for (const [keyPath, expected] of Object.entries(testCase.expect.artifact_equals ?? {})) {
    assert.deepEqual(deepValue(artifact, keyPath), expected, `${testCase.id} expected ${keyPath}`);
  }
  for (const suffix of testCase.expect.source_files_include ?? []) {
    assert.ok(
      (artifact.source_files ?? []).some((file) => file.endsWith(suffix)),
      `${testCase.id} expected source file suffix ${suffix}`,
    );
  }
  for (const pattern of testCase.expect.serialized_matches ?? []) {
    assert.match(serialized, new RegExp(pattern), `${testCase.id} expected serialized pattern ${pattern}`);
  }
  for (const pattern of testCase.expect.serialized_excludes ?? []) {
    assert.doesNotMatch(serialized, new RegExp(pattern), `${testCase.id} expected serialized exclusion ${pattern}`);
  }

  return {
    summary: `definition=${artifact.definition_id} source_files=${artifact.source_files?.length ?? 0}`,
    observed: {
      definition_id: artifact.definition_id,
      source_file_count: artifact.source_files?.length ?? 0,
    },
    evidence: {
      source_files: artifact.source_files ?? [],
      state_count: Object.keys(artifact.workflow?.states ?? {}).length,
    },
  };
}

async function runCompilerErrorCase(testCase) {
  const entry = path.join(root, testCase.entry);
  let rejected = false;
  try {
    await execFileAsync(
      "node",
      [
        compilerPath,
        "--entry",
        entry,
        "--export",
        testCase.export_name,
        "--definition-id",
        testCase.definition_id,
        "--version",
        String(testCase.version ?? 1),
      ],
      { cwd: root },
    );
  } catch (error) {
    rejected = true;
    const output = `${error.stdout ?? ""}${error.stderr ?? ""}`;
    for (const pattern of testCase.expect.error_matches ?? []) {
      assert.match(output, new RegExp(pattern), `${testCase.id} expected error pattern ${pattern}`);
    }
    return {
      summary: "compiler rejection matched expected diagnostics",
      observed: {
        rejected: true,
      },
      evidence: {
        combined_excerpt: clipText(tailLines(output)),
      },
    };
  }
  assert.ok(rejected, `${testCase.id} expected compiler failure`);
  return {
    summary: "compiler rejection expected",
    observed: {
      rejected,
    },
  };
}

async function runCommandCase(testCase) {
  const { stdout, stderr } = await execFileAsync(testCase.command, testCase.args ?? [], { cwd: root });
  const combined = `${stdout ?? ""}${stderr ?? ""}`;
  const rustSummary = parseRustTestSummary(combined);
  const runningCount = parseRustRunningCount(combined);
  if (testCase.expect?.output_matches) {
    for (const pattern of testCase.expect.output_matches) {
      assert.match(combined, new RegExp(pattern), `${testCase.id} expected output pattern ${pattern}`);
    }
  }
  if (testCase.expect?.output_excludes) {
    for (const pattern of testCase.expect.output_excludes) {
      assert.doesNotMatch(combined, new RegExp(pattern), `${testCase.id} expected output exclusion ${pattern}`);
    }
  }
  if (testCase.expect?.executed_tests_exact != null) {
    assert.ok(rustSummary, `${testCase.id} expected a Rust test summary line`);
    assert.equal(
      rustSummary.executed,
      testCase.expect.executed_tests_exact,
      `${testCase.id} expected executed tests = ${testCase.expect.executed_tests_exact}`,
    );
  }
  if (testCase.expect?.executed_tests_min != null) {
    assert.ok(rustSummary, `${testCase.id} expected a Rust test summary line`);
    assert.ok(
      rustSummary.executed >= testCase.expect.executed_tests_min,
      `${testCase.id} expected executed tests >= ${testCase.expect.executed_tests_min}`,
    );
  }
  if (testCase.expect?.filtered_tests_max != null) {
    assert.ok(rustSummary, `${testCase.id} expected a Rust test summary line`);
    assert.ok(
      rustSummary.filtered_out <= testCase.expect.filtered_tests_max,
      `${testCase.id} expected filtered tests <= ${testCase.expect.filtered_tests_max}`,
    );
  }
  if (testCase.expect?.running_tests_exact != null) {
    assert.equal(
      runningCount,
      testCase.expect.running_tests_exact,
      `${testCase.id} expected running tests = ${testCase.expect.running_tests_exact}`,
    );
  }
  return {
    summary: summarizeCommandOutput(combined),
    observed: {
      command: [testCase.command, ...(testCase.args ?? [])].join(" "),
      rust_test_summary: rustSummary,
      running_tests: runningCount,
    },
    evidence: {
      stdout_excerpt: clipText(tailLines(stdout)),
      stderr_excerpt: clipText(tailLines(stderr)),
      combined_excerpt: clipText(tailLines(combined)),
    },
  };
}

export async function runConformanceCase(testCase) {
  const startedAt = Date.now();
  try {
    let details;
    if (testCase.kind === "analyzer") {
      details = await runAnalyzerCase(testCase);
    } else if (testCase.kind === "compiler") {
      details = await runCompilerCase(testCase);
    } else if (testCase.kind === "compiler_error") {
      details = await runCompilerErrorCase(testCase);
    } else if (testCase.kind === "command") {
      details = await runCommandCase(testCase);
    } else {
      throw new Error(`unsupported case kind ${testCase.kind}`);
    }
    return {
      id: testCase.id,
      title: testCase.title ?? testCase.id,
      kind: testCase.kind,
      status: "passed",
      duration_ms: Date.now() - startedAt,
      ...details,
    };
  } catch (error) {
    const details = failureDetails(error);
    return {
      id: testCase.id,
      title: testCase.title ?? testCase.id,
      kind: testCase.kind,
      status: "failed",
      duration_ms: Date.now() - startedAt,
      ...details,
    };
  }
}

export async function runManifest(manifestPath) {
  const manifest = await loadManifest(manifestPath);
  const results = [];
  for (const testCase of manifest.cases ?? []) {
    results.push(await runConformanceCase(testCase));
  }
  return {
    schema_version: 1,
    layer_id: manifest.layer_id,
    title: manifest.title,
    purpose: manifest.purpose,
    case_count: results.length,
    passed_count: results.filter((result) => result.status === "passed").length,
    failed_count: results.filter((result) => result.status === "failed").length,
    results,
  };
}

function parseArgs(argv) {
  let manifestPath = null;
  let layerId = null;
  let outputPath = null;
  for (let index = 0; index < argv.length; index += 1) {
    const arg = argv[index];
    if (arg === "--manifest") {
      manifestPath = argv[index + 1];
      index += 1;
    } else if (arg === "--layer") {
      layerId = argv[index + 1];
      index += 1;
    } else if (arg === "--output") {
      outputPath = argv[index + 1];
      index += 1;
    } else {
      throw new Error(`unknown arg ${arg}`);
    }
  }
  if (!manifestPath && !layerId) {
    throw new Error("usage: node sdk/typescript-compiler/conformance-runner.mjs --layer <layer_id> [--output <path>]");
  }
  if (!manifestPath && layerId) {
    manifestPath = defaultManifestByLayer[layerId];
  }
  if (!manifestPath) {
    throw new Error(`no manifest configured for layer ${layerId}`);
  }
  return { manifestPath, outputPath };
}

async function main() {
  const { manifestPath, outputPath } = parseArgs(process.argv.slice(2));
  const report = await runManifest(manifestPath);
  if (outputPath) {
    const resolved = path.isAbsolute(outputPath) ? outputPath : path.join(root, outputPath);
    await fs.mkdir(path.dirname(resolved), { recursive: true });
    await fs.writeFile(resolved, JSON.stringify(report, null, 2));
  }
  process.stdout.write(`${JSON.stringify(report, null, 2)}\n`);
  if (report.failed_count > 0) {
    process.exitCode = 2;
  }
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  main().catch((error) => {
    console.error(error);
    process.exit(1);
  });
}
