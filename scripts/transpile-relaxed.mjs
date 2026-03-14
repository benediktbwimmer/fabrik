import fs from "node:fs/promises";
import path from "node:path";
import ts from "typescript";

const [projectRootArg, distDirArg] = process.argv.slice(2);

if (!projectRootArg || !distDirArg) {
  console.error("usage: node scripts/transpile-relaxed.mjs <project-root> <dist-dir>");
  process.exit(1);
}

const projectRoot = path.resolve(projectRootArg);
const distDir = path.resolve(distDirArg);
const CODE_EXTENSIONS = new Set([".ts", ".tsx", ".mts", ".cts", ".js", ".jsx", ".mjs", ".cjs"]);
const COPY_EXTENSIONS = new Set([".json"]);

async function walk(dir, files = []) {
  for (const entry of await fs.readdir(dir, { withFileTypes: true })) {
    if (entry.name === "node_modules" || entry.name === "dist" || entry.name === ".git") {
      continue;
    }
    const fullPath = path.join(dir, entry.name);
    if (entry.isDirectory()) {
      await walk(fullPath, files);
      continue;
    }
    files.push(fullPath);
  }
  return files;
}

function outputPathFor(filePath) {
  const rel = path.relative(projectRoot, filePath);
  const ext = path.extname(rel);
  if (ext === ".ts" || ext === ".tsx" || ext === ".mts" || ext === ".cts") {
    return path.join(distDir, rel.replace(/\.(ts|tsx|mts|cts)$/u, ".js"));
  }
  return path.join(distDir, rel);
}

function compilerOptions() {
  return {
    target: ts.ScriptTarget.ES2022,
    module: ts.ModuleKind.ES2022,
    moduleResolution: ts.ModuleResolutionKind.Bundler,
    esModuleInterop: true,
    resolveJsonModule: true,
    allowJs: true,
    checkJs: false,
    skipLibCheck: true,
    sourceMap: false,
    inlineSources: false,
  };
}

async function ensureDir(filePath) {
  await fs.mkdir(path.dirname(filePath), { recursive: true });
}

async function emitCode(filePath) {
  const source = await fs.readFile(filePath, "utf8");
  const result = ts.transpileModule(source, {
    compilerOptions: compilerOptions(),
    fileName: filePath,
    reportDiagnostics: false,
  });
  const outPath = outputPathFor(filePath);
  await ensureDir(outPath);
  await fs.writeFile(outPath, result.outputText, "utf8");
}

async function copyFile(filePath) {
  const outPath = outputPathFor(filePath);
  await ensureDir(outPath);
  await fs.copyFile(filePath, outPath);
}

const files = await walk(projectRoot);
let emitted = 0;
for (const filePath of files) {
  const ext = path.extname(filePath);
  if (CODE_EXTENSIONS.has(ext)) {
    await emitCode(filePath);
    emitted += 1;
    continue;
  }
  if (COPY_EXTENSIONS.has(ext)) {
    await copyFile(filePath);
  }
}

if (emitted === 0) {
  console.error(`no transpileable inputs found in ${projectRoot}`);
  process.exit(1);
}
