import fsSync from "node:fs";
import fs from "node:fs/promises";
import path from "node:path";
import process from "node:process";
import ts from "typescript";

const WORKFLOW_SUPPORTED_IMPORTS = new Set([
  "ActivityCancellationType",
  "ActivityFailure",
  "ApplicationFailure",
  "CancellationScope",
  "condition",
  "continueAsNew",
  "defineQuery",
  "defineSignal",
  "defineUpdate",
  "executeChild",
  "getExternalWorkflowHandle",
  "inWorkflowContext",
  "isCancellation",
  "log",
  "ParentClosePolicy",
  "patched",
  "proxyActivities",
  "proxyLocalActivities",
  "proxySinks",
  "SearchAttributes",
  "Sinks",
  "WorkflowInterceptors",
  "deprecatePatch",
  "setWorkflowOptions",
  "setHandler",
  "sleep",
  "startChild",
  "upsertSearchAttributes",
  "uuid4",
  "workflowInfo",
]);

const BLOCKED_PAYLOAD_IMPORTS = new Set([
  "DefaultFailureConverter",
  "LoadedDataConverter",
  "PayloadConverter",
]);

const SUPPORTED_DATA_CONVERTER_IMPORTS = new Set([
  "defaultDataConverter",
]);

const SUPPORTED_PAYLOAD_CONVERTER_IMPORTS = new Set([
  "defaultPayloadConverter",
]);

const SUPPORTED_PAYLOAD_CONVERTER_CONSTRUCTORS = new Set([
  "DefaultPayloadConverter",
]);

const WORKER_BLOCKING_PROPERTIES = new Map([
  ["payloadCodec", "custom payload codecs are not supported by the migration pipeline"],
  ["payloadConverterPath", "custom payload converters are not supported by the migration pipeline"],
  ["codecServer", "codec servers are not supported by the migration pipeline"],
  ["workflowInterceptorModules", "workflow interceptors are not migration-ready yet"],
]);

const SUPPORT_MATRIX_URL = new URL("./temporal-ts-subset-support-matrix.json", import.meta.url);

function usage() {
  console.error("usage: node sdk/typescript-compiler/migration-analyzer.mjs --project <dir>");
  process.exit(1);
}

async function loadSupportMatrixDocument() {
  return JSON.parse(await fs.readFile(SUPPORT_MATRIX_URL, "utf8"));
}

function parseArgs(argv) {
  if (argv.length !== 2 || argv[0] !== "--project") {
    usage();
  }
  return {
    projectRoot: path.resolve(argv[1]),
  };
}

function relativeProjectPath(projectRoot, fileName) {
  return path.relative(projectRoot, fileName).split(path.sep).join("/");
}

function formatNodeLocation(projectRoot, node) {
  const sourceFile = node.getSourceFile?.() ?? node.parent?.getSourceFile?.();
  if (!sourceFile) {
    return { file: "<generated>", line: 1, column: 1 };
  }
  const start = typeof node.getStart === "function" ? node.getStart(sourceFile) : (node.pos ?? 0);
  const { line, character } = sourceFile.getLineAndCharacterOfPosition(start);
  return {
    file: relativeProjectPath(projectRoot, sourceFile.fileName),
    line: line + 1,
    column: character + 1,
  };
}

function createFinding(projectRoot, severity, code, feature, node, message, remediation, symbol = null) {
  const location = formatNodeLocation(projectRoot, node);
  return {
    code,
    severity,
    feature,
    file: location.file,
    line: location.line,
    column: location.column,
    symbol,
    message,
    remediation,
  };
}

function isProjectSourceFile(projectRoot, sourceFile) {
  const normalized = path.resolve(sourceFile.fileName);
  return !sourceFile.isDeclarationFile && normalized.startsWith(projectRoot);
}

async function collectProjectFiles(projectRoot) {
  const files = [];
  async function visit(currentDir) {
    const entries = await fs.readdir(currentDir, { withFileTypes: true });
    entries.sort((left, right) => left.name.localeCompare(right.name));
    for (const entry of entries) {
      if ([".git", ".next", ".turbo", "dist", "build", "coverage", "node_modules"].includes(entry.name)) {
        continue;
      }
      const fullPath = path.join(currentDir, entry.name);
      if (entry.isDirectory()) {
        await visit(fullPath);
        continue;
      }
      if (!/\.(c|m)?(t|j)sx?$/.test(entry.name) || entry.name.endsWith(".d.ts")) {
        continue;
      }
      files.push(fullPath);
    }
  }
  await visit(projectRoot);
  return files;
}

function createProgram(projectRoot, files) {
  const configPath = ts.findConfigFile(projectRoot, ts.sys.fileExists, "tsconfig.json");
  const options = configPath
    ? ts.parseJsonConfigFileContent(
        ts.readConfigFile(configPath, ts.sys.readFile).config,
        ts.sys,
        path.dirname(configPath),
      ).options
    : {
        target: ts.ScriptTarget.ES2022,
        module: ts.ModuleKind.NodeNext,
        moduleResolution: ts.ModuleResolutionKind.NodeNext,
        allowJs: true,
      };
  return ts.createProgram(files, options);
}

function collectImportInfo(sourceFile) {
  const workflowImports = new Map();
  const workerImports = new Map();
  const clientImports = new Map();
  const commonImports = new Map();

  for (const statement of sourceFile.statements) {
    if (!ts.isImportDeclaration(statement) || !statement.importClause) {
      continue;
    }
    const moduleName = statement.moduleSpecifier.text;
    const bindings = statement.importClause.namedBindings;
    if (!bindings || !ts.isNamedImports(bindings)) {
      continue;
    }
    for (const element of bindings.elements) {
      const importedName = element.propertyName?.text ?? element.name.text;
      const localName = element.name.text;
      if (moduleName === "@temporalio/workflow") workflowImports.set(localName, importedName);
      if (moduleName === "@temporalio/worker") workerImports.set(localName, importedName);
      if (moduleName === "@temporalio/client") clientImports.set(localName, importedName);
      if (moduleName === "@temporalio/common") commonImports.set(localName, importedName);
    }
  }

  return { workflowImports, workerImports, clientImports, commonImports };
}

function findStaticString(expression, bindings = null, checker = null) {
  if (ts.isStringLiteral(expression) || ts.isNoSubstitutionTemplateLiteral(expression)) {
    return expression.text;
  }
  if (bindings != null) {
    const value = evaluateStaticValue(expression, bindings, checker);
    return typeof value === "string" ? value : null;
  }
  return null;
}

function findObjectProperty(objectLiteral, propertyName) {
  return objectLiteral.properties.find((property) => {
    if ((!ts.isPropertyAssignment(property) && !ts.isShorthandPropertyAssignment(property)) || property.name == null) {
      return false;
    }
    if (ts.isIdentifier(property.name)) return property.name.text === propertyName;
    if (ts.isStringLiteral(property.name)) return property.name.text === propertyName;
    return false;
  });
}

function isStaticPrimitiveLiteral(node) {
  if (
    ts.isStringLiteral(node) ||
    ts.isNoSubstitutionTemplateLiteral(node) ||
    ts.isNumericLiteral(node)
  ) {
    return true;
  }
  return node.kind === ts.SyntaxKind.TrueKeyword || node.kind === ts.SyntaxKind.FalseKeyword || node.kind === ts.SyntaxKind.NullKeyword;
}

function createScopeBindings(parentBindings, node) {
  const bindings = new Map(parentBindings);
  const statements = node.statements ?? [];
  for (const statement of statements) {
    if (!ts.isVariableStatement(statement) || !(statement.declarationList.flags & ts.NodeFlags.Const)) {
      continue;
    }
    for (const declaration of statement.declarationList.declarations) {
      if (ts.isIdentifier(declaration.name) && declaration.initializer) {
        bindings.set(declaration.name.text, declaration.initializer);
      }
    }
  }
  return bindings;
}

function findNearestTsconfig(startFile, projectRoot) {
  let current = path.dirname(startFile);
  const boundary = path.resolve(projectRoot);
  while (true) {
    const candidate = path.join(current, "tsconfig.json");
    if (ts.sys.fileExists(candidate)) {
      return candidate;
    }
    if (current === boundary) {
      return null;
    }
    const parent = path.dirname(current);
    if (parent === current) {
      return null;
    }
    current = parent;
  }
}

function parseCompilerOptionsForFile(projectRoot, fromFileName) {
  const configPath = findNearestTsconfig(fromFileName, projectRoot);
  if (!configPath) {
    return {
      target: ts.ScriptTarget.ES2022,
      module: ts.ModuleKind.NodeNext,
      moduleResolution: ts.ModuleResolutionKind.NodeNext,
      allowJs: true,
    };
  }
  const config = ts.readConfigFile(configPath, ts.sys.readFile);
  if (config.error) {
    return {
      target: ts.ScriptTarget.ES2022,
      module: ts.ModuleKind.NodeNext,
      moduleResolution: ts.ModuleResolutionKind.NodeNext,
      allowJs: true,
    };
  }
  return ts.parseJsonConfigFileContent(
    config.config,
    ts.sys,
    path.dirname(configPath),
    undefined,
    configPath,
  ).options;
}

function moduleResolutionCandidates(base) {
  const candidates = [];
  if (path.extname(base)) {
    candidates.push(base);
    return candidates;
  }
  for (const extension of [".ts", ".mts", ".cts", ".js", ".mjs", ".cjs"]) {
    candidates.push(`${base}${extension}`);
  }
  for (const indexName of ["index.ts", "index.mts", "index.cts", "index.js", "index.mjs", "index.cjs"]) {
    candidates.push(path.join(base, indexName));
  }
  return candidates;
}

function findWorkspacePackages(projectRoot) {
  const packages = [];
  const stack = [projectRoot];
  while (stack.length > 0) {
    const current = stack.pop();
    for (const entry of fsSync.readdirSync(current, { withFileTypes: true })) {
      if (entry.isDirectory()) {
        if ([".git", "node_modules", "dist", "build", "coverage"].includes(entry.name)) {
          continue;
        }
        stack.push(path.join(current, entry.name));
        continue;
      }
      if (entry.name !== "package.json") {
        continue;
      }
      const packageJsonPath = path.join(current, entry.name);
      try {
        const manifest = JSON.parse(fsSync.readFileSync(packageJsonPath, "utf8"));
        if (typeof manifest.name === "string" && manifest.name.length > 0) {
          packages.push({ dir: current, manifest });
        }
      } catch {
        // Ignore invalid workspace manifests during static analysis.
      }
    }
  }
  return packages;
}

function resolveWorkspaceModulePath(projectRoot, specifier) {
  const packages = findWorkspacePackages(projectRoot);
  const matched = packages
    .filter(({ manifest }) => specifier === manifest.name || specifier.startsWith(`${manifest.name}/`))
    .sort((left, right) => right.manifest.name.length - left.manifest.name.length)[0];
  if (!matched) {
    return null;
  }
  const { dir, manifest } = matched;
  const subpath =
    specifier === manifest.name ? "" : specifier.slice(manifest.name.length + 1);
  const sourceRoots = new Set([dir]);
  for (const field of ["types", "source", "module", "main"]) {
    if (typeof manifest[field] !== "string" || manifest[field].length === 0) {
      continue;
    }
    sourceRoots.add(path.resolve(dir, path.dirname(manifest[field])));
  }
  const subpaths = new Set();
  if (subpath.length === 0) {
    for (const field of ["types", "source", "module", "main"]) {
      if (typeof manifest[field] === "string" && manifest[field].length > 0) {
        subpaths.add(manifest[field]);
      }
    }
    subpaths.add("index");
  } else {
    subpaths.add(subpath);
    for (const prefix of ["lib/", "dist/", "build/", "src/"]) {
      if (subpath.startsWith(prefix)) {
        subpaths.add(subpath.slice(prefix.length));
      }
    }
  }
  for (const root of sourceRoots) {
    for (const candidateSubpath of subpaths) {
      for (const candidate of moduleResolutionCandidates(path.resolve(root, candidateSubpath))) {
        if (ts.sys.fileExists(candidate)) {
          return candidate;
        }
      }
    }
  }
  return null;
}

function declarationKey(declaration) {
  const sourceFile = declaration.getSourceFile?.();
  return `${sourceFile?.fileName ?? "<unknown>"}:${declaration.pos}:${declaration.end}`;
}

function resolveIdentifierInitializer(node, bindings, checker, seen) {
  if (!seen.has(`binding:${node.text}`) && bindings.has(node.text)) {
    return bindings.get(node.text) ?? null;
  }
  if (!checker) {
    return null;
  }
  if (ts.isShorthandPropertyAssignment(node.parent) && node.parent.name === node) {
    let shorthandSymbol = checker.getShorthandAssignmentValueSymbol(node.parent);
    if (shorthandSymbol) {
      if (shorthandSymbol.flags & ts.SymbolFlags.Alias) {
        shorthandSymbol = checker.getAliasedSymbol(shorthandSymbol);
      }
      for (const declaration of shorthandSymbol.declarations ?? []) {
        if (!ts.isVariableDeclaration(declaration) || declaration.initializer == null) {
          continue;
        }
        const key = declarationKey(declaration);
        if (seen.has(key)) {
          continue;
        }
        return declaration.initializer;
      }
    }
  }
  let symbol = checker.getSymbolAtLocation(node);
  if (!symbol) {
    return null;
  }
  if (symbol.flags & ts.SymbolFlags.Alias) {
    symbol = checker.getAliasedSymbol(symbol);
  }
  for (const declaration of symbol.declarations ?? []) {
    if (!ts.isVariableDeclaration(declaration) || declaration.initializer == null) {
      continue;
    }
    const key = declarationKey(declaration);
    if (seen.has(key)) {
      continue;
    }
    return declaration.initializer;
  }
  return null;
}

function isImportMetaUrlExpression(node) {
  return (
    ts.isPropertyAccessExpression(node) &&
    ts.isMetaProperty(node.expression) &&
    node.expression.keywordToken === ts.SyntaxKind.ImportKeyword &&
    node.expression.name.text === "meta" &&
    node.name.text === "url"
  );
}

function isRequireResolveCall(node) {
  return (
    ts.isCallExpression(node) &&
    ts.isPropertyAccessExpression(node.expression) &&
    ts.isIdentifier(node.expression.expression) &&
    node.expression.expression.text === "require" &&
    node.expression.name.text === "resolve"
  );
}

function isPathExtnameImportMetaUrlCall(node) {
  return (
    ts.isCallExpression(node) &&
    ts.isPropertyAccessExpression(node.expression) &&
    node.expression.name.text === "extname" &&
    node.arguments.length === 1 &&
    isImportMetaUrlExpression(node.arguments[0])
  );
}

function isFileUrlToPathCall(node) {
  return ts.isCallExpression(node) && ts.isIdentifier(node.expression) && node.expression.text === "fileURLToPath";
}

function isUrlConstructorWithImportMetaBase(node) {
  return (
    ts.isNewExpression(node) &&
    ts.isIdentifier(node.expression) &&
    node.expression.text === "URL" &&
    node.arguments?.length === 2 &&
    isImportMetaUrlExpression(node.arguments[1])
  );
}

function evaluateStaticValue(node, bindings, checker = null, seen = new Set()) {
  if (
    ts.isStringLiteral(node) ||
    ts.isNoSubstitutionTemplateLiteral(node)
  ) {
    return node.text;
  }
  if (ts.isNumericLiteral(node)) {
    return Number(node.text);
  }
  if (node.kind === ts.SyntaxKind.TrueKeyword) {
    return true;
  }
  if (node.kind === ts.SyntaxKind.FalseKeyword) {
    return false;
  }
  if (node.kind === ts.SyntaxKind.NullKeyword) {
    return null;
  }
  if (ts.isParenthesizedExpression(node)) {
    return evaluateStaticValue(node.expression, bindings, checker, seen);
  }
  if (ts.isIdentifier(node)) {
    if (node.text === "__filename") {
      return node.getSourceFile().fileName;
    }
    if (node.text === "__dirname") {
      return path.dirname(node.getSourceFile().fileName);
    }
    const initializer = resolveIdentifierInitializer(node, bindings, checker, seen);
    if (initializer == null) {
      return undefined;
    }
    const key = ts.isVariableDeclaration(initializer.parent)
      ? declarationKey(initializer.parent)
      : `binding:${node.text}`;
    if (seen.has(key)) {
      return undefined;
    }
    seen.add(key);
    const resolved = evaluateStaticValue(initializer, bindings, checker, seen);
    seen.delete(key);
    return resolved;
  }
  if (ts.isTemplateExpression(node)) {
    let value = node.head.text;
    for (const span of node.templateSpans) {
      let resolved = evaluateStaticValue(span.expression, bindings, checker, seen);
      if (resolved === undefined && isPathExtnameImportMetaUrlCall(span.expression)) {
        resolved = path.extname(node.getSourceFile().fileName);
      }
      if (resolved === undefined || (typeof resolved !== "string" && typeof resolved !== "number")) {
        return undefined;
      }
      value += String(resolved);
      value += span.literal.text;
    }
    return value;
  }
  if (ts.isObjectLiteralExpression(node)) {
    const value = {};
    for (const property of node.properties) {
      if (ts.isPropertyAssignment(property)) {
        if (property.name == null) {
          return undefined;
        }
        const key =
          ts.isIdentifier(property.name) ? property.name.text :
          ts.isStringLiteral(property.name) ? property.name.text :
          null;
        if (key == null) {
          return undefined;
        }
        const resolved = evaluateStaticValue(property.initializer, bindings, checker, seen);
        if (resolved === undefined) {
          return undefined;
        }
        value[key] = resolved;
        continue;
      }
      if (ts.isShorthandPropertyAssignment(property)) {
        const resolved = evaluateStaticValue(property.name, bindings, checker, seen);
        if (resolved === undefined) {
          return undefined;
        }
        value[property.name.text] = resolved;
        continue;
      }
      return undefined;
    }
    return value;
  }
  if (ts.isArrayLiteralExpression(node)) {
    const value = [];
    for (const element of node.elements) {
      const resolved = evaluateStaticValue(element, bindings, checker, seen);
      if (resolved === undefined) {
        return undefined;
      }
      value.push(resolved);
    }
    return value;
  }
  if (ts.isConditionalExpression(node)) {
    const condition = evaluateStaticValue(node.condition, bindings, checker, seen);
    if (typeof condition !== "boolean") {
      return undefined;
    }
    return evaluateStaticValue(condition ? node.whenTrue : node.whenFalse, bindings, checker, seen);
  }
  if (ts.isBinaryExpression(node)) {
    const left = evaluateStaticValue(node.left, bindings, checker, seen);
    const right = evaluateStaticValue(node.right, bindings, checker, seen);
    if (left === undefined || right === undefined) {
      if (
        node.operatorToken.kind === ts.SyntaxKind.BarBarToken &&
        (left === undefined || left === false || left === null || left === "")
      ) {
        return right;
      }
      if (
        node.operatorToken.kind === ts.SyntaxKind.QuestionQuestionToken &&
        (left === undefined || left === null)
      ) {
        return right;
      }
      return undefined;
    }
    switch (node.operatorToken.kind) {
      case ts.SyntaxKind.PlusToken:
        if (
          (typeof left === "string" || typeof left === "number") &&
          (typeof right === "string" || typeof right === "number")
        ) {
          return `${left}${right}`;
        }
        return undefined;
      case ts.SyntaxKind.EqualsEqualsEqualsToken:
      case ts.SyntaxKind.EqualsEqualsToken:
        return left === right;
      case ts.SyntaxKind.ExclamationEqualsEqualsToken:
      case ts.SyntaxKind.ExclamationEqualsToken:
        return left !== right;
      case ts.SyntaxKind.BarBarToken:
        return left || right;
      case ts.SyntaxKind.QuestionQuestionToken:
        return left ?? right;
      default:
        return undefined;
    }
  }
  if (ts.isPrefixUnaryExpression(node) && node.operator === ts.SyntaxKind.ExclamationToken) {
    const value = evaluateStaticValue(node.operand, bindings, checker, seen);
    return typeof value === "boolean" ? !value : undefined;
  }
  if (isRequireResolveCall(node) && node.arguments.length === 1) {
    const value = evaluateStaticValue(node.arguments[0], bindings, checker, seen);
    return typeof value === "string" ? value : undefined;
  }
  if (isPathExtnameImportMetaUrlCall(node)) {
    return path.extname(node.getSourceFile().fileName);
  }
  if (isUrlConstructorWithImportMetaBase(node)) {
    const value = evaluateStaticValue(node.arguments[0], bindings, checker, seen);
    return typeof value === "string" ? value : undefined;
  }
  if (isFileUrlToPathCall(node) && node.arguments.length === 1) {
    const value = evaluateStaticValue(node.arguments[0], bindings, checker, seen);
    return typeof value === "string" ? value : undefined;
  }
  if (
    ts.isNewExpression(node) &&
    ts.isIdentifier(node.expression) &&
    node.expression.text === "Date"
  ) {
    if ((node.arguments?.length ?? 0) === 0) {
      return new Date(0);
    }
    if ((node.arguments?.length ?? 0) === 1) {
      const value = evaluateStaticValue(node.arguments[0], bindings, checker, seen);
      if (typeof value === "string" || typeof value === "number") {
        const date = new Date(value);
        return Number.isNaN(date.getTime()) ? undefined : date;
      }
    }
    return undefined;
  }
  return undefined;
}

function isSupportedMemoObject(node, bindings, checker = null) {
  const value = evaluateStaticValue(node, bindings, checker);
  if (value == null || Array.isArray(value) || typeof value !== "object") {
    return false;
  }
  return Object.values(value).every(
    (entry) =>
      entry == null ||
      typeof entry === "string" ||
      typeof entry === "number" ||
      typeof entry === "boolean",
  );
}

function isSupportedSearchAttributesObject(node, bindings, checker = null) {
  const value = evaluateStaticValue(node, bindings, checker);
  if (value == null || Array.isArray(value) || typeof value !== "object") {
    return false;
  }
  const isSupportedEntry = (entry) =>
    typeof entry === "string" ||
    typeof entry === "number" ||
    typeof entry === "boolean" ||
    entry instanceof Date;
  return Object.values(value).every((entry) => {
    if (isSupportedEntry(entry)) {
      return true;
    }
    if (!Array.isArray(entry)) {
      return false;
    }
    return entry.every(isSupportedEntry);
  });
}

function maybeImportedModulePath(sourceFile, localName) {
  for (const statement of sourceFile.statements) {
    if (!ts.isImportDeclaration(statement) || !statement.importClause) {
      continue;
    }
    const bindings = statement.importClause.namedBindings;
    if (!bindings) {
      continue;
    }
    if (ts.isNamespaceImport(bindings) && bindings.name.text === localName) {
      return statement.moduleSpecifier.text;
    }
    if (!ts.isNamedImports(bindings)) {
      continue;
    }
    for (const element of bindings.elements) {
      if (element.name.text === localName) {
        return statement.moduleSpecifier.text;
      }
    }
  }
  return null;
}

function isTestSupportFile(relativeFile) {
  const normalized = relativeFile.replaceAll("\\", "/");
  if (normalized.includes("/test/") || normalized.includes("/tests/") || normalized.includes("/__tests__/")) {
    return true;
  }
  const fileName = normalized.split("/").at(-1) ?? normalized;
  return fileName.includes(".test.") || fileName.includes(".spec.");
}

function transpileExpressionToJavaScript(expressionText) {
  const result = ts
    .transpileModule(`export default (${expressionText});`, {
      compilerOptions: {
        module: ts.ModuleKind.ES2020,
        target: ts.ScriptTarget.ES2020,
      },
    })
    .outputText.trim();
  const prefix = "export default ";
  if (!result.startsWith(prefix)) {
    return null;
  }
  return result.slice(prefix.length).replace(/;\s*$/, "");
}

function staticBootstrapArgExpression(node, currentBindings, checker) {
  if (!node) {
    return null;
  }
  const resolved = resolveStaticExpression(node, currentBindings, checker) ?? node;
  if (
    ts.isStringLiteralLike(resolved) ||
    ts.isNumericLiteral(resolved) ||
    resolved.kind === ts.SyntaxKind.TrueKeyword ||
    resolved.kind === ts.SyntaxKind.FalseKeyword ||
    resolved.kind === ts.SyntaxKind.NullKeyword ||
    ts.isArrayLiteralExpression(resolved) ||
    ts.isObjectLiteralExpression(resolved)
  ) {
    return transpileExpressionToJavaScript(resolved.getText(resolved.getSourceFile()));
  }
  if (ts.isIdentifier(resolved)) {
    const binding = currentBindings.get(resolved.text);
    if (binding?.initializer) {
      return staticBootstrapArgExpression(binding.initializer, currentBindings, checker);
    }
  }
  return null;
}

function analyzeActivitiesRegistration(sourceFile, activitiesProperty, currentBindings, checker) {
  if (
    ts.isPropertyAssignment(activitiesProperty) &&
    ts.isIdentifier(activitiesProperty.initializer)
  ) {
    const activitiesReference = activitiesProperty.initializer.text;
    return {
      supported: true,
      activities_reference: activitiesReference,
      activity_module: maybeImportedModulePath(sourceFile, activitiesReference),
      activity_factory_export: null,
      activity_factory_args_js: [],
    };
  }
  if (ts.isShorthandPropertyAssignment(activitiesProperty)) {
    const activitiesReference = activitiesProperty.name.text;
    return {
      supported: true,
      activities_reference: activitiesReference,
      activity_module: maybeImportedModulePath(sourceFile, activitiesReference),
      activity_factory_export: null,
      activity_factory_args_js: [],
    };
  }
  if (
    ts.isPropertyAssignment(activitiesProperty) &&
    ts.isObjectLiteralExpression(activitiesProperty.initializer)
  ) {
    return {
      supported: true,
      activities_reference: "inline-object",
      activity_module: null,
      activity_factory_export: null,
      activity_factory_args_js: [],
    };
  }
  if (
    ts.isPropertyAssignment(activitiesProperty) &&
    ts.isCallExpression(activitiesProperty.initializer)
  ) {
    const call = activitiesProperty.initializer;
    let modulePath = null;
    let factoryExport = null;
    if (ts.isIdentifier(call.expression)) {
      modulePath = maybeImportedModulePath(sourceFile, call.expression.text);
      factoryExport = call.expression.text;
    } else if (
      ts.isPropertyAccessExpression(call.expression) &&
      ts.isIdentifier(call.expression.expression)
    ) {
      modulePath = maybeImportedModulePath(sourceFile, call.expression.expression.text);
      factoryExport = call.expression.name.text;
    }
    if (modulePath && factoryExport) {
      const argsJs = [];
      let requiresRuntimeHelper = false;
      for (const argument of call.arguments) {
        const rendered = staticBootstrapArgExpression(argument, currentBindings, checker);
        if (rendered == null) {
          requiresRuntimeHelper = true;
          break;
        }
        argsJs.push(rendered);
      }
      return {
        supported: true,
        activities_reference: factoryExport,
        activity_module: modulePath,
        activity_factory_export: factoryExport,
        activity_factory_args_js: argsJs,
        activity_runtime_helper: requiresRuntimeHelper,
      };
    }
  }
  return { supported: false };
}

function supportsStaticWorkflowInterceptors(interceptorsProperty, currentBindings, checker) {
  if (!ts.isPropertyAssignment(interceptorsProperty)) {
    return false;
  }
  const resolved = resolveStaticExpression(interceptorsProperty.initializer, currentBindings, checker);
  if (!ts.isObjectLiteralExpression(resolved)) {
    return false;
  }
  const workflowModules = findObjectProperty(resolved, "workflowModules");
  if (workflowModules == null || !ts.isPropertyAssignment(workflowModules)) {
    return false;
  }
  const rendered = staticBootstrapArgExpression(workflowModules.initializer, currentBindings, checker);
  return rendered != null;
}

function resolveProjectModulePath(projectRoot, fromFileName, specifier) {
  const compilerOptions = parseCompilerOptionsForFile(projectRoot, fromFileName);
  const resolved = ts.resolveModuleName(specifier, fromFileName, compilerOptions, ts.sys).resolvedModule;
  if (resolved?.resolvedFileName && ts.sys.fileExists(resolved.resolvedFileName)) {
    return resolved.resolvedFileName;
  }
  if (specifier.startsWith(".")) {
    const candidates = [];
    for (const base of [
      path.resolve(path.dirname(fromFileName), specifier),
      path.resolve(projectRoot, specifier),
    ]) {
      if (path.extname(base)) {
        candidates.push(base);
      } else {
        for (const extension of [".ts", ".mts", ".cts", ".js", ".mjs", ".cjs"]) {
          candidates.push(`${base}${extension}`);
        }
        for (const indexName of ["index.ts", "index.mts", "index.cts", "index.js", "index.mjs", "index.cjs"]) {
          candidates.push(path.join(base, indexName));
        }
      }
    }
    const relativeMatch = candidates.find((candidate) => ts.sys.fileExists(candidate));
    if (relativeMatch) {
      return relativeMatch;
    }
  }
  return resolveWorkspaceModulePath(projectRoot, specifier);
}

function createSourceBindings(parentBindings, sourceFile, projectRoot, program) {
  const bindings = createScopeBindings(parentBindings, sourceFile);
  const checker = program.getTypeChecker();
  for (const statement of sourceFile.statements) {
    if (!ts.isImportDeclaration(statement) || !statement.importClause) {
      continue;
    }
    const namedBindings = statement.importClause.namedBindings;
    if (!namedBindings || !ts.isNamedImports(namedBindings)) {
      continue;
    }
    const resolvedModulePath = resolveProjectModulePath(
      projectRoot,
      sourceFile.fileName,
      statement.moduleSpecifier.text,
    );
    if (resolvedModulePath == null) {
      continue;
    }
    const moduleSourceFile = program.getSourceFile(resolvedModulePath);
    if (!moduleSourceFile) {
      continue;
    }
    const moduleBindings = createScopeBindings(new Map(), moduleSourceFile);
    for (const element of namedBindings.elements) {
      const importedName = element.propertyName?.text ?? element.name.text;
      const initializer = findExportedBindingExpression(
        moduleSourceFile,
        importedName,
        moduleBindings,
      );
      if (initializer == null) {
        continue;
      }
      const staticValue = evaluateStaticValue(initializer, moduleBindings, checker);
      if (
        staticValue !== undefined ||
        ts.isFunctionDeclaration(initializer) ||
        ts.isFunctionExpression(initializer) ||
        ts.isArrowFunction(initializer)
      ) {
        bindings.set(element.name.text, initializer);
      }
    }
  }
  return bindings;
}

function combinedTemporalImports(...maps) {
  const combined = new Map();
  for (const map of maps) {
    for (const [localName, importedName] of map.entries()) {
      combined.set(localName, importedName);
    }
  }
  return combined;
}

function resolveStaticExpression(node, bindings, checker = null, seen = new Set()) {
  if (ts.isParenthesizedExpression(node)) {
    return resolveStaticExpression(node.expression, bindings, checker, seen);
  }
  if (ts.isIdentifier(node)) {
    const initializer = resolveIdentifierInitializer(node, bindings, checker, seen);
    if (initializer == null) {
      return node;
    }
    const key = ts.isVariableDeclaration(initializer.parent)
      ? declarationKey(initializer.parent)
      : `binding:${node.text}`;
    if (seen.has(key)) {
      return node;
    }
    seen.add(key);
    const resolved = resolveStaticExpression(initializer, bindings, checker, seen);
    seen.delete(key);
    return resolved;
  }
  return node;
}

function importedTemporalName(node, bindings, temporalImports, checker = null) {
  const resolved = resolveStaticExpression(node, bindings, checker);
  return ts.isIdentifier(resolved) ? temporalImports.get(resolved.text) ?? null : null;
}

function isSupportedPayloadConverterExpression(node, bindings, temporalImports, checker = null) {
  const resolved = resolveStaticExpression(node, bindings, checker);
  const importedName = importedTemporalName(resolved, bindings, temporalImports, checker);
  if (importedName != null && SUPPORTED_PAYLOAD_CONVERTER_IMPORTS.has(importedName)) {
    return true;
  }
  if (!ts.isNewExpression(resolved)) {
    return false;
  }
  const constructorName = importedTemporalName(resolved.expression, bindings, temporalImports, checker);
  return (
    constructorName != null &&
    SUPPORTED_PAYLOAD_CONVERTER_CONSTRUCTORS.has(constructorName) &&
    (resolved.arguments == null || resolved.arguments.length === 0)
  );
}

function isSupportedEmptyCodecList(node, bindings, checker = null) {
  const value = evaluateStaticValue(node, bindings, checker);
  return Array.isArray(value) && value.length === 0;
}

function analyzeSupportedDataConverter(node, bindings, temporalImports, checker = null) {
  const resolved = resolveStaticExpression(node, bindings, checker);
  const importedName = importedTemporalName(resolved, bindings, temporalImports, checker);
  if (importedName != null && SUPPORTED_DATA_CONVERTER_IMPORTS.has(importedName)) {
    return { supported: true, mode: "default_temporal" };
  }
  const awaitedCall = ts.isAwaitExpression(resolved) ? resolved.expression : resolved;
  if (
    ts.isCallExpression(awaitedCall) &&
    awaitedCall.arguments.length === 0 &&
    ts.isIdentifier(awaitedCall.expression)
  ) {
    const factory = resolveIdentifierInitializer(awaitedCall.expression, bindings, checker, new Set());
    if (
      factory &&
      (
        ts.isFunctionDeclaration(factory) ||
        ts.isFunctionExpression(factory) ||
        ts.isArrowFunction(factory)
      ) &&
      factory.parameters.length === 0
    ) {
      return { supported: true, mode: "static_data_converter_factory" };
    }
  }
  if (!ts.isObjectLiteralExpression(resolved)) {
    return { supported: false, mode: null };
  }

  for (const property of resolved.properties) {
    if ((!ts.isPropertyAssignment(property) && !ts.isShorthandPropertyAssignment(property)) || property.name == null) {
      return { supported: false, mode: null };
    }
    const propertyName =
      ts.isIdentifier(property.name) ? property.name.text :
      ts.isStringLiteral(property.name) ? property.name.text :
      null;
    if (propertyName == null) {
      return { supported: false, mode: null };
    }
    const initializer = ts.isPropertyAssignment(property) ? property.initializer : property.name;
    if (propertyName === "payloadConverter") {
      if (!isSupportedPayloadConverterExpression(initializer, bindings, temporalImports, checker)) {
        return { supported: false, mode: null };
      }
      continue;
    }
    if (propertyName === "payloadCodecs") {
      if (!isSupportedEmptyCodecList(initializer, bindings, checker)) {
        return { supported: false, mode: null };
      }
      continue;
    }
    return { supported: false, mode: null };
  }

  return { supported: true, mode: "default_temporal" };
}

function findExportedBindingExpression(sourceFile, exportName, bindings) {
  for (const statement of sourceFile.statements) {
    if (
      ts.isFunctionDeclaration(statement) &&
      statement.name?.text === exportName &&
      statement.modifiers?.some((modifier) => modifier.kind === ts.SyntaxKind.ExportKeyword)
    ) {
      return statement;
    }
  }
  for (const statement of sourceFile.statements) {
    if (!ts.isVariableStatement(statement)) {
      continue;
    }
    const isExported = statement.modifiers?.some((modifier) => modifier.kind === ts.SyntaxKind.ExportKeyword);
    for (const declaration of statement.declarationList.declarations) {
      if (!ts.isIdentifier(declaration.name) || declaration.initializer == null) {
        continue;
      }
      bindings.set(declaration.name.text, declaration.initializer);
      if (isExported && declaration.name.text === exportName) {
        return declaration.initializer;
      }
    }
  }

  for (const statement of sourceFile.statements) {
    if (!ts.isExportDeclaration(statement) || statement.exportClause == null || !ts.isNamedExports(statement.exportClause)) {
      continue;
    }
    for (const element of statement.exportClause.elements) {
      const exportedName = element.name.text;
      const localName = element.propertyName?.text ?? element.name.text;
      if (exportedName === exportName && bindings.has(localName)) {
        return bindings.get(localName);
      }
    }
  }

  return null;
}

function analyzePayloadConverterModule(program, projectRoot, sourceFile, specifier) {
  const resolvedModulePath = resolveProjectModulePath(projectRoot, sourceFile.fileName, specifier);
  if (resolvedModulePath == null) {
    return { supported: false, mode: null, resolvedModulePath: null };
  }
  const moduleSourceFile = program.getSourceFile(resolvedModulePath);
  if (!moduleSourceFile) {
    return { supported: false, mode: null, resolvedModulePath };
  }
  const moduleBindings = createScopeBindings(new Map(), moduleSourceFile);
  const { commonImports } = collectImportInfo(moduleSourceFile);
  const temporalImports = combinedTemporalImports(commonImports);
  const exportedPayloadConverter =
    findExportedBindingExpression(moduleSourceFile, "payloadConverter", moduleBindings);
  if (exportedPayloadConverter == null) {
    const checker = program.getTypeChecker();
    const moduleSymbol = checker.getSymbolAtLocation(moduleSourceFile);
    const hasNamedExport =
      moduleSymbol != null &&
      checker.getExportsOfModule(moduleSymbol).some((candidate) => candidate.getName() === "payloadConverter");
    if (!hasNamedExport) {
      return { supported: false, mode: null, resolvedModulePath };
    }
    return { supported: true, mode: "path_static_payload_converter", resolvedModulePath };
  }
  const checker = program.getTypeChecker();
  if (isSupportedPayloadConverterExpression(exportedPayloadConverter, moduleBindings, temporalImports, checker)) {
    return { supported: true, mode: "path_default_temporal", resolvedModulePath };
  }
  return { supported: true, mode: "path_static_payload_converter", resolvedModulePath };
}

function analyzeWorkerDataConverter(program, projectRoot, sourceFile, node, bindings, temporalImports) {
  const checker = program.getTypeChecker();
  const directSupport = analyzeSupportedDataConverter(node, bindings, temporalImports, checker);
  if (directSupport.supported) {
    return directSupport;
  }
  const resolved = resolveStaticExpression(node, bindings, checker);
  if (!ts.isObjectLiteralExpression(resolved)) {
    return { supported: false, mode: null, resolvedModulePath: null, payloadConverterPathProperty: null };
  }
  const payloadConverterPathProperty = findObjectProperty(resolved, "payloadConverterPath");
  if (
    payloadConverterPathProperty == null ||
    !ts.isPropertyAssignment(payloadConverterPathProperty)
  ) {
    return { supported: false, mode: null, resolvedModulePath: null, payloadConverterPathProperty: null };
  }
  const payloadConverterPath = findStaticString(payloadConverterPathProperty.initializer, bindings, checker);
  if (payloadConverterPath == null) {
    return { supported: false, mode: null, resolvedModulePath: null, payloadConverterPathProperty };
  }
  const pathSupport = analyzePayloadConverterModule(program, projectRoot, sourceFile, payloadConverterPath);
  if (!pathSupport.supported || pathSupport.resolvedModulePath == null) {
    return {
      supported: false,
      mode: null,
      resolvedModulePath: pathSupport.resolvedModulePath ?? null,
      payloadConverterPathProperty,
    };
  }
  return {
    supported: true,
    mode: pathSupport.mode,
    resolvedModulePath: pathSupport.resolvedModulePath,
    payloadConverterPathProperty,
    payload_converter_module: payloadConverterPath,
  };
}

function collectWorkflowOptionAnnotations(sourceFile, workflowImports) {
  const annotations = new Map();
  for (const statement of sourceFile.statements) {
    if (
      !ts.isExpressionStatement(statement) ||
      !ts.isCallExpression(statement.expression) ||
      !ts.isIdentifier(statement.expression.expression) ||
      workflowImports.get(statement.expression.expression.text) !== "setWorkflowOptions" ||
      statement.expression.arguments.length !== 2
    ) {
      continue;
    }
    const [optionsArg, workflowArg] = statement.expression.arguments;
    if (!ts.isObjectLiteralExpression(optionsArg) || !ts.isIdentifier(workflowArg)) {
      continue;
    }
    const versioningBehaviorProperty = findObjectProperty(optionsArg, "versioningBehavior");
    if (
      versioningBehaviorProperty == null ||
      !ts.isPropertyAssignment(versioningBehaviorProperty) ||
      !ts.isStringLiteralLike(versioningBehaviorProperty.initializer)
    ) {
      continue;
    }
    const versioningBehavior = versioningBehaviorProperty.initializer.text;
    if (!["AUTO_UPGRADE", "PINNED"].includes(versioningBehavior)) {
      continue;
    }
    annotations.set(workflowArg.text, { versioning_behavior: versioningBehavior });
  }
  return annotations;
}

function extractExportedAsyncWorkflows(projectRoot, program, sourceFile, fileUses, workflowAnnotations = new Map()) {
  const checker = program.getTypeChecker();
  const symbol = checker.getSymbolAtLocation(sourceFile);
  if (!symbol) return [];
  const exports = checker.getExportsOfModule(symbol);
  const workflows = [];
  for (const candidate of exports) {
    const declaration = candidate.valueDeclaration ?? candidate.declarations?.[0];
    if (!declaration) continue;
    if (
      ts.isFunctionDeclaration(declaration) ||
      ts.isFunctionExpression(declaration) ||
      ts.isArrowFunction(declaration)
    ) {
      if (!declaration.modifiers?.some((modifier) => modifier.kind === ts.SyntaxKind.AsyncKeyword)) {
        continue;
      }
      workflows.push({
        file: relativeProjectPath(projectRoot, sourceFile.fileName),
        export_name: candidate.getName(),
        definition_id_suggestion: definitionIdSuggestion(projectRoot, sourceFile.fileName, candidate.getName()),
        uses: [...fileUses].sort(),
        ...(workflowAnnotations.get(candidate.getName()) ?? {}),
      });
    }
  }
  return workflows;
}

function definitionIdSuggestion(projectRoot, fileName, exportName) {
  const relative = relativeProjectPath(projectRoot, fileName)
    .replace(/\.[^.]+$/, "")
    .replace(/[^a-zA-Z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "")
    .toLowerCase();
  const exportPart = exportName.replace(/[^a-zA-Z0-9]+/g, "-").toLowerCase();
  return `${relative}-${exportPart}`.replace(/-+/g, "-");
}

function dedupeFindings(findings) {
  const seen = new Set();
  return findings.filter((finding) => {
    const key = [
      finding.code,
      finding.file,
      finding.line ?? "",
      finding.column ?? "",
      finding.symbol ?? "",
      finding.message,
    ].join("|");
    if (seen.has(key)) return false;
    seen.add(key);
    return true;
  });
}

async function main() {
  const { projectRoot } = parseArgs(process.argv.slice(2));
  const supportMatrixDocument = await loadSupportMatrixDocument();
  const files = await collectProjectFiles(projectRoot);
  const program = createProgram(projectRoot, files);

  const analyzedFiles = [];
  const workflows = [];
  const workers = [];
  let findings = [];
  const supportedPayloadConverterPathNodes = new WeakSet();

  for (const sourceFile of program.getSourceFiles()) {
    if (!isProjectSourceFile(projectRoot, sourceFile)) {
      continue;
    }

    const relativeFile = relativeProjectPath(projectRoot, sourceFile.fileName);
    const { workflowImports, workerImports, clientImports, commonImports } = collectImportInfo(sourceFile);
    const workflowAnnotations = collectWorkflowOptionAnnotations(sourceFile, workflowImports);
    const temporalImportAliases =
      combinedTemporalImports(workflowImports, workerImports, clientImports, commonImports);
    const temporalImports = [];
    const fileUses = new Set();

    for (const [localName, importedName] of workflowImports) {
      temporalImports.push(`@temporalio/workflow:${importedName} as ${localName}`);
      if (WORKFLOW_SUPPORTED_IMPORTS.has(importedName)) {
        if (importedName === "proxyActivities" || importedName === "proxyLocalActivities") {
          fileUses.add("proxy_activities");
        }
        if (["defineSignal", "defineQuery", "defineUpdate"].includes(importedName)) {
          fileUses.add("signals_queries_updates");
        }
        if (importedName === "condition") fileUses.add("condition_waits");
        if (importedName === "CancellationScope") fileUses.add("cancellation_scopes");
        if (["executeChild", "startChild"].includes(importedName)) fileUses.add("child_workflows");
        if (importedName === "getExternalWorkflowHandle") fileUses.add("external_workflow_handles");
        if (importedName === "continueAsNew") fileUses.add("continue_as_new");
        if (["SearchAttributes", "upsertSearchAttributes", "workflowInfo"].includes(importedName)) {
          fileUses.add("search_attributes_memo");
        }
        if (["patched", "deprecatePatch", "setWorkflowOptions"].includes(importedName)) {
          fileUses.add("ctx_version_workflow_evolution");
          if (importedName === "setWorkflowOptions") {
            fileUses.add("worker_build_ids_and_routing");
          }
        }
        continue;
      }
      findings.push(
        createFinding(
          projectRoot,
          "hard_block",
          "unsupported_temporal_workflow_import",
          "unsupported_temporal_api",
          sourceFile,
          `Temporal workflow import ${importedName} is not currently supported by Fabrik migration`,
          "remove the import or extend the compiler support matrix before migrating this workflow",
          importedName,
        ),
      );
    }

    for (const [localName, importedName] of workerImports) {
      temporalImports.push(`@temporalio/worker:${importedName} as ${localName}`);
      if (importedName === "Worker") {
        fileUses.add("worker_bootstrap_patterns");
      }
    }
    for (const [localName, importedName] of clientImports) {
      temporalImports.push(`@temporalio/client:${importedName} as ${localName}`);
    }
    for (const [localName, importedName] of commonImports) {
      temporalImports.push(`@temporalio/common:${importedName} as ${localName}`);
      if (BLOCKED_PAYLOAD_IMPORTS.has(importedName)) {
        findings.push(
          createFinding(
            projectRoot,
            "hard_block",
            "blocked_payload_converter_import",
            "payload_data_converter_usage",
            sourceFile,
            `Temporal import ${importedName} indicates custom payload/data converter behavior`,
            "remove custom payload conversion or add a Fabrik adapter before migration",
            importedName,
          ),
        );
      }
    }

    function visit(node, scopeBindings = new Map()) {
      const currentBindings =
        ts.isSourceFile(node)
          ? createSourceBindings(scopeBindings, node, projectRoot, program)
          : ts.isBlock(node) || ts.isModuleBlock(node)
            ? createScopeBindings(scopeBindings, node)
          : scopeBindings;
      if (ts.isCallExpression(node) && ts.isIdentifier(node.expression)) {
        const importedWorkflowName = workflowImports.get(node.expression.text);
        if (importedWorkflowName === "proxyActivities" || importedWorkflowName === "proxyLocalActivities") {
          fileUses.add("proxy_activities");
          fileUses.add("activity_options_and_retries");
        } else if (["defineSignal", "defineQuery", "defineUpdate"].includes(importedWorkflowName)) {
          fileUses.add("signals_queries_updates");
        } else if (importedWorkflowName === "condition") {
          fileUses.add("condition_waits");
        } else if (["executeChild", "startChild"].includes(importedWorkflowName)) {
          fileUses.add("child_workflows");
        } else if (importedWorkflowName === "getExternalWorkflowHandle") {
          fileUses.add("external_workflow_handles");
        } else if (importedWorkflowName === "continueAsNew") {
          fileUses.add("continue_as_new");
        } else if (importedWorkflowName === "setHandler") {
          fileUses.add("signals_queries_updates");
          const handler = node.arguments[1];
          if (handler && (ts.isArrowFunction(handler) || ts.isFunctionExpression(handler))) {
            if (handler.modifiers?.some((modifier) => modifier.kind === ts.SyntaxKind.AsyncKeyword)) {
              fileUses.add("async_handlers");
            }
          }
        }
      }

      if (
        ts.isCallExpression(node) &&
        ts.isPropertyAccessExpression(node.expression) &&
        ts.isIdentifier(node.expression.expression) &&
        workerImports.get(node.expression.expression.text) === "Worker" &&
        node.expression.name.text === "create"
      ) {
        fileUses.add("worker_bootstrap_patterns");
        const checker = program.getTypeChecker();
        const firstArg = node.arguments[0]
          ? resolveStaticExpression(node.arguments[0], currentBindings, checker)
          : null;
        if (!firstArg || !ts.isObjectLiteralExpression(firstArg)) {
          findings.push(
            createFinding(
              projectRoot,
              "hard_block",
              "dynamic_worker_bootstrap",
              "worker_bootstrap_patterns",
              node,
              "Worker.create must receive a static object literal for migration packaging",
              "rewrite the worker bootstrap to a static Worker.create({ ... }) call",
              "Worker.create",
            ),
          );
        } else {
          const taskQueueProperty = findObjectProperty(firstArg, "taskQueue");
          const workflowsPathProperty = findObjectProperty(firstArg, "workflowsPath");
          const activitiesProperty = findObjectProperty(firstArg, "activities");
          const buildIdProperty = findObjectProperty(firstArg, "buildId");
          const dataConverterProperty = findObjectProperty(firstArg, "dataConverter");

          const taskQueue =
            taskQueueProperty && ts.isPropertyAssignment(taskQueueProperty)
              ? findStaticString(taskQueueProperty.initializer, currentBindings, checker)
              : taskQueueProperty && ts.isShorthandPropertyAssignment(taskQueueProperty)
                ? findStaticString(taskQueueProperty.name, currentBindings, checker)
                : null;
          const workflowsPath =
            workflowsPathProperty && ts.isPropertyAssignment(workflowsPathProperty)
              ? findStaticString(workflowsPathProperty.initializer, currentBindings, checker)
              : workflowsPathProperty && ts.isShorthandPropertyAssignment(workflowsPathProperty)
                ? findStaticString(workflowsPathProperty.name, currentBindings, checker)
                : null;
          if (workflowsPathProperty && workflowsPath == null) {
            findings.push(
              createFinding(
                projectRoot,
                "hard_block",
                "dynamic_workflows_path",
                "worker_bootstrap_patterns",
                workflowsPathProperty,
                "Worker workflowsPath must be a static string literal for packaging",
                "use a static workflowsPath string in Worker.create",
                "workflowsPath",
              ),
            );
          }

          let dataConverterMode = null;
          let payloadConverterModule = null;
          if (dataConverterProperty) {
            fileUses.add("payload_data_converter_usage");
            const initializer =
              ts.isPropertyAssignment(dataConverterProperty)
                ? dataConverterProperty.initializer
                : dataConverterProperty.name;
            const converterSupport = analyzeWorkerDataConverter(
              program,
              projectRoot,
              sourceFile,
              initializer,
              currentBindings,
              temporalImportAliases,
            );
            if (!converterSupport.supported) {
              findings.push(
                createFinding(
                  projectRoot,
                  "hard_block",
                  "blocked_data_converter_usage",
                  "payload_data_converter_usage",
                  dataConverterProperty,
                  "Worker dataConverter must stay within Fabrik's supported static adapter subset",
                  "use defaultDataConverter, a static object literal with defaultPayloadConverter and an empty payloadCodecs list, or a static payloadConverterPath module",
                  "dataConverter",
                ),
              );
            } else {
              dataConverterMode = converterSupport.mode;
              payloadConverterModule = converterSupport.payload_converter_module ?? null;
              if (converterSupport.payloadConverterPathProperty) {
                supportedPayloadConverterPathNodes.add(
                  converterSupport.payloadConverterPathProperty,
                );
              }
            }
          }

          for (const [propertyName, remediation] of WORKER_BLOCKING_PROPERTIES.entries()) {
            const property = findObjectProperty(firstArg, propertyName);
            if (property) {
              if (
                propertyName === "interceptors" &&
                supportsStaticWorkflowInterceptors(property, currentBindings, checker)
              ) {
                fileUses.add("interceptors_middleware");
                continue;
              }
              findings.push(
                createFinding(
                  projectRoot,
                  "hard_block",
                  `blocked_worker_${propertyName}`,
                  propertyName.includes("interceptor")
                    ? "interceptors_middleware"
                    : "payload_data_converter_usage",
                  property,
                  `Worker option ${propertyName} is not migration-ready yet`,
                  remediation,
                  propertyName,
                ),
              );
            }
          }

          let activitiesReference = null;
          let activityModule = null;
          let activityFactoryExport = null;
          let activityFactoryArgsJs = [];
          let activityRuntimeHelper = false;
          if (activitiesProperty) {
            const activityRegistration = analyzeActivitiesRegistration(
              sourceFile,
              activitiesProperty,
              currentBindings,
              checker,
            );
            if (activityRegistration.supported) {
              activitiesReference = activityRegistration.activities_reference;
              activityModule = activityRegistration.activity_module;
              activityFactoryExport = activityRegistration.activity_factory_export;
              activityFactoryArgsJs = activityRegistration.activity_factory_args_js;
              activityRuntimeHelper = activityRegistration.activity_runtime_helper ?? false;
            } else {
              findings.push(
                createFinding(
                  projectRoot,
                  "hard_block",
                  "dynamic_activities_registration",
                  "worker_bootstrap_patterns",
                  activitiesProperty,
                  "Worker activities registration must be a static identifier or object literal",
                  "rewrite activities: ... to a static imported object or inline object literal",
                  "activities",
                ),
              );
            }
          }

          workers.push({
            file: relativeFile,
            task_queue: taskQueue,
            build_id:
              buildIdProperty && ts.isPropertyAssignment(buildIdProperty)
                ? findStaticString(buildIdProperty.initializer, currentBindings, checker)
                : null,
            workflows_path: workflowsPath,
            activities_reference: activitiesReference,
            activity_module: activityModule,
            activity_factory_export: activityFactoryExport,
            activity_factory_args_js: activityFactoryArgsJs,
            activity_runtime_helper: activityRuntimeHelper,
            data_converter_mode: dataConverterMode,
            payload_converter_module: payloadConverterModule,
            bootstrap_pattern: "worker_create_static",
            worker_index: workers.filter((worker) => worker.file === relativeFile).length + 1,
            uses: [...fileUses].sort(),
          });
        }
      }

      if (ts.isPropertyAssignment(node) && node.name != null) {
        const propertyName =
          ts.isIdentifier(node.name) ? node.name.text : ts.isStringLiteral(node.name) ? node.name.text : null;
        if (propertyName === "searchAttributes" || propertyName === "memo") {
          fileUses.add("search_attributes_memo");
          const supported =
            propertyName === "memo"
              ? isSupportedMemoObject(node.initializer, currentBindings, program.getTypeChecker())
              : isSupportedSearchAttributesObject(node.initializer, currentBindings, program.getTypeChecker());
          if (!supported) {
            findings.push(
              createFinding(
                projectRoot,
                "hard_block",
                `blocked_${propertyName}_usage`,
                "visibility_search_usage",
                node,
                `Temporal ${propertyName} usage is outside Fabrik's alpha visibility/search subset`,
                propertyName === "memo"
                  ? "use a static top-level memo object with primitive literal values only"
                  : "use static top-level search attributes with primitive or Date values, or arrays of primitive or Date values",
                propertyName,
              ),
            );
          }
        }
        if (propertyName === "payloadConverterPath") {
          fileUses.add("payload_data_converter_usage");
          ts.forEachChild(node, (child) => visit(child, currentBindings));
          return;
        }
        if (propertyName === "payloadCodec" || propertyName === "codecServer") {
          if (supportedPayloadConverterPathNodes.has(node)) {
            ts.forEachChild(node, (child) => visit(child, currentBindings));
            return;
          }
          fileUses.add("payload_data_converter_usage");
          findings.push(
            createFinding(
              projectRoot,
              "hard_block",
              `blocked_${propertyName}_usage`,
              "payload_data_converter_usage",
              node,
              `Temporal ${propertyName} usage is not supported by the migration pipeline`,
              "remove custom payload conversion/codec behavior or add an adapter before migration",
              propertyName,
            ),
          );
        }
      }

      if (ts.isPropertyAccessExpression(node) && ts.isIdentifier(node.expression)) {
        if (workflowImports.get(node.expression.text) === "CancellationScope") {
          fileUses.add("cancellation_scopes");
        }
      }

      ts.forEachChild(node, (child) => visit(child, currentBindings));
    }

    visit(sourceFile, new Map());

    const exportedWorkflows = workflowImports.size > 0
      ? extractExportedAsyncWorkflows(projectRoot, program, sourceFile, fileUses, workflowAnnotations)
      : [];
    workflows.push(...exportedWorkflows);

    analyzedFiles.push({
      path: relativeFile,
      temporal_imports: temporalImports.sort(),
      exported_workflows: exportedWorkflows.map((workflow) => workflow.export_name),
      uses: [...fileUses].sort(),
    });
  }

  findings = dedupeFindings(findings);
  const hasNonTestWorker = workers.some((worker) => !isTestSupportFile(worker.file));
  if (hasNonTestWorker) {
    findings = findings.map((finding) => {
      if (
        finding.severity === "hard_block" &&
        finding.feature === "worker_bootstrap_patterns" &&
        isTestSupportFile(finding.file)
      ) {
        return {
          ...finding,
          severity: "warning",
          remediation: "ignore test-only worker bootstrap findings while migrating the production worker entrypoint",
        };
      }
      return finding;
    });
  }
  const hardBlockCount = findings.filter((finding) => finding.severity === "hard_block").length;
  const warningCount = findings.filter((finding) => finding.severity === "warning").length;
  const infoCount = findings.filter((finding) => finding.severity === "info").length;

  process.stdout.write(
    `${JSON.stringify(
      {
        schema_version: 1,
        project_root: projectRoot,
        support_matrix_meta: {
          schema_version: supportMatrixDocument.schema_version,
          milestone_scope: supportMatrixDocument.milestone_scope,
          goal: supportMatrixDocument.goal,
          trusted_confidence_floor: supportMatrixDocument.trusted_confidence_floor,
          upgrade_confidence_floor: supportMatrixDocument.upgrade_confidence_floor,
          promotion_requirements: supportMatrixDocument.promotion_requirements,
        },
        support_matrix: supportMatrixDocument.features,
        files: analyzedFiles.sort((left, right) => left.path.localeCompare(right.path)),
        workflows: workflows.sort((left, right) =>
          `${left.file}:${left.export_name}`.localeCompare(`${right.file}:${right.export_name}`),
        ),
        workers: workers.sort((left, right) => left.file.localeCompare(right.file)),
        findings,
        summary: {
          workflow_count: workflows.length,
          worker_count: workers.length,
          hard_block_count: hardBlockCount,
          warning_count: warningCount,
          info_count: infoCount,
        },
      },
      null,
      2,
    )}\n`,
  );
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
