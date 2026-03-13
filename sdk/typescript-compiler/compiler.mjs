import crypto from "node:crypto";
import fsSync from "node:fs";
import fs from "node:fs/promises";
import path from "node:path";
import process from "node:process";
import ts from "typescript";

const MAX_BULK_CHUNK_SIZE = 1024;

function usage() {
  console.error(
    "usage: node sdk/typescript-compiler/compiler.mjs --entry <file> --export <name> --definition-id <id> --version <n> [--out <file>]",
  );
  process.exit(1);
}

function parseArgs(argv) {
  const args = {};
  for (let i = 0; i < argv.length; i += 1) {
    const key = argv[i];
    const value = argv[i + 1];
    if (!key.startsWith("--") || value == null) {
      usage();
    }
    args[key.slice(2)] = value;
    i += 1;
  }
  if (!args.entry || !args.export || !args["definition-id"] || !args.version) {
    usage();
  }
  return {
    entry: path.resolve(args.entry),
    exportName: args.export,
    definitionId: args["definition-id"],
    version: Number(args.version),
    out: args.out ? path.resolve(args.out) : null,
  };
}

class CompilerError extends Error {
  constructor(message, node = null) {
    const location = node ? formatNodeLocation(node) : null;
    super(location ? `${location.file}:${location.line}:${location.column}: ${message}` : message);
    this.name = "CompilerError";
    this.file = location?.file ?? null;
    this.line = location?.line ?? null;
    this.column = location?.column ?? null;
  }
}

function compilerError(message, node = null) {
  return new CompilerError(message, node);
}

function formatNodeLocation(node) {
  const sourceFile = node.getSourceFile?.() ?? node.parent?.getSourceFile?.();
  if (!sourceFile) {
    return {
      file: "<generated>",
      line: 1,
      column: 1,
    };
  }
  const start = typeof node.getStart === "function" ? node.getStart(sourceFile) : (node.pos ?? 0);
  const { line, character } = sourceFile.getLineAndCharacterOfPosition(start);
  return {
    file: path.relative(process.cwd(), sourceFile.fileName),
    line: line + 1,
    column: character + 1,
  };
}

function sourceLocation(node) {
  const location = formatNodeLocation(node);
  return {
    file: location.file,
    line: location.line,
    column: location.column,
  };
}

function shortHash(value) {
  return crypto.createHash("sha1").update(value).digest("hex").slice(0, 10);
}

function stableNodeKey(node) {
  let text = null;
  try {
    const sourceFile = node.getSourceFile?.();
    if (sourceFile) {
      text = node.getText(sourceFile);
    }
  } catch {}
  if (typeof text !== "string" || text.length === 0) {
    text = `${node.pos ?? "na"}:${node.end ?? "na"}`;
  }
  const parts = [node.kind, text];
  let current = node;
  let depth = 0;
  while (current && depth < 3) {
    current = current.parent;
    if (current) {
      parts.push(current.kind);
    }
    depth += 1;
  }
  return shortHash(parts.join("|"));
}

function rootIdentifierName(expression) {
  if (
    ts.isParenthesizedExpression(expression) ||
    ts.isAsExpression(expression) ||
    ts.isTypeAssertionExpression(expression) ||
    ts.isNonNullExpression(expression) ||
    ts.isSatisfiesExpression?.(expression)
  ) {
    return rootIdentifierName(expression.expression);
  }
  if (ts.isIdentifier(expression)) {
    return expression.text;
  }
  if (
    ts.isPropertyAccessExpression(expression) ||
    ts.isElementAccessExpression(expression) ||
    ts.isPropertyAccessChain?.(expression) ||
    ts.isElementAccessChain?.(expression)
  ) {
    return rootIdentifierName(expression.expression);
  }
  return null;
}

function isCtxReceiver(expression) {
  return ts.isIdentifier(expression) && expression.text === "ctx";
}

function resolveTemporalActivityBinding(expression, bindings = currentTemporalActivityBindings) {
  if (ts.isIdentifier(expression)) {
    const activity = bindings.direct.get(expression.text);
    return activity ? { activityType: activity.activityType, options: activity.options } : null;
  }
  if (
    ts.isPropertyAccessExpression(expression) &&
    ts.isIdentifier(expression.expression) &&
    bindings.objects.has(expression.expression.text)
  ) {
    return {
      activityType: expression.name.text,
      options: bindings.objectOptions.get(expression.expression.text) ?? {},
    };
  }
  return null;
}

function jsonValueToExpression(value) {
  if (value == null) {
    return { kind: "literal", value: null };
  }
  if (typeof value === "string" || typeof value === "number" || typeof value === "boolean") {
    return { kind: "literal", value };
  }
  if (Array.isArray(value)) {
    return { kind: "array", items: value.map(jsonValueToExpression) };
  }
  const fields = {};
  for (const [key, entry] of Object.entries(value)) {
    fields[key] = jsonValueToExpression(entry);
  }
  return { kind: "object", fields };
}

function compileTemporalActivityDescriptor(activityType, options, args) {
  const fields = {
    __kind: { kind: "literal", value: "activity_descriptor" },
    activity_type: { kind: "literal", value: activityType },
    input: compileCallArgumentsAsInput(args),
  };
  if (options.task_queue) {
    fields.task_queue = options.task_queue;
  }
  if (options.retry) {
    fields.retry = jsonValueToExpression(options.retry);
  }
  if (options.schedule_to_start_timeout_ms != null) {
    fields.schedule_to_start_timeout_ms = {
      kind: "literal",
      value: options.schedule_to_start_timeout_ms,
    };
  }
  if (options.schedule_to_close_timeout_ms != null) {
    fields.schedule_to_close_timeout_ms = {
      kind: "literal",
      value: options.schedule_to_close_timeout_ms,
    };
  }
  if (options.start_to_close_timeout_ms != null) {
    fields.start_to_close_timeout_ms = {
      kind: "literal",
      value: options.start_to_close_timeout_ms,
    };
  }
  if (options.heartbeat_timeout_ms != null) {
    fields.heartbeat_timeout_ms = {
      kind: "literal",
      value: options.heartbeat_timeout_ms,
    };
  }
  return { kind: "object", fields };
}

function compileDeferredActivityThunkExpression(expression) {
  if (!ts.isArrowFunction(expression) && !ts.isFunctionExpression(expression)) {
    return null;
  }
  if (expression.modifiers?.some((modifier) => modifier.kind === ts.SyntaxKind.AsyncKeyword)) {
    throw compilerError(`deferred workflow thunks must not be async`, expression);
  }
  if (expression.parameters.length !== 0) {
    throw compilerError(`deferred workflow thunks must not declare parameters`, expression);
  }
  const bodyExpression = functionBodyExpression(expression.body);
  if (!bodyExpression || !ts.isCallExpression(bodyExpression)) {
    throw compilerError(
      `deferred workflow thunks must return a supported Temporal activity call`,
      expression.body,
    );
  }
  const temporalActivity = resolveTemporalActivityBinding(bodyExpression.expression);
  if (!temporalActivity) {
    throw compilerError(
      `deferred workflow thunks currently support only Temporal activity calls`,
      bodyExpression,
    );
  }
  return compileTemporalActivityDescriptor(
    temporalActivity.activityType,
    temporalActivity.options,
    bodyExpression.arguments,
  );
}

function assertAllowedRootIdentifier(expression) {
  const root = rootIdentifierName(expression);
  if (root && ["Date", "Math", "process", "globalThis", "window", "document"].includes(root)) {
    throw compilerError(`unsupported global access ${root}`, expression);
  }
}

function createProgram(entry) {
  const configPath = ts.findConfigFile(process.cwd(), ts.sys.fileExists, "tsconfig.json");
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
  return ts.createProgram([entry], options);
}

function collectTemporalWorkflowApi(sourceFile) {
  const api = {
    activityFailure: new Set(),
    proxyActivities: new Set(),
    sleep: new Set(),
    continueAsNew: new Set(),
    condition: new Set(),
    cancellationScope: new Set(),
    isCancellation: new Set(),
    getExternalWorkflowHandle: new Set(),
    executeChild: new Set(),
    startChild: new Set(),
    defineQuery: new Set(),
    defineUpdate: new Set(),
    defineSignal: new Set(),
    setHandler: new Set(),
    setWorkflowOptions: new Set(),
    upsertSearchAttributes: new Set(),
    workflowInfo: new Set(),
    inWorkflowContext: new Set(),
    log: new Set(),
    uuid4: new Set(),
    patched: new Set(),
    deprecatePatch: new Set(),
    applicationFailure: new Set(),
    parentClosePolicy: new Set(),
    activityCancellationType: new Set(),
    namespaceImports: new Set(),
  };
  for (const statement of sourceFile.statements) {
    if (!ts.isImportDeclaration(statement) || !statement.importClause) {
      continue;
    }
    if (statement.moduleSpecifier.text !== "@temporalio/workflow") {
      continue;
    }
    const namedBindings = statement.importClause.namedBindings;
    if (!namedBindings) {
      continue;
    }
    if (ts.isNamespaceImport(namedBindings)) {
      api.namespaceImports.add(namedBindings.name.text);
      continue;
    }
    for (const element of namedBindings.elements) {
      const importedName = element.propertyName?.text ?? element.name.text;
      const localName = element.name.text;
      if (importedName === "ActivityFailure") api.activityFailure.add(localName);
      if (importedName === "proxyActivities") api.proxyActivities.add(localName);
      if (importedName === "sleep") api.sleep.add(localName);
      if (importedName === "continueAsNew") api.continueAsNew.add(localName);
      if (importedName === "condition") api.condition.add(localName);
      if (importedName === "CancellationScope") api.cancellationScope.add(localName);
      if (importedName === "isCancellation") api.isCancellation.add(localName);
      if (importedName === "getExternalWorkflowHandle") api.getExternalWorkflowHandle.add(localName);
      if (importedName === "executeChild") api.executeChild.add(localName);
      if (importedName === "startChild") api.startChild.add(localName);
      if (importedName === "defineQuery") api.defineQuery.add(localName);
      if (importedName === "defineUpdate") api.defineUpdate.add(localName);
      if (importedName === "defineSignal") api.defineSignal.add(localName);
      if (importedName === "setHandler") api.setHandler.add(localName);
      if (importedName === "setWorkflowOptions") api.setWorkflowOptions.add(localName);
      if (importedName === "upsertSearchAttributes") api.upsertSearchAttributes.add(localName);
      if (importedName === "workflowInfo") api.workflowInfo.add(localName);
      if (importedName === "inWorkflowContext") api.inWorkflowContext.add(localName);
      if (importedName === "log") api.log.add(localName);
      if (importedName === "uuid4") api.uuid4.add(localName);
      if (importedName === "patched") api.patched.add(localName);
      if (importedName === "deprecatePatch") api.deprecatePatch.add(localName);
      if (importedName === "ApplicationFailure") api.applicationFailure.add(localName);
      if (importedName === "ParentClosePolicy") api.parentClosePolicy.add(localName);
      if (importedName === "ActivityCancellationType") api.activityCancellationType.add(localName);
    }
  }
  return api;
}

let currentTemporalApi = collectTemporalWorkflowApi(ts.createSourceFile(
  "__empty__.ts",
  "",
  ts.ScriptTarget.ES2022,
  false,
  ts.ScriptKind.TS,
));
let currentTemporalActivityBindings = {
  direct: new Map(),
  objects: new Set(),
  objectOptions: new Map(),
};
let currentStaticTopLevelInitializers = new Map();

function unwrapStaticReferenceExpression(expression) {
  if (
    ts.isParenthesizedExpression(expression) ||
    ts.isAsExpression(expression) ||
    ts.isTypeAssertionExpression(expression) ||
    ts.isNonNullExpression(expression) ||
    ts.isSatisfiesExpression?.(expression)
  ) {
    return unwrapStaticReferenceExpression(expression.expression);
  }
  if (ts.isIdentifier(expression) && currentStaticTopLevelInitializers.has(expression.text)) {
    return unwrapStaticReferenceExpression(currentStaticTopLevelInitializers.get(expression.text));
  }
  return expression;
}

function collectStaticTopLevelInitializers(sourceFile) {
  const bindings = new Map();
  for (const statement of sourceFile.statements) {
    if (!ts.isVariableStatement(statement)) {
      continue;
    }
    for (const declaration of statement.declarationList.declarations) {
      if (
        ts.isIdentifier(declaration.name) &&
        declaration.initializer &&
        isStaticTopLevelInitializer(declaration.initializer)
      ) {
        bindings.set(declaration.name.text, declaration.initializer);
      }
    }
  }
  return bindings;
}

function temporalImportedReferenceMatches(expression, aliases, importedName, temporalApi = currentTemporalApi) {
  if (ts.isIdentifier(expression)) {
    return aliases.has(expression.text);
  }
  return (
    ts.isPropertyAccessExpression(expression) &&
    ts.isIdentifier(expression.expression) &&
    temporalApi.namespaceImports.has(expression.expression.text) &&
    expression.name.text === importedName
  );
}

function temporalImportedCallMatches(callExpression, aliases, importedName, temporalApi = currentTemporalApi) {
  return temporalImportedReferenceMatches(callExpression.expression, aliases, importedName, temporalApi);
}

function temporalLogReceiverMatches(expression, temporalApi = currentTemporalApi) {
  return temporalImportedReferenceMatches(expression, temporalApi.log, "log", temporalApi);
}

function temporalLogCallMatches(callExpression, temporalApi = currentTemporalApi) {
  return (
    ts.isPropertyAccessExpression(callExpression.expression) &&
    ["trace", "debug", "info", "warn", "error"].includes(callExpression.expression.name.text) &&
    temporalLogReceiverMatches(callExpression.expression.expression, temporalApi)
  );
}

function temporalEnumReferenceMatches(expression, aliases, importedName, temporalApi = currentTemporalApi) {
  return temporalImportedReferenceMatches(expression, aliases, importedName, temporalApi);
}

function isTemporalProxyDeclaration(declaration, temporalApi) {
  const initializer = declaration.initializer
    ? unwrapStaticReferenceExpression(declaration.initializer)
    : null;
  if (!initializer || !ts.isCallExpression(initializer)) {
    return false;
  }
  if (!temporalImportedCallMatches(initializer, temporalApi.proxyActivities, "proxyActivities", temporalApi)) {
    return false;
  }
  if (ts.isIdentifier(declaration.name)) {
    return true;
  }
  if (!ts.isObjectBindingPattern(declaration.name)) {
    return false;
  }
  return declaration.name.elements.every(
    (element) => !element.propertyName && ts.isIdentifier(element.name) && !element.initializer,
  );
}

function temporalDefinitionKind(declaration, temporalApi) {
  if (!declaration.initializer || !ts.isCallExpression(declaration.initializer)) {
    return null;
  }
  if (temporalImportedCallMatches(declaration.initializer, temporalApi.defineQuery, "defineQuery", temporalApi)) {
    return "query";
  }
  if (temporalImportedCallMatches(declaration.initializer, temporalApi.defineUpdate, "defineUpdate", temporalApi)) {
    return "update";
  }
  if (temporalImportedCallMatches(declaration.initializer, temporalApi.defineSignal, "defineSignal", temporalApi)) {
    return "signal";
  }
  return null;
}

function parseTemporalDurationMs(expression, label) {
  if (ts.isNumericLiteral(expression)) {
    return Number(expression.text);
  }
  if (ts.isStringLiteral(expression) || ts.isNoSubstitutionTemplateLiteral(expression)) {
    const match = /^(\d+)\s*(ms|millisecond|milliseconds|s|sec|secs|second|seconds|m|min|mins|minute|minutes|h|hr|hrs|hour|hours)$/.exec(
      expression.text.trim().toLowerCase(),
    );
    if (!match) {
      throw compilerError(`${label} must be a static duration like "500ms", "30s", "5m", or "1h"`, expression);
    }
    const value = Number(match[1]);
    const unit = match[2];
    const multiplier =
      ["ms", "millisecond", "milliseconds"].includes(unit) ? 1
      : ["s", "sec", "secs", "second", "seconds"].includes(unit) ? 1_000
      : ["m", "min", "mins", "minute", "minutes"].includes(unit) ? 60_000
      : 3_600_000;
    return value * multiplier;
  }
  throw compilerError(`${label} must be a numeric literal or static duration string`, expression);
}

function parseTemporalDurationRef(expression, label) {
  if (ts.isNumericLiteral(expression)) {
    return `${expression.text}ms`;
  }
  if (ts.isStringLiteral(expression) || ts.isNoSubstitutionTemplateLiteral(expression)) {
    const text = expression.text.trim();
    const match = /^(\d+)\s*(ms|millisecond|milliseconds|s|sec|secs|second|seconds|m|min|mins|minute|minutes|h|hr|hrs|hour|hours)$/i.exec(
      text,
    );
    if (match) {
      const value = match[1];
      const unit = match[2].toLowerCase();
      if (["ms", "millisecond", "milliseconds"].includes(unit)) return `${value}ms`;
      if (["s", "sec", "secs", "second", "seconds"].includes(unit)) return `${value}s`;
      if (["m", "min", "mins", "minute", "minutes"].includes(unit)) return `${value}m`;
      return `${value}h`;
    }
    return text;
  }
  throw compilerError(`${label} must be a static duration literal`, expression);
}

function compileTemporalTimer(expression, label) {
  if (
    ts.isNumericLiteral(expression) ||
    ts.isStringLiteral(expression) ||
    ts.isNoSubstitutionTemplateLiteral(expression)
  ) {
    return { timer_ref: parseTemporalDurationRef(expression, label) };
  }
  return { timer_expr: compileExpression(expression) };
}

function compileTemporalPatchedExpression(expression, temporalApi = currentTemporalApi) {
  if (!temporalImportedCallMatches(expression, temporalApi.patched, "patched", temporalApi)) {
    return null;
  }
  if (expression.arguments.length !== 1) {
    throw compilerError(`patched() requires exactly one change id`, expression);
  }
  return {
    kind: "binary",
    op: "greater_than",
    left: {
      kind: "version",
      change_id: literalString(expression.arguments[0], "patched changeId"),
      min_supported: 0,
      max_supported: 1,
    },
    right: { kind: "literal", value: 0 },
  };
}

function literalTemporalEnumMember(expression, label, aliases, importedName) {
  if (
    ts.isPropertyAccessExpression(expression) &&
    temporalEnumReferenceMatches(expression.expression, aliases, importedName)
  ) {
    return expression.name.text;
  }
  return literalString(expression, label);
}

function compileApplicationFailureExpression(messageExpression, options = {}, originNode = null) {
  const fields = {
    type: { kind: "literal", value: "ApplicationFailure" },
    message: messageExpression ? compileExpression(messageExpression) : { kind: "literal", value: "" },
    nonRetryable: { kind: "literal", value: options.nonRetryable ?? false },
  };
  if (options.category !== undefined) {
    fields.category = compileExpression(options.category);
  }
  if (options.details !== undefined) {
    fields.details = compileExpression(options.details);
  }
  return { kind: "object", fields };
}

function compileApplicationFailureCreateExpression(expression) {
  const config = expression.arguments[0];
  if (!config || !ts.isObjectLiteralExpression(config)) {
    throw compilerError(`ApplicationFailure.create requires a static object argument`, expression);
  }
  const fields = {
    type: { kind: "literal", value: "ApplicationFailure" },
    message: { kind: "literal", value: "" },
    nonRetryable: { kind: "literal", value: false },
  };
  for (const property of config.properties) {
    if (!ts.isPropertyAssignment(property) && !ts.isShorthandPropertyAssignment(property)) {
      throw compilerError(`unsupported ApplicationFailure.create property ${property.getText()}`, property);
    }
    const key = property.name.getText().replaceAll(/^["']|["']$/g, "");
    const initializer = ts.isPropertyAssignment(property) ? property.initializer : property.name;
    if (key === "message") {
      fields.message = compileExpression(initializer);
      continue;
    }
    if (key === "nonRetryable") {
      fields.nonRetryable = compileExpression(initializer);
      continue;
    }
    if (key === "details" || key === "category") {
      fields[key] = compileExpression(initializer);
      continue;
    }
    throw compilerError(`unsupported ApplicationFailure.create property ${key}`, property);
  }
  return { kind: "object", fields };
}

function parseTemporalRetryOptions(expression) {
  expression = unwrapStaticReferenceExpression(expression);
  if (!ts.isObjectLiteralExpression(expression)) {
    throw compilerError(`proxyActivities retry must be a static object`, expression);
  }
  const retry = {};
  for (const property of expression.properties) {
    if (!ts.isPropertyAssignment(property)) {
      throw compilerError(`unsupported proxyActivities retry option ${property.getText()}`, property);
    }
    const key = property.name.getText().replaceAll(/^["']|["']$/g, "");
    if (key === "maximumAttempts") {
      if (!ts.isNumericLiteral(property.initializer)) {
        throw compilerError(`proxyActivities retry.maximumAttempts must be a numeric literal`, property.initializer);
      }
      retry.max_attempts = Number(property.initializer.text);
      continue;
    }
    if (key === "initialInterval") {
      retry.delay = literalString(property.initializer, "proxyActivities retry.initialInterval");
      continue;
    }
    if (key === "maximumInterval") {
      retry.maximum_interval = literalString(
        property.initializer,
        "proxyActivities retry.maximumInterval",
      );
      continue;
    }
    if (key === "backoffCoefficient") {
      if (!ts.isNumericLiteral(property.initializer)) {
        throw compilerError(
          `proxyActivities retry.backoffCoefficient must be a numeric literal`,
          property.initializer,
        );
      }
      retry.backoff_coefficient_millis = Math.round(Number(property.initializer.text) * 1000);
      continue;
    }
    if (key === "nonRetryableErrorTypes") {
      if (
        !ts.isArrayLiteralExpression(property.initializer) ||
        property.initializer.elements.some(
          (element) =>
            !ts.isStringLiteral(element) && !ts.isNoSubstitutionTemplateLiteral(element),
        )
      ) {
        throw compilerError(
          `proxyActivities retry.nonRetryableErrorTypes must be a static string array`,
          property.initializer,
        );
      }
      retry.non_retryable_error_types = property.initializer.elements.map((element) => element.text);
      continue;
    }
    throw compilerError(`unsupported proxyActivities retry option ${key}`, property);
  }
  return Object.keys(retry).length > 0 ? retry : null;
}

function parseTemporalProxyActivityOptions(callExpression) {
  const optionsExpression = callExpression.arguments[0]
    ? unwrapStaticReferenceExpression(callExpression.arguments[0])
    : null;
  if (!optionsExpression) {
    return {};
  }
  if (!ts.isObjectLiteralExpression(optionsExpression)) {
    throw compilerError(`proxyActivities options must be a static object`, optionsExpression);
  }
  const options = {};
  for (const property of optionsExpression.properties) {
    if (!ts.isPropertyAssignment(property)) {
      throw compilerError(`unsupported proxyActivities option ${property.getText()}`, property);
    }
    const key = property.name.getText().replaceAll(/^["']|["']$/g, "");
    if (key === "taskQueue") {
      options.task_queue = compileExpression(property.initializer);
      continue;
    }
    if (key === "scheduleToStartTimeout") {
      options.schedule_to_start_timeout_ms = parseTemporalDurationMs(
        property.initializer,
        "proxyActivities scheduleToStartTimeout",
      );
      continue;
    }
    if (key === "scheduleToCloseTimeout") {
      options.schedule_to_close_timeout_ms = parseTemporalDurationMs(
        property.initializer,
        "proxyActivities scheduleToCloseTimeout",
      );
      continue;
    }
    if (key === "startToCloseTimeout") {
      options.start_to_close_timeout_ms = parseTemporalDurationMs(
        property.initializer,
        "proxyActivities startToCloseTimeout",
      );
      continue;
    }
    if (key === "heartbeatTimeout") {
      options.heartbeat_timeout_ms = parseTemporalDurationMs(
        property.initializer,
        "proxyActivities heartbeatTimeout",
      );
      continue;
    }
    if (key === "retry") {
      const retry = parseTemporalRetryOptions(property.initializer);
      if (retry) options.retry = retry;
      continue;
    }
    if (key === "cancellationType") {
      literalTemporalEnumMember(
        property.initializer,
        "proxyActivities cancellationType",
        currentTemporalApi.activityCancellationType,
        "ActivityCancellationType",
      );
      continue;
    }
    throw compilerError(`unsupported proxyActivities option ${key}`, property);
  }
  return options;
}

function isTemporalDefinitionDeclaration(declaration, temporalApi) {
  return ts.isIdentifier(declaration.name) && temporalDefinitionKind(declaration, temporalApi) != null;
}

function isStaticTopLevelInitializer(expression) {
  if (
    ts.isParenthesizedExpression(expression) ||
    ts.isAsExpression(expression) ||
    ts.isTypeAssertionExpression(expression) ||
    ts.isNonNullExpression(expression) ||
    ts.isSatisfiesExpression?.(expression)
  ) {
    return isStaticTopLevelInitializer(expression.expression);
  }
  if (
    ts.isStringLiteral(expression) ||
    ts.isNoSubstitutionTemplateLiteral(expression) ||
    ts.isNumericLiteral(expression) ||
    expression.kind === ts.SyntaxKind.TrueKeyword ||
    expression.kind === ts.SyntaxKind.FalseKeyword ||
    expression.kind === ts.SyntaxKind.NullKeyword
  ) {
    return true;
  }
  if (ts.isArrayLiteralExpression(expression)) {
    return expression.elements.every((element) => isStaticTopLevelInitializer(element));
  }
  if (ts.isObjectLiteralExpression(expression)) {
    return expression.properties.every((property) => {
      if (ts.isPropertyAssignment(property)) {
        return isStaticTopLevelInitializer(property.initializer);
      }
      if (ts.isMethodDeclaration(property)) {
        return true;
      }
      return false;
    });
  }
  if (ts.isArrowFunction(expression) || ts.isFunctionExpression(expression)) {
    return true;
  }
  return false;
}

function isSupportedWorkflowContextGuard(statement, temporalApi) {
  if (!ts.isIfStatement(statement)) {
    return false;
  }
  const condition = statement.expression;
  if (
    !ts.isPrefixUnaryExpression(condition) ||
    condition.operator !== ts.SyntaxKind.ExclamationToken ||
    !ts.isCallExpression(condition.operand) ||
    !temporalImportedCallMatches(
      condition.operand,
      temporalApi.inWorkflowContext,
      "inWorkflowContext",
      temporalApi,
    ) ||
    condition.operand.arguments.length !== 0
  ) {
    return false;
  }
  return true;
}

function isSupportedSetWorkflowOptionsStatement(statement, temporalApi) {
  if (
    !ts.isExpressionStatement(statement) ||
    !ts.isCallExpression(statement.expression) ||
    !temporalImportedCallMatches(statement.expression, temporalApi.setWorkflowOptions, "setWorkflowOptions", temporalApi)
  ) {
    return false;
  }
  if (statement.expression.arguments.length !== 2) {
    return false;
  }
  const [optionsArg, workflowArg] = statement.expression.arguments;
  if (!ts.isObjectLiteralExpression(optionsArg) || !ts.isIdentifier(workflowArg)) {
    return false;
  }
  const versioningBehaviorProperty = optionsArg.properties.find(
    (property) =>
      ts.isPropertyAssignment(property) &&
      (
        (ts.isIdentifier(property.name) && property.name.text === "versioningBehavior") ||
        (ts.isStringLiteral(property.name) && property.name.text === "versioningBehavior")
      ),
  );
  if (
    versioningBehaviorProperty == null ||
    !ts.isStringLiteralLike(versioningBehaviorProperty.initializer)
  ) {
    return false;
  }
  return ["AUTO_UPGRADE", "PINNED"].includes(versioningBehaviorProperty.initializer.text);
}

function assertNoTopLevelSideEffects(sourceFile) {
  const temporalApi = collectTemporalWorkflowApi(sourceFile);
  for (const statement of sourceFile.statements) {
    if (
      ts.isImportDeclaration(statement) ||
      ts.isImportEqualsDeclaration(statement) ||
      ts.isExportDeclaration(statement) ||
      ts.isClassDeclaration(statement) ||
      ts.isFunctionDeclaration(statement) ||
      ts.isTypeAliasDeclaration(statement) ||
      ts.isInterfaceDeclaration(statement) ||
      ts.isEnumDeclaration(statement)
    ) {
      continue;
    }
    if (ts.isVariableStatement(statement)) {
      if (
        statement.declarationList.declarations.every((declaration) =>
          isTemporalProxyDeclaration(declaration, temporalApi) ||
          isTemporalDefinitionDeclaration(declaration, temporalApi),
        )
      ) {
        continue;
      }
      if (
        statement.declarationList.declarations.every((declaration) =>
          ts.isIdentifier(declaration.name) &&
          declaration.initializer &&
          isStaticTopLevelInitializer(declaration.initializer),
        )
      ) {
        continue;
      }
    }
    if (isSupportedSetWorkflowOptionsStatement(statement, temporalApi)) {
      continue;
    }
    if (isSupportedWorkflowContextGuard(statement, temporalApi)) {
      continue;
    }
    if (!ts.isEmptyStatement(statement)) {
      throw compilerError(
        `top-level side effects are not allowed in workflow modules (${sourceFile.fileName})`,
        statement,
      );
    }
  }
}

function getResolvedSources(program) {
  return program
    .getSourceFiles()
    .filter((sourceFile) => !sourceFile.isDeclarationFile)
    .filter((sourceFile) => !sourceFile.fileName.includes(`${path.sep}node_modules${path.sep}`));
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

function findWorkspaceRoot(startFile) {
  let current = path.dirname(startFile);
  let nearestPackageRoot = null;
  while (true) {
    const packageJsonPath = path.join(current, "package.json");
    if (ts.sys.fileExists(packageJsonPath)) {
      nearestPackageRoot ??= current;
      try {
        const manifest = JSON.parse(fsSync.readFileSync(packageJsonPath, "utf8"));
        if (manifest.workspaces) {
          return current;
        }
      } catch {
        // Ignore malformed manifests and continue walking upward.
      }
    }
    const parent = path.dirname(current);
    if (parent === current) {
      return nearestPackageRoot ?? path.dirname(startFile);
    }
    current = parent;
  }
}

function findWorkspacePackages(workspaceRoot) {
  const packages = [];
  const stack = [workspaceRoot];
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
        // Ignore malformed workspace manifests during import resolution.
      }
    }
  }
  return packages;
}

function resolveWorkspaceImportPath(fromFileName, moduleName) {
  const workspaceRoot = findWorkspaceRoot(fromFileName);
  const packages = findWorkspacePackages(workspaceRoot);
  const matched = packages
    .filter(({ manifest }) => moduleName === manifest.name || moduleName.startsWith(`${manifest.name}/`))
    .sort((left, right) => right.manifest.name.length - left.manifest.name.length)[0];
  if (!matched) {
    return null;
  }
  const { dir, manifest } = matched;
  const subpath =
    moduleName === manifest.name ? "" : moduleName.slice(manifest.name.length + 1);
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

function findExportedFunction(program, exportName) {
  const checker = program.getTypeChecker();
  for (const sourceFile of getResolvedSources(program)) {
    const symbol = checker.getSymbolAtLocation(sourceFile);
    if (!symbol) continue;
    const exports = checker.getExportsOfModule(symbol);
    const workflow = exports.find((candidate) => candidate.getName() === exportName);
    if (!workflow) continue;
    const declaration = workflow.valueDeclaration ?? workflow.declarations?.[0];
    if (!declaration) continue;
    if (
      ts.isFunctionDeclaration(declaration) ||
      ts.isFunctionExpression(declaration) ||
      ts.isArrowFunction(declaration)
    ) {
      assertNoTopLevelSideEffects(declaration.getSourceFile());
      if (!declaration.modifiers?.some((modifier) => modifier.kind === ts.SyntaxKind.AsyncKeyword)) {
        throw compilerError(`workflow export ${exportName} must be async`, declaration);
      }
      return declaration;
    }
  }
  throw compilerError(`exported async workflow ${exportName} not found`);
}

function buildHelperRegistry(program, workflowDeclaration) {
  const helpers = new Map();
  const checker = program.getTypeChecker();
  const importedHelpers = collectImportedHelpers(program, workflowDeclaration.getSourceFile());

  const visit = (node) => {
    if (ts.isCallExpression(node) && ts.isIdentifier(node.expression)) {
      const importedDeclaration = importedHelpers.get(node.expression.text);
      if (importedDeclaration) {
        helpers.set(
          node.expression.text,
          compileHelperFunction(node.expression.text, importedDeclaration),
        );
      }
      const symbol = checker.getSymbolAtLocation(node.expression);
      if (symbol) {
        const resolvedSymbol =
          symbol.flags & ts.SymbolFlags.Alias ? checker.getAliasedSymbol(symbol) : symbol;
        const declaration = resolvedSymbol.valueDeclaration ?? resolvedSymbol.declarations?.[0];
        if (
          hasCompilableHelperBody(declaration) &&
          !isAsyncHelperDeclaration(declaration) &&
          declaration !== workflowDeclaration
        ) {
          helpers.set(node.expression.text, compileHelperFunction(node.expression.text, declaration));
        }
      }
    }
    ts.forEachChild(node, visit);
  };

  visit(workflowDeclaration.body);
  return Object.fromEntries(helpers.entries());
}

function collectImportedHelpers(program, sourceFile) {
  const helpers = new Map();
  for (const statement of sourceFile.statements) {
    if (!ts.isImportDeclaration(statement) || !statement.importClause) {
      continue;
    }
    if (statement.importClause.isTypeOnly) {
      continue;
    }
    const moduleName = statement.moduleSpecifier.text;
    if (moduleName === "@temporalio/workflow") {
      continue;
    }
    let resolved = ts.resolveModuleName(
      moduleName,
      sourceFile.fileName,
      program.getCompilerOptions(),
      ts.sys,
    ).resolvedModule;
    if (!resolved) {
      const workspaceResolved = resolveWorkspaceImportPath(sourceFile.fileName, moduleName);
      if (workspaceResolved) {
        resolved = { resolvedFileName: workspaceResolved };
      }
    }
    if (!resolved) {
      continue;
    }
    const importedSource = loadSourceFile(program, resolved.resolvedFileName);
    if (!importedSource) {
      continue;
    }
    for (const binding of extractImportedBindings(statement.importClause)) {
      const declaration = findExportedFunctionDeclaration(importedSource, binding.exportName);
      if (hasCompilableHelperBody(declaration)) {
        helpers.set(binding.localName, declaration);
      }
    }
  }
  return helpers;
}

function injectTemporalBuiltinHelpers(helpers, temporalApi) {
  const merged = new Map(Object.entries(helpers));
  for (const localName of temporalApi.isCancellation) {
    merged.set(localName, {
      params: ["error"],
      statements: [],
      body: {
        kind: "call",
        callee: "__temporal_is_cancellation",
        args: [{ kind: "identifier", name: "error" }],
      },
    });
  }
  return Object.fromEntries(merged.entries());
}

function extractImportedBindings(importClause) {
  const bindings = [];
  if (importClause.isTypeOnly) {
    return bindings;
  }
  if (importClause.name) {
    bindings.push({ localName: importClause.name.text, exportName: "default" });
  }
  if (
    importClause.namedBindings &&
    ts.isNamedImports(importClause.namedBindings)
  ) {
    for (const element of importClause.namedBindings.elements) {
      if (element.isTypeOnly) {
        continue;
      }
      bindings.push({
        localName: element.name.text,
        exportName: element.propertyName?.text ?? element.name.text,
      });
    }
  }
  return bindings;
}

function findExportedFunctionDeclaration(sourceFile, exportName) {
  for (const statement of sourceFile.statements) {
    if (!ts.isFunctionDeclaration(statement) || !statement.name) {
      continue;
    }
    if (
      exportName === "default" &&
      statement.modifiers?.some((modifier) => modifier.kind === ts.SyntaxKind.DefaultKeyword)
    ) {
      return statement;
    }
    if (
      statement.name.text === exportName &&
      statement.modifiers?.some((modifier) => modifier.kind === ts.SyntaxKind.ExportKeyword)
    ) {
      return statement;
    }
  }
  return null;
}

function loadSourceFile(program, fileName) {
  const existing = program.getSourceFile(fileName);
  if (existing) {
    return existing;
  }
  const contents = ts.sys.readFile(fileName);
  if (contents == null) {
    return null;
  }
  return ts.createSourceFile(
    fileName,
    contents,
    program.getCompilerOptions().target ?? ts.ScriptTarget.ES2022,
    true,
  );
}

function hasCompilableHelperBody(declaration) {
  return (
    !!declaration &&
    (ts.isFunctionDeclaration(declaration) ||
      ts.isFunctionExpression(declaration) ||
      ts.isArrowFunction(declaration)) &&
    declaration.body != null
  );
}

function isAsyncHelperDeclaration(declaration) {
  return Boolean(
    declaration?.modifiers?.some((modifier) => modifier.kind === ts.SyntaxKind.AsyncKeyword),
  );
}

function compileHelperFunction(name, declaration) {
  if (declaration.modifiers?.some((modifier) => modifier.kind === ts.SyntaxKind.AsyncKeyword)) {
    throw compilerError(`helper ${name} must not be async`, declaration);
  }
  const params = declaration.parameters.map((parameter) => parameter.name.getText());
  if (ts.isBlock(declaration.body)) {
    const last = declaration.body.statements.at(-1);
    if (!last || !ts.isReturnStatement(last) || !last.expression) {
      throw compilerError(`helper ${name} must end with a return expression`, declaration.body);
    }
    return {
      params,
      statements: compileHelperStatements(declaration.body.statements.slice(0, -1)),
      body: compileExpression(last.expression),
    };
  }
  return { params, statements: [], body: compileExpression(declaration.body) };
}

function compileHelperStatements(statements) {
  const compiled = [];
  for (const statement of statements) {
    if (ts.isEmptyStatement(statement)) {
      continue;
    }
    if (ts.isVariableStatement(statement)) {
      for (const declaration of statement.declarationList.declarations) {
        if (!ts.isIdentifier(declaration.name) || !declaration.initializer) {
          throw compilerError(`unsupported helper declaration: ${statement.getText()}`, declaration);
        }
        compiled.push({
          type: "assign",
          target: declaration.name.text,
          expr: compileExpression(declaration.initializer),
        });
      }
      continue;
    }
    if (ts.isExpressionStatement(statement)) {
      if (
        ts.isBinaryExpression(statement.expression) &&
        (statement.expression.operatorToken.kind === ts.SyntaxKind.EqualsToken ||
          statement.expression.operatorToken.kind === ts.SyntaxKind.PlusEqualsToken)
      ) {
        compiled.push(
          compileHelperAssignmentStatement(
            statement,
            statement.expression.left,
            statement.expression.operatorToken.kind,
            statement.expression.right,
          ),
        );
        continue;
      }
      throw compilerError(`unsupported helper expression statement: ${statement.getText()}`, statement);
    }
    if (ts.isForStatement(statement)) {
      compiled.push(compileHelperForStatement(statement));
      continue;
    }
    if (ts.isIfStatement(statement)) {
      compiled.push(compileHelperIfStatement(statement));
      continue;
    }
    throw compilerError(`unsupported helper statement: ${statement.getText()}`, statement);
  }
  return compiled;
}

function compileHelperIfStatement(statement) {
  const thenStatements = ts.isBlock(statement.thenStatement)
    ? statement.thenStatement.statements
    : [statement.thenStatement];
  const elseStatements = statement.elseStatement
    ? ts.isBlock(statement.elseStatement)
      ? statement.elseStatement.statements
      : [statement.elseStatement]
    : [];
  return {
    type: "if",
    condition: compileExpression(statement.expression),
    then_body: compileHelperStatements(thenStatements),
    else_body: compileHelperStatements(elseStatements),
  };
}

function compileHelperAssignmentStatement(statement, left, operatorKind, right) {
  if (ts.isIdentifier(left)) {
    if (operatorKind === ts.SyntaxKind.EqualsToken) {
      return { type: "assign", target: left.text, expr: compileExpression(right) };
    }
    return {
      type: "assign",
      target: left.text,
      expr: {
        kind: "binary",
        op: "add",
        left: { kind: "identifier", name: left.text },
        right: compileExpression(right),
      },
    };
  }
  if (ts.isElementAccessExpression(left) && ts.isIdentifier(left.expression)) {
    const expr =
      operatorKind === ts.SyntaxKind.EqualsToken
        ? compileExpression(right)
        : {
            kind: "binary",
            op: "add",
            left: {
              kind: "index",
              object: { kind: "identifier", name: left.expression.text },
              index: compileExpression(left.argumentExpression),
            },
            right: compileExpression(right),
          };
    return {
      type: "assign_index",
      target: left.expression.text,
      index: compileExpression(left.argumentExpression),
      expr,
    };
  }
  throw compilerError(`unsupported helper assignment target: ${statement.getText()}`, statement);
}

function compileHelperForStatement(statement) {
  if (!statement.initializer || !ts.isVariableDeclarationList(statement.initializer)) {
    throw compilerError(`helper for-loops require a variable initializer`, statement);
  }
  if (statement.initializer.declarations.length !== 1) {
    throw compilerError(`helper for-loops require exactly one initializer`, statement.initializer);
  }
  const declaration = statement.initializer.declarations[0];
  if (!ts.isIdentifier(declaration.name) || !declaration.initializer) {
    throw compilerError(`helper for-loop initializer must bind one identifier`, declaration);
  }
  if (
    !statement.condition ||
    !ts.isBinaryExpression(statement.condition) ||
    statement.condition.operatorToken.kind !== ts.SyntaxKind.LessThanToken ||
    !ts.isIdentifier(statement.condition.left) ||
    statement.condition.left.text !== declaration.name.text
  ) {
    throw compilerError(`helper for-loops must use ${declaration.name.text} < end`, statement);
  }
  if (
    !statement.incrementor ||
    !(
      (ts.isPostfixUnaryExpression(statement.incrementor) || ts.isPrefixUnaryExpression(statement.incrementor)) &&
      ts.isIdentifier(statement.incrementor.operand) &&
      statement.incrementor.operand.text === declaration.name.text &&
      statement.incrementor.operator === ts.SyntaxKind.PlusPlusToken
    )
  ) {
    throw compilerError(`helper for-loops must increment ${declaration.name.text} with ++`, statement);
  }
  const bodyStatements = ts.isBlock(statement.statement)
    ? statement.statement.statements
    : [statement.statement];
  return {
    type: "for_range",
    index_var: declaration.name.text,
    start: compileExpression(declaration.initializer),
    end: compileExpression(statement.condition.right),
    body: compileHelperStatements(bodyStatements),
  };
}

function compileDeleteAssignment(statement, expression) {
  if (!ts.isElementAccessExpression(expression.expression)) {
    throw compilerError(`unsupported delete target: ${statement.getText()}`, statement);
  }
  if (!ts.isIdentifier(expression.expression.expression)) {
    throw compilerError(`unsupported delete target: ${statement.getText()}`, statement);
  }
  return {
    target: expression.expression.expression.text,
    expr: {
      kind: "call",
      callee: "__builtin_object_omit",
      args: [
        { kind: "identifier", name: expression.expression.expression.text },
        compileExpression(expression.expression.argumentExpression),
      ],
    },
  };
}

function collectBoundIdentifiers(name, names) {
  if (ts.isIdentifier(name)) {
    names.add(name.text);
    return;
  }
  if (ts.isObjectBindingPattern(name) || ts.isArrayBindingPattern(name)) {
    for (const element of name.elements) {
      if (ts.isBindingElement(element)) {
        collectBoundIdentifiers(element.name, names);
      }
    }
  }
}

function collectAsyncHelperBindingNames(declaration) {
  const names = new Set();
  for (const parameter of declaration.parameters ?? []) {
    collectBoundIdentifiers(parameter.name, names);
  }
  const body = declaration.body;
  if (!body || !ts.isBlock(body)) {
    return names;
  }
  const visit = (node) => {
    if (ts.isVariableDeclaration(node)) {
      collectBoundIdentifiers(node.name, names);
    } else if (ts.isCatchClause(node) && node.variableDeclaration) {
      collectBoundIdentifiers(node.variableDeclaration.name, names);
    } else if (ts.isParameter(node)) {
      collectBoundIdentifiers(node.name, names);
    }
    ts.forEachChild(node, visit);
  };
  visit(body);
  return names;
}

function shouldRenameIdentifierNode(node) {
  const parent = node.parent;
  if (!parent) {
    return true;
  }
  if (ts.isPropertyAccessExpression(parent) && parent.name === node) {
    return false;
  }
  if (
    (ts.isPropertyAssignment(parent) || ts.isMethodDeclaration(parent) || ts.isPropertySignature(parent)) &&
    parent.name === node
  ) {
    return false;
  }
  if (ts.isShorthandPropertyAssignment(parent) && parent.name === node) {
    return false;
  }
  if (ts.isImportSpecifier(parent) || ts.isExportSpecifier(parent)) {
    return false;
  }
  if (ts.isLabeledStatement(parent) && parent.label === node) {
    return false;
  }
  return true;
}

function ensureAwaitedReturnExpression(expression) {
  if (!expression) {
    return expression;
  }
  return (
    ts.isAwaitExpression(expression) ||
    !ts.isCallExpression(expression)
  )
    ? expression
    : ts.setTextRange(ts.factory.createAwaitExpression(expression), expression);
}

function extractWrappedAwaitExpression(expression) {
  if (ts.isAwaitExpression(expression)) {
    return {
      awaitExpression: expression,
      rebuild: (replacement) => replacement,
    };
  }
  if (ts.isParenthesizedExpression(expression)) {
    const inner = extractWrappedAwaitExpression(expression.expression);
    if (!inner) {
      return null;
    }
    return {
      awaitExpression: inner.awaitExpression,
      rebuild: (replacement) =>
        ts.setTextRange(
          ts.factory.updateParenthesizedExpression(expression, inner.rebuild(replacement)),
          expression,
        ),
    };
  }
  if (
    ts.isAsExpression(expression) ||
    ts.isTypeAssertionExpression(expression) ||
    ts.isNonNullExpression(expression) ||
    ts.isSatisfiesExpression?.(expression)
  ) {
    return extractWrappedAwaitExpression(expression.expression);
  }
  if (ts.isPrefixUnaryExpression(expression)) {
    const inner = extractWrappedAwaitExpression(expression.operand);
    if (!inner) {
      return null;
    }
    return {
      awaitExpression: inner.awaitExpression,
      rebuild: (replacement) =>
        ts.setTextRange(
          ts.factory.updatePrefixUnaryExpression(expression, inner.rebuild(replacement)),
          expression,
        ),
    };
  }
  return null;
}

function prepareAsyncHelperStatements(declaration, renames) {
  const sourceStatements =
    declaration.body && ts.isBlock(declaration.body)
      ? declaration.body.statements
      : [
          ts.setTextRange(
            ts.factory.createReturnStatement(
              ensureAwaitedReturnExpression(declaration.body),
            ),
            declaration.body ?? declaration,
          ),
        ];
  const transformer = (context) => {
    const visit = (node) => {
      if (ts.isReturnStatement(node) && node.expression) {
        return ts.visitEachChild(
          ts.setTextRange(
            ts.factory.createReturnStatement(ensureAwaitedReturnExpression(node.expression)),
            node,
          ),
          visit,
          context,
        );
      }
      if (ts.isIdentifier(node) && renames.has(node.text) && shouldRenameIdentifierNode(node)) {
        return ts.setTextRange(ts.factory.createIdentifier(renames.get(node.text)), node);
      }
      return ts.visitEachChild(node, visit, context);
    };
    return (root) => ts.visitNode(root, visit);
  };
  const result = ts.transform(sourceStatements, [transformer]);
  try {
    return result.transformed;
  } finally {
    result.dispose();
  }
}

function compileAssignmentAction(statement, left, operatorKind, right) {
  if (ts.isIdentifier(left)) {
    if (operatorKind === ts.SyntaxKind.EqualsToken) {
      return {
        target: left.text,
        expr: compileExpression(right),
      };
    }
    return {
      target: left.text,
      expr: {
        kind: "binary",
        op: "add",
        left: { kind: "identifier", name: left.text },
        right: compileExpression(right),
      },
    };
  }
  if (
    ts.isPropertyAccessExpression(left) &&
    ts.isIdentifier(left.expression)
  ) {
    const expr =
      operatorKind === ts.SyntaxKind.EqualsToken
        ? compileExpression(right)
        : {
            kind: "binary",
            op: "add",
            left: {
              kind: "member",
              object: { kind: "identifier", name: left.expression.text },
              property: left.name.text,
            },
            right: compileExpression(right),
          };
    return {
      target: left.expression.text,
      expr: {
        kind: "call",
        callee: "__builtin_object_set",
        args: [
          { kind: "identifier", name: left.expression.text },
          { kind: "literal", value: left.name.text },
          expr,
        ],
      },
    };
  }
  if (
    ts.isElementAccessExpression(left) &&
    ts.isIdentifier(left.expression)
  ) {
    const expr =
      operatorKind === ts.SyntaxKind.EqualsToken
        ? compileExpression(right)
        : {
            kind: "binary",
            op: "add",
            left: {
              kind: "index",
              object: { kind: "identifier", name: left.expression.text },
              index: compileExpression(left.argumentExpression),
            },
            right: compileExpression(right),
          };
    return {
      target: left.expression.text,
      expr: {
        kind: "call",
        callee: "__builtin_object_set",
        args: [
          { kind: "identifier", name: left.expression.text },
          compileExpression(left.argumentExpression),
          expr,
        ],
      },
    };
  }
  throw compilerError(`unsupported assignment target: ${statement.getText()}`, statement);
}

function compileMutatingCollectionAction(callExpression) {
  if (!ts.isPropertyAccessExpression(callExpression.expression)) {
    return null;
  }
  const receiver = callExpression.expression.expression;
  const method = callExpression.expression.name.text;
  if (ts.isIdentifier(receiver) && method === "set" && callExpression.arguments.length === 2) {
    return {
      target: receiver.text,
      expr: {
        kind: "call",
        callee: "__builtin_map_set",
        args: [
          { kind: "identifier", name: receiver.text },
          compileExpression(callExpression.arguments[0]),
          compileExpression(callExpression.arguments[1]),
        ],
      },
    };
  }
  if (ts.isIdentifier(receiver) && method === "setDate" && callExpression.arguments.length === 1) {
    return {
      target: receiver.text,
      expr: {
        kind: "call",
        callee: "__builtin_date_set_date",
        args: [
          { kind: "identifier", name: receiver.text },
          compileExpression(callExpression.arguments[0]),
        ],
      },
    };
  }
  if (
    ts.isIdentifier(receiver) &&
    method === "setHours" &&
    callExpression.arguments.length >= 1 &&
    callExpression.arguments.length <= 4
  ) {
    return {
      target: receiver.text,
      expr: {
        kind: "call",
        callee: "__builtin_date_set_hours",
        args: [
          { kind: "identifier", name: receiver.text },
          ...callExpression.arguments.map(compileExpression),
        ],
      },
    };
  }
  return null;
}

function compileMutatingCollectionExpression(callExpression) {
  if (!ts.isPropertyAccessExpression(callExpression.expression)) {
    return null;
  }
  const receiver = callExpression.expression.expression;
  const method = callExpression.expression.name.text;
  if (!ts.isIdentifier(receiver) || method !== "set" || callExpression.arguments.length !== 2) {
    return null;
  }
  return {
    kind: "call",
    callee: "__builtin_binding_map_set_and_null",
    args: [
      { kind: "literal", value: receiver.text },
      compileExpression(callExpression.arguments[0]),
      compileExpression(callExpression.arguments[1]),
    ],
  };
}

function blockFromInlineHandlerBody(body) {
  if (ts.isBlock(body)) {
    if (
      body.statements.length === 1 &&
      ts.isReturnStatement(body.statements[0]) &&
      body.statements[0].expression
    ) {
      const expression = body.statements[0].expression;
      if (
        ts.isVoidExpression(expression) &&
        ts.isCallExpression(expression.expression) &&
        compileMutatingCollectionAction(expression.expression)
      ) {
        return ts.factory.createBlock([ts.factory.createExpressionStatement(expression.expression)], true);
      }
      if (ts.isCallExpression(expression) && compileMutatingCollectionAction(expression)) {
        return ts.factory.createBlock([ts.factory.createExpressionStatement(expression)], true);
      }
    }
    return body;
  }
  if (
    ts.isVoidExpression(body) &&
    ts.isCallExpression(body.expression) &&
    compileMutatingCollectionAction(body.expression)
  ) {
    return ts.factory.createBlock([ts.factory.createExpressionStatement(body.expression)], true);
  }
  if (ts.isCallExpression(body) && compileMutatingCollectionAction(body)) {
    return ts.factory.createBlock([ts.factory.createExpressionStatement(body)], true);
  }
  return ts.factory.createBlock([ts.factory.createReturnStatement(body)], true);
}

function compileWorkflowParam(parameter) {
  if (!ts.isIdentifier(parameter.name)) {
    throw compilerError(`workflow parameters must be plain identifiers`, parameter.name);
  }
  return {
    name: parameter.name.text,
    rest: Boolean(parameter.dotDotDotToken) || undefined,
    default: parameter.initializer ? compileExpression(parameter.initializer) : undefined,
  };
}

function functionBodyExpression(body) {
  if (!ts.isBlock(body)) {
    return body;
  }
  if (
    body.statements.length === 1 &&
    ts.isReturnStatement(body.statements[0]) &&
    body.statements[0].expression
  ) {
    return body.statements[0].expression;
  }
  return null;
}

function compileArrayMethodHandler(method, handler) {
  if (!ts.isArrowFunction(handler) && !ts.isFunctionExpression(handler)) {
    throw compilerError(`${method} requires an inline function handler`, handler);
  }
  if (handler.parameters.length !== 1 || !ts.isIdentifier(handler.parameters[0].name)) {
    throw compilerError(`${method} handlers must declare exactly one identifier parameter`, handler);
  }
  const expression = functionBodyExpression(handler.body);
  if (!expression) {
    throw compilerError(`${method} handlers must be a single return expression`, handler.body);
  }
  return {
    itemName: handler.parameters[0].name.text,
    expr: compileExpression(expression),
  };
}

function compileArrayReduceHandler(handler) {
  if (!ts.isArrowFunction(handler) && !ts.isFunctionExpression(handler)) {
    throw compilerError(`reduce requires an inline function handler`, handler);
  }
  if (
    handler.parameters.length !== 2 ||
    !ts.isIdentifier(handler.parameters[0].name) ||
    !ts.isIdentifier(handler.parameters[1].name)
  ) {
    throw compilerError(`reduce handlers must declare accumulator and item identifier parameters`, handler);
  }
  const expression = functionBodyExpression(handler.body);
  if (!expression) {
    throw compilerError(`reduce handlers must be a single return expression`, handler.body);
  }
  return {
    accumulatorName: handler.parameters[0].name.text,
    itemName: handler.parameters[1].name.text,
    expr: compileExpression(expression),
  };
}

function compileArrayPushAction(statement, callExpression) {
  if (
    !ts.isPropertyAccessExpression(callExpression.expression) ||
    callExpression.expression.name.text !== "push" ||
    !ts.isIdentifier(callExpression.expression.expression) ||
    callExpression.arguments.length !== 1
  ) {
    return null;
  }
  const arrayName = callExpression.expression.expression.text;
  return {
    target: arrayName,
    expr: {
      kind: "call",
      callee: "__builtin_array_append",
      args: [
        { kind: "identifier", name: arrayName },
        compileExpression(callExpression.arguments[0]),
      ],
    },
  };
}

function compileArrayUnshiftAction(statement, callExpression) {
  if (
    !ts.isPropertyAccessExpression(callExpression.expression) ||
    callExpression.expression.name.text !== "unshift" ||
    !ts.isIdentifier(callExpression.expression.expression) ||
    callExpression.arguments.length !== 1
  ) {
    return null;
  }
  const arrayName = callExpression.expression.expression.text;
  return {
    target: arrayName,
    expr: {
      kind: "call",
      callee: "__builtin_array_prepend",
      args: [
        { kind: "identifier", name: arrayName },
        compileExpression(callExpression.arguments[0]),
      ],
    },
  };
}

function resolveArrayShiftCall(callExpression) {
  if (
    !ts.isPropertyAccessExpression(callExpression.expression) ||
    callExpression.expression.name.text !== "shift" ||
    !ts.isIdentifier(callExpression.expression.expression) ||
    callExpression.arguments.length !== 0
  ) {
    return null;
  }
  return { arrayName: callExpression.expression.expression.text };
}

function compileSignalHandlerActions(handler) {
  if (!ts.isArrowFunction(handler) && !ts.isFunctionExpression(handler)) {
    throw compilerError(`Temporal signal setHandler requires an inline function handler`, handler);
  }
  if (handler.modifiers?.some((modifier) => modifier.kind === ts.SyntaxKind.AsyncKeyword)) {
    throw compilerError(`Temporal signal handlers must not be async`, handler);
  }
  const argName =
    handler.parameters[0] && ts.isIdentifier(handler.parameters[0].name)
      ? handler.parameters[0].name.text
      : null;
  const statements = ts.isBlock(handler.body)
    ? handler.body.statements
    : [ts.factory.createExpressionStatement(handler.body)];
  const actions = [];
  for (const statement of statements) {
    if (ts.isEmptyStatement(statement)) {
      continue;
    }
    if (
      ts.isExpressionStatement(statement) &&
      ts.isBinaryExpression(statement.expression) &&
      (
        statement.expression.operatorToken.kind === ts.SyntaxKind.EqualsToken ||
        statement.expression.operatorToken.kind === ts.SyntaxKind.PlusEqualsToken
      )
    ) {
      actions.push(
        compileAssignmentAction(
          statement,
          statement.expression.left,
          statement.expression.operatorToken.kind,
          statement.expression.right,
        ),
      );
      continue;
    }
    if (
      ts.isExpressionStatement(statement) &&
      ts.isDeleteExpression(statement.expression)
    ) {
      actions.push(compileDeleteAssignment(statement, statement.expression));
      continue;
    }
    if (
      ts.isExpressionStatement(statement) &&
      ts.isCallExpression(statement.expression)
    ) {
      const pushAction = compileArrayPushAction(statement, statement.expression);
      if (pushAction) {
        actions.push(pushAction);
        continue;
      }
      const unshiftAction = compileArrayUnshiftAction(statement, statement.expression);
      if (unshiftAction) {
        actions.push(unshiftAction);
        continue;
      }
    }
    if (
      ts.isExpressionStatement(statement) &&
      (ts.isPostfixUnaryExpression(statement.expression) || ts.isPrefixUnaryExpression(statement.expression)) &&
      ts.isIdentifier(statement.expression.operand) &&
      (
        statement.expression.operator === ts.SyntaxKind.PlusPlusToken ||
        statement.expression.operator === ts.SyntaxKind.MinusMinusToken
      )
    ) {
      const op =
        statement.expression.operator === ts.SyntaxKind.PlusPlusToken ? "add" : "subtract";
      actions.push({
        target: statement.expression.operand.text,
        expr: {
          kind: "binary",
          op,
          left: { kind: "identifier", name: statement.expression.operand.text },
          right: { kind: "literal", value: 1 },
        },
      });
      continue;
    }
    throw compilerError(
      `Temporal signal handlers currently support only simple identifier assignments`,
      statement,
    );
  }
  return { argName, actions };
}

function tryCompileSignalHandlerActions(handler) {
  try {
    return compileSignalHandlerActions(handler);
  } catch (error) {
    if (error instanceof CompilerError) {
      return null;
    }
    throw error;
  }
}

function compileExpression(expression) {
  while (
    ts.isParenthesizedExpression(expression) ||
    ts.isAsExpression(expression) ||
    ts.isTypeAssertionExpression(expression) ||
    ts.isNonNullExpression(expression) ||
    ts.isSatisfiesExpression?.(expression)
  ) {
    expression = expression.expression;
  }
  assertAllowedRootIdentifier(expression);
  if (ts.isStringLiteral(expression) || ts.isNoSubstitutionTemplateLiteral(expression)) {
    return { kind: "literal", value: expression.text };
  }
  if (ts.isTemplateExpression(expression)) {
    let compiled = { kind: "literal", value: expression.head.text };
    for (const span of expression.templateSpans) {
      compiled = {
        kind: "binary",
        op: "add",
        left: compiled,
        right: compileExpression(span.expression),
      };
      if (span.literal.text.length > 0) {
        compiled = {
          kind: "binary",
          op: "add",
          left: compiled,
          right: { kind: "literal", value: span.literal.text },
        };
      }
    }
    return compiled;
  }
  if (ts.isNumericLiteral(expression)) {
    return { kind: "literal", value: Number(expression.text) };
  }
  if (expression.kind === ts.SyntaxKind.TrueKeyword) {
    return { kind: "literal", value: true };
  }
  if (expression.kind === ts.SyntaxKind.FalseKeyword) {
    return { kind: "literal", value: false };
  }
  if (expression.kind === ts.SyntaxKind.NullKeyword) {
    return { kind: "literal", value: null };
  }
  if (ts.isIdentifier(expression)) {
    return { kind: "identifier", name: expression.text };
  }
  if (ts.isPropertyAccessExpression(expression) || ts.isPropertyAccessChain?.(expression)) {
    return {
      kind: "member",
      object: compileExpression(expression.expression),
      property: expression.name.text,
    };
  }
  if (ts.isElementAccessExpression(expression) || ts.isElementAccessChain?.(expression)) {
    return {
      kind: "index",
      object: compileExpression(expression.expression),
      index: compileExpression(expression.argumentExpression),
    };
  }
  if (ts.isArrayLiteralExpression(expression)) {
    return { kind: "array", items: expression.elements.map(compileExpression) };
  }
  if (ts.isObjectLiteralExpression(expression)) {
    const fields = {};
    let merged = null;
    const flushFields = () => {
      if (Object.keys(fields).length === 0) {
        return;
      }
      const objectExpr = { kind: "object", fields: { ...fields } };
      for (const key of Object.keys(fields)) {
        delete fields[key];
      }
      merged = merged == null
        ? objectExpr
        : { kind: "object_merge", left: merged, right: objectExpr };
    };
    for (const property of expression.properties) {
      if (ts.isSpreadAssignment(property)) {
        flushFields();
        const spreadExpr = compileExpression(property.expression);
        merged = merged == null
          ? { kind: "object_merge", left: { kind: "object", fields: {} }, right: spreadExpr }
          : { kind: "object_merge", left: merged, right: spreadExpr };
        continue;
      }
      if (ts.isShorthandPropertyAssignment(property)) {
        fields[property.name.text] = { kind: "identifier", name: property.name.text };
        continue;
      }
      if (!ts.isPropertyAssignment(property)) {
        throw compilerError(`unsupported object literal property: ${property.getText()}`, property);
      }
      const key = property.name.getText().replaceAll(/^["']|["']$/g, "");
      fields[key] = compileExpression(property.initializer);
    }
    flushFields();
    return merged ?? { kind: "object", fields: {} };
  }
  if (ts.isParenthesizedExpression(expression)) {
    return compileExpression(expression.expression);
  }
  if (ts.isVoidExpression(expression)) {
    if (ts.isCallExpression(expression.expression)) {
      const mutatingCollectionExpression = compileMutatingCollectionExpression(expression.expression);
      if (mutatingCollectionExpression) {
        return mutatingCollectionExpression;
      }
    }
  }
  if (ts.isPrefixUnaryExpression(expression)) {
    if (expression.operator === ts.SyntaxKind.ExclamationToken) {
      return { kind: "unary", op: "not", expr: compileExpression(expression.operand) };
    }
    if (expression.operator === ts.SyntaxKind.MinusToken) {
      return { kind: "unary", op: "negate", expr: compileExpression(expression.operand) };
    }
  }
  if (ts.isBinaryExpression(expression)) {
    if (
      expression.operatorToken.kind === ts.SyntaxKind.InstanceOfKeyword &&
      temporalImportedReferenceMatches(
        expression.right,
        currentTemporalApi.activityFailure,
        "ActivityFailure",
      )
    ) {
      return {
        kind: "call",
        callee: "__temporal_is_activity_failure",
        args: [compileExpression(expression.left)],
      };
    }
    if (
      expression.operatorToken.kind === ts.SyntaxKind.InstanceOfKeyword &&
      temporalImportedReferenceMatches(
        expression.right,
        currentTemporalApi.applicationFailure,
        "ApplicationFailure",
      )
    ) {
      return {
        kind: "call",
        callee: "__temporal_is_application_failure",
        args: [compileExpression(expression.left)],
      };
    }
    const logicalMap = new Map([
      [ts.SyntaxKind.AmpersandAmpersandToken, "and"],
      [ts.SyntaxKind.BarBarToken, "or"],
      [ts.SyntaxKind.QuestionQuestionToken, "coalesce"],
    ]);
    if (logicalMap.has(expression.operatorToken.kind)) {
      return {
        kind: "logical",
        op: logicalMap.get(expression.operatorToken.kind),
        left: compileExpression(expression.left),
        right: compileExpression(expression.right),
      };
    }
    const binaryMap = new Map([
      [ts.SyntaxKind.PlusToken, "add"],
      [ts.SyntaxKind.MinusToken, "subtract"],
      [ts.SyntaxKind.AsteriskToken, "multiply"],
      [ts.SyntaxKind.SlashToken, "divide"],
      [ts.SyntaxKind.PercentToken, "remainder"],
      [ts.SyntaxKind.EqualsEqualsEqualsToken, "equal"],
      [ts.SyntaxKind.ExclamationEqualsEqualsToken, "not_equal"],
      [ts.SyntaxKind.InKeyword, "in"],
      [ts.SyntaxKind.LessThanToken, "less_than"],
      [ts.SyntaxKind.LessThanEqualsToken, "less_than_or_equal"],
      [ts.SyntaxKind.GreaterThanToken, "greater_than"],
      [ts.SyntaxKind.GreaterThanEqualsToken, "greater_than_or_equal"],
    ]);
    if (!binaryMap.has(expression.operatorToken.kind)) {
      throw compilerError(
        `unsupported binary operator ${expression.operatorToken.getText()}`,
        expression.operatorToken,
      );
    }
    return {
      kind: "binary",
      op: binaryMap.get(expression.operatorToken.kind),
      left: compileExpression(expression.left),
      right: compileExpression(expression.right),
    };
  }
  if (ts.isConditionalExpression(expression)) {
    return {
      kind: "conditional",
      condition: compileExpression(expression.condition),
      then_expr: compileExpression(expression.whenTrue),
      else_expr: compileExpression(expression.whenFalse),
    };
  }
  if (ts.isCallExpression(expression)) {
    const patchedExpression = compileTemporalPatchedExpression(expression);
    if (patchedExpression) {
      return patchedExpression;
    }
    if (
      ts.isPropertyAccessExpression(expression.expression) &&
      ts.isIdentifier(expression.expression.expression) &&
      expression.expression.expression.text === "Math" &&
      (expression.expression.name.text === "floor" || expression.expression.name.text === "min")
    ) {
      if (
        (expression.expression.name.text === "floor" && expression.arguments.length !== 1) ||
        (expression.expression.name.text === "min" && expression.arguments.length !== 2)
      ) {
        throw compilerError(`Math.${expression.expression.name.text}() has unsupported arity`, expression);
      }
      return {
        kind: "call",
        callee:
          expression.expression.name.text === "floor"
            ? "__builtin_math_floor"
            : "__builtin_math_min",
        args: expression.arguments.map(compileExpression),
      };
    }
    if (
      ts.isPropertyAccessExpression(expression.expression) &&
      ts.isIdentifier(expression.expression.expression) &&
      expression.expression.expression.text === "Date" &&
      expression.expression.name.text === "now"
    ) {
      if (expression.arguments.length !== 0) {
        throw compilerError(`Date.now() does not accept arguments`, expression);
      }
      return { kind: "now" };
    }
    if (
      ts.isPropertyAccessExpression(expression.expression) &&
      ts.isIdentifier(expression.expression.expression) &&
      expression.expression.expression.text === "Math" &&
      expression.expression.name.text === "random"
    ) {
      if (expression.arguments.length !== 0) {
        throw compilerError(`Math.random() does not accept arguments`, expression);
      }
      return { kind: "random", scope: stableNodeKey(expression) };
    }
    if (
      ts.isPropertyAccessExpression(expression.expression) &&
      ts.isIdentifier(expression.expression.expression) &&
      expression.expression.expression.text === "Object" &&
      expression.expression.name.text === "keys"
    ) {
      if (expression.arguments.length !== 1) {
        throw compilerError(`Object.keys() requires exactly one argument`, expression);
      }
      return {
        kind: "call",
        callee: "__builtin_object_keys",
        args: [compileExpression(expression.arguments[0])],
      };
    }
    if (temporalImportedCallMatches(expression, currentTemporalApi.workflowInfo, "workflowInfo")) {
      if (expression.arguments.length !== 0) {
        throw compilerError(`workflowInfo() does not accept arguments`, expression);
      }
      return { kind: "workflow_info" };
    }
    if (temporalImportedCallMatches(expression, currentTemporalApi.uuid4, "uuid4")) {
      if (expression.arguments.length !== 0) {
        throw compilerError(`uuid4() does not accept arguments`, expression);
      }
      return { kind: "uuid", scope: stableNodeKey(expression) };
    }
    if (
      ts.isPropertyAccessExpression(expression.expression) &&
      temporalImportedReferenceMatches(
        expression.expression.expression,
        currentTemporalApi.applicationFailure,
        "ApplicationFailure",
      )
    ) {
      const method = expression.expression.name.text;
      if (method === "nonRetryable" || method === "retryable") {
        return compileApplicationFailureExpression(
          expression.arguments[0],
          { nonRetryable: method === "nonRetryable" },
          expression,
        );
      }
      if (method === "create") {
        return compileApplicationFailureCreateExpression(expression);
      }
    }
    if (ts.isPropertyAccessExpression(expression.expression)) {
      const method = expression.expression.name.text;
      if (method === "get") {
        if (expression.arguments.length !== 1) {
          throw compilerError(`Map.get() requires exactly one key argument`, expression);
        }
        return {
          kind: "call",
          callee: "__builtin_map_get",
          args: [
            compileExpression(expression.expression.expression),
            compileExpression(expression.arguments[0]),
          ],
        };
      }
      if (method === "has") {
        if (expression.arguments.length !== 1) {
          throw compilerError(`Map.has() requires exactly one key argument`, expression);
        }
        return {
          kind: "call",
          callee: "__builtin_map_has",
          args: [
            compileExpression(expression.expression.expression),
            compileExpression(expression.arguments[0]),
          ],
        };
      }
      if (method === "valueOf") {
        if (expression.arguments.length !== 0) {
          throw compilerError(`Date.valueOf() does not accept arguments`, expression);
        }
        return {
          kind: "call",
          callee: "__builtin_date_value_of",
          args: [compileExpression(expression.expression.expression)],
        };
      }
      if (method === "getDate") {
        if (expression.arguments.length !== 0) {
          throw compilerError(`Date.getDate() does not accept arguments`, expression);
        }
        return {
          kind: "call",
          callee: "__builtin_date_get_date",
          args: [compileExpression(expression.expression.expression)],
        };
      }
      if (
        method === "fill" &&
        ts.isNewExpression(expression.expression.expression) &&
        ts.isIdentifier(expression.expression.expression.expression) &&
        expression.expression.expression.expression.text === "Array"
      ) {
        if (
          expression.expression.expression.arguments?.length !== 1 ||
          expression.arguments.length !== 1
        ) {
          throw compilerError(`new Array(...).fill(...) requires one length and one fill value`, expression);
        }
        return {
          kind: "call",
          callee: "__builtin_array_fill",
          args: [
            compileExpression(expression.expression.expression.arguments[0]),
            compileExpression(expression.arguments[0]),
          ],
        };
      }
      if (method === "join") {
        if (expression.arguments.length > 1) {
          throw compilerError(`join() supports at most one separator argument`, expression);
        }
        return {
          kind: "call",
          callee: "__builtin_array_join",
          args: [
            compileExpression(expression.expression.expression),
            expression.arguments[0]
              ? compileExpression(expression.arguments[0])
              : { kind: "literal", value: "," },
          ],
        };
      }
      if (method === "toLowerCase" || method === "toUpperCase") {
        if (expression.arguments.length !== 0) {
          throw compilerError(`${method}() does not accept arguments`, expression);
        }
        return {
          kind: "call",
          callee:
            method === "toLowerCase"
              ? "__builtin_string_to_lowercase"
              : "__builtin_string_to_uppercase",
          args: [compileExpression(expression.expression.expression)],
        };
      }
      if (method === "reduce") {
        const handler = expression.arguments[0];
        const initial = expression.arguments[1];
        if (!handler || !initial) {
          throw compilerError(`reduce requires a reducer function and initial value`, expression);
        }
        const compiled = compileArrayReduceHandler(handler);
        return {
          kind: "array_reduce",
          array: compileExpression(expression.expression.expression),
          accumulator_name: compiled.accumulatorName,
          item_name: compiled.itemName,
          initial: compileExpression(initial),
          expr: compiled.expr,
        };
      }
      if (method === "sort") {
        if (expression.arguments.length === 0) {
          return {
            kind: "call",
            callee: "__builtin_array_sort_default",
            args: [compileExpression(expression.expression.expression)],
          };
        }
        if (
          expression.arguments.length === 1 &&
          (ts.isArrowFunction(expression.arguments[0]) || ts.isFunctionExpression(expression.arguments[0]))
        ) {
          const handler = expression.arguments[0];
          if (
            handler.parameters.length === 2 &&
            ts.isIdentifier(handler.parameters[0].name) &&
            ts.isIdentifier(handler.parameters[1].name)
          ) {
            const comparatorBody = functionBodyExpression(handler.body);
            if (
              comparatorBody &&
              ts.isBinaryExpression(comparatorBody) &&
              comparatorBody.operatorToken.kind === ts.SyntaxKind.MinusToken &&
              ts.isIdentifier(comparatorBody.left) &&
              ts.isIdentifier(comparatorBody.right) &&
              comparatorBody.left.text === handler.parameters[0].name.text &&
              comparatorBody.right.text === handler.parameters[1].name.text
            ) {
              return {
                kind: "call",
                callee: "__builtin_array_sort_numeric_asc",
                args: [compileExpression(expression.expression.expression)],
              };
            }
          }
        }
        throw compilerError(`sort() only supports no-arg default sorting or (a, b) => a - b`, expression);
      }
      if (method === "find" || method === "map") {
        const handler = expression.arguments[0];
        if (!handler) {
          throw compilerError(`${method} requires a function handler`, expression);
        }
        if (
          method === "map" &&
          ts.isIdentifier(handler) &&
          handler.text === "Number"
        ) {
          return {
            kind: "call",
            callee: "__builtin_array_map_number",
            args: [compileExpression(expression.expression.expression)],
          };
        }
        const compiled = compileArrayMethodHandler(method, handler);
        if (method === "find") {
          return {
            kind: "array_find",
            array: compileExpression(expression.expression.expression),
            item_name: compiled.itemName,
            predicate: compiled.expr,
          };
        }
        return {
          kind: "array_map",
          array: compileExpression(expression.expression.expression),
          item_name: compiled.itemName,
          expr: compiled.expr,
        };
      }
    }
    if (
      ts.isPropertyAccessExpression(expression.expression) &&
      isCtxReceiver(expression.expression.expression)
    ) {
      const method = expression.expression.name.text;
      if (method === "now") {
        if (expression.arguments.length !== 0) {
          throw compilerError(`ctx.now() does not accept arguments`, expression);
        }
        return { kind: "now" };
      }
      if (method === "uuid") {
        if (expression.arguments.length !== 0) {
          throw compilerError(`ctx.uuid() does not accept arguments`, expression);
        }
        return { kind: "uuid", scope: stableNodeKey(expression) };
      }
      if (method === "sideEffect") {
        if (expression.arguments.length !== 1) {
          throw compilerError(`ctx.sideEffect() requires exactly one argument`, expression);
        }
        return {
          kind: "side_effect",
          marker_id: `marker_${stableNodeKey(expression)}`,
          expr: compileExpression(expression.arguments[0]),
        };
      }
      if (method === "version") {
        if (expression.arguments.length !== 3) {
          throw compilerError(`ctx.version() requires changeId, minSupported, and maxSupported`, expression);
        }
        return {
          kind: "version",
          change_id: literalString(expression.arguments[0], "ctx.version changeId"),
          min_supported: literalNumber(expression.arguments[1], "ctx.version minSupported"),
          max_supported: literalNumber(expression.arguments[2], "ctx.version maxSupported"),
        };
      }
      throw compilerError(
        `ctx.${method} is only allowed as an awaited workflow primitive or terminal call`,
        expression,
      );
    }
    if (ts.isIdentifier(expression.expression)) {
      if (expression.expression.text === "String") {
        if (expression.arguments.length !== 1) {
          throw compilerError(`String() requires exactly one argument`, expression);
        }
        return {
          kind: "call",
          callee: "__builtin_string",
          args: [compileExpression(expression.arguments[0])],
        };
      }
      return {
        kind: "call",
        callee: expression.expression.text,
        args: expression.arguments.map(compileExpression),
      };
    }
  }
  if (ts.isNewExpression(expression)) {
    if (ts.isIdentifier(expression.expression) && expression.expression.text === "Date") {
      if ((expression.arguments?.length ?? 0) > 1) {
        throw compilerError(`new Date() currently supports zero or one argument`, expression);
      }
      return {
        kind: "call",
        callee: "__builtin_date_new",
        args: expression.arguments ? expression.arguments.map(compileExpression) : [],
      };
    }
    if (ts.isIdentifier(expression.expression) && expression.expression.text === "Map") {
      if ((expression.arguments?.length ?? 0) !== 0) {
        throw compilerError(`new Map() currently supports only the zero-argument form`, expression);
      }
      return { kind: "object", fields: {} };
    }
    if (
      temporalImportedReferenceMatches(
        expression.expression,
        currentTemporalApi.applicationFailure,
        "ApplicationFailure",
      )
    ) {
      return compileApplicationFailureExpression(expression.arguments?.[0], {}, expression);
    }
  }
  if (ts.isArrowFunction(expression) || ts.isFunctionExpression(expression)) {
    const descriptor = compileDeferredActivityThunkExpression(expression);
    if (descriptor) {
      return descriptor;
    }
  }

  throw compilerError(`unsupported expression: ${expression.getText()}`, expression);
}

class WorkflowLowerer {
  constructor(
    definitionId,
    version,
    workflowDeclaration,
    temporalApi = {
      activityFailure: new Set(),
      proxyActivities: new Set(),
      sleep: new Set(),
      continueAsNew: new Set(),
      condition: new Set(),
      cancellationScope: new Set(),
      isCancellation: new Set(),
      getExternalWorkflowHandle: new Set(),
      executeChild: new Set(),
      startChild: new Set(),
      defineQuery: new Set(),
      defineUpdate: new Set(),
      defineSignal: new Set(),
      setHandler: new Set(),
      setWorkflowOptions: new Set(),
      upsertSearchAttributes: new Set(),
      workflowInfo: new Set(),
      log: new Set(),
      uuid4: new Set(),
      patched: new Set(),
      deprecatePatch: new Set(),
      applicationFailure: new Set(),
      parentClosePolicy: new Set(),
      activityCancellationType: new Set(),
      namespaceImports: new Set(),
    },
    statePrefix = "",
    typeChecker = null,
  ) {
    this.definitionId = definitionId;
    this.version = version;
    this.workflowDeclaration = workflowDeclaration;
    this.temporalApi = temporalApi;
    this.statePrefix = statePrefix;
    this.typeChecker = typeChecker;
    this.states = {};
    this.sourceMap = {};
    this.syntheticCounts = new Map();
    this.queries = {};
    this.signals = {};
    this.updates = {};
    this.nonCancellableStates = new Set();
    this.childHandleVars = new Set();
    this.childPromiseArrayVars = new Set();
    this.externalHandleVars = new Map();
    this.bulkHandleVars = new Set();
    this.temporalSignalHandlers = new Map();
    const sourceFile =
      workflowDeclaration && typeof workflowDeclaration.getSourceFile === "function"
        ? workflowDeclaration.getSourceFile()
        : null;
    this.temporalDefinitions = sourceFile
      ? this.discoverTemporalDefinitions(sourceFile)
      : { query: new Map(), update: new Map(), signal: new Map() };
    this.temporalActivityBindings = sourceFile
      ? this.discoverTemporalActivityBindings(sourceFile)
      : { direct: new Map(), objects: new Set() };
    this.asyncLocalHelpers = sourceFile ? this.discoverAsyncLocalHelpers(sourceFile) : new Map();
    this.discoverHandleDeclarations(workflowDeclaration.body);
  }

  discoverTemporalDefinitions(sourceFile) {
    const definitions = {
      query: new Map(),
      update: new Map(),
      signal: new Map(),
    };
    for (const statement of sourceFile.statements) {
      if (!ts.isVariableStatement(statement)) {
        continue;
      }
      for (const declaration of statement.declarationList.declarations) {
        if (!isTemporalDefinitionDeclaration(declaration, this.temporalApi)) {
          continue;
        }
        const kind = temporalDefinitionKind(declaration, this.temporalApi);
        const name =
          declaration.initializer.arguments[0] != null
            ? literalString(
                declaration.initializer.arguments[0],
                `Temporal ${kind} definition name`,
              )
            : declaration.name.text;
        definitions[kind].set(declaration.name.text, name);
      }
    }
    return definitions;
  }

  resolveTemporalDefinitionReference(expression) {
    if (ts.isIdentifier(expression)) {
      const localKinds = [
        ["query", this.temporalDefinitions.query],
        ["update", this.temporalDefinitions.update],
        ["signal", this.temporalDefinitions.signal],
      ];
      for (const [kind, definitions] of localKinds) {
        if (definitions.has(expression.text)) {
          return { kind, name: definitions.get(expression.text) };
        }
      }
    }
    if (!this.typeChecker) {
      return null;
    }
    const symbolTarget =
      ts.isPropertyAccessExpression(expression) ? expression.name : expression;
    let symbol = this.typeChecker.getSymbolAtLocation(symbolTarget);
    if (!symbol) {
      return null;
    }
    const declaration = symbol?.valueDeclaration ?? symbol?.declarations?.[0] ?? null;
    if (
      (symbol.flags & ts.SymbolFlags.Alias) !== 0 ||
      ts.isImportSpecifier(declaration) ||
      ts.isImportClause(declaration) ||
      ts.isNamespaceImport(declaration)
    ) {
      symbol = this.typeChecker.getAliasedSymbol(symbol);
    }
    const resolvedDeclaration = symbol?.valueDeclaration ?? symbol?.declarations?.[0] ?? null;
    const declarationApi = resolvedDeclaration
      ? collectTemporalWorkflowApi(resolvedDeclaration.getSourceFile())
      : this.temporalApi;
    if (
      !resolvedDeclaration ||
      !ts.isVariableDeclaration(resolvedDeclaration) ||
      !isTemporalDefinitionDeclaration(resolvedDeclaration, declarationApi)
    ) {
      return null;
    }
    const kind = temporalDefinitionKind(resolvedDeclaration, declarationApi);
    const name =
      resolvedDeclaration.initializer.arguments[0] != null
        ? literalString(
            resolvedDeclaration.initializer.arguments[0],
            `Temporal ${kind} definition name`,
          )
        : resolvedDeclaration.name.text;
    return { kind, name };
  }

  discoverTemporalActivityBindings(sourceFile) {
    const bindings = {
      direct: new Map(),
      objects: new Set(),
      objectOptions: new Map(),
    };
    for (const statement of sourceFile.statements) {
      if (!ts.isVariableStatement(statement)) {
        continue;
      }
      for (const declaration of statement.declarationList.declarations) {
        if (!isTemporalProxyDeclaration(declaration, this.temporalApi)) {
          continue;
        }
        const proxyCall = unwrapStaticReferenceExpression(declaration.initializer);
        if (!ts.isCallExpression(proxyCall)) {
          throw compilerError(`proxyActivities options must be declared inline`, declaration);
        }
        const options = parseTemporalProxyActivityOptions(proxyCall);
        if (ts.isIdentifier(declaration.name)) {
          bindings.objects.add(declaration.name.text);
          bindings.objectOptions.set(declaration.name.text, options);
          continue;
        }
        for (const element of declaration.name.elements) {
          bindings.direct.set(element.name.text, {
            activityType: element.name.text,
            options,
          });
        }
      }
    }
    return bindings;
  }

  discoverAsyncLocalHelpers(sourceFile) {
    const helpers = new Map();
    for (const statement of sourceFile.statements) {
      if (
        ts.isFunctionDeclaration(statement) &&
        statement.name &&
        statement.modifiers?.some((modifier) => modifier.kind === ts.SyntaxKind.AsyncKeyword)
      ) {
        helpers.set(statement.name.text, statement);
        continue;
      }
      if (!ts.isVariableStatement(statement)) {
        continue;
      }
      for (const declaration of statement.declarationList.declarations) {
        if (!ts.isIdentifier(declaration.name) || !declaration.initializer) {
          continue;
        }
        if (
          (ts.isArrowFunction(declaration.initializer) || ts.isFunctionExpression(declaration.initializer)) &&
          declaration.initializer.modifiers?.some((modifier) => modifier.kind === ts.SyntaxKind.AsyncKeyword)
        ) {
          helpers.set(declaration.name.text, declaration.initializer);
        }
      }
    }
    return helpers;
  }

  resolveTemporalActivityCall(expression) {
    if (ts.isIdentifier(expression)) {
      const activity = this.temporalActivityBindings.direct.get(expression.text);
      return activity ? { activityType: activity.activityType, options: activity.options } : null;
    }
    if (
      ts.isPropertyAccessExpression(expression) &&
      ts.isIdentifier(expression.expression) &&
      this.temporalActivityBindings.objects.has(expression.expression.text)
    ) {
      return {
        activityType: expression.name.text,
        options: this.temporalActivityBindings.objectOptions.get(expression.expression.text) ?? {},
      };
    }
    return null;
  }

  resolveTemporalDynamicActivityCall(expression) {
    if (
      ts.isElementAccessExpression(expression) &&
      ts.isIdentifier(expression.expression) &&
      this.temporalActivityBindings.objects.has(expression.expression.text)
    ) {
      return {
        activityTypeExpr: compileExpression(expression.argumentExpression),
        options: this.temporalActivityBindings.objectOptions.get(expression.expression.text) ?? {},
      };
    }
    return null;
  }

  resolveTemporalSignalName(expression, label) {
    const definition = this.resolveTemporalDefinitionReference(expression);
    if (definition?.kind === "signal") {
      return definition.name;
    }
    if (ts.isIdentifier(expression)) {
      return expression.text;
    }
    return literalString(expression, label);
  }

  resolveTemporalCancellationRequestedAwait(expression) {
    if (
      !ts.isPropertyAccessExpression(expression) ||
      expression.name.text !== "cancelRequested" ||
      !ts.isCallExpression(expression.expression) ||
      !ts.isPropertyAccessExpression(expression.expression.expression)
    ) {
      return false;
    }
    const receiver = expression.expression.expression.expression;
    const method = expression.expression.expression.name.text;
    return (
      temporalImportedReferenceMatches(
        receiver,
        this.temporalApi.cancellationScope,
        "CancellationScope",
        this.temporalApi,
      ) &&
      method === "current" &&
      expression.expression.arguments.length === 0
    );
  }

  resolveAsyncLocalHelperCall(callExpression) {
    if (!ts.isIdentifier(callExpression.expression)) {
      return null;
    }
    return this.asyncLocalHelpers.get(callExpression.expression.text) ?? null;
  }

  resolveTemporalPromiseFanout(call) {
    if (
      !ts.isPropertyAccessExpression(call.expression) ||
      !ts.isIdentifier(call.expression.expression) ||
      call.expression.expression.text !== "Promise" ||
      call.arguments.length !== 1
    ) {
    return null;
  }
    const reducer = call.expression.name.text === "all"
      ? "collect_results"
      : call.expression.name.text === "allSettled"
        ? "collect_settled_results"
        : null;
    if (!reducer) {
      return null;
    }
    const mapCall = call.arguments[0];
    if (
      !ts.isCallExpression(mapCall) ||
      !ts.isPropertyAccessExpression(mapCall.expression) ||
      mapCall.expression.name.text !== "map" ||
      mapCall.arguments.length !== 1
    ) {
      return null;
    }
    const mapper = mapCall.arguments[0];
    if (!ts.isArrowFunction(mapper) && !ts.isFunctionExpression(mapper)) {
      return null;
    }
    if (mapper.parameters.length !== 1 || !ts.isIdentifier(mapper.parameters[0].name)) {
      return null;
    }
    const mapperArg = mapper.parameters[0].name.text;
    const mappedExpression = functionBodyExpression(mapper.body);
    if (!mappedExpression) {
      return null;
    }
    const activityCall =
      ts.isAwaitExpression(mappedExpression) ? mappedExpression.expression : mappedExpression;
    if (!ts.isCallExpression(activityCall)) {
      return null;
    }
    const temporalActivity = this.resolveTemporalActivityCall(activityCall.expression);
    if (!temporalActivity) {
      return null;
    }
    if (
      activityCall.arguments.length !== 1 ||
      !ts.isIdentifier(activityCall.arguments[0]) ||
      activityCall.arguments[0].text !== mapperArg
    ) {
      return null;
    }
    return {
      activityType: temporalActivity.activityType,
      itemsExpr: compileExpression(mapCall.expression.expression),
      reducer,
      options: temporalActivity.options ?? {},
    };
  }

  discoverHandleDeclarations(node) {
    const visit = (current) => {
      if (
        ts.isVariableDeclaration(current) &&
        ts.isIdentifier(current.name) &&
        current.initializer &&
        ts.isCallExpression(current.initializer) &&
        temporalImportedCallMatches(
          current.initializer,
          this.temporalApi.getExternalWorkflowHandle,
          "getExternalWorkflowHandle",
          this.temporalApi,
        )
      ) {
        const call = current.initializer;
        if (call.arguments.length < 1 || call.arguments.length > 2) {
          throw compilerError(`getExternalWorkflowHandle requires workflowId and optional runId`, call);
        }
        this.externalHandleVars.set(current.name.text, {
          targetInstanceId: compileExpression(call.arguments[0]),
          targetRunId: call.arguments[1] ? compileExpression(call.arguments[1]) : null,
        });
      }
      if (
        ts.isVariableDeclaration(current) &&
        ts.isIdentifier(current.name) &&
        current.initializer &&
        ts.isAwaitExpression(current.initializer) &&
        ts.isCallExpression(current.initializer.expression)
      ) {
        const awaitedCall = current.initializer.expression;
        if (
          ts.isPropertyAccessExpression(awaitedCall.expression) &&
          isCtxReceiver(awaitedCall.expression.expression)
        ) {
          const method = awaitedCall.expression.name.text;
          if (method === "startChild") this.childHandleVars.add(current.name.text);
          if (method === "bulkActivity") this.bulkHandleVars.add(current.name.text);
        }
        if (
          temporalImportedCallMatches(awaitedCall, this.temporalApi.startChild, "startChild", this.temporalApi)
        ) {
          this.childHandleVars.add(current.name.text);
        }
      }
      if (
        ts.isExpressionStatement(current) &&
        ts.isCallExpression(current.expression) &&
        ts.isPropertyAccessExpression(current.expression.expression) &&
        current.expression.expression.name.text === "push" &&
        ts.isIdentifier(current.expression.expression.expression) &&
        current.expression.arguments.length === 1 &&
        ts.isCallExpression(current.expression.arguments[0]) &&
        (
          temporalImportedCallMatches(
            current.expression.arguments[0],
            this.temporalApi.executeChild,
            "executeChild",
            this.temporalApi,
          ) ||
          temporalImportedCallMatches(
            current.expression.arguments[0],
            this.temporalApi.startChild,
            "startChild",
            this.temporalApi,
          )
        )
      ) {
        this.childPromiseArrayVars.add(current.expression.expression.expression.text);
      }
      ts.forEachChild(current, visit);
    };
    visit(node);
  }

  registerTemporalNamedHandler(callExpression) {
    const definition = callExpression.arguments[0];
    const handler = callExpression.arguments[1];
    if (
      !definition ||
      (!ts.isIdentifier(definition) && !ts.isPropertyAccessExpression(definition))
    ) {
      throw compilerError(`setHandler requires a named Temporal definition`, callExpression);
    }
    const resolvedDefinition = this.resolveTemporalDefinitionReference(definition);
    if (resolvedDefinition?.kind === "query") {
      const queryName = resolvedDefinition.name;
      if (!handler || (!ts.isArrowFunction(handler) && !ts.isFunctionExpression(handler))) {
        throw compilerError(`Temporal query setHandler requires an inline function handler`, callExpression);
      }
      if (handler.modifiers?.some((modifier) => modifier.kind === ts.SyntaxKind.AsyncKeyword)) {
        throw compilerError(`Temporal query handlers must not be async`, handler);
      }
      const body = compilePureHandlerExpression(handler, "setHandler(query)");
      this.queries[queryName] = {
        arg_name: handler.parameters[0] ? handler.parameters[0].name.getText() : undefined,
        expr: body,
      };
      return;
    }
    if (resolvedDefinition?.kind === "update") {
      const updateName = resolvedDefinition.name;
      if (!handler || (!ts.isArrowFunction(handler) && !ts.isFunctionExpression(handler))) {
        throw compilerError(`Temporal update setHandler requires an inline function handler`, callExpression);
      }
      const bodyBlock = blockFromInlineHandlerBody(handler.body);
      const lowered = new WorkflowLowerer(
        this.definitionId,
        this.version,
        { body: bodyBlock },
        this.temporalApi,
        `update_${shortHash(updateName)}_`,
        this.typeChecker,
      );
      lowered.temporalDefinitions = this.temporalDefinitions;
      lowered.temporalActivityBindings = this.temporalActivityBindings;
      const terminalFail = lowered.addState("fail_terminal", {
        type: "fail",
        reason: { kind: "literal", value: `update ${updateName} terminated without explicit completion` },
      });
      const initialState = lowered.lowerBlock(bodyBlock.statements, terminalFail, null, null, null);
      this.updates[updateName] = {
        arg_name: handler.parameters[0] ? handler.parameters[0].name.getText() : undefined,
        initial_state: initialState,
        states: lowered.states,
      };
      for (const stateId of lowered.nonCancellableStates) {
        this.nonCancellableStates.add(stateId);
      }
      return;
    }
    if (resolvedDefinition?.kind === "signal") {
      const signalName = resolvedDefinition.name;
      if (!handler || (!ts.isArrowFunction(handler) && !ts.isFunctionExpression(handler))) {
        throw compilerError(`Temporal signal setHandler requires an inline function handler`, callExpression);
      }
      const bodyBlock = blockFromInlineHandlerBody(handler.body);
      const lowered = new WorkflowLowerer(
        this.definitionId,
        this.version,
        { body: bodyBlock },
        this.temporalApi,
        `signal_${shortHash(signalName)}_`,
        this.typeChecker,
      );
      lowered.temporalDefinitions = this.temporalDefinitions;
      lowered.temporalActivityBindings = this.temporalActivityBindings;
      const terminalComplete = lowered.addState("signal_complete", {
        type: "succeed",
      });
      const initialState = lowered.lowerBlock(bodyBlock.statements, terminalComplete, null, null, null);
      this.signals[signalName] = {
        arg_name: handler.parameters[0] ? handler.parameters[0].name.getText() : undefined,
        initial_state: initialState,
        states: lowered.states,
      };
      for (const stateId of lowered.nonCancellableStates) {
        this.nonCancellableStates.add(stateId);
      }
      const conditionSignalHandler = tryCompileSignalHandlerActions(handler);
      if (conditionSignalHandler) {
        this.temporalSignalHandlers.set(definition.text, {
          signalName,
          ...conditionSignalHandler,
        });
      } else {
        this.temporalSignalHandlers.set(definition.text, {
          signalName,
          argName: handler.parameters[0] ? handler.parameters[0].name.getText() : null,
          actions: null,
        });
      }
      return;
    }
    throw compilerError(`setHandler requires a Temporal query/update/signal definition`, definition);
  }

  lowerTemporalCondition(callExpression, targetVar, nextState) {
    const predicate = callExpression.arguments[0];
    if (!predicate || (!ts.isArrowFunction(predicate) && !ts.isFunctionExpression(predicate))) {
      throw compilerError(`condition requires an inline function predicate`, callExpression);
    }
    if (callExpression.arguments.length > 2) {
      throw compilerError(`condition accepts at most a predicate and an optional timeout`, callExpression);
    }
    const predicateExpr = compilePureHandlerExpression(predicate, "condition");
    let continueState = nextState;
    if (targetVar) {
      continueState = this.addState("assign", {
        type: "assign",
        actions: [{ target: targetVar, expr: { kind: "literal", value: true } }],
        next: nextState,
      }, callExpression);
    }
    const timeoutRef = callExpression.arguments[1]
      ? parseTemporalDurationRef(callExpression.arguments[1], "condition timeout")
      : null;
    const timeoutState = timeoutRef
      ? targetVar
        ? this.addState("assign", {
            type: "assign",
            actions: [{ target: targetVar, expr: { kind: "literal", value: false } }],
            next: nextState,
          }, callExpression)
        : nextState
      : null;
    return this.addState("wait_condition", {
      type: "wait_for_condition",
      condition: predicateExpr,
      next: continueState,
      timeout_ref: timeoutRef ?? undefined,
      timeout_next: timeoutState ?? undefined,
    }, callExpression);
  }

  nextId(prefix, node = null) {
    if (!node) {
      const count = (this.syntheticCounts.get(prefix) ?? 0) + 1;
      this.syntheticCounts.set(prefix, count);
      return `${this.statePrefix}${prefix}_${count}`;
    }

    const nodeKey = `${this.statePrefix}${prefix}:${stableNodeKey(node)}`;
    const count = (this.syntheticCounts.get(nodeKey) ?? 0) + 1;
    this.syntheticCounts.set(nodeKey, count);
    return count === 1
      ? `${this.statePrefix}${prefix}_${stableNodeKey(node)}`
      : `${this.statePrefix}${prefix}_${stableNodeKey(node)}_${count}`;
  }

  lower() {
    const terminalFail = this.addState("fail_terminal", {
      type: "fail",
      reason: { kind: "literal", value: "workflow terminated without explicit completion" },
    });
    const initialState = this.lowerBlock(
      this.workflowDeclaration.body.statements,
      terminalFail,
      null,
      null,
      null,
    );
    return {
      initialState,
      states: this.states,
      sourceMap: this.sourceMap,
      queries: this.queries,
      signals: this.signals,
      updates: this.updates,
      params: this.compileWorkflowParams(),
      nonCancellableStates: Array.from(this.nonCancellableStates),
    };
  }

  compileWorkflowParams() {
    const parameters = Array.isArray(this.workflowDeclaration?.parameters)
      ? this.workflowDeclaration.parameters
      : [];
    return parameters.map((parameter) => compileWorkflowParam(parameter));
  }

  addState(prefix, state, node = null) {
    const id = this.nextId(prefix, node);
    this.states[id] = state;
    if ((this.nonCancellableDepth ?? 0) > 0) {
      this.nonCancellableStates.add(id);
    }
    if (node) {
      this.sourceMap[id] = sourceLocation(node);
    }
    return id;
  }

  lowerBlock(statements, nextState, breakTarget, continueTarget, errorTarget, returnTarget = null) {
    for (const statement of statements) {
      if (
        ts.isExpressionStatement(statement) &&
        ts.isCallExpression(statement.expression) &&
        temporalImportedCallMatches(statement.expression, this.temporalApi.setHandler, "setHandler", this.temporalApi)
      ) {
        this.registerTemporalNamedHandler(statement.expression);
      }
    }
    let cursor = nextState;
    for (let index = statements.length - 1; index >= 0; index -= 1) {
      cursor = this.lowerStatement(
        statements[index],
        cursor,
        breakTarget,
        continueTarget,
        errorTarget,
        returnTarget,
      );
    }
    return cursor;
  }

  lowerStatement(statement, nextState, breakTarget, continueTarget, errorTarget, returnTarget = null) {
    if (ts.isExpressionStatement(statement) && ts.isCallExpression(statement.expression)) {
      const mutatingCollectionAction = compileMutatingCollectionAction(statement.expression);
      if (mutatingCollectionAction) {
        return this.addState("assign", {
          type: "assign",
          actions: [mutatingCollectionAction],
          next: nextState,
        }, statement);
      }
      if (temporalImportedCallMatches(statement.expression, this.temporalApi.setHandler, "setHandler", this.temporalApi)) {
        this.registerTemporalNamedHandler(statement.expression);
        return nextState;
      }
      if (temporalImportedCallMatches(statement.expression, this.temporalApi.deprecatePatch, "deprecatePatch", this.temporalApi)) {
        if (statement.expression.arguments.length !== 1) {
          throw compilerError(`deprecatePatch() requires exactly one change id`, statement.expression);
        }
        return nextState;
      }
      if (
        temporalImportedCallMatches(
          statement.expression,
          this.temporalApi.upsertSearchAttributes,
          "upsertSearchAttributes",
          this.temporalApi,
        )
      ) {
        if (statement.expression.arguments.length !== 1) {
          throw compilerError(
            `upsertSearchAttributes() requires exactly one search attribute patch object`,
            statement.expression,
          );
        }
        return this.addState("assign", {
          type: "assign",
          actions: [
            {
              target: "__search_attributes",
              expr: {
                kind: "call",
                callee: "__builtin_search_attributes_upsert",
                args: [
                  {
                    kind: "member",
                    object: { kind: "workflow_info" },
                    property: "searchAttributes",
                  },
                  compileExpression(statement.expression.arguments[0]),
                ],
              },
            },
          ],
          next: nextState,
        }, statement);
      }
      const pushedChildState = this.lowerTemporalChildPromisePush(statement.expression, nextState, statement);
      if (pushedChildState) {
        return pushedChildState;
      }
      const pushAction = compileArrayPushAction(statement, statement.expression);
      if (pushAction) {
        return this.addState("assign", {
          type: "assign",
          actions: [pushAction],
          next: nextState,
        }, statement);
      }
      const unshiftAction = compileArrayUnshiftAction(statement, statement.expression);
      if (unshiftAction) {
        return this.addState("assign", {
          type: "assign",
          actions: [unshiftAction],
          next: nextState,
        }, statement);
      }
      if (temporalLogCallMatches(statement.expression, this.temporalApi)) {
        return nextState;
      }
      if (
        ts.isPropertyAccessExpression(statement.expression.expression) &&
        isCtxReceiver(statement.expression.expression.expression)
      ) {
        const method = statement.expression.expression.name.text;
        if (method === "query") {
          this.registerQueryHandler(statement.expression);
          return nextState;
        }
        if (method === "update") {
          this.registerUpdateHandler(statement.expression);
          return nextState;
        }
      }
    }

    if (ts.isVariableStatement(statement)) {
      const declarations = statement.declarationList.declarations;
      const awaitDeclaration = declarations.find(
        (declaration) => declaration.initializer && ts.isAwaitExpression(declaration.initializer),
      );
      if (awaitDeclaration) {
        if (declarations.length !== 1 || !ts.isIdentifier(awaitDeclaration.name)) {
          throw compilerError(
            `await variable declarations must declare exactly one identifier`,
            statement,
          );
        }
        return this.lowerAwait(
          awaitDeclaration.initializer,
          awaitDeclaration.name.text,
          nextState,
          errorTarget,
        );
      }
      if (
        declarations.length === 1 &&
        ts.isIdentifier(declarations[0].name) &&
        declarations[0].initializer
      ) {
        const wrappedAwaitState = this.lowerWrappedAwaitExpression(
          declarations[0].initializer,
          declarations[0].name.text,
          nextState,
          errorTarget,
        );
        if (wrappedAwaitState) {
          return wrappedAwaitState;
        }
      }
      if (
        declarations.length === 1 &&
        ts.isIdentifier(declarations[0].name) &&
        declarations[0].initializer &&
        ts.isCallExpression(declarations[0].initializer)
      ) {
        const shiftedArray = resolveArrayShiftCall(declarations[0].initializer);
        if (shiftedArray) {
          const targetName = declarations[0].name.text;
          return this.addState("assign", {
            type: "assign",
            actions: [
              {
                target: targetName,
                expr: {
                  kind: "call",
                  callee: "__builtin_array_shift_head",
                  args: [{ kind: "identifier", name: shiftedArray.arrayName }],
                },
              },
              {
                target: shiftedArray.arrayName,
                expr: {
                  kind: "call",
                  callee: "__builtin_array_shift_tail",
                  args: [{ kind: "identifier", name: shiftedArray.arrayName }],
                },
              },
            ],
            next: nextState,
          }, declarations[0]);
        }
      }
      const actions = declarations.map((declaration) => {
        if (!ts.isIdentifier(declaration.name)) {
          throw compilerError(`unsupported declaration: ${statement.getText()}`, declaration);
        }
        if (!declaration.initializer) {
          return {
            target: declaration.name.text,
            expr: { kind: "literal", value: null },
          };
        }
        if (
          ts.isCallExpression(declaration.initializer) &&
          temporalImportedCallMatches(
            declaration.initializer,
            this.temporalApi.getExternalWorkflowHandle,
            "getExternalWorkflowHandle",
            this.temporalApi,
          )
        ) {
          return null;
        }
        return {
          target: declaration.name.text,
          expr: compileExpression(declaration.initializer),
        };
      }).filter(Boolean);
      if (actions.length === 0) {
        return nextState;
      }
      return this.addState("assign", { type: "assign", actions, next: nextState }, statement);
    }

    if (ts.isExpressionStatement(statement)) {
      if (
        ts.isVoidExpression(statement.expression) &&
        ts.isCallExpression(statement.expression.expression)
      ) {
        const mutatingCollectionAction = compileMutatingCollectionAction(statement.expression.expression);
        if (mutatingCollectionAction) {
          return this.addState("assign", {
            type: "assign",
            actions: [mutatingCollectionAction],
            next: nextState,
          }, statement);
        }
      }
      if (ts.isAwaitExpression(statement.expression)) {
        return this.lowerAwait(statement.expression, null, nextState, errorTarget);
      }
      if (
        (ts.isPostfixUnaryExpression(statement.expression) || ts.isPrefixUnaryExpression(statement.expression)) &&
        ts.isIdentifier(statement.expression.operand) &&
        (
          statement.expression.operator === ts.SyntaxKind.PlusPlusToken ||
          statement.expression.operator === ts.SyntaxKind.MinusMinusToken
        )
      ) {
        return this.lowerSyntheticExpression(statement.expression, nextState);
      }
      if (
        ts.isBinaryExpression(statement.expression) &&
        (
          statement.expression.operatorToken.kind === ts.SyntaxKind.EqualsToken ||
          statement.expression.operatorToken.kind === ts.SyntaxKind.PlusEqualsToken
        )
      ) {
        if (ts.isAwaitExpression(statement.expression.right)) {
          if (!ts.isIdentifier(statement.expression.left)) {
            throw compilerError(`unsupported assignment target: ${statement.getText()}`, statement);
          }
          return this.lowerAwait(
            statement.expression.right,
            statement.expression.left.text,
            nextState,
            errorTarget,
          );
        }
        if (ts.isIdentifier(statement.expression.left)) {
          const wrappedAwaitState = this.lowerWrappedAwaitExpression(
            statement.expression.right,
            statement.expression.left.text,
            nextState,
            errorTarget,
          );
          if (wrappedAwaitState) {
            return wrappedAwaitState;
          }
        }
        return this.addState("assign", {
          type: "assign",
          actions: [
            compileAssignmentAction(
              statement,
              statement.expression.left,
              statement.expression.operatorToken.kind,
              statement.expression.right,
            ),
          ],
          next: nextState,
        }, statement);
      }
      if (ts.isDeleteExpression(statement.expression)) {
        return this.addState("assign", {
          type: "assign",
          actions: [compileDeleteAssignment(statement, statement.expression)],
          next: nextState,
        }, statement);
      }
      if (ts.isCallExpression(statement.expression) && temporalLogCallMatches(statement.expression, this.temporalApi)) {
        return nextState;
      }
      throw compilerError(`unsupported expression statement: ${statement.getText()}`, statement);
    }

    if (ts.isIfStatement(statement)) {
      const thenNext = this.lowerStatementOrBlock(
        statement.thenStatement,
        nextState,
        breakTarget,
        continueTarget,
        errorTarget,
        returnTarget,
      );
      const elseNext = statement.elseStatement
        ? this.lowerStatementOrBlock(
            statement.elseStatement,
            nextState,
            breakTarget,
            continueTarget,
            errorTarget,
            returnTarget,
          )
        : nextState;
      return this.addState("choice", {
        type: "choice",
        condition: compileExpression(statement.expression),
        then_next: thenNext,
        else_next: elseNext,
      }, statement.expression);
    }

    if (ts.isSwitchStatement(statement)) {
      const switchValueVar = this.nextId("__switch_value", statement.expression);
      let cursor = nextState;
      for (let index = statement.caseBlock.clauses.length - 1; index >= 0; index -= 1) {
        const clause = statement.caseBlock.clauses[index];
        const clauseNext = this.lowerBlock(
          clause.statements,
          nextState,
          nextState,
          continueTarget,
          errorTarget,
          returnTarget,
        );
        if (ts.isDefaultClause(clause)) {
          cursor = clauseNext;
          continue;
        }
        cursor = this.addState("choice", {
          type: "choice",
          condition: {
            kind: "binary",
            op: "equal",
            left: { kind: "identifier", name: switchValueVar },
            right: compileExpression(clause.expression),
          },
          then_next: clauseNext,
          else_next: cursor,
        }, clause.expression);
      }
      return this.addState("assign", {
        type: "assign",
        actions: [{ target: switchValueVar, expr: compileExpression(statement.expression) }],
        next: cursor,
      }, statement.expression);
    }

    if (ts.isWhileStatement(statement)) {
      const choiceState = this.nextId("while_choice", statement.expression);
      const bodyStart = this.lowerStatementOrBlock(
        statement.statement,
        choiceState,
        nextState,
        choiceState,
        errorTarget,
        returnTarget,
      );
      this.states[choiceState] = {
        type: "choice",
        condition: compileExpression(statement.expression),
        then_next: bodyStart,
        else_next: nextState,
      };
      this.sourceMap[choiceState] = sourceLocation(statement.expression);
      return choiceState;
    }

    if (ts.isDoStatement(statement)) {
      const choiceState = this.nextId("do_choice", statement.expression);
      const bodyStart = this.lowerStatementOrBlock(
        statement.statement,
        choiceState,
        nextState,
        choiceState,
        errorTarget,
        returnTarget,
      );
      this.states[choiceState] = {
        type: "choice",
        condition: compileExpression(statement.expression),
        then_next: bodyStart,
        else_next: nextState,
      };
      this.sourceMap[choiceState] = sourceLocation(statement.expression);
      return bodyStart;
    }

    if (ts.isForStatement(statement)) {
      const updateState = statement.incrementor
        ? this.lowerSyntheticExpression(statement.incrementor, null, null, null, null)
        : null;
      const choiceState = this.nextId("for_choice", statement.condition ?? statement);
      const continueState = updateState ?? choiceState;
      const bodyStart = this.lowerStatementOrBlock(
        statement.statement,
        continueState,
        nextState,
        continueState,
        errorTarget,
        returnTarget,
      );
      if (updateState) {
        this.states[updateState].next = choiceState;
      }
      this.states[choiceState] = {
        type: "choice",
        condition: statement.condition ? compileExpression(statement.condition) : { kind: "literal", value: true },
        then_next: bodyStart,
        else_next: nextState,
      };
      this.sourceMap[choiceState] = sourceLocation(statement.condition ?? statement);
      return statement.initializer
        ? this.lowerForInitializer(statement.initializer, choiceState)
        : choiceState;
    }

    if (ts.isForOfStatement(statement)) {
      if (!ts.isVariableDeclarationList(statement.initializer)) {
        throw compilerError(`unsupported for-of initializer: ${statement.getText()}`, statement);
      }
      const loopVar = statement.initializer.declarations[0];
      if (!ts.isIdentifier(loopVar.name)) {
        throw compilerError(`unsupported for-of binding: ${statement.getText()}`, loopVar.name);
      }
      const arrayVar = this.nextId("__items", statement.expression);
      const indexVar = this.nextId("__index", statement.expression);
      const assignLoopVar = this.addState("assign", {
        type: "assign",
        actions: [
          {
            target: loopVar.name.text,
            expr: {
              kind: "index",
              object: { kind: "identifier", name: arrayVar },
              index: { kind: "identifier", name: indexVar },
            },
          },
        ],
        next: "",
      }, statement);
      const updateIndex = this.addState("assign", {
        type: "assign",
        actions: [
          {
            target: indexVar,
            expr: {
              kind: "binary",
              op: "add",
              left: { kind: "identifier", name: indexVar },
              right: { kind: "literal", value: 1 },
            },
          },
        ],
        next: "",
      }, statement);
      const choiceState = this.nextId("for_of_choice", statement.expression);
      const bodyStart = this.lowerStatementOrBlock(
        statement.statement,
        updateIndex,
        nextState,
        updateIndex,
        errorTarget,
        returnTarget,
      );
      this.states[assignLoopVar].next = bodyStart;
      this.states[updateIndex].next = choiceState;
      this.states[choiceState] = {
        type: "choice",
        condition: {
          kind: "binary",
          op: "less_than",
          left: { kind: "identifier", name: indexVar },
          right: {
            kind: "member",
            object: { kind: "identifier", name: arrayVar },
            property: "length",
          },
        },
        then_next: assignLoopVar,
        else_next: nextState,
      };
      this.sourceMap[choiceState] = sourceLocation(statement.expression);
      return this.addState("assign", {
        type: "assign",
        actions: [
          { target: arrayVar, expr: compileExpression(statement.expression) },
          { target: indexVar, expr: { kind: "literal", value: 0 } },
        ],
        next: choiceState,
      }, statement);
    }

    if (ts.isBreakStatement(statement)) {
      if (!breakTarget) throw compilerError(`break used outside loop`, statement);
      return breakTarget;
    }

    if (ts.isContinueStatement(statement)) {
      if (!continueTarget) throw compilerError(`continue used outside loop`, statement);
      return continueTarget;
    }

    if (ts.isReturnStatement(statement)) {
      if (returnTarget) {
        if (!statement.expression) {
          if (!returnTarget.targetVar) {
            return returnTarget.next;
          }
          return this.addState("assign", {
            type: "assign",
            actions: [{ target: returnTarget.targetVar, expr: { kind: "literal", value: null } }],
            next: returnTarget.next,
          }, statement);
        }
        if (ts.isAwaitExpression(statement.expression)) {
          return this.lowerAwait(
            statement.expression,
            returnTarget.targetVar ?? null,
            returnTarget.next,
            errorTarget,
          );
        }
        const mutatingCollectionAction =
          ts.isCallExpression(statement.expression)
            ? compileMutatingCollectionAction(statement.expression)
            : ts.isVoidExpression(statement.expression) &&
                ts.isCallExpression(statement.expression.expression)
              ? compileMutatingCollectionAction(statement.expression.expression)
              : null;
        if (mutatingCollectionAction) {
          const actions = [mutatingCollectionAction];
          if (returnTarget.targetVar) {
            actions.push({
              target: returnTarget.targetVar,
              expr: { kind: "literal", value: null },
            });
          }
          return this.addState("assign", {
            type: "assign",
            actions,
            next: returnTarget.next,
          }, statement);
        }
        if (
          ts.isCallExpression(statement.expression) &&
          (
            (temporalImportedCallMatches(
              statement.expression,
              this.temporalApi.continueAsNew,
              "continueAsNew",
              this.temporalApi,
            )) ||
            (
              ts.isPropertyAccessExpression(statement.expression.expression) &&
              isCtxReceiver(statement.expression.expression.expression)
            )
          )
        ) {
          throw compilerError(
            `workflow terminal calls are not allowed inside CancellationScope handlers`,
            statement.expression,
          );
        }
        if (!returnTarget.targetVar) {
          return returnTarget.next;
        }
        return this.addState("assign", {
          type: "assign",
          actions: [{
            target: returnTarget.targetVar,
            expr: compileExpression(statement.expression),
          }],
          next: returnTarget.next,
        }, statement);
      }
      if (!statement.expression) {
        return this.addState("complete", { type: "succeed" }, statement);
      }
      if (ts.isAwaitExpression(statement.expression)) {
        const returnVar = this.nextId("return_value", statement.expression);
        const completeState = this.addState("complete", {
          type: "succeed",
          output: { kind: "identifier", name: returnVar },
        }, statement);
        return this.lowerAwait(statement.expression, returnVar, completeState, errorTarget);
      }
      if (!ts.isCallExpression(statement.expression)) {
        return this.addState("complete", {
          type: "succeed",
          output: compileExpression(statement.expression),
        }, statement);
      }
      if (
        temporalImportedCallMatches(
          statement.expression,
          this.temporalApi.continueAsNew,
          "continueAsNew",
          this.temporalApi,
        )
      ) {
        return this.addState("continue_as_new", {
          type: "continue_as_new",
          input: compileCallArgumentsAsInput(statement.expression.arguments),
        }, statement.expression);
      }
      if (
        ts.isPropertyAccessExpression(statement.expression.expression) &&
        isCtxReceiver(statement.expression.expression.expression)
      ) {
        return this.lowerTerminalCall(statement.expression);
      }
      return this.addState("complete", {
        type: "succeed",
        output: compileExpression(statement.expression),
      }, statement);
    }

    if (ts.isThrowStatement(statement)) {
      if (errorTarget) {
        const actions = [];
        if (errorTarget.error_var && statement.expression) {
          actions.push({ target: errorTarget.error_var, expr: compileExpression(statement.expression) });
        }
        return this.addState("assign", { type: "assign", actions, next: errorTarget.next }, statement);
      }
      return this.addState("fail", {
        type: "fail",
        reason: statement.expression ? compileExpression(statement.expression) : undefined,
      }, statement);
    }

    if (ts.isTryStatement(statement)) {
      const finallyStart = statement.finallyBlock
        ? this.lowerBlock(
            statement.finallyBlock.statements,
            nextState,
            breakTarget,
            continueTarget,
            errorTarget,
            returnTarget,
          )
        : nextState;
	      const catchError = statement.catchClause
	        ? {
	            next: this.lowerBlock(
	              statement.catchClause.block.statements,
	              finallyStart,
              breakTarget,
              continueTarget,
	              errorTarget,
	              returnTarget,
	            ),
	            error_var: ts.isIdentifier(statement.catchClause.variableDeclaration?.name)
	              ? statement.catchClause.variableDeclaration.name.text
	              : "__error",
	          }
	        : errorTarget;
      return this.lowerStatementOrBlock(
        statement.tryBlock,
        finallyStart,
        breakTarget,
        continueTarget,
        catchError,
        returnTarget,
      );
    }

    if (ts.isBlock(statement)) {
      return this.lowerBlock(
        statement.statements,
        nextState,
        breakTarget,
        continueTarget,
        errorTarget,
        returnTarget,
      );
    }

    throw compilerError(`unsupported statement: ${statement.getText()}`, statement);
  }

  lowerTemporalChildPromisePush(callExpression, nextState, node) {
    if (
      !ts.isPropertyAccessExpression(callExpression.expression) ||
      callExpression.expression.name.text !== "push" ||
      !ts.isIdentifier(callExpression.expression.expression) ||
      callExpression.arguments.length !== 1 ||
      !ts.isCallExpression(callExpression.arguments[0])
    ) {
      return null;
    }
    const arrayVar = callExpression.expression.expression.text;
    const pushedCall = callExpression.arguments[0];
    const pushHandleToArray = (childDefinition, options) => {
      const handleVar = this.nextId("child_handle", pushedCall);
      this.childHandleVars.add(handleVar);
      this.childPromiseArrayVars.add(arrayVar);
      const appendState = this.addState("assign", {
        type: "assign",
        actions: [{
          target: arrayVar,
          expr: {
            kind: "call",
            callee: "__builtin_array_append",
            args: [
              { kind: "identifier", name: arrayVar },
              { kind: "identifier", name: handleVar },
            ],
          },
        }],
        next: nextState,
      }, node);
      return this.addState("start_child", {
        type: "start_child",
        child_definition_id: childDefinition,
        input: options.input,
        next: appendState,
        handle_var: handleVar,
        workflow_id: options.workflow_id,
        task_queue: options.task_queue,
        parent_close_policy: options.parent_close_policy ?? "TERMINATE",
      }, pushedCall);
    };
    if (temporalImportedCallMatches(pushedCall, this.temporalApi.executeChild, "executeChild", this.temporalApi)) {
      const childDefinition = literalIdentifierOrString(
        pushedCall.arguments[0],
        "executeChild workflow",
      );
      const options = pushedCall.arguments[1]
        ? compileTemporalChildOptions(pushedCall.arguments[1], "executeChild")
        : { input: { kind: "literal", value: null } };
      return pushHandleToArray(childDefinition, options);
    }
    if (temporalImportedCallMatches(pushedCall, this.temporalApi.startChild, "startChild", this.temporalApi)) {
      const childDefinition = literalIdentifierOrString(
        pushedCall.arguments[0],
        "startChild workflow",
      );
      const options = pushedCall.arguments[1]
        ? compileTemporalChildOptions(pushedCall.arguments[1], "startChild")
        : { input: { kind: "literal", value: null } };
      return pushHandleToArray(childDefinition, options);
    }
    return null;
  }

  lowerStatementOrBlock(statement, nextState, breakTarget, continueTarget, errorTarget, returnTarget = null) {
    return ts.isBlock(statement)
      ? this.lowerBlock(
          statement.statements,
          nextState,
          breakTarget,
          continueTarget,
          errorTarget,
          returnTarget,
        )
      : this.lowerStatement(
          statement,
          nextState,
          breakTarget,
          continueTarget,
          errorTarget,
          returnTarget,
        );
  }

  lowerForInitializer(initializer, nextState) {
    if (ts.isVariableDeclarationList(initializer)) {
      const actions = initializer.declarations.map((declaration) => ({
        target: declaration.name.getText(),
        expr: declaration.initializer ? compileExpression(declaration.initializer) : { kind: "literal", value: null },
      }));
      return this.addState("assign", { type: "assign", actions, next: nextState }, initializer);
    }
    return this.lowerSyntheticExpression(initializer, nextState);
  }

  lowerWrappedAwaitExpression(expression, targetVar, nextState, errorTarget) {
    const wrappedAwait = extractWrappedAwaitExpression(expression);
    if (!wrappedAwait) {
      return null;
    }
    const tempVar = this.nextId("await_value", wrappedAwait.awaitExpression);
    const assignState = this.addState("assign", {
      type: "assign",
      actions: [
        {
          target: targetVar,
          expr: compileExpression(wrappedAwait.rebuild(ts.factory.createIdentifier(tempVar))),
        },
      ],
      next: nextState,
    }, expression);
    return this.lowerAwait(wrappedAwait.awaitExpression, tempVar, assignState, errorTarget);
  }

  lowerSyntheticExpression(expression, nextState) {
    if (
      ts.isBinaryExpression(expression) &&
      (
        expression.operatorToken.kind === ts.SyntaxKind.EqualsToken ||
        expression.operatorToken.kind === ts.SyntaxKind.PlusEqualsToken
      )
    ) {
      return this.addState("assign", {
        type: "assign",
        actions: [
          compileAssignmentAction(
            expression,
            expression.left,
            expression.operatorToken.kind,
            expression.right,
          ),
        ],
        next: nextState,
      }, expression);
    }
    if (ts.isPostfixUnaryExpression(expression) && ts.isIdentifier(expression.operand)) {
      const op = expression.operator === ts.SyntaxKind.PlusPlusToken ? "add" : "subtract";
      return this.addState("assign", {
        type: "assign",
        actions: [
          {
            target: expression.operand.text,
            expr: {
              kind: "binary",
              op,
              left: { kind: "identifier", name: expression.operand.text },
              right: { kind: "literal", value: 1 },
            },
          },
        ],
        next: nextState,
      }, expression);
    }
    throw compilerError(`unsupported for-loop update expression: ${expression.getText()}`, expression);
  }

  parseTemporalCancellationScopeCall(call) {
    if (!ts.isPropertyAccessExpression(call.expression)) {
      return null;
    }
    if (
      !temporalImportedReferenceMatches(
        call.expression.expression,
        this.temporalApi.cancellationScope,
        "CancellationScope",
        this.temporalApi,
      )
    ) {
      return null;
    }
    const method = call.expression.name.text;
    if (method !== "cancellable" && method !== "nonCancellable") {
      return null;
    }
    if (call.arguments.length !== 1) {
      throw compilerError(`CancellationScope.${method} requires exactly one inline function`, call);
    }
    const handler = call.arguments[0];
    if (!ts.isArrowFunction(handler) && !ts.isFunctionExpression(handler)) {
      throw compilerError(`CancellationScope.${method} requires an inline function`, handler);
    }
    if (!handler.modifiers?.some((modifier) => modifier.kind === ts.SyntaxKind.AsyncKeyword)) {
      throw compilerError(`CancellationScope.${method} currently requires an async inline function`, handler);
    }
    if (handler.parameters.length !== 0) {
      throw compilerError(`CancellationScope.${method} handlers must not declare parameters`, handler);
    }
    return { method, handler };
  }

  lowerTemporalCancellationScope(scopeCall, targetVar, nextState, errorTarget, originNode) {
    const statements = ts.isBlock(scopeCall.handler.body)
      ? scopeCall.handler.body.statements
      : [
          ts.setTextRange(
            ts.factory.createReturnStatement(
              ts.isAwaitExpression(scopeCall.handler.body)
                ? scopeCall.handler.body
                : ts.setTextRange(
                    ts.factory.createAwaitExpression(scopeCall.handler.body),
                    scopeCall.handler.body,
                  ),
            ),
            scopeCall.handler.body,
          ),
        ];
    const shielded = scopeCall.method === "nonCancellable";
    if (shielded) {
      this.nonCancellableDepth = (this.nonCancellableDepth ?? 0) + 1;
    }
    try {
      const fallthroughState = targetVar
        ? this.addState("assign", {
            type: "assign",
            actions: [{ target: targetVar, expr: { kind: "literal", value: null } }],
            next: nextState,
          }, originNode)
        : nextState;
      return this.lowerBlock(
        statements,
        fallthroughState,
        null,
        null,
        errorTarget,
        { next: nextState, targetVar: targetVar ?? null },
      );
    } finally {
      if (shielded) {
        this.nonCancellableDepth -= 1;
      }
    }
  }

  lowerAsyncLocalHelperCall(helperDeclaration, call, targetVar, nextState, errorTarget, originNode) {
    const helperName =
      ts.isFunctionDeclaration(helperDeclaration) && helperDeclaration.name
        ? helperDeclaration.name.text
        : originNode.getText();
    const renames = new Map();
    for (const name of collectAsyncHelperBindingNames(helperDeclaration)) {
      renames.set(name, this.nextId(`__helper_${name}`, originNode));
    }
    const statements = prepareAsyncHelperStatements(helperDeclaration, renames);
    const helperBlock = ts.factory.createBlock(statements, true);
    this.discoverHandleDeclarations(helperBlock);

    const parameterActions = [];
    const parameters = helperDeclaration.parameters ?? [];
    for (let index = 0; index < parameters.length; index += 1) {
      const parameter = parameters[index];
      if (parameter.dotDotDotToken) {
        throw compilerError(`async helper ${helperName} does not support rest parameters`, parameter);
      }
      if (!ts.isIdentifier(parameter.name)) {
        throw compilerError(`async helper ${helperName} parameters must be plain identifiers`, parameter.name);
      }
      const target = renames.get(parameter.name.text) ?? parameter.name.text;
      const provided = call.arguments[index];
      parameterActions.push({
        target,
        expr: provided
          ? compileExpression(provided)
          : parameter.initializer
            ? compileExpression(parameter.initializer)
            : { kind: "literal", value: null },
      });
    }
    if (call.arguments.length > parameters.length) {
      throw compilerError(
        `async helper ${helperName} received too many arguments for the current subset`,
        call.arguments[parameters.length],
      );
    }

    const helperResultVar = targetVar ?? this.nextId("__helper_result", originNode);
    const fallthroughState = targetVar
      ? this.addState("assign", {
          type: "assign",
          actions: [{ target: helperResultVar, expr: { kind: "literal", value: null } }],
          next: nextState,
        }, originNode)
      : nextState;
    const bodyStart = this.lowerBlock(
      statements,
      fallthroughState,
      null,
      null,
      errorTarget,
      { next: nextState, targetVar: helperResultVar },
    );
    if (parameterActions.length === 0) {
      return bodyStart;
    }
    return this.addState("assign", {
      type: "assign",
      actions: parameterActions,
      next: bodyStart,
    }, originNode);
  }

  lowerAwait(awaitExpression, targetVar, nextState, errorTarget) {
    if (this.resolveTemporalCancellationRequestedAwait(awaitExpression.expression)) {
      return this.addState("wait_cancel", {
        type: "wait_for_event",
        event_type: "__workflow_cancellation_requested",
        next: nextState,
        output_var: targetVar ?? undefined,
      }, awaitExpression);
    }
    if (!ts.isCallExpression(awaitExpression.expression)) {
      throw compilerError(
        `non-deterministic await detected; all async operations must go through ctx.* methods`,
        awaitExpression,
      );
    }
    const scopeCall = this.parseTemporalCancellationScopeCall(awaitExpression.expression);
    if (scopeCall) {
      return this.lowerTemporalCancellationScope(
        scopeCall,
        targetVar,
        nextState,
        errorTarget,
        awaitExpression,
      );
    }
    const call = awaitExpression.expression;
    const asyncLocalHelper = this.resolveAsyncLocalHelperCall(call);
    if (asyncLocalHelper) {
      return this.lowerAsyncLocalHelperCall(
        asyncLocalHelper,
        call,
        targetVar,
        nextState,
        errorTarget,
        awaitExpression,
      );
    }
    const localChildPromiseAll = this.resolveLocalChildPromiseAll(call);
    if (localChildPromiseAll) {
      return this.lowerLocalChildPromiseAll(
        localChildPromiseAll.arrayVar,
        targetVar,
        nextState,
        errorTarget,
        awaitExpression,
      );
    }
    const mappedChildPromiseAll = this.resolveMappedChildPromiseAll(call);
    if (mappedChildPromiseAll) {
      return this.lowerMappedChildPromiseAll(
        mappedChildPromiseAll,
        targetVar,
        nextState,
        errorTarget,
        awaitExpression,
      );
    }
    if (
      ts.isPropertyAccessExpression(call.expression) &&
      call.expression.name.text === "result" &&
      ts.isIdentifier(call.expression.expression)
    ) {
      const handleName = call.expression.expression.text;
      if (this.childHandleVars.has(handleName)) {
        return this.addState("wait_child", {
          type: "wait_for_child",
          child_ref_var: handleName,
          next: nextState,
          output_var: targetVar ?? undefined,
          on_error: errorTarget ?? undefined,
        }, awaitExpression);
      }
      if (this.bulkHandleVars.has(handleName)) {
        return this.addState("wait_bulk", {
          type: "wait_for_bulk_activity",
          bulk_ref_var: handleName,
          next: nextState,
          output_var: targetVar ?? undefined,
          on_error: errorTarget ?? undefined,
        }, awaitExpression);
      }
      throw compilerError(`unknown handle ${handleName}.result()`, awaitExpression);
    }
    if (
      ts.isPropertyAccessExpression(call.expression) &&
      ts.isIdentifier(call.expression.expression) &&
      this.childHandleVars.has(call.expression.expression.text)
    ) {
      const handleName = call.expression.expression.text;
      const method = call.expression.name.text;
      const continueState = targetVar
        ? this.addState("assign", {
            type: "assign",
            actions: [{ target: targetVar, expr: { kind: "literal", value: null } }],
            next: nextState,
          }, awaitExpression)
        : nextState;
      if (method === "signal") {
        if (call.arguments.length < 1 || call.arguments.length > 2) {
          throw compilerError(`childHandle.signal requires a signal name and optional payload`, call);
        }
        return this.addState("signal_child", {
          type: "signal_child",
          child_ref_var: handleName,
          signal_name: this.resolveTemporalSignalName(
            call.arguments[0],
            "childHandle.signal signal name",
          ),
          payload: call.arguments[1]
            ? compileExpression(call.arguments[1])
            : { kind: "literal", value: null },
          next: continueState,
        }, awaitExpression);
      }
      if (method === "cancel") {
        if (call.arguments.length > 1) {
          throw compilerError(`childHandle.cancel accepts at most one optional reason`, call);
        }
        return this.addState("cancel_child", {
          type: "cancel_child",
          child_ref_var: handleName,
          reason: call.arguments[0] ? compileExpression(call.arguments[0]) : undefined,
          next: continueState,
        }, awaitExpression);
      }
    }
    if (
      ts.isPropertyAccessExpression(call.expression) &&
      ts.isIdentifier(call.expression.expression) &&
      this.externalHandleVars.has(call.expression.expression.text)
    ) {
      const handleName = call.expression.expression.text;
      const handle = this.externalHandleVars.get(handleName);
      const method = call.expression.name.text;
      const continueState = targetVar
        ? this.addState("assign", {
            type: "assign",
            actions: [{ target: targetVar, expr: { kind: "literal", value: null } }],
            next: nextState,
          }, awaitExpression)
        : nextState;
      if (method === "signal") {
        if (call.arguments.length < 1 || call.arguments.length > 2) {
          throw compilerError(`externalWorkflowHandle.signal requires a signal name and optional payload`, call);
        }
        return this.addState("signal_external", {
          type: "signal_external",
          target_instance_id: handle.targetInstanceId,
          target_run_id: handle.targetRunId ?? undefined,
          signal_name: this.resolveTemporalSignalName(
            call.arguments[0],
            "externalWorkflowHandle.signal signal name",
          ),
          payload: call.arguments[1]
            ? compileExpression(call.arguments[1])
            : { kind: "literal", value: null },
          next: continueState,
        }, awaitExpression);
      }
      if (method === "cancel") {
        if (call.arguments.length > 1) {
          throw compilerError(`externalWorkflowHandle.cancel accepts at most one optional reason`, call);
        }
        return this.addState("cancel_external", {
          type: "cancel_external",
          target_instance_id: handle.targetInstanceId,
          target_run_id: handle.targetRunId ?? undefined,
          reason: call.arguments[0] ? compileExpression(call.arguments[0]) : undefined,
          next: continueState,
        }, awaitExpression);
      }
    }
    const temporalFanout = this.resolveTemporalPromiseFanout(call);
    if (temporalFanout) {
      const fanoutRef = this.nextId("fanout_handle", awaitExpression);
      const waitState = this.addState("wait_all", {
        type: "wait_for_all_activities",
        fanout_ref_var: fanoutRef,
        next: nextState,
        output_var: targetVar ?? undefined,
        on_error: errorTarget ?? undefined,
      }, awaitExpression);
      return this.addState("fanout", {
        type: "fan_out",
        activity_type: temporalFanout.activityType,
        items: temporalFanout.itemsExpr,
        next: waitState,
        handle_var: fanoutRef,
        task_queue: temporalFanout.options.task_queue,
        reducer: temporalFanout.reducer,
        retry: temporalFanout.options.retry,
        schedule_to_start_timeout_ms: temporalFanout.options.schedule_to_start_timeout_ms,
        schedule_to_close_timeout_ms: temporalFanout.options.schedule_to_close_timeout_ms,
        start_to_close_timeout_ms: temporalFanout.options.start_to_close_timeout_ms,
        heartbeat_timeout_ms: temporalFanout.options.heartbeat_timeout_ms,
      }, awaitExpression);
    }
    if (!ts.isPropertyAccessExpression(call.expression) || !isCtxReceiver(call.expression.expression)) {
      const temporalActivity = this.resolveTemporalActivityCall(call.expression);
      if (temporalActivity) {
        return this.addState("step", {
          type: "step",
          handler: temporalActivity.activityType,
          input: compileCallArgumentsAsInput(call.arguments),
          next: nextState,
          task_queue: temporalActivity.options.task_queue,
          retry: temporalActivity.options.retry,
          schedule_to_start_timeout_ms: temporalActivity.options.schedule_to_start_timeout_ms,
          schedule_to_close_timeout_ms: temporalActivity.options.schedule_to_close_timeout_ms,
          start_to_close_timeout_ms: temporalActivity.options.start_to_close_timeout_ms,
          heartbeat_timeout_ms: temporalActivity.options.heartbeat_timeout_ms,
          output_var: targetVar ?? undefined,
          on_error: errorTarget ?? undefined,
        }, awaitExpression);
      }
      const dynamicTemporalActivity = this.resolveTemporalDynamicActivityCall(call.expression);
      if (dynamicTemporalActivity) {
        return this.addState("dynamic_step", {
          type: "dynamic_step",
          descriptor: {
            kind: "object",
            fields: {
              __kind: { kind: "literal", value: "activity_descriptor" },
              activity_type: dynamicTemporalActivity.activityTypeExpr,
              input: compileCallArgumentsAsInput(call.arguments),
              ...(dynamicTemporalActivity.options.task_queue
                ? { task_queue: dynamicTemporalActivity.options.task_queue }
                : {}),
              ...(dynamicTemporalActivity.options.retry
                ? { retry: jsonValueToExpression(dynamicTemporalActivity.options.retry) }
                : {}),
              ...(dynamicTemporalActivity.options.schedule_to_start_timeout_ms != null
                ? {
                    schedule_to_start_timeout_ms: {
                      kind: "literal",
                      value: dynamicTemporalActivity.options.schedule_to_start_timeout_ms,
                    },
                  }
                : {}),
              ...(dynamicTemporalActivity.options.schedule_to_close_timeout_ms != null
                ? {
                    schedule_to_close_timeout_ms: {
                      kind: "literal",
                      value: dynamicTemporalActivity.options.schedule_to_close_timeout_ms,
                    },
                  }
                : {}),
              ...(dynamicTemporalActivity.options.start_to_close_timeout_ms != null
                ? {
                    start_to_close_timeout_ms: {
                      kind: "literal",
                      value: dynamicTemporalActivity.options.start_to_close_timeout_ms,
                    },
                  }
                : {}),
              ...(dynamicTemporalActivity.options.heartbeat_timeout_ms != null
                ? {
                    heartbeat_timeout_ms: {
                      kind: "literal",
                      value: dynamicTemporalActivity.options.heartbeat_timeout_ms,
                    },
                  }
                : {}),
            },
          },
          next: nextState,
          output_var: targetVar ?? undefined,
          on_error: errorTarget ?? undefined,
        }, awaitExpression);
      }
      if (temporalImportedCallMatches(call, this.temporalApi.sleep, "sleep", this.temporalApi)) {
        return this.addState("wait_timer", {
          type: "wait_for_timer",
          ...compileTemporalTimer(call.arguments[0], "sleep duration"),
          next: nextState,
        }, awaitExpression);
      }
      if (
        temporalImportedCallMatches(call, this.temporalApi.continueAsNew, "continueAsNew", this.temporalApi)
      ) {
        return this.addState("continue_as_new", {
          type: "continue_as_new",
          input: compileCallArgumentsAsInput(call.arguments),
        }, awaitExpression);
      }
      if (temporalImportedCallMatches(call, this.temporalApi.condition, "condition", this.temporalApi)) {
        return this.lowerTemporalCondition(call, targetVar, nextState);
      }
      if (temporalImportedCallMatches(call, this.temporalApi.executeChild, "executeChild", this.temporalApi)) {
        const childDefinition = call.arguments[0];
        const options = call.arguments[1]
          ? compileTemporalChildOptions(call.arguments[1], "executeChild")
          : { input: { kind: "literal", value: null } };
        const handleVar = this.nextId("child_handle", awaitExpression);
        const waitState = this.addState("wait_child", {
          type: "wait_for_child",
          child_ref_var: handleVar,
          next: nextState,
          output_var: targetVar ?? undefined,
          on_error: errorTarget ?? undefined,
        }, awaitExpression);
        return this.addState("start_child", {
          type: "start_child",
          child_definition_id: literalIdentifierOrString(childDefinition, "executeChild workflow"),
          input: options.input,
          next: waitState,
          handle_var: handleVar,
          workflow_id: options.workflow_id,
          task_queue: options.task_queue,
          parent_close_policy: options.parent_close_policy ?? "TERMINATE",
        }, awaitExpression);
      }
      if (temporalImportedCallMatches(call, this.temporalApi.startChild, "startChild", this.temporalApi)) {
        const childDefinition = call.arguments[0];
        const options = call.arguments[1]
          ? compileTemporalChildOptions(call.arguments[1], "startChild")
          : { input: { kind: "literal", value: null } };
        if (targetVar) {
          this.childHandleVars.add(targetVar);
        }
        return this.addState("start_child", {
          type: "start_child",
          child_definition_id: literalIdentifierOrString(childDefinition, "startChild workflow"),
          input: options.input,
          next: nextState,
          handle_var: targetVar ?? undefined,
          workflow_id: options.workflow_id,
          task_queue: options.task_queue,
          parent_close_policy: options.parent_close_policy ?? "TERMINATE",
        }, awaitExpression);
      }
      if (
        call.arguments.length === 0 &&
        (
          ts.isIdentifier(call.expression) ||
          ts.isPropertyAccessExpression(call.expression) ||
          ts.isElementAccessExpression(call.expression)
        )
      ) {
        return this.addState("dynamic_step", {
          type: "dynamic_step",
          descriptor: compileExpression(call.expression),
          next: nextState,
          output_var: targetVar ?? undefined,
          on_error: errorTarget ?? undefined,
        }, awaitExpression);
      }
      throw compilerError(
        `non-deterministic await detected; only await ctx.* calls or supported Temporal workflow primitives are allowed in workflows`,
        call.expression,
      );
    }
    const method = call.expression.name.text;
    if (method === "waitForSignal") {
      return this.addState("wait_signal", {
        type: "wait_for_event",
        event_type: literalString(call.arguments[0], "ctx.waitForSignal signal name"),
        next: nextState,
        output_var: targetVar ?? undefined,
      }, awaitExpression);
    }
    if (method === "sleep") {
      return this.addState("wait_timer", {
        type: "wait_for_timer",
        ...compileTemporalTimer(call.arguments[0], "ctx.sleep duration"),
        next: nextState,
      }, awaitExpression);
    }
    if (method === "activity") {
      return this.addState("step", {
        type: "step",
        handler: literalString(call.arguments[0], "ctx.activity handler"),
        input: call.arguments[1] ? compileExpression(call.arguments[1]) : { kind: "literal", value: null },
        next: nextState,
        output_var: targetVar ?? undefined,
        on_error: errorTarget ?? undefined,
      }, awaitExpression);
    }
    if (method === "startChild") {
      const options = call.arguments[2] ? compileChildOptions(call.arguments[2]) : {};
      if (targetVar) this.childHandleVars.add(targetVar);
      return this.addState("start_child", {
        type: "start_child",
        child_definition_id: literalString(call.arguments[0], "ctx.startChild workflow name"),
        input: call.arguments[1] ? compileExpression(call.arguments[1]) : { kind: "literal", value: null },
        next: nextState,
        handle_var: targetVar ?? undefined,
        workflow_id: options.workflow_id,
        task_queue: options.task_queue,
        parent_close_policy: options.parent_close_policy ?? "TERMINATE",
      }, awaitExpression);
    }
    if (method === "bulkActivity") {
      const options = call.arguments[2] ? compileBulkOptions(call.arguments[2]) : {};
      if (!targetVar) {
        throw compilerError(`await ctx.bulkActivity(...) must be assigned to a handle variable`, awaitExpression);
      }
      this.bulkHandleVars.add(targetVar);
      return this.addState("start_bulk", {
        type: "start_bulk_activity",
        activity_type: literalString(call.arguments[0], "ctx.bulkActivity handler"),
        items: call.arguments[1] ? compileExpression(call.arguments[1]) : { kind: "literal", value: [] },
        next: nextState,
        handle_var: targetVar,
        task_queue: options.task_queue,
        execution_policy: options.execution_policy,
        reducer: options.reducer,
        chunk_size: options.chunk_size,
        retry: options.retry,
      }, awaitExpression);
    }
    if (method === "sideEffect") {
      throw compilerError(
        `await ctx.sideEffect() is no longer supported for host-backed work; use ctx.activity() instead`,
        call,
      );
    }
    if (method === "httpRequest") {
      const requestArg = call.arguments[0];
      if (!requestArg || !ts.isObjectLiteralExpression(requestArg)) {
        throw compilerError(`ctx.httpRequest requires a static request object`, call);
      }
      const config = compileHttpConfig(requestArg);
      return this.addState("http_step", {
        type: "step",
        handler: "http.request",
        input: call.arguments[1] ? compileExpression(call.arguments[1]) : { kind: "literal", value: null },
        next: nextState,
        output_var: targetVar ?? undefined,
        config,
        on_error: errorTarget ?? undefined,
      }, awaitExpression);
    }
    throw compilerError(`unsupported ctx method ctx.${method}`, call.expression.name);
  }

  resolveLocalChildPromiseAll(call) {
    if (
      !ts.isPropertyAccessExpression(call.expression) ||
      !ts.isIdentifier(call.expression.expression) ||
      call.expression.expression.text !== "Promise" ||
      call.expression.name.text !== "all" ||
      call.arguments.length !== 1 ||
      !ts.isIdentifier(call.arguments[0])
    ) {
      return null;
    }
    const arrayVar = call.arguments[0].text;
    return this.childPromiseArrayVars.has(arrayVar) ? { arrayVar } : null;
  }

  resolveMappedChildPromiseAll(call) {
    if (
      !ts.isPropertyAccessExpression(call.expression) ||
      !ts.isIdentifier(call.expression.expression) ||
      call.expression.expression.text !== "Promise" ||
      call.expression.name.text !== "all" ||
      call.arguments.length !== 1
    ) {
      return null;
    }
    const mapCall = call.arguments[0];
    if (
      !ts.isCallExpression(mapCall) ||
      !ts.isPropertyAccessExpression(mapCall.expression) ||
      mapCall.expression.name.text !== "map" ||
      mapCall.arguments.length !== 1
    ) {
      return null;
    }
    const mapper = mapCall.arguments[0];
    if (!ts.isArrowFunction(mapper) && !ts.isFunctionExpression(mapper)) {
      return null;
    }
    if (mapper.parameters.length !== 1 || !ts.isIdentifier(mapper.parameters[0].name)) {
      return null;
    }
    const mappedExpression = functionBodyExpression(mapper.body);
    if (!mappedExpression || !ts.isCallExpression(mappedExpression)) {
      return null;
    }
    if (
      !temporalImportedCallMatches(
        mappedExpression,
        this.temporalApi.executeChild,
        "executeChild",
        this.temporalApi,
      )
    ) {
      return null;
    }
    const childDefinition = mappedExpression.arguments[0];
    if (!childDefinition) {
      return null;
    }
    const options = mappedExpression.arguments[1]
      ? compileTemporalChildOptions(mappedExpression.arguments[1], "executeChild")
      : { input: { kind: "literal", value: null } };
    return {
      itemsExpr: compileExpression(mapCall.expression.expression),
      mapperVar: mapper.parameters[0].name.text,
      childDefinition: literalIdentifierOrString(childDefinition, "executeChild workflow"),
      options,
    };
  }

  lowerLocalChildPromiseAll(arrayVar, targetVar, nextState, errorTarget, node) {
    const indexVar = this.nextId("child_join_index", node);
    const handleVar = this.nextId("child_join_handle", node);
    const resultVar = this.nextId("child_join_result", node);
    this.childHandleVars.add(handleVar);
    const choiceState = this.nextId("child_join_choice", node);
    const incrementState = this.addState("assign", {
      type: "assign",
      actions: [{ target: indexVar, expr: {
        kind: "binary",
        op: "add",
        left: { kind: "identifier", name: indexVar },
        right: { kind: "literal", value: 1 },
      } }],
      next: choiceState,
    }, node);
    const collectState = targetVar
      ? this.addState("assign", {
          type: "assign",
          actions: [{
            target: targetVar,
            expr: {
              kind: "call",
              callee: "__builtin_array_append",
              args: [
                { kind: "identifier", name: targetVar },
                { kind: "identifier", name: resultVar },
              ],
            },
          }],
          next: incrementState,
        }, node)
      : incrementState;
    const waitState = this.addState("wait_child", {
      type: "wait_for_child",
      child_ref_var: handleVar,
      next: collectState,
      output_var: targetVar ? resultVar : undefined,
      on_error: errorTarget ?? undefined,
    }, node);
    const assignHandleState = this.addState("assign", {
      type: "assign",
      actions: [{
        target: handleVar,
        expr: {
          kind: "index",
          object: { kind: "identifier", name: arrayVar },
          index: { kind: "identifier", name: indexVar },
        },
      }],
      next: waitState,
    }, node);
    this.states[choiceState] = {
      type: "choice",
      condition: {
        kind: "binary",
        op: "less_than",
        left: { kind: "identifier", name: indexVar },
        right: {
          kind: "member",
          object: { kind: "identifier", name: arrayVar },
          property: "length",
        },
      },
      then_next: assignHandleState,
      else_next: nextState,
    };
    this.sourceMap[choiceState] = sourceLocation(node);
    const initActions = [{ target: indexVar, expr: { kind: "literal", value: 0 } }];
    if (targetVar) {
      initActions.push({ target: targetVar, expr: { kind: "literal", value: [] } });
    }
    return this.addState("assign", {
      type: "assign",
      actions: initActions,
      next: choiceState,
    }, node);
  }

  lowerMappedChildPromiseAll(mappedChildPromiseAll, targetVar, nextState, errorTarget, node) {
    const handleArrayVar = this.nextId("child_handle_array", node);
    const indexVar = this.nextId("child_start_index", node);
    const handleVar = this.nextId("child_start_handle", node);
    this.childHandleVars.add(handleVar);
    this.childPromiseArrayVars.add(handleArrayVar);
    const joinState = this.lowerLocalChildPromiseAll(handleArrayVar, targetVar, nextState, errorTarget, node);
    const choiceState = this.nextId("child_start_choice", node);
    const incrementState = this.addState("assign", {
      type: "assign",
      actions: [{
        target: indexVar,
        expr: {
          kind: "binary",
          op: "add",
          left: { kind: "identifier", name: indexVar },
          right: { kind: "literal", value: 1 },
        },
      }],
      next: choiceState,
    }, node);
    const appendState = this.addState("assign", {
      type: "assign",
      actions: [{
        target: handleArrayVar,
        expr: {
          kind: "call",
          callee: "__builtin_array_append",
          args: [
            { kind: "identifier", name: handleArrayVar },
            { kind: "identifier", name: handleVar },
          ],
        },
      }],
      next: incrementState,
    }, node);
    const startState = this.addState("start_child", {
      type: "start_child",
      child_definition_id: mappedChildPromiseAll.childDefinition,
      input: mappedChildPromiseAll.options.input,
      next: appendState,
      handle_var: handleVar,
      workflow_id: mappedChildPromiseAll.options.workflow_id,
      task_queue: mappedChildPromiseAll.options.task_queue,
      parent_close_policy: mappedChildPromiseAll.options.parent_close_policy ?? "TERMINATE",
    }, node);
    const assignMapperState = this.addState("assign", {
      type: "assign",
      actions: [{
        target: mappedChildPromiseAll.mapperVar,
        expr: {
          kind: "index",
          object: mappedChildPromiseAll.itemsExpr,
          index: { kind: "identifier", name: indexVar },
        },
      }],
      next: startState,
    }, node);
    this.states[choiceState] = {
      type: "choice",
      condition: {
        kind: "binary",
        op: "less_than",
        left: { kind: "identifier", name: indexVar },
        right: {
          kind: "member",
          object: mappedChildPromiseAll.itemsExpr,
          property: "length",
        },
      },
      then_next: assignMapperState,
      else_next: joinState,
    };
    this.sourceMap[choiceState] = sourceLocation(node);
    return this.addState("assign", {
      type: "assign",
      actions: [
        { target: indexVar, expr: { kind: "literal", value: 0 } },
        { target: handleArrayVar, expr: { kind: "literal", value: [] } },
      ],
      next: choiceState,
    }, node);
  }

  lowerTerminalCall(callExpression) {
    if (!ts.isPropertyAccessExpression(callExpression.expression) || !isCtxReceiver(callExpression.expression.expression)) {
      throw compilerError(`terminal return must be ctx.complete/fail/continueAsNew`, callExpression);
    }
    const method = callExpression.expression.name.text;
    if (method === "complete") {
      return this.addState("complete", {
        type: "succeed",
        output: callExpression.arguments[0] ? compileExpression(callExpression.arguments[0]) : undefined,
      }, callExpression);
    }
    if (method === "fail") {
      return this.addState("fail", {
        type: "fail",
        reason: callExpression.arguments[0] ? compileExpression(callExpression.arguments[0]) : undefined,
      }, callExpression);
    }
    if (method === "continueAsNew") {
      return this.addState("continue_as_new", {
        type: "continue_as_new",
        input: callExpression.arguments[0] ? compileExpression(callExpression.arguments[0]) : undefined,
      }, callExpression);
    }
    throw compilerError(`unsupported terminal call ctx.${method}`, callExpression.expression.name);
  }

  registerQueryHandler(callExpression) {
    const queryName = literalString(callExpression.arguments[0], "ctx.query name");
    const handler = callExpression.arguments[1];
    if (!handler || (!ts.isArrowFunction(handler) && !ts.isFunctionExpression(handler))) {
      throw compilerError(`ctx.query requires an inline function handler`, callExpression);
    }
    if (handler.modifiers?.some((modifier) => modifier.kind === ts.SyntaxKind.AsyncKeyword)) {
      throw compilerError(`ctx.query handlers must not be async`, handler);
    }
    const body = compilePureHandlerExpression(handler, "ctx.query");
    this.queries[queryName] = {
      arg_name: handler.parameters[0] ? handler.parameters[0].name.getText() : undefined,
      expr: body,
    };
  }

  registerUpdateHandler(callExpression) {
    const updateName = literalString(callExpression.arguments[0], "ctx.update name");
    const handler = callExpression.arguments[1];
    if (!handler || (!ts.isArrowFunction(handler) && !ts.isFunctionExpression(handler))) {
      throw compilerError(`ctx.update requires an inline function handler`, callExpression);
    }
    const bodyBlock = blockFromInlineHandlerBody(handler.body);
    const lowered = new WorkflowLowerer(
      this.definitionId,
      this.version,
      { body: bodyBlock },
      this.temporalApi,
      `update_${shortHash(updateName)}_`,
      this.typeChecker,
    );
    const terminalFail = lowered.addState("fail_terminal", {
      type: "fail",
      reason: { kind: "literal", value: `update ${updateName} terminated without explicit completion` },
    });
    const initialState = lowered.lowerBlock(bodyBlock.statements, terminalFail, null, null, null);
    this.updates[updateName] = {
      arg_name: handler.parameters[0] ? handler.parameters[0].name.getText() : undefined,
      initial_state: initialState,
      states: lowered.states,
    };
    for (const stateId of lowered.nonCancellableStates) {
      this.nonCancellableStates.add(stateId);
    }
    Object.assign(this.sourceMap, lowered.sourceMap);
  }
}

function literalString(expression, label) {
  if (!expression || (!ts.isStringLiteral(expression) && !ts.isNoSubstitutionTemplateLiteral(expression))) {
    throw compilerError(`${label} must be a string literal`, expression);
  }
  return expression.text;
}

function literalNumber(expression, label) {
  if (!expression || !ts.isNumericLiteral(expression)) {
    throw compilerError(`${label} must be a numeric literal`, expression);
  }
  const value = Number(expression.text);
  if (!Number.isInteger(value) || value < 0) {
    throw compilerError(`${label} must be a non-negative integer literal`, expression);
  }
  return value;
}

function compileHttpConfig(objectLiteral) {
  const config = { kind: "http_request", method: "GET", url: "", headers: {}, body_from_input: true };
  for (const property of objectLiteral.properties) {
    if (!ts.isPropertyAssignment(property)) {
      throw compilerError(`unsupported httpRequest property ${property.getText()}`, property);
    }
    const key = property.name.getText().replaceAll(/^["']|["']$/g, "");
    if (key === "method") config.method = literalString(property.initializer, "httpRequest.method");
    else if (key === "url") config.url = literalString(property.initializer, "httpRequest.url");
    else if (key === "bodyFromInput") config.body_from_input = property.initializer.kind === ts.SyntaxKind.TrueKeyword;
    else if (key === "headers") {
      if (!ts.isObjectLiteralExpression(property.initializer)) {
        throw compilerError(`httpRequest.headers must be a static object`, property.initializer);
      }
      for (const header of property.initializer.properties) {
        if (!ts.isPropertyAssignment(header)) {
          throw compilerError(`unsupported header config ${header.getText()}`, header);
        }
        config.headers[header.name.getText().replaceAll(/^["']|["']$/g, "")] = literalString(
          header.initializer,
          "httpRequest header",
        );
      }
    }
  }
  return config;
}

function compileEffectOptions(expression) {
  if (!ts.isObjectLiteralExpression(expression)) {
    throw compilerError(`ctx.sideEffect options must be a static object`, expression);
  }
  const options = {};
  for (const property of expression.properties) {
    if (!ts.isPropertyAssignment(property)) {
      throw compilerError(`unsupported sideEffect option ${property.getText()}`, property);
    }
    const key = property.name.getText().replaceAll(/^["']|["']$/g, "");
    if (key === "timeout") options.timeout = literalString(property.initializer, "ctx.sideEffect timeout");
    else throw compilerError(`unsupported sideEffect option ${key}`, property);
  }
  return options;
}

function compilePureHandlerExpression(handler, label) {
  if (ts.isBlock(handler.body)) {
    if (
      handler.body.statements.length !== 1 ||
      !ts.isReturnStatement(handler.body.statements[0]) ||
      !handler.body.statements[0].expression
    ) {
      throw compilerError(`${label} handlers must be a single return expression`, handler.body);
    }
    return compileExpression(handler.body.statements[0].expression);
  }
  return compileExpression(handler.body);
}

function compileChildOptions(expression) {
  if (!ts.isObjectLiteralExpression(expression)) {
    throw compilerError(`ctx.startChild options must be a static object`, expression);
  }
  const options = {};
  for (const property of expression.properties) {
    if (!ts.isPropertyAssignment(property)) {
      throw compilerError(`unsupported startChild option ${property.getText()}`, property);
    }
    const key = property.name.getText().replaceAll(/^["']|["']$/g, "");
    if (key === "workflowId") options.workflow_id = compileExpression(property.initializer);
    else if (key === "taskQueue") options.task_queue = compileExpression(property.initializer);
    else if (key === "parentClosePolicy") {
      options.parent_close_policy = literalTemporalEnumMember(
        property.initializer,
        "ctx.startChild parentClosePolicy",
        currentTemporalApi.parentClosePolicy,
        "ParentClosePolicy",
      );
    } else {
      throw compilerError(`unsupported startChild option ${key}`, property);
    }
  }
  return options;
}

function literalIdentifierOrString(expression, label) {
  if (ts.isIdentifier(expression)) {
    return expression.text;
  }
  return literalString(expression, label);
}

function compileTemporalChildOptions(expression, label) {
  if (!ts.isObjectLiteralExpression(expression)) {
    throw compilerError(`${label} options must be a static object`, expression);
  }
  const compiled = {
    input: { kind: "literal", value: null },
  };
  for (const property of expression.properties) {
    if (!ts.isPropertyAssignment(property)) {
      throw compilerError(`unsupported ${label} option ${property.getText()}`, property);
    }
    const key = property.name.getText().replaceAll(/^["']|["']$/g, "");
    if (key === "args") {
      if (!ts.isArrayLiteralExpression(property.initializer)) {
        throw compilerError(`${label} args must be a static array literal`, property.initializer);
      }
      if (property.initializer.elements.length === 0) {
        compiled.input = { kind: "literal", value: null };
      } else if (property.initializer.elements.length === 1) {
        compiled.input = compileExpression(property.initializer.elements[0]);
      } else {
        compiled.input = {
          kind: "array",
          items: property.initializer.elements.map(compileExpression),
        };
      }
      continue;
    }
    if (key === "workflowId") {
      compiled.workflow_id = compileExpression(property.initializer);
      continue;
    }
    if (key === "taskQueue") {
      compiled.task_queue = compileExpression(property.initializer);
      continue;
    }
    if (key === "parentClosePolicy") {
      compiled.parent_close_policy = literalTemporalEnumMember(
        property.initializer,
        `${label} parentClosePolicy`,
        currentTemporalApi.parentClosePolicy,
        "ParentClosePolicy",
      );
      continue;
    }
    throw compilerError(`unsupported ${label} option ${key}`, property);
  }
  return compiled;
}

function compileCallArgumentsAsInput(argumentsList) {
  if (argumentsList.length === 0) {
    return { kind: "literal", value: null };
  }
  if (argumentsList.length === 1 && ts.isSpreadElement(argumentsList[0])) {
    return compileExpression(argumentsList[0].expression);
  }
  if (argumentsList.some((argument) => ts.isSpreadElement(argument))) {
    throw compilerError(
      `spread call arguments are only supported as a single ...args array`,
      argumentsList[0],
    );
  }
  if (argumentsList.length === 1) {
    return compileExpression(argumentsList[0]);
  }
  return {
    kind: "array",
    items: argumentsList.map(compileExpression),
  };
}

function compileBulkOptions(expression) {
  if (!ts.isObjectLiteralExpression(expression)) {
    throw compilerError(`ctx.bulkActivity options must be a static object`, expression);
  }
  const options = {};
  for (const property of expression.properties) {
    if (!ts.isPropertyAssignment(property)) {
      throw compilerError(`unsupported bulkActivity option ${property.getText()}`, property);
    }
    const key = property.name.getText().replaceAll(/^["']|["']$/g, "");
    if (key === "taskQueue") {
      options.task_queue = compileExpression(property.initializer);
      continue;
    }
    if (key === "chunkSize") {
      if (!ts.isNumericLiteral(property.initializer)) {
        throw compilerError(
          `ctx.bulkActivity chunkSize must be a numeric literal`,
          property.initializer,
        );
      }
      const chunkSize = Number(property.initializer.text);
      if (!Number.isInteger(chunkSize) || chunkSize < 1 || chunkSize > MAX_BULK_CHUNK_SIZE) {
        throw compilerError(
          `ctx.bulkActivity chunkSize must be between 1 and ${MAX_BULK_CHUNK_SIZE}`,
          property.initializer,
        );
      }
      options.chunk_size = chunkSize;
      continue;
    }
    if (key === "backend") {
      throw compilerError(
        `ctx.bulkActivity backend selection is server-controlled; remove the backend option`,
        property,
      );
    }
    if (key === "execution") {
      const execution = literalString(property.initializer, "ctx.bulkActivity execution");
      if (execution !== "default" && execution !== "eager") {
        throw compilerError(
          `ctx.bulkActivity execution must be "default" or "eager"`,
          property.initializer,
        );
      }
      options.execution_policy = execution;
      continue;
    }
    if (key === "reducer") {
      const reducer = literalString(property.initializer, "ctx.bulkActivity reducer");
      if (
        reducer !== "all_succeeded"
        && reducer !== "all_settled"
        && reducer !== "count"
        && reducer !== "sum"
        && reducer !== "min"
        && reducer !== "max"
        && reducer !== "avg"
        && reducer !== "histogram"
        && reducer !== "sample_errors"
        && reducer !== "collect_results"
      ) {
        throw compilerError(
          `ctx.bulkActivity reducer must be "all_succeeded", "all_settled", "count", "sum", "min", "max", "avg", "histogram", "sample_errors", or "collect_results"`,
          property.initializer,
        );
      }
      options.reducer = reducer;
      continue;
    }
    if (key === "retry") {
      if (!ts.isObjectLiteralExpression(property.initializer)) {
        throw compilerError(`ctx.bulkActivity retry must be a static object`, property.initializer);
      }
      const retry = {};
      for (const retryProperty of property.initializer.properties) {
        if (!ts.isPropertyAssignment(retryProperty)) {
          throw compilerError(
            `unsupported bulkActivity retry option ${retryProperty.getText()}`,
            retryProperty,
          );
        }
        const retryKey = retryProperty.name.getText().replaceAll(/^["']|["']$/g, "");
        if (retryKey === "maxAttempts") {
          if (!ts.isNumericLiteral(retryProperty.initializer)) {
            throw compilerError(
              `ctx.bulkActivity retry.maxAttempts must be a numeric literal`,
              retryProperty.initializer,
            );
          }
          retry.max_attempts = Number(retryProperty.initializer.text);
        } else if (retryKey === "delay") {
          retry.delay = literalString(
            retryProperty.initializer,
            "ctx.bulkActivity retry.delay",
          );
        } else {
          throw compilerError(`unsupported bulkActivity retry option ${retryKey}`, retryProperty);
        }
      }
      options.retry = retry;
      continue;
    }
    throw compilerError(`unsupported bulkActivity option ${key}`, property);
  }
  return options;
}

function hashArtifact(artifact) {
  const clone = { ...artifact, artifact_hash: "" };
  return crypto
    .createHash("sha256")
    .update(JSON.stringify(canonicalize(clone)))
    .digest("hex");
}

function canonicalize(value) {
  if (Array.isArray(value)) {
    return value.map(canonicalize);
  }
  if (value && typeof value === "object") {
    return Object.keys(value)
      .sort()
      .reduce((acc, key) => {
        acc[key] = canonicalize(value[key]);
        return acc;
      }, {});
  }
  return value;
}

function walkExpression(expression, visitor) {
  if (!expression || typeof expression !== "object") {
    return;
  }
  visitor(expression);
  switch (expression.kind) {
    case "member":
      walkExpression(expression.object, visitor);
      break;
    case "index":
      walkExpression(expression.object, visitor);
      walkExpression(expression.index, visitor);
      break;
    case "binary":
    case "logical":
      walkExpression(expression.left, visitor);
      walkExpression(expression.right, visitor);
      break;
    case "unary":
      walkExpression(expression.expr, visitor);
      break;
    case "conditional":
      walkExpression(expression.condition, visitor);
      walkExpression(expression.then_expr, visitor);
      walkExpression(expression.else_expr, visitor);
      break;
    case "array":
      expression.items.forEach((item) => walkExpression(item, visitor));
      break;
    case "array_find":
      walkExpression(expression.array, visitor);
      walkExpression(expression.predicate, visitor);
      break;
    case "array_map":
      walkExpression(expression.array, visitor);
      walkExpression(expression.expr, visitor);
      break;
    case "array_reduce":
      walkExpression(expression.array, visitor);
      walkExpression(expression.initial, visitor);
      walkExpression(expression.expr, visitor);
      break;
    case "object":
      Object.values(expression.fields).forEach((field) => walkExpression(field, visitor));
      break;
    case "call":
      expression.args.forEach((arg) => walkExpression(arg, visitor));
      break;
    case "side_effect":
      walkExpression(expression.expr, visitor);
      break;
    default:
      break;
  }
}

function validateArtifactCalls(artifact) {
  const helperNames = new Set(Object.keys(artifact.helpers));
  const builtinCallNames = new Set([
    "__temporal_is_cancellation",
    "__temporal_is_activity_failure",
    "__temporal_is_application_failure",
    "__builtin_array_join",
    "__builtin_array_fill",
    "__builtin_array_append",
    "__builtin_array_prepend",
    "__builtin_array_shift_head",
    "__builtin_array_shift_tail",
    "__builtin_array_map_number",
    "__builtin_array_sort_default",
    "__builtin_array_sort_numeric_asc",
    "__builtin_math_floor",
    "__builtin_math_min",
    "__builtin_object_omit",
    "__builtin_object_set",
    "__builtin_object_keys",
    "__builtin_map_get",
    "__builtin_map_has",
    "__builtin_map_set",
    "__builtin_binding_map_set_and_null",
    "__builtin_date_new",
    "__builtin_date_value_of",
    "__builtin_date_get_date",
    "__builtin_date_set_date",
    "__builtin_date_set_hours",
    "__builtin_search_attributes_upsert",
    "__builtin_string",
    "__builtin_string_to_lowercase",
    "__builtin_string_to_uppercase",
  ]);
  const validateExpression = (expression) => {
    walkExpression(expression, (node) => {
      if (node.kind === "call" && !helperNames.has(node.callee) && !builtinCallNames.has(node.callee)) {
        throw compilerError(
          `unsupported function call ${node.callee}; only imported or local pure helpers are allowed`,
        );
      }
    });
  };
  const validateHelperStatements = (statements) => {
    for (const statement of statements ?? []) {
      if (statement.type === "assign") {
        validateExpression(statement.expr);
        continue;
      }
      if (statement.type === "assign_index") {
        validateExpression(statement.index);
        validateExpression(statement.expr);
        continue;
      }
      if (statement.type === "for_range") {
        validateExpression(statement.start);
        validateExpression(statement.end);
        validateHelperStatements(statement.body);
      }
    }
  };

  Object.values(artifact.helpers).forEach((helper) => {
    validateHelperStatements(helper.statements);
    validateExpression(helper.body);
  });
  Object.values(artifact.workflow.states).forEach((state) => validateCompiledState(state, validateExpression));
  Object.values(artifact.signals ?? {}).forEach((signal) => {
    Object.values(signal.states).forEach((state) => validateCompiledState(state, validateExpression));
  });
  Object.values(artifact.updates ?? {}).forEach((update) => {
    Object.values(update.states).forEach((state) => validateCompiledState(state, validateExpression));
  });
  Object.values(artifact.queries ?? {}).forEach((query) => validateExpression(query.expr));
}

function validateCompiledState(state, validateExpression) {
  switch (state.type) {
    case "assign":
      state.actions.forEach((action) => validateExpression(action.expr));
      break;
    case "choice":
      validateExpression(state.condition);
      break;
    case "wait_for_condition":
      validateExpression(state.condition);
      break;
    case "wait_for_timer":
      if (state.timer_expr) validateExpression(state.timer_expr);
      break;
    case "step":
      validateExpression(state.input);
      if (state.task_queue) validateExpression(state.task_queue);
      break;
    case "dynamic_step":
      validateExpression(state.descriptor);
      break;
    case "fan_out":
      validateExpression(state.items);
      if (state.task_queue) validateExpression(state.task_queue);
      break;
    case "start_child":
      validateExpression(state.input);
      if (state.workflow_id) validateExpression(state.workflow_id);
      if (state.task_queue) validateExpression(state.task_queue);
      break;
    case "start_bulk_activity":
      validateExpression(state.items);
      if (state.task_queue) validateExpression(state.task_queue);
      break;
    case "succeed":
      if (state.output) validateExpression(state.output);
      break;
    case "fail":
      if (state.reason) validateExpression(state.reason);
      break;
    case "continue_as_new":
      if (state.input) validateExpression(state.input);
      break;
    default:
      break;
  }
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  const program = createProgram(args.entry);
  const workflow = findExportedFunction(program, args.exportName);
  const temporalApi = collectTemporalWorkflowApi(workflow.getSourceFile());
  currentTemporalApi = temporalApi;
  currentStaticTopLevelInitializers = collectStaticTopLevelInitializers(workflow.getSourceFile());
  const lowerer = new WorkflowLowerer(
    args.definitionId,
    args.version,
    workflow,
    temporalApi,
    "",
    program.getTypeChecker(),
  );
  currentTemporalActivityBindings = lowerer.temporalActivityBindings;
  const helpers = injectTemporalBuiltinHelpers(buildHelperRegistry(program, workflow), temporalApi);
  const { initialState, states, sourceMap, queries, signals, updates, params, nonCancellableStates } =
    lowerer.lower();

  const artifact = {
    definition_id: args.definitionId,
    definition_version: args.version,
    compiler_version: "0.1.0",
    source_language: "typescript",
    entrypoint: {
      module: path.relative(process.cwd(), args.entry),
      export: args.exportName,
    },
    source_files: getResolvedSources(program).map((sourceFile) =>
      path.relative(process.cwd(), sourceFile.fileName),
    ),
    source_map: sourceMap,
    helpers,
    queries,
    signals,
    updates,
    workflow: {
      initial_state: initialState,
      states,
      params,
      non_cancellable_states: nonCancellableStates,
    },
    artifact_hash: "",
  };
  validateArtifactCalls(artifact);
  artifact.artifact_hash = hashArtifact(artifact);

  const encoded = JSON.stringify(artifact, null, 2);
  if (args.out) {
    await fs.writeFile(args.out, encoded);
  } else {
    process.stdout.write(`${encoded}\n`);
  }
}

main().catch((error) => {
  console.error(error instanceof CompilerError ? error.message : error);
  process.exit(1);
});
