import crypto from "node:crypto";
import fs from "node:fs/promises";
import path from "node:path";
import process from "node:process";
import ts from "typescript";

function usage() {
  console.error(
    "usage: node sdk/typescript-compiler/stream-compiler.mjs --entry <file> --export <name> --definition-id <id> --version <n> [--out <file>]",
  );
  process.exit(1);
}

function parseArgs(argv) {
  const args = {};
  for (let i = 0; i < argv.length; i += 1) {
    const key = argv[i];
    const value = argv[i + 1];
    if (!key?.startsWith("--") || value == null) {
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
  }
}

function compilerError(message, node = null) {
  return new CompilerError(message, node);
}

function formatNodeLocation(node) {
  const sourceFile = node.getSourceFile?.() ?? node.parent?.getSourceFile?.();
  if (!sourceFile) {
    return { file: "<generated>", line: 1, column: 1 };
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

function createProgram(entry) {
  return ts.createProgram([entry], {
    target: ts.ScriptTarget.ES2020,
    module: ts.ModuleKind.NodeNext,
    moduleResolution: ts.ModuleResolutionKind.NodeNext,
    allowJs: false,
    checkJs: false,
    strict: true,
    skipLibCheck: true,
    esModuleInterop: true,
    resolveJsonModule: true,
  });
}

function unwrapExpression(node) {
  let current = node;
  while (
    current
    && (
      ts.isParenthesizedExpression(current)
      || ts.isAsExpression(current)
      || ts.isTypeAssertionExpression(current)
      || ts.isNonNullExpression(current)
      || ts.isSatisfiesExpression?.(current)
    )
  ) {
    current = current.expression;
  }
  return current;
}

function findExportedStreamJob(sourceFile, exportName) {
  for (const statement of sourceFile.statements) {
    if (!ts.isVariableStatement(statement)) {
      continue;
    }
    const isExported = statement.modifiers?.some((modifier) => modifier.kind === ts.SyntaxKind.ExportKeyword);
    if (!isExported) {
      continue;
    }
    for (const declaration of statement.declarationList.declarations) {
      if (!ts.isIdentifier(declaration.name) || declaration.name.text !== exportName) {
        continue;
      }
      return declaration.initializer ? unwrapExpression(declaration.initializer) : null;
    }
  }
  return null;
}

function literalString(expression, label) {
  const node = unwrapExpression(expression);
  if (ts.isStringLiteral(node) || ts.isNoSubstitutionTemplateLiteral(node)) {
    return node.text;
  }
  throw compilerError(`${label} must be a string literal`, expression);
}

function literalNumber(expression, label) {
  const node = unwrapExpression(expression);
  if (ts.isNumericLiteral(node)) {
    return Number(node.text);
  }
  throw compilerError(`${label} must be a numeric literal`, expression);
}

function compileStaticValue(expression) {
  const node = unwrapExpression(expression);
  if (ts.isStringLiteral(node) || ts.isNoSubstitutionTemplateLiteral(node)) {
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
  if (ts.isArrayLiteralExpression(node)) {
    return node.elements.map((element) => compileStaticValue(element));
  }
  if (ts.isObjectLiteralExpression(node)) {
    const value = {};
    for (const property of node.properties) {
      if (!ts.isPropertyAssignment(property)) {
        throw compilerError(`unsupported object property ${property.getText()}`, property);
      }
      const key = propertyNameText(property.name).replaceAll(/^["']|["']$/g, "");
      value[key] = compileStaticValue(property.initializer);
    }
    return value;
  }
  throw compilerError(`expression must be statically serializable`, expression);
}

function propertyNameText(name) {
  if (ts.isIdentifier(name) || ts.isStringLiteral(name) || ts.isNumericLiteral(name)) {
    return name.text;
  }
  return name.getText();
}

function objectPropertyByName(objectLiteral, name) {
  return objectLiteral.properties.find(
    (property) =>
      ts.isPropertyAssignment(property)
      && propertyNameText(property.name).replaceAll(/^["']|["']$/g, "") === name,
  );
}

function requiredObjectLiteral(expression, label) {
  const node = unwrapExpression(expression);
  if (!ts.isObjectLiteralExpression(node)) {
    throw compilerError(`${label} must be a static object literal`, expression);
  }
  return node;
}

function requiredArrayLiteral(expression, label) {
  const node = unwrapExpression(expression);
  if (!ts.isArrayLiteralExpression(node)) {
    throw compilerError(`${label} must be a static array literal`, expression);
  }
  return node;
}

function compileSource(expression) {
  const source = requiredObjectLiteral(expression, "streamJob source");
  return {
    kind: literalString(
      objectPropertyByName(source, "kind")?.initializer ?? source,
      "streamJob source.kind",
    ),
    name: objectPropertyByName(source, "name")
      ? literalString(objectPropertyByName(source, "name").initializer, "streamJob source.name")
      : null,
    binding: objectPropertyByName(source, "binding")
      ? literalString(objectPropertyByName(source, "binding").initializer, "streamJob source.binding")
      : null,
    config: objectPropertyByName(source, "config")
      ? compileStaticValue(objectPropertyByName(source, "config").initializer)
      : null,
  };
}

function compileOperator(expression) {
  const object = requiredObjectLiteral(expression, "streamJob operator");
  return {
    kind: literalString(
      objectPropertyByName(object, "kind")?.initializer ?? object,
      "streamJob operator.kind",
    ),
    operator_id: objectPropertyByName(object, "operatorId")
      ? literalString(objectPropertyByName(object, "operatorId").initializer, "streamJob operator.operatorId")
      : null,
    name: objectPropertyByName(object, "name")
      ? literalString(objectPropertyByName(object, "name").initializer, "streamJob operator.name")
      : null,
    inputs: objectPropertyByName(object, "inputs")
      ? requiredArrayLiteral(objectPropertyByName(object, "inputs").initializer, "streamJob operator.inputs")
          .elements.map((element) => literalString(element, "streamJob operator.inputs[]"))
      : [],
    outputs: objectPropertyByName(object, "outputs")
      ? requiredArrayLiteral(objectPropertyByName(object, "outputs").initializer, "streamJob operator.outputs")
          .elements.map((element) => literalString(element, "streamJob operator.outputs[]"))
      : [],
    state_ids: objectPropertyByName(object, "stateIds")
      ? requiredArrayLiteral(objectPropertyByName(object, "stateIds").initializer, "streamJob operator.stateIds")
          .elements.map((element) => literalString(element, "streamJob operator.stateIds[]"))
      : [],
    config: objectPropertyByName(object, "config")
      ? compileStaticValue(objectPropertyByName(object, "config").initializer)
      : null,
  };
}

function compileState(expression) {
  const object = requiredObjectLiteral(expression, "streamJob state");
  return {
    id: literalString(objectPropertyByName(object, "id")?.initializer ?? object, "streamJob state.id"),
    kind: literalString(objectPropertyByName(object, "kind")?.initializer ?? object, "streamJob state.kind"),
    key_fields: objectPropertyByName(object, "keyFields")
      ? requiredArrayLiteral(objectPropertyByName(object, "keyFields").initializer, "streamJob state.keyFields")
          .elements.map((element) => literalString(element, "streamJob state.keyFields[]"))
      : [],
    value_fields: objectPropertyByName(object, "valueFields")
      ? requiredArrayLiteral(objectPropertyByName(object, "valueFields").initializer, "streamJob state.valueFields")
          .elements.map((element) => literalString(element, "streamJob state.valueFields[]"))
      : [],
    retention_seconds: objectPropertyByName(object, "retentionSeconds")
      ? literalNumber(objectPropertyByName(object, "retentionSeconds").initializer, "streamJob state.retentionSeconds")
      : null,
    config: objectPropertyByName(object, "config")
      ? compileStaticValue(objectPropertyByName(object, "config").initializer)
      : null,
  };
}

function compileView(expression) {
  const object = requiredObjectLiteral(expression, "streamJob view");
  return {
    name: literalString(objectPropertyByName(object, "name")?.initializer ?? object, "streamJob view.name"),
    consistency: literalString(
      objectPropertyByName(object, "consistency")?.initializer ?? object,
      "streamJob view.consistency",
    ),
    query_mode: literalString(
      objectPropertyByName(object, "queryMode")?.initializer ?? object,
      "streamJob view.queryMode",
    ),
    view_id: objectPropertyByName(object, "viewId")
      ? literalString(objectPropertyByName(object, "viewId").initializer, "streamJob view.viewId")
      : null,
    key_field: objectPropertyByName(object, "keyField")
      ? literalString(objectPropertyByName(object, "keyField").initializer, "streamJob view.keyField")
      : null,
    value_fields: objectPropertyByName(object, "valueFields")
      ? requiredArrayLiteral(
          objectPropertyByName(object, "valueFields").initializer,
          "streamJob view.valueFields",
        ).elements.map((element) => literalString(element, "streamJob view.valueFields[]"))
      : [],
    supported_consistencies: objectPropertyByName(object, "supportedConsistencies")
      ? requiredArrayLiteral(
          objectPropertyByName(object, "supportedConsistencies").initializer,
          "streamJob view.supportedConsistencies",
        ).elements.map((element) => literalString(element, "streamJob view.supportedConsistencies[]"))
      : [],
    retention_seconds: objectPropertyByName(object, "retentionSeconds")
      ? literalNumber(
          objectPropertyByName(object, "retentionSeconds").initializer,
          "streamJob view.retentionSeconds",
        )
      : null,
  };
}

function compileQuery(expression) {
  const object = requiredObjectLiteral(expression, "streamJob query");
  return {
    name: literalString(
      objectPropertyByName(object, "name")?.initializer ?? object,
      "streamJob query.name",
    ),
    view_name: literalString(
      objectPropertyByName(object, "viewName")?.initializer ?? object,
      "streamJob query.viewName",
    ),
    consistency: literalString(
      objectPropertyByName(object, "consistency")?.initializer ?? object,
      "streamJob query.consistency",
    ),
    query_id: objectPropertyByName(object, "queryId")
      ? literalString(objectPropertyByName(object, "queryId").initializer, "streamJob query.queryId")
      : null,
    arg_fields: objectPropertyByName(object, "argFields")
      ? requiredArrayLiteral(objectPropertyByName(object, "argFields").initializer, "streamJob query.argFields")
          .elements.map((element) => literalString(element, "streamJob query.argFields[]"))
      : [],
  };
}

function compileJob(jobLiteral) {
  const sourceProperty = objectPropertyByName(jobLiteral, "source");
  if (!sourceProperty) {
    throw compilerError(`streamJob definition requires a source`, jobLiteral);
  }
  const statesProperty = objectPropertyByName(jobLiteral, "states");
  const operatorsProperty = objectPropertyByName(jobLiteral, "operators");
  const viewsProperty = objectPropertyByName(jobLiteral, "views");
  const queriesProperty = objectPropertyByName(jobLiteral, "queries");

  return {
    name: literalString(
      objectPropertyByName(jobLiteral, "name")?.initializer ?? jobLiteral,
      "streamJob name",
    ),
    runtime: literalString(
      objectPropertyByName(jobLiteral, "runtime")?.initializer ?? jobLiteral,
      "streamJob runtime",
    ),
    source: compileSource(sourceProperty.initializer),
    key_by: objectPropertyByName(jobLiteral, "keyBy")
      ? literalString(objectPropertyByName(jobLiteral, "keyBy").initializer, "streamJob keyBy")
      : null,
    states: statesProperty
      ? requiredArrayLiteral(statesProperty.initializer, "streamJob states").elements.map(
          (element) => compileState(element),
        )
      : [],
    operators: operatorsProperty
      ? requiredArrayLiteral(operatorsProperty.initializer, "streamJob operators").elements.map(
          (element) => compileOperator(element),
        )
      : [],
    checkpoint_policy: objectPropertyByName(jobLiteral, "checkpointPolicy")
      ? compileStaticValue(objectPropertyByName(jobLiteral, "checkpointPolicy").initializer)
      : null,
    views: viewsProperty
      ? requiredArrayLiteral(viewsProperty.initializer, "streamJob views").elements.map((element) =>
          compileView(element),
        )
      : [],
    queries: queriesProperty
      ? requiredArrayLiteral(queriesProperty.initializer, "streamJob queries").elements.map(
          (element) => compileQuery(element),
        )
      : [],
    classification: objectPropertyByName(jobLiteral, "classification")
      ? literalString(objectPropertyByName(jobLiteral, "classification").initializer, "streamJob classification")
      : null,
    metadata: objectPropertyByName(jobLiteral, "metadata")
      ? compileStaticValue(objectPropertyByName(jobLiteral, "metadata").initializer)
      : null,
  };
}

function expectObject(value, label) {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    throw new CompilerError(`${label} must be a static object`);
  }
  return value;
}

function validateKeyedRollupJob(job) {
  if (job.name !== "keyed-rollup") {
    throw new CompilerError(`keyed_rollup runtime requires job name "keyed-rollup"`);
  }
  if (job.source.kind !== "bounded_input") {
    throw new CompilerError(`keyed_rollup runtime requires source.kind "bounded_input"`);
  }
  if (job.key_by !== "accountId") {
    throw new CompilerError(`keyed_rollup runtime requires keyBy "accountId"`);
  }
  if (job.operators.length !== 2) {
    throw new CompilerError(`keyed_rollup runtime requires exactly 2 operators`);
  }

  const reduce = job.operators[0];
  if (reduce.kind !== "reduce") {
    throw new CompilerError(`keyed_rollup runtime requires operator[0] to be reduce`);
  }
  const reduceConfig = expectObject(reduce.config, "keyed_rollup reduce.config");
  if (reduceConfig.reducer !== "sum") {
    throw new CompilerError(`keyed_rollup runtime requires reduce.config.reducer "sum"`);
  }
  if (typeof reduceConfig.valueField !== "string" || reduceConfig.valueField.length === 0) {
    throw new CompilerError(`keyed_rollup runtime requires reduce.config.valueField`);
  }

  const checkpoint = job.operators[1];
  if (checkpoint.kind !== "emit_checkpoint") {
    throw new CompilerError(`keyed_rollup runtime requires operator[1] to be emit_checkpoint`);
  }
  if (typeof checkpoint.name !== "string" || checkpoint.name.length === 0) {
    throw new CompilerError(`keyed_rollup runtime requires emit_checkpoint.name`);
  }
  const checkpointConfig = expectObject(checkpoint.config, "keyed_rollup emit_checkpoint.config");
  if (!Number.isInteger(checkpointConfig.sequence)) {
    throw new CompilerError(`keyed_rollup runtime requires emit_checkpoint.config.sequence`);
  }

  if (job.views.length !== 1) {
    throw new CompilerError(`keyed_rollup runtime requires exactly 1 materialized view`);
  }
  const view = job.views[0];
  if (view.consistency !== "strong" || view.query_mode !== "by_key" || view.key_field !== "accountId") {
    throw new CompilerError(`keyed_rollup runtime requires a strong by_key accountTotals-style view`);
  }

  if (job.queries.length !== 1) {
    throw new CompilerError(`keyed_rollup runtime requires exactly 1 query`);
  }
  const query = job.queries[0];
  if (query.view_name !== view.name || query.consistency !== "strong") {
    throw new CompilerError(`keyed_rollup runtime requires a strong query bound to the declared view`);
  }

  const checkpointPolicy = expectObject(job.checkpoint_policy, "keyed_rollup checkpointPolicy");
  if (checkpointPolicy.kind !== "named_checkpoints" || !Array.isArray(checkpointPolicy.checkpoints)) {
    throw new CompilerError(`keyed_rollup runtime requires a named checkpoint policy`);
  }
  if (checkpointPolicy.checkpoints.length !== 1) {
    throw new CompilerError(`keyed_rollup runtime requires exactly 1 checkpoint declaration`);
  }
  const declaredCheckpoint = expectObject(
    checkpointPolicy.checkpoints[0],
    "keyed_rollup checkpointPolicy.checkpoints[0]",
  );
  if (declaredCheckpoint.name !== checkpoint.name) {
    throw new CompilerError(`emit_checkpoint.name must match checkpointPolicy.checkpoints[0].name`);
  }
  if (declaredCheckpoint.delivery !== "workflow_awaitable") {
    throw new CompilerError(`keyed_rollup checkpoint delivery must be workflow_awaitable`);
  }
  if (declaredCheckpoint.sequence !== checkpointConfig.sequence) {
    throw new CompilerError(`emit_checkpoint.config.sequence must match checkpoint policy sequence`);
  }
}

function validateAggregateV2Job(job) {
  if (job.source.kind !== "bounded_input" && job.source.kind !== "topic") {
    throw new CompilerError(`aggregate_v2 runtime requires source.kind "bounded_input" or "topic"`);
  }
  if (typeof job.key_by !== "string" || job.key_by.length === 0) {
    throw new CompilerError(`aggregate_v2 runtime requires keyBy`);
  }
  if (!Array.isArray(job.operators) || job.operators.length === 0) {
    throw new CompilerError(`aggregate_v2 runtime requires at least 1 operator`);
  }
  if (!Array.isArray(job.views) || job.views.length === 0) {
    throw new CompilerError(`aggregate_v2 runtime requires at least 1 materialized view`);
  }
  const materializeCount = job.operators.filter((operator) => operator.kind === "materialize").length;
  if (materializeCount === 0) {
    throw new CompilerError(`aggregate_v2 runtime requires at least 1 materialize operator`);
  }
  const allowedKinds = new Set([
    "map",
    "filter",
    "route",
    "window",
    "reduce",
    "aggregate",
    "dedupe",
    "materialize",
    "emit_checkpoint",
    "signal_workflow",
    "sink",
  ]);
  for (const operator of job.operators) {
    if (!allowedKinds.has(operator.kind)) {
      throw new CompilerError(`aggregate_v2 runtime does not support operator kind "${operator.kind}"`);
    }
  }
  const reducerKinds = new Set(["count", "sum", "min", "max", "avg", "histogram", "threshold"]);
  for (const operator of job.operators) {
    if (operator.kind !== "reduce" && operator.kind !== "aggregate") {
      continue;
    }
    const config = expectObject(operator.config, `aggregate_v2 ${operator.kind}.config`);
    if (!reducerKinds.has(config.reducer)) {
      throw new CompilerError(`aggregate_v2 ${operator.kind}.config.reducer must be a built-in reducer`);
    }
  }
  for (const operator of job.operators) {
    if (operator.kind !== "window") {
      continue;
    }
    const config = expectObject(operator.config, "aggregate_v2 window.config");
    if (config.mode !== "tumbling") {
      throw new CompilerError(`aggregate_v2 window.config.mode must be "tumbling"`);
    }
    if (typeof config.size !== "string" || config.size.length === 0) {
      throw new CompilerError(`aggregate_v2 window.config.size must be present`);
    }
  }
  const supportedQueryModes = new Set(["by_key", "prefix_scan"]);
  for (const view of job.views) {
    if (!supportedQueryModes.has(view.query_mode)) {
      throw new CompilerError(`aggregate_v2 views must use queryMode "by_key" or "prefix_scan"`);
    }
  }
  const viewNames = new Set(job.views.map((view) => view.name));
  for (const query of job.queries) {
    if (!viewNames.has(query.view_name)) {
      throw new CompilerError(`aggregate_v2 queries must target a declared view`);
    }
  }
}

function validateJob(job) {
  if (job.runtime === "keyed_rollup") {
    validateKeyedRollupJob(job);
    return;
  }
  if (job.runtime === "aggregate_v2") {
    validateAggregateV2Job(job);
  }
}

function runtimeContractForJob(job) {
  if (job.runtime === "aggregate_v2") {
    return "streams_kernel_v2";
  }
  return "streams_kernel_v1";
}

function buildSourceMap(jobLiteral) {
  const sourceMap = { job: sourceLocation(jobLiteral) };
  for (const [propertyName, pathName] of [
    ["source", "source"],
    ["states", "states"],
    ["checkpointPolicy", "checkpoint_policy"],
    ["views", "views"],
    ["queries", "queries"],
    ["operators", "operators"],
  ]) {
    const property = objectPropertyByName(jobLiteral, propertyName);
    if (property) {
      sourceMap[pathName] = sourceLocation(property.initializer);
    }
  }
  return sourceMap;
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

function hashArtifact(artifact) {
  const clone = { ...artifact, artifact_hash: "" };
  return crypto
    .createHash("sha256")
    .update(JSON.stringify(canonicalize(clone)))
    .digest("hex");
}

function getResolvedSources(program) {
  return program.getSourceFiles().filter((sourceFile) => {
    if (sourceFile.isDeclarationFile) {
      return false;
    }
    if (sourceFile.fileName.includes("/node_modules/")) {
      return false;
    }
    return sourceFile.fileName.startsWith(process.cwd());
  });
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  const program = createProgram(args.entry);
  const sourceFile = program.getSourceFile(args.entry);
  if (!sourceFile) {
    throw compilerError(`entry source ${args.entry} not found`);
  }
  const exported = findExportedStreamJob(sourceFile, args.exportName);
  if (!exported) {
    throw compilerError(`export ${args.exportName} was not found`, sourceFile);
  }
  if (!ts.isCallExpression(exported) || exported.arguments.length !== 1) {
    throw compilerError(`export ${args.exportName} must be initialized with streamJob({...})`, exported);
  }
  const callee = unwrapExpression(exported.expression);
  if (!ts.isIdentifier(callee) || callee.text !== "streamJob") {
    throw compilerError(`export ${args.exportName} must call streamJob({...})`, exported.expression);
  }

  const jobLiteral = requiredObjectLiteral(exported.arguments[0], "streamJob definition");
  const artifact = {
    definition_id: args.definitionId,
    definition_version: args.version,
    compiler_version: "0.1.0",
    source_language: "typescript",
    entrypoint: {
      module: path.relative(process.cwd(), args.entry),
      export: args.exportName,
    },
    source_files: getResolvedSources(program).map((resolved) =>
      path.relative(process.cwd(), resolved.fileName),
    ),
    source_map: buildSourceMap(jobLiteral),
    job: compileJob(jobLiteral),
    artifact_hash: "",
  };
  validateJob(artifact.job);
  artifact.runtime_contract = runtimeContractForJob(artifact.job);
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
