import crypto from "node:crypto";
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
  const sourceFile = node.getSourceFile();
  const { line, character } = sourceFile.getLineAndCharacterOfPosition(node.getStart(sourceFile));
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
  const parts = [node.kind, node.getText(node.getSourceFile())];
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
  if (ts.isIdentifier(expression)) {
    return expression.text;
  }
  if (ts.isPropertyAccessExpression(expression) || ts.isElementAccessExpression(expression)) {
    return rootIdentifierName(expression.expression);
  }
  return null;
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
    proxyActivities: new Set(),
    sleep: new Set(),
    continueAsNew: new Set(),
    condition: new Set(),
    executeChild: new Set(),
    startChild: new Set(),
    defineQuery: new Set(),
    defineUpdate: new Set(),
    defineSignal: new Set(),
    setHandler: new Set(),
  };
  for (const statement of sourceFile.statements) {
    if (!ts.isImportDeclaration(statement) || !statement.importClause) {
      continue;
    }
    if (statement.moduleSpecifier.text !== "@temporalio/workflow") {
      continue;
    }
    const namedBindings = statement.importClause.namedBindings;
    if (!namedBindings || !ts.isNamedImports(namedBindings)) {
      continue;
    }
    for (const element of namedBindings.elements) {
      const importedName = element.propertyName?.text ?? element.name.text;
      const localName = element.name.text;
      if (importedName === "proxyActivities") api.proxyActivities.add(localName);
      if (importedName === "sleep") api.sleep.add(localName);
      if (importedName === "continueAsNew") api.continueAsNew.add(localName);
      if (importedName === "condition") api.condition.add(localName);
      if (importedName === "executeChild") api.executeChild.add(localName);
      if (importedName === "startChild") api.startChild.add(localName);
      if (importedName === "defineQuery") api.defineQuery.add(localName);
      if (importedName === "defineUpdate") api.defineUpdate.add(localName);
      if (importedName === "defineSignal") api.defineSignal.add(localName);
      if (importedName === "setHandler") api.setHandler.add(localName);
    }
  }
  return api;
}

function isTemporalProxyDeclaration(declaration, temporalApi) {
  if (!declaration.initializer || !ts.isCallExpression(declaration.initializer)) {
    return false;
  }
  if (
    !ts.isIdentifier(declaration.initializer.expression) ||
    !temporalApi.proxyActivities.has(declaration.initializer.expression.text)
  ) {
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
  if (!ts.isIdentifier(declaration.initializer.expression)) {
    return null;
  }
  const callee = declaration.initializer.expression.text;
  if (temporalApi.defineQuery.has(callee)) return "query";
  if (temporalApi.defineUpdate.has(callee)) return "update";
  if (temporalApi.defineSignal.has(callee)) return "signal";
  return null;
}

function parseTemporalDurationMs(expression, label) {
  if (ts.isNumericLiteral(expression)) {
    return Number(expression.text);
  }
  if (ts.isStringLiteral(expression) || ts.isNoSubstitutionTemplateLiteral(expression)) {
    const match = /^(\d+)(ms|s|m|h)$/.exec(expression.text.trim());
    if (!match) {
      throw compilerError(`${label} must be a static duration like "500ms", "30s", "5m", or "1h"`, expression);
    }
    const value = Number(match[1]);
    const unit = match[2];
    const multiplier =
      unit === "ms" ? 1
      : unit === "s" ? 1_000
      : unit === "m" ? 60_000
      : 3_600_000;
    return value * multiplier;
  }
  throw compilerError(`${label} must be a numeric literal or static duration string`, expression);
}

function parseTemporalRetryOptions(expression) {
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
  const optionsExpression = callExpression.arguments[0];
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
    throw compilerError(`unsupported proxyActivities option ${key}`, property);
  }
  return options;
}

function isTemporalDefinitionDeclaration(declaration, temporalApi) {
  return ts.isIdentifier(declaration.name) && temporalDefinitionKind(declaration, temporalApi) != null;
}

function assertNoTopLevelSideEffects(sourceFile) {
  const temporalApi = collectTemporalWorkflowApi(sourceFile);
  for (const statement of sourceFile.statements) {
    if (
      ts.isImportDeclaration(statement) ||
      ts.isExportDeclaration(statement) ||
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

function findExportedFunction(program, exportName) {
  const checker = program.getTypeChecker();
  for (const sourceFile of getResolvedSources(program)) {
    assertNoTopLevelSideEffects(sourceFile);
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
      if (importedHelpers.has(node.expression.text)) {
        helpers.set(node.expression.text, importedHelpers.get(node.expression.text));
      }
      const symbol = checker.getSymbolAtLocation(node.expression);
      if (symbol) {
        const resolvedSymbol =
          symbol.flags & ts.SymbolFlags.Alias ? checker.getAliasedSymbol(symbol) : symbol;
        const declaration = resolvedSymbol.valueDeclaration ?? resolvedSymbol.declarations?.[0];
        if (hasCompilableHelperBody(declaration) && declaration !== workflowDeclaration) {
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
    const moduleName = statement.moduleSpecifier.text;
    if (moduleName === "@temporalio/workflow") {
      continue;
    }
    const resolved = ts.resolveModuleName(
      moduleName,
      sourceFile.fileName,
      program.getCompilerOptions(),
      ts.sys,
    ).resolvedModule;
    if (!resolved) {
      continue;
    }
    const importedSource = program.getSourceFile(resolved.resolvedFileName);
    if (!importedSource) {
      continue;
    }
    for (const binding of extractImportedBindings(statement.importClause)) {
      const declaration = findExportedFunctionDeclaration(importedSource, binding.exportName);
      if (hasCompilableHelperBody(declaration)) {
        helpers.set(binding.localName, compileHelperFunction(binding.localName, declaration));
      }
    }
  }
  return helpers;
}

function extractImportedBindings(importClause) {
  const bindings = [];
  if (importClause.name) {
    bindings.push({ localName: importClause.name.text, exportName: "default" });
  }
  if (
    importClause.namedBindings &&
    ts.isNamedImports(importClause.namedBindings)
  ) {
    for (const element of importClause.namedBindings.elements) {
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

function hasCompilableHelperBody(declaration) {
  return (
    !!declaration &&
    (ts.isFunctionDeclaration(declaration) ||
      ts.isFunctionExpression(declaration) ||
      ts.isArrowFunction(declaration)) &&
    declaration.body != null
  );
}

function compileHelperFunction(name, declaration) {
  if (declaration.modifiers?.some((modifier) => modifier.kind === ts.SyntaxKind.AsyncKeyword)) {
    throw compilerError(`helper ${name} must not be async`, declaration);
  }
  const params = declaration.parameters.map((parameter) => parameter.name.getText());
  if (ts.isBlock(declaration.body)) {
    if (
      declaration.body.statements.length !== 1 ||
      !ts.isReturnStatement(declaration.body.statements[0]) ||
      !declaration.body.statements[0].expression
    ) {
      throw compilerError(`helper ${name} must be a single return expression`, declaration.body);
    }
    return { params, body: compileExpression(declaration.body.statements[0].expression) };
  }
  return { params, body: compileExpression(declaration.body) };
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
      statement.expression.operatorToken.kind === ts.SyntaxKind.EqualsToken &&
      ts.isIdentifier(statement.expression.left)
    ) {
      actions.push({
        target: statement.expression.left.text,
        expr: compileExpression(statement.expression.right),
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
  assertAllowedRootIdentifier(expression);
  if (ts.isStringLiteral(expression) || ts.isNoSubstitutionTemplateLiteral(expression)) {
    return { kind: "literal", value: expression.text };
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
  if (ts.isPropertyAccessExpression(expression)) {
    return {
      kind: "member",
      object: compileExpression(expression.expression),
      property: expression.name.text,
    };
  }
  if (ts.isElementAccessExpression(expression)) {
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
    for (const property of expression.properties) {
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
    return { kind: "object", fields };
  }
  if (ts.isParenthesizedExpression(expression)) {
    return compileExpression(expression.expression);
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
    const logicalMap = new Map([
      [ts.SyntaxKind.AmpersandAmpersandToken, "and"],
      [ts.SyntaxKind.BarBarToken, "or"],
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
    if (ts.isPropertyAccessExpression(expression.expression)) {
      const method = expression.expression.name.text;
      if (method === "find" || method === "map") {
        const handler = expression.arguments[0];
        if (!handler) {
          throw compilerError(`${method} requires a function handler`, expression);
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
      expression.expression.expression.getText() === "ctx"
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
      throw compilerError(
        `ctx.${method} is only allowed as an awaited workflow primitive or terminal call`,
        expression,
      );
    }
    if (ts.isIdentifier(expression.expression)) {
      return {
        kind: "call",
        callee: expression.expression.text,
        args: expression.arguments.map(compileExpression),
      };
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
      proxyActivities: new Set(),
      sleep: new Set(),
      continueAsNew: new Set(),
      condition: new Set(),
      executeChild: new Set(),
      startChild: new Set(),
      defineQuery: new Set(),
      defineUpdate: new Set(),
      defineSignal: new Set(),
      setHandler: new Set(),
    },
    statePrefix = "",
  ) {
    this.definitionId = definitionId;
    this.version = version;
    this.workflowDeclaration = workflowDeclaration;
    this.temporalApi = temporalApi;
    this.statePrefix = statePrefix;
    this.states = {};
    this.sourceMap = {};
    this.syntheticCounts = new Map();
    this.queries = {};
    this.signals = {};
    this.updates = {};
    this.childHandleVars = new Set();
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
        const options = parseTemporalProxyActivityOptions(declaration.initializer);
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
        ts.isAwaitExpression(current.initializer) &&
        ts.isCallExpression(current.initializer.expression)
      ) {
        const awaitedCall = current.initializer.expression;
        if (
          ts.isPropertyAccessExpression(awaitedCall.expression) &&
          awaitedCall.expression.expression.getText() === "ctx"
        ) {
          const method = awaitedCall.expression.name.text;
          if (method === "startChild") this.childHandleVars.add(current.name.text);
          if (method === "bulkActivity") this.bulkHandleVars.add(current.name.text);
        }
        if (
          ts.isIdentifier(awaitedCall.expression) &&
          this.temporalApi.startChild.has(awaitedCall.expression.text)
        ) {
          this.childHandleVars.add(current.name.text);
        }
      }
      ts.forEachChild(current, visit);
    };
    visit(node);
  }

  registerTemporalNamedHandler(callExpression) {
    const definition = callExpression.arguments[0];
    const handler = callExpression.arguments[1];
    if (!definition || !ts.isIdentifier(definition)) {
      throw compilerError(`setHandler requires a named Temporal definition`, callExpression);
    }
    if (this.temporalDefinitions.query.has(definition.text)) {
      const queryName = this.temporalDefinitions.query.get(definition.text);
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
    if (this.temporalDefinitions.update.has(definition.text)) {
      const updateName = this.temporalDefinitions.update.get(definition.text);
      if (!handler || (!ts.isArrowFunction(handler) && !ts.isFunctionExpression(handler))) {
        throw compilerError(`Temporal update setHandler requires an inline function handler`, callExpression);
      }
      const lowered = new WorkflowLowerer(
        this.definitionId,
        this.version,
        {
          body: ts.isBlock(handler.body)
            ? handler.body
            : ts.factory.createBlock([ts.factory.createReturnStatement(handler.body)], true),
        },
        this.temporalApi,
        `update_${shortHash(updateName)}_`,
      );
      lowered.temporalDefinitions = this.temporalDefinitions;
      lowered.temporalActivityBindings = this.temporalActivityBindings;
      const bodyBlock = ts.isBlock(handler.body)
        ? handler.body
        : ts.factory.createBlock([ts.factory.createReturnStatement(handler.body)], true);
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
      return;
    }
    if (this.temporalDefinitions.signal.has(definition.text)) {
      const signalName = this.temporalDefinitions.signal.get(definition.text);
      if (!handler || (!ts.isArrowFunction(handler) && !ts.isFunctionExpression(handler))) {
        throw compilerError(`Temporal signal setHandler requires an inline function handler`, callExpression);
      }
      const lowered = new WorkflowLowerer(
        this.definitionId,
        this.version,
        {
          body: ts.isBlock(handler.body)
            ? handler.body
            : ts.factory.createBlock([ts.factory.createReturnStatement(handler.body)], true),
        },
        this.temporalApi,
        `signal_${shortHash(signalName)}_`,
      );
      lowered.temporalDefinitions = this.temporalDefinitions;
      lowered.temporalActivityBindings = this.temporalActivityBindings;
      const bodyBlock = ts.isBlock(handler.body)
        ? handler.body
        : ts.factory.createBlock([ts.factory.createReturnStatement(handler.body)], true);
      const terminalComplete = lowered.addState("signal_complete", {
        type: "succeed",
      });
      const initialState = lowered.lowerBlock(bodyBlock.statements, terminalComplete, null, null, null);
      this.signals[signalName] = {
        arg_name: handler.parameters[0] ? handler.parameters[0].name.getText() : undefined,
        initial_state: initialState,
        states: lowered.states,
      };
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
    const predicateExpr = compilePureHandlerExpression(predicate, "condition");
    if (Object.keys(this.signals).length === 0) {
      throw compilerError(
        `condition currently requires at least one registered Temporal signal handler`,
        callExpression,
      );
    }
    let continueState = nextState;
    if (targetVar) {
      continueState = this.addState("assign", {
        type: "assign",
        actions: [{ target: targetVar, expr: { kind: "literal", value: true } }],
        next: nextState,
      }, callExpression);
    }
    return this.addState("wait_condition", {
      type: "wait_for_condition",
      condition: predicateExpr,
      next: continueState,
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
    };
  }

  addState(prefix, state, node = null) {
    const id = this.nextId(prefix, node);
    this.states[id] = state;
    if (node) {
      this.sourceMap[id] = sourceLocation(node);
    }
    return id;
  }

  lowerBlock(statements, nextState, breakTarget, continueTarget, errorTarget) {
    for (const statement of statements) {
      if (
        ts.isExpressionStatement(statement) &&
        ts.isCallExpression(statement.expression) &&
        ts.isIdentifier(statement.expression.expression) &&
        this.temporalApi.setHandler.has(statement.expression.expression.text)
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
      );
    }
    return cursor;
  }

  lowerStatement(statement, nextState, breakTarget, continueTarget, errorTarget) {
    if (ts.isExpressionStatement(statement) && ts.isCallExpression(statement.expression)) {
      if (
        ts.isIdentifier(statement.expression.expression) &&
        this.temporalApi.setHandler.has(statement.expression.expression.text)
      ) {
        this.registerTemporalNamedHandler(statement.expression);
        return nextState;
      }
      if (
        ts.isPropertyAccessExpression(statement.expression.expression) &&
        statement.expression.expression.expression.getText() === "ctx"
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
      const actions = declarations.map((declaration) => {
        if (!ts.isIdentifier(declaration.name) || !declaration.initializer) {
          throw compilerError(`unsupported declaration: ${statement.getText()}`, declaration);
        }
        return {
          target: declaration.name.text,
          expr: compileExpression(declaration.initializer),
        };
      });
      return this.addState("assign", { type: "assign", actions, next: nextState }, statement);
    }

    if (ts.isExpressionStatement(statement)) {
      if (ts.isAwaitExpression(statement.expression)) {
        return this.lowerAwait(statement.expression, null, nextState, errorTarget);
      }
      if (
        ts.isBinaryExpression(statement.expression) &&
        statement.expression.operatorToken.kind === ts.SyntaxKind.EqualsToken
      ) {
        if (!ts.isIdentifier(statement.expression.left)) {
          throw compilerError(`unsupported assignment target: ${statement.getText()}`, statement);
        }
        if (ts.isAwaitExpression(statement.expression.right)) {
          return this.lowerAwait(
            statement.expression.right,
            statement.expression.left.text,
            nextState,
            errorTarget,
          );
        }
        return this.addState("assign", {
          type: "assign",
          actions: [
            {
              target: statement.expression.left.text,
              expr: compileExpression(statement.expression.right),
            },
          ],
          next: nextState,
        }, statement);
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
      );
      const elseNext = statement.elseStatement
        ? this.lowerStatementOrBlock(
            statement.elseStatement,
            nextState,
            breakTarget,
            continueTarget,
            errorTarget,
          )
        : nextState;
      return this.addState("choice", {
        type: "choice",
        condition: compileExpression(statement.expression),
        then_next: thenNext,
        else_next: elseNext,
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
        ts.isIdentifier(statement.expression.expression) &&
        this.temporalApi.continueAsNew.has(statement.expression.expression.text)
      ) {
        return this.addState("continue_as_new", {
          type: "continue_as_new",
          input: compileCallArgumentsAsInput(statement.expression.arguments),
        }, statement.expression);
      }
      if (
        ts.isPropertyAccessExpression(statement.expression.expression) &&
        statement.expression.expression.expression.getText() === "ctx"
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
        ? this.lowerBlock(statement.finallyBlock.statements, nextState, breakTarget, continueTarget, errorTarget)
        : nextState;
      const catchError = statement.catchClause
        ? {
            next: this.lowerBlock(
              statement.catchClause.block.statements,
              finallyStart,
              breakTarget,
              continueTarget,
              errorTarget,
            ),
            error_var: statement.catchClause.variableDeclaration?.name.getText() ?? "__error",
          }
        : errorTarget;
      return this.lowerStatementOrBlock(
        statement.tryBlock,
        finallyStart,
        breakTarget,
        continueTarget,
        catchError,
      );
    }

    if (ts.isBlock(statement)) {
      return this.lowerBlock(statement.statements, nextState, breakTarget, continueTarget, errorTarget);
    }

    throw compilerError(`unsupported statement: ${statement.getText()}`, statement);
  }

  lowerStatementOrBlock(statement, nextState, breakTarget, continueTarget, errorTarget) {
    return ts.isBlock(statement)
      ? this.lowerBlock(statement.statements, nextState, breakTarget, continueTarget, errorTarget)
      : this.lowerStatement(statement, nextState, breakTarget, continueTarget, errorTarget);
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

  lowerSyntheticExpression(expression, nextState) {
    if (
      ts.isBinaryExpression(expression) &&
      expression.operatorToken.kind === ts.SyntaxKind.EqualsToken &&
      ts.isIdentifier(expression.left)
    ) {
      return this.addState("assign", {
        type: "assign",
        actions: [{ target: expression.left.text, expr: compileExpression(expression.right) }],
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

  lowerAwait(awaitExpression, targetVar, nextState, errorTarget) {
    if (!ts.isCallExpression(awaitExpression.expression)) {
      throw compilerError(
        `non-deterministic await detected; all async operations must go through ctx.* methods`,
        awaitExpression,
      );
    }
    const call = awaitExpression.expression;
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
        start_to_close_timeout_ms: temporalFanout.options.start_to_close_timeout_ms,
        heartbeat_timeout_ms: temporalFanout.options.heartbeat_timeout_ms,
      }, awaitExpression);
    }
    if (!ts.isPropertyAccessExpression(call.expression) || call.expression.expression.getText() !== "ctx") {
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
          start_to_close_timeout_ms: temporalActivity.options.start_to_close_timeout_ms,
          heartbeat_timeout_ms: temporalActivity.options.heartbeat_timeout_ms,
          output_var: targetVar ?? undefined,
          on_error: errorTarget ?? undefined,
        }, awaitExpression);
      }
      if (ts.isIdentifier(call.expression) && this.temporalApi.sleep.has(call.expression.text)) {
        return this.addState("wait_timer", {
          type: "wait_for_timer",
          timer_ref: literalString(call.arguments[0], "sleep duration"),
          next: nextState,
        }, awaitExpression);
      }
      if (ts.isIdentifier(call.expression) && this.temporalApi.condition.has(call.expression.text)) {
        return this.lowerTemporalCondition(call, targetVar, nextState);
      }
      if (ts.isIdentifier(call.expression) && this.temporalApi.executeChild.has(call.expression.text)) {
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
      if (ts.isIdentifier(call.expression) && this.temporalApi.startChild.has(call.expression.text)) {
        if (!targetVar) {
          throw compilerError(`await startChild(...) must be assigned to a handle variable`, awaitExpression);
        }
        const childDefinition = call.arguments[0];
        const options = call.arguments[1]
          ? compileTemporalChildOptions(call.arguments[1], "startChild")
          : { input: { kind: "literal", value: null } };
        this.childHandleVars.add(targetVar);
        return this.addState("start_child", {
          type: "start_child",
          child_definition_id: literalIdentifierOrString(childDefinition, "startChild workflow"),
          input: options.input,
          next: nextState,
          handle_var: targetVar,
          workflow_id: options.workflow_id,
          task_queue: options.task_queue,
          parent_close_policy: options.parent_close_policy ?? "TERMINATE",
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
        timer_ref: literalString(call.arguments[0], "ctx.sleep duration"),
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

  lowerTerminalCall(callExpression) {
    if (!ts.isPropertyAccessExpression(callExpression.expression) || callExpression.expression.expression.getText() !== "ctx") {
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
    const lowered = new WorkflowLowerer(
      this.definitionId,
      this.version,
      { body: ts.isBlock(handler.body) ? handler.body : ts.factory.createBlock([ts.factory.createReturnStatement(handler.body)], true) },
      this.temporalApi,
      `update_${shortHash(updateName)}_`,
    );
    const bodyBlock = ts.isBlock(handler.body)
      ? handler.body
      : ts.factory.createBlock([ts.factory.createReturnStatement(handler.body)], true);
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
    Object.assign(this.sourceMap, lowered.sourceMap);
  }
}

function literalString(expression, label) {
  if (!expression || (!ts.isStringLiteral(expression) && !ts.isNoSubstitutionTemplateLiteral(expression))) {
    throw compilerError(`${label} must be a string literal`, expression);
  }
  return expression.text;
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
      options.parent_close_policy = literalString(property.initializer, "ctx.startChild parentClosePolicy");
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
      compiled.parent_close_policy = literalString(property.initializer, `${label} parentClosePolicy`);
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
        && reducer !== "collect_results"
      ) {
        throw compilerError(
          `ctx.bulkActivity reducer must be "all_succeeded", "all_settled", "count", or "collect_results"`,
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
  const validateExpression = (expression) => {
    walkExpression(expression, (node) => {
      if (node.kind === "call" && !helperNames.has(node.callee)) {
        throw compilerError(
          `unsupported function call ${node.callee}; only imported or local pure helpers are allowed`,
        );
      }
    });
  };

  Object.values(artifact.helpers).forEach((helper) => validateExpression(helper.body));
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
    case "step":
      validateExpression(state.input);
      if (state.task_queue) validateExpression(state.task_queue);
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
  const helpers = buildHelperRegistry(program, workflow);
  const lowerer = new WorkflowLowerer(args.definitionId, args.version, workflow, temporalApi);
  const { initialState, states, sourceMap, queries, signals, updates } = lowerer.lower();

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
