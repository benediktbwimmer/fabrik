import crypto from "node:crypto";
import fs from "node:fs/promises";
import path from "node:path";
import process from "node:process";
import ts from "typescript";

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

function assertNoTopLevelSideEffects(sourceFile) {
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
      throw compilerError(
        `top-level variables are not allowed in workflow modules (${sourceFile.fileName})`,
        statement,
      );
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
        if (
          declaration &&
          declaration !== workflowDeclaration &&
          (ts.isFunctionDeclaration(declaration) ||
            ts.isFunctionExpression(declaration) ||
            ts.isArrowFunction(declaration))
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
    const moduleName = statement.moduleSpecifier.text;
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
      if (declaration) {
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
  if (ts.isCallExpression(expression) && ts.isIdentifier(expression.expression)) {
    return {
      kind: "call",
      callee: expression.expression.text,
      args: expression.arguments.map(compileExpression),
    };
  }

  throw compilerError(`unsupported expression: ${expression.getText()}`, expression);
}

class WorkflowLowerer {
  constructor(definitionId, version, workflowDeclaration) {
    this.definitionId = definitionId;
    this.version = version;
    this.workflowDeclaration = workflowDeclaration;
    this.states = {};
    this.sourceMap = {};
    this.id = 0;
  }

  nextId(prefix) {
    this.id += 1;
    return `${prefix}_${this.id}`;
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
    return { initialState, states: this.states, sourceMap: this.sourceMap };
  }

  addState(prefix, state, node = null) {
    const id = this.nextId(prefix);
    this.states[id] = state;
    if (node) {
      this.sourceMap[id] = sourceLocation(node);
    }
    return id;
  }

  lowerBlock(statements, nextState, breakTarget, continueTarget, errorTarget) {
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
      const choiceState = this.nextId("while_choice");
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
      const choiceState = this.nextId("do_choice");
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
      const choiceState = this.nextId("for_choice");
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
      const arrayVar = this.nextId("__items");
      const indexVar = this.nextId("__index");
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
      const choiceState = this.nextId("for_of_choice");
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
      if (!statement.expression || !ts.isCallExpression(statement.expression)) {
        throw compilerError(`return statements must be ctx terminal calls`, statement);
      }
      return this.lowerTerminalCall(statement.expression);
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
      throw compilerError(`await must target ctx.* call`, awaitExpression);
    }
    const call = awaitExpression.expression;
    if (!ts.isPropertyAccessExpression(call.expression) || call.expression.expression.getText() !== "ctx") {
      throw compilerError(`only await ctx.* calls are allowed`, call.expression);
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
    case "object":
      Object.values(expression.fields).forEach((field) => walkExpression(field, visitor));
      break;
    case "call":
      expression.args.forEach((arg) => walkExpression(arg, visitor));
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
  Object.values(artifact.workflow.states).forEach((state) => {
    switch (state.type) {
      case "assign":
        state.actions.forEach((action) => validateExpression(action.expr));
        break;
      case "choice":
        validateExpression(state.condition);
        break;
      case "step":
        validateExpression(state.input);
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
  });
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  const program = createProgram(args.entry);
  const workflow = findExportedFunction(program, args.exportName);
  const helpers = buildHelperRegistry(program, workflow);
  const lowerer = new WorkflowLowerer(args.definitionId, args.version, workflow);
  const { initialState, states, sourceMap } = lowerer.lower();

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
