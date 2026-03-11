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

class CompilerError extends Error {}

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
      throw new CompilerError(
        `top-level variables are not allowed in workflow modules (${sourceFile.fileName})`,
      );
    }
    if (!ts.isEmptyStatement(statement)) {
      throw new CompilerError(
        `top-level side effects are not allowed in workflow modules (${sourceFile.fileName})`,
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
        throw new CompilerError(`workflow export ${exportName} must be async`);
      }
      return declaration;
    }
  }
  throw new CompilerError(`exported async workflow ${exportName} not found`);
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
    throw new CompilerError(`helper ${name} must not be async`);
  }
  const params = declaration.parameters.map((parameter) => parameter.name.getText());
  if (ts.isBlock(declaration.body)) {
    if (
      declaration.body.statements.length !== 1 ||
      !ts.isReturnStatement(declaration.body.statements[0]) ||
      !declaration.body.statements[0].expression
    ) {
      throw new CompilerError(`helper ${name} must be a single return expression`);
    }
    return { params, body: compileExpression(declaration.body.statements[0].expression) };
  }
  return { params, body: compileExpression(declaration.body) };
}

function compileExpression(expression) {
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
        throw new CompilerError(`unsupported object literal property: ${property.getText()}`);
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
      throw new CompilerError(`unsupported binary operator ${expression.operatorToken.getText()}`);
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

  throw new CompilerError(`unsupported expression: ${expression.getText()}`);
}

class WorkflowLowerer {
  constructor(definitionId, version, workflowDeclaration) {
    this.definitionId = definitionId;
    this.version = version;
    this.workflowDeclaration = workflowDeclaration;
    this.states = {};
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
    return { initialState, states: this.states };
  }

  addState(prefix, state) {
    const id = this.nextId(prefix);
    this.states[id] = state;
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
          throw new CompilerError(`await variable declarations must declare exactly one identifier`);
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
          throw new CompilerError(`unsupported declaration: ${statement.getText()}`);
        }
        return {
          target: declaration.name.text,
          expr: compileExpression(declaration.initializer),
        };
      });
      return this.addState("assign", { type: "assign", actions, next: nextState });
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
          throw new CompilerError(`unsupported assignment target: ${statement.getText()}`);
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
        });
      }
      throw new CompilerError(`unsupported expression statement: ${statement.getText()}`);
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
      });
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
      return statement.initializer
        ? this.lowerForInitializer(statement.initializer, choiceState)
        : choiceState;
    }

    if (ts.isForOfStatement(statement)) {
      if (!ts.isVariableDeclarationList(statement.initializer)) {
        throw new CompilerError(`unsupported for-of initializer: ${statement.getText()}`);
      }
      const loopVar = statement.initializer.declarations[0];
      if (!ts.isIdentifier(loopVar.name)) {
        throw new CompilerError(`unsupported for-of binding: ${statement.getText()}`);
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
      });
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
      });
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
      return this.addState("assign", {
        type: "assign",
        actions: [
          { target: arrayVar, expr: compileExpression(statement.expression) },
          { target: indexVar, expr: { kind: "literal", value: 0 } },
        ],
        next: choiceState,
      });
    }

    if (ts.isBreakStatement(statement)) {
      if (!breakTarget) throw new CompilerError(`break used outside loop`);
      return breakTarget;
    }

    if (ts.isContinueStatement(statement)) {
      if (!continueTarget) throw new CompilerError(`continue used outside loop`);
      return continueTarget;
    }

    if (ts.isReturnStatement(statement)) {
      if (!statement.expression || !ts.isCallExpression(statement.expression)) {
        throw new CompilerError(`return statements must be ctx terminal calls`);
      }
      return this.lowerTerminalCall(statement.expression);
    }

    if (ts.isThrowStatement(statement)) {
      if (errorTarget) {
        const actions = [];
        if (errorTarget.error_var && statement.expression) {
          actions.push({ target: errorTarget.error_var, expr: compileExpression(statement.expression) });
        }
        return this.addState("assign", { type: "assign", actions, next: errorTarget.next });
      }
      return this.addState("fail", {
        type: "fail",
        reason: statement.expression ? compileExpression(statement.expression) : undefined,
      });
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

    throw new CompilerError(`unsupported statement: ${statement.getText()}`);
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
      return this.addState("assign", { type: "assign", actions, next: nextState });
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
      });
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
      });
    }
    throw new CompilerError(`unsupported for-loop update expression: ${expression.getText()}`);
  }

  lowerAwait(awaitExpression, targetVar, nextState, errorTarget) {
    if (!ts.isCallExpression(awaitExpression.expression)) {
      throw new CompilerError(`await must target ctx.* call`);
    }
    const call = awaitExpression.expression;
    if (!ts.isPropertyAccessExpression(call.expression) || call.expression.expression.getText() !== "ctx") {
      throw new CompilerError(`only await ctx.* calls are allowed`);
    }
    const method = call.expression.name.text;
    if (method === "waitForSignal") {
      return this.addState("wait_signal", {
        type: "wait_for_event",
        event_type: literalString(call.arguments[0], "ctx.waitForSignal signal name"),
        next: nextState,
        output_var: targetVar ?? undefined,
      });
    }
    if (method === "sleep") {
      return this.addState("wait_timer", {
        type: "wait_for_timer",
        timer_ref: literalString(call.arguments[0], "ctx.sleep duration"),
        next: nextState,
      });
    }
    if (method === "activity") {
      return this.addState("step", {
        type: "step",
        handler: literalString(call.arguments[0], "ctx.activity handler"),
        input: call.arguments[1] ? compileExpression(call.arguments[1]) : { kind: "literal", value: null },
        next: nextState,
        output_var: targetVar ?? undefined,
        on_error: errorTarget ?? undefined,
      });
    }
    if (method === "httpRequest") {
      const requestArg = call.arguments[0];
      if (!requestArg || !ts.isObjectLiteralExpression(requestArg)) {
        throw new CompilerError(`ctx.httpRequest requires a static request object`);
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
      });
    }
    throw new CompilerError(`unsupported ctx method ctx.${method}`);
  }

  lowerTerminalCall(callExpression) {
    if (!ts.isPropertyAccessExpression(callExpression.expression) || callExpression.expression.expression.getText() !== "ctx") {
      throw new CompilerError(`terminal return must be ctx.complete/fail/continueAsNew`);
    }
    const method = callExpression.expression.name.text;
    if (method === "complete") {
      return this.addState("complete", {
        type: "succeed",
        output: callExpression.arguments[0] ? compileExpression(callExpression.arguments[0]) : undefined,
      });
    }
    if (method === "fail") {
      return this.addState("fail", {
        type: "fail",
        reason: callExpression.arguments[0] ? compileExpression(callExpression.arguments[0]) : undefined,
      });
    }
    if (method === "continueAsNew") {
      return this.addState("continue_as_new", {
        type: "continue_as_new",
        input: callExpression.arguments[0] ? compileExpression(callExpression.arguments[0]) : undefined,
      });
    }
    throw new CompilerError(`unsupported terminal call ctx.${method}`);
  }
}

function literalString(expression, label) {
  if (!expression || (!ts.isStringLiteral(expression) && !ts.isNoSubstitutionTemplateLiteral(expression))) {
    throw new CompilerError(`${label} must be a string literal`);
  }
  return expression.text;
}

function compileHttpConfig(objectLiteral) {
  const config = { kind: "http_request", method: "GET", url: "", headers: {}, body_from_input: true };
  for (const property of objectLiteral.properties) {
    if (!ts.isPropertyAssignment(property)) {
      throw new CompilerError(`unsupported httpRequest property ${property.getText()}`);
    }
    const key = property.name.getText().replaceAll(/^["']|["']$/g, "");
    if (key === "method") config.method = literalString(property.initializer, "httpRequest.method");
    else if (key === "url") config.url = literalString(property.initializer, "httpRequest.url");
    else if (key === "bodyFromInput") config.body_from_input = property.initializer.kind === ts.SyntaxKind.TrueKeyword;
    else if (key === "headers") {
      if (!ts.isObjectLiteralExpression(property.initializer)) {
        throw new CompilerError(`httpRequest.headers must be a static object`);
      }
      for (const header of property.initializer.properties) {
        if (!ts.isPropertyAssignment(header)) {
          throw new CompilerError(`unsupported header config ${header.getText()}`);
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

async function main() {
  const args = parseArgs(process.argv.slice(2));
  const program = createProgram(args.entry);
  const workflow = findExportedFunction(program, args.exportName);
  const helpers = buildHelperRegistry(program, workflow);
  const lowerer = new WorkflowLowerer(args.definitionId, args.version, workflow);
  const { initialState, states } = lowerer.lower();

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
    source_map: {},
    helpers,
    workflow: {
      initial_state: initialState,
      states,
    },
    artifact_hash: "",
  };
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
