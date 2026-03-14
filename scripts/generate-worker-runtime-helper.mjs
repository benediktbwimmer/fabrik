#!/usr/bin/env node
import fs from "node:fs/promises";
import path from "node:path";
import ts from "typescript";

function argValue(flag) {
  const index = process.argv.indexOf(flag);
  if (index === -1 || index + 1 >= process.argv.length) {
    throw new Error(`missing ${flag}`);
  }
  return process.argv[index + 1];
}

function isWorkerCreate(node) {
  return (
    ts.isCallExpression(node) &&
    ts.isPropertyAccessExpression(node.expression) &&
    ts.isIdentifier(node.expression.expression) &&
    node.expression.expression.text === "Worker" &&
    node.expression.name.text === "create"
  );
}

function findObjectProperty(objectLiteral, name) {
  return objectLiteral.properties.find(
    (property) =>
      ts.isPropertyAssignment(property) &&
      ((ts.isIdentifier(property.name) && property.name.text === name) ||
        (ts.isStringLiteral(property.name) && property.name.text === name)),
  );
}

function gatherWorkerCreates(sourceFile) {
  const calls = [];
  function visit(node) {
    if (isWorkerCreate(node)) {
      calls.push(node);
    }
    ts.forEachChild(node, visit);
  }
  visit(sourceFile);
  return calls;
}

function enclosingStatement(node) {
  let current = node;
  while (current && !ts.isStatement(current)) {
    current = current.parent;
  }
  return current;
}

function enclosingBlockLike(node) {
  let current = node.parent;
  while (current && !ts.isBlock(current) && !ts.isSourceFile(current)) {
    current = current.parent;
  }
  return current;
}

const workerJs = path.resolve(argValue("--worker-js"));
const workerIndex = Number(argValue("--worker-index"));
const output = path.resolve(argValue("--output"));

const sourceText = await fs.readFile(workerJs, "utf8");
const sourceFile = ts.createSourceFile(workerJs, sourceText, ts.ScriptTarget.Latest, true, ts.ScriptKind.JS);
const workerCreates = gatherWorkerCreates(sourceFile);
const workerCreate = workerCreates[workerIndex - 1];
if (!workerCreate) {
  throw new Error(`could not find Worker.create #${workerIndex} in ${workerJs}`);
}
const firstArg = workerCreate.arguments[0];
if (!firstArg || !ts.isObjectLiteralExpression(firstArg)) {
  throw new Error(`Worker.create #${workerIndex} does not use an object literal`);
}
const activitiesProperty = findObjectProperty(firstArg, "activities");
if (!activitiesProperty) {
  throw new Error(`Worker.create #${workerIndex} does not define activities`);
}
const taskQueueProperty = findObjectProperty(firstArg, "taskQueue");
const block = enclosingBlockLike(workerCreate);
const statement = enclosingStatement(workerCreate);
if (!block || !statement) {
  throw new Error(`could not determine enclosing block for Worker.create #${workerIndex}`);
}

const imports = sourceFile.statements
  .filter((statement) => ts.isImportDeclaration(statement))
  .filter((statement) => {
    const specifier = statement.moduleSpecifier.text;
    return specifier !== "@temporalio/worker";
  })
  .map((statement) => statement.getText(sourceFile));

const setupStatements = block.statements
  .filter((candidate) => candidate.pos < statement.pos)
  .filter((candidate) => !ts.isImportDeclaration(candidate))
  .map((candidate) => candidate.getText(sourceFile));

const activitiesExpression = activitiesProperty.initializer.getText(sourceFile);
const taskQueueExpression = taskQueueProperty?.initializer?.getText(sourceFile) ?? "null";
const helperSource = `${imports.join("\n")}

export async function createFabrikWorkerRuntime() {
${setupStatements.map((statement) => `  ${statement.replaceAll("\n", "\n  ")}`).join("\n")}
  return {
    activities: ${activitiesExpression},
    taskQueue: ${taskQueueExpression},
  };
}
`;

await fs.mkdir(path.dirname(output), { recursive: true });
await fs.writeFile(output, helperSource);
