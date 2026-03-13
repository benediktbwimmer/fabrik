import fs from "node:fs/promises";
import path from "node:path";
import process from "node:process";
import ts from "typescript";

const WORKFLOW_SUPPORTED_IMPORTS = new Set([
  "CancellationScope",
  "condition",
  "continueAsNew",
  "defineQuery",
  "defineSignal",
  "defineUpdate",
  "executeChild",
  "getExternalWorkflowHandle",
  "isCancellation",
  "proxyActivities",
  "setHandler",
  "sleep",
  "startChild",
]);

const BLOCKED_WORKFLOW_IMPORTS = new Map([
  ["patched", "use ctx.version(...) or version markers in Fabrik instead of Temporal patched APIs"],
  ["deprecatePatch", "use ctx.version(...) or version markers in Fabrik instead of Temporal patched APIs"],
  ["upsertSearchAttributes", "search attributes are not migration-ready yet in Fabrik"],
  ["workflowInfo", "workflow info inspection is not migration-ready yet in Fabrik"],
]);

const BLOCKED_PAYLOAD_IMPORTS = new Set([
  "CompositePayloadConverter",
  "DataConverter",
  "DefaultFailureConverter",
  "DefaultPayloadConverterWithProtobufs",
  "LoadedDataConverter",
  "PayloadCodec",
  "PayloadConverter",
  "PayloadConverterWithEncoding",
]);

const WORKER_BLOCKING_PROPERTIES = new Map([
  ["dataConverter", "custom data converters are not supported by the migration pipeline"],
  ["payloadCodec", "custom payload codecs are not supported by the migration pipeline"],
  ["payloadConverterPath", "custom payload converters are not supported by the migration pipeline"],
  ["codecServer", "codec servers are not supported by the migration pipeline"],
  ["interceptors", "worker interceptors are not migration-ready yet"],
  ["sinks", "custom worker sinks are not migration-ready yet"],
  ["workflowInterceptorModules", "workflow interceptors are not migration-ready yet"],
]);

const SUPPORT_MATRIX = [
  {
    feature: "proxy_activities",
    support_level: "native",
    replay_validation: true,
    deploy_validation: true,
    semantic_caveats: ["static proxyActivities options only"],
    blocking_severity: "warning",
  },
  {
    feature: "activity_options_and_retries",
    support_level: "native",
    replay_validation: true,
    deploy_validation: true,
    semantic_caveats: ["static retry and timeout literals only"],
    blocking_severity: "warning",
  },
  {
    feature: "signals_queries_updates",
    support_level: "native",
    replay_validation: true,
    deploy_validation: true,
    semantic_caveats: ["handler bodies must stay within the compiler subset"],
    blocking_severity: "warning",
  },
  {
    feature: "async_handlers",
    support_level: "native",
    replay_validation: true,
    deploy_validation: true,
    semantic_caveats: ["async handlers must stay within the supported control-flow subset"],
    blocking_severity: "warning",
  },
  {
    feature: "condition_waits",
    support_level: "native",
    replay_validation: true,
    deploy_validation: true,
    semantic_caveats: ["timeout arguments must remain static literals"],
    blocking_severity: "warning",
  },
  {
    feature: "child_workflows",
    support_level: "native",
    replay_validation: true,
    deploy_validation: true,
    semantic_caveats: ["current support targets the existing compiler subset"],
    blocking_severity: "warning",
  },
  {
    feature: "external_workflow_handles",
    support_level: "native",
    replay_validation: true,
    deploy_validation: false,
    semantic_caveats: ["external-handle support is limited to the currently compiled subset"],
    blocking_severity: "warning",
  },
  {
    feature: "cancellation_scopes",
    support_level: "native",
    replay_validation: true,
    deploy_validation: true,
    semantic_caveats: ["current support covers the narrow Temporal-style patterns already compiled"],
    blocking_severity: "warning",
  },
  {
    feature: "continue_as_new",
    support_level: "native",
    replay_validation: true,
    deploy_validation: true,
    semantic_caveats: [],
    blocking_severity: "warning",
  },
  {
    feature: "ctx_version_workflow_evolution",
    support_level: "native",
    replay_validation: true,
    deploy_validation: true,
    semantic_caveats: ["migration should map Temporal versioning patterns onto Fabrik version markers"],
    blocking_severity: "warning",
  },
  {
    feature: "worker_build_ids_and_routing",
    support_level: "adapter",
    replay_validation: false,
    deploy_validation: true,
    semantic_caveats: ["compiled workflows run on Fabrik runtime; activity builds are packaged separately"],
    blocking_severity: "warning",
  },
  {
    feature: "search_attributes_memo",
    support_level: "blocked",
    replay_validation: false,
    deploy_validation: false,
    semantic_caveats: ["apps that rely on search or memo semantics are blocked until the visibility surface is complete"],
    blocking_severity: "hard_block",
  },
  {
    feature: "payload_data_converter_usage",
    support_level: "blocked",
    replay_validation: false,
    deploy_validation: false,
    semantic_caveats: ["custom payload conversion and codec behavior are blocked by default"],
    blocking_severity: "hard_block",
  },
  {
    feature: "worker_bootstrap_patterns",
    support_level: "adapter",
    replay_validation: false,
    deploy_validation: true,
    semantic_caveats: ["static Worker.create bootstraps are packaged; dynamic bootstraps are blocked"],
    blocking_severity: "warning",
  },
  {
    feature: "interceptors_middleware",
    support_level: "blocked",
    replay_validation: false,
    deploy_validation: false,
    semantic_caveats: ["interceptor behavior is not migration-ready yet"],
    blocking_severity: "hard_block",
  },
  {
    feature: "unsupported_temporal_api",
    support_level: "blocked",
    replay_validation: false,
    deploy_validation: false,
    semantic_caveats: ["unsupported Temporal APIs block migration until explicitly implemented"],
    blocking_severity: "hard_block",
  },
];

function usage() {
  console.error("usage: node sdk/typescript-compiler/migration-analyzer.mjs --project <dir>");
  process.exit(1);
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

function findStaticString(expression) {
  if (ts.isStringLiteral(expression) || ts.isNoSubstitutionTemplateLiteral(expression)) {
    return expression.text;
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

function extractExportedAsyncWorkflows(projectRoot, program, sourceFile, fileUses) {
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
  const files = await collectProjectFiles(projectRoot);
  const program = createProgram(projectRoot, files);

  const analyzedFiles = [];
  const workflows = [];
  const workers = [];
  let findings = [];

  for (const sourceFile of program.getSourceFiles()) {
    if (!isProjectSourceFile(projectRoot, sourceFile)) {
      continue;
    }

    const relativeFile = relativeProjectPath(projectRoot, sourceFile.fileName);
    const { workflowImports, workerImports, clientImports, commonImports } = collectImportInfo(sourceFile);
    const temporalImports = [];
    const fileUses = new Set();

    for (const [localName, importedName] of workflowImports) {
      temporalImports.push(`@temporalio/workflow:${importedName} as ${localName}`);
      if (WORKFLOW_SUPPORTED_IMPORTS.has(importedName)) {
        if (importedName === "proxyActivities") fileUses.add("proxy_activities");
        if (["defineSignal", "defineQuery", "defineUpdate"].includes(importedName)) {
          fileUses.add("signals_queries_updates");
        }
        if (importedName === "condition") fileUses.add("condition_waits");
        if (importedName === "CancellationScope") fileUses.add("cancellation_scopes");
        if (["executeChild", "startChild"].includes(importedName)) fileUses.add("child_workflows");
        if (importedName === "getExternalWorkflowHandle") fileUses.add("external_workflow_handles");
        if (importedName === "continueAsNew") fileUses.add("continue_as_new");
        continue;
      }
      if (BLOCKED_WORKFLOW_IMPORTS.has(importedName)) {
        findings.push(
          createFinding(
            projectRoot,
            "hard_block",
            "blocked_temporal_workflow_import",
            importedName === "upsertSearchAttributes" ? "visibility_search_usage" : "unsupported_temporal_api",
            sourceFile,
            `Temporal workflow import ${importedName} is not supported by the migration pipeline`,
            BLOCKED_WORKFLOW_IMPORTS.get(importedName),
            importedName,
          ),
        );
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

    function visit(node) {
      if (ts.isCallExpression(node) && ts.isIdentifier(node.expression)) {
        const importedWorkflowName = workflowImports.get(node.expression.text);
        if (importedWorkflowName === "proxyActivities") {
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
        const firstArg = node.arguments[0];
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

          const taskQueue =
            taskQueueProperty && ts.isPropertyAssignment(taskQueueProperty)
              ? findStaticString(taskQueueProperty.initializer)
              : taskQueueProperty && ts.isShorthandPropertyAssignment(taskQueueProperty)
                ? null
                : null;
          if (taskQueueProperty && taskQueue == null) {
            findings.push(
              createFinding(
                projectRoot,
                "hard_block",
                "dynamic_task_queue",
                "worker_bootstrap_patterns",
                taskQueueProperty,
                "Worker taskQueue must be a static string literal",
                "use a static taskQueue string in Worker.create",
                "taskQueue",
              ),
            );
          }

          const workflowsPath =
            workflowsPathProperty && ts.isPropertyAssignment(workflowsPathProperty)
              ? findStaticString(workflowsPathProperty.initializer)
              : workflowsPathProperty && ts.isShorthandPropertyAssignment(workflowsPathProperty)
                ? null
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

          for (const [propertyName, remediation] of WORKER_BLOCKING_PROPERTIES.entries()) {
            const property = findObjectProperty(firstArg, propertyName);
            if (property) {
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
          if (activitiesProperty) {
            if (
              ts.isPropertyAssignment(activitiesProperty) &&
              ts.isIdentifier(activitiesProperty.initializer)
            ) {
              activitiesReference = activitiesProperty.initializer.text;
              activityModule = maybeImportedModulePath(sourceFile, activitiesReference);
            } else if (ts.isShorthandPropertyAssignment(activitiesProperty)) {
              activitiesReference = activitiesProperty.name.text;
              activityModule = maybeImportedModulePath(sourceFile, activitiesReference);
            } else if (
              ts.isPropertyAssignment(activitiesProperty) &&
              ts.isObjectLiteralExpression(activitiesProperty.initializer)
            ) {
              activitiesReference = "inline-object";
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
                ? findStaticString(buildIdProperty.initializer)
                : null,
            workflows_path: workflowsPath,
            activities_reference: activitiesReference,
            activity_module: activityModule,
            bootstrap_pattern: "worker_create_static",
            uses: [...fileUses].sort(),
          });
        }
      }

      if (ts.isPropertyAssignment(node) && node.name != null) {
        const propertyName =
          ts.isIdentifier(node.name) ? node.name.text : ts.isStringLiteral(node.name) ? node.name.text : null;
        if (propertyName === "searchAttributes" || propertyName === "memo") {
          fileUses.add("search_attributes_memo");
          findings.push(
            createFinding(
              projectRoot,
              "hard_block",
              `blocked_${propertyName}_usage`,
              "visibility_search_usage",
              node,
              `Temporal ${propertyName} usage is not migration-ready yet`,
              "remove or replace visibility-dependent semantics before migration",
              propertyName,
            ),
          );
        }
        if (
          propertyName === "dataConverter" ||
          propertyName === "payloadCodec" ||
          propertyName === "payloadConverterPath" ||
          propertyName === "codecServer"
        ) {
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

      ts.forEachChild(node, visit);
    }

    visit(sourceFile);

    const exportedWorkflows = workflowImports.size > 0
      ? extractExportedAsyncWorkflows(projectRoot, program, sourceFile, fileUses)
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
  const hardBlockCount = findings.filter((finding) => finding.severity === "hard_block").length;
  const warningCount = findings.filter((finding) => finding.severity === "warning").length;
  const infoCount = findings.filter((finding) => finding.severity === "info").length;

  process.stdout.write(
    `${JSON.stringify(
      {
        schema_version: 1,
        project_root: projectRoot,
        support_matrix: SUPPORT_MATRIX,
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
