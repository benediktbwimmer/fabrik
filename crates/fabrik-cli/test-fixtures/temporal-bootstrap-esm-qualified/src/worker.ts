import path from "node:path";
import { URL, fileURLToPath } from "node:url";
import { Worker } from "@temporalio/worker";
import * as activities from "./activities";

const workflowsPathUrl = new URL(`./workflows${path.extname(import.meta.url)}`, import.meta.url);

export async function createWorker() {
  return Worker.create({
    workflowsPath: fileURLToPath(workflowsPathUrl),
    activities,
    taskQueue: "bootstrap-esm-qualified",
  });
}
