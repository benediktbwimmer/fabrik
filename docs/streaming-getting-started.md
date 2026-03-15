# Streaming Getting Started

This guide shows the shortest path to a working Fabrik streaming setup:

1. compile a workflow that uses `ctx.bulkActivity()` or `ctx.startStreamJob()`
2. register or publish that workflow definition
3. create a topic adapter that starts or signals the workflow
4. watch ingress, lag, routing, and reducer progress in the console
5. replay dead letters if a topic record fails mapping or dispatch

For the higher-level product framing, see [streaming-product-guide.md](streaming-product-guide.md). For the current benchmark envelope, see [benchmarking/streaming-performance-envelope.md](benchmarking/streaming-performance-envelope.md).

If you want one runnable end-to-end flow against the local stack, use [streaming-local-quickstart.md](streaming-local-quickstart.md).

## 1. Start With The Example Workflows

Three example workflows live under [`examples/typescript-workflows`](/Users/bene/code/fabrik/examples/typescript-workflows):

- [`bulk-enrichment-workflow.ts`](/Users/bene/code/fabrik/examples/typescript-workflows/bulk-enrichment-workflow.ts) shows topic-driven fan-out with a `histogram` reducer.
- [`inventory-signal-workflow.ts`](/Users/bene/code/fabrik/examples/typescript-workflows/inventory-signal-workflow.ts) shows a signal-driven workflow that uses a `sum` reducer over inventory adjustments.
- [`stream-job-rollup-workflow.ts`](/Users/bene/code/fabrik/examples/typescript-workflows/stream-job-rollup-workflow.ts) shows the stream-native path: a workflow starts the `keyed-rollup` stream job, waits for `hourly-rollup-ready`, then issues a strong keyed query against `accountTotals`.
- [`stream-job-signal-workflow.ts`](/Users/bene/code/fabrik/examples/typescript-workflows/stream-job-signal-workflow.ts) shows the hybrid path: a workflow starts a topic-backed stream job, waits for a `signal_workflow` callback such as `account.rollup.ready`, then issues a strong query and shuts the job down explicitly.

The bulk examples use the current throughput-mode pattern:

```ts
const batch = await ctx.bulkActivity("process.enrich", items, {
  execution: "eager",
  chunkSize: 256,
  reducer: "histogram",
});

const summary = await batch.result();
return ctx.complete(summary);
```

The stream-job example uses the current workflow/streams bridge surface:

```ts
import type {
  KeyedRollupJob,
  WorkflowContext,
} from "../../sdk/typescript-compiler/workflow-authoring.js";

export async function streamJobRollupWorkflow(ctx: WorkflowContext, input) {
const job = await ctx.startStreamJob<KeyedRollupJob>("keyed-rollup", {
  input: {
    kind: "bounded_items",
    items: input.payments,
  },
});

await job.awaitCheckpoint("hourly-rollup-ready");
const account = await job.query("accountTotals", { key: input.accountId }, {
  consistency: "strong",
});

return ctx.complete({ account });
}
```

The stream-to-workflow hybrid example uses the same bridge plus a declared `signal_workflow` operator:

```ts
const job = await ctx.startStreamJob("keyed-rollup", {
  input: { topic: input.topic },
  config: {
    source: { kind: "topic", name: input.topic },
    operators: [
      { kind: "reduce", name: "sum-account-totals", config: { reducer: "sum", valueField: "amount", outputField: "totalAmount" } },
      { kind: "emit_checkpoint", name: "hourly-rollup-ready", config: { sequence: 1 } },
      { kind: "signal_workflow", name: "notify-account-rollup", config: { view: "accountTotals", signalType: "account.rollup.ready", whenOutputField: "totalAmount" } },
    ],
  },
});

const signal = await ctx.waitForSignal("account.rollup.ready");
const account = await job.query("accountTotals", { key: input.accountId }, {
  consistency: "strong",
});
await job.cancel({ reason: "workflow-threshold-handled" });
const result = await job.awaitTerminal();

return ctx.complete({ signal, account, terminalStatus: result.status });
```

## 2. Compile A Workflow Artifact

Compile a workflow definition with the TypeScript compiler:

```bash
node sdk/typescript-compiler/compiler.mjs \
  --entry examples/typescript-workflows/bulk-enrichment-workflow.ts \
  --export bulkEnrichmentWorkflow \
  --definition-id bulk-enrichment-workflow \
  --version 1 \
  --out target/workflow-artifacts/bulk-enrichment-workflow.json
```

Example for the signal-driven workflow:

```bash
node sdk/typescript-compiler/compiler.mjs \
  --entry examples/typescript-workflows/inventory-signal-workflow.ts \
  --export inventorySignalWorkflow \
  --definition-id inventory-signal-workflow \
  --version 1 \
  --out target/workflow-artifacts/inventory-signal-workflow.json
```

Example for the stream-job workflow:

```bash
node sdk/typescript-compiler/compiler.mjs \
  --entry examples/typescript-workflows/stream-job-rollup-workflow.ts \
  --export streamJobRollupWorkflow \
  --definition-id stream-job-rollup-workflow \
  --version 1 \
  --out target/workflow-artifacts/stream-job-rollup-workflow.json
```

At that point the artifact can be published through the normal workflow-definition path for your environment.

## 3. Run A Standalone Stream Directly

If you want `Fabrik Streams` without a workflow wrapper, publish a stream artifact and submit or deploy it directly through ingest-service.

One-off or manually managed stream job:

```bash
curl -sS -X POST http://localhost:3001/tenants/tenant-a/streams/jobs \
  -H 'content-type: application/json' \
  -d '{
    "definition_id": "fraud-detector",
    "version": 1,
    "instance_id": "fraud-standalone",
    "run_id": "run-1",
    "job_id": "fraud-job",
    "input": { "kind": "topic", "topic": "payments" },
    "config": { "threshold": 0.97 }
  }'
```

Long-lived deployed stream with rollout identity:

```bash
curl -sS -X POST http://localhost:3001/tenants/tenant-a/streams/deployments \
  -H 'content-type: application/json' \
  -d '{
    "definition_id": "fraud-detector",
    "version": 1,
    "deployment_id": "fraud-prod",
    "job_id": "fraud-detector-job",
    "input": { "kind": "topic", "topic": "payments" },
    "config": { "threshold": 0.97 }
  }'
```

The standalone control plane is split deliberately:

- `POST /tenants/{tenant_id}/streams/jobs` creates one standalone job handle
- `POST /tenants/{tenant_id}/streams/jobs/{instance_id}/{run_id}/{job_id}/pause` pauses a direct standalone job
- `POST /tenants/{tenant_id}/streams/jobs/{instance_id}/{run_id}/{job_id}/resume` resumes it
- `POST /tenants/{tenant_id}/streams/jobs/{instance_id}/{run_id}/{job_id}/cancel` or `/drain` requests shutdown
- `POST /tenants/{tenant_id}/streams/deployments` creates or rolls a deployment revision
- `POST /tenants/{tenant_id}/streams/deployments/{deployment_id}/pause|resume|drain|rollback` controls the deployment lifecycle

Visibility stays the same whether a job came from a workflow or from the standalone API:

```bash
curl -sS http://localhost:3002/tenants/tenant-a/streams/jobs?origin_kind=standalone
curl -sS http://localhost:3002/tenants/tenant-a/streams/jobs/fraud-standalone/run-1/fraud-job
curl -sS http://localhost:3002/tenants/tenant-a/streams/deployments/fraud-prod
```

The job detail, runtime, checkpoint, signal, and materialized-view endpoints under query-service work for both workflow-originated and standalone jobs.

## 4. Create A Topic Adapter

Sample adapter configs live under [`examples/topic-adapters`](/Users/bene/code/fabrik/examples/topic-adapters):

- [`orders-start-workflow.json`](/Users/bene/code/fabrik/examples/topic-adapters/orders-start-workflow.json) maps an order event into `start_workflow`.
- [`inventory-signal.json`](/Users/bene/code/fabrik/examples/topic-adapters/inventory-signal.json) maps an inventory event into `signal_workflow`.

The start-workflow example uses deterministic templates for workflow input, memo, and search attributes:

```json
{
  "action": "start_workflow",
  "definition_id": "bulk-enrichment-workflow",
  "payload_template_json": {
    "items": {
      "$from": "/payload/items"
    }
  },
  "request_id_json_pointer": "/request_id"
}
```

The same adapter pattern works for stream-job-backed workflows too: the topic adapter still starts the workflow, and the workflow decides whether to fan out with `bulkActivity()` or hand the payload to `ctx.startStreamJob(...)`.

The signal example keeps the adapter narrow: map the record to a workflow instance and signal payload, then let the workflow decide how to batch the work.

## 5. Preview Before Saving

Use the Topic Adapters page in the console to:

- create or clone an adapter
- choose pointer or template mapping for each field
- preview a sample topic payload before saving
- inspect field-level validation errors directly in the editor

The preview path uses the same mapping resolver as live ingestion, so a passing preview is a real signal that the adapter configuration is valid.

## 6. Watch The Streaming Surfaces

Once records start flowing, the main operator surfaces are:

- `/streaming` for unified ingress, queue pressure, routing, reducer mix, and active batch visibility
- `/topic-adapters` for lag, ownership, dead letters, replay, and adapter authoring
- `/task-queues` for throughput backend pressure, pause/resume, and routing explanation
- workflow run detail for batch-level reducer output and live progress

## 7. Recover From Bad Records

If a record fails mapping or dispatch:

1. inspect the dead letter on `/topic-adapters`
2. fix the adapter config or downstream issue
3. replay the dead letter from the adapter page

Dead letters keep replay metadata, so operators can see replay attempts, last replay error, and whether a record has been resolved.

## 8. Choose A Good Workload Shape

The streaming backend is strongest on fewer, wider workflows.

From the current target runs:

- `1 workflow x 100000 activities` performed much better than `100 workflows x 1000 activities`
- `10 workflows x 10000 activities` also materially outperformed `100 workflows x 1000 activities`

If the business semantics allow it, prefer:

- one wide workflow per logical batch
- mergeable reducers such as `sum`, `avg`, or `histogram`
- batch-level visibility instead of per-item workflow history

## 9. Recommended Next Reads

- [streaming-product-guide.md](streaming-product-guide.md)
- [spec/throughput-mode.md](spec/throughput-mode.md)
- [benchmarking/streaming-performance-envelope.md](benchmarking/streaming-performance-envelope.md)
- [benchmarking/streaming-release-scorecard.md](benchmarking/streaming-release-scorecard.md)
