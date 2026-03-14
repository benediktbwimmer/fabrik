# Streaming Getting Started

This guide shows the shortest path to a working Fabrik streaming setup:

1. compile a workflow that uses `ctx.bulkActivity()`
2. register or publish that workflow definition
3. create a topic adapter that starts or signals the workflow
4. watch ingress, lag, routing, and reducer progress in the console
5. replay dead letters if a topic record fails mapping or dispatch

For the higher-level product framing, see [streaming-product-guide.md](streaming-product-guide.md). For the current benchmark envelope, see [benchmarking/streaming-performance-envelope.md](benchmarking/streaming-performance-envelope.md).

If you want one runnable end-to-end flow against the local stack, use [streaming-local-quickstart.md](streaming-local-quickstart.md).

## 1. Start With The Example Workflows

Two example workflows live under [`examples/typescript-workflows`](/Users/bene/code/fabrik/examples/typescript-workflows):

- [`bulk-enrichment-workflow.ts`](/Users/bene/code/fabrik/examples/typescript-workflows/bulk-enrichment-workflow.ts) shows topic-driven fan-out with a `histogram` reducer.
- [`inventory-signal-workflow.ts`](/Users/bene/code/fabrik/examples/typescript-workflows/inventory-signal-workflow.ts) shows a signal-driven workflow that uses a `sum` reducer over inventory adjustments.

Both examples use the same supported pattern:

```ts
const batch = await ctx.bulkActivity("process.enrich", items, {
  execution: "eager",
  chunkSize: 256,
  reducer: "histogram",
});

const summary = await batch.result();
return ctx.complete(summary);
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

At that point the artifact can be published through the normal workflow-definition path for your environment.

## 3. Create A Topic Adapter

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

The signal example keeps the adapter narrow: map the record to a workflow instance and signal payload, then let the workflow decide how to batch the work.

## 4. Preview Before Saving

Use the Topic Adapters page in the console to:

- create or clone an adapter
- choose pointer or template mapping for each field
- preview a sample topic payload before saving
- inspect field-level validation errors directly in the editor

The preview path uses the same mapping resolver as live ingestion, so a passing preview is a real signal that the adapter configuration is valid.

## 5. Watch The Streaming Surfaces

Once records start flowing, the main operator surfaces are:

- `/streaming` for unified ingress, queue pressure, routing, reducer mix, and active batch visibility
- `/topic-adapters` for lag, ownership, dead letters, replay, and adapter authoring
- `/task-queues` for throughput backend pressure, pause/resume, and routing explanation
- workflow run detail for batch-level reducer output and live progress

## 6. Recover From Bad Records

If a record fails mapping or dispatch:

1. inspect the dead letter on `/topic-adapters`
2. fix the adapter config or downstream issue
3. replay the dead letter from the adapter page

Dead letters keep replay metadata, so operators can see replay attempts, last replay error, and whether a record has been resolved.

## 7. Choose A Good Workload Shape

The streaming backend is strongest on fewer, wider workflows.

From the current target runs:

- `1 workflow x 100000 activities` performed much better than `100 workflows x 1000 activities`
- `10 workflows x 10000 activities` also materially outperformed `100 workflows x 1000 activities`

If the business semantics allow it, prefer:

- one wide workflow per logical batch
- mergeable reducers such as `sum`, `avg`, or `histogram`
- batch-level visibility instead of per-item workflow history

## 8. Recommended Next Reads

- [streaming-product-guide.md](streaming-product-guide.md)
- [spec/throughput-mode.md](spec/throughput-mode.md)
- [benchmarking/streaming-performance-envelope.md](benchmarking/streaming-performance-envelope.md)
- [benchmarking/streaming-release-scorecard.md](benchmarking/streaming-release-scorecard.md)
