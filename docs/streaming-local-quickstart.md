# Streaming Local Quickstart

This quickstart runs one complete streaming path against the local Fabrik stack:

1. compile a workflow artifact
2. publish it to the local API gateway
3. create a topic adapter
4. preview the adapter mapping
5. publish one Redpanda record
6. wait for the workflow to materialize and the bulk batch to complete

The runnable entry point is [`scripts/run-streaming-local-quickstart.sh`](/Users/bene/code/fabrik/scripts/run-streaming-local-quickstart.sh).

## What It Uses

Workflow:

- [`local-streaming-quickstart-workflow.ts`](/Users/bene/code/fabrik/examples/typescript-workflows/local-streaming-quickstart-workflow.ts)

Adapter config:

- [`local-streaming-quickstart.json`](/Users/bene/code/fabrik/examples/topic-adapters/local-streaming-quickstart.json)

Sample topic event:

- [`local-streaming-quickstart-event.json`](/Users/bene/code/fabrik/examples/topic-adapters/local-streaming-quickstart-event.json)

The workflow is intentionally runnable on the dev stack as-is:

- it uses `benchmark.echo`, which the built-in activity worker already supports
- it pins `taskQueue: "bulk"` to match the dev stack worker defaults
- it uses the `histogram` reducer so the result is easy to inspect

## Run It

Start the local stack first if it is not already running:

```bash
make up
```

Then run:

```bash
./scripts/run-streaming-local-quickstart.sh
```

The script will:

- compile the workflow artifact under `target/quickstarts/streaming-local-quickstart/`
- publish the artifact to `/tenants/dev/workflow-artifacts`
- pin the `bulk` activity queue to `stream-v2`
- upsert the topic adapter under `/admin/tenants/dev/topic-adapters/local-streaming-quickstart`
- preview the adapter dispatch
- create the Redpanda topic if needed
- publish one sample topic record
- poll until the workflow run materializes and the bulk batch is complete
- try to fetch the final workflow state, but still finish if workflow terminalization is lagging behind the completed batch projection

## What To Inspect

After the script finishes, the useful outputs are:

- `target/quickstarts/streaming-local-quickstart/artifact-response.json`
- `target/quickstarts/streaming-local-quickstart/adapter-detail.json`
- `target/quickstarts/streaming-local-quickstart/workflow.json`
- `target/quickstarts/streaming-local-quickstart/bulk-batches.json`
- `target/quickstarts/streaming-local-quickstart/summary.md`

In the console, the same run should now be visible in:

- `/streaming`
- `/topic-adapters`
- `/task-queues`

## Notes

- This quickstart publishes the compiled artifact directly. That is the runnable registration path for local execution.
- It does not publish a separate workflow-definition graph model.
- The adapter config is rendered into the output directory before it is applied, so you can inspect the exact brokers, topic name, queue, and definition ID used for the run.
- If your local dev stack has a large amount of old benchmark history, the durable runtime may spend noticeable time replaying broker history after a restart. In that case, adapter ingestion can succeed immediately while the run stays `running` until unified-runtime catches up.
- The script treats completed bulk-batch state as the primary success condition. On a fresh stack, that usually arrives immediately; workflow terminalization can still lag and may leave `workflow.json` showing `status: running` for a short period even though `bulk-batches.json` already shows the completed `stream-v2` result.

## Next Steps

After the quickstart works, the next useful reads are:

- [streaming-getting-started.md](streaming-getting-started.md)
- [streaming-product-guide.md](streaming-product-guide.md)
- [benchmarking/streaming-performance-envelope.md](benchmarking/streaming-performance-envelope.md)
