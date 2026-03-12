# Throughput Mode

## Purpose

Throughput mode is an explicit workflow primitive for high-cardinality fan-out / fan-in workloads. It trades per-item durable history for chunk-level durability, reducing workflow overhead from `O(items)` to `O(chunks)`.

Eager execution is an execution policy inside throughput mode, not a separate workflow model and not a change to the default workflow runtime.

## Summary

The product contract for throughput mode is:

- throughput mode is opt-in per `ctx.bulkActivity()` callsite
- workflow state remains single-owner and strongly ordered
- bulk work remains batch/chunk based
- only batch barrier events are workflow-authoritative
- intermediate batch and chunk progress is projection-only and may be eventually consistent

The core rule is:

> Chunk work may execute and complete ahead of workflow observation, but it does not directly mutate workflow state. The workflow only observes a deterministic batch outcome at the barrier.

This makes eager execution a safe extension of throughput mode rather than eventual consistency for workflows.

## Public Interface

### Workflow API

Workflows opt in to throughput mode per callsite using `ctx.bulkActivity()`:

```ts
const batch = await ctx.bulkActivity("process.enrich", items, {
  execution: "eager",
  chunkSize: 256,
  retry: { maxAttempts: 3, delay: "1s" },
  reducer: "collect_results",
});

const result = await batch.result();
```

- `ctx.bulkActivity()` returns a handle immediately after durable batch creation.
- `await handle.result()` is the only workflow-authoritative synchronization point.
- `items` may be any runtime expression evaluating to an array.
- `taskQueue`, `chunkSize`, `execution`, `retry`, and `reducer` must be static literal options in the compiled artifact.
- `chunkSize` defaults to `256` when omitted.
- backend selection is server-controlled and does not appear in workflow code.

### Result Shape

On success, `await handle.result()` returns:

```ts
{
  batchId: string,
  status: "completed",
  totalItems: number,
  succeededItems: number,
  failedItems: number,
  cancelledItems: number,
  chunkCount: number,
  resultHandle: { batchId: string }
}
```

On failure or cancellation, the workflow resumes through the error path with a structured error containing `batchId`, terminal status, message, and the same counters.

The `resultHandle` is opaque. Workflow code may pass it to downstream activities but must not deserialize chunk storage directly or materialize all per-item results into workflow state.

### Execution Options

Throughput mode supports these execution policies:

- `execution: "default"` — throughput runtime behavior is backend-specific but still barrier-based and replay-safe
- `execution: "eager"` — chunk work may begin and complete before workflow observation; intermediate progress stays non-authoritative

The `execution` option is a semantic choice. It is not a backend selector.

### Reducers

Initial reducers are built-in and deterministic:

- `all_succeeded`
- `all_settled`
- `count`
- `collect_results`

`all_succeeded`, `all_settled`, and `count` are the mergeable reducers used by the `stream-v2` fast lane. `collect_results` remains supported, but stays on the legacy result-materialization path rather than the optimized mergeable reduction path.

User-defined reducers are out of scope until they can be compiled, version-pinned, and replay-safe.

`collect_results` remains handle-backed rather than workflow-memory-backed. It may return summary metadata plus an opaque result handle, but it must not force full per-item result materialization inside workflow state.

## Non-Workflow Progress Reads

Intermediate progress reads are useful but must not affect replayed workflow execution.

Progress reads therefore live outside normal workflow execution semantics:

- external client/query APIs
- operator/debug APIs
- optional read-only query handlers, if they do not affect workflow state transitions

Example client-side reads:

```ts
const progress = await client.bulkBatch(batchId).peek({
  consistency: "eventual",
});
```

```ts
const live = await client.bulkBatch(batchId).peek({
  consistency: "strong",
});
```

Hard rule:

- `peek()` is non-durable, non-replayed, and non-authoritative
- `peek()` must never influence workflow state transitions

## Semantics

The semantic contract for throughput mode is:

- barrier-only workflow wakeups
- chunk-level retry and coarse cancellation
- batch/chunk visibility only; no per-item indexed visibility
- at-least-once chunk execution
- batch-level workflow history events only
- mixed durable and bulk steps are allowed in one workflow

The semantic contract for eager execution is:

- workflow state remains single-owner and strongly ordered
- eager bulk work may start and finish before the workflow observes it
- intermediate batch/chunk progress is projection-only
- only terminal batch outcomes are workflow-authoritative
- workers never directly mutate workflow state
- owner-applied reduction produces one durable batch outcome

### Consistency Levels

#### Authoritative

Used for:

- workflow history
- workflow owner state
- terminal batch outcomes committed into workflow history

#### Strong

Used for owner-routed live reads:

- workflow strong reads route to the workflow owner
- batch strong reads route to the throughput owner

#### Eventual

Used for:

- projections
- batch/chunk progress
- operator and UI status
- cheap status reads

Eventual reads may lag authoritative state and are not replay-stable.

### Guardrails

Eager execution is valid only for workloads with these properties:

- no per-item workflow-visible side effects
- no dependence on exact in-order item completion
- no direct workflow mutation from workers
- dedupe-safe chunk completion
- deterministic batch terminal reduction
- tolerance for eventual intermediate visibility
- no requirement for per-item replay into workflow code

Good fits:

- enrich
- score
- classify
- validate
- transform
- embed
- mergeable map-style batch work

Bad fits:

- per-item workflow-visible effects
- order-sensitive orchestration
- workflows that branch on intermediate chunk progress
- workloads requiring per-item audit or replay semantics in workflow history

### Workflow Cancellation Fanout

- `WorkflowCancellationRequested` fans out to nonterminal bulk batches
- remaining scheduled or started chunks are marked cancelled coarsely at the batch level
- late reports from previously leased chunks are ignored by fencing once cancellation clears active leases
- exactly one terminal batch cancellation outcome is emitted into workflow history

## Backends

Throughput mode supports multiple backend implementations behind a stable internal interface. Backend selection is server-controlled per batch and pinned for the lifetime of that batch.

Workflow code does not select `pg-v1` vs `stream-v2`. Selection is made by task-queue policy or other server-side admission and routing rules.

### Common Backend Interface

Both backends implement the same operations:

| Operation | Description |
|---|---|
| `create_batch` | Persist batch metadata and chunk manifest |
| `poll_chunks` | Lease scheduled chunks by task queue and build compatibility |
| `report_chunk_terminal` | Apply chunk completion, failure, or cancellation |
| `cancel_batch` | Stop leasing new chunks and terminate the batch |
| `get_batch_summary` | Read batch metadata and counters |
| `get_batch_results` | Paginated chunk outputs in chunk order |

Both backends preserve these stable identifiers:

- `batch_id`, `chunk_id`, `attempt`, `group_id`
- `lease_epoch`, `owner_epoch`, `report_id`
- `input_handle`, `result_handle`
- `throughput_backend`, `throughput_backend_version`

### `pg-v1` — Postgres-First Backend

The Postgres-first backend is suitable for most bulk workloads up to hundreds of thousands of items.

Infrastructure requirements:

- Postgres

Behavior:

- batch and chunk state is stored in `workflow_bulk_batches` and `workflow_bulk_chunks`
- chunk scheduling and leasing runs through Postgres queries
- chunk terminal results apply atomic counter updates in Postgres
- batch terminal events publish through the async `workflow_event_outbox`
- query results are immediately consistent

Tradeoffs:

- no additional infrastructure beyond what `fabrik` already requires
- operationally simple and queryable with standard SQL
- Postgres becomes the throughput ceiling for very large batches or very high completion rates

### `stream-v2` — Streaming Throughput Backend

The streaming backend removes Postgres from the throughput hot path and is suitable for batches with millions of items or sustained high completion rates.

Infrastructure requirements:

- Postgres for projections
- Redpanda or Kafka for command, report, and changelog streams
- RocksDB for shard-local state
- checkpoint storage
- S3-compatible object storage for large payload manifests

Behavior:

- authoritative state lives in the throughput shard runtime, not Postgres
- dedicated `throughput-runtime` owns throughput shards independently of workflow executors
- dedicated `throughput-projector` consumes projection events and updates Postgres asynchronously
- workers use a dedicated throughput worker RPC served by the throughput owner
- local owner state persists counters, retry scheduling fields, lease deadlines, and group/barrier state
- terminal workflow events are emitted directly by the throughput runtime
- chunk inputs and outputs may be externalized behind payload handles
- query results are eventually consistent unless routed to the active throughput owner

Log roles:

| Log | Contents | Purpose |
|---|---|---|
| Command | `CreateBatch`, `CancelBatch`, `TimeoutBatch` | Intent from workflow bridge |
| Report | `ChunkCompleted`, `ChunkFailed`, `ChunkCancelled` | Raw worker observations |
| Owner changelog | `BatchCreated`, `ChunkLeased`, `ChunkRequeued`, `ChunkApplied`, `GroupTerminal`, `BatchTerminal` | Restore and audit |

Fencing protocol:

Every leased chunk carries `(chunk_id, attempt, lease_epoch, lease_token, owner_epoch, report_id)`. A report is valid only if it matches the active lease token and current owner epoch. Stale reports are retained for audit but do not mutate state. Retries advance `lease_epoch` and mint a new `lease_token`. Failover advances `owner_epoch`. Exactly one terminal workflow event is emitted per batch.

Tradeoffs:

- significantly higher throughput ceiling than `pg-v1`
- requires additional infrastructure
- batch/chunk visibility is eventually consistent by default
- operationally more complex

Admission control:

- `throughput-runtime` enforces active chunk caps per batch, per tenant, and per task queue
- when a cap is hit, the runtime stops leasing new chunks until capacity is available
- throttle reasons are surfaced through the runtime debug endpoint

Ownership and rebalancing:

- runtime instances heartbeat membership with advertised capacity and debug endpoints
- reconciles compute partition assignments from active membership
- only the active owner of a partition may poll chunks, process reports, or run lease-expiry sweeps
- ownership remains active/passive, not dual-active

### Backend Pinning

`throughput_backend` and `throughput_backend_version` are recorded on every batch. In-flight batches always finish on the backend that started them. Batches are never migrated across backends while active.

## Execution Model

Eager execution reuses throughput mode internals as much as possible.

Lifecycle:

1. workflow schedules a bulk activity with `execution: "eager"`
2. batch is durably created
3. throughput runtime begins leasing chunks immediately
4. workers execute chunk work and submit fenced terminal reports
5. throughput runtime reduces reports into batch state
6. at barrier resolution, the runtime emits exactly one terminal workflow event:
   - `BulkActivityBatchCompleted`
   - `BulkActivityBatchFailed`
   - `BulkActivityBatchCancelled`

Key rule:

- intermediate progress does not resume the workflow
- only terminal barrier resolution resumes the workflow

## Workflow History

Throughput mode emits batch-level workflow history events only:

- `BulkActivityBatchScheduled`
- `BulkActivityBatchCompleted`
- `BulkActivityBatchFailed`
- `BulkActivityBatchCancelled`

Per-item and per-chunk events are not written to workflow history. Chunk-level visibility is available through dedicated query endpoints, not through workflow replay.

Eager execution does not introduce a separate workflow-visible event family unless a future runtime requirement makes that unavoidable. Execution policy is recorded as batch and event metadata, not as a separate semantic track.

## IR Nodes

The compiled workflow artifact continues to use the normal throughput-mode IR shape:

- `start_bulk_activity` — creates the batch and chunk manifest
- `wait_for_bulk_activity` — blocks until the batch terminal event arrives

These nodes carry batch metadata such as:

- `execution_policy`
- `reducer`
- read-consistency capabilities

The compiler should reuse the existing bulk handle pattern rather than introducing a separate eager workflow model.

## Query Endpoints

Batch and chunk visibility is exposed through additive query endpoints:

- `GET /tenants/{tenant_id}/workflows/{instance_id}/runs/{run_id}/bulk-batches`
- `GET /tenants/{tenant_id}/workflows/{instance_id}/runs/{run_id}/bulk-batches/{batch_id}`
- `GET /tenants/{tenant_id}/workflows/{instance_id}/runs/{run_id}/bulk-batches/{batch_id}/chunks`
- `GET /tenants/{tenant_id}/workflows/{instance_id}/runs/{run_id}/bulk-batches/{batch_id}/results`

`/results` is paginated by chunk order and returns chunk output arrays. There is no per-item indexed visibility API.

Read consistency is a query concern, not an execution concern. Query surfaces should accept `consistency=strong` or `consistency=eventual`.

Strong reads:

- workflow status routes to the workflow owner
- live batch status routes to the throughput owner

Eventual reads:

- batch progress
- chunk progress
- lag-aware operator and UI views

For `pg-v1`, query results are immediately consistent. For `stream-v2`, default query results are eventually consistent projections and may lag authoritative shard state.

## Worker Protocol

Throughput mode uses dedicated gRPC messages for chunk polling and reporting, separate from the single-activity worker protocol.

Bulk task payloads include:

- batch/chunk identity
- activity type and task queue
- attempt number
- ordered item array
- chunk input metadata

Bulk result payloads report one chunk terminal outcome:

- completed with ordered output array
- failed with an error string
- cancelled with a reason and optional metadata

Workers process items sequentially within a chunk and preserve order in the output array. Throughput comes from chunk amortization and worker parallelism across chunks, not intra-chunk parallelism.

Workers never directly mutate workflow state.

## Cancellation

Cancellation is coarse-grained at the batch level:

- pending chunks stop leasing immediately
- started chunks observe cancellation between item executions
- in-flight chunks that complete after cancellation have results discarded
- one terminal cancellation event is emitted per batch

## Retry

Retry operates at the chunk level:

- failed chunks are retried up to `maxAttempts`
- retry re-executes all items in the chunk
- activities invoked via throughput mode should be idempotent
- if any chunk permanently fails, the default completion policy fails the batch

## Comparison with Durable Mode Activities

| Aspect | Durable mode (`ctx.activity`) | Throughput mode (`ctx.bulkActivity`) |
|---|---|---|
| Durability unit | per activity | per chunk |
| Workflow history | per-activity events | batch-level events only |
| Workflow task wakeups | per activity completion | one per batch terminal |
| Retry granularity | per activity | per chunk |
| Cancellation granularity | per activity | per batch |
| Result visibility | per activity in workflow state | opaque handle plus query surfaces |
| Overhead scaling | `O(items)` | `O(chunks)` |
| Use case | individual critical operations | high-cardinality fan-out / fan-in |
