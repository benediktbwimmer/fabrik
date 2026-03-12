# Throughput Mode

## Purpose

Throughput mode is an explicit workflow primitive for high-cardinality fan-out / fan-in workloads. It trades per-item durable history for chunk-level durability, reducing workflow overhead from `O(items)` to `O(chunks)`.

## Product Contract

### API Surface

Workflows opt in to throughput mode per call-site using `ctx.bulkActivity()`:

```ts
const bulk = await ctx.bulkActivity("process.enrich", items, {
  taskQueue: "default",
  chunkSize: 256,
  backend: "pg-v1",
  retry: { maxAttempts: 2, delay: "1s" },
});

const summary = await bulk.result();
```

- `ctx.bulkActivity()` returns a handle immediately after durable batch/chunk creation.
- `await handle.result()` blocks the workflow until the batch reaches a terminal state.
- `items` may be any runtime expression evaluating to an array.
- `taskQueue`, `chunkSize`, `backend`, and `retry` are static literal options.
- `chunkSize` defaults to `256` when omitted.

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

The `resultHandle` is opaque. Workflow code may pass it to downstream activities but must not read chunk storage directly.

### Semantics

- barrier-only workflow wakeups (one resume per batch terminal)
- chunk-level retry and coarse cancellation
- batch/chunk visibility only; no per-item indexed visibility
- at-least-once chunk execution
- batch-level workflow history events only
- mixed durable and bulk steps are allowed in one workflow

Workflow cancellation fanout:

- `WorkflowCancellationRequested` fans out to nonterminal bulk batches
- v2.0 uses immediate coarse batch cancellation: remaining scheduled or started chunks are marked cancelled and one batch terminal event is emitted
- late reports from previously leased chunks are ignored by fencing once the batch cancellation clears active leases

## Backends

Throughput mode supports multiple backend implementations behind a stable internal interface. The backend is selected per batch and pinned for the lifetime of that batch.

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

The default backend. Suitable for most bulk workloads up to hundreds of thousands of items.

Infrastructure requirements:

- Postgres (shared with the rest of the platform)

Behavior:

- batch and chunk state stored in `workflow_bulk_batches` and `workflow_bulk_chunks` tables
- chunk scheduling and leasing through Postgres queries
- chunk terminal results applied with atomic counter updates in Postgres
- batch terminal event published via an async `workflow_event_outbox` table
- the transaction that marks a batch terminal also writes the outbox record and enqueues the workflow resume

Tradeoffs:

- no additional infrastructure beyond what `fabrik` already requires
- operationally simple — batch state is queryable with standard SQL
- Postgres becomes the throughput ceiling for very large batches or very high chunk completion rates
- batch/chunk visibility is immediately consistent

When to use:

- batches with up to ~100K items
- workloads where operational simplicity is more important than maximum throughput
- development and testing environments

### `stream-v2` — Streaming Throughput Backend

A specialized streaming backend that removes Postgres from the throughput hot path. Suitable for batches with millions of items or sustained high completion rates.

Infrastructure requirements:

- Postgres (projection-only; not on the hot path)
- Redpanda / Kafka (command log, report log, owner changelog)
- RocksDB (shard-local state)
- checkpoint storage
  - current implementation: S3-compatible object storage checkpoints with local filesystem fallback for restore compatibility
- S3-compatible object storage for large payload manifests
  - local compose stack: MinIO

Behavior:

- authoritative state lives in the throughput shard runtime, not Postgres
- three log roles: command log, report log, owner changelog
- recovery from latest checkpoint plus owner changelog tail
- current implementation restores from the latest object-store checkpoint plus owner changelog replay
- dedicated `throughput-runtime` service owns throughput shards independently of workflow executors
- dedicated `throughput-projector` service consumes throughput projection events and updates Postgres asynchronously
- ownership can run in either static-partition mode or assignment mode
  - static mode: each runtime instance is configured with explicit throughput partition ids
  - assignment mode: runtimes heartbeat membership and advertised capacity, then reconcile partition assignments periodically
- workers use a dedicated throughput worker RPC served by the shard owner
- fencing protocol ensures at-most-once state application and exactly one terminal event per batch
- local RocksDB state persists batch counters, retry scheduling fields, and lease deadlines so replay rebuilds a usable owner snapshot instead of only coarse statuses
- new `stream-v2` batches are materialized directly into owner state plus throughput projection rows; they are no longer inserted into `workflow_bulk_batches` / `workflow_bulk_chunks`
- when `aggregation_group_count > 1`, the runtime assigns chunks across groups deterministically by chunk index and routes each group through its own throughput partition
- if the caller leaves `aggregation_group_count = 1`, the runtime may still auto-enable grouping for large batches using server-side chunk-count policy
- throughput partitions use an explicit lease-based ownership protocol; only the active owner of a partition may poll chunks, process reports, or run lease-expiry sweeps for that shard
- in assignment mode, ownership is rebalanced through the `workflow_throughput_membership` and `workflow_throughput_partition_assignments` tables; stale assignment rows are pruned on every reconcile so the partition map converges after topology changes
- the owner runtime computes expected chunk and batch transitions locally before applying a terminal report, then records divergence if the store result differs
- grouped batches keep chunk execution and retries shard-local, but only the owner of group `0` emits the batch terminal event
- grouped batches now emit explicit `GroupTerminal` owner decisions before the parent barrier emits the single batch terminal event
- success completion for grouped batches waits for the parent barrier to observe every group terminal; permanent failure or cancellation still resolves the batch as soon as that terminal outcome is visible
- terminal projection updates for `stream-v2` are applied from the owner-derived transition, not copied back from the execution tables
- lease-expiry requeues are also projected from owner state and persisted through the owner changelog without reloading the chunk from Postgres
- terminal batches, chunks, and group summaries are pruned from local RocksDB owner state after `THROUGHPUT_TERMINAL_STATE_RETENTION_SECONDS`; long-lived visibility stays in Postgres projections and object-backed manifests
- terminal workflow events for `stream-v2` are now published directly by `throughput-runtime` instead of being emitted through the bulk execution tables' outbox
- chunk inputs and outputs may be externalized behind per-chunk `input_handle` / `result_handle` manifests once they cross configured inline-size thresholds
- current implementation supports both local filesystem and S3-compatible manifest storage, selected by `THROUGHPUT_PAYLOAD_STORE`
- the local compose stack uses MinIO with path-style S3 requests for throughput payload manifests
- throughput checkpoints are stored in the same configured object store under `THROUGHPUT_CHECKPOINT_KEY_PREFIX`
- worker polling and query reads resolve those handles transparently, so the public API stays unchanged while large payloads move out of Postgres rows
- Postgres receives async projections for visibility queries through the throughput projection log
- the throughput projector only rebuilds batch result manifests for chunk updates that actually carry output or a non-null `result_handle`

Log roles:

| Log | Contents | Purpose |
|---|---|---|
| Command | `CreateBatch`, `CancelBatch`, `TimeoutBatch` | Intent from workflow bridge |
| Report | `ChunkCompleted`, `ChunkFailed`, `ChunkCancelled` | Raw worker observations |
| Owner changelog | `BatchCreated`, `ChunkLeased`, `ChunkRequeued`, `ChunkApplied`, `GroupTerminal`, `BatchTerminal` | Restore and audit, including lease-expiry reschedules, group barrier decisions, retry schedule fields, and terminal counter snapshots |

Fencing protocol:

Every leased chunk carries `(chunk_id, attempt, lease_epoch, lease_token, owner_epoch, report_id)`. A report is valid only if it matches the active lease token and current owner epoch. Stale reports are retained for audit but do not mutate state. Retries advance `lease_epoch` and mint a new `lease_token`. Failover advances `owner_epoch`. Exactly one terminal workflow event is emitted per batch.

Tradeoffs:

- significantly higher throughput ceiling than `pg-v1`
- requires additional infrastructure (RocksDB, S3, throughput-runtime service)
- batch/chunk visibility is eventually consistent (projection lag)
- more complex operational model

Admission control:

- `throughput-runtime` enforces active chunk caps per batch, per tenant, and per task queue
- when a cap is hit, the runtime stops leasing new chunks until capacity is available
- throttle reasons are surfaced through the runtime debug endpoint

Ownership and rebalancing:

- each runtime instance heartbeats a membership record with an advertised capacity and debug query endpoint
- reconciles compute partition assignments from the active membership set
- runtime instances only claim or renew leases for partitions assigned to them
- replicas already stay warm by continuously applying the owner changelog into local RocksDB, so failover does not require a cold restore
- in assignment mode, a partition can transfer as soon as the old owner falls out of throughput membership; failover does not have to wait for the full ownership lease TTL
- a runtime only claims a newly assigned partition once its local changelog mirror has caught up to the observed high watermark for that partition
- ownership is still active/passive, not dual-active: only one runtime may hold the lease for a partition at a time

When to use:

- batches with millions of items
- sustained high completion rates where Postgres is a bottleneck
- workloads where throughput matters more than operational simplicity

### Backend Selection

Backend is specified per `ctx.bulkActivity()` call via the `backend` option:

```ts
// Uses default Postgres backend
const bulk1 = await ctx.bulkActivity("process", items);

// Explicitly selects streaming backend
const bulk2 = await ctx.bulkActivity("process", items, {
  backend: "stream-v2",
});
```

If omitted, the default is `pg-v1`. Server-side task queue configuration may override the default backend for specific queues.

`throughput_backend` and `throughput_backend_version` are recorded on every batch. In-flight batches always finish on the backend that started them; batches are never migrated across backends.

## Workflow History

Throughput mode emits batch-level workflow history events only:

- `BulkActivityBatchScheduled`
- `BulkActivityBatchCompleted`
- `BulkActivityBatchFailed`
- `BulkActivityBatchCancelled`

Per-item and per-chunk events are not written to workflow history. Chunk-level visibility is available through dedicated query endpoints, not through workflow replay.

## IR Nodes

The compiled workflow artifact includes two throughput-mode IR nodes:

- `start_bulk_activity` — creates the batch and chunk manifest
- `wait_for_bulk_activity` — blocks until the batch terminal event arrives

These follow the same handle pattern as child workflows.

## Query Endpoints

Batch and chunk visibility is exposed through additive query endpoints:

- `GET /tenants/{tenant_id}/workflows/{instance_id}/runs/{run_id}/bulk-batches`
- `GET /tenants/{tenant_id}/workflows/{instance_id}/runs/{run_id}/bulk-batches/{batch_id}`
- `GET /tenants/{tenant_id}/workflows/{instance_id}/runs/{run_id}/bulk-batches/{batch_id}/chunks`
- `GET /tenants/{tenant_id}/workflows/{instance_id}/runs/{run_id}/bulk-batches/{batch_id}/results`

`/results` is paginated by chunk order and returns chunk output arrays. There is no per-item indexed visibility API.

For `pg-v1`, query results are immediately consistent. For `stream-v2`, query results are eventually consistent projections and may lag behind authoritative shard state.

## Worker Protocol

Throughput mode uses dedicated gRPC messages for chunk polling and reporting, separate from the single-activity worker protocol.

Bulk task payloads include:

- batch/chunk identity
- activity type and task queue
- attempt number
- ordered item array
- chunk input metadata

Bulk result payloads report one chunk terminal outcome:

- completed (with ordered output array)
- failed (with error string)
- cancelled (with reason and optional metadata)

Workers process items sequentially within a chunk and preserve order in the output array. Throughput comes from chunk amortization and worker parallelism across chunks, not intra-chunk parallelism.

## Cancellation

Cancellation is coarse-grained at the batch level:

- pending chunks stop leasing immediately
- started chunks observe cancellation between item executions
- in-flight chunks that complete after cancellation have results discarded
- one terminal cancellation event is emitted per batch

## Retry

Retry operates at the chunk level:

- failed chunks are retried up to `maxAttempts`
- retry re-executes all items in the chunk (the chunk is the atomic unit)
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
| Result visibility | per activity in workflow state | opaque handle, paginated chunk query |
| Overhead scaling | `O(items)` | `O(chunks)` |
| Use case | individual critical operations | high-cardinality fan-out / fan-in |
