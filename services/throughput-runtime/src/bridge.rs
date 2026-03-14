use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use fabrik_broker::{decode_consumed_workflow_event, decode_json_record};
use fabrik_events::{EventEnvelope, WorkflowEvent, WorkflowIdentity};
use fabrik_store::{StreamJobCheckpointRecord, StreamJobRecord};
use fabrik_throughput::{
    StreamJobBridgeHandleStatus, StreamJobQueryConsistency, StreamJobQueryStatus, StreamJobStatus,
    ThroughputBackend, ThroughputBatchIdentity, ThroughputCommand, ThroughputCommandEnvelope,
    ThroughputRunStatus, stream_job_bridge_request_id, stream_job_callback_event_id,
    stream_job_checkpoint_callback_dedupe_key, stream_job_query_callback_dedupe_key,
    stream_job_terminal_callback_dedupe_key,
};
use futures_util::StreamExt;
use serde::Deserialize;
use serde_json::json;
use std::collections::BTreeMap;
use tokio::task::JoinSet;
use tokio::time::{Duration, sleep};
use tracing::{error, warn};
use uuid::Uuid;

use crate::{
    AppState, BRIDGE_EVENT_CONCURRENCY, activate_native_stream_run, activate_stream_batch,
    workflow_event_from_throughput_command,
};

pub(super) async fn run_bridge_loop(
    state: AppState,
    mut consumer: fabrik_broker::WorkflowConsumerStream,
) {
    let mut in_flight = JoinSet::new();
    while let Some(message) = consumer.next().await {
        let record = match message {
            Ok(record) => record,
            Err(error) => {
                error!(error = %error, "streams-runtime consumer error");
                continue;
            }
        };
        let event = match decode_consumed_workflow_event(&record) {
            Ok(event) => event,
            Err(error) => {
                warn!(error = %error, "streams-runtime skipping invalid workflow event");
                continue;
            }
        };
        while in_flight.len() >= BRIDGE_EVENT_CONCURRENCY {
            if let Some(result) = in_flight.join_next().await {
                if let Err(error) = result {
                    error!(error = %error, "streams-runtime bridge worker task failed");
                }
            }
        }
        let state = state.clone();
        let debug_state = state.clone();
        in_flight.spawn(async move {
            if let Err(error) = handle_bridge_event(state, event).await {
                let mut debug = debug_state.debug.lock().expect("throughput debug lock poisoned");
                debug.bridge_failures = debug.bridge_failures.saturating_add(1);
                drop(debug);
                error!(error = %error, "streams-runtime failed to handle workflow bridge event");
            }
        });
    }

    while let Some(result) = in_flight.join_next().await {
        if let Err(error) = result {
            error!(error = %error, "streams-runtime bridge worker task failed");
        }
    }
}

async fn handle_bridge_event(state: AppState, event: EventEnvelope<WorkflowEvent>) -> Result<()> {
    if let WorkflowEvent::WorkflowCancellationRequested { reason } = &event.payload {
        crate::cancel_stream_batches_for_run(&state, &event, reason).await?;
        return Ok(());
    }

    if let WorkflowEvent::StreamJobScheduled { job_id, .. } = &event.payload {
        publish_stream_job_callbacks_from_bridge(&state, &event, job_id).await?;
        return Ok(());
    }

    if let WorkflowEvent::StreamJobCancellationRequested { job_id, .. } = &event.payload {
        if let Some(handle) = await_stream_job_bridge_handle(&state, &event, job_id).await? {
            sync_stream_job_record(
                &state,
                &handle,
                StreamJobStatus::Draining,
                None,
                event.occurred_at,
            )
            .await?;
            publish_stream_job_terminal_event(&state, &handle).await?;
        }
        return Ok(());
    }

    if let WorkflowEvent::StreamJobQueryRequested { job_id, query_id, .. } = &event.payload {
        publish_stream_job_query_callback_from_bridge(&state, &event, job_id, query_id).await?;
        return Ok(());
    }

    let WorkflowEvent::BulkActivityBatchScheduled {
        batch_id,
        task_queue: _,
        activity_type: _,
        items: _,
        input_handle: _,
        result_handle: _,
        chunk_size: _,
        max_attempts: _,
        retry_delay_ms: _,
        aggregation_group_count: _,
        execution_policy: _,
        reducer: _,
        throughput_backend,
        throughput_backend_version: _,
        state: _,
        ..
    } = &event.payload
    else {
        return Ok(());
    };
    if throughput_backend != ThroughputBackend::StreamV2.as_str() {
        return Ok(());
    }
    if state
        .store
        .get_throughput_run(&event.tenant_id, &event.instance_id, &event.run_id, batch_id)
        .await?
        .is_some_and(|run| run.command_published_at.is_some())
    {
        let mut debug = state.debug.lock().expect("throughput debug lock poisoned");
        debug.duplicate_bridge_events = debug.duplicate_bridge_events.saturating_add(1);
        return Ok(());
    }

    let dedupe_key = format!("throughput-create:{}", event.event_id);
    let published_at = state
        .store
        .ensure_throughput_bridge_progress(
            event.event_id,
            &event.tenant_id,
            &event.instance_id,
            &event.run_id,
            batch_id,
            throughput_backend,
            &dedupe_key,
            event.occurred_at,
        )
        .await
        .with_context(|| format!("failed to record throughput bridge progress for {batch_id}"))?;
    if published_at.is_some() {
        let mut debug = state.debug.lock().expect("throughput debug lock poisoned");
        debug.duplicate_bridge_events = debug.duplicate_bridge_events.saturating_add(1);
        return Ok(());
    }

    activate_stream_batch(&state, &event).await?;

    state
        .store
        .mark_throughput_bridge_command_published(event.event_id, Utc::now())
        .await
        .with_context(|| {
            format!("failed to mark throughput create command published for {batch_id}")
        })?;
    let mut debug = state.debug.lock().expect("throughput debug lock poisoned");
    debug.bridged_batches = debug.bridged_batches.saturating_add(1);
    debug.last_bridge_at = Some(Utc::now());
    drop(debug);
    state.bulk_notify.notify_waiters();
    Ok(())
}

fn checkpoint_is_newer(
    current_sequence: Option<i64>,
    current_at: Option<DateTime<Utc>>,
    checkpoint: &StreamJobCheckpointRecord,
) -> bool {
    match (checkpoint.checkpoint_sequence, current_sequence) {
        (Some(next), Some(current)) if next != current => next > current,
        _ => checkpoint.reached_at >= current_at,
    }
}

async fn sync_stream_job_record(
    state: &AppState,
    handle: &fabrik_store::StreamJobBridgeHandleRecord,
    status: StreamJobStatus,
    latest_checkpoint: Option<&StreamJobCheckpointRecord>,
    occurred_at: DateTime<Utc>,
) -> Result<()> {
    let mut record = state
        .store
        .get_stream_job(&handle.tenant_id, &handle.instance_id, &handle.run_id, &handle.job_id)
        .await?
        .unwrap_or(StreamJobRecord {
            tenant_id: handle.tenant_id.clone(),
            instance_id: handle.instance_id.clone(),
            run_id: handle.run_id.clone(),
            job_id: handle.job_id.clone(),
            handle_id: handle.handle_id.clone(),
            protocol_version: handle.protocol_version.clone(),
            operation_kind: handle.operation_kind.clone(),
            workflow_event_id: handle.workflow_event_id,
            bridge_request_id: handle.bridge_request_id.clone(),
            definition_id: handle.definition_id.clone(),
            definition_version: handle.definition_version,
            artifact_hash: handle.artifact_hash.clone(),
            job_name: handle.job_name.clone(),
            input_ref: handle.input_ref.clone(),
            config_ref: handle.config_ref.clone(),
            checkpoint_policy: handle.checkpoint_policy.clone(),
            view_definitions: handle.view_definitions.clone(),
            status: StreamJobStatus::Created.as_str().to_owned(),
            workflow_owner_epoch: handle.workflow_owner_epoch,
            stream_owner_epoch: handle.stream_owner_epoch,
            starting_at: None,
            running_at: None,
            draining_at: None,
            latest_checkpoint_name: None,
            latest_checkpoint_sequence: None,
            latest_checkpoint_at: None,
            latest_checkpoint_output: None,
            cancellation_requested_at: handle.cancellation_requested_at,
            cancellation_reason: handle.cancellation_reason.clone(),
            workflow_accepted_at: handle.workflow_accepted_at,
            terminal_event_id: handle.terminal_event_id,
            terminal_at: handle.terminal_at,
            terminal_output: handle.terminal_output.clone(),
            terminal_error: handle.terminal_error.clone(),
            created_at: handle.created_at,
            updated_at: handle.updated_at,
        });

    record.handle_id = handle.handle_id.clone();
    record.protocol_version = handle.protocol_version.clone();
    record.operation_kind = handle.operation_kind.clone();
    record.workflow_event_id = handle.workflow_event_id;
    record.bridge_request_id = handle.bridge_request_id.clone();
    record.definition_id = handle.definition_id.clone();
    record.definition_version = handle.definition_version;
    record.artifact_hash = handle.artifact_hash.clone();
    record.job_name = handle.job_name.clone();
    record.input_ref = handle.input_ref.clone();
    record.config_ref = handle.config_ref.clone();
    record.checkpoint_policy = handle.checkpoint_policy.clone();
    record.view_definitions = handle.view_definitions.clone();
    record.status = status.as_str().to_owned();
    record.workflow_owner_epoch = handle.workflow_owner_epoch;
    record.stream_owner_epoch = handle.stream_owner_epoch;
    record.cancellation_requested_at = handle.cancellation_requested_at;
    record.cancellation_reason = handle.cancellation_reason.clone();
    record.workflow_accepted_at = handle.workflow_accepted_at;
    record.terminal_event_id = handle.terminal_event_id;
    record.terminal_at = handle.terminal_at;
    record.terminal_output = handle.terminal_output.clone();
    record.terminal_error = handle.terminal_error.clone();

    match status {
        StreamJobStatus::Created => {}
        StreamJobStatus::Starting => {
            if record.starting_at.is_none() {
                record.starting_at = Some(occurred_at);
            }
        }
        StreamJobStatus::Running => {
            if record.starting_at.is_none() {
                record.starting_at = Some(occurred_at);
            }
            if record.running_at.is_none() {
                record.running_at = Some(occurred_at);
            }
        }
        StreamJobStatus::Draining => {
            if record.starting_at.is_none() {
                record.starting_at = Some(occurred_at);
            }
            if record.running_at.is_none() {
                record.running_at = Some(occurred_at);
            }
            if record.draining_at.is_none() {
                record.draining_at = Some(handle.cancellation_requested_at.unwrap_or(occurred_at));
            }
        }
        StreamJobStatus::Completed | StreamJobStatus::Failed | StreamJobStatus::Cancelled => {
            if record.starting_at.is_none() {
                record.starting_at = Some(occurred_at);
            }
            if record.running_at.is_none() {
                record.running_at = Some(occurred_at);
            }
            if record.terminal_at.is_none() {
                record.terminal_at = Some(occurred_at);
            }
        }
    }

    if let Some(checkpoint) = latest_checkpoint
        && checkpoint_is_newer(
            record.latest_checkpoint_sequence,
            record.latest_checkpoint_at,
            checkpoint,
        )
    {
        record.latest_checkpoint_name = Some(checkpoint.checkpoint_name.clone());
        record.latest_checkpoint_sequence = checkpoint.checkpoint_sequence;
        record.latest_checkpoint_at = checkpoint.reached_at;
        record.latest_checkpoint_output = checkpoint.output.clone();
    }

    record.updated_at = occurred_at;
    state.store.upsert_stream_job(&record).await
}

async fn publish_stream_job_callbacks_from_bridge(
    state: &AppState,
    event: &EventEnvelope<WorkflowEvent>,
    job_id: &str,
) -> Result<()> {
    let Some(handle) = await_stream_job_bridge_handle(state, event, job_id).await? else {
        return Ok(());
    };
    sync_stream_job_record(state, &handle, StreamJobStatus::Starting, None, event.occurred_at)
        .await?;
    if handle.job_name == KEYED_ROLLUP_JOB_NAME {
        materialize_keyed_rollup_checkpoint(state, &handle).await?;
    }
    if handle.terminal_at.is_some() || handle.terminal_event_id.is_some() {
        let mut debug = state.debug.lock().expect("throughput debug lock poisoned");
        debug.duplicate_bridge_events = debug.duplicate_bridge_events.saturating_add(1);
        return Ok(());
    }

    // Give the workflow runtime a short window to persist awaited checkpoints before
    // we synthesize callbacks from the current Streams bridge implementation.
    sleep(Duration::from_millis(100)).await;
    let checkpoints = state
        .store
        .list_stream_job_bridge_checkpoints_for_handle_page(&handle.handle_id, 10_000, 0)
        .await?;
    for checkpoint in checkpoints {
        if checkpoint.reached_at.is_some() || checkpoint.accepted_at.is_some() {
            continue;
        }
        publish_stream_job_checkpoint_event(state, &handle, &checkpoint).await?;
    }
    let refreshed_handle = state
        .store
        .get_stream_job_bridge_handle_by_handle_id(&handle.handle_id)
        .await?
        .unwrap_or(handle);
    if refreshed_handle.terminal_at.is_some() || refreshed_handle.terminal_event_id.is_some() {
        return Ok(());
    }
    publish_stream_job_terminal_event(state, &refreshed_handle).await?;
    Ok(())
}

async fn await_stream_job_bridge_handle(
    state: &AppState,
    event: &EventEnvelope<WorkflowEvent>,
    job_id: &str,
) -> Result<Option<fabrik_store::StreamJobBridgeHandleRecord>> {
    for _ in 0..10 {
        if let Some(handle) = state
            .store
            .get_stream_job_bridge_handle(
                &event.tenant_id,
                &event.instance_id,
                &event.run_id,
                job_id,
            )
            .await?
        {
            return Ok(Some(handle));
        }
        sleep(Duration::from_millis(25)).await;
    }
    Ok(None)
}

async fn await_stream_job_bridge_query(
    state: &AppState,
    query_id: &str,
) -> Result<Option<fabrik_store::StreamJobQueryRecord>> {
    for _ in 0..10 {
        if let Some(query) = state.store.get_stream_job_bridge_query(query_id).await? {
            return Ok(Some(query));
        }
        sleep(Duration::from_millis(25)).await;
    }
    Ok(None)
}

async fn publish_stream_job_checkpoint_event(
    state: &AppState,
    handle: &fabrik_store::StreamJobBridgeHandleRecord,
    checkpoint: &fabrik_store::StreamJobCheckpointRecord,
) -> Result<()> {
    let identity = WorkflowIdentity::new(
        handle.tenant_id.clone(),
        handle.definition_id.clone(),
        handle.definition_version.unwrap_or_default(),
        handle.artifact_hash.clone().unwrap_or_default(),
        handle.instance_id.clone(),
        handle.run_id.clone(),
        "throughput-runtime",
    );
    let payload = WorkflowEvent::StreamJobCheckpointReached {
        job_id: handle.job_id.clone(),
        handle_id: handle.handle_id.clone(),
        checkpoint_name: checkpoint.checkpoint_name.clone(),
        checkpoint_sequence: checkpoint.checkpoint_sequence.unwrap_or(0),
        output: Some(json!({
            "jobId": handle.job_id,
            "jobName": handle.job_name,
            "checkpoint": checkpoint.checkpoint_name,
            "sequence": checkpoint.checkpoint_sequence.unwrap_or(0),
        })),
        stream_owner_epoch: Some(1),
    };
    let dedupe_key = stream_job_checkpoint_callback_dedupe_key(
        &handle.tenant_id,
        &handle.instance_id,
        &handle.run_id,
        &handle.job_id,
        &checkpoint.checkpoint_name,
        checkpoint.checkpoint_sequence.unwrap_or(0),
    );
    let mut envelope = EventEnvelope::new(payload.event_type(), identity, payload);
    envelope.event_id = stream_job_callback_event_id(&dedupe_key);
    envelope.occurred_at = Utc::now();
    envelope.dedupe_key = Some(dedupe_key);
    envelope.metadata.insert(
        "bridge_request_id".to_owned(),
        stream_job_bridge_request_id(
            &handle.tenant_id,
            &handle.instance_id,
            &handle.run_id,
            &handle.job_id,
        ),
    );
    envelope.metadata.insert(
        "bridge_protocol_version".to_owned(),
        fabrik_throughput::THROUGHPUT_BRIDGE_PROTOCOL_VERSION.to_owned(),
    );
    envelope.metadata.insert(
        "bridge_operation_kind".to_owned(),
        fabrik_throughput::ThroughputBridgeOperationKind::StreamJob.as_str().to_owned(),
    );
    envelope.metadata.insert("bridge_owner_epoch".to_owned(), "1".to_owned());
    enqueue_stream_job_callback_event(state, &envelope).await
}

async fn publish_stream_job_terminal_event(
    state: &AppState,
    handle: &fabrik_store::StreamJobBridgeHandleRecord,
) -> Result<()> {
    let identity = WorkflowIdentity::new(
        handle.tenant_id.clone(),
        handle.definition_id.clone(),
        handle.definition_version.unwrap_or_default(),
        handle.artifact_hash.clone().unwrap_or_default(),
        handle.instance_id.clone(),
        handle.run_id.clone(),
        "throughput-runtime",
    );
    let status =
        if handle.parsed_status() == Some(StreamJobBridgeHandleStatus::CancellationRequested) {
            StreamJobBridgeHandleStatus::Cancelled
        } else {
            StreamJobBridgeHandleStatus::Completed
        };
    let payload = match status {
        StreamJobBridgeHandleStatus::Cancelled => WorkflowEvent::StreamJobCancelled {
            job_id: handle.job_id.clone(),
            handle_id: handle.handle_id.clone(),
            reason: handle
                .cancellation_reason
                .clone()
                .unwrap_or_else(|| "stream job cancelled".to_owned()),
            stream_owner_epoch: Some(1),
        },
        StreamJobBridgeHandleStatus::Completed => WorkflowEvent::StreamJobCompleted {
            job_id: handle.job_id.clone(),
            handle_id: handle.handle_id.clone(),
            output: json!({
                "jobId": handle.job_id,
                "jobName": handle.job_name,
                "status": "completed",
            }),
            stream_owner_epoch: Some(1),
        },
        _ => unreachable!("only completed/cancelled terminal callbacks are synthesized"),
    };
    let dedupe_key = stream_job_terminal_callback_dedupe_key(
        &handle.tenant_id,
        &handle.instance_id,
        &handle.run_id,
        &handle.job_id,
        status,
    );
    let mut envelope = EventEnvelope::new(payload.event_type(), identity, payload);
    envelope.event_id = stream_job_callback_event_id(&dedupe_key);
    envelope.occurred_at = Utc::now();
    envelope.dedupe_key = Some(dedupe_key);
    envelope.metadata.insert(
        "bridge_request_id".to_owned(),
        stream_job_bridge_request_id(
            &handle.tenant_id,
            &handle.instance_id,
            &handle.run_id,
            &handle.job_id,
        ),
    );
    envelope.metadata.insert(
        "bridge_protocol_version".to_owned(),
        fabrik_throughput::THROUGHPUT_BRIDGE_PROTOCOL_VERSION.to_owned(),
    );
    envelope.metadata.insert(
        "bridge_operation_kind".to_owned(),
        fabrik_throughput::ThroughputBridgeOperationKind::StreamJob.as_str().to_owned(),
    );
    envelope.metadata.insert("bridge_owner_epoch".to_owned(), "1".to_owned());
    enqueue_stream_job_callback_event(state, &envelope).await
}

async fn publish_stream_job_query_callback_from_bridge(
    state: &AppState,
    event: &EventEnvelope<WorkflowEvent>,
    job_id: &str,
    query_id: &str,
) -> Result<()> {
    let Some(handle) = await_stream_job_bridge_handle(state, event, job_id).await? else {
        return Ok(());
    };
    let Some(query) = await_stream_job_bridge_query(state, query_id).await? else {
        return Ok(());
    };
    if query.parsed_status().is_some_and(|status| {
        matches!(
            status,
            StreamJobQueryStatus::Completed
                | StreamJobQueryStatus::Failed
                | StreamJobQueryStatus::Accepted
                | StreamJobQueryStatus::Cancelled
        )
    }) {
        return Ok(());
    }

    let identity = WorkflowIdentity::new(
        handle.tenant_id.clone(),
        handle.definition_id.clone(),
        handle.definition_version.unwrap_or_default(),
        handle.artifact_hash.clone().unwrap_or_default(),
        handle.instance_id.clone(),
        handle.run_id.clone(),
        "throughput-runtime",
    );
    let payload = build_stream_job_query_payload(state, &handle, &query);
    let dedupe_key = stream_job_query_callback_dedupe_key(
        &handle.tenant_id,
        &handle.instance_id,
        &handle.run_id,
        &handle.job_id,
        &query.query_id,
        match &payload {
            WorkflowEvent::StreamJobQueryFailed { .. } => StreamJobQueryStatus::Failed,
            _ => StreamJobQueryStatus::Completed,
        },
    );
    let mut envelope = EventEnvelope::new(payload.event_type(), identity, payload);
    envelope.event_id = stream_job_callback_event_id(&dedupe_key);
    envelope.occurred_at = Utc::now();
    envelope.dedupe_key = Some(dedupe_key);
    envelope.metadata.insert(
        "bridge_request_id".to_owned(),
        stream_job_bridge_request_id(
            &handle.tenant_id,
            &handle.instance_id,
            &handle.run_id,
            &handle.job_id,
        ),
    );
    envelope.metadata.insert(
        "bridge_protocol_version".to_owned(),
        fabrik_throughput::THROUGHPUT_BRIDGE_PROTOCOL_VERSION.to_owned(),
    );
    envelope.metadata.insert(
        "bridge_operation_kind".to_owned(),
        fabrik_throughput::ThroughputBridgeOperationKind::StreamJob.as_str().to_owned(),
    );
    envelope.metadata.insert("bridge_owner_epoch".to_owned(), "1".to_owned());
    enqueue_stream_job_callback_event(state, &envelope).await
}

fn build_stream_job_query_payload(
    state: &AppState,
    handle: &fabrik_store::StreamJobBridgeHandleRecord,
    query: &fabrik_store::StreamJobQueryRecord,
) -> WorkflowEvent {
    let consistency = query.parsed_consistency().unwrap_or(StreamJobQueryConsistency::Eventual);
    let owner_backed_payload =
        || match build_keyed_rollup_query_output(&state.local_state, handle, query) {
            Ok(output) => WorkflowEvent::StreamJobQueryCompleted {
                job_id: handle.job_id.clone(),
                handle_id: handle.handle_id.clone(),
                query_id: query.query_id.clone(),
                query_name: query.query_name.clone(),
                output,
                stream_owner_epoch: Some(1),
            },
            Err(error) => WorkflowEvent::StreamJobQueryFailed {
                job_id: handle.job_id.clone(),
                handle_id: handle.handle_id.clone(),
                query_id: query.query_id.clone(),
                query_name: query.query_name.clone(),
                error: error.to_string(),
                stream_owner_epoch: Some(1),
            },
        };
    match consistency {
        StreamJobQueryConsistency::Strong => {
            if handle.job_name == KEYED_ROLLUP_JOB_NAME {
                owner_backed_payload()
            } else {
                WorkflowEvent::StreamJobQueryFailed {
                    job_id: handle.job_id.clone(),
                    handle_id: handle.handle_id.clone(),
                    query_id: query.query_id.clone(),
                    query_name: query.query_name.clone(),
                    error: format!(
                        "strong query consistency is not supported for stream job {}",
                        handle.job_name
                    ),
                    stream_owner_epoch: Some(1),
                }
            }
        }
        StreamJobQueryConsistency::Eventual => {
            if handle.job_name == KEYED_ROLLUP_JOB_NAME {
                owner_backed_payload()
            } else {
                WorkflowEvent::StreamJobQueryCompleted {
                    job_id: handle.job_id.clone(),
                    handle_id: handle.handle_id.clone(),
                    query_id: query.query_id.clone(),
                    query_name: query.query_name.clone(),
                    output: json!({
                        "jobId": handle.job_id,
                        "jobName": handle.job_name,
                        "query": query.query_name,
                        "consistency": query.consistency,
                        "consistencySource": "streams-runtime-snapshot",
                        "status": handle.status,
                    }),
                    stream_owner_epoch: Some(1),
                }
            }
        }
    }
}

const KEYED_ROLLUP_JOB_NAME: &str = "keyed-rollup";
const KEYED_ROLLUP_CHECKPOINT_NAME: &str = "initial-rollup-ready";
const KEYED_ROLLUP_CHECKPOINT_SEQUENCE: i64 = 1;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct KeyedRollupItem {
    account_id: String,
    amount: f64,
}

#[derive(Debug, Deserialize)]
struct KeyedRollupInputEnvelope {
    kind: Option<String>,
    items: Vec<KeyedRollupItem>,
}

#[derive(Debug, Deserialize)]
struct AccountTotalsQueryArgs {
    key: String,
}

fn parse_keyed_rollup_items(input_ref: &str) -> Result<Vec<KeyedRollupItem>> {
    let value: serde_json::Value =
        serde_json::from_str(input_ref).context("failed to decode stream job input_ref JSON")?;
    if let Ok(items) = serde_json::from_value::<Vec<KeyedRollupItem>>(value.clone()) {
        return Ok(items);
    }
    let envelope: KeyedRollupInputEnvelope = serde_json::from_value(value)
        .context("keyed-rollup input must be an item array or bounded_items envelope")?;
    if envelope.kind.as_deref().is_some_and(|kind| kind != "bounded_items") {
        anyhow::bail!("keyed-rollup input kind must be bounded_items");
    }
    Ok(envelope.items)
}

fn keyed_rollup_totals(items: &[KeyedRollupItem]) -> BTreeMap<String, f64> {
    let mut totals = BTreeMap::new();
    for item in items {
        *totals.entry(item.account_id.clone()).or_insert(0.0) += item.amount;
    }
    totals
}

async fn materialize_keyed_rollup_checkpoint(
    state: &AppState,
    handle: &fabrik_store::StreamJobBridgeHandleRecord,
) -> Result<()> {
    let items = parse_keyed_rollup_items(&handle.input_ref)?;
    let totals = keyed_rollup_totals(&items);
    let updated_at = Utc::now();
    for (account_id, total_amount) in totals {
        state.local_state.upsert_stream_job_view_value(
            &handle.handle_id,
            &handle.job_id,
            "accountTotals",
            &account_id,
            json!({
                "accountId": account_id,
                "totalAmount": total_amount,
                "asOfCheckpoint": KEYED_ROLLUP_CHECKPOINT_SEQUENCE,
            }),
            KEYED_ROLLUP_CHECKPOINT_SEQUENCE,
            updated_at,
        )?;
    }
    let existing = state
        .store
        .list_stream_job_bridge_checkpoints_for_handle_page(&handle.handle_id, 10_000, 0)
        .await?
        .into_iter()
        .find(|checkpoint| checkpoint.checkpoint_name == KEYED_ROLLUP_CHECKPOINT_NAME);
    let mut checkpoint = existing.unwrap_or(StreamJobCheckpointRecord {
        workflow_event_id: Uuid::now_v7(),
        protocol_version: fabrik_throughput::THROUGHPUT_BRIDGE_PROTOCOL_VERSION.to_owned(),
        operation_kind: fabrik_throughput::ThroughputBridgeOperationKind::StreamJob
            .as_str()
            .to_owned(),
        tenant_id: handle.tenant_id.clone(),
        instance_id: handle.instance_id.clone(),
        run_id: handle.run_id.clone(),
        job_id: handle.job_id.clone(),
        handle_id: handle.handle_id.clone(),
        bridge_request_id: handle.bridge_request_id.clone(),
        await_request_id: format!(
            "stream-job-reached:{}:{}:{}",
            handle.handle_id, KEYED_ROLLUP_CHECKPOINT_NAME, KEYED_ROLLUP_CHECKPOINT_SEQUENCE
        ),
        checkpoint_name: KEYED_ROLLUP_CHECKPOINT_NAME.to_owned(),
        checkpoint_sequence: Some(KEYED_ROLLUP_CHECKPOINT_SEQUENCE),
        status: fabrik_throughput::StreamJobCheckpointStatus::Awaiting.as_str().to_owned(),
        workflow_owner_epoch: handle.workflow_owner_epoch,
        stream_owner_epoch: Some(1),
        reached_at: None,
        output: None,
        accepted_at: None,
        cancelled_at: None,
        created_at: Utc::now(),
        updated_at: Utc::now(),
    });
    if checkpoint.reached_at.is_none() {
        checkpoint.status =
            fabrik_throughput::StreamJobCheckpointStatus::Reached.as_str().to_owned();
        checkpoint.checkpoint_sequence = Some(KEYED_ROLLUP_CHECKPOINT_SEQUENCE);
        checkpoint.stream_owner_epoch = Some(1);
        checkpoint.reached_at = Some(Utc::now());
        checkpoint.output = Some(json!({
            "jobId": handle.job_id,
            "checkpoint": KEYED_ROLLUP_CHECKPOINT_NAME,
            "checkpointSequence": KEYED_ROLLUP_CHECKPOINT_SEQUENCE,
            "viewName": "accountTotals",
        }));
        checkpoint.updated_at = Utc::now();
        state.store.upsert_stream_job_bridge_checkpoint(&checkpoint).await?;
    }
    sync_stream_job_record(state, handle, StreamJobStatus::Running, Some(&checkpoint), Utc::now())
        .await?;
    Ok(())
}

fn build_keyed_rollup_query_output(
    local_state: &crate::local_state::LocalThroughputState,
    handle: &fabrik_store::StreamJobBridgeHandleRecord,
    query: &fabrik_store::StreamJobQueryRecord,
) -> Result<serde_json::Value> {
    if query.query_name != "accountTotals" {
        anyhow::bail!("unsupported keyed-rollup query {}", query.query_name);
    }
    let args: AccountTotalsQueryArgs =
        serde_json::from_value(query.query_args.clone().unwrap_or(serde_json::Value::Null))
            .context("accountTotals query requires args with a key field")?;
    let Some(view) =
        local_state.load_stream_job_view_state(&handle.handle_id, "accountTotals", &args.key)?
    else {
        anyhow::bail!("accountTotals key {} is not materialized", args.key);
    };
    let mut output = view.output;
    if let Some(object) = output.as_object_mut() {
        object
            .insert("consistency".to_owned(), serde_json::Value::String(query.consistency.clone()));
        object.insert(
            "consistencySource".to_owned(),
            serde_json::Value::String("stream_owner_local_state".to_owned()),
        );
    }
    Ok(output)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    fn temp_path(prefix: &str) -> PathBuf {
        std::env::temp_dir().join(format!("{prefix}-{}", Uuid::now_v7()))
    }

    #[test]
    fn keyed_rollup_items_parse_from_bounded_items_envelope() {
        let items = parse_keyed_rollup_items(
            r#"{"kind":"bounded_items","items":[{"accountId":"acct_1","amount":4},{"accountId":"acct_1","amount":5.5},{"accountId":"acct_2","amount":1}]}"#,
        )
        .expect("items should parse");
        let totals = keyed_rollup_totals(&items);
        assert_eq!(totals.get("acct_1"), Some(&9.5));
        assert_eq!(totals.get("acct_2"), Some(&1.0));
    }

    #[test]
    fn keyed_rollup_query_output_returns_owner_view_shape() {
        let db_path = temp_path("bridge-keyed-rollup-db");
        let checkpoint_dir = temp_path("bridge-keyed-rollup-checkpoints");
        let local_state =
            crate::local_state::LocalThroughputState::open(&db_path, &checkpoint_dir, 3)
                .expect("local state should open");
        let handle = fabrik_store::StreamJobBridgeHandleRecord {
            workflow_event_id: Uuid::now_v7(),
            protocol_version: fabrik_throughput::THROUGHPUT_BRIDGE_PROTOCOL_VERSION.to_owned(),
            operation_kind: fabrik_throughput::ThroughputBridgeOperationKind::StreamJob.as_str().to_owned(),
            tenant_id: "tenant-a".to_owned(),
            instance_id: "instance-a".to_owned(),
            run_id: "run-a".to_owned(),
            job_id: "job-a".to_owned(),
            handle_id: "handle-a".to_owned(),
            bridge_request_id: "bridge-a".to_owned(),
            definition_id: "demo".to_owned(),
            definition_version: Some(1),
            artifact_hash: Some("artifact-a".to_owned()),
            job_name: KEYED_ROLLUP_JOB_NAME.to_owned(),
            input_ref: r#"{"kind":"bounded_items","items":[{"accountId":"acct_1","amount":2},{"accountId":"acct_1","amount":3}]}"#.to_owned(),
            config_ref: None,
            checkpoint_policy: Some(json!({
                "kind": "named_checkpoints",
                "checkpoints": [
                    {
                        "name": KEYED_ROLLUP_CHECKPOINT_NAME,
                        "delivery": "workflow_awaitable",
                        "sequence": KEYED_ROLLUP_CHECKPOINT_SEQUENCE
                    }
                ]
            })),
            view_definitions: Some(json!([
                {
                    "name": "accountTotals",
                    "consistency": "strong",
                    "queryMode": "by_key",
                    "keyField": "accountId",
                    "valueFields": ["accountId", "totalAmount", "asOfCheckpoint"]
                }
            ])),
            status: fabrik_throughput::StreamJobBridgeHandleStatus::Admitted.as_str().to_owned(),
            workflow_owner_epoch: Some(1),
            stream_owner_epoch: Some(1),
            cancellation_requested_at: None,
            cancellation_reason: None,
            terminal_event_id: None,
            terminal_at: None,
            workflow_accepted_at: None,
            terminal_output: None,
            terminal_error: None,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        };
        local_state
            .upsert_stream_job_view_value(
                &handle.handle_id,
                &handle.job_id,
                "accountTotals",
                "acct_1",
                json!({"accountId": "acct_1", "totalAmount": 5.0, "asOfCheckpoint": 1}),
                1,
                Utc::now(),
            )
            .expect("view state should persist");
        let query = fabrik_store::StreamJobQueryRecord {
            workflow_event_id: Uuid::now_v7(),
            protocol_version: fabrik_throughput::THROUGHPUT_BRIDGE_PROTOCOL_VERSION.to_owned(),
            operation_kind: fabrik_throughput::ThroughputBridgeOperationKind::StreamJob
                .as_str()
                .to_owned(),
            tenant_id: "tenant-a".to_owned(),
            instance_id: "instance-a".to_owned(),
            run_id: "run-a".to_owned(),
            job_id: "job-a".to_owned(),
            handle_id: "handle-a".to_owned(),
            bridge_request_id: "bridge-a".to_owned(),
            query_id: "query-a".to_owned(),
            query_name: "accountTotals".to_owned(),
            query_args: Some(json!({"key": "acct_1"})),
            consistency: "strong".to_owned(),
            status: fabrik_throughput::StreamJobQueryStatus::Requested.as_str().to_owned(),
            workflow_owner_epoch: Some(1),
            stream_owner_epoch: Some(1),
            output: None,
            error: None,
            requested_at: Utc::now(),
            completed_at: None,
            accepted_at: None,
            cancelled_at: None,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        };

        let output = build_keyed_rollup_query_output(&local_state, &handle, &query)
            .expect("query should succeed");
        assert_eq!(output["accountId"], "acct_1");
        assert_eq!(output["totalAmount"], 5.0);
        assert_eq!(output["asOfCheckpoint"], KEYED_ROLLUP_CHECKPOINT_SEQUENCE);
        assert_eq!(output["consistency"], "strong");
        assert_eq!(output["consistencySource"], "stream_owner_local_state");

        let _ = std::fs::remove_dir_all(&db_path);
        let _ = std::fs::remove_dir_all(&checkpoint_dir);
    }
}

async fn enqueue_stream_job_callback_event(
    state: &AppState,
    envelope: &EventEnvelope<WorkflowEvent>,
) -> Result<()> {
    state.store.put_workflow_event_outbox(envelope, envelope.occurred_at).await?;
    state.outbox_notify.notify_waiters();
    Ok(())
}

pub(super) async fn run_command_loop(
    state: AppState,
    mut consumer: fabrik_broker::JsonConsumerStream,
) {
    while let Some(message) = consumer.next().await {
        let record = match message {
            Ok(record) => record,
            Err(error) => {
                error!(error = %error, "streams-runtime command consumer error");
                continue;
            }
        };
        let command = match decode_json_record::<ThroughputCommandEnvelope>(&record.record) {
            Ok(command) => command,
            Err(error) => {
                warn!(error = %error, "streams-runtime skipping invalid command");
                continue;
            }
        };
        if let Err(error) = handle_throughput_command(state.clone(), command).await {
            error!(error = %error, "streams-runtime failed to handle throughput command");
        }
    }
}

async fn handle_throughput_command(
    state: AppState,
    command: ThroughputCommandEnvelope,
) -> Result<()> {
    let (tenant_id, instance_id, run_id, batch_id, throughput_backend) = match &command.payload {
        ThroughputCommand::StartThroughputRun(command) => (
            command.tenant_id.as_str(),
            command.instance_id.as_str(),
            command.run_id.as_str(),
            command.batch_id.as_str(),
            command.throughput_backend.as_str(),
        ),
        ThroughputCommand::CreateBatch(command) => (
            command.tenant_id.as_str(),
            command.instance_id.as_str(),
            command.run_id.as_str(),
            command.batch_id.as_str(),
            command.throughput_backend.as_str(),
        ),
        ThroughputCommand::CancelBatch { identity, .. } => (
            identity.tenant_id.as_str(),
            identity.instance_id.as_str(),
            identity.run_id.as_str(),
            identity.batch_id.as_str(),
            ThroughputBackend::StreamV2.as_str(),
        ),
        ThroughputCommand::TinyWorkflowStart(_) | ThroughputCommand::TinyWorkflowStartBatch(_) => {
            return Ok(());
        }
        _ => return Ok(()),
    };
    if throughput_backend != ThroughputBackend::StreamV2.as_str() {
        return Ok(());
    }
    if state.store.get_throughput_run(tenant_id, instance_id, run_id, batch_id).await?.is_some_and(
        |run| run.parsed_status().is_some_and(ThroughputRunStatus::is_active_or_terminal),
    ) {
        return Ok(());
    }
    match &command.payload {
        ThroughputCommand::StartThroughputRun(start) => {
            activate_native_stream_run(&state, &command, start).await
        }
        ThroughputCommand::CreateBatch(_) => {
            let event = workflow_event_from_throughput_command(&command)?;
            activate_stream_batch(&state, &event).await
        }
        ThroughputCommand::CancelBatch { identity, reason } => {
            cancel_stream_batch_via_bridge(&state, identity, reason, command.occurred_at).await
        }
        ThroughputCommand::TinyWorkflowStart(_) | ThroughputCommand::TinyWorkflowStartBatch(_) => {
            Ok(())
        }
        _ => Ok(()),
    }
}

async fn cancel_stream_batch_via_bridge(
    state: &AppState,
    identity: &ThroughputBatchIdentity,
    reason: &str,
    occurred_at: DateTime<Utc>,
) -> Result<()> {
    let cancelled = state
        .store
        .cancel_bulk_batch(
            &identity.tenant_id,
            &identity.instance_id,
            &identity.run_id,
            &identity.batch_id,
            reason,
            None,
            occurred_at,
        )
        .await?;
    let _ = state
        .store
        .mark_throughput_run_terminal(
            &identity.tenant_id,
            &identity.instance_id,
            &identity.run_id,
            &identity.batch_id,
            ThroughputRunStatus::Cancelled,
            occurred_at,
        )
        .await?;
    let _ = state
        .store
        .mark_throughput_bridge_cancelled(
            &identity.tenant_id,
            &identity.instance_id,
            &identity.run_id,
            &identity.batch_id,
            occurred_at,
        )
        .await?;
    if !cancelled.cancelled_chunks.is_empty() {
        state.bulk_notify.notify_waiters();
        state.outbox_notify.notify_waiters();
    }
    Ok(())
}
