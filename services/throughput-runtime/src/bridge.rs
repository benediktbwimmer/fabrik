use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use fabrik_broker::{decode_consumed_workflow_event, decode_json_record};
use fabrik_events::{EventEnvelope, WorkflowEvent, WorkflowIdentity};
use fabrik_store::{StreamJobCheckpointRecord, StreamJobRecord};
use fabrik_throughput::{
    CancelStreamJobCommand, STREAM_SOURCE_TOPIC, ScheduleStreamJobCommand,
    StreamJobBridgeHandleStatus, StreamJobCheckpointStatus, StreamJobOriginKind,
    StreamJobQueryConsistency, StreamJobQueryStatus, StreamJobStatus, ThroughputBackend,
    ThroughputBatchIdentity, ThroughputCommand, ThroughputCommandEnvelope, ThroughputRunStatus,
    stream_job_bridge_request_id, stream_job_callback_event_id,
    stream_job_checkpoint_callback_dedupe_key, stream_job_query_callback_dedupe_key,
    stream_job_terminal_callback_dedupe_key,
};
use futures_util::StreamExt;
use serde_json::{Value, json};
use tokio::task::JoinSet;
use tokio::time::{Duration, sleep};
use tracing::{error, info, warn};

use crate::{
    AppState, BRIDGE_EVENT_CONCURRENCY, activate_native_stream_run, activate_stream_batch,
    publish_changelog_records, publish_projection_records, stream_job_owner_epoch,
    stream_jobs::{
        activate_stream_job_on_local_state, build_stream_job_query_output,
        materialization_outcome_from_local_state, resolve_stream_job_definition,
        should_defer_terminal_callback_until_query_boundary, terminal_result_after_query,
    },
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
        info!(
            partition_id = record.partition_id,
            event_id = %event.event_id,
            event_type = event.event_type,
            tenant_id = %event.tenant_id,
            instance_id = %event.instance_id,
            run_id = %event.run_id,
            "streams-runtime consumed workflow event"
        );
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

pub(super) async fn handle_bridge_event(
    state: AppState,
    event: EventEnvelope<WorkflowEvent>,
) -> Result<()> {
    if let WorkflowEvent::WorkflowCancellationRequested { reason } = &event.payload {
        crate::cancel_stream_batches_for_run(&state, &event, reason).await?;
        return Ok(());
    }

    if let WorkflowEvent::StreamJobScheduled { job_id, .. } = &event.payload {
        info!(
            tenant_id = %event.tenant_id,
            instance_id = %event.instance_id,
            run_id = %event.run_id,
            job_id = %job_id,
            "streams-runtime received stream job schedule event"
        );
        publish_stream_job_callbacks_from_bridge(&state, &event, job_id).await?;
        return Ok(());
    }

    if let WorkflowEvent::StreamJobCancellationRequested { job_id, .. } = &event.payload {
        if let Some(handle) = await_stream_job_bridge_handle(&state, &event, job_id).await? {
            if stream_job_uses_topic_source(&state, &handle).await? {
                sync_stream_job_record(
                    &state,
                    &handle,
                    StreamJobStatus::Draining,
                    None,
                    event.occurred_at,
                )
                .await?;
            } else {
                let finalized =
                    finalize_stream_job_cancellation(&state, &handle, event.occurred_at).await?;
                publish_stream_job_terminal_event(&state, &finalized).await?;
            }
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
            stream_instance_id: handle.stream_instance_id.clone(),
            stream_run_id: handle.stream_run_id.clone(),
            job_id: handle.job_id.clone(),
            handle_id: handle.handle_id.clone(),
            protocol_version: handle.protocol_version.clone(),
            operation_kind: handle.operation_kind.clone(),
            workflow_event_id: handle.workflow_event_id,
            bridge_request_id: handle.bridge_request_id.clone(),
            origin_kind: handle
                .parsed_origin_kind()
                .unwrap_or(StreamJobOriginKind::Workflow)
                .as_str()
                .to_owned(),
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

    record.tenant_id = handle.tenant_id.clone();
    record.instance_id = handle.instance_id.clone();
    record.run_id = handle.run_id.clone();
    record.stream_instance_id = handle.stream_instance_id.clone();
    record.stream_run_id = handle.stream_run_id.clone();
    record.handle_id = handle.handle_id.clone();
    record.protocol_version = handle.protocol_version.clone();
    record.operation_kind = handle.operation_kind.clone();
    record.workflow_event_id = handle.workflow_event_id;
    record.bridge_request_id = handle.bridge_request_id.clone();
    record.origin_kind =
        handle.parsed_origin_kind().unwrap_or(StreamJobOriginKind::Workflow).as_str().to_owned();
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
        info!(
            tenant_id = %event.tenant_id,
            instance_id = %event.instance_id,
            run_id = %event.run_id,
            job_id = %job_id,
            "streams-runtime did not find stream job handle after schedule event"
        );
        return Ok(());
    };
    materialize_stream_job_from_handle(state, handle, event.occurred_at).await
}

fn should_emit_workflow_callbacks(handle: &fabrik_store::StreamJobBridgeHandleRecord) -> bool {
    !matches!(handle.parsed_origin_kind(), Some(StreamJobOriginKind::Standalone))
}

pub(crate) async fn materialize_stream_job_from_handle(
    state: &AppState,
    mut handle: fabrik_store::StreamJobBridgeHandleRecord,
    occurred_at: DateTime<Utc>,
) -> Result<()> {
    info!(
        handle_id = %handle.handle_id,
        job_id = %handle.job_id,
        job_name = %handle.job_name,
        "streams-runtime materializing stream job"
    );
    sync_stream_job_record(state, &handle, default_stream_job_status(&handle), None, occurred_at)
        .await?;
    if let Some(activation) = activate_stream_job(state, &handle).await? {
        publish_changelog_records(state, activation.changelog_records).await;
        if let Err(error) = publish_projection_records(state, activation.projection_records).await {
            error!(
                error = %error,
                handle_id = %handle.handle_id,
                job_id = %handle.job_id,
                "failed to publish stream job projection records"
            );
        }
        let materialized = materialization_outcome_from_local_state(&state.local_state, &handle)?;
        let mut checkpoint_to_publish = None;
        if let Some(checkpoint) =
            materialized.as_ref().and_then(|outcome| outcome.checkpoint.clone())
        {
            let checkpoint = merge_stream_job_checkpoint_bridge_state(state, checkpoint).await?;
            state.store.upsert_stream_job_callback_checkpoint(&checkpoint).await?;
            if handle.stream_owner_epoch.is_none() && checkpoint.stream_owner_epoch.is_some() {
                handle.stream_owner_epoch = checkpoint.stream_owner_epoch;
                handle.updated_at = checkpoint.reached_at.unwrap_or(occurred_at);
                state.store.upsert_stream_job_callback_handle(&handle).await?;
            }
            sync_stream_job_record(
                state,
                &handle,
                StreamJobStatus::Running,
                Some(&checkpoint),
                checkpoint.reached_at.unwrap_or(occurred_at),
            )
            .await?;
            checkpoint_to_publish = Some(checkpoint);
        }
        if let Some(terminal) = materialized.and_then(|outcome| outcome.terminal) {
            let finalized =
                persist_stream_job_terminal_result(state, &handle, terminal, occurred_at).await?;
            sync_stream_job_record(
                state,
                &finalized,
                default_stream_job_status(&finalized),
                None,
                finalized.terminal_at.unwrap_or(occurred_at),
            )
            .await?;
            if !should_emit_workflow_callbacks(&finalized) {
                return Ok(());
            }
            if let Some(checkpoint) = checkpoint_to_publish.as_ref() {
                if checkpoint.reached_at.is_some()
                    && checkpoint.accepted_at.is_none()
                    && checkpoint.cancelled_at.is_none()
                {
                    info!(
                        handle_id = %finalized.handle_id,
                        job_id = %finalized.job_id,
                        checkpoint_name = %checkpoint.checkpoint_name,
                        checkpoint_sequence = checkpoint.checkpoint_sequence,
                        "streams-runtime publishing stream job checkpoint callback"
                    );
                    publish_stream_job_checkpoint_event(state, &finalized, checkpoint).await?;
                } else if checkpoint.reached_at.is_some() {
                    record_duplicate_stream_job_checkpoint_publication(
                        state, &finalized, checkpoint,
                    );
                }
            }
            if should_defer_terminal_callback_until_query_boundary(&finalized) {
                info!(
                    handle_id = %finalized.handle_id,
                    job_id = %finalized.job_id,
                    "streams-runtime deferring stream job terminal callback until workflow bridge is ready"
                );
            } else {
                publish_stream_job_terminal_event(state, &finalized).await?;
            }
        } else if let Some(checkpoint) = checkpoint_to_publish.as_ref()
            && should_emit_workflow_callbacks(&handle)
        {
            if checkpoint.reached_at.is_some()
                && checkpoint.accepted_at.is_none()
                && checkpoint.cancelled_at.is_none()
            {
                info!(
                    handle_id = %handle.handle_id,
                    job_id = %handle.job_id,
                    checkpoint_name = %checkpoint.checkpoint_name,
                    checkpoint_sequence = checkpoint.checkpoint_sequence,
                    "streams-runtime publishing stream job checkpoint callback"
                );
                publish_stream_job_checkpoint_event(state, &handle, checkpoint).await?;
            } else if checkpoint.reached_at.is_some() {
                record_duplicate_stream_job_checkpoint_publication(state, &handle, checkpoint);
            }
        }
    }
    Ok(())
}

async fn handle_schedule_stream_job_command(
    state: &AppState,
    command: &ScheduleStreamJobCommand,
    occurred_at: DateTime<Utc>,
) -> Result<()> {
    let Some(handle) =
        state.store.get_stream_job_callback_handle_by_handle_id(&command.handle_id).await?
    else {
        info!(
            tenant_id = %command.tenant_id,
            stream_instance_id = %command.stream_instance_id,
            stream_run_id = %command.stream_run_id,
            job_id = %command.job_id,
            handle_id = %command.handle_id,
            "streams-runtime did not find stream job handle for schedule command"
        );
        return Ok(());
    };
    materialize_stream_job_from_handle(state, handle, occurred_at).await
}

async fn handle_cancel_stream_job_command(
    state: &AppState,
    command: &CancelStreamJobCommand,
    occurred_at: DateTime<Utc>,
) -> Result<()> {
    let Some(handle) =
        state.store.get_stream_job_callback_handle_by_handle_id(&command.handle_id).await?
    else {
        info!(
            tenant_id = %command.tenant_id,
            stream_instance_id = %command.stream_instance_id,
            stream_run_id = %command.stream_run_id,
            job_id = %command.job_id,
            handle_id = %command.handle_id,
            "streams-runtime did not find stream job handle for cancel command"
        );
        return Ok(());
    };
    if stream_job_uses_topic_source(state, &handle).await? {
        sync_stream_job_record(state, &handle, StreamJobStatus::Draining, None, occurred_at)
            .await?;
    } else {
        let finalized = finalize_stream_job_cancellation(state, &handle, occurred_at).await?;
        if should_emit_workflow_callbacks(&finalized) {
            publish_stream_job_terminal_event(state, &finalized).await?;
        }
    }
    Ok(())
}

async fn stream_job_uses_topic_source(
    state: &AppState,
    handle: &fabrik_store::StreamJobBridgeHandleRecord,
) -> Result<bool> {
    let Some(job) = resolve_stream_job_definition(Some(&state.store), handle).await? else {
        return Ok(false);
    };
    Ok(job.source.kind == STREAM_SOURCE_TOPIC)
}

pub(crate) async fn reconcile_active_topic_stream_jobs(
    state: &AppState,
    page_size: i64,
) -> Result<()> {
    let mut offset = 0;
    loop {
        let handles =
            state.store.list_active_stream_job_callback_handles_page(page_size, offset).await?;
        if handles.is_empty() {
            return Ok(());
        }
        for handle in &handles {
            if stream_job_uses_topic_source(state, handle).await? {
                materialize_stream_job_from_handle(state, handle.clone(), Utc::now()).await?;
            }
        }
        if handles.len() < usize::try_from(page_size).unwrap_or(usize::MAX) {
            return Ok(());
        }
        offset += page_size;
    }
}

async fn await_stream_job_bridge_handle(
    state: &AppState,
    event: &EventEnvelope<WorkflowEvent>,
    job_id: &str,
) -> Result<Option<fabrik_store::StreamJobBridgeHandleRecord>> {
    for _ in 0..10 {
        if let Some(handle) = state
            .store
            .get_stream_job_callback_handle(
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
        if let Some(query) = state.store.get_stream_job_callback_query(query_id).await? {
            return Ok(Some(query));
        }
        sleep(Duration::from_millis(25)).await;
    }
    Ok(None)
}

async fn activate_stream_job(
    state: &AppState,
    handle: &fabrik_store::StreamJobBridgeHandleRecord,
) -> Result<Option<crate::StreamJobActivationResult>> {
    let Some(shard_workers) = state.shard_workers.get() else {
        return activate_stream_job_on_local_state(
            Some(&state.store),
            Some(state),
            &state.local_state,
            handle,
            state.throughput_partitions,
            stream_job_owner_epoch(state, &handle.job_id),
            Utc::now(),
        )
        .await;
    };
    shard_workers.activate_stream_job(handle.clone(), state.throughput_partitions).await
}

fn default_stream_job_status(
    handle: &fabrik_store::StreamJobBridgeHandleRecord,
) -> StreamJobStatus {
    match handle.parsed_status() {
        Some(StreamJobBridgeHandleStatus::Completed) => StreamJobStatus::Completed,
        Some(StreamJobBridgeHandleStatus::Failed) => StreamJobStatus::Failed,
        Some(StreamJobBridgeHandleStatus::Cancelled) => StreamJobStatus::Cancelled,
        Some(StreamJobBridgeHandleStatus::CancellationRequested) => StreamJobStatus::Draining,
        Some(StreamJobBridgeHandleStatus::Admitted) => StreamJobStatus::Starting,
        Some(StreamJobBridgeHandleStatus::Running) | None => StreamJobStatus::Running,
    }
}

async fn persist_stream_job_terminal_result(
    state: &AppState,
    handle: &fabrik_store::StreamJobBridgeHandleRecord,
    terminal: crate::StreamJobTerminalResult,
    occurred_at: DateTime<Utc>,
) -> Result<fabrik_store::StreamJobBridgeHandleRecord> {
    let mut finalized = state
        .store
        .get_stream_job_callback_handle_by_handle_id(&handle.handle_id)
        .await?
        .unwrap_or_else(|| handle.clone());
    if finalized.parsed_status().is_some_and(StreamJobBridgeHandleStatus::is_terminal) {
        return Ok(finalized);
    }
    finalized.status = terminal.status.as_str().to_owned();
    finalized.stream_owner_epoch = finalized
        .stream_owner_epoch
        .or_else(|| stream_job_owner_epoch(state, &finalized.job_id))
        .or(Some(1));
    finalized.terminal_at = Some(occurred_at);
    finalized.terminal_output = terminal.output;
    finalized.terminal_error = terminal.error;
    finalized.updated_at = occurred_at;
    state.store.upsert_stream_job_callback_handle(&finalized).await?;
    Ok(finalized)
}

async fn merge_stream_job_checkpoint_bridge_state(
    state: &AppState,
    mut checkpoint: fabrik_store::StreamJobCheckpointRecord,
) -> Result<fabrik_store::StreamJobCheckpointRecord> {
    let Some(existing) =
        state.store.get_stream_job_callback_checkpoint(&checkpoint.await_request_id).await?
    else {
        return Ok(checkpoint);
    };
    checkpoint.accepted_at = checkpoint.accepted_at.or(existing.accepted_at);
    checkpoint.cancelled_at = checkpoint.cancelled_at.or(existing.cancelled_at);
    checkpoint.workflow_owner_epoch =
        checkpoint.workflow_owner_epoch.or(existing.workflow_owner_epoch);
    checkpoint.stream_owner_epoch = checkpoint.stream_owner_epoch.or(existing.stream_owner_epoch);
    if checkpoint.accepted_at.is_some() {
        checkpoint.status = StreamJobCheckpointStatus::Accepted.as_str().to_owned();
    } else if checkpoint.cancelled_at.is_some() {
        checkpoint.status = StreamJobCheckpointStatus::Cancelled.as_str().to_owned();
    }
    Ok(checkpoint)
}

async fn finalize_stream_job_cancellation(
    state: &AppState,
    handle: &fabrik_store::StreamJobBridgeHandleRecord,
    occurred_at: DateTime<Utc>,
) -> Result<fabrik_store::StreamJobBridgeHandleRecord> {
    let reason =
        handle.cancellation_reason.clone().unwrap_or_else(|| "stream job cancelled".to_owned());
    let finalized = persist_stream_job_terminal_result(
        state,
        handle,
        crate::StreamJobTerminalResult {
            status: StreamJobBridgeHandleStatus::Cancelled,
            output: None,
            error: Some(reason),
        },
        occurred_at,
    )
    .await?;
    sync_stream_job_record(state, &finalized, StreamJobStatus::Cancelled, None, occurred_at)
        .await?;
    Ok(finalized)
}

async fn publish_stream_job_checkpoint_event(
    state: &AppState,
    handle: &fabrik_store::StreamJobBridgeHandleRecord,
    checkpoint: &fabrik_store::StreamJobCheckpointRecord,
) -> Result<()> {
    let checkpoint = state
        .store
        .get_stream_job_callback_checkpoint(&checkpoint.await_request_id)
        .await?
        .unwrap_or_else(|| checkpoint.clone());
    if checkpoint.parsed_status().is_some_and(|status| {
        matches!(status, StreamJobCheckpointStatus::Accepted | StreamJobCheckpointStatus::Cancelled)
    }) || checkpoint.accepted_at.is_some()
        || checkpoint.cancelled_at.is_some()
    {
        record_duplicate_stream_job_checkpoint_publication(state, handle, &checkpoint);
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
        stream_owner_epoch: resolved_stream_job_owner_epoch(
            state,
            handle,
            checkpoint.stream_owner_epoch,
        ),
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
    envelope.occurred_at = checkpoint.reached_at.unwrap_or_else(Utc::now);
    envelope.dedupe_key = Some(dedupe_key);
    attach_stream_job_bridge_metadata(state, handle, checkpoint.stream_owner_epoch, &mut envelope);
    enqueue_stream_job_callback_event(state, &envelope).await
}

async fn publish_stream_job_terminal_event(
    state: &AppState,
    handle: &fabrik_store::StreamJobBridgeHandleRecord,
) -> Result<()> {
    let handle = state
        .store
        .get_stream_job_callback_handle_by_handle_id(&handle.handle_id)
        .await?
        .unwrap_or_else(|| handle.clone());
    if handle.workflow_accepted_at.is_some() || handle.terminal_event_id.is_some() {
        record_duplicate_stream_job_terminal_publication(state, &handle);
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
    let Some(status) = handle.parsed_status() else {
        return Ok(());
    };
    let stream_owner_epoch =
        resolved_stream_job_owner_epoch(state, &handle, handle.stream_owner_epoch);
    let payload = match status {
        StreamJobBridgeHandleStatus::Cancelled => WorkflowEvent::StreamJobCancelled {
            job_id: handle.job_id.clone(),
            handle_id: handle.handle_id.clone(),
            reason: handle
                .cancellation_reason
                .clone()
                .unwrap_or_else(|| "stream job cancelled".to_owned()),
            stream_owner_epoch,
        },
        StreamJobBridgeHandleStatus::Completed => WorkflowEvent::StreamJobCompleted {
            job_id: handle.job_id.clone(),
            handle_id: handle.handle_id.clone(),
            output: handle.terminal_output.clone().unwrap_or_else(|| {
                json!({
                    "jobId": handle.job_id,
                    "jobName": handle.job_name,
                    "status": "completed",
                })
            }),
            stream_owner_epoch,
        },
        StreamJobBridgeHandleStatus::Failed => WorkflowEvent::StreamJobFailed {
            job_id: handle.job_id.clone(),
            handle_id: handle.handle_id.clone(),
            error: handle.terminal_error.clone().unwrap_or_else(|| "stream job failed".to_owned()),
            stream_owner_epoch,
        },
        _ => return Ok(()),
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
    envelope.occurred_at = handle.terminal_at.unwrap_or_else(Utc::now);
    envelope.dedupe_key = Some(dedupe_key);
    attach_stream_job_bridge_metadata(state, &handle, stream_owner_epoch, &mut envelope);
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
        record_duplicate_stream_job_query_request(state, event, &query);
        return Ok(());
    }

    let owner_epoch = stream_job_owner_epoch(state, &handle.job_id);
    let (status, output, error) = match build_stream_job_query_payload(state, &handle, &query).await
    {
        WorkflowEvent::StreamJobQueryCompleted { output, .. } => {
            (StreamJobQueryStatus::Completed, Some(output), None)
        }
        WorkflowEvent::StreamJobQueryFailed { error, .. } => {
            (StreamJobQueryStatus::Failed, None, Some(error))
        }
        other => anyhow::bail!("unexpected stream job query payload {other:?}"),
    };
    let mut query = query;
    query.status = status.as_str().to_owned();
    query.stream_owner_epoch = owner_epoch;
    query.output = output;
    query.error = error;
    query.completed_at = Some(event.occurred_at);
    query.updated_at = event.occurred_at;
    state.store.upsert_stream_job_callback_query(&query).await?;

    let identity = WorkflowIdentity::new(
        handle.tenant_id.clone(),
        handle.definition_id.clone(),
        handle.definition_version.unwrap_or_default(),
        handle.artifact_hash.clone().unwrap_or_default(),
        handle.instance_id.clone(),
        handle.run_id.clone(),
        "throughput-runtime",
    );
    let payload = build_stream_job_query_payload(state, &handle, &query).await;
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
    envelope.occurred_at = query.completed_at.unwrap_or(event.occurred_at);
    envelope.dedupe_key = Some(dedupe_key);
    attach_stream_job_bridge_metadata(state, &handle, query.stream_owner_epoch, &mut envelope);
    enqueue_stream_job_callback_event(state, &envelope).await?;
    publish_stream_job_terminal_after_query_if_pending(state, &handle, event.occurred_at).await
}

fn record_duplicate_stream_job_query_request(
    state: &AppState,
    event: &EventEnvelope<WorkflowEvent>,
    query: &fabrik_store::StreamJobQueryRecord,
) {
    let mut debug = state.debug.lock().expect("throughput debug lock poisoned");
    debug.duplicate_bridge_events = debug.duplicate_bridge_events.saturating_add(1);
    debug.duplicate_stream_job_query_requests_ignored =
        debug.duplicate_stream_job_query_requests_ignored.saturating_add(1);
    debug.last_duplicate_stream_job_query_request_at = Some(event.occurred_at);
    debug.last_duplicate_stream_job_query_request = Some(format!(
        "{}:{}:{}:{}:{}:{}",
        event.tenant_id,
        event.instance_id,
        event.run_id,
        query.job_id,
        query.query_id,
        query.status
    ));
    drop(debug);

    info!(
        tenant_id = %event.tenant_id,
        instance_id = %event.instance_id,
        run_id = %event.run_id,
        job_id = %query.job_id,
        query_id = %query.query_id,
        query_name = %query.query_name,
        status = %query.status,
        "streams-runtime ignored duplicate stream job query request"
    );
}

fn record_duplicate_stream_job_checkpoint_publication(
    state: &AppState,
    handle: &fabrik_store::StreamJobBridgeHandleRecord,
    checkpoint: &fabrik_store::StreamJobCheckpointRecord,
) {
    let mut debug = state.debug.lock().expect("throughput debug lock poisoned");
    debug.duplicate_bridge_events = debug.duplicate_bridge_events.saturating_add(1);
    debug.duplicate_stream_job_checkpoint_publications_ignored =
        debug.duplicate_stream_job_checkpoint_publications_ignored.saturating_add(1);
    debug.last_duplicate_stream_job_checkpoint_publication_at =
        checkpoint.accepted_at.or(checkpoint.cancelled_at).or(checkpoint.reached_at);
    debug.last_duplicate_stream_job_checkpoint_publication = Some(format!(
        "{}:{}:{}:{}:{}:{}",
        handle.tenant_id,
        handle.instance_id,
        handle.run_id,
        checkpoint.job_id,
        checkpoint.checkpoint_name,
        checkpoint.status
    ));
    drop(debug);

    info!(
        tenant_id = %handle.tenant_id,
        instance_id = %handle.instance_id,
        run_id = %handle.run_id,
        job_id = %checkpoint.job_id,
        checkpoint_name = %checkpoint.checkpoint_name,
        checkpoint_sequence = checkpoint.checkpoint_sequence,
        status = %checkpoint.status,
        accepted_at = ?checkpoint.accepted_at,
        cancelled_at = ?checkpoint.cancelled_at,
        "streams-runtime ignored duplicate stream job checkpoint callback publication"
    );
}

fn record_duplicate_stream_job_terminal_publication(
    state: &AppState,
    handle: &fabrik_store::StreamJobBridgeHandleRecord,
) {
    let mut debug = state.debug.lock().expect("throughput debug lock poisoned");
    debug.duplicate_bridge_events = debug.duplicate_bridge_events.saturating_add(1);
    debug.duplicate_stream_job_terminal_publications_ignored =
        debug.duplicate_stream_job_terminal_publications_ignored.saturating_add(1);
    debug.last_duplicate_stream_job_terminal_publication_at =
        handle.workflow_accepted_at.or(handle.terminal_at);
    debug.last_duplicate_stream_job_terminal_publication = Some(format!(
        "{}:{}:{}:{}:{}",
        handle.tenant_id, handle.instance_id, handle.run_id, handle.job_id, handle.status
    ));
    drop(debug);

    info!(
        tenant_id = %handle.tenant_id,
        instance_id = %handle.instance_id,
        run_id = %handle.run_id,
        job_id = %handle.job_id,
        status = %handle.status,
        terminal_event_id = ?handle.terminal_event_id,
        workflow_accepted_at = ?handle.workflow_accepted_at,
        "streams-runtime ignored duplicate stream job terminal callback publication"
    );
}

async fn publish_stream_job_terminal_after_query_if_pending(
    state: &AppState,
    handle: &fabrik_store::StreamJobBridgeHandleRecord,
    occurred_at: DateTime<Utc>,
) -> Result<()> {
    info!(
        handle_id = %handle.handle_id,
        job_id = %handle.job_id,
        status = %handle.status,
        workflow_accepted_at = ?handle.workflow_accepted_at,
        "streams-runtime evaluating deferred stream job terminal callback after query"
    );
    let Some(terminal) = terminal_result_after_query(&handle) else {
        info!(
            handle_id = %handle.handle_id,
            job_id = %handle.job_id,
            status = %handle.status,
            workflow_accepted_at = ?handle.workflow_accepted_at,
            "streams-runtime found no deferred stream job terminal callback to publish after query"
        );
        return Ok(());
    };
    let finalized =
        persist_stream_job_terminal_result(state, handle, terminal, occurred_at).await?;
    sync_stream_job_record(
        state,
        &finalized,
        default_stream_job_status(&finalized),
        None,
        finalized.terminal_at.unwrap_or(occurred_at),
    )
    .await?;
    info!(
        handle_id = %finalized.handle_id,
        job_id = %finalized.job_id,
        status = %finalized.status,
        "streams-runtime publishing pending stream job terminal callback after query"
    );
    publish_stream_job_terminal_event(state, &finalized).await
}

async fn build_stream_job_query_payload(
    state: &AppState,
    handle: &fabrik_store::StreamJobBridgeHandleRecord,
    query: &fabrik_store::StreamJobQueryRecord,
) -> WorkflowEvent {
    let consistency = query.parsed_consistency().unwrap_or(StreamJobQueryConsistency::Eventual);
    let owner_epoch = resolved_stream_job_owner_epoch(state, handle, query.stream_owner_epoch);
    let owner_backed_payload = |result: Result<Option<Value>>| match result {
        Ok(Some(output)) => WorkflowEvent::StreamJobQueryCompleted {
            job_id: handle.job_id.clone(),
            handle_id: handle.handle_id.clone(),
            query_id: query.query_id.clone(),
            query_name: query.query_name.clone(),
            output,
            stream_owner_epoch: owner_epoch,
        },
        Ok(None) => WorkflowEvent::StreamJobQueryFailed {
            job_id: handle.job_id.clone(),
            handle_id: handle.handle_id.clone(),
            query_id: query.query_id.clone(),
            query_name: query.query_name.clone(),
            error: format!(
                "strong query consistency is not supported for stream job {}",
                handle.job_name
            ),
            stream_owner_epoch: owner_epoch,
        },
        Err(error) => WorkflowEvent::StreamJobQueryFailed {
            job_id: handle.job_id.clone(),
            handle_id: handle.handle_id.clone(),
            query_id: query.query_id.clone(),
            query_name: query.query_name.clone(),
            error: format!("{error}"),
            stream_owner_epoch: owner_epoch,
        },
    };
    let query_result = build_stream_job_query_output(state, handle, query).await;
    match consistency {
        StreamJobQueryConsistency::Strong => owner_backed_payload(query_result),
        StreamJobQueryConsistency::Eventual => match query_result {
            Ok(Some(output)) => owner_backed_payload(Ok(Some(output))),
            Err(error) => owner_backed_payload(Err(error)),
            Ok(None) => WorkflowEvent::StreamJobQueryCompleted {
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
                stream_owner_epoch: owner_epoch,
            },
        },
    }
}

fn resolved_stream_job_owner_epoch(
    state: &AppState,
    handle: &fabrik_store::StreamJobBridgeHandleRecord,
    callback_owner_epoch: Option<u64>,
) -> Option<u64> {
    callback_owner_epoch
        .or(handle.stream_owner_epoch)
        .or_else(|| stream_job_owner_epoch(state, &handle.job_id))
}

fn attach_stream_job_bridge_metadata(
    state: &AppState,
    handle: &fabrik_store::StreamJobBridgeHandleRecord,
    callback_owner_epoch: Option<u64>,
    envelope: &mut EventEnvelope<WorkflowEvent>,
) {
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
    if let Some(owner_epoch) = resolved_stream_job_owner_epoch(state, handle, callback_owner_epoch)
    {
        envelope.metadata.insert("bridge_owner_epoch".to_owned(), owner_epoch.to_string());
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum CommandPlane {
    Throughput,
    Streams,
}

fn note_command_received(
    state: &AppState,
    plane: CommandPlane,
    occurred_at: chrono::DateTime<chrono::Utc>,
) {
    let mut debug = state.debug.lock().expect("throughput debug lock poisoned");
    match plane {
        CommandPlane::Throughput => {
            debug.throughput_commands_received =
                debug.throughput_commands_received.saturating_add(1);
            debug.last_throughput_command_at = Some(occurred_at);
        }
        CommandPlane::Streams => {
            debug.streams_commands_received = debug.streams_commands_received.saturating_add(1);
            debug.last_streams_command_at = Some(occurred_at);
        }
    }
}

pub(super) async fn run_command_loop(
    state: AppState,
    mut consumer: fabrik_broker::JsonConsumerStream,
    plane: CommandPlane,
) {
    while let Some(message) = consumer.next().await {
        let record = match message {
            Ok(record) => record,
            Err(error) => {
                error!(error = %error, command_plane = ?plane, "streams-runtime command consumer error");
                continue;
            }
        };
        let command = match decode_json_record::<ThroughputCommandEnvelope>(&record.record) {
            Ok(command) => command,
            Err(error) => {
                warn!(error = %error, command_plane = ?plane, "streams-runtime skipping invalid command");
                continue;
            }
        };
        note_command_received(&state, plane, command.occurred_at);
        if let Err(error) = handle_throughput_command(state.clone(), command).await {
            error!(error = %error, command_plane = ?plane, "streams-runtime failed to handle throughput command");
        }
    }
}

pub(crate) async fn handle_throughput_command(
    state: AppState,
    command: ThroughputCommandEnvelope,
) -> Result<()> {
    match &command.payload {
        ThroughputCommand::ScheduleStreamJob(schedule) => {
            return handle_schedule_stream_job_command(&state, schedule, command.occurred_at).await;
        }
        ThroughputCommand::CancelStreamJob(cancel) => {
            return handle_cancel_stream_job_command(&state, cancel, command.occurred_at).await;
        }
        _ => {}
    }
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
        ThroughputCommand::TinyWorkflowStart(_)
        | ThroughputCommand::TinyWorkflowStartBatch(_)
        | ThroughputCommand::ScheduleStreamJob(_)
        | ThroughputCommand::CancelStreamJob(_) => {
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
        ThroughputCommand::TinyWorkflowStart(_)
        | ThroughputCommand::TinyWorkflowStartBatch(_)
        | ThroughputCommand::ScheduleStreamJob(_)
        | ThroughputCommand::CancelStreamJob(_) => Ok(()),
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
