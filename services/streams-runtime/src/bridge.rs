use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use fabrik_broker::{decode_consumed_workflow_event, decode_json_record};
use fabrik_events::{EventEnvelope, WorkflowEvent, WorkflowIdentity};
use fabrik_store::{StreamJobCheckpointRecord, StreamJobRecord};
use fabrik_throughput::{
    CancelStreamJobCommand, CompiledStreamJob, PauseStreamJobCommand, ResumeStreamJobCommand,
    STREAM_SOURCE_TOPIC, ScheduleStreamJobCommand, StreamJobBridgeHandleStatus,
    StreamJobCheckpointStatus, StreamJobOriginKind, StreamJobQueryConsistency,
    StreamJobQueryStatus, StreamJobStatus, ThroughputBackend, ThroughputBatchIdentity,
    ThroughputCommand, ThroughputCommandEnvelope, ThroughputRunStatus,
    stream_job_bridge_request_id, stream_job_callback_event_id,
    stream_job_checkpoint_callback_dedupe_key, stream_job_query_callback_dedupe_key,
    stream_job_signal_callback_dedupe_key, stream_job_terminal_callback_dedupe_key,
};
use futures_util::StreamExt;
use serde_json::{Value, json};
use std::collections::{BTreeMap, BTreeSet, HashSet};
use tokio::task::JoinSet;
use tokio::time::{Duration, sleep};
use tracing::{error, info, warn};

use crate::{
    AppState, BRIDGE_EVENT_CONCURRENCY, activate_native_stream_run, activate_stream_batch,
    publish_changelog_records, publish_projection_records, stream_job_owner_epoch,
    stream_jobs::{
        activate_stream_job_on_local_state, build_stream_job_query_output,
        materialization_outcome_from_local_state, resolve_stream_job_definition,
        should_defer_terminal_callback_until_query_boundary,
        sync_stream_job_bridge_callback_records_on_local_state, terminal_result_after_query,
    },
    throughput_partition_for_stream_key, workflow_event_from_throughput_command,
};

const STREAM_DEPLOYMENT_PHASE_RUNNING: &str = "running";
const STREAM_DEPLOYMENT_PHASE_PENDING_CHECKPOINT: &str = "pending_checkpoint";
const STREAM_DEPLOYMENT_PHASE_HANDOFF_IN_PROGRESS: &str = "handoff_in_progress";
const STREAM_DEPLOYMENT_PHASE_STABILIZING: &str = "stabilizing";
const STREAM_DEPLOYMENT_PHASE_PAUSED: &str = "paused";
const STREAM_DEPLOYMENT_PHASE_DRAINING: &str = "draining";
const STREAM_DEPLOYMENT_PHASE_ROLLBACK_PENDING: &str = "rollback_pending";
const STREAM_DEPLOYMENT_PHASE_ROLLED_BACK: &str = "rolled_back";
const STREAM_DEPLOYMENT_PHASE_FAILED: &str = "failed";

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
        StreamJobStatus::Paused => {
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
    materialize_stream_job_from_handle(state, handle.clone(), event.occurred_at).await?;
    if stream_job_uses_topic_source(state, &handle).await? {
        ensure_active_topic_stream_job_worker(state.clone(), handle.handle_id.clone());
    }
    Ok(())
}

fn should_emit_workflow_callbacks(handle: &fabrik_store::StreamJobBridgeHandleRecord) -> bool {
    !matches!(handle.parsed_origin_kind(), Some(StreamJobOriginKind::Standalone))
}

fn rollout_checkpoint_reached(
    job: &StreamJobRecord,
    rollout_checkpoint_name: Option<&str>,
) -> bool {
    rollout_checkpoint_name.is_none_or(|checkpoint_name| {
        job.latest_checkpoint_name.as_deref() == Some(checkpoint_name)
            && job.latest_checkpoint_at.is_some()
    })
}

fn rollout_stable_after_handoff(
    job: &StreamJobRecord,
    handoff_checkpoint_sequence: Option<i64>,
    rollout_checkpoint_name: Option<&str>,
) -> bool {
    if !rollout_checkpoint_reached(job, rollout_checkpoint_name) {
        return false;
    }
    match (job.latest_checkpoint_sequence, handoff_checkpoint_sequence) {
        (Some(current), Some(handoff)) => current > handoff,
        (Some(_), None) => true,
        _ => false,
    }
}

async fn persist_stream_deployment(
    state: &AppState,
    deployment: &mut fabrik_store::StreamDeploymentRecord,
    occurred_at: DateTime<Utc>,
) -> Result<()> {
    deployment.updated_at = occurred_at;
    state.store.upsert_stream_deployment(deployment).await
}

async fn set_deployment_phase(
    state: &AppState,
    deployment: &mut fabrik_store::StreamDeploymentRecord,
    phase: &str,
    occurred_at: DateTime<Utc>,
) -> Result<()> {
    if deployment.rollout_phase == phase {
        return Ok(());
    }
    deployment.rollout_phase = phase.to_owned();
    persist_stream_deployment(state, deployment, occurred_at).await
}

async fn mark_stream_job_drain_requested(
    state: &AppState,
    handle: &mut fabrik_store::StreamJobBridgeHandleRecord,
    occurred_at: DateTime<Utc>,
    reason: &str,
) -> Result<()> {
    if handle.parsed_status().is_some_and(StreamJobBridgeHandleStatus::is_terminal)
        || handle.cancellation_requested_at.is_some()
    {
        return Ok(());
    }
    handle.status = StreamJobBridgeHandleStatus::CancellationRequested.as_str().to_owned();
    handle.cancellation_requested_at = Some(occurred_at);
    handle.cancellation_reason = Some(reason.to_owned());
    handle.updated_at = occurred_at;
    state.store.upsert_stream_job_callback_handle(handle).await?;
    if let Some(mut job) = state
        .store
        .get_stream_job(&handle.tenant_id, &handle.instance_id, &handle.run_id, &handle.job_id)
        .await?
    {
        job.status = StreamJobStatus::Draining.as_str().to_owned();
        job.draining_at = job.draining_at.or(Some(occurred_at));
        job.cancellation_requested_at = Some(occurred_at);
        job.cancellation_reason = Some(reason.to_owned());
        job.updated_at = occurred_at;
        state.store.upsert_stream_job(&job).await?;
    }
    Ok(())
}

async fn pause_stream_job_for_deployment(
    state: &AppState,
    handle: &mut fabrik_store::StreamJobBridgeHandleRecord,
    occurred_at: DateTime<Utc>,
) -> Result<()> {
    if handle.parsed_status().is_some_and(StreamJobBridgeHandleStatus::is_terminal)
        || matches!(
            handle.parsed_status(),
            Some(StreamJobBridgeHandleStatus::CancellationRequested)
                | Some(StreamJobBridgeHandleStatus::Paused)
        )
    {
        return Ok(());
    }
    if !stream_job_uses_topic_source(state, handle).await? {
        return Ok(());
    }
    handle.status = StreamJobBridgeHandleStatus::Paused.as_str().to_owned();
    handle.updated_at = occurred_at;
    handle.stream_owner_epoch = handle
        .stream_owner_epoch
        .or_else(|| stream_job_owner_epoch(state, &handle.job_id))
        .or(Some(1));
    state.store.upsert_stream_job_callback_handle(handle).await?;
    sync_stream_job_record(state, handle, StreamJobStatus::Paused, None, occurred_at).await
}

async fn resume_stream_job_for_deployment(
    state: &AppState,
    handle: &mut fabrik_store::StreamJobBridgeHandleRecord,
    occurred_at: DateTime<Utc>,
    clear_terminal: bool,
) -> Result<()> {
    if !clear_terminal
        && (handle.parsed_status().is_some_and(StreamJobBridgeHandleStatus::is_terminal)
            || matches!(
                handle.parsed_status(),
                Some(StreamJobBridgeHandleStatus::CancellationRequested)
            ))
    {
        return Ok(());
    }
    if !stream_job_uses_topic_source(state, handle).await? {
        return Ok(());
    }
    if clear_terminal {
        handle.terminal_event_id = None;
        handle.terminal_at = None;
        handle.terminal_output = None;
        handle.terminal_error = None;
    }
    handle.status = if handle.stream_owner_epoch.is_some() {
        StreamJobBridgeHandleStatus::Running.as_str().to_owned()
    } else {
        StreamJobBridgeHandleStatus::Admitted.as_str().to_owned()
    };
    handle.cancellation_requested_at = None;
    handle.cancellation_reason = None;
    handle.updated_at = occurred_at;
    handle.stream_owner_epoch = handle
        .stream_owner_epoch
        .or_else(|| stream_job_owner_epoch(state, &handle.job_id))
        .or(Some(1));
    state.store.upsert_stream_job_callback_handle(handle).await?;

    if let Some(mut job) = state
        .store
        .get_stream_job(&handle.tenant_id, &handle.instance_id, &handle.run_id, &handle.job_id)
        .await?
    {
        job.status = if job.running_at.is_some() {
            StreamJobStatus::Running.as_str().to_owned()
        } else {
            StreamJobStatus::Starting.as_str().to_owned()
        };
        job.draining_at = None;
        job.cancellation_requested_at = None;
        job.cancellation_reason = None;
        if clear_terminal {
            job.terminal_event_id = None;
            job.terminal_at = None;
            job.terminal_output = None;
            job.terminal_error = None;
        }
        job.updated_at = occurred_at;
        state.store.upsert_stream_job(&job).await?;
    }

    if clear_terminal
        && let Some(mut runtime_state) =
            state.local_state.load_stream_job_runtime_state(&handle.handle_id)?
    {
        runtime_state.terminal_status = None;
        runtime_state.terminal_output = None;
        runtime_state.terminal_error = None;
        runtime_state.terminal_at = None;
        runtime_state.source_partition_leases.clear();
        runtime_state.updated_at = occurred_at;
        state.local_state.upsert_stream_job_runtime_state(&runtime_state)?;
    }

    materialize_stream_job_from_handle(state, handle.clone(), occurred_at).await
}

fn rollout_partition_ids(
    active_runtime: Option<&crate::local_state::LocalStreamJobRuntimeState>,
    previous_runtime: Option<&crate::local_state::LocalStreamJobRuntimeState>,
) -> BTreeSet<i32> {
    let mut partition_ids = BTreeSet::new();
    if let Some(runtime_state) = previous_runtime {
        for cursor in &runtime_state.source_cursors {
            partition_ids.insert(cursor.source_partition_id);
        }
        for lease in &runtime_state.source_partition_leases {
            partition_ids.insert(lease.source_partition_id);
        }
    }
    if partition_ids.is_empty()
        && let Some(runtime_state) = active_runtime
    {
        for cursor in &runtime_state.source_cursors {
            partition_ids.insert(cursor.source_partition_id);
        }
        for lease in &runtime_state.source_partition_leases {
            partition_ids.insert(lease.source_partition_id);
        }
    }
    partition_ids
}

fn runtime_state_has_any_partition_advanced_beyond(
    active_runtime: Option<&crate::local_state::LocalStreamJobRuntimeState>,
    previous_runtime: Option<&crate::local_state::LocalStreamJobRuntimeState>,
) -> bool {
    let Some(active_runtime) = active_runtime else {
        return false;
    };
    let Some(previous_runtime) = previous_runtime else {
        return active_runtime
            .source_cursors
            .iter()
            .any(|cursor| cursor.last_applied_offset.is_some());
    };
    active_runtime.source_cursors.iter().any(|active_cursor| {
        let previous_offset = previous_runtime
            .source_cursors
            .iter()
            .find(|cursor| cursor.source_partition_id == active_cursor.source_partition_id)
            .and_then(|cursor| cursor.last_applied_offset);
        match (active_cursor.last_applied_offset, previous_offset) {
            (Some(active), Some(previous)) => active > previous,
            (Some(_), None) => true,
            _ => false,
        }
    })
}

fn runtime_state_has_all_partitions_advanced_beyond(
    active_runtime: Option<&crate::local_state::LocalStreamJobRuntimeState>,
    previous_runtime: Option<&crate::local_state::LocalStreamJobRuntimeState>,
) -> bool {
    let Some(active_runtime) = active_runtime else {
        return false;
    };
    let partition_ids = rollout_partition_ids(Some(active_runtime), previous_runtime);
    if partition_ids.is_empty() {
        return false;
    }
    partition_ids.into_iter().all(|source_partition_id| {
        let active_offset = active_runtime
            .source_cursors
            .iter()
            .find(|cursor| cursor.source_partition_id == source_partition_id)
            .and_then(|cursor| cursor.last_applied_offset);
        let previous_offset = previous_runtime.and_then(|runtime_state| {
            runtime_state
                .source_cursors
                .iter()
                .find(|cursor| cursor.source_partition_id == source_partition_id)
                .and_then(|cursor| cursor.last_applied_offset)
        });
        match (active_offset, previous_offset) {
            (Some(active), Some(previous)) => active > previous,
            (Some(_), None) => true,
            _ => false,
        }
    })
}

async fn handoff_stream_rollout_state(
    state: &AppState,
    deployment: &mut fabrik_store::StreamDeploymentRecord,
    active_handle: &fabrik_store::StreamJobBridgeHandleRecord,
    previous_handle: &fabrik_store::StreamJobBridgeHandleRecord,
    previous_job: &StreamJobRecord,
    occurred_at: DateTime<Utc>,
) -> Result<()> {
    let existing_runtime_state =
        state.local_state.load_stream_job_runtime_state(&active_handle.handle_id)?;
    let should_copy_runtime = existing_runtime_state.as_ref().is_none_or(|runtime_state| {
        runtime_state.source_cursors.is_empty() && runtime_state.materialized_key_count == 0
    });
    if should_copy_runtime
        && let Some(previous_runtime_state) =
            state.local_state.load_stream_job_runtime_state(&previous_handle.handle_id)?
    {
        let mut next_runtime_state = previous_runtime_state;
        next_runtime_state.handle_id = active_handle.handle_id.clone();
        next_runtime_state.job_id = active_handle.job_id.clone();
        next_runtime_state.job_name = active_handle.job_name.clone();
        next_runtime_state.stream_owner_epoch =
            stream_job_owner_epoch(state, &active_handle.job_id)
                .or(active_handle.stream_owner_epoch)
                .or(Some(1))
                .unwrap_or(1);
        next_runtime_state.terminal_status = None;
        next_runtime_state.terminal_output = None;
        next_runtime_state.terminal_error = None;
        next_runtime_state.terminal_at = None;
        next_runtime_state.updated_at = occurred_at;
        next_runtime_state.source_partition_leases.clear();
        state.local_state.upsert_stream_job_runtime_state(&next_runtime_state)?;
        if let Some(workers) = state.shard_workers.get() {
            for local_state in workers.local_state_by_partition.values() {
                local_state.upsert_stream_job_runtime_state(&next_runtime_state)?;
            }
        }
    }

    let copied_views = state
        .local_state
        .load_all_stream_job_views()?
        .into_iter()
        .filter(|state| state.handle_id == previous_handle.handle_id)
        .map(|mut state| {
            state.handle_id = active_handle.handle_id.clone();
            state.job_id = active_handle.job_id.clone();
            state.updated_at = occurred_at;
            state
        })
        .collect::<Vec<_>>();
    if !copied_views.is_empty() {
        state.local_state.upsert_stream_job_view_states(&copied_views)?;
        if let Some(workers) = state.shard_workers.get() {
            let mut views_by_partition = BTreeMap::<i32, Vec<_>>::new();
            for view in &copied_views {
                let partition_id = throughput_partition_for_stream_key(
                    &view.logical_key,
                    state.throughput_partitions,
                );
                views_by_partition.entry(partition_id).or_default().push(view.clone());
            }
            for (partition_id, views) in views_by_partition {
                if let Some(local_state) = workers.local_state_for_partition(partition_id) {
                    local_state.upsert_stream_job_view_states(&views)?;
                }
            }
        }
        for view in &copied_views {
            state
                .store
                .upsert_stream_job_view_query(&fabrik_store::StreamJobViewRecord {
                    tenant_id: active_handle.tenant_id.clone(),
                    instance_id: active_handle.instance_id.clone(),
                    run_id: active_handle.run_id.clone(),
                    job_id: active_handle.job_id.clone(),
                    handle_id: active_handle.handle_id.clone(),
                    view_name: view.view_name.clone(),
                    logical_key: view.logical_key.clone(),
                    output: view.output.clone(),
                    checkpoint_sequence: view.checkpoint_sequence,
                    updated_at: view.updated_at,
                })
                .await?;
        }
    }

    if let Some(mut active_job) = state
        .store
        .get_stream_job(
            &active_handle.tenant_id,
            &active_handle.instance_id,
            &active_handle.run_id,
            &active_handle.job_id,
        )
        .await?
    {
        active_job.latest_checkpoint_name = previous_job.latest_checkpoint_name.clone();
        active_job.latest_checkpoint_sequence = previous_job.latest_checkpoint_sequence;
        active_job.latest_checkpoint_at = previous_job.latest_checkpoint_at;
        active_job.latest_checkpoint_output = previous_job.latest_checkpoint_output.clone();
        active_job.updated_at = occurred_at;
        state.store.upsert_stream_job(&active_job).await?;
    }

    deployment.rollout_phase = STREAM_DEPLOYMENT_PHASE_STABILIZING.to_owned();
    deployment.rollout_handoff_checkpoint_sequence = previous_job.latest_checkpoint_sequence;
    deployment.failed_revision_id = None;
    deployment.rollout_failure = None;
    persist_stream_deployment(state, deployment, occurred_at).await?;
    Ok(())
}

async fn gate_stream_job_rollout(
    state: &AppState,
    handle: &mut fabrik_store::StreamJobBridgeHandleRecord,
    occurred_at: DateTime<Utc>,
) -> Result<bool> {
    if !matches!(handle.parsed_origin_kind(), Some(StreamJobOriginKind::Standalone)) {
        return Ok(true);
    }
    let Some(mut deployment) =
        state.store.get_stream_deployment(&handle.tenant_id, &handle.stream_instance_id).await?
    else {
        return Ok(true);
    };

    if deployment.active_stream_run_id.as_deref() == Some(handle.stream_run_id.as_str())
        && deployment.desired_state == StreamJobStatus::Draining.as_str()
    {
        set_deployment_phase(state, &mut deployment, STREAM_DEPLOYMENT_PHASE_DRAINING, occurred_at)
            .await?;
        mark_stream_job_drain_requested(state, handle, occurred_at, "deployment drain requested")
            .await?;
        return Ok(false);
    }

    if deployment.active_stream_run_id.as_deref() == Some(handle.stream_run_id.as_str())
        && deployment.desired_state == StreamJobStatus::Paused.as_str()
    {
        set_deployment_phase(state, &mut deployment, STREAM_DEPLOYMENT_PHASE_PAUSED, occurred_at)
            .await?;
        pause_stream_job_for_deployment(state, handle, occurred_at).await?;
        return Ok(false);
    }

    if deployment.active_stream_run_id.as_deref() != Some(handle.stream_run_id.as_str()) {
        return Ok(true);
    }
    let Some(previous_stream_run_id) = deployment.previous_stream_run_id.clone() else {
        if deployment.desired_state == StreamJobStatus::Running.as_str()
            && deployment.rollout_phase != STREAM_DEPLOYMENT_PHASE_ROLLED_BACK
        {
            set_deployment_phase(
                state,
                &mut deployment,
                STREAM_DEPLOYMENT_PHASE_RUNNING,
                occurred_at,
            )
            .await?;
        }
        return Ok(true);
    };
    if deployment.rollout_handoff_checkpoint_sequence.is_some() {
        return Ok(true);
    }

    let Some(mut previous_handle) = state
        .store
        .get_stream_job_callback_handle_by_stream_identity(
            &handle.tenant_id,
            &handle.stream_instance_id,
            &previous_stream_run_id,
            &handle.job_id,
        )
        .await?
    else {
        deployment.previous_stream_run_id = None;
        deployment.rollout_phase = STREAM_DEPLOYMENT_PHASE_RUNNING.to_owned();
        deployment.rollout_handoff_checkpoint_sequence = None;
        persist_stream_deployment(state, &mut deployment, occurred_at).await?;
        return Ok(true);
    };
    let Some(previous_job) = state
        .store
        .get_stream_job_by_stream_identity(
            &handle.tenant_id,
            &handle.stream_instance_id,
            &previous_stream_run_id,
            &handle.job_id,
        )
        .await?
    else {
        deployment.previous_stream_run_id = None;
        deployment.rollout_phase = STREAM_DEPLOYMENT_PHASE_RUNNING.to_owned();
        deployment.rollout_handoff_checkpoint_sequence = None;
        persist_stream_deployment(state, &mut deployment, occurred_at).await?;
        return Ok(true);
    };

    if !rollout_checkpoint_reached(&previous_job, deployment.rollout_checkpoint_name.as_deref()) {
        set_deployment_phase(
            state,
            &mut deployment,
            STREAM_DEPLOYMENT_PHASE_PENDING_CHECKPOINT,
            occurred_at,
        )
        .await?;
        sync_stream_job_record(state, handle, StreamJobStatus::Starting, None, occurred_at).await?;
        return Ok(false);
    }

    if !previous_handle.parsed_status().is_some_and(StreamJobBridgeHandleStatus::is_terminal)
        && previous_handle.cancellation_requested_at.is_none()
    {
        set_deployment_phase(
            state,
            &mut deployment,
            STREAM_DEPLOYMENT_PHASE_HANDOFF_IN_PROGRESS,
            occurred_at,
        )
        .await?;
        mark_stream_job_drain_requested(
            state,
            &mut previous_handle,
            occurred_at,
            "deployment checkpoint handoff",
        )
        .await?;
        sync_stream_job_record(state, handle, StreamJobStatus::Starting, None, occurred_at).await?;
        return Ok(false);
    }

    if !previous_handle.parsed_status().is_some_and(StreamJobBridgeHandleStatus::is_terminal) {
        set_deployment_phase(
            state,
            &mut deployment,
            STREAM_DEPLOYMENT_PHASE_HANDOFF_IN_PROGRESS,
            occurred_at,
        )
        .await?;
        sync_stream_job_record(state, handle, StreamJobStatus::Starting, None, occurred_at).await?;
        return Ok(false);
    }

    handoff_stream_rollout_state(
        state,
        &mut deployment,
        handle,
        &previous_handle,
        &previous_job,
        occurred_at,
    )
    .await?;
    Ok(true)
}

async fn mark_deployment_failed(
    state: &AppState,
    deployment: &mut fabrik_store::StreamDeploymentRecord,
    failed_revision_id: Option<String>,
    failure: impl Into<String>,
    occurred_at: DateTime<Utc>,
) -> Result<()> {
    deployment.rollout_phase = STREAM_DEPLOYMENT_PHASE_FAILED.to_owned();
    deployment.failed_revision_id = failed_revision_id;
    deployment.rollout_failure = Some(failure.into());
    persist_stream_deployment(state, deployment, occurred_at).await
}

async fn rollback_deployment_to_previous_revision(
    state: &AppState,
    deployment: &mut fabrik_store::StreamDeploymentRecord,
    active_handle: &fabrik_store::StreamJobBridgeHandleRecord,
    active_job: Option<&StreamJobRecord>,
    previous_handle: &fabrik_store::StreamJobBridgeHandleRecord,
    previous_job: Option<&StreamJobRecord>,
    occurred_at: DateTime<Utc>,
) -> Result<()> {
    let active_runtime =
        state.local_state.load_stream_job_runtime_state(&active_handle.handle_id)?;
    let previous_runtime =
        state.local_state.load_stream_job_runtime_state(&previous_handle.handle_id)?;
    let active_checkpoint_sequence = active_job.and_then(|job| job.latest_checkpoint_sequence);
    if matches!(
        (active_checkpoint_sequence, deployment.rollout_handoff_checkpoint_sequence),
        (Some(active), Some(handoff)) if active > handoff
    ) || runtime_state_has_any_partition_advanced_beyond(
        active_runtime.as_ref(),
        previous_runtime.as_ref(),
    ) {
        let failure_cause = active_job
            .and_then(|job| job.terminal_error.clone())
            .or_else(|| active_handle.terminal_error.clone())
            .unwrap_or_else(|| {
                "failed revision terminated during rollout stabilization".to_owned()
            });
        let failure = format!(
            "{failure_cause}; rollback blocked because failed revision advanced source progress past handoff"
        );
        return mark_deployment_failed(
            state,
            deployment,
            Some(active_handle.stream_run_id.clone()),
            failure,
            occurred_at,
        )
        .await;
    }

    let mut resumed_previous = previous_handle.clone();
    resume_stream_job_for_deployment(
        state,
        &mut resumed_previous,
        occurred_at,
        previous_handle.parsed_status().is_some_and(StreamJobBridgeHandleStatus::is_terminal)
            || previous_handle.cancellation_requested_at.is_some(),
    )
    .await?;

    deployment.desired_state = StreamJobStatus::Running.as_str().to_owned();
    deployment.rollout_phase = STREAM_DEPLOYMENT_PHASE_ROLLED_BACK.to_owned();
    deployment.active_stream_run_id = Some(resumed_previous.stream_run_id.clone());
    deployment.active_job_id = Some(resumed_previous.job_id.clone());
    deployment.active_handle_id = Some(resumed_previous.handle_id.clone());
    deployment.previous_stream_run_id = None;
    deployment.rollout_handoff_checkpoint_sequence = None;
    deployment.failed_revision_id = Some(active_handle.stream_run_id.clone());
    deployment.rollout_failure = active_job
        .and_then(|job| job.terminal_error.clone())
        .or_else(|| active_handle.terminal_error.clone())
        .or_else(|| {
            previous_job.map(|job| {
                format!(
                    "rolled back to {} after failed revision {}",
                    job.stream_run_id, active_handle.stream_run_id
                )
            })
        })
        .or_else(|| {
            Some(format!("rolled back after failed revision {}", active_handle.stream_run_id))
        });
    persist_stream_deployment(state, deployment, occurred_at).await
}

async fn reconcile_stream_deployment(
    state: &AppState,
    deployment: &mut fabrik_store::StreamDeploymentRecord,
    occurred_at: DateTime<Utc>,
) -> Result<()> {
    let Some(active_revision_id) = deployment.active_stream_run_id.clone() else {
        return Ok(());
    };
    let Some(active_job_id) = deployment.active_job_id.clone() else {
        return Ok(());
    };
    let active_handle = state
        .store
        .get_stream_job_callback_handle_by_stream_identity(
            &deployment.tenant_id,
            &deployment.deployment_id,
            &active_revision_id,
            &active_job_id,
        )
        .await?;
    let active_job = state
        .store
        .get_stream_job_by_stream_identity(
            &deployment.tenant_id,
            &deployment.deployment_id,
            &active_revision_id,
            &active_job_id,
        )
        .await?;

    if deployment.desired_state == StreamJobStatus::Draining.as_str() {
        if let Some(mut active_handle) = active_handle {
            mark_stream_job_drain_requested(
                state,
                &mut active_handle,
                occurred_at,
                "deployment desired_state draining",
            )
            .await?;
        }
        return set_deployment_phase(
            state,
            deployment,
            STREAM_DEPLOYMENT_PHASE_DRAINING,
            occurred_at,
        )
        .await;
    }

    if deployment.desired_state == StreamJobStatus::Paused.as_str() {
        if let Some(mut active_handle) = active_handle {
            pause_stream_job_for_deployment(state, &mut active_handle, occurred_at).await?;
        }
        return set_deployment_phase(
            state,
            deployment,
            STREAM_DEPLOYMENT_PHASE_PAUSED,
            occurred_at,
        )
        .await;
    }

    let previous_handle =
        if let Some(previous_stream_run_id) = deployment.previous_stream_run_id.clone() {
            state
                .store
                .get_stream_job_callback_handle_by_stream_identity(
                    &deployment.tenant_id,
                    &deployment.deployment_id,
                    &previous_stream_run_id,
                    &active_job_id,
                )
                .await?
        } else {
            None
        };
    let previous_job =
        if let Some(previous_stream_run_id) = deployment.previous_stream_run_id.clone() {
            state
                .store
                .get_stream_job_by_stream_identity(
                    &deployment.tenant_id,
                    &deployment.deployment_id,
                    &previous_stream_run_id,
                    &active_job_id,
                )
                .await?
        } else {
            None
        };

    let active_failed = active_handle.as_ref().is_some_and(|handle| {
        matches!(handle.parsed_status(), Some(StreamJobBridgeHandleStatus::Failed))
    }) || active_job
        .as_ref()
        .is_some_and(|job| matches!(job.parsed_status(), Some(StreamJobStatus::Failed)));

    if deployment.rollout_phase == STREAM_DEPLOYMENT_PHASE_ROLLBACK_PENDING || active_failed {
        if deployment.rollout_phase == STREAM_DEPLOYMENT_PHASE_ROLLBACK_PENDING
            && !active_failed
            && let Some(mut active_handle) = active_handle.clone()
            && !active_handle.parsed_status().is_some_and(StreamJobBridgeHandleStatus::is_terminal)
        {
            mark_stream_job_drain_requested(
                state,
                &mut active_handle,
                occurred_at,
                "deployment rollback requested",
            )
            .await?;
            return Ok(());
        }
        let Some(active_handle) = active_handle.as_ref() else {
            return mark_deployment_failed(
                state,
                deployment,
                Some(active_revision_id),
                "rollback failed because active deployment handle is missing",
                occurred_at,
            )
            .await;
        };
        let Some(previous_handle) = previous_handle.as_ref() else {
            return mark_deployment_failed(
                state,
                deployment,
                Some(active_revision_id),
                "rollback failed because no previous revision is available",
                occurred_at,
            )
            .await;
        };
        return rollback_deployment_to_previous_revision(
            state,
            deployment,
            active_handle,
            active_job.as_ref(),
            previous_handle,
            previous_job.as_ref(),
            occurred_at,
        )
        .await;
    }

    if deployment.previous_stream_run_id.is_some() {
        if let (Some(active_job), Some(active_handle)) =
            (active_job.as_ref(), active_handle.as_ref())
        {
            let active_runtime =
                state.local_state.load_stream_job_runtime_state(&active_handle.handle_id)?;
            let previous_runtime = if let Some(previous_handle) = previous_handle.as_ref() {
                state.local_state.load_stream_job_runtime_state(&previous_handle.handle_id)?
            } else {
                None
            };
            let has_partitioned_rollout =
                !rollout_partition_ids(active_runtime.as_ref(), previous_runtime.as_ref())
                    .is_empty();
            let rollout_stable = if has_partitioned_rollout {
                runtime_state_has_all_partitions_advanced_beyond(
                    active_runtime.as_ref(),
                    previous_runtime.as_ref(),
                )
            } else {
                rollout_stable_after_handoff(
                    active_job,
                    deployment.rollout_handoff_checkpoint_sequence,
                    deployment.rollout_checkpoint_name.as_deref(),
                )
            };
            if rollout_stable {
                deployment.desired_state = StreamJobStatus::Running.as_str().to_owned();
                deployment.rollout_phase = STREAM_DEPLOYMENT_PHASE_RUNNING.to_owned();
                deployment.previous_stream_run_id = None;
                deployment.rollout_handoff_checkpoint_sequence = None;
                deployment.rollout_failure = None;
                persist_stream_deployment(state, deployment, occurred_at).await?;
                return Ok(());
            }
        }

        if deployment.rollout_handoff_checkpoint_sequence.is_some() {
            return set_deployment_phase(
                state,
                deployment,
                STREAM_DEPLOYMENT_PHASE_STABILIZING,
                occurred_at,
            )
            .await;
        }

        if previous_job.as_ref().is_some_and(|job| {
            rollout_checkpoint_reached(job, deployment.rollout_checkpoint_name.as_deref())
        }) {
            return set_deployment_phase(
                state,
                deployment,
                STREAM_DEPLOYMENT_PHASE_HANDOFF_IN_PROGRESS,
                occurred_at,
            )
            .await;
        }

        return set_deployment_phase(
            state,
            deployment,
            STREAM_DEPLOYMENT_PHASE_PENDING_CHECKPOINT,
            occurred_at,
        )
        .await;
    }

    if let Some(mut active_handle) = active_handle {
        if matches!(active_handle.parsed_status(), Some(StreamJobBridgeHandleStatus::Paused)) {
            resume_stream_job_for_deployment(state, &mut active_handle, occurred_at, false).await?;
        }
    }
    if deployment.rollout_phase != STREAM_DEPLOYMENT_PHASE_ROLLED_BACK {
        set_deployment_phase(state, deployment, STREAM_DEPLOYMENT_PHASE_RUNNING, occurred_at)
            .await?;
    }
    Ok(())
}

pub(crate) async fn reconcile_stream_deployments(state: &AppState, page_size: i64) -> Result<()> {
    let mut offset = 0_i64;
    loop {
        let mut batch = state.store.list_stream_deployments_page(page_size, offset).await?;
        if batch.is_empty() {
            break;
        }
        for deployment in &mut batch {
            reconcile_stream_deployment(state, deployment, Utc::now()).await?;
        }
        if batch.len() < usize::try_from(page_size).unwrap_or(usize::MAX) {
            break;
        }
        offset += page_size;
    }
    Ok(())
}

async fn resolve_stream_job_signal_target_identity(
    state: &AppState,
    handle: &fabrik_store::StreamJobBridgeHandleRecord,
    signal: &crate::local_state::LocalStreamJobWorkflowSignalState,
) -> Result<Option<WorkflowIdentity>> {
    if !matches!(handle.parsed_origin_kind(), Some(StreamJobOriginKind::Standalone)) {
        return Ok(Some(WorkflowIdentity::new(
            handle.tenant_id.clone(),
            handle.definition_id.clone(),
            handle.definition_version.unwrap_or_default(),
            handle.artifact_hash.clone().unwrap_or_default(),
            handle.instance_id.clone(),
            handle.run_id.clone(),
            "throughput-runtime",
        )));
    }

    let target_instance_id = signal
        .payload
        .get("targetWorkflowInstanceId")
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .unwrap_or(signal.logical_key.as_str());
    let Some(instance) = state.store.get_instance(&handle.tenant_id, target_instance_id).await?
    else {
        warn!(
            tenant_id = %handle.tenant_id,
            handle_id = %handle.handle_id,
            job_id = %handle.job_id,
            target_instance_id = %target_instance_id,
            signal_id = %signal.signal_id,
            signal_type = %signal.signal_type,
            "standalone stream signal target workflow instance not found; leaving signal unpublished for retry"
        );
        return Ok(None);
    };

    Ok(Some(WorkflowIdentity::new(
        handle.tenant_id.clone(),
        instance.definition_id.clone(),
        instance.definition_version.unwrap_or_default(),
        instance.artifact_hash.clone().unwrap_or_default(),
        instance.instance_id.clone(),
        instance.run_id.clone(),
        "throughput-runtime",
    )))
}

pub(crate) async fn materialize_stream_job_from_handle(
    state: &AppState,
    handle: fabrik_store::StreamJobBridgeHandleRecord,
    occurred_at: DateTime<Utc>,
) -> Result<()> {
    materialize_stream_job_from_handle_with_runtime_override(state, handle, occurred_at, None).await
}

async fn materialize_stream_job_from_handle_with_runtime_override(
    state: &AppState,
    mut handle: fabrik_store::StreamJobBridgeHandleRecord,
    occurred_at: DateTime<Utc>,
    runtime_state_override: Option<&crate::local_state::LocalStreamJobRuntimeState>,
) -> Result<()> {
    info!(
        handle_id = %handle.handle_id,
        job_id = %handle.job_id,
        job_name = %handle.job_name,
        "streams-runtime materializing stream job"
    );
    if matches!(handle.parsed_status(), Some(StreamJobBridgeHandleStatus::Paused)) {
        sync_stream_job_record(state, &handle, StreamJobStatus::Paused, None, occurred_at).await?;
        return Ok(());
    }
    sync_stream_job_record(state, &handle, default_stream_job_status(&handle), None, occurred_at)
        .await?;
    if !gate_stream_job_rollout(state, &mut handle, occurred_at).await? {
        return Ok(());
    }
    if let Some(activation) =
        activate_stream_job_with_runtime_override(state, &handle, None, runtime_state_override)
            .await?
    {
        if !state.runtime.owner_first_apply {
            crate::repair_eventual_stream_job_views_from_changelog(
                state,
                &handle,
                &activation.changelog_records,
            )
            .await?;
        }
        publish_changelog_records(state, activation.changelog_records).await;
        if let Err(error) = publish_projection_records(state, activation.projection_records).await {
            error!(
                error = %error,
                handle_id = %handle.handle_id,
                job_id = %handle.job_id,
                "failed to publish stream job projection records"
            );
        }
    }
    sync_stream_job_materialization_outcome(state, &handle, occurred_at, false).await
}

async fn sync_stream_job_materialization_outcome(
    state: &AppState,
    handle: &fabrik_store::StreamJobBridgeHandleRecord,
    occurred_at: DateTime<Utc>,
    sync_running_status: bool,
) -> Result<()> {
    sync_stream_job_bridge_callback_records_on_local_state(&state.local_state, handle)?;
    let Some(materialized) = materialization_outcome_from_local_state(&state.local_state, &handle)?
    else {
        return Ok(());
    };
    if sync_running_status {
        sync_stream_job_record(state, handle, StreamJobStatus::Running, None, occurred_at).await?;
    }
    let mut checkpoint_to_publish = None;
    if let Some(checkpoint) = materialized.checkpoint.clone() {
        let checkpoint = merge_stream_job_checkpoint_bridge_state(state, checkpoint).await?;
        state.store.upsert_stream_job_callback_checkpoint(&checkpoint).await?;
        let mut checkpoint_handle = handle.clone();
        if checkpoint_handle.stream_owner_epoch.is_none() && checkpoint.stream_owner_epoch.is_some()
        {
            checkpoint_handle.stream_owner_epoch = checkpoint.stream_owner_epoch;
            checkpoint_handle.updated_at = checkpoint.reached_at.unwrap_or(occurred_at);
            state.store.upsert_stream_job_callback_handle(&checkpoint_handle).await?;
        }
        sync_stream_job_record(
            state,
            &checkpoint_handle,
            StreamJobStatus::Running,
            Some(&checkpoint),
            checkpoint.reached_at.unwrap_or(occurred_at),
        )
        .await?;
        checkpoint_to_publish = Some(checkpoint);
    }
    for signal in &materialized.workflow_signals {
        publish_stream_job_signal_event(state, handle, signal).await?;
    }
    if let Some(terminal) = materialized.terminal {
        let finalized =
            persist_stream_job_terminal_result(state, handle, terminal, occurred_at).await?;
        let mut synced_finalized = finalized.clone();
        if let Some(latest_finalized) =
            state.store.get_stream_job_callback_handle_by_handle_id(&finalized.handle_id).await?
        {
            synced_finalized.workflow_accepted_at =
                latest_finalized.workflow_accepted_at.or(synced_finalized.workflow_accepted_at);
        }
        sync_stream_job_record(
            state,
            &synced_finalized,
            default_stream_job_status(&synced_finalized),
            None,
            synced_finalized.terminal_at.unwrap_or(occurred_at),
        )
        .await?;
        if !should_emit_workflow_callbacks(&synced_finalized) {
            return Ok(());
        }
        if let Some(checkpoint) = checkpoint_to_publish.as_ref() {
            if checkpoint.reached_at.is_some()
                && checkpoint.accepted_at.is_none()
                && checkpoint.cancelled_at.is_none()
            {
                info!(
                    handle_id = %synced_finalized.handle_id,
                    job_id = %synced_finalized.job_id,
                    checkpoint_name = %checkpoint.checkpoint_name,
                    checkpoint_sequence = checkpoint.checkpoint_sequence,
                    "streams-runtime publishing stream job checkpoint callback"
                );
                publish_stream_job_checkpoint_event(state, &synced_finalized, checkpoint).await?;
            } else if checkpoint.reached_at.is_some() {
                record_duplicate_stream_job_checkpoint_publication(
                    state,
                    &synced_finalized,
                    checkpoint,
                );
            }
        }
        if should_defer_terminal_callback_until_query_boundary(&synced_finalized) {
            info!(
                handle_id = %synced_finalized.handle_id,
                job_id = %synced_finalized.job_id,
                "streams-runtime deferring stream job terminal callback until workflow bridge is ready"
            );
        } else {
            publish_stream_job_terminal_event(state, &synced_finalized).await?;
        }
    } else if let Some(checkpoint) = checkpoint_to_publish.as_ref()
        && should_emit_workflow_callbacks(handle)
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
            publish_stream_job_checkpoint_event(state, handle, checkpoint).await?;
        } else if checkpoint.reached_at.is_some() {
            record_duplicate_stream_job_checkpoint_publication(state, handle, checkpoint);
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
    materialize_stream_job_from_handle(state, handle.clone(), occurred_at).await?;
    if stream_job_uses_topic_source(state, &handle).await? {
        ensure_active_topic_stream_job_worker(state.clone(), handle.handle_id.clone());
    }
    Ok(())
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
            let latest_finalized = state
                .store
                .get_stream_job_callback_handle_by_handle_id(&finalized.handle_id)
                .await?
                .unwrap_or(finalized);
            publish_stream_job_terminal_event(state, &latest_finalized).await?;
        }
    }
    Ok(())
}

async fn handle_pause_stream_job_command(
    state: &AppState,
    command: &PauseStreamJobCommand,
    occurred_at: DateTime<Utc>,
) -> Result<()> {
    let Some(mut handle) =
        state.store.get_stream_job_callback_handle_by_handle_id(&command.handle_id).await?
    else {
        info!(
            tenant_id = %command.tenant_id,
            stream_instance_id = %command.stream_instance_id,
            stream_run_id = %command.stream_run_id,
            job_id = %command.job_id,
            handle_id = %command.handle_id,
            "streams-runtime did not find stream job handle for pause command"
        );
        return Ok(());
    };
    if handle.parsed_status().is_some_and(StreamJobBridgeHandleStatus::is_terminal)
        || matches!(
            handle.parsed_status(),
            Some(StreamJobBridgeHandleStatus::CancellationRequested)
        )
    {
        return Ok(());
    }
    if !stream_job_uses_topic_source(state, &handle).await? {
        return Ok(());
    }
    handle.status = StreamJobBridgeHandleStatus::Paused.as_str().to_owned();
    handle.updated_at = occurred_at;
    if handle.stream_owner_epoch.is_none() {
        handle.stream_owner_epoch = stream_job_owner_epoch(state, &handle.job_id);
    }
    state.store.upsert_stream_job_callback_handle(&handle).await?;
    sync_stream_job_record(state, &handle, StreamJobStatus::Paused, None, occurred_at).await
}

async fn handle_resume_stream_job_command(
    state: &AppState,
    command: &ResumeStreamJobCommand,
    occurred_at: DateTime<Utc>,
) -> Result<()> {
    let Some(mut handle) =
        state.store.get_stream_job_callback_handle_by_handle_id(&command.handle_id).await?
    else {
        info!(
            tenant_id = %command.tenant_id,
            stream_instance_id = %command.stream_instance_id,
            stream_run_id = %command.stream_run_id,
            job_id = %command.job_id,
            handle_id = %command.handle_id,
            "streams-runtime did not find stream job handle for resume command"
        );
        return Ok(());
    };
    if handle.parsed_status().is_some_and(StreamJobBridgeHandleStatus::is_terminal)
        || matches!(
            handle.parsed_status(),
            Some(StreamJobBridgeHandleStatus::CancellationRequested)
        )
    {
        return Ok(());
    }
    if !stream_job_uses_topic_source(state, &handle).await? {
        return Ok(());
    }
    handle.status = StreamJobBridgeHandleStatus::Running.as_str().to_owned();
    handle.updated_at = occurred_at;
    if handle.stream_owner_epoch.is_none() {
        handle.stream_owner_epoch = stream_job_owner_epoch(state, &handle.job_id);
    }
    state.store.upsert_stream_job_callback_handle(&handle).await?;
    sync_stream_job_record(state, &handle, StreamJobStatus::Running, None, occurred_at).await?;
    materialize_stream_job_from_handle(state, handle.clone(), occurred_at).await?;
    ensure_active_topic_stream_job_worker(state.clone(), handle.handle_id.clone());
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
    reconcile_stream_deployments(state, page_size).await?;
    tick_active_topic_stream_job_workers(state, page_size).await?;
    reconcile_stream_job_callback_repairs(state, page_size).await
}

pub(crate) async fn tick_active_topic_stream_job_workers(
    state: &AppState,
    page_size: i64,
) -> Result<()> {
    retain_finished_active_topic_stream_job_workers(state);
    let mut offset = 0;
    loop {
        let handles =
            state.store.list_active_stream_job_callback_handles_page(page_size, offset).await?;
        if handles.is_empty() {
            break;
        }
        for handle in &handles {
            let is_topic_backed = resolve_stream_job_definition(Some(&state.store), handle)
                .await?
                .is_some_and(|job| job.source.kind == STREAM_SOURCE_TOPIC);
            if is_topic_backed {
                ensure_active_topic_stream_job_worker(state.clone(), handle.handle_id.clone());
            }
        }
        if handles.len() < usize::try_from(page_size).unwrap_or(usize::MAX) {
            break;
        }
        offset += page_size;
    }
    Ok(())
}

pub(crate) async fn reconcile_stream_job_callback_repairs(
    state: &AppState,
    page_size: i64,
) -> Result<()> {
    let mut reconciled_handle_ids = HashSet::new();
    let repair_limit = page_size.max(1);
    loop {
        let pending_repairs =
            state.store.list_pending_stream_job_callback_repairs(repair_limit).await?;
        if pending_repairs.is_empty() {
            break;
        }
        let mut repaired_any = false;
        for ledger in pending_repairs {
            if !reconciled_handle_ids.insert(ledger.handle_id.clone()) {
                continue;
            }
            let Some(handle) =
                state.store.get_stream_job_callback_handle_by_handle_id(&ledger.handle_id).await?
            else {
                continue;
            };
            materialize_stream_job_from_handle(state, handle, Utc::now()).await?;
            repaired_any = true;
        }
        if !repaired_any {
            break;
        }
    }
    let pending_queries =
        state.store.list_pending_stream_job_query_callback_repairs(repair_limit).await?;
    let mut reconciled_query_ids = HashSet::new();
    for query in pending_queries {
        if !reconciled_query_ids.insert(query.query_id.clone()) {
            continue;
        }
        let Some(handle) =
            state.store.get_stream_job_callback_handle_by_handle_id(&query.handle_id).await?
        else {
            continue;
        };
        publish_stream_job_query_callback_event(
            state,
            &handle,
            &query,
            query.completed_at.unwrap_or_else(Utc::now),
        )
        .await?;
    }
    Ok(())
}

fn retain_finished_active_topic_stream_job_workers(state: &AppState) {
    let mut workers =
        state.active_topic_job_workers.lock().expect("active topic worker lock poisoned");
    workers.retain(|_, task| !task.is_finished());
}

fn ensure_active_topic_stream_job_worker(state: AppState, handle_id: String) -> bool {
    let mut workers =
        state.active_topic_job_workers.lock().expect("active topic worker lock poisoned");
    if workers.contains_key(&handle_id) {
        return false;
    }
    let worker_handle_id = handle_id.clone();
    let worker_state = state.clone();
    let task = tokio::spawn(async move {
        run_active_topic_stream_job_worker(worker_state, worker_handle_id).await;
    });
    workers.insert(handle_id, task);
    true
}

fn topic_runtime_is_caught_up(
    runtime_state: Option<&crate::local_state::LocalStreamJobRuntimeState>,
) -> bool {
    let Some(runtime_state) = runtime_state else {
        return true;
    };
    runtime_state.source_cursors.iter().all(|cursor| {
        let frontier = cursor
            .last_high_watermark
            .unwrap_or(cursor.initial_checkpoint_target_offset)
            .max(cursor.initial_checkpoint_target_offset);
        cursor.next_offset >= frontier
    })
}

fn topic_runtime_progressed(
    previous: Option<&crate::local_state::LocalStreamJobRuntimeState>,
    current: Option<&crate::local_state::LocalStreamJobRuntimeState>,
) -> bool {
    let Some(current) = current else {
        return false;
    };
    let Some(previous) = previous else {
        return current.source_cursors.iter().any(|cursor| cursor.last_applied_offset.is_some());
    };
    current.source_cursors.iter().any(|cursor| {
        let previous_cursor = previous
            .source_cursors
            .iter()
            .find(|candidate| candidate.source_partition_id == cursor.source_partition_id);
        cursor.last_applied_offset
            > previous_cursor.and_then(|candidate| candidate.last_applied_offset)
            || cursor.next_offset
                > previous_cursor.map(|candidate| candidate.next_offset).unwrap_or(0)
    }) || current.checkpoint_sequence > previous.checkpoint_sequence
}

async fn run_active_topic_stream_job_worker(state: AppState, handle_id: String) {
    let mut handle = match state.store.get_stream_job_callback_handle_by_handle_id(&handle_id).await
    {
        Ok(Some(handle)) => handle,
        Ok(None) => return,
        Err(error) => {
            error!(error = %error, handle_id = %handle_id, "failed to load active topic stream job handle");
            return;
        }
    };
    let mut resolved_job: Option<CompiledStreamJob> = None;
    loop {
        if handle.parsed_status().is_some_and(StreamJobBridgeHandleStatus::is_terminal) {
            break;
        }
        if resolved_job.is_none() {
            resolved_job = match resolve_stream_job_definition(Some(&state.store), &handle).await {
                Ok(job) => job,
                Err(error) => {
                    error!(error = %error, handle_id = %handle_id, "failed to resolve active topic stream job definition");
                    sleep(Duration::from_millis(100)).await;
                    continue;
                }
            };
        }
        if !resolved_job.as_ref().is_some_and(|job| job.source.kind == STREAM_SOURCE_TOPIC) {
            break;
        }

        let mut current_runtime =
            state.local_state.load_stream_job_runtime_state(&handle.handle_id).ok().flatten();
        if current_runtime.is_none() {
            if let Err(error) =
                materialize_stream_job_from_handle(&state, handle.clone(), Utc::now()).await
            {
                error!(error = %error, handle_id = %handle_id, job_id = %handle.job_id, "active topic stream job worker materialization failed");
                sleep(Duration::from_millis(100)).await;
                continue;
            }
            current_runtime =
                state.local_state.load_stream_job_runtime_state(&handle.handle_id).ok().flatten();
        }
        let mut progressed_any = false;
        let mut caught_up = topic_runtime_is_caught_up(current_runtime.as_ref());
        let mut batched_projection_records = Vec::new();
        let mut batched_changelog_records = Vec::new();
        for round in 0..64 {
            if caught_up && round > 0 {
                break;
            }
            let before_runtime = current_runtime.clone();
            let activation = match prepare_topic_stream_job_direct_activation(
                &state,
                &handle,
                resolved_job.as_ref(),
                before_runtime.clone(),
            )
            .await
            {
                Ok(activation) => activation,
                Err(error) => {
                    error!(error = %error, handle_id = %handle_id, job_id = %handle.job_id, "active topic stream job worker direct activation failed");
                    current_runtime = None;
                    break;
                }
            };
            if let Some(mut activation) = activation.activation {
                batched_projection_records.append(&mut activation.projection_records);
                batched_changelog_records.append(&mut activation.changelog_records);
            }
            let after_runtime = activation.runtime_state;
            let progressed =
                topic_runtime_progressed(before_runtime.as_ref(), after_runtime.as_ref());
            progressed_any |= progressed;
            caught_up = topic_runtime_is_caught_up(after_runtime.as_ref());
            if after_runtime
                .as_ref()
                .and_then(|runtime_state| runtime_state.terminal_status.as_deref())
                .is_some()
            {
                break;
            }
            current_runtime = after_runtime;
            if !progressed {
                break;
            }
        }
        if !batched_changelog_records.is_empty() || !batched_projection_records.is_empty() {
            if !state.runtime.owner_first_apply && !batched_changelog_records.is_empty() {
                if let Err(error) = crate::repair_eventual_stream_job_views_from_changelog(
                    &state,
                    &handle,
                    &batched_changelog_records,
                )
                .await
                {
                    error!(
                        error = %error,
                        handle_id = %handle_id,
                        job_id = %handle.job_id,
                        "failed to repair eventual stream job views during direct topic activation batch"
                    );
                }
            }
            publish_changelog_records(&state, batched_changelog_records).await;
            if let Err(error) = publish_projection_records(&state, batched_projection_records).await
            {
                error!(
                    error = %error,
                    handle_id = %handle_id,
                    job_id = %handle.job_id,
                    "failed to publish batched stream job projection records during direct topic activation"
                );
            }
            if let Err(error) =
                sync_stream_job_materialization_outcome(&state, &handle, Utc::now(), false).await
            {
                error!(
                    error = %error,
                    handle_id = %handle_id,
                    job_id = %handle.job_id,
                    "failed to sync materialization outcome after direct topic activation batch"
                );
            }
        }
        if caught_up && !progressed_any {
            match state.store.get_stream_job_callback_handle_by_handle_id(&handle_id).await {
                Ok(Some(latest_handle)) => {
                    handle = latest_handle;
                }
                Ok(None) => break,
                Err(error) => {
                    error!(error = %error, handle_id = %handle_id, "failed to refresh active topic stream job handle");
                    sleep(Duration::from_millis(100)).await;
                    continue;
                }
            }
            sleep(Duration::from_millis(20)).await;
        } else {
            tokio::task::yield_now().await;
        }
    }

    let mut workers =
        state.active_topic_job_workers.lock().expect("active topic worker lock poisoned");
    workers.remove(&handle_id);
}

async fn prepare_topic_stream_job_direct_activation(
    state: &AppState,
    handle: &fabrik_store::StreamJobBridgeHandleRecord,
    resolved_job_override: Option<&CompiledStreamJob>,
    runtime_state_override: Option<crate::local_state::LocalStreamJobRuntimeState>,
) -> Result<TopicDirectDrainOutcome> {
    if let Some(shard_workers) = state.shard_workers.get()
        && let Some(resolved_job) = resolved_job_override.cloned()
    {
        let result = shard_workers
            .drain_topic_stream_job_directly(
                handle.clone(),
                resolved_job,
                state.throughput_partitions,
                runtime_state_override,
            )
            .await?;
        return Ok(TopicDirectDrainOutcome {
            activation: result.activation,
            runtime_state: result.runtime_state,
        });
    }
    let activation = activate_stream_job_with_runtime_override(
        state,
        handle,
        resolved_job_override,
        runtime_state_override.as_ref(),
    )
    .await?;
    let runtime_state = state.local_state.load_stream_job_runtime_state(&handle.handle_id)?;
    Ok(TopicDirectDrainOutcome { activation, runtime_state })
}

struct TopicDirectDrainOutcome {
    activation: Option<crate::StreamJobActivationResult>,
    runtime_state: Option<crate::local_state::LocalStreamJobRuntimeState>,
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

async fn activate_stream_job_with_runtime_override(
    state: &AppState,
    handle: &fabrik_store::StreamJobBridgeHandleRecord,
    resolved_job_override: Option<&CompiledStreamJob>,
    runtime_state_override: Option<&crate::local_state::LocalStreamJobRuntimeState>,
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
    shard_workers
        .activate_stream_job_with_runtime_override(
            handle.clone(),
            resolved_job_override.cloned(),
            state.throughput_partitions,
            runtime_state_override.cloned(),
        )
        .await
}

fn default_stream_job_status(
    handle: &fabrik_store::StreamJobBridgeHandleRecord,
) -> StreamJobStatus {
    match handle.parsed_status() {
        Some(StreamJobBridgeHandleStatus::Completed) => StreamJobStatus::Completed,
        Some(StreamJobBridgeHandleStatus::Failed) => StreamJobStatus::Failed,
        Some(StreamJobBridgeHandleStatus::Cancelled) => StreamJobStatus::Cancelled,
        Some(StreamJobBridgeHandleStatus::Paused) => StreamJobStatus::Paused,
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

async fn publish_stream_job_signal_event(
    state: &AppState,
    handle: &fabrik_store::StreamJobBridgeHandleRecord,
    signal: &crate::local_state::LocalStreamJobWorkflowSignalState,
) -> Result<()> {
    if signal.published_at.is_some() {
        return Ok(());
    }
    let Some(identity) = resolve_stream_job_signal_target_identity(state, handle, signal).await?
    else {
        return Ok(());
    };
    let payload = WorkflowEvent::SignalQueued {
        signal_id: signal.signal_id.clone(),
        signal_type: signal.signal_type.clone(),
        payload: signal.payload.clone(),
    };
    let dedupe_key = stream_job_signal_callback_dedupe_key(
        &handle.tenant_id,
        &handle.instance_id,
        &handle.run_id,
        &handle.job_id,
        &signal.operator_id,
        &signal.logical_key,
    );
    let mut envelope = EventEnvelope::new(payload.event_type(), identity, payload);
    envelope.event_id = stream_job_callback_event_id(&dedupe_key);
    envelope.occurred_at = signal.signaled_at;
    envelope.dedupe_key = Some(dedupe_key);
    attach_stream_job_bridge_metadata(
        state,
        handle,
        Some(signal.stream_owner_epoch),
        &mut envelope,
    );
    enqueue_stream_job_callback_event(state, &envelope).await?;
    state.local_state.mark_stream_job_workflow_signal_published(
        &signal.handle_id,
        &signal.operator_id,
        &signal.logical_key,
        Utc::now(),
    )?;
    Ok(())
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
    if handle.terminal_event_id.is_some() {
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

    publish_stream_job_query_callback_event(state, &handle, &query, event.occurred_at).await
}

async fn publish_stream_job_query_callback_event(
    state: &AppState,
    handle: &fabrik_store::StreamJobBridgeHandleRecord,
    query: &fabrik_store::StreamJobQueryRecord,
    occurred_at: DateTime<Utc>,
) -> Result<()> {
    if query.parsed_status().is_some_and(|status| {
        matches!(status, StreamJobQueryStatus::Accepted | StreamJobQueryStatus::Cancelled)
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
    envelope.occurred_at = query.completed_at.unwrap_or(occurred_at);
    envelope.dedupe_key = Some(dedupe_key);
    attach_stream_job_bridge_metadata(state, &handle, query.stream_owner_epoch, &mut envelope);
    enqueue_stream_job_callback_event(state, &envelope).await?;
    publish_stream_job_terminal_after_query_if_pending(state, handle, occurred_at).await
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
    #[cfg(test)]
    if state
        .fail_next_workflow_outbox_writes
        .fetch_update(
            std::sync::atomic::Ordering::SeqCst,
            std::sync::atomic::Ordering::SeqCst,
            |remaining| {
                if remaining > 0 { Some(remaining - 1) } else { None }
            },
        )
        .is_ok()
    {
        anyhow::bail!("test failpoint: workflow event outbox enqueue failed");
    }
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
        ThroughputCommand::PauseStreamJob(pause) => {
            return handle_pause_stream_job_command(&state, pause, command.occurred_at).await;
        }
        ThroughputCommand::ResumeStreamJob(resume) => {
            return handle_resume_stream_job_command(&state, resume, command.occurred_at).await;
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
        | ThroughputCommand::PauseStreamJob(_)
        | ThroughputCommand::ResumeStreamJob(_)
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
