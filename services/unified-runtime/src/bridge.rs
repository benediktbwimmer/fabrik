use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use fabrik_events::{EventEnvelope, WorkflowEvent, WorkflowIdentity};
use fabrik_store::{
    StreamJobBridgeHandleRecord, StreamJobCheckpointRecord, StreamJobQueryRecord, StreamJobRecord,
    ThroughputBridgeProgressRecord, ThroughputRunRecord,
};
use fabrik_throughput::{
    ActivityExecutionCapabilities, AwaitStreamCheckpointRequest, AwaitStreamJobTerminalRequest,
    CancelStreamJobRequest, PayloadHandle, QueryStreamJobRequest, StartThroughputRunCommand,
    StreamJobBridgeHandleStatus, StreamJobCheckpointStatus, StreamJobOriginKind,
    StreamJobQueryStatus, StreamJobStatus, SubmitStreamJobRequest, SubmitStreamJobResponse,
    THROUGHPUT_BRIDGE_PROTOCOL_VERSION, ThroughputBackend, ThroughputBridgeOperationKind,
    ThroughputBridgeState, ThroughputBridgeTerminalStatus, ThroughputCommand,
    ThroughputCommandEnvelope, ThroughputExecutionPath, ThroughputRunStatus,
    stream_job_callback_event_id, stream_job_checkpoint_callback_dedupe_key,
    stream_job_query_callback_dedupe_key, stream_job_terminal_callback_dedupe_key,
    throughput_bridge_request_id, throughput_bridge_request_matches,
    throughput_cancel_command_envelope, throughput_partition_key, throughput_start_command_id,
    throughput_start_dedupe_key,
};
use fabrik_workflow::{ExecutionTurnContext, WorkflowInstanceState, execution_state_for_event};
use serde_json::{Value, json};
use tracing::{error, info};
use uuid::Uuid;

use crate::{
    ActiveActivityMeta, AppState, BulkAdmissionDecision, PostPlanEffect, PreparedDbAction,
    QueueKey, RunKey, apply_compiled_plan, apply_db_actions, apply_post_plan_effects,
    bulk_terminal_payload, mark_runtime_dirty, maybe_enact_pending_workflow_cancellation_unified,
    schedule_activities_from_plan,
};

fn bridge_submission_state(progress: &ThroughputBridgeProgressRecord) -> ThroughputBridgeState {
    ThroughputBridgeState {
        protocol_version: progress.protocol_version.clone(),
        operation_kind: progress
            .parsed_operation_kind()
            .unwrap_or(ThroughputBridgeOperationKind::BulkRun),
        workflow_event_id: Some(progress.workflow_event_id),
        bridge_request_id: progress.bridge_request_id.clone(),
        submission_status: progress.parsed_submission_status(),
        command_id: progress.command_id,
        command_partition_key: progress.command_partition_key.clone(),
        command_published_at: progress.command_published_at,
        cancellation_requested_at: progress.cancellation_requested_at,
        cancellation_reason: progress.cancellation_reason.clone(),
        cancel_command_published_at: progress.cancel_command_published_at,
        cancelled_at: progress.cancelled_at,
        stream_status: None,
        stream_terminal_at: None,
        workflow_status: None,
        workflow_terminal_event_id: None,
        workflow_owner_epoch: None,
        workflow_accepted_at: None,
    }
}

fn bridge_run_terminal_state(run: &ThroughputRunRecord) -> ThroughputBridgeState {
    ThroughputBridgeState {
        protocol_version: run.bridge_protocol_version.clone(),
        operation_kind: run
            .parsed_bridge_operation_kind()
            .unwrap_or(ThroughputBridgeOperationKind::BulkRun),
        workflow_event_id: None,
        bridge_request_id: run.bridge_request_id.clone(),
        submission_status: None,
        command_id: Some(run.command.command_id),
        command_partition_key: Some(run.command.partition_key.clone()),
        command_published_at: run.command_published_at,
        cancellation_requested_at: None,
        cancellation_reason: None,
        cancel_command_published_at: None,
        cancelled_at: None,
        stream_status: None,
        stream_terminal_at: run.terminal_at,
        workflow_status: run.parsed_workflow_terminal_status(),
        workflow_terminal_event_id: run.bridge_terminal_event_id,
        workflow_owner_epoch: run.bridge_terminal_owner_epoch,
        workflow_accepted_at: run.bridge_terminal_accepted_at,
    }
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

fn default_stream_job_status(handle: &StreamJobBridgeHandleRecord) -> StreamJobStatus {
    match handle.parsed_status().unwrap_or(StreamJobBridgeHandleStatus::Admitted) {
        StreamJobBridgeHandleStatus::Admitted => StreamJobStatus::Created,
        StreamJobBridgeHandleStatus::Running => StreamJobStatus::Running,
        StreamJobBridgeHandleStatus::Paused => StreamJobStatus::Paused,
        StreamJobBridgeHandleStatus::CancellationRequested => StreamJobStatus::Draining,
        StreamJobBridgeHandleStatus::Completed => StreamJobStatus::Completed,
        StreamJobBridgeHandleStatus::Failed => StreamJobStatus::Failed,
        StreamJobBridgeHandleStatus::Cancelled => StreamJobStatus::Cancelled,
    }
}

async fn sync_stream_job_record(
    state: &AppState,
    handle: &StreamJobBridgeHandleRecord,
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
            origin_kind: handle.origin_kind.clone(),
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
    record.stream_instance_id = handle.stream_instance_id.clone();
    record.stream_run_id = handle.stream_run_id.clone();
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
        StreamJobStatus::Paused => {
            if record.starting_at.is_none() {
                record.starting_at = Some(occurred_at);
            }
            if record.running_at.is_none() {
                record.running_at = Some(occurred_at);
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
            if record.draining_at.is_none()
                && matches!(status, StreamJobStatus::Cancelled)
                && handle.cancellation_requested_at.is_some()
            {
                record.draining_at = handle.cancellation_requested_at;
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum BulkTerminalBridgeAcceptance {
    Applied,
    IgnoredMissingRun,
    IgnoredStaleOwnerEpoch,
    IgnoredClosedRun,
}

fn bulk_terminal_batch_id(event: &WorkflowEvent) -> Option<&str> {
    match event {
        WorkflowEvent::BulkActivityBatchCompleted { batch_id, .. }
        | WorkflowEvent::BulkActivityBatchFailed { batch_id, .. }
        | WorkflowEvent::BulkActivityBatchCancelled { batch_id, .. } => Some(batch_id.as_str()),
        _ => None,
    }
}

fn bulk_terminal_status(event: &WorkflowEvent) -> Option<ThroughputBridgeTerminalStatus> {
    match event {
        WorkflowEvent::BulkActivityBatchCompleted { .. } => {
            Some(ThroughputBridgeTerminalStatus::Completed)
        }
        WorkflowEvent::BulkActivityBatchFailed { .. } => {
            Some(ThroughputBridgeTerminalStatus::Failed)
        }
        WorkflowEvent::BulkActivityBatchCancelled { .. } => {
            Some(ThroughputBridgeTerminalStatus::Cancelled)
        }
        _ => None,
    }
}

fn bulk_terminal_owner_epoch(event: &EventEnvelope<WorkflowEvent>) -> Option<u64> {
    event.metadata.get("bridge_owner_epoch").and_then(|value| value.parse::<u64>().ok())
}

async fn bulk_terminal_owner_epoch_is_stale(
    state: &AppState,
    event: &EventEnvelope<WorkflowEvent>,
) -> Result<bool> {
    let Some(callback_owner_epoch) = bulk_terminal_owner_epoch(event) else {
        return Ok(false);
    };
    let Some(batch_id) = bulk_terminal_batch_id(&event.payload) else {
        return Ok(false);
    };
    let Some(current_owner_epoch) = state
        .store
        .get_throughput_projection_batch_max_owner_epoch(
            &event.tenant_id,
            &event.instance_id,
            &event.run_id,
            batch_id,
        )
        .await?
    else {
        return Ok(false);
    };
    Ok(callback_owner_epoch < current_owner_epoch)
}

async fn classify_cold_bulk_terminal_via_bridge(
    state: &AppState,
    event: &EventEnvelope<WorkflowEvent>,
) -> Result<BulkTerminalBridgeAcceptance> {
    let Some(batch_id) = bulk_terminal_batch_id(&event.payload) else {
        return Ok(BulkTerminalBridgeAcceptance::IgnoredMissingRun);
    };

    if let Some(run) = state
        .store
        .get_throughput_run(&event.tenant_id, &event.instance_id, &event.run_id, batch_id)
        .await?
    {
        if !throughput_bridge_request_matches(
            &run.bridge_request_id,
            &event.tenant_id,
            &event.instance_id,
            &event.run_id,
            batch_id,
        ) {
            return Ok(BulkTerminalBridgeAcceptance::IgnoredMissingRun);
        }
        if bridge_run_terminal_state(&run).has_workflow_acceptance() {
            return Ok(BulkTerminalBridgeAcceptance::IgnoredClosedRun);
        }
    }

    let stored = state.store.get_instance(&event.tenant_id, &event.instance_id).await?;
    Ok(
        if stored.is_some_and(|instance| {
            instance.run_id == event.run_id && instance.status.is_terminal()
        }) {
            BulkTerminalBridgeAcceptance::IgnoredClosedRun
        } else {
            BulkTerminalBridgeAcceptance::IgnoredMissingRun
        },
    )
}

pub(super) async fn accept_bulk_batch_terminal_via_bridge(
    state: &AppState,
    event: EventEnvelope<WorkflowEvent>,
) -> Result<BulkTerminalBridgeAcceptance> {
    if bulk_terminal_owner_epoch_is_stale(state, &event).await? {
        return Ok(BulkTerminalBridgeAcceptance::IgnoredStaleOwnerEpoch);
    }

    let run_key = RunKey {
        tenant_id: event.tenant_id.clone(),
        instance_id: event.instance_id.clone(),
        run_id: event.run_id.clone(),
    };

    let hot_result = {
        let mut inner = state.inner.lock().expect("unified runtime lock poisoned");
        match inner.instances.get_mut(&run_key) {
            None => None,
            Some(runtime) if runtime.instance.status.is_terminal() => {
                Some(Err(BulkTerminalBridgeAcceptance::IgnoredClosedRun))
            }
            Some(runtime) => {
                let mut general = Vec::new();
                let mut schedules = Vec::new();
                let current_state = runtime
                    .instance
                    .current_state
                    .clone()
                    .unwrap_or_else(|| runtime.artifact.workflow.initial_state.clone());
                let execution_state = execution_state_for_event(&runtime.instance, Some(&event));
                let turn_context = ExecutionTurnContext {
                    event_id: event.event_id,
                    occurred_at: event.occurred_at,
                };
                let plan = match &event.payload {
                    WorkflowEvent::BulkActivityBatchCompleted {
                        batch_id,
                        total_items,
                        succeeded_items,
                        failed_items,
                        cancelled_items,
                        chunk_count,
                        reducer_output,
                    } => runtime.artifact.execute_after_bulk_completion_with_turn(
                        &current_state,
                        batch_id,
                        &bulk_terminal_payload(
                            batch_id,
                            "completed",
                            None,
                            *total_items,
                            *succeeded_items,
                            *failed_items,
                            *cancelled_items,
                            *chunk_count,
                            reducer_output.as_ref(),
                        ),
                        execution_state,
                        turn_context,
                    )?,
                    WorkflowEvent::BulkActivityBatchFailed {
                        batch_id,
                        total_items,
                        succeeded_items,
                        failed_items,
                        cancelled_items,
                        chunk_count,
                        message,
                        reducer_output,
                    } => runtime.artifact.execute_after_bulk_failure_with_turn(
                        &current_state,
                        batch_id,
                        &bulk_terminal_payload(
                            batch_id,
                            "failed",
                            Some(message.as_str()),
                            *total_items,
                            *succeeded_items,
                            *failed_items,
                            *cancelled_items,
                            *chunk_count,
                            reducer_output.as_ref(),
                        ),
                        execution_state,
                        turn_context,
                    )?,
                    WorkflowEvent::BulkActivityBatchCancelled {
                        batch_id,
                        total_items,
                        succeeded_items,
                        failed_items,
                        cancelled_items,
                        chunk_count,
                        message,
                        reducer_output,
                    } => runtime.artifact.execute_after_bulk_failure_with_turn(
                        &current_state,
                        batch_id,
                        &bulk_terminal_payload(
                            batch_id,
                            "cancelled",
                            Some(message.as_str()),
                            *total_items,
                            *succeeded_items,
                            *failed_items,
                            *cancelled_items,
                            *chunk_count,
                            reducer_output.as_ref(),
                        ),
                        execution_state,
                        turn_context,
                    )?,
                    _ => return Ok(BulkTerminalBridgeAcceptance::IgnoredMissingRun),
                };

                runtime.instance.apply_event(&event);
                apply_compiled_plan(&mut runtime.instance, &plan);
                let scheduled_tasks = schedule_activities_from_plan(
                    &runtime.artifact,
                    &runtime.instance,
                    &plan,
                    event.occurred_at,
                )?;
                for task in &scheduled_tasks {
                    schedules.push(PreparedDbAction::Schedule(task.clone()));
                    runtime.active_activities.insert(
                        task.activity_id.clone(),
                        ActiveActivityMeta {
                            attempt: task.attempt,
                            task_queue: task.task_queue.clone(),
                            activity_type: task.activity_type.clone(),
                            activity_capabilities: task.activity_capabilities.clone(),
                            wait_state: task.state.clone(),
                            omit_success_output: task.omit_success_output,
                        },
                    );
                }
                let ready_tasks = scheduled_tasks;
                let final_instance = runtime.instance.clone();
                general.push(PreparedDbAction::UpsertInstance(runtime.instance.clone()));
                if let (Some(batch_id), Some(terminal_status)) =
                    (bulk_terminal_batch_id(&event.payload), bulk_terminal_status(&event.payload))
                {
                    general.push(PreparedDbAction::RecordBridgeTerminalAcceptance {
                        tenant_id: event.tenant_id.clone(),
                        instance_id: event.instance_id.clone(),
                        run_id: event.run_id.clone(),
                        batch_id: batch_id.to_owned(),
                        terminal_status: terminal_status.as_str().to_owned(),
                        terminal_event_id: event.event_id,
                        owner_epoch: bulk_terminal_owner_epoch(&event),
                        accepted_at: event.occurred_at,
                    });
                }
                let post_plan = PostPlanEffect {
                    run_key: run_key.clone(),
                    artifact: runtime.artifact.clone(),
                    instance: runtime.instance.clone(),
                    plan,
                    source_event_id: event.event_id,
                    occurred_at: event.occurred_at,
                };
                mark_runtime_dirty(&mut inner, "bulk_terminal", true);
                for task in &ready_tasks {
                    inner
                        .ready
                        .entry(QueueKey {
                            tenant_id: task.tenant_id.clone(),
                            task_queue: task.task_queue.clone(),
                        })
                        .or_default()
                        .push_back(task.clone());
                }
                Some(Ok((general, schedules, final_instance, post_plan)))
            }
        }
    };

    let Some(hot_result) = hot_result else {
        return classify_cold_bulk_terminal_via_bridge(state, &event).await;
    };
    let (general, schedules, final_instance, post_plan) = match hot_result {
        Ok(applied) => applied,
        Err(outcome) => return Ok(outcome),
    };

    apply_db_actions(state, general, schedules).await?;
    Box::pin(apply_post_plan_effects(state, post_plan)).await?;
    if final_instance.status.is_terminal() {
        maybe_enact_pending_workflow_cancellation_unified(
            state,
            &run_key,
            event.event_id,
            event.occurred_at,
        )
        .await?;
    }
    Ok(BulkTerminalBridgeAcceptance::Applied)
}

#[allow(dead_code)]
pub(super) async fn submit_stream_job_via_bridge(
    state: &AppState,
    request: &SubmitStreamJobRequest,
) -> Result<SubmitStreamJobResponse> {
    if let Some(existing) = state
        .store
        .get_stream_job_callback_handle(
            &request.identity.tenant_id,
            &request.identity.instance_id,
            &request.identity.run_id,
            &request.identity.job_id,
        )
        .await?
    {
        let status = existing.parsed_status().unwrap_or(StreamJobBridgeHandleStatus::Admitted);
        sync_stream_job_record(
            state,
            &existing,
            default_stream_job_status(&existing),
            None,
            existing.updated_at,
        )
        .await?;
        return Ok(SubmitStreamJobResponse { handle_id: existing.handle_id, status });
    }

    let handle = StreamJobBridgeHandleRecord {
        workflow_event_id: request.workflow_event_id,
        protocol_version: request.protocol_version.clone(),
        operation_kind: request.operation_kind.as_str().to_owned(),
        tenant_id: request.identity.tenant_id.clone(),
        instance_id: request.identity.instance_id.clone(),
        run_id: request.identity.run_id.clone(),
        stream_instance_id: request.identity.instance_id.clone(),
        stream_run_id: request.identity.run_id.clone(),
        job_id: request.identity.job_id.clone(),
        handle_id: request.handle_id.clone(),
        bridge_request_id: request.bridge_request_id.clone(),
        origin_kind: request
            .origin_kind
            .unwrap_or(StreamJobOriginKind::Workflow)
            .as_str()
            .to_owned(),
        definition_id: request.definition_id.clone(),
        definition_version: request.definition_version,
        artifact_hash: request.artifact_hash.clone(),
        job_name: request.job_name.clone(),
        input_ref: request.input_ref.clone(),
        config_ref: request.config_ref.clone(),
        checkpoint_policy: request.checkpoint_policy.clone(),
        view_definitions: request.view_definitions.clone(),
        status: StreamJobBridgeHandleStatus::Admitted.as_str().to_owned(),
        workflow_owner_epoch: request.workflow_owner_epoch,
        stream_owner_epoch: request.stream_owner_epoch,
        cancellation_requested_at: None,
        cancellation_reason: None,
        terminal_event_id: None,
        terminal_at: None,
        workflow_accepted_at: None,
        terminal_output: None,
        terminal_error: None,
        created_at: request.admitted_at,
        updated_at: request.admitted_at,
    };
    state.store.upsert_stream_job_callback_handle(&handle).await?;
    sync_stream_job_record(state, &handle, StreamJobStatus::Created, None, request.admitted_at)
        .await?;

    Ok(SubmitStreamJobResponse {
        handle_id: request.handle_id.clone(),
        status: StreamJobBridgeHandleStatus::Admitted,
    })
}

#[allow(dead_code)]
pub(super) async fn await_stream_job_checkpoint_via_bridge(
    state: &AppState,
    request: &AwaitStreamCheckpointRequest,
) -> Result<StreamJobCheckpointRecord> {
    if let Some(existing) =
        state.store.get_stream_job_callback_checkpoint(&request.await_request_id).await?
    {
        return Ok(existing);
    }

    let handle = state
        .store
        .get_stream_job_callback_handle_by_handle_id(&request.handle_id)
        .await?
        .with_context(|| format!("stream job handle {} was not admitted", request.handle_id))?;
    let handle_status = handle.parsed_status().unwrap_or(StreamJobBridgeHandleStatus::Admitted);
    let existing_checkpoint = state
        .store
        .list_stream_job_callback_checkpoints_for_handle_page(&handle.handle_id, 10_000, 0)
        .await?
        .into_iter()
        .find(|checkpoint| checkpoint.checkpoint_name == request.checkpoint_name);
    let status = if matches!(handle_status, StreamJobBridgeHandleStatus::Cancelled) {
        StreamJobCheckpointStatus::Cancelled
    } else if existing_checkpoint.as_ref().is_some_and(|checkpoint| {
        checkpoint.parsed_status().is_some_and(|status| {
            matches!(
                status,
                StreamJobCheckpointStatus::Reached | StreamJobCheckpointStatus::Accepted
            )
        })
    }) {
        StreamJobCheckpointStatus::Reached
    } else {
        StreamJobCheckpointStatus::Awaiting
    };
    let record = StreamJobCheckpointRecord {
        workflow_event_id: request.workflow_event_id,
        protocol_version: request.protocol_version.clone(),
        operation_kind: request.operation_kind.as_str().to_owned(),
        tenant_id: handle.tenant_id.clone(),
        instance_id: handle.instance_id.clone(),
        run_id: handle.run_id.clone(),
        job_id: handle.job_id.clone(),
        handle_id: handle.handle_id.clone(),
        bridge_request_id: handle.bridge_request_id.clone(),
        await_request_id: request.await_request_id.clone(),
        checkpoint_name: request.checkpoint_name.clone(),
        checkpoint_sequence: existing_checkpoint
            .as_ref()
            .and_then(|checkpoint| checkpoint.checkpoint_sequence),
        status: status.as_str().to_owned(),
        workflow_owner_epoch: request.workflow_owner_epoch.or(handle.workflow_owner_epoch),
        stream_owner_epoch: existing_checkpoint
            .as_ref()
            .and_then(|checkpoint| checkpoint.stream_owner_epoch)
            .or(handle.stream_owner_epoch),
        reached_at: existing_checkpoint.as_ref().and_then(|checkpoint| checkpoint.reached_at),
        output: existing_checkpoint.as_ref().and_then(|checkpoint| checkpoint.output.clone()),
        accepted_at: None,
        cancelled_at: if matches!(status, StreamJobCheckpointStatus::Cancelled) {
            Some(request.requested_at)
        } else {
            None
        },
        created_at: request.requested_at,
        updated_at: request.requested_at,
    };
    state.store.upsert_stream_job_callback_checkpoint(&record).await?;
    Ok(record)
}

#[allow(dead_code)]
pub(super) async fn query_stream_job_via_bridge(
    state: &AppState,
    request: &QueryStreamJobRequest,
) -> Result<StreamJobQueryRecord> {
    let handle = state
        .store
        .get_stream_job_callback_handle_by_handle_id(&request.handle_id)
        .await?
        .with_context(|| format!("stream job handle {} was not admitted", request.handle_id))?;
    let existing = state.store.get_stream_job_callback_query(&request.query_id).await?;
    let status = existing
        .as_ref()
        .and_then(StreamJobQueryRecord::parsed_status)
        .unwrap_or(StreamJobQueryStatus::Requested);
    let record = StreamJobQueryRecord {
        workflow_event_id: request.workflow_event_id,
        protocol_version: request.protocol_version.clone(),
        operation_kind: request.operation_kind.as_str().to_owned(),
        tenant_id: handle.tenant_id.clone(),
        instance_id: handle.instance_id.clone(),
        run_id: handle.run_id.clone(),
        job_id: handle.job_id.clone(),
        handle_id: handle.handle_id.clone(),
        bridge_request_id: request.bridge_request_id.clone(),
        query_id: request.query_id.clone(),
        query_name: request.query_name.clone(),
        query_args: request.args.clone(),
        consistency: request.consistency.as_str().to_owned(),
        status: status.as_str().to_owned(),
        workflow_owner_epoch: request.workflow_owner_epoch.or(handle.workflow_owner_epoch),
        stream_owner_epoch: existing.as_ref().and_then(|query| query.stream_owner_epoch),
        output: existing.as_ref().and_then(|query| query.output.clone()),
        error: existing.as_ref().and_then(|query| query.error.clone()),
        requested_at: existing
            .as_ref()
            .map(|query| query.requested_at)
            .unwrap_or(request.requested_at),
        completed_at: existing.as_ref().and_then(|query| query.completed_at),
        accepted_at: existing.as_ref().and_then(|query| query.accepted_at),
        cancelled_at: existing.as_ref().and_then(|query| query.cancelled_at),
        created_at: existing.as_ref().map(|query| query.created_at).unwrap_or(request.requested_at),
        updated_at: request.requested_at,
    };
    state.store.upsert_stream_job_callback_query(&record).await?;
    Ok(record)
}

#[allow(dead_code)]
pub(super) async fn cancel_stream_job_via_bridge(
    state: &AppState,
    request: &CancelStreamJobRequest,
) -> Result<StreamJobBridgeHandleRecord> {
    let mut handle = state
        .store
        .get_stream_job_callback_handle_by_handle_id(&request.handle_id)
        .await?
        .with_context(|| format!("stream job handle {} was not admitted", request.handle_id))?;
    let parsed_status = handle.parsed_status().unwrap_or(StreamJobBridgeHandleStatus::Admitted);
    if !parsed_status.is_terminal() {
        handle.status = StreamJobBridgeHandleStatus::CancellationRequested.as_str().to_owned();
        handle.workflow_owner_epoch = request.workflow_owner_epoch.or(handle.workflow_owner_epoch);
        handle.cancellation_requested_at = Some(request.requested_at);
        handle.cancellation_reason = request.reason.clone();
        handle.updated_at = request.requested_at;
        state.store.upsert_stream_job_callback_handle(&handle).await?;
        sync_stream_job_record(
            state,
            &handle,
            StreamJobStatus::Draining,
            None,
            request.requested_at,
        )
        .await?;

        let checkpoints = state
            .store
            .list_stream_job_callback_checkpoints_for_handle_page(&handle.handle_id, 10_000, 0)
            .await?;
        for mut checkpoint in checkpoints {
            let checkpoint_status =
                checkpoint.parsed_status().unwrap_or(StreamJobCheckpointStatus::Awaiting);
            if matches!(
                checkpoint_status,
                StreamJobCheckpointStatus::Accepted | StreamJobCheckpointStatus::Cancelled
            ) {
                continue;
            }
            checkpoint.status = StreamJobCheckpointStatus::Cancelled.as_str().to_owned();
            checkpoint.cancelled_at = Some(request.requested_at);
            checkpoint.updated_at = request.requested_at;
            state.store.upsert_stream_job_callback_checkpoint(&checkpoint).await?;
        }
    }
    Ok(handle)
}

#[allow(dead_code)]
pub(super) async fn await_stream_job_terminal_via_bridge(
    state: &AppState,
    request: &AwaitStreamJobTerminalRequest,
) -> Result<StreamJobBridgeHandleRecord> {
    let mut handle = state
        .store
        .get_stream_job_callback_handle_by_handle_id(&request.handle_id)
        .await?
        .with_context(|| format!("stream job handle {} was not admitted", request.handle_id))?;
    let mut changed = false;
    if request.workflow_owner_epoch.is_some() && handle.workflow_owner_epoch.is_none() {
        handle.workflow_owner_epoch = request.workflow_owner_epoch;
        changed = true;
    }
    if handle.workflow_accepted_at.is_none() {
        handle.workflow_accepted_at = Some(request.requested_at);
        changed = true;
    }
    if changed {
        handle.updated_at = request.requested_at;
        state.store.upsert_stream_job_callback_handle(&handle).await?;
    }
    Ok(handle)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum StreamJobBridgeAcceptance {
    Applied,
    PersistedOnly,
    IgnoredMissingRun,
    IgnoredStaleOwnerEpoch,
    IgnoredClosedRun,
}

fn stream_job_owner_epoch_is_stale(
    handle: &StreamJobBridgeHandleRecord,
    callback_owner_epoch: Option<u64>,
) -> bool {
    match (handle.stream_owner_epoch, callback_owner_epoch) {
        (Some(current), Some(callback)) => callback < current,
        _ => false,
    }
}

fn stream_job_query_already_accepted(query: &StreamJobQueryRecord) -> bool {
    query.accepted_at.is_some()
        && query.parsed_status().is_some_and(|status| status == StreamJobQueryStatus::Accepted)
}

fn stream_job_checkpoint_already_accepted(checkpoint: &StreamJobCheckpointRecord) -> bool {
    checkpoint.accepted_at.is_some()
        && checkpoint
            .parsed_status()
            .is_some_and(|status| status == StreamJobCheckpointStatus::Accepted)
}

fn stream_job_terminal_already_accepted(handle: &StreamJobBridgeHandleRecord) -> bool {
    handle.terminal_event_id.is_some()
        && handle.parsed_status().is_some_and(StreamJobBridgeHandleStatus::is_terminal)
}

pub(super) async fn accept_stream_job_query_via_bridge(
    state: &AppState,
    event: EventEnvelope<WorkflowEvent>,
) -> Result<StreamJobBridgeAcceptance> {
    let (job_id, handle_id, query_id, query_name, query_output, error, stream_owner_epoch) =
        match &event.payload {
            WorkflowEvent::StreamJobQueryCompleted {
                job_id,
                handle_id,
                query_id,
                query_name,
                output,
                stream_owner_epoch,
            } => (
                job_id.clone(),
                handle_id.clone(),
                query_id.clone(),
                query_name.clone(),
                Some(output.clone()),
                None,
                *stream_owner_epoch,
            ),
            WorkflowEvent::StreamJobQueryFailed {
                job_id,
                handle_id,
                query_id,
                query_name,
                error,
                stream_owner_epoch,
            } => (
                job_id.clone(),
                handle_id.clone(),
                query_id.clone(),
                query_name.clone(),
                None,
                Some(error.clone()),
                *stream_owner_epoch,
            ),
            _ => return Ok(StreamJobBridgeAcceptance::IgnoredMissingRun),
        };
    let Some(mut handle) = state
        .store
        .get_stream_job_callback_handle(
            &event.tenant_id,
            &event.instance_id,
            &event.run_id,
            &job_id,
        )
        .await?
    else {
        return Ok(StreamJobBridgeAcceptance::IgnoredMissingRun);
    };
    if stream_job_owner_epoch_is_stale(&handle, stream_owner_epoch) {
        return Ok(StreamJobBridgeAcceptance::IgnoredStaleOwnerEpoch);
    }
    handle.stream_owner_epoch = stream_owner_epoch;
    handle.updated_at = event.occurred_at;
    state.store.upsert_stream_job_callback_handle(&handle).await?;

    let Some(mut query_record) = state.store.get_stream_job_callback_query(&query_id).await? else {
        return Ok(StreamJobBridgeAcceptance::IgnoredMissingRun);
    };
    if stream_job_query_already_accepted(&query_record) {
        return Ok(StreamJobBridgeAcceptance::PersistedOnly);
    }
    query_record.handle_id = handle_id;
    query_record.stream_owner_epoch = stream_owner_epoch;
    query_record.output = query_output.clone();
    query_record.error = error.clone();
    query_record.completed_at = Some(event.occurred_at);
    query_record.status = if error.is_some() {
        StreamJobQueryStatus::Failed.as_str().to_owned()
    } else {
        StreamJobQueryStatus::Completed.as_str().to_owned()
    };
    query_record.updated_at = event.occurred_at;
    state.store.upsert_stream_job_callback_query(&query_record).await?;

    let run_key = RunKey {
        tenant_id: event.tenant_id.clone(),
        instance_id: event.instance_id.clone(),
        run_id: event.run_id.clone(),
    };
    let hot_result = {
        let mut inner = state.inner.lock().expect("unified runtime lock poisoned");
        match inner.instances.get_mut(&run_key) {
            None => None,
            Some(runtime) if runtime.instance.status.is_terminal() => {
                Some(Err(StreamJobBridgeAcceptance::IgnoredClosedRun))
            }
            Some(runtime) => {
                let current_state = runtime
                    .instance
                    .current_state
                    .clone()
                    .unwrap_or_else(|| runtime.artifact.workflow.initial_state.clone());
                if !matches!(
                    runtime.artifact.workflow.states.get(&current_state),
                    Some(fabrik_workflow::CompiledStateNode::QueryStreamJob { .. })
                ) {
                    Some(Err(StreamJobBridgeAcceptance::PersistedOnly))
                } else {
                    let plan = runtime.artifact.execute_after_stream_query_with_turn(
                        &current_state,
                        &job_id,
                        &query_id,
                        &query_name,
                        query_output.as_ref(),
                        error.as_deref(),
                        execution_state_for_event(&runtime.instance, Some(&event)),
                        ExecutionTurnContext {
                            event_id: event.event_id,
                            occurred_at: event.occurred_at,
                        },
                    )?;
                    runtime.instance.apply_event(&event);
                    apply_compiled_plan(&mut runtime.instance, &plan);
                    let scheduled_tasks = schedule_activities_from_plan(
                        &runtime.artifact,
                        &runtime.instance,
                        &plan,
                        event.occurred_at,
                    )?;
                    let artifact = runtime.artifact.clone();
                    let final_instance = runtime.instance.clone();
                    let mut schedules = Vec::new();
                    let mut notifies = false;
                    for task in &scheduled_tasks {
                        schedules.push(PreparedDbAction::Schedule(task.clone()));
                        runtime.active_activities.insert(
                            task.activity_id.clone(),
                            ActiveActivityMeta {
                                attempt: task.attempt,
                                task_queue: task.task_queue.clone(),
                                activity_type: task.activity_type.clone(),
                                activity_capabilities: task.activity_capabilities.clone(),
                                wait_state: task.state.clone(),
                                omit_success_output: task.omit_success_output,
                            },
                        );
                    }
                    let _ = runtime;
                    for task in scheduled_tasks {
                        inner
                            .ready
                            .entry(QueueKey {
                                tenant_id: task.tenant_id.clone(),
                                task_queue: task.task_queue.clone(),
                            })
                            .or_default()
                            .push_back(task);
                        notifies = true;
                    }
                    Some(Ok((artifact, final_instance, plan, schedules, notifies)))
                }
            }
        }
    };
    let Some(hot_result) = hot_result else {
        return Ok(StreamJobBridgeAcceptance::PersistedOnly);
    };
    let (artifact, instance, plan, schedules, notifies) = match hot_result {
        Ok(applied) => applied,
        Err(outcome) => return Ok(outcome),
    };
    let general = vec![PreparedDbAction::UpsertInstance(instance.clone())];
    apply_db_actions(state, general, schedules).await?;
    Box::pin(apply_post_plan_effects(
        state,
        PostPlanEffect {
            run_key,
            artifact,
            instance,
            plan,
            source_event_id: event.event_id,
            occurred_at: event.occurred_at,
        },
    ))
    .await?;
    query_record.status = StreamJobQueryStatus::Accepted.as_str().to_owned();
    query_record.accepted_at = Some(event.occurred_at);
    query_record.updated_at = event.occurred_at;
    state.store.upsert_stream_job_callback_query(&query_record).await?;
    if notifies {
        state.notify.notify_waiters();
    }
    Ok(StreamJobBridgeAcceptance::Applied)
}

pub(super) fn stream_job_query_callback_event(
    handle: &StreamJobBridgeHandleRecord,
    query: &StreamJobQueryRecord,
) -> Option<EventEnvelope<WorkflowEvent>> {
    let status = query.parsed_status()?;
    let payload = match status {
        StreamJobQueryStatus::Completed => WorkflowEvent::StreamJobQueryCompleted {
            job_id: handle.job_id.clone(),
            handle_id: handle.handle_id.clone(),
            query_id: query.query_id.clone(),
            query_name: query.query_name.clone(),
            output: query.output.clone().unwrap_or(Value::Null),
            stream_owner_epoch: query.stream_owner_epoch.or(handle.stream_owner_epoch),
        },
        StreamJobQueryStatus::Failed => WorkflowEvent::StreamJobQueryFailed {
            job_id: handle.job_id.clone(),
            handle_id: handle.handle_id.clone(),
            query_id: query.query_id.clone(),
            query_name: query.query_name.clone(),
            error: query.error.clone().unwrap_or_else(|| "stream job query failed".to_owned()),
            stream_owner_epoch: query.stream_owner_epoch.or(handle.stream_owner_epoch),
        },
        _ => return None,
    };
    let identity = WorkflowIdentity::new(
        handle.tenant_id.clone(),
        handle.definition_id.clone(),
        handle.definition_version.unwrap_or_default(),
        handle.artifact_hash.clone().unwrap_or_default(),
        handle.instance_id.clone(),
        handle.run_id.clone(),
        "unified-runtime",
    );
    let dedupe_key = stream_job_query_callback_dedupe_key(
        &handle.tenant_id,
        &handle.instance_id,
        &handle.run_id,
        &handle.job_id,
        &query.query_id,
        status,
    );
    let mut envelope = EventEnvelope::new(payload.event_type(), identity, payload);
    envelope.event_id = stream_job_callback_event_id(&dedupe_key);
    envelope.occurred_at = query.completed_at.unwrap_or(query.updated_at);
    envelope.dedupe_key = Some(dedupe_key);
    envelope.metadata.insert("bridge_request_id".to_owned(), query.bridge_request_id.clone());
    envelope.metadata.insert(
        "bridge_protocol_version".to_owned(),
        if query.protocol_version.trim().is_empty() {
            THROUGHPUT_BRIDGE_PROTOCOL_VERSION.to_owned()
        } else {
            query.protocol_version.clone()
        },
    );
    envelope.metadata.insert(
        "bridge_operation_kind".to_owned(),
        query
            .parsed_operation_kind()
            .unwrap_or(ThroughputBridgeOperationKind::StreamJob)
            .as_str()
            .to_owned(),
    );
    if let Some(owner_epoch) = query.stream_owner_epoch.or(handle.stream_owner_epoch) {
        envelope.metadata.insert("bridge_owner_epoch".to_owned(), owner_epoch.to_string());
    }
    Some(envelope)
}

pub(super) fn stream_job_checkpoint_callback_event(
    handle: &StreamJobBridgeHandleRecord,
    checkpoint: &StreamJobCheckpointRecord,
) -> Option<EventEnvelope<WorkflowEvent>> {
    let status = checkpoint.parsed_status()?;
    if status != StreamJobCheckpointStatus::Reached {
        return None;
    }
    let payload = WorkflowEvent::StreamJobCheckpointReached {
        job_id: handle.job_id.clone(),
        handle_id: handle.handle_id.clone(),
        checkpoint_name: checkpoint.checkpoint_name.clone(),
        checkpoint_sequence: checkpoint.checkpoint_sequence.unwrap_or_default(),
        output: checkpoint.output.clone(),
        stream_owner_epoch: checkpoint.stream_owner_epoch.or(handle.stream_owner_epoch),
    };
    let identity = WorkflowIdentity::new(
        handle.tenant_id.clone(),
        handle.definition_id.clone(),
        handle.definition_version.unwrap_or_default(),
        handle.artifact_hash.clone().unwrap_or_default(),
        handle.instance_id.clone(),
        handle.run_id.clone(),
        "unified-runtime",
    );
    let dedupe_key = stream_job_checkpoint_callback_dedupe_key(
        &handle.tenant_id,
        &handle.instance_id,
        &handle.run_id,
        &handle.job_id,
        &checkpoint.checkpoint_name,
        checkpoint.checkpoint_sequence.unwrap_or_default(),
    );
    let mut envelope = EventEnvelope::new(payload.event_type(), identity, payload);
    envelope.event_id = stream_job_callback_event_id(&dedupe_key);
    envelope.occurred_at = checkpoint.reached_at.unwrap_or(checkpoint.updated_at);
    envelope.dedupe_key = Some(dedupe_key);
    envelope.metadata.insert("bridge_request_id".to_owned(), checkpoint.bridge_request_id.clone());
    envelope.metadata.insert(
        "bridge_protocol_version".to_owned(),
        if checkpoint.protocol_version.trim().is_empty() {
            THROUGHPUT_BRIDGE_PROTOCOL_VERSION.to_owned()
        } else {
            checkpoint.protocol_version.clone()
        },
    );
    envelope.metadata.insert(
        "bridge_operation_kind".to_owned(),
        checkpoint
            .parsed_operation_kind()
            .unwrap_or(ThroughputBridgeOperationKind::StreamJob)
            .as_str()
            .to_owned(),
    );
    if let Some(owner_epoch) = checkpoint.stream_owner_epoch.or(handle.stream_owner_epoch) {
        envelope.metadata.insert("bridge_owner_epoch".to_owned(), owner_epoch.to_string());
    }
    Some(envelope)
}

pub(super) fn stream_job_terminal_callback_event(
    handle: &StreamJobBridgeHandleRecord,
) -> Option<EventEnvelope<WorkflowEvent>> {
    let status = handle.parsed_status()?;
    let payload = match status {
        StreamJobBridgeHandleStatus::Completed => WorkflowEvent::StreamJobCompleted {
            job_id: handle.job_id.clone(),
            handle_id: handle.handle_id.clone(),
            output: handle.terminal_output.clone().unwrap_or(Value::Null),
            stream_owner_epoch: handle.stream_owner_epoch,
        },
        StreamJobBridgeHandleStatus::Failed => WorkflowEvent::StreamJobFailed {
            job_id: handle.job_id.clone(),
            handle_id: handle.handle_id.clone(),
            error: handle.terminal_error.clone().unwrap_or_else(|| "stream job failed".to_owned()),
            stream_owner_epoch: handle.stream_owner_epoch,
        },
        StreamJobBridgeHandleStatus::Cancelled => WorkflowEvent::StreamJobCancelled {
            job_id: handle.job_id.clone(),
            handle_id: handle.handle_id.clone(),
            reason: handle
                .terminal_error
                .clone()
                .or(handle.cancellation_reason.clone())
                .unwrap_or_else(|| "stream job cancelled".to_owned()),
            stream_owner_epoch: handle.stream_owner_epoch,
        },
        _ => return None,
    };
    let identity = WorkflowIdentity::new(
        handle.tenant_id.clone(),
        handle.definition_id.clone(),
        handle.definition_version.unwrap_or_default(),
        handle.artifact_hash.clone().unwrap_or_default(),
        handle.instance_id.clone(),
        handle.run_id.clone(),
        "unified-runtime",
    );
    let dedupe_key = stream_job_terminal_callback_dedupe_key(
        &handle.tenant_id,
        &handle.instance_id,
        &handle.run_id,
        &handle.job_id,
        status,
    );
    let mut envelope = EventEnvelope::new(payload.event_type(), identity, payload);
    envelope.event_id = stream_job_callback_event_id(&dedupe_key);
    envelope.occurred_at = handle.terminal_at.unwrap_or(handle.updated_at);
    envelope.dedupe_key = Some(dedupe_key);
    envelope.metadata.insert("bridge_request_id".to_owned(), handle.bridge_request_id.clone());
    envelope.metadata.insert(
        "bridge_protocol_version".to_owned(),
        if handle.protocol_version.trim().is_empty() {
            THROUGHPUT_BRIDGE_PROTOCOL_VERSION.to_owned()
        } else {
            handle.protocol_version.clone()
        },
    );
    envelope.metadata.insert(
        "bridge_operation_kind".to_owned(),
        handle
            .parsed_operation_kind()
            .unwrap_or(ThroughputBridgeOperationKind::StreamJob)
            .as_str()
            .to_owned(),
    );
    if let Some(owner_epoch) = handle.stream_owner_epoch {
        envelope.metadata.insert("bridge_owner_epoch".to_owned(), owner_epoch.to_string());
    }
    Some(envelope)
}

pub(super) async fn accept_stream_job_checkpoint_via_bridge(
    state: &AppState,
    event: EventEnvelope<WorkflowEvent>,
) -> Result<StreamJobBridgeAcceptance> {
    let WorkflowEvent::StreamJobCheckpointReached {
        job_id,
        handle_id,
        checkpoint_name,
        checkpoint_sequence,
        output,
        stream_owner_epoch,
    } = &event.payload
    else {
        return Ok(StreamJobBridgeAcceptance::IgnoredMissingRun);
    };
    let Some(mut handle) = state
        .store
        .get_stream_job_callback_handle(&event.tenant_id, &event.instance_id, &event.run_id, job_id)
        .await?
    else {
        return Ok(StreamJobBridgeAcceptance::IgnoredMissingRun);
    };
    if stream_job_owner_epoch_is_stale(&handle, *stream_owner_epoch) {
        return Ok(StreamJobBridgeAcceptance::IgnoredStaleOwnerEpoch);
    }
    if !handle.parsed_status().is_some_and(StreamJobBridgeHandleStatus::is_terminal) {
        handle.status = StreamJobBridgeHandleStatus::Running.as_str().to_owned();
    }
    handle.stream_owner_epoch = *stream_owner_epoch;
    handle.updated_at = event.occurred_at;
    state.store.upsert_stream_job_callback_handle(&handle).await?;

    let existing = state
        .store
        .list_stream_job_callback_checkpoints_for_handle_page(&handle.handle_id, 10_000, 0)
        .await?
        .into_iter()
        .find(|checkpoint| checkpoint.checkpoint_name == *checkpoint_name);
    if existing.as_ref().is_some_and(stream_job_checkpoint_already_accepted) {
        return Ok(StreamJobBridgeAcceptance::PersistedOnly);
    }
    let mut checkpoint = existing.unwrap_or(StreamJobCheckpointRecord {
        workflow_event_id: event.event_id,
        protocol_version: THROUGHPUT_BRIDGE_PROTOCOL_VERSION.to_owned(),
        operation_kind: ThroughputBridgeOperationKind::StreamJob.as_str().to_owned(),
        tenant_id: event.tenant_id.clone(),
        instance_id: event.instance_id.clone(),
        run_id: event.run_id.clone(),
        job_id: job_id.clone(),
        handle_id: handle_id.clone(),
        bridge_request_id: handle.bridge_request_id.clone(),
        await_request_id: format!(
            "stream-job-reached:{handle_id}:{checkpoint_name}:{checkpoint_sequence}"
        ),
        checkpoint_name: checkpoint_name.clone(),
        checkpoint_sequence: None,
        status: StreamJobCheckpointStatus::Awaiting.as_str().to_owned(),
        workflow_owner_epoch: handle.workflow_owner_epoch,
        stream_owner_epoch: None,
        reached_at: None,
        output: None,
        accepted_at: None,
        cancelled_at: None,
        created_at: event.occurred_at,
        updated_at: event.occurred_at,
    });
    checkpoint.status = StreamJobCheckpointStatus::Reached.as_str().to_owned();
    checkpoint.checkpoint_sequence = Some(*checkpoint_sequence);
    checkpoint.stream_owner_epoch = *stream_owner_epoch;
    checkpoint.reached_at = Some(event.occurred_at);
    checkpoint.output = output.clone();
    checkpoint.updated_at = event.occurred_at;
    state.store.upsert_stream_job_callback_checkpoint(&checkpoint).await?;
    sync_stream_job_record(
        state,
        &handle,
        default_stream_job_status(&handle),
        Some(&checkpoint),
        event.occurred_at,
    )
    .await?;

    let run_key = RunKey {
        tenant_id: event.tenant_id.clone(),
        instance_id: event.instance_id.clone(),
        run_id: event.run_id.clone(),
    };
    let hot_result = {
        let mut inner = state.inner.lock().expect("unified runtime lock poisoned");
        match inner.instances.get_mut(&run_key) {
            None => None,
            Some(runtime) if runtime.instance.status.is_terminal() => {
                Some(Err(StreamJobBridgeAcceptance::IgnoredClosedRun))
            }
            Some(runtime) => {
                let current_state = runtime
                    .instance
                    .current_state
                    .clone()
                    .unwrap_or_else(|| runtime.artifact.workflow.initial_state.clone());
                if !runtime.artifact.waits_on_stream_job(
                    &current_state,
                    &execution_state_for_event(&runtime.instance, Some(&event)),
                ) {
                    Some(Err(StreamJobBridgeAcceptance::PersistedOnly))
                } else {
                    let plan = runtime.artifact.execute_after_stream_checkpoint_with_turn(
                        &current_state,
                        job_id,
                        checkpoint_name,
                        *checkpoint_sequence,
                        output.as_ref(),
                        execution_state_for_event(&runtime.instance, Some(&event)),
                        ExecutionTurnContext {
                            event_id: event.event_id,
                            occurred_at: event.occurred_at,
                        },
                    )?;
                    runtime.instance.apply_event(&event);
                    apply_compiled_plan(&mut runtime.instance, &plan);
                    let scheduled_tasks = schedule_activities_from_plan(
                        &runtime.artifact,
                        &runtime.instance,
                        &plan,
                        event.occurred_at,
                    )?;
                    let artifact = runtime.artifact.clone();
                    let final_instance = runtime.instance.clone();
                    let mut schedules = Vec::new();
                    let mut notifies = false;
                    for task in &scheduled_tasks {
                        schedules.push(PreparedDbAction::Schedule(task.clone()));
                        runtime.active_activities.insert(
                            task.activity_id.clone(),
                            ActiveActivityMeta {
                                attempt: task.attempt,
                                task_queue: task.task_queue.clone(),
                                activity_type: task.activity_type.clone(),
                                activity_capabilities: task.activity_capabilities.clone(),
                                wait_state: task.state.clone(),
                                omit_success_output: task.omit_success_output,
                            },
                        );
                    }
                    let _ = runtime;
                    for task in scheduled_tasks {
                        inner
                            .ready
                            .entry(QueueKey {
                                tenant_id: task.tenant_id.clone(),
                                task_queue: task.task_queue.clone(),
                            })
                            .or_default()
                            .push_back(task);
                        notifies = true;
                    }
                    Some(Ok((artifact, final_instance, plan, schedules, notifies)))
                }
            }
        }
    };
    let Some(hot_result) = hot_result else {
        return Ok(StreamJobBridgeAcceptance::PersistedOnly);
    };
    let (artifact, instance, plan, schedules, notifies) = match hot_result {
        Ok(applied) => applied,
        Err(outcome) => return Ok(outcome),
    };
    let general = vec![PreparedDbAction::UpsertInstance(instance.clone())];
    apply_db_actions(state, general, schedules).await?;
    Box::pin(apply_post_plan_effects(
        state,
        PostPlanEffect {
            run_key,
            artifact,
            instance,
            plan,
            source_event_id: event.event_id,
            occurred_at: event.occurred_at,
        },
    ))
    .await?;
    checkpoint.status = StreamJobCheckpointStatus::Accepted.as_str().to_owned();
    checkpoint.accepted_at = Some(event.occurred_at);
    checkpoint.updated_at = event.occurred_at;
    state.store.upsert_stream_job_callback_checkpoint(&checkpoint).await?;
    if notifies {
        state.notify.notify_waiters();
    }
    Ok(StreamJobBridgeAcceptance::Applied)
}

pub(super) async fn accept_stream_job_terminal_via_bridge(
    state: &AppState,
    event: EventEnvelope<WorkflowEvent>,
) -> Result<StreamJobBridgeAcceptance> {
    let (
        job_id,
        _handle_id,
        terminal_status,
        terminal_output,
        terminal_error,
        workflow_error,
        stream_owner_epoch,
    ): (
        String,
        String,
        StreamJobBridgeHandleStatus,
        Option<Value>,
        Option<String>,
        Option<String>,
        Option<u64>,
    ) = match &event.payload {
        WorkflowEvent::StreamJobCompleted { job_id, handle_id, output, stream_owner_epoch } => (
            job_id.clone(),
            handle_id.clone(),
            StreamJobBridgeHandleStatus::Completed,
            Some(output.clone()),
            None,
            None,
            *stream_owner_epoch,
        ),
        WorkflowEvent::StreamJobFailed { job_id, handle_id, error, stream_owner_epoch } => (
            job_id.clone(),
            handle_id.clone(),
            StreamJobBridgeHandleStatus::Failed,
            None,
            Some(error.clone()),
            Some(error.clone()),
            *stream_owner_epoch,
        ),
        WorkflowEvent::StreamJobCancelled { job_id, handle_id, reason, stream_owner_epoch } => (
            job_id.clone(),
            handle_id.clone(),
            StreamJobBridgeHandleStatus::Cancelled,
            Some(json!({
                "jobId": job_id,
                "status": "cancelled",
                "reason": reason,
            })),
            Some(reason.clone()),
            None,
            *stream_owner_epoch,
        ),
        _ => return Ok(StreamJobBridgeAcceptance::IgnoredMissingRun),
    };
    let Some(mut handle) = state
        .store
        .get_stream_job_callback_handle(
            &event.tenant_id,
            &event.instance_id,
            &event.run_id,
            &job_id,
        )
        .await?
    else {
        return Ok(StreamJobBridgeAcceptance::IgnoredMissingRun);
    };
    if stream_job_owner_epoch_is_stale(&handle, stream_owner_epoch) {
        return Ok(StreamJobBridgeAcceptance::IgnoredStaleOwnerEpoch);
    }
    if stream_job_terminal_already_accepted(&handle) {
        return Ok(StreamJobBridgeAcceptance::PersistedOnly);
    }
    handle.status = terminal_status.as_str().to_owned();
    handle.stream_owner_epoch = stream_owner_epoch;
    handle.terminal_event_id = Some(event.event_id);
    handle.terminal_at = Some(event.occurred_at);
    handle.terminal_output = terminal_output.clone();
    handle.terminal_error = terminal_error.clone();
    handle.updated_at = event.occurred_at;
    state.store.upsert_stream_job_callback_handle(&handle).await?;
    for mut checkpoint in state
        .store
        .list_stream_job_callback_checkpoints_for_handle_page(&handle.handle_id, 10_000, 0)
        .await?
    {
        if checkpoint.parsed_status().is_some_and(|status| {
            matches!(
                status,
                StreamJobCheckpointStatus::Accepted | StreamJobCheckpointStatus::Cancelled
            )
        }) {
            continue;
        }
        checkpoint.status = StreamJobCheckpointStatus::Cancelled.as_str().to_owned();
        checkpoint.cancelled_at = Some(event.occurred_at);
        checkpoint.stream_owner_epoch = stream_owner_epoch;
        checkpoint.updated_at = event.occurred_at;
        state.store.upsert_stream_job_callback_checkpoint(&checkpoint).await?;
    }
    sync_stream_job_record(
        state,
        &handle,
        match terminal_status {
            StreamJobBridgeHandleStatus::Completed => StreamJobStatus::Completed,
            StreamJobBridgeHandleStatus::Failed => StreamJobStatus::Failed,
            StreamJobBridgeHandleStatus::Cancelled => StreamJobStatus::Cancelled,
            StreamJobBridgeHandleStatus::Admitted
            | StreamJobBridgeHandleStatus::Running
            | StreamJobBridgeHandleStatus::Paused
            | StreamJobBridgeHandleStatus::CancellationRequested => StreamJobStatus::Running,
        },
        None,
        event.occurred_at,
    )
    .await?;

    let run_key = RunKey {
        tenant_id: event.tenant_id.clone(),
        instance_id: event.instance_id.clone(),
        run_id: event.run_id.clone(),
    };
    let hot_result = {
        let mut inner = state.inner.lock().expect("unified runtime lock poisoned");
        match inner.instances.get_mut(&run_key) {
            None => None,
            Some(runtime) if runtime.instance.status.is_terminal() => {
                Some(Err(StreamJobBridgeAcceptance::IgnoredClosedRun))
            }
            Some(runtime) => {
                let current_state = runtime
                    .instance
                    .current_state
                    .clone()
                    .unwrap_or_else(|| runtime.artifact.workflow.initial_state.clone());
                if !runtime.artifact.waits_on_stream_job(
                    &current_state,
                    &execution_state_for_event(&runtime.instance, Some(&event)),
                ) {
                    Some(Err(StreamJobBridgeAcceptance::PersistedOnly))
                } else {
                    let plan = runtime.artifact.execute_after_stream_terminal_with_turn(
                        &current_state,
                        &job_id,
                        terminal_output.as_ref(),
                        workflow_error.as_deref(),
                        execution_state_for_event(&runtime.instance, Some(&event)),
                        ExecutionTurnContext {
                            event_id: event.event_id,
                            occurred_at: event.occurred_at,
                        },
                    )?;
                    runtime.instance.apply_event(&event);
                    apply_compiled_plan(&mut runtime.instance, &plan);
                    let scheduled_tasks = schedule_activities_from_plan(
                        &runtime.artifact,
                        &runtime.instance,
                        &plan,
                        event.occurred_at,
                    )?;
                    let artifact = runtime.artifact.clone();
                    let final_instance = runtime.instance.clone();
                    let mut schedules = Vec::new();
                    let mut notifies = false;
                    for task in &scheduled_tasks {
                        schedules.push(PreparedDbAction::Schedule(task.clone()));
                        runtime.active_activities.insert(
                            task.activity_id.clone(),
                            ActiveActivityMeta {
                                attempt: task.attempt,
                                task_queue: task.task_queue.clone(),
                                activity_type: task.activity_type.clone(),
                                activity_capabilities: task.activity_capabilities.clone(),
                                wait_state: task.state.clone(),
                                omit_success_output: task.omit_success_output,
                            },
                        );
                    }
                    let _ = runtime;
                    for task in scheduled_tasks {
                        inner
                            .ready
                            .entry(QueueKey {
                                tenant_id: task.tenant_id.clone(),
                                task_queue: task.task_queue.clone(),
                            })
                            .or_default()
                            .push_back(task);
                        notifies = true;
                    }
                    Some(Ok((artifact, final_instance, plan, schedules, notifies)))
                }
            }
        }
    };
    let Some(hot_result) = hot_result else {
        return Ok(StreamJobBridgeAcceptance::PersistedOnly);
    };
    let (artifact, instance, plan, schedules, notifies) = match hot_result {
        Ok(applied) => applied,
        Err(outcome) => return Ok(outcome),
    };
    let general = vec![PreparedDbAction::UpsertInstance(instance.clone())];
    apply_db_actions(state, general, schedules).await?;
    Box::pin(apply_post_plan_effects(
        state,
        PostPlanEffect {
            run_key,
            artifact,
            instance,
            plan,
            source_event_id: event.event_id,
            occurred_at: event.occurred_at,
        },
    ))
    .await?;
    handle.workflow_accepted_at = Some(event.occurred_at);
    handle.updated_at = event.occurred_at;
    state.store.upsert_stream_job_callback_handle(&handle).await?;
    sync_stream_job_record(
        state,
        &handle,
        default_stream_job_status(&handle),
        None,
        event.occurred_at,
    )
    .await?;
    if notifies {
        state.notify.notify_waiters();
    }
    Ok(StreamJobBridgeAcceptance::Applied)
}

pub(super) async fn start_throughput_run_command(
    instance: &WorkflowInstanceState,
    batch_id: &str,
    activity_type: &str,
    activity_capabilities: Option<ActivityExecutionCapabilities>,
    task_queue: &str,
    total_items: usize,
    result_handle: &Value,
    workflow_state: Option<String>,
    chunk_size: u32,
    max_attempts: u32,
    retry_delay_ms: u64,
    aggregation_group_count: u32,
    execution_policy: Option<String>,
    reducer: Option<String>,
    throughput_backend: &str,
    throughput_backend_version: &str,
    admission: &BulkAdmissionDecision,
    input_handle: PayloadHandle,
    source_event_id: Uuid,
    occurred_at: DateTime<Utc>,
) -> Result<ThroughputCommandEnvelope> {
    let result_handle: PayloadHandle = serde_json::from_value(result_handle.clone())
        .context("failed to deserialize bulk result handle for throughput command")?;
    let dedupe_key = throughput_start_dedupe_key(source_event_id, batch_id);
    Ok(ThroughputCommandEnvelope {
        command_id: throughput_start_command_id(source_event_id, batch_id),
        occurred_at,
        dedupe_key: dedupe_key.clone(),
        partition_key: throughput_partition_key(batch_id, 0),
        payload: ThroughputCommand::StartThroughputRun(StartThroughputRunCommand {
            dedupe_key,
            bridge_request_id: throughput_bridge_request_id(
                &instance.tenant_id,
                &instance.instance_id,
                &instance.run_id,
                batch_id,
            ),
            bridge_protocol_version: THROUGHPUT_BRIDGE_PROTOCOL_VERSION.to_owned(),
            bridge_operation_kind: ThroughputBridgeOperationKind::BulkRun.as_str().to_owned(),
            tenant_id: instance.tenant_id.clone(),
            definition_id: instance.definition_id.clone(),
            definition_version: instance.definition_version,
            artifact_hash: instance.artifact_hash.clone(),
            instance_id: instance.instance_id.clone(),
            run_id: instance.run_id.clone(),
            batch_id: batch_id.to_owned(),
            activity_type: activity_type.to_owned(),
            activity_capabilities,
            task_queue: task_queue.to_owned(),
            state: workflow_state,
            chunk_size,
            max_attempts,
            retry_delay_ms,
            total_items: total_items as u32,
            aggregation_group_count,
            execution_policy,
            reducer,
            throughput_backend: throughput_backend.to_owned(),
            throughput_backend_version: throughput_backend_version.to_owned(),
            routing_reason: admission.routing_reason.clone(),
            admission_policy_version: admission.admission_policy_version.clone(),
            input_handle,
            result_handle,
        }),
    })
}

pub(super) async fn submit_native_stream_run_via_bridge(
    state: &AppState,
    instance: &WorkflowInstanceState,
    batch_id: &str,
    activity_type: &str,
    activity_capabilities: Option<ActivityExecutionCapabilities>,
    task_queue: &str,
    total_items: usize,
    result_handle: &Value,
    workflow_state: Option<String>,
    chunk_size: u32,
    max_attempts: u32,
    retry_delay_ms: u64,
    aggregation_group_count: u32,
    execution_policy: Option<String>,
    reducer: Option<String>,
    throughput_backend: &str,
    throughput_backend_version: &str,
    admission: &BulkAdmissionDecision,
    input_handle: PayloadHandle,
    source_event_id: Uuid,
    occurred_at: DateTime<Utc>,
) -> Result<()> {
    let command = start_throughput_run_command(
        instance,
        batch_id,
        activity_type,
        activity_capabilities,
        task_queue,
        total_items,
        result_handle,
        workflow_state,
        chunk_size,
        max_attempts,
        retry_delay_ms,
        aggregation_group_count,
        execution_policy,
        reducer,
        throughput_backend,
        throughput_backend_version,
        admission,
        input_handle,
        source_event_id,
        occurred_at,
    )
    .await?;
    let bridge_request_id = throughput_bridge_request_id(
        &instance.tenant_id,
        &instance.instance_id,
        &instance.run_id,
        batch_id,
    );
    let bridge_progress = state
        .store
        .upsert_throughput_bridge_submission(
            source_event_id,
            THROUGHPUT_BRIDGE_PROTOCOL_VERSION,
            ThroughputBridgeOperationKind::BulkRun,
            &instance.tenant_id,
            &instance.instance_id,
            &instance.run_id,
            batch_id,
            throughput_backend,
            &bridge_request_id,
            &command.dedupe_key,
            Some(command.command_id),
            Some(&command.partition_key),
            occurred_at,
        )
        .await?;
    state
        .store
        .upsert_throughput_run(&ThroughputRunRecord {
            tenant_id: instance.tenant_id.clone(),
            instance_id: instance.instance_id.clone(),
            run_id: instance.run_id.clone(),
            definition_id: instance.definition_id.clone(),
            definition_version: instance.definition_version,
            artifact_hash: instance.artifact_hash.clone(),
            batch_id: batch_id.to_owned(),
            bridge_protocol_version: THROUGHPUT_BRIDGE_PROTOCOL_VERSION.to_owned(),
            bridge_operation_kind: ThroughputBridgeOperationKind::BulkRun.as_str().to_owned(),
            bridge_request_id,
            throughput_backend: throughput_backend.to_owned(),
            execution_path: ThroughputExecutionPath::NativeStreamV2.as_str().to_owned(),
            status: ThroughputRunStatus::Scheduled.as_str().to_owned(),
            command_dedupe_key: command.dedupe_key.clone(),
            command: command.clone(),
            command_published_at: None,
            started_at: None,
            terminal_at: None,
            bridge_terminal_status: None,
            bridge_terminal_event_id: None,
            bridge_terminal_owner_epoch: None,
            bridge_terminal_accepted_at: None,
            created_at: occurred_at,
            updated_at: occurred_at,
        })
        .await?;
    if bridge_submission_state(&bridge_progress).needs_start_publication() {
        let command_publisher = state
            .throughput_command_publisher
            .as_ref()
            .context("stream-v2 bulk admission requires throughput command publisher")?;
        let published_at = Utc::now();
        command_publisher.publish(&command, &command.partition_key).await?;
        let _ = state
            .store
            .mark_throughput_run_command_published(
                &instance.tenant_id,
                &instance.instance_id,
                &instance.run_id,
                batch_id,
                published_at,
            )
            .await?;
        state.store.mark_throughput_bridge_command_published(source_event_id, published_at).await?;
    }
    Ok(())
}

pub(super) async fn request_stream_batch_cancellations_for_run(
    state: &AppState,
    tenant_id: &str,
    instance_id: &str,
    run_id: &str,
    reason: &str,
    requested_at: DateTime<Utc>,
) -> Result<()> {
    let batches = state
        .store
        .list_bulk_batches_for_run_page_query_view(tenant_id, instance_id, run_id, 10_000, 0)
        .await?;
    for batch in batches {
        if batch.throughput_backend != ThroughputBackend::StreamV2.as_str()
            || batch.status.is_terminal()
        {
            continue;
        }
        let Some(progress) = state
            .store
            .get_throughput_bridge_progress_for_batch(
                tenant_id,
                instance_id,
                run_id,
                &batch.batch_id,
            )
            .await?
        else {
            continue;
        };
        let _ = state
            .store
            .request_throughput_bridge_cancellation(
                progress.workflow_event_id,
                requested_at,
                reason,
            )
            .await?;
    }
    Ok(())
}

pub(super) async fn repair_pending_stream_v2_bridge(
    state: &AppState,
) -> Result<(usize, usize, usize, usize, usize, usize)> {
    let command_publisher = state.throughput_command_publisher.as_ref();
    let workflow_publisher = state.publisher.as_ref();
    let ledgers = state
        .store
        .list_pending_throughput_bridge_repairs_for_backend(
            ThroughputBackend::StreamV2.as_str(),
            10_000,
        )
        .await?;
    let mut repaired_starts = 0_usize;
    let mut repaired_cancels = 0_usize;
    let mut repaired_acceptance = 0_usize;
    let mut repaired_stream_queries = 0_usize;
    let mut repaired_stream_checkpoints = 0_usize;
    let mut repaired_stream_terminals = 0_usize;
    for ledger in ledgers {
        match ledger.next_repair() {
            Some(fabrik_throughput::ThroughputBridgeRepairKind::PublishStart) => {
                let Some(command_publisher) = command_publisher else {
                    continue;
                };
                let Some(workflow_event_id) = ledger.workflow_event_id else {
                    continue;
                };
                let Some(run) = state
                    .store
                    .get_throughput_run(
                        &ledger.tenant_id,
                        &ledger.instance_id,
                        &ledger.run_id,
                        &ledger.batch_id,
                    )
                    .await?
                else {
                    continue;
                };
                let ThroughputCommand::StartThroughputRun(_) = &run.command.payload else {
                    continue;
                };
                command_publisher
                    .publish(&run.command, &run.command.partition_key)
                    .await
                    .with_context(|| {
                        format!(
                            "failed to republish native throughput run command for batch {}",
                            run.batch_id
                        )
                    })?;
                let published_at = Utc::now();
                let _ = state
                    .store
                    .mark_throughput_run_command_published(
                        &run.tenant_id,
                        &run.instance_id,
                        &run.run_id,
                        &run.batch_id,
                        published_at,
                    )
                    .await?;
                state
                    .store
                    .mark_throughput_bridge_command_published(workflow_event_id, published_at)
                    .await?;
                let mut debug = state.debug.lock().expect("unified debug lock poisoned");
                debug.repaired_bridge_start_commands =
                    debug.repaired_bridge_start_commands.saturating_add(1);
                debug.last_bridge_repair_at = Some(published_at);
                debug.last_bridge_repair_kind = Some("publish_start".to_owned());
                debug.last_bridge_repair_batch_id = Some(run.batch_id.clone());
                repaired_starts = repaired_starts.saturating_add(1);
            }
            Some(fabrik_throughput::ThroughputBridgeRepairKind::PublishCancel) => {
                let Some(command_publisher) = command_publisher else {
                    continue;
                };
                let Some(workflow_event_id) = ledger.workflow_event_id else {
                    continue;
                };
                let reason = ledger
                    .cancellation_reason
                    .clone()
                    .unwrap_or_else(|| "workflow cancelled".to_owned());
                let command = throughput_cancel_command_envelope(
                    workflow_event_id,
                    fabrik_throughput::ThroughputBatchIdentity {
                        tenant_id: ledger.tenant_id.clone(),
                        instance_id: ledger.instance_id.clone(),
                        run_id: ledger.run_id.clone(),
                        batch_id: ledger.batch_id.clone(),
                    },
                    reason,
                    ledger.cancellation_requested_at.unwrap_or_else(Utc::now),
                );
                command_publisher.publish(&command, &command.partition_key).await.with_context(
                    || {
                        format!(
                            "failed to publish native throughput cancel command for batch {}",
                            ledger.batch_id
                        )
                    },
                )?;
                state
                    .store
                    .mark_throughput_bridge_cancel_command_published(workflow_event_id, Utc::now())
                    .await?;
                let mut debug = state.debug.lock().expect("unified debug lock poisoned");
                debug.repaired_bridge_cancel_commands =
                    debug.repaired_bridge_cancel_commands.saturating_add(1);
                debug.last_bridge_repair_at = Some(Utc::now());
                debug.last_bridge_repair_kind = Some("publish_cancel".to_owned());
                debug.last_bridge_repair_batch_id = Some(ledger.batch_id.clone());
                repaired_cancels = repaired_cancels.saturating_add(1);
            }
            Some(fabrik_throughput::ThroughputBridgeRepairKind::AcceptTerminal) => {
                let Some(publisher) = workflow_publisher else {
                    continue;
                };
                let Some(terminal) = state
                    .store
                    .get_throughput_terminal(
                        &ledger.tenant_id,
                        &ledger.instance_id,
                        &ledger.run_id,
                        &ledger.batch_id,
                    )
                    .await?
                else {
                    continue;
                };
                publisher
                    .publish(&terminal.terminal_event, &terminal.terminal_event.partition_key)
                    .await
                    .with_context(|| {
                        format!("failed to republish bridge terminal for batch {}", ledger.batch_id)
                    })?;
                let mut debug = state.debug.lock().expect("unified debug lock poisoned");
                debug.repaired_bridge_terminal_callbacks =
                    debug.repaired_bridge_terminal_callbacks.saturating_add(1);
                debug.last_bridge_repair_at = Some(Utc::now());
                debug.last_bridge_repair_kind = Some("accept_terminal".to_owned());
                debug.last_bridge_repair_batch_id = Some(ledger.batch_id.clone());
                repaired_acceptance = repaired_acceptance.saturating_add(1);
            }
            None => {}
            Some(_) => {}
        }
    }
    if let Some(publisher) = workflow_publisher {
        let pending_stream_job_ledgers =
            state.store.list_pending_stream_job_callback_repairs(10_000).await?;
        for ledger in pending_stream_job_ledgers {
            let Some(handle) =
                state.store.get_stream_job_callback_handle_by_handle_id(&ledger.handle_id).await?
            else {
                continue;
            };
            for repair in ledger.pending_repairs() {
                match repair {
                    fabrik_throughput::StreamJobBridgeRepairKind::AcceptCheckpoint => {
                        let pending_checkpoints = state
                            .store
                            .list_stream_job_callback_checkpoints_for_handle_page(
                                &handle.handle_id,
                                10_000,
                                0,
                            )
                            .await?;
                        for checkpoint in pending_checkpoints {
                            if checkpoint.next_repair_for_handle_epoch(handle.stream_owner_epoch)
                                != Some(
                                    fabrik_throughput::StreamJobBridgeRepairKind::AcceptCheckpoint,
                                )
                            {
                                continue;
                            }
                            let Some(event) =
                                stream_job_checkpoint_callback_event(&handle, &checkpoint)
                            else {
                                continue;
                            };
                            publisher.publish(&event, &event.partition_key).await.with_context(
                                || {
                                    format!(
                                        "failed to republish bridge stream-job checkpoint callback for await {}",
                                        checkpoint.await_request_id
                                    )
                                },
                            )?;
                            let repaired_at = Utc::now();
                            let mut debug =
                                state.debug.lock().expect("unified debug lock poisoned");
                            debug.repaired_bridge_stream_job_checkpoint_callbacks = debug
                                .repaired_bridge_stream_job_checkpoint_callbacks
                                .saturating_add(1);
                            debug.last_bridge_repair_at = Some(repaired_at);
                            debug.last_bridge_repair_kind =
                                Some("accept_stream_checkpoint".to_owned());
                            debug.last_bridge_repair_batch_id =
                                Some(checkpoint.await_request_id.clone());
                            repaired_stream_checkpoints =
                                repaired_stream_checkpoints.saturating_add(1);
                        }
                    }
                    fabrik_throughput::StreamJobBridgeRepairKind::AcceptQuery => {
                        let pending_queries = state
                            .store
                            .list_stream_job_callback_queries_for_handle_page(
                                &handle.handle_id,
                                10_000,
                                0,
                            )
                            .await?;
                        for query in pending_queries {
                            if query.next_repair_for_handle_epoch(handle.stream_owner_epoch)
                                != Some(fabrik_throughput::StreamJobBridgeRepairKind::AcceptQuery)
                            {
                                continue;
                            }
                            let Some(event) = stream_job_query_callback_event(&handle, &query)
                            else {
                                continue;
                            };
                            publisher.publish(&event, &event.partition_key).await.with_context(
                                || {
                                    format!(
                                        "failed to republish bridge stream-job query callback for query {}",
                                        query.query_id
                                    )
                                },
                            )?;
                            let repaired_at = Utc::now();
                            let mut debug =
                                state.debug.lock().expect("unified debug lock poisoned");
                            debug.repaired_bridge_stream_job_query_callbacks =
                                debug.repaired_bridge_stream_job_query_callbacks.saturating_add(1);
                            debug.last_bridge_repair_at = Some(repaired_at);
                            debug.last_bridge_repair_kind = Some("accept_stream_query".to_owned());
                            debug.last_bridge_repair_batch_id = Some(query.query_id.clone());
                            repaired_stream_queries = repaired_stream_queries.saturating_add(1);
                        }
                    }
                    fabrik_throughput::StreamJobBridgeRepairKind::AcceptTerminal => {
                        let Some(event) = stream_job_terminal_callback_event(&handle) else {
                            continue;
                        };
                        publisher.publish(&event, &event.partition_key).await.with_context(|| {
                            format!(
                                "failed to republish bridge stream-job terminal callback for handle {}",
                                handle.handle_id
                            )
                        })?;
                        let repaired_at = Utc::now();
                        let mut debug = state.debug.lock().expect("unified debug lock poisoned");
                        debug.repaired_bridge_stream_job_terminal_callbacks =
                            debug.repaired_bridge_stream_job_terminal_callbacks.saturating_add(1);
                        debug.last_bridge_repair_at = Some(repaired_at);
                        debug.last_bridge_repair_kind = Some("accept_stream_terminal".to_owned());
                        debug.last_bridge_repair_batch_id = Some(handle.handle_id.clone());
                        repaired_stream_terminals = repaired_stream_terminals.saturating_add(1);
                    }
                }
            }
        }
    }
    Ok((
        repaired_starts,
        repaired_cancels,
        repaired_acceptance,
        repaired_stream_queries,
        repaired_stream_checkpoints,
        repaired_stream_terminals,
    ))
}

pub(super) async fn run_stream_v2_bridge_repair_loop(state: AppState) {
    loop {
        match repair_pending_stream_v2_bridge(&state).await {
            Ok((
                repaired_starts,
                repaired_cancels,
                repaired_acceptance,
                repaired_stream_queries,
                repaired_stream_checkpoints,
                repaired_stream_terminals,
            )) if repaired_starts > 0
                || repaired_cancels > 0
                || repaired_acceptance > 0
                || repaired_stream_queries > 0
                || repaired_stream_checkpoints > 0
                || repaired_stream_terminals > 0 =>
            {
                info!(
                    repaired_starts,
                    repaired_cancels,
                    repaired_acceptance,
                    repaired_stream_queries,
                    repaired_stream_checkpoints,
                    repaired_stream_terminals,
                    "repaired pending native stream-v2 bridge actions"
                );
            }
            Ok(_) => {}
            Err(error) => {
                error!(error = %error, "failed to repair pending native stream-v2 bridge actions");
            }
        }
        tokio::time::sleep(std::time::Duration::from_millis(1_000)).await;
    }
}
