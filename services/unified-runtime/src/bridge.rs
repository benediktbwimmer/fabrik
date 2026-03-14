use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use fabrik_events::{EventEnvelope, WorkflowEvent};
use fabrik_store::{ThroughputBridgeProgressRecord, ThroughputRunRecord};
use fabrik_throughput::{
    ActivityExecutionCapabilities, PayloadHandle, StartThroughputRunCommand, ThroughputBackend,
    ThroughputBridgeState, ThroughputBridgeSubmissionStatus, ThroughputBridgeTerminalStatus,
    ThroughputCommand, ThroughputCommandEnvelope, ThroughputExecutionPath, ThroughputRunStatus,
    throughput_bridge_request_id, throughput_bridge_request_matches,
    throughput_cancel_command_envelope, throughput_partition_key, throughput_start_command_id,
    throughput_start_dedupe_key,
};
use fabrik_workflow::{ExecutionTurnContext, WorkflowInstanceState, execution_state_for_event};
use serde_json::Value;
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
        bridge_request_id: progress.bridge_request_id.clone(),
        submission_status: progress.parsed_submission_status(),
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
        bridge_request_id: run.bridge_request_id.clone(),
        submission_status: None,
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

async fn repair_unpublished_stream_v2_runs(state: &AppState) -> Result<usize> {
    let Some(command_publisher) = state.throughput_command_publisher.as_ref() else {
        return Ok(0);
    };
    let submissions = state
        .store
        .list_pending_throughput_bridge_submissions_for_backend(
            ThroughputBackend::StreamV2.as_str(),
            10_000,
        )
        .await?;
    let mut repaired = 0_usize;
    for submission in submissions {
        if !bridge_submission_state(&submission).needs_start_publication() {
            continue;
        }
        let Some(run) = state
            .store
            .get_throughput_run(
                &submission.tenant_id,
                &submission.instance_id,
                &submission.run_id,
                &submission.batch_id,
            )
            .await?
        else {
            continue;
        };
        let ThroughputCommand::StartThroughputRun(_) = &run.command.payload else {
            continue;
        };
        command_publisher.publish(&run.command, &run.command.partition_key).await.with_context(
            || {
                format!(
                    "failed to republish native throughput run command for batch {}",
                    run.batch_id
                )
            },
        )?;
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
            .mark_throughput_bridge_command_published(submission.workflow_event_id, published_at)
            .await?;
        repaired = repaired.saturating_add(1);
    }
    Ok(repaired)
}

async fn repair_unpublished_stream_v2_cancellations(state: &AppState) -> Result<usize> {
    let Some(command_publisher) = state.throughput_command_publisher.as_ref() else {
        return Ok(0);
    };
    let cancellations = state
        .store
        .list_pending_throughput_bridge_cancellations_for_backend(
            ThroughputBackend::StreamV2.as_str(),
            10_000,
        )
        .await?;
    let mut repaired = 0_usize;
    for cancellation in cancellations {
        if !bridge_submission_state(&cancellation).needs_cancel_publication() {
            continue;
        }
        let reason = cancellation
            .cancellation_reason
            .clone()
            .unwrap_or_else(|| "workflow cancelled".to_owned());
        let command = throughput_cancel_command_envelope(
            cancellation.workflow_event_id,
            fabrik_throughput::ThroughputBatchIdentity {
                tenant_id: cancellation.tenant_id.clone(),
                instance_id: cancellation.instance_id.clone(),
                run_id: cancellation.run_id.clone(),
                batch_id: cancellation.batch_id.clone(),
            },
            reason,
            cancellation.cancellation_requested_at.unwrap_or(cancellation.updated_at),
        );
        command_publisher.publish(&command, &command.partition_key).await.with_context(|| {
            format!(
                "failed to publish native throughput cancel command for batch {}",
                cancellation.batch_id
            )
        })?;
        state
            .store
            .mark_throughput_bridge_cancel_command_published(
                cancellation.workflow_event_id,
                Utc::now(),
            )
            .await?;
        repaired = repaired.saturating_add(1);
    }
    Ok(repaired)
}

async fn repair_pending_stream_v2_terminal_acceptance(state: &AppState) -> Result<usize> {
    let Some(publisher) = state.publisher.as_ref() else {
        return Ok(0);
    };
    let runs = state
        .store
        .list_pending_bridge_terminal_acceptance_for_backend(
            ThroughputBackend::StreamV2.as_str(),
            10_000,
        )
        .await?;
    let mut repaired = 0_usize;
    for run in runs {
        if !bridge_run_terminal_state(&run).needs_workflow_acceptance_repair() {
            continue;
        }
        let Some(terminal) = state
            .store
            .get_throughput_terminal(&run.tenant_id, &run.instance_id, &run.run_id, &run.batch_id)
            .await?
        else {
            continue;
        };
        publisher
            .publish(&terminal.terminal_event, &terminal.terminal_event.partition_key)
            .await
            .with_context(|| {
            format!("failed to republish bridge terminal for batch {}", run.batch_id)
        })?;
        repaired = repaired.saturating_add(1);
    }
    Ok(repaired)
}

pub(super) async fn run_stream_v2_bridge_repair_loop(state: AppState) {
    loop {
        match repair_unpublished_stream_v2_runs(&state).await {
            Ok(repaired) if repaired > 0 => {
                info!(repaired, "repaired unpublished native stream-v2 admissions");
            }
            Ok(_) => {}
            Err(error) => {
                error!(error = %error, "failed to repair unpublished native stream-v2 admissions");
            }
        }
        match repair_unpublished_stream_v2_cancellations(&state).await {
            Ok(repaired) if repaired > 0 => {
                info!(repaired, "repaired unpublished native stream-v2 cancellations");
            }
            Ok(_) => {}
            Err(error) => {
                error!(error = %error, "failed to repair unpublished native stream-v2 cancellations");
            }
        }
        match repair_pending_stream_v2_terminal_acceptance(&state).await {
            Ok(repaired) if repaired > 0 => {
                info!(repaired, "repaired pending native stream-v2 terminal acceptance");
            }
            Ok(_) => {}
            Err(error) => {
                error!(error = %error, "failed to repair pending native stream-v2 terminal acceptance");
            }
        }
        tokio::time::sleep(std::time::Duration::from_millis(1_000)).await;
    }
}
