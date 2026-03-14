use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use fabrik_broker::{decode_consumed_workflow_event, decode_json_record};
use fabrik_events::{EventEnvelope, WorkflowEvent};
use fabrik_throughput::{
    ThroughputBackend, ThroughputBatchIdentity, ThroughputCommand, ThroughputCommandEnvelope,
    ThroughputRunStatus,
};
use futures_util::StreamExt;
use tokio::task::JoinSet;
use tracing::{error, warn};

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
