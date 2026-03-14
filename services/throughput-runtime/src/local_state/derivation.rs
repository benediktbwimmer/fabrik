use super::*;

pub(super) fn derive_batch_state_from_chunk_states(
    batch: &WorkflowBulkBatchRecord,
    chunks: &[LocalChunkState],
) -> LocalBatchState {
    let identity = ThroughputBatchIdentity {
        tenant_id: batch.tenant_id.clone(),
        instance_id: batch.instance_id.clone(),
        run_id: batch.run_id.clone(),
        batch_id: batch.batch_id.clone(),
    };
    let mut succeeded_items = 0_u32;
    let mut failed_items = 0_u32;
    let mut cancelled_items = 0_u32;
    let mut terminal_chunk_count = 0_u32;
    let mut chunk_error = None;
    let mut updated_at = batch.updated_at;
    let mut terminal_at = batch.terminal_at;
    let mut saw_active_chunk = false;

    for chunk in chunks {
        updated_at = updated_at.max(chunk.updated_at);
        match chunk.status.as_str() {
            "completed" => {
                succeeded_items = succeeded_items.saturating_add(chunk.item_count);
                terminal_chunk_count = terminal_chunk_count.saturating_add(1);
                terminal_at = Some(
                    terminal_at
                        .unwrap_or(chunk.completed_at.unwrap_or(chunk.updated_at))
                        .max(chunk.completed_at.unwrap_or(chunk.updated_at)),
                );
                saw_active_chunk = true;
            }
            "failed" => {
                failed_items = failed_items.saturating_add(chunk.item_count);
                terminal_chunk_count = terminal_chunk_count.saturating_add(1);
                chunk_error = chunk_error.or_else(|| chunk.error.clone());
                terminal_at = Some(
                    terminal_at
                        .unwrap_or(chunk.completed_at.unwrap_or(chunk.updated_at))
                        .max(chunk.completed_at.unwrap_or(chunk.updated_at)),
                );
                saw_active_chunk = true;
            }
            "cancelled" => {
                cancelled_items = cancelled_items.saturating_add(chunk.item_count);
                terminal_chunk_count = terminal_chunk_count.saturating_add(1);
                chunk_error = chunk_error.or_else(|| chunk.error.clone());
                terminal_at = Some(
                    terminal_at
                        .unwrap_or(chunk.completed_at.unwrap_or(chunk.updated_at))
                        .max(chunk.completed_at.unwrap_or(chunk.updated_at)),
                );
                saw_active_chunk = true;
            }
            "started" => saw_active_chunk = true,
            _ => {}
        }
    }

    let derived_group_terminals = chunks
        .iter()
        .map(|chunk| chunk.group_id)
        .collect::<HashSet<_>>()
        .into_iter()
        .filter_map(|group_id| {
            aggregation_state::derive_terminal_group_from_chunks(chunks, group_id)
        })
        .collect::<Vec<_>>();
    let grouped_terminal = if batch.aggregation_group_count > 1
        && !derived_group_terminals.is_empty()
        && u32::try_from(derived_group_terminals.len()).unwrap_or_default()
            >= batch.aggregation_group_count
    {
        let mut grouped_succeeded = 0_u32;
        let mut grouped_failed = 0_u32;
        let mut grouped_cancelled = 0_u32;
        let mut grouped_error = None;
        let mut grouped_terminal_at = DateTime::<Utc>::UNIX_EPOCH;
        for group in &derived_group_terminals {
            grouped_succeeded = grouped_succeeded.saturating_add(group.succeeded_items);
            grouped_failed = grouped_failed.saturating_add(group.failed_items);
            grouped_cancelled = grouped_cancelled.saturating_add(group.cancelled_items);
            grouped_error = grouped_error.or_else(|| group.error.clone());
            grouped_terminal_at = grouped_terminal_at.max(group.terminal_at);
        }
        let status = if grouped_failed > 0 {
            Some(WorkflowBulkBatchStatus::Failed)
        } else if grouped_cancelled > 0 {
            Some(WorkflowBulkBatchStatus::Cancelled)
        } else if batch.total_items > 0 && grouped_succeeded >= batch.total_items {
            Some(WorkflowBulkBatchStatus::Completed)
        } else {
            None
        };
        status.map(|status| {
            (
                status,
                grouped_succeeded,
                grouped_failed,
                grouped_cancelled,
                grouped_error,
                grouped_terminal_at,
            )
        })
    } else {
        None
    };

    let mut status = batch.status.clone();
    let mut error = batch.error.clone().or(chunk_error.clone());
    if matches!(
        status,
        WorkflowBulkBatchStatus::Completed
            | WorkflowBulkBatchStatus::Failed
            | WorkflowBulkBatchStatus::Cancelled
    ) && batch.terminal_at.is_some()
    {
        succeeded_items = succeeded_items.max(batch.succeeded_items);
        failed_items = failed_items.max(batch.failed_items);
        cancelled_items = cancelled_items.max(batch.cancelled_items);
        terminal_chunk_count = batch.chunk_count;
        terminal_at = batch.terminal_at;
        error = batch.error.clone().or(chunk_error);
    } else if let Some((
        grouped_status,
        grouped_succeeded,
        grouped_failed,
        grouped_cancelled,
        grouped_error,
        grouped_terminal_at,
    )) = grouped_terminal
    {
        status = grouped_status;
        succeeded_items = grouped_succeeded;
        failed_items = grouped_failed;
        cancelled_items = grouped_cancelled;
        terminal_chunk_count = batch.chunk_count;
        terminal_at = Some(grouped_terminal_at);
        error = grouped_error;
    } else if batch.aggregation_group_count <= 1 {
        if failed_items > 0 {
            status = WorkflowBulkBatchStatus::Failed;
            terminal_chunk_count = batch.chunk_count;
            terminal_at = Some(terminal_at.unwrap_or(updated_at));
        } else if cancelled_items > 0 {
            status = WorkflowBulkBatchStatus::Cancelled;
            terminal_chunk_count = batch.chunk_count;
            terminal_at = Some(terminal_at.unwrap_or(updated_at));
        } else if batch.total_items > 0
            && succeeded_items >= batch.total_items
            && terminal_chunk_count >= batch.chunk_count
        {
            status = WorkflowBulkBatchStatus::Completed;
            terminal_chunk_count = batch.chunk_count;
            terminal_at = Some(terminal_at.unwrap_or(updated_at));
            error = None;
        } else if saw_active_chunk {
            status = WorkflowBulkBatchStatus::Running;
            terminal_at = None;
        } else {
            status = WorkflowBulkBatchStatus::Scheduled;
            terminal_at = None;
            error = batch.error.clone();
        }
    } else if saw_active_chunk {
        status = WorkflowBulkBatchStatus::Running;
        terminal_at = None;
    } else {
        status = WorkflowBulkBatchStatus::Scheduled;
        terminal_at = None;
        error = batch.error.clone();
    }

    LocalBatchState {
        identity,
        definition_id: batch.definition_id.clone(),
        definition_version: batch.definition_version,
        artifact_hash: batch.artifact_hash.clone(),
        task_queue: batch.task_queue.clone(),
        activity_capabilities: batch.activity_capabilities.clone(),
        execution_policy: batch.execution_policy.clone(),
        reducer: batch.reducer.clone(),
        reducer_class: batch.reducer_class.clone(),
        aggregation_tree_depth: batch.aggregation_tree_depth.max(1),
        fast_lane_enabled: batch.fast_lane_enabled,
        aggregation_group_count: batch.aggregation_group_count.max(1),
        total_items: batch.total_items,
        chunk_count: batch.chunk_count,
        terminal_chunk_count,
        succeeded_items,
        failed_items,
        cancelled_items,
        status: status.as_str().to_owned(),
        last_report_id: None,
        error,
        reducer_output: batch.reducer_output.clone(),
        created_at: batch.scheduled_at,
        updated_at,
        terminal_at,
    }
}

pub(super) fn infer_ungrouped_batch_terminal_from_chunk_apply(
    batch: &LocalBatchState,
    chunk_status: &str,
) -> Option<&'static str> {
    if batch.aggregation_group_count > 1 {
        return None;
    }

    match chunk_status {
        "failed" => Some(WorkflowBulkBatchStatus::Failed.as_str()),
        "cancelled" => Some(WorkflowBulkBatchStatus::Cancelled.as_str()),
        "completed"
            if batch.total_items > 0
                && batch.succeeded_items >= batch.total_items
                && batch.terminal_chunk_count >= batch.chunk_count =>
        {
            Some(WorkflowBulkBatchStatus::Completed.as_str())
        }
        _ => None,
    }
}
