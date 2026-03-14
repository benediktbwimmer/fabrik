use std::collections::HashSet;

use anyhow::Result;
use serde_json::Value;

use fabrik_store::ThroughputProjectionBatchStateUpdate;
use fabrik_throughput::{
    ThroughputBatchIdentity, ThroughputChunkReport, bulk_reducer_summary_field_name,
};

use crate::local_state::{
    LocalBatchSnapshot, LocalThroughputState, ProjectedBatchRollup, ProjectedBatchTerminal,
    ProjectedChunkApply, ProjectedGroupTerminal,
};
use crate::{WorkflowBulkBatchStatus, collect_results_uses_batch_result_handle_only};

#[derive(Clone)]
pub(crate) struct AggregationCoordinator {
    local_state: LocalThroughputState,
}

impl AggregationCoordinator {
    pub(crate) fn new(local_state: LocalThroughputState) -> Self {
        Self { local_state }
    }

    pub(crate) fn batch_snapshot(
        &self,
        identity: &ThroughputBatchIdentity,
    ) -> Result<Option<LocalBatchSnapshot>> {
        self.local_state.batch_snapshot(identity)
    }

    pub(crate) fn reducer_output_after_report(
        &self,
        identity: &ThroughputBatchIdentity,
        reducer: Option<&str>,
        pending_report: Option<&ThroughputChunkReport>,
    ) -> Result<Option<Value>> {
        if collect_results_uses_batch_result_handle_only(reducer) {
            return Ok(None);
        }
        if bulk_reducer_summary_field_name(reducer).is_none() {
            return Ok(None);
        }
        self.local_state.reduce_batch_success_outputs_after_report(identity, pending_report)
    }

    pub(crate) fn project_batch_rollup_after_chunk_apply(
        &self,
        identity: &ThroughputBatchIdentity,
        report: &ThroughputChunkReport,
        chunk: &ProjectedChunkApply,
    ) -> Result<Option<ProjectedBatchRollup>> {
        self.local_state.project_batch_rollup_after_chunk_apply(identity, report, chunk)
    }

    pub(crate) fn group_barrier_batches(
        &self,
        owned_partitions: Option<&HashSet<i32>>,
        partition_count: i32,
    ) -> Result<Vec<ThroughputBatchIdentity>> {
        self.local_state.group_barrier_batches(owned_partitions, partition_count)
    }

    pub(crate) fn project_batch_terminal_from_groups(
        &self,
        identity: &ThroughputBatchIdentity,
        occurred_at: chrono::DateTime<chrono::Utc>,
    ) -> Result<Option<ProjectedBatchTerminal>> {
        self.local_state.project_batch_terminal_from_groups(identity, occurred_at)
    }

    pub(crate) fn project_group_terminal(
        &self,
        identity: &ThroughputBatchIdentity,
        group_id: u32,
        occurred_at: chrono::DateTime<chrono::Utc>,
    ) -> Result<Option<ProjectedGroupTerminal>> {
        self.local_state.project_group_terminal(identity, group_id, occurred_at)
    }

    pub(crate) fn local_terminal_projection_batch_update(
        &self,
        batch: &LocalBatchSnapshot,
    ) -> Result<Option<ThroughputProjectionBatchStateUpdate>> {
        if !matches!(batch.status.as_str(), "completed" | "failed" | "cancelled") {
            return Ok(None);
        }
        let terminal_at = batch.terminal_at.unwrap_or(batch.updated_at);
        let reducer_output = if batch.status == WorkflowBulkBatchStatus::Completed.as_str() {
            self.reducer_output_after_report(&batch.identity, batch.reducer.as_deref(), None)?
        } else {
            None
        };
        Ok(Some(ThroughputProjectionBatchStateUpdate {
            tenant_id: batch.identity.tenant_id.clone(),
            instance_id: batch.identity.instance_id.clone(),
            run_id: batch.identity.run_id.clone(),
            batch_id: batch.identity.batch_id.clone(),
            status: batch.status.clone(),
            succeeded_items: batch.succeeded_items,
            failed_items: batch.failed_items,
            cancelled_items: batch.cancelled_items,
            error: batch.error.clone(),
            reducer_output: reducer_output.or_else(|| batch.reducer_output.clone()),
            terminal_at: Some(terminal_at),
            updated_at: batch.updated_at.max(terminal_at),
        }))
    }
}
