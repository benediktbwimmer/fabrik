use super::*;

impl LocalThroughputState {
    pub(crate) fn batch_snapshot(
        &self,
        identity: &ThroughputBatchIdentity,
    ) -> Result<Option<LocalBatchSnapshot>> {
        self.load_batch_state(identity).map(|batch| {
            batch.map(|batch| LocalBatchSnapshot {
                identity: batch.identity,
                definition_id: batch.definition_id,
                definition_version: batch.definition_version,
                artifact_hash: batch.artifact_hash,
                task_queue: batch.task_queue,
                activity_capabilities: batch.activity_capabilities,
                execution_policy: batch.execution_policy,
                reducer: batch.reducer,
                reducer_class: batch.reducer_class,
                aggregation_tree_depth: batch.aggregation_tree_depth,
                fast_lane_enabled: batch.fast_lane_enabled,
                total_items: batch.total_items,
                chunk_count: batch.chunk_count,
                succeeded_items: batch.succeeded_items,
                failed_items: batch.failed_items,
                cancelled_items: batch.cancelled_items,
                status: batch.status,
                error: batch.error,
                reducer_output: batch.reducer_output,
                updated_at: batch.updated_at,
                terminal_at: batch.terminal_at,
            })
        })
    }

    pub fn project_batch_running(
        &self,
        identity: &ThroughputBatchIdentity,
        occurred_at: DateTime<Utc>,
    ) -> Result<Option<LocalBatchSnapshot>> {
        let Some(mut batch) = self.batch_snapshot(identity)? else {
            return Ok(None);
        };
        if batch.status != WorkflowBulkBatchStatus::Scheduled.as_str() {
            return Ok(None);
        }
        batch.status = WorkflowBulkBatchStatus::Running.as_str().to_owned();
        batch.updated_at = occurred_at;
        Ok(Some(batch))
    }

    pub fn has_collect_results_batches(&self) -> Result<bool> {
        Ok(self
            .load_all_batches()?
            .into_iter()
            .any(|batch| batch.reducer.as_deref() == Some("collect_results")))
    }

    pub fn chunk_snapshots_for_batch(
        &self,
        identity: &ThroughputBatchIdentity,
    ) -> Result<Vec<LocalChunkSnapshot>> {
        self.load_chunks_for_batch(identity).map(|chunks| {
            chunks
                .into_iter()
                .map(|chunk| LocalChunkSnapshot {
                    chunk_id: chunk.chunk_id,
                    status: chunk.status,
                    attempt: chunk.attempt,
                    lease_epoch: chunk.lease_epoch,
                    owner_epoch: chunk.owner_epoch,
                    result_handle: chunk.result_handle,
                    output: chunk.output,
                    error: chunk.error,
                    cancellation_requested: chunk.cancellation_requested,
                    cancellation_reason: chunk.cancellation_reason,
                    cancellation_metadata: chunk.cancellation_metadata,
                    worker_id: chunk.worker_id,
                    lease_token: chunk.lease_token,
                    report_id: chunk.report_id,
                    available_at: chunk.available_at,
                    started_at: chunk.started_at,
                    lease_expires_at: chunk.lease_expires_at,
                    completed_at: chunk.completed_at,
                    updated_at: chunk.updated_at,
                })
                .collect()
        })
    }
}
