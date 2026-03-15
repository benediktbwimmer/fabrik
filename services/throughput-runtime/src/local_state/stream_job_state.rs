use super::*;

impl LocalThroughputState {
    pub(crate) fn upsert_stream_job_runtime_state(
        &self,
        state: &LocalStreamJobRuntimeState,
    ) -> Result<()> {
        self.db
            .delete_cf(self.cf(STREAM_JOBS_CF), legacy_stream_job_runtime_key(&state.handle_id))
            .context(
                "failed to delete legacy stream job runtime state from column family stream_jobs",
            )?;
        self.db
            .put_cf(
                self.cf(STREAM_JOBS_CF),
                &stream_job_runtime_key(&state.handle_id),
                encode_rocksdb_value(state, "stream job runtime state")?,
            )
            .context("failed to store stream job runtime state in column family stream_jobs")?;
        Ok(())
    }

    pub(crate) fn replace_stream_job_dispatch_manifest(
        &self,
        handle_id: &str,
        dispatch_batches: Vec<LocalStreamJobDispatchBatch>,
        updated_at: DateTime<Utc>,
    ) -> Result<()> {
        let Some(mut runtime_state) = self.load_stream_job_runtime_state(handle_id)? else {
            anyhow::bail!("stream job runtime state missing for handle {handle_id}");
        };
        runtime_state.dispatch_batches = dispatch_batches;
        runtime_state.dispatch_completed_at = runtime_state.dispatch_completed_at.filter(|_| {
            runtime_state.applied_dispatch_batch_ids.len() == runtime_state.dispatch_batches.len()
        });
        runtime_state.updated_at = updated_at;
        self.upsert_stream_job_runtime_state(&runtime_state)
    }

    pub(crate) fn mark_stream_job_dispatch_batch_applied(
        &self,
        handle_id: &str,
        batch_id: &str,
        updated_at: DateTime<Utc>,
    ) -> Result<bool> {
        let Some(mut runtime_state) = self.load_stream_job_runtime_state(handle_id)? else {
            anyhow::bail!("stream job runtime state missing for handle {handle_id}");
        };
        if runtime_state.applied_dispatch_batch_ids.iter().any(|existing| existing == batch_id) {
            return Ok(false);
        }
        runtime_state.applied_dispatch_batch_ids.push(batch_id.to_owned());
        runtime_state.applied_dispatch_batch_ids.sort();
        runtime_state.applied_dispatch_batch_ids.dedup();
        if runtime_state.applied_dispatch_batch_ids.len() == runtime_state.dispatch_batches.len() {
            runtime_state.dispatch_completed_at = Some(updated_at);
        }
        runtime_state.updated_at = updated_at;
        self.upsert_stream_job_runtime_state(&runtime_state)?;
        Ok(true)
    }

    pub(crate) fn mark_stream_job_dispatch_cancelled(
        &self,
        handle_id: &str,
        cancelled_at: DateTime<Utc>,
    ) -> Result<()> {
        let Some(mut runtime_state) = self.load_stream_job_runtime_state(handle_id)? else {
            anyhow::bail!("stream job runtime state missing for handle {handle_id}");
        };
        runtime_state.dispatch_cancelled_at = Some(cancelled_at);
        runtime_state.updated_at = cancelled_at;
        self.upsert_stream_job_runtime_state(&runtime_state)
    }

    pub(crate) fn upsert_stream_job_view_value(
        &self,
        handle_id: &str,
        job_id: &str,
        view_name: &str,
        logical_key: &str,
        output: Value,
        checkpoint_sequence: i64,
        updated_at: DateTime<Utc>,
    ) -> Result<()> {
        let state = LocalStreamJobViewState {
            handle_id: handle_id.to_owned(),
            job_id: job_id.to_owned(),
            view_name: view_name.to_owned(),
            logical_key: logical_key.to_owned(),
            output,
            checkpoint_sequence,
            updated_at,
        };
        self.db
            .delete_cf(
                self.cf(STREAM_JOBS_CF),
                legacy_stream_job_view_key(handle_id, view_name, logical_key),
            )
            .context(
                "failed to delete legacy stream job view state from column family stream_jobs",
            )?;
        self.db
            .put_cf(
                self.cf(STREAM_JOBS_CF),
                &stream_job_view_key(handle_id, view_name, logical_key),
                encode_rocksdb_value(&state, "stream job view state")?,
            )
            .context("failed to store stream job view state in column family stream_jobs")?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use uuid::Uuid;

    fn temp_path(prefix: &str) -> PathBuf {
        std::env::temp_dir().join(format!("{prefix}-{}", Uuid::now_v7()))
    }

    #[test]
    fn stream_job_view_round_trips_through_checkpoint_restore() -> Result<()> {
        let db_path = temp_path("stream-job-view-db");
        let checkpoint_dir = temp_path("stream-job-view-checkpoints");
        let state = LocalThroughputState::open(&db_path, &checkpoint_dir, 3)?;
        let updated_at = Utc::now();
        state.upsert_stream_job_view_value(
            "handle-a",
            "job-a",
            "accountTotals",
            "acct_1",
            json!({"accountId": "acct_1", "totalAmount": 7, "asOfCheckpoint": 1}),
            1,
            updated_at,
        )?;

        let checkpoint = state.write_checkpoint()?;
        assert!(checkpoint.exists());

        let restored_db_path = temp_path("stream-job-view-restore-db");
        let restored = LocalThroughputState::open(&restored_db_path, &checkpoint_dir, 3)?;
        assert!(restored.restore_from_latest_checkpoint_if_empty()?);

        let restored_view = restored
            .load_stream_job_view_state("handle-a", "accountTotals", "acct_1")?
            .expect("restored stream job view should exist");
        assert_eq!(restored_view.job_id, "job-a");
        assert_eq!(restored_view.output["totalAmount"], 7);
        assert_eq!(restored_view.checkpoint_sequence, 1);

        let _ = fs::remove_dir_all(&db_path);
        let _ = fs::remove_dir_all(&restored_db_path);
        let _ = fs::remove_dir_all(&checkpoint_dir);
        Ok(())
    }

    #[test]
    fn stream_job_view_round_trips_through_partition_checkpoint_restore() -> Result<()> {
        let db_path = temp_path("stream-job-view-partition-db");
        let checkpoint_dir = temp_path("stream-job-view-partition-checkpoints");
        let state = LocalThroughputState::open(&db_path, &checkpoint_dir, 3)?;
        let updated_at = Utc::now();
        let partition_count = 8;
        let first_key = "acct_partition_a".to_owned();
        let second_key = (1..32)
            .map(|index| format!("acct_partition_b_{index}"))
            .find(|candidate| {
                super::throughput_partition_id(candidate, 0, partition_count)
                    != super::throughput_partition_id(&first_key, 0, partition_count)
            })
            .expect("should find a logical key on a different partition");
        state.upsert_stream_job_view_value(
            "handle-a",
            "job-a",
            "accountTotals",
            &first_key,
            json!({"accountId": first_key.clone(), "totalAmount": 11, "asOfCheckpoint": 1}),
            1,
            updated_at,
        )?;
        state.upsert_stream_job_view_value(
            "handle-b",
            "job-b",
            "accountTotals",
            &second_key,
            json!({"accountId": second_key.clone(), "totalAmount": 3, "asOfCheckpoint": 1}),
            1,
            updated_at,
        )?;

        let owned_partition = super::throughput_partition_id(&first_key, 0, partition_count);
        let checkpoint =
            state.snapshot_partition_checkpoint_value(owned_partition, partition_count)?;

        let restored_db_path = temp_path("stream-job-view-partition-restore-db");
        let restored = LocalThroughputState::open(&restored_db_path, &checkpoint_dir, 3)?;
        assert!(restored.restore_from_checkpoint_value_if_empty(checkpoint)?);

        assert!(
            restored.load_stream_job_view_state("handle-a", "accountTotals", &first_key)?.is_some()
        );
        assert!(
            restored
                .load_stream_job_view_state("handle-b", "accountTotals", &second_key)?
                .is_none()
        );

        let _ = fs::remove_dir_all(&db_path);
        let _ = fs::remove_dir_all(&restored_db_path);
        let _ = fs::remove_dir_all(&checkpoint_dir);
        Ok(())
    }

    #[test]
    fn stream_job_view_loads_legacy_string_key_during_binary_key_transition() -> Result<()> {
        let db_path = temp_path("stream-job-view-legacy-db");
        let checkpoint_dir = temp_path("stream-job-view-legacy-checkpoints");
        let state = LocalThroughputState::open(&db_path, &checkpoint_dir, 3)?;
        let updated_at = Utc::now();
        let legacy_state = LocalStreamJobViewState {
            handle_id: "handle-a".to_owned(),
            job_id: "job-a".to_owned(),
            view_name: "accountTotals".to_owned(),
            logical_key: "acct_legacy".to_owned(),
            output: json!({"accountId": "acct_legacy", "totalAmount": 5, "asOfCheckpoint": 1}),
            checkpoint_sequence: 1,
            updated_at,
        };

        state
            .db
            .put_cf(
                state.cf(STREAM_JOBS_CF),
                legacy_stream_job_view_key("handle-a", "accountTotals", "acct_legacy"),
                encode_rocksdb_value(&legacy_state, "legacy stream job view state")?,
            )
            .context("failed to write legacy stream job view state")?;

        let restored = state
            .load_stream_job_view_state("handle-a", "accountTotals", "acct_legacy")?
            .expect("legacy view state should be readable");
        assert_eq!(restored.output["totalAmount"], 5);

        let _ = fs::remove_dir_all(&db_path);
        let _ = fs::remove_dir_all(&checkpoint_dir);
        Ok(())
    }
}
