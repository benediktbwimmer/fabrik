use super::*;

impl LocalThroughputState {
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
            .put_cf(
                self.cf(STREAM_JOBS_CF),
                stream_job_view_key(handle_id, view_name, logical_key),
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
}
