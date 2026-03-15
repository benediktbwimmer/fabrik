use super::*;
use rocksdb::WriteOptions;

pub(crate) struct StreamJobDispatchBatchAppliedOutcome {
    pub(crate) inserted: bool,
    pub(crate) runtime_state_updated: bool,
}

impl LocalThroughputState {
    fn write_stream_job_runtime_state_to_batch(
        &self,
        write_batch: &mut WriteBatch,
        state: &LocalStreamJobRuntimeState,
    ) -> Result<()> {
        self.write_batch_delete_cf_and_legacy(
            write_batch,
            STREAM_JOBS_CF,
            &legacy_stream_job_runtime_key(&state.handle_id),
        );
        self.write_batch_put_cf_bytes(
            write_batch,
            STREAM_JOBS_CF,
            &stream_job_runtime_key(&state.handle_id),
            encode_rocksdb_value(state, "stream job runtime state")?,
        );
        Ok(())
    }

    fn optimized_stream_job_mirror_supported(entry: &StreamsChangelogEntry) -> bool {
        matches!(
            entry.payload,
            StreamsChangelogPayload::StreamJobCheckpointReached { .. }
                | StreamsChangelogPayload::StreamJobWorkflowSignaled { .. }
        )
    }

    fn write_optimized_stream_job_mirrors(
        &self,
        write_batch: &mut WriteBatch,
        handle_id: &str,
        compact_checkpoint_mirrors: bool,
        write_mirrored_entry_markers: bool,
        completes_dispatch: bool,
        updated_at: DateTime<Utc>,
        mirrored_stream_entries: &[StreamsChangelogEntry],
    ) -> Result<bool> {
        let requires_runtime_state = completes_dispatch
            || mirrored_stream_entries.iter().any(|entry| {
                matches!(entry.payload, StreamsChangelogPayload::StreamJobCheckpointReached { .. })
            });
        let mut runtime_state = if requires_runtime_state {
            self.load_stream_job_runtime_state(handle_id)?
        } else {
            None
        };
        if requires_runtime_state && runtime_state.is_none() {
            anyhow::bail!("stream job runtime state missing for handle {handle_id}");
        }

        let mut runtime_state_updated = false;
        if completes_dispatch {
            let state = runtime_state.as_mut().expect("checked runtime state existence");
            state.dispatch_completed_at = Some(updated_at);
            state.updated_at = updated_at;
            runtime_state_updated = true;
        }

        for entry in mirrored_stream_entries {
            match &entry.payload {
                StreamsChangelogPayload::StreamJobCheckpointReached {
                    handle_id,
                    job_id,
                    checkpoint_name,
                    checkpoint_sequence,
                    stream_partition_id,
                    owner_epoch,
                    reached_at,
                } => {
                    let checkpoint_state = LocalStreamJobCheckpointState {
                        handle_id: handle_id.clone(),
                        job_id: job_id.clone(),
                        checkpoint_name: checkpoint_name.clone(),
                        checkpoint_sequence: *checkpoint_sequence,
                        stream_partition_id: *stream_partition_id,
                        stream_owner_epoch: *owner_epoch,
                        reached_at: *reached_at,
                        updated_at: *reached_at,
                    };
                    if !compact_checkpoint_mirrors {
                        self.write_batch_delete_cf_and_legacy(
                            write_batch,
                            STREAM_JOBS_CF,
                            &legacy_stream_job_checkpoint_key(
                                handle_id,
                                checkpoint_name,
                                *stream_partition_id,
                            ),
                        );
                        self.write_batch_put_cf_bytes(
                            write_batch,
                            STREAM_JOBS_CF,
                            &stream_job_checkpoint_key(
                                handle_id,
                                checkpoint_name,
                                *stream_partition_id,
                            ),
                            encode_rocksdb_value(&checkpoint_state, "stream job checkpoint state")?,
                        );
                    }

                    let state = runtime_state.as_mut().expect("checked runtime state existence");
                    state.job_id = job_id.clone();
                    state.checkpoint_name = checkpoint_name.clone();
                    state.checkpoint_sequence = *checkpoint_sequence;
                    state.stream_owner_epoch = *owner_epoch;
                    if let Some(cursor) = state
                        .source_cursors
                        .iter_mut()
                        .find(|cursor| cursor.source_partition_id == *stream_partition_id)
                    {
                        cursor.checkpoint_reached_at = Some(*reached_at);
                        cursor.updated_at = *reached_at;
                    }
                    state.latest_checkpoint_at = Some(*reached_at);
                    state.updated_at = *reached_at;
                    if compact_checkpoint_mirrors {
                        state.record_checkpoint_partition(checkpoint_state);
                    }
                    runtime_state_updated = true;
                }
                StreamsChangelogPayload::StreamJobWorkflowSignaled {
                    handle_id,
                    job_id,
                    operator_id,
                    view_name,
                    logical_key,
                    signal_id,
                    signal_type,
                    payload,
                    owner_epoch,
                    signaled_at,
                } => {
                    let signal_state = LocalStreamJobWorkflowSignalState {
                        handle_id: handle_id.clone(),
                        job_id: job_id.clone(),
                        operator_id: operator_id.clone(),
                        view_name: view_name.clone(),
                        logical_key: logical_key.clone(),
                        signal_id: signal_id.clone(),
                        signal_type: signal_type.clone(),
                        payload: payload.clone(),
                        stream_owner_epoch: *owner_epoch,
                        signaled_at: *signaled_at,
                        published_at: None,
                        updated_at: *signaled_at,
                    };
                    self.write_batch_delete_cf_and_legacy(
                        write_batch,
                        STREAM_JOBS_CF,
                        &legacy_stream_job_signal_key(handle_id, operator_id, logical_key),
                    );
                    self.write_batch_put_cf_bytes(
                        write_batch,
                        STREAM_JOBS_CF,
                        &stream_job_signal_key(handle_id, operator_id, logical_key),
                        encode_rocksdb_value(&signal_state, "stream job workflow signal state")?,
                    );
                }
                _ => unreachable!("optimized stream job mirror received unsupported entry"),
            }
            if write_mirrored_entry_markers {
                self.write_batch_put_cf(
                    write_batch,
                    META_CF,
                    &mirrored_entry_key(LocalChangelogPlane::Streams, entry.entry_id),
                    b"1".to_vec(),
                );
            }
        }

        if let Some(state) = runtime_state.as_ref()
            && runtime_state_updated
        {
            self.write_stream_job_runtime_state_to_batch(write_batch, state)?;
        }

        Ok(runtime_state_updated)
    }

    fn stream_job_view_overlay_key(
        handle_id: &str,
        view_name: &str,
        logical_key: &str,
    ) -> StreamJobViewOverlayKey {
        (handle_id.to_owned(), view_name.to_owned(), logical_key.to_owned())
    }

    pub(crate) fn overlay_stream_job_view_state(&self, state: LocalStreamJobViewState) {
        let key = Self::stream_job_view_overlay_key(
            &state.handle_id,
            &state.view_name,
            &state.logical_key,
        );
        let mut overlay =
            self.stream_job_view_overlay.lock().expect("stream job view overlay lock poisoned");
        overlay.insert(key, StreamJobViewOverlayEntry::Present(state));
    }

    pub(crate) fn overlay_stream_job_view_states(&self, states: &[LocalStreamJobViewState]) {
        if states.is_empty() {
            return;
        }
        let mut overlay =
            self.stream_job_view_overlay.lock().expect("stream job view overlay lock poisoned");
        overlay.reserve(states.len());
        for state in states {
            let key = Self::stream_job_view_overlay_key(
                &state.handle_id,
                &state.view_name,
                &state.logical_key,
            );
            overlay.insert(key, StreamJobViewOverlayEntry::Present(state.clone()));
        }
    }

    pub(crate) fn overlay_delete_stream_job_view_state(
        &self,
        handle_id: &str,
        view_name: &str,
        logical_key: &str,
    ) {
        let key = Self::stream_job_view_overlay_key(handle_id, view_name, logical_key);
        let mut overlay =
            self.stream_job_view_overlay.lock().expect("stream job view overlay lock poisoned");
        overlay.insert(key, StreamJobViewOverlayEntry::Deleted);
    }

    pub(crate) fn overlay_delete_stream_job_view_states(&self, views: &[(String, String, String)]) {
        if views.is_empty() {
            return;
        }
        let mut overlay =
            self.stream_job_view_overlay.lock().expect("stream job view overlay lock poisoned");
        for (handle_id, view_name, logical_key) in views {
            let key = Self::stream_job_view_overlay_key(handle_id, view_name, logical_key);
            overlay.insert(key, StreamJobViewOverlayEntry::Deleted);
        }
    }

    pub(crate) fn overlay_stream_job_view_lookup(
        &self,
        handle_id: &str,
        view_name: &str,
        logical_key: &str,
    ) -> Option<Option<LocalStreamJobViewState>> {
        let overlay =
            self.stream_job_view_overlay.lock().expect("stream job view overlay lock poisoned");
        match overlay.get(&Self::stream_job_view_overlay_key(handle_id, view_name, logical_key)) {
            Some(StreamJobViewOverlayEntry::Present(state)) => Some(Some(state.clone())),
            Some(StreamJobViewOverlayEntry::Deleted) => Some(None),
            None => None,
        }
    }

    pub(crate) fn overlay_all_stream_job_view_entries(
        &self,
    ) -> HashMap<StreamJobViewOverlayKey, StreamJobViewOverlayEntry> {
        self.stream_job_view_overlay.lock().expect("stream job view overlay lock poisoned").clone()
    }

    fn write_stream_job_state_batch(
        &self,
        write_batch: WriteBatch,
        context: &'static str,
    ) -> Result<()> {
        let mut options = WriteOptions::default();
        // Stream-job local state is replayable from the durable streams changelog and checkpoints.
        options.disable_wal(true);
        self.db.write_opt(write_batch, &options).context(context)?;
        Ok(())
    }

    fn write_stream_job_view_states_batch(
        &self,
        write_batch: &mut WriteBatch,
        states: &[LocalStreamJobViewState],
    ) -> Result<()> {
        for state in states {
            self.write_batch_put_cf_bytes(
                write_batch,
                STREAM_JOBS_CF,
                &stream_job_view_key(&state.handle_id, &state.view_name, &state.logical_key),
                encode_stream_job_view_state_value(state)?,
            );
        }
        Ok(())
    }

    pub(crate) fn upsert_stream_job_runtime_state(
        &self,
        state: &LocalStreamJobRuntimeState,
    ) -> Result<()> {
        let mut write_batch = WriteBatch::default();
        self.write_batch_delete_cf_and_legacy(
            &mut write_batch,
            STREAM_JOBS_CF,
            &legacy_stream_job_runtime_key(&state.handle_id),
        );
        self.write_batch_put_cf_bytes(
            &mut write_batch,
            STREAM_JOBS_CF,
            &stream_job_runtime_key(&state.handle_id),
            encode_rocksdb_value(state, "stream job runtime state")?,
        );
        self.write_stream_job_state_batch(
            write_batch,
            "failed to store stream job runtime state in column family stream_jobs",
        )
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

    pub(crate) fn replace_stream_job_source_cursors(
        &self,
        handle_id: &str,
        source_cursors: Vec<LocalStreamJobSourceCursorState>,
        updated_at: DateTime<Utc>,
    ) -> Result<()> {
        let Some(mut runtime_state) = self.load_stream_job_runtime_state(handle_id)? else {
            anyhow::bail!("stream job runtime state missing for handle {handle_id}");
        };
        runtime_state.source_cursors = source_cursors;
        runtime_state.updated_at = updated_at;
        self.upsert_stream_job_runtime_state(&runtime_state)
    }

    pub(crate) fn mark_stream_job_dispatch_batch_applied_outcome(
        &self,
        handle_id: &str,
        batch_id: &str,
        completes_dispatch: bool,
        updated_at: DateTime<Utc>,
    ) -> Result<StreamJobDispatchBatchAppliedOutcome> {
        if self.has_stream_job_applied_dispatch_batch(handle_id, batch_id)? {
            return Ok(StreamJobDispatchBatchAppliedOutcome {
                inserted: false,
                runtime_state_updated: false,
            });
        }
        let mut write_batch = WriteBatch::default();
        self.write_batch_put_cf_bytes(
            &mut write_batch,
            STREAM_JOBS_CF,
            &stream_job_dispatch_applied_key(handle_id, batch_id),
            Vec::new(),
        );
        let runtime_state_updated = if completes_dispatch {
            let Some(mut runtime_state) = self.load_stream_job_runtime_state(handle_id)? else {
                anyhow::bail!("stream job runtime state missing for handle {handle_id}");
            };
            runtime_state.dispatch_completed_at = Some(updated_at);
            runtime_state.updated_at = updated_at;
            self.write_batch_delete_cf_and_legacy(
                &mut write_batch,
                STREAM_JOBS_CF,
                &legacy_stream_job_runtime_key(&runtime_state.handle_id),
            );
            self.write_batch_put_cf_bytes(
                &mut write_batch,
                STREAM_JOBS_CF,
                &stream_job_runtime_key(&runtime_state.handle_id),
                encode_rocksdb_value(&runtime_state, "stream job runtime state")?,
            );
            true
        } else {
            false
        };
        self.write_stream_job_state_batch(
            write_batch,
            "failed to store stream job applied dispatch batch in column family stream_jobs",
        )?;
        Ok(StreamJobDispatchBatchAppliedOutcome { inserted: true, runtime_state_updated })
    }

    pub(crate) fn mark_stream_job_dispatch_batch_applied(
        &self,
        handle_id: &str,
        batch_id: &str,
        completes_dispatch: bool,
        updated_at: DateTime<Utc>,
    ) -> Result<bool> {
        Ok(self
            .mark_stream_job_dispatch_batch_applied_outcome(
                handle_id,
                batch_id,
                completes_dispatch,
                updated_at,
            )?
            .inserted)
    }

    pub(crate) fn persist_stream_job_batch_apply(
        &self,
        handle_id: &str,
        batch_id: &str,
        compact_checkpoint_mirrors: bool,
        completes_dispatch: bool,
        updated_at: DateTime<Utc>,
        owner_view_updates: Vec<LocalStreamJobViewState>,
        mirrored_stream_entries: &[StreamsChangelogEntry],
    ) -> Result<StreamJobDispatchBatchAppliedOutcome> {
        if self.has_stream_job_applied_dispatch_batch(handle_id, batch_id)? {
            return Ok(StreamJobDispatchBatchAppliedOutcome {
                inserted: false,
                runtime_state_updated: false,
            });
        }

        let mut write_batch = WriteBatch::default();
        self.write_batch_put_cf_bytes(
            &mut write_batch,
            STREAM_JOBS_CF,
            &stream_job_dispatch_applied_key(handle_id, batch_id),
            Vec::new(),
        );
        let runtime_state_updated =
            if mirrored_stream_entries.iter().all(Self::optimized_stream_job_mirror_supported) {
                self.write_optimized_stream_job_mirrors(
                    &mut write_batch,
                    handle_id,
                    compact_checkpoint_mirrors,
                    true,
                    completes_dispatch,
                    updated_at,
                    mirrored_stream_entries,
                )?
            } else {
                let runtime_state_updated = if completes_dispatch {
                    let Some(mut runtime_state) = self.load_stream_job_runtime_state(handle_id)?
                    else {
                        anyhow::bail!("stream job runtime state missing for handle {handle_id}");
                    };
                    runtime_state.dispatch_completed_at = Some(updated_at);
                    runtime_state.updated_at = updated_at;
                    self.write_stream_job_runtime_state_to_batch(&mut write_batch, &runtime_state)?;
                    true
                } else {
                    false
                };

                for entry in mirrored_stream_entries {
                    self.write_streams_entry_payload(&mut write_batch, entry)?;
                    self.write_batch_put_cf(
                        &mut write_batch,
                        META_CF,
                        &mirrored_entry_key(LocalChangelogPlane::Streams, entry.entry_id),
                        b"1".to_vec(),
                    );
                }
                runtime_state_updated
            };

        self.write_stream_job_state_batch(
            write_batch,
            "failed to persist stream job batch apply to local state",
        )?;
        let mut overlay =
            self.stream_job_view_overlay.lock().expect("stream job view overlay lock poisoned");
        overlay.reserve(owner_view_updates.len());
        for state in owner_view_updates {
            let key = Self::stream_job_view_overlay_key(
                &state.handle_id,
                &state.view_name,
                &state.logical_key,
            );
            overlay.insert(key, StreamJobViewOverlayEntry::Present(state));
        }
        Ok(StreamJobDispatchBatchAppliedOutcome { inserted: true, runtime_state_updated })
    }

    pub(crate) fn persist_stream_job_activation_apply(
        &self,
        handle_id: &str,
        batch_ids: &[String],
        compact_checkpoint_mirrors: bool,
        completes_dispatch: bool,
        updated_at: DateTime<Utc>,
        owner_view_updates: Vec<LocalStreamJobViewState>,
        mirrored_stream_entries: &[StreamsChangelogEntry],
    ) -> Result<StreamJobDispatchBatchAppliedOutcome> {
        let mut pending_batch_ids = Vec::with_capacity(batch_ids.len());
        for batch_id in batch_ids {
            if !self.has_stream_job_applied_dispatch_batch(handle_id, batch_id)? {
                pending_batch_ids.push(batch_id.clone());
            }
        }
        if pending_batch_ids.is_empty() {
            return Ok(StreamJobDispatchBatchAppliedOutcome {
                inserted: false,
                runtime_state_updated: false,
            });
        }

        let mut write_batch = WriteBatch::default();
        for batch_id in &pending_batch_ids {
            self.write_batch_put_cf_bytes(
                &mut write_batch,
                STREAM_JOBS_CF,
                &stream_job_dispatch_applied_key(handle_id, batch_id),
                Vec::new(),
            );
        }

        let runtime_state_updated =
            if mirrored_stream_entries.iter().all(Self::optimized_stream_job_mirror_supported) {
                self.write_optimized_stream_job_mirrors(
                    &mut write_batch,
                    handle_id,
                    compact_checkpoint_mirrors,
                    false,
                    completes_dispatch,
                    updated_at,
                    mirrored_stream_entries,
                )?
            } else {
                let runtime_state_updated = if completes_dispatch {
                    let Some(mut runtime_state) = self.load_stream_job_runtime_state(handle_id)?
                    else {
                        anyhow::bail!("stream job runtime state missing for handle {handle_id}");
                    };
                    runtime_state.dispatch_completed_at = Some(updated_at);
                    runtime_state.updated_at = updated_at;
                    self.write_stream_job_runtime_state_to_batch(&mut write_batch, &runtime_state)?;
                    true
                } else {
                    false
                };
                runtime_state_updated
            };

        self.write_stream_job_state_batch(
            write_batch,
            "failed to persist stream job activation apply to local state",
        )?;
        let mut overlay =
            self.stream_job_view_overlay.lock().expect("stream job view overlay lock poisoned");
        overlay.reserve(owner_view_updates.len());
        for state in owner_view_updates {
            let key = Self::stream_job_view_overlay_key(
                &state.handle_id,
                &state.view_name,
                &state.logical_key,
            );
            overlay.insert(key, StreamJobViewOverlayEntry::Present(state));
        }
        Ok(StreamJobDispatchBatchAppliedOutcome { inserted: true, runtime_state_updated })
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
        let mut write_batch = WriteBatch::default();
        self.write_batch_put_cf_bytes(
            &mut write_batch,
            STREAM_JOBS_CF,
            &stream_job_view_key(handle_id, view_name, logical_key),
            encode_stream_job_view_state_value(&state)?,
        );
        self.write_stream_job_state_batch(
            write_batch,
            "failed to store stream job view state in column family stream_jobs",
        )?;
        self.overlay_stream_job_view_state(state);
        Ok(())
    }

    pub(crate) fn upsert_stream_job_view_states(
        &self,
        states: &[LocalStreamJobViewState],
    ) -> Result<()> {
        if states.is_empty() {
            return Ok(());
        }
        let mut write_batch = WriteBatch::default();
        self.write_stream_job_view_states_batch(&mut write_batch, states)?;
        self.write_stream_job_state_batch(
            write_batch,
            "failed to store stream job view states in column family stream_jobs",
        )?;
        self.overlay_stream_job_view_states(states);
        Ok(())
    }

    pub(crate) fn delete_stream_job_view_state(
        &self,
        handle_id: &str,
        view_name: &str,
        logical_key: &str,
    ) -> Result<()> {
        let mut write_batch = WriteBatch::default();
        write_batch.delete_cf(
            self.cf(STREAM_JOBS_CF),
            &stream_job_view_key(handle_id, view_name, logical_key),
        );
        self.write_batch_delete_cf_and_legacy(
            &mut write_batch,
            STREAM_JOBS_CF,
            &legacy_stream_job_view_key(handle_id, view_name, logical_key),
        );
        self.write_stream_job_state_batch(
            write_batch,
            "failed to delete stream job view state from column family stream_jobs",
        )?;
        self.overlay_delete_stream_job_view_state(handle_id, view_name, logical_key);
        Ok(())
    }

    pub(crate) fn delete_stream_job_view_states(
        &self,
        views: &[(String, String, String)],
    ) -> Result<()> {
        if views.is_empty() {
            return Ok(());
        }
        let mut write_batch = WriteBatch::default();
        for (handle_id, view_name, logical_key) in views {
            self.write_batch_delete_cf_and_legacy(
                &mut write_batch,
                STREAM_JOBS_CF,
                &legacy_stream_job_view_key(handle_id, view_name, logical_key),
            );
            write_batch.delete_cf(
                self.cf(STREAM_JOBS_CF),
                &stream_job_view_key(handle_id, view_name, logical_key),
            );
        }
        self.write_stream_job_state_batch(
            write_batch,
            "failed to delete stream job view states from column family stream_jobs",
        )?;
        self.overlay_delete_stream_job_view_states(views);
        Ok(())
    }

    pub(crate) fn mark_stream_job_workflow_signal_published(
        &self,
        handle_id: &str,
        operator_id: &str,
        logical_key: &str,
        published_at: DateTime<Utc>,
    ) -> Result<()> {
        let Some(mut state) =
            self.load_stream_job_workflow_signal_state(handle_id, operator_id, logical_key)?
        else {
            return Ok(());
        };
        if state.published_at.is_some() {
            return Ok(());
        }
        state.published_at = Some(published_at);
        state.updated_at = published_at;
        let mut write_batch = WriteBatch::default();
        self.write_batch_delete_cf_and_legacy(
            &mut write_batch,
            STREAM_JOBS_CF,
            &legacy_stream_job_signal_key(handle_id, operator_id, logical_key),
        );
        self.write_batch_put_cf_bytes(
            &mut write_batch,
            STREAM_JOBS_CF,
            &stream_job_signal_key(handle_id, operator_id, logical_key),
            encode_rocksdb_value(&state, "stream job workflow signal state")?,
        );
        self.write_stream_job_state_batch(
            write_batch,
            "failed to mark stream job workflow signal published in column family stream_jobs",
        )
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
