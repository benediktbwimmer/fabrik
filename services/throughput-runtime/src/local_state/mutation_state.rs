use super::*;

impl LocalThroughputState {
    pub fn apply_changelog_entry(
        &self,
        partition_id: i32,
        offset: i64,
        entry: &ThroughputChangelogEntry,
    ) -> Result<bool> {
        let applied = self.apply_changelog_entry_for_plane(
            LocalChangelogPlane::Throughput,
            partition_id,
            offset,
            entry.entry_id,
            entry.occurred_at,
            |state, write_batch| state.write_entry_payload(write_batch, entry),
        )?;
        if applied {
            self.sync_stream_job_view_overlay_from_throughput_entry(entry);
        }
        Ok(applied)
    }

    pub fn apply_streams_changelog_entry(
        &self,
        partition_id: i32,
        offset: i64,
        entry: &StreamsChangelogEntry,
    ) -> Result<bool> {
        let applied = self.apply_changelog_entry_for_plane(
            LocalChangelogPlane::Streams,
            partition_id,
            offset,
            entry.entry_id,
            entry.occurred_at,
            |state, write_batch| state.write_streams_entry_payload(write_batch, entry),
        )?;
        if applied {
            self.sync_stream_job_view_overlay_from_streams_entry(entry);
        }
        Ok(applied)
    }

    pub fn mirror_changelog_entry(&self, entry: &ThroughputChangelogEntry) -> Result<()> {
        self.mirror_changelog_records(std::slice::from_ref(entry))
    }

    pub fn mirror_changelog_records(&self, entries: &[ThroughputChangelogEntry]) -> Result<()> {
        self.mirror_changelog_records_for_plane(
            LocalChangelogPlane::Throughput,
            entries.iter().map(|entry| {
                (entry.entry_id, |state: &LocalThroughputState, write_batch: &mut WriteBatch| {
                    state.write_entry_payload(write_batch, entry)
                })
            }),
        )?;
        for entry in entries {
            self.sync_stream_job_view_overlay_from_throughput_entry(entry);
        }
        Ok(())
    }

    pub fn mirror_streams_changelog_entry(&self, entry: &StreamsChangelogEntry) -> Result<()> {
        self.mirror_streams_changelog_records(std::slice::from_ref(entry))
    }

    pub fn mirror_streams_changelog_records(
        &self,
        entries: &[StreamsChangelogEntry],
    ) -> Result<()> {
        self.mirror_changelog_records_for_plane(
            LocalChangelogPlane::Streams,
            entries.iter().map(|entry| {
                (entry.entry_id, |state: &LocalThroughputState, write_batch: &mut WriteBatch| {
                    state.write_streams_entry_payload(write_batch, entry)
                })
            }),
        )?;
        for entry in entries {
            self.sync_stream_job_view_overlay_from_streams_entry(entry);
        }
        Ok(())
    }

    fn sync_stream_job_view_overlay_from_throughput_entry(&self, entry: &ThroughputChangelogEntry) {
        match &entry.payload {
            ThroughputChangelogPayload::StreamJobViewUpdated {
                handle_id,
                job_id,
                view_name,
                logical_key,
                output,
                checkpoint_sequence,
                updated_at,
            } => self.overlay_stream_job_view_state(LocalStreamJobViewState {
                handle_id: handle_id.clone(),
                job_id: job_id.clone(),
                view_name: view_name.clone(),
                logical_key: logical_key.clone(),
                output: output.clone(),
                checkpoint_sequence: *checkpoint_sequence,
                updated_at: *updated_at,
            }),
            ThroughputChangelogPayload::StreamJobViewEvicted {
                handle_id,
                view_name,
                logical_key,
                ..
            } => self.overlay_delete_stream_job_view_state(handle_id, view_name, logical_key),
            _ => {}
        }
    }

    fn sync_stream_job_view_overlay_from_streams_entry(&self, entry: &StreamsChangelogEntry) {
        match &entry.payload {
            StreamsChangelogPayload::StreamJobViewUpdated {
                handle_id,
                job_id,
                view_name,
                logical_key,
                output,
                checkpoint_sequence,
                updated_at,
            } => self.overlay_stream_job_view_state(LocalStreamJobViewState {
                handle_id: handle_id.clone(),
                job_id: job_id.clone(),
                view_name: view_name.clone(),
                logical_key: logical_key.clone(),
                output: output.clone(),
                checkpoint_sequence: *checkpoint_sequence,
                updated_at: *updated_at,
            }),
            StreamsChangelogPayload::StreamJobViewBatchUpdated {
                handle_id,
                job_id,
                updates,
                checkpoint_sequence,
                updated_at,
            } => self.overlay_stream_job_view_states(
                &updates
                    .iter()
                    .map(|update| LocalStreamJobViewState {
                        handle_id: handle_id.clone(),
                        job_id: job_id.clone(),
                        view_name: update.view_name.clone(),
                        logical_key: update.logical_key.clone(),
                        output: update.output.clone(),
                        checkpoint_sequence: *checkpoint_sequence,
                        updated_at: *updated_at,
                    })
                    .collect::<Vec<_>>(),
            ),
            StreamsChangelogPayload::StreamJobViewEvicted {
                handle_id,
                view_name,
                logical_key,
                ..
            } => self.overlay_delete_stream_job_view_state(handle_id, view_name, logical_key),
            _ => {}
        }
    }

    fn apply_changelog_entry_for_plane<F>(
        &self,
        plane: LocalChangelogPlane,
        partition_id: i32,
        offset: i64,
        entry_id: uuid::Uuid,
        occurred_at: DateTime<Utc>,
        write_payload: F,
    ) -> Result<bool>
    where
        F: FnOnce(&LocalThroughputState, &mut WriteBatch) -> Result<()>,
    {
        if self
            .last_applied_offset_for_plane(plane, partition_id)?
            .is_some_and(|current| current >= offset)
        {
            return Ok(false);
        }
        let mut write_batch = WriteBatch::default();
        if self.is_mirrored_entry_pending_for_plane(plane, entry_id)? {
            self.write_batch_delete_cf_and_legacy(
                &mut write_batch,
                META_CF,
                &mirrored_entry_key(plane, entry_id),
            );
        } else {
            write_payload(self, &mut write_batch)?;
        }
        self.write_batch_put_cf(
            &mut write_batch,
            META_CF,
            &offset_key(plane, partition_id),
            offset.to_string().into_bytes(),
        );
        self.db.write(write_batch).context(match plane {
            LocalChangelogPlane::Throughput => {
                "failed to apply throughput changelog entry to local state"
            }
            LocalChangelogPlane::Streams => {
                "failed to apply streams changelog entry to local state"
            }
        })?;
        let mut meta = self.meta.lock().expect("local throughput meta lock poisoned");
        meta.changelog_entries_applied = meta.changelog_entries_applied.saturating_add(1);
        meta.last_changelog_apply_at = Some(occurred_at);
        Ok(true)
    }

    fn mirror_changelog_records_for_plane<I, F>(
        &self,
        plane: LocalChangelogPlane,
        entries: I,
    ) -> Result<()>
    where
        I: IntoIterator<Item = (uuid::Uuid, F)>,
        F: FnOnce(&LocalThroughputState, &mut WriteBatch) -> Result<()>,
    {
        let entries = entries.into_iter().collect::<Vec<_>>();
        if entries.is_empty() {
            return Ok(());
        }
        let mut write_batch = WriteBatch::default();
        for (entry_id, write_payload) in entries {
            write_payload(self, &mut write_batch)?;
            self.write_batch_put_cf(
                &mut write_batch,
                META_CF,
                &mirrored_entry_key(plane, entry_id),
                b"1".to_vec(),
            );
        }
        self.db.write(write_batch).context(match plane {
            LocalChangelogPlane::Throughput => {
                "failed to mirror throughput changelog entries into local state"
            }
            LocalChangelogPlane::Streams => {
                "failed to mirror streams changelog entries into local state"
            }
        })?;
        Ok(())
    }

    #[cfg(test)]
    pub fn upsert_batch_record(&self, batch: &WorkflowBulkBatchRecord) -> Result<()> {
        let state = LocalBatchState {
            identity: ThroughputBatchIdentity {
                tenant_id: batch.tenant_id.clone(),
                instance_id: batch.instance_id.clone(),
                run_id: batch.run_id.clone(),
                batch_id: batch.batch_id.clone(),
            },
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
            terminal_chunk_count: batch.terminal_at.map(|_| batch.chunk_count).unwrap_or_default(),
            succeeded_items: batch.succeeded_items,
            failed_items: batch.failed_items,
            cancelled_items: batch.cancelled_items,
            status: batch.status.as_str().to_owned(),
            last_report_id: None,
            error: batch.error.clone(),
            reducer_output: batch.reducer_output.clone(),
            created_at: batch.scheduled_at,
            updated_at: batch.updated_at,
            terminal_at: batch.terminal_at,
        };
        self.db
            .put_cf(
                self.cf(BATCHES_CF),
                batch_key(&state.identity),
                encode_rocksdb_value(&state, "direct batch state")?,
            )
            .context("failed to store direct throughput batch state in column family batches")?;
        Ok(())
    }

    pub fn upsert_batch_with_chunks(
        &self,
        batch: &WorkflowBulkBatchRecord,
        chunks: &[WorkflowBulkChunkRecord],
    ) -> Result<()> {
        let strip_collect_results_output = batch.reducer.as_deref() == Some("collect_results");
        let chunk_states = chunks
            .iter()
            .map(|chunk| LocalChunkState {
                identity: ThroughputBatchIdentity {
                    tenant_id: chunk.tenant_id.clone(),
                    instance_id: chunk.instance_id.clone(),
                    run_id: chunk.run_id.clone(),
                    batch_id: chunk.batch_id.clone(),
                },
                chunk_id: chunk.chunk_id.clone(),
                activity_type: chunk.activity_type.clone(),
                task_queue: chunk.task_queue.clone(),
                chunk_index: chunk.chunk_index,
                group_id: chunk.group_id,
                item_count: chunk.item_count,
                attempt: chunk.attempt,
                max_attempts: chunk.max_attempts,
                retry_delay_ms: chunk.retry_delay_ms,
                lease_epoch: chunk.lease_epoch,
                owner_epoch: chunk.owner_epoch,
                status: chunk.status.as_str().to_owned(),
                worker_id: chunk.worker_id.clone(),
                lease_token: chunk.lease_token.map(|value| value.to_string()),
                report_id: chunk.last_report_id.clone(),
                result_handle: chunk.result_handle.clone(),
                output: if strip_collect_results_output { None } else { chunk.output.clone() },
                error: chunk.error.clone(),
                input_handle: chunk.input_handle.clone(),
                items: chunk.items.clone(),
                cancellation_requested: chunk.cancellation_requested,
                cancellation_reason: chunk.cancellation_reason.clone(),
                cancellation_metadata: chunk.cancellation_metadata.clone(),
                scheduled_at: chunk.scheduled_at,
                available_at: chunk.available_at,
                started_at: chunk.started_at,
                lease_expires_at: chunk.lease_expires_at,
                completed_at: chunk.completed_at,
                updated_at: chunk.updated_at,
            })
            .collect::<Vec<_>>();
        let batch_state = derive_batch_state_from_chunk_states(batch, &chunk_states);
        let mut write_batch = WriteBatch::default();
        self.write_batch_put_cf(
            &mut write_batch,
            BATCHES_CF,
            &batch_key(&batch_state.identity),
            encode_rocksdb_value(&batch_state, "batched direct throughput batch state")?,
        );
        for chunk_state in &chunk_states {
            let existing = self.load_chunk_state(&chunk_state.identity, &chunk_state.chunk_id)?;
            self.write_chunk_state(&mut write_batch, existing.as_ref(), chunk_state)?;
        }
        self.db
            .write(write_batch)
            .context("failed to batched-upsert direct throughput batch/chunk state")?;
        Ok(())
    }

    #[cfg(test)]
    pub fn upsert_chunk_record(&self, chunk: &WorkflowBulkChunkRecord) -> Result<()> {
        let identity = ThroughputBatchIdentity {
            tenant_id: chunk.tenant_id.clone(),
            instance_id: chunk.instance_id.clone(),
            run_id: chunk.run_id.clone(),
            batch_id: chunk.batch_id.clone(),
        };
        let strip_collect_results_output =
            self.load_batch_state(&identity)?.and_then(|batch| batch.reducer).as_deref()
                == Some("collect_results");
        let existing = self.load_chunk_state(&identity, &chunk.chunk_id)?;
        let state = LocalChunkState {
            identity,
            chunk_id: chunk.chunk_id.clone(),
            activity_type: chunk.activity_type.clone(),
            task_queue: chunk.task_queue.clone(),
            chunk_index: chunk.chunk_index,
            group_id: chunk.group_id,
            item_count: chunk.item_count,
            attempt: chunk.attempt,
            max_attempts: chunk.max_attempts,
            retry_delay_ms: chunk.retry_delay_ms,
            lease_epoch: chunk.lease_epoch,
            owner_epoch: chunk.owner_epoch,
            status: chunk.status.as_str().to_owned(),
            worker_id: chunk.worker_id.clone(),
            lease_token: chunk.lease_token.map(|value| value.to_string()),
            report_id: chunk.last_report_id.clone(),
            result_handle: chunk.result_handle.clone(),
            output: if strip_collect_results_output { None } else { chunk.output.clone() },
            error: chunk.error.clone(),
            input_handle: chunk.input_handle.clone(),
            items: chunk.items.clone(),
            cancellation_requested: chunk.cancellation_requested,
            cancellation_reason: chunk.cancellation_reason.clone(),
            cancellation_metadata: chunk.cancellation_metadata.clone(),
            scheduled_at: chunk.scheduled_at,
            available_at: chunk.available_at,
            started_at: chunk.started_at,
            lease_expires_at: chunk.lease_expires_at,
            completed_at: chunk.completed_at,
            updated_at: chunk.updated_at,
        };
        let mut write_batch = WriteBatch::default();
        self.write_chunk_state(&mut write_batch, existing.as_ref(), &state)?;
        self.db.write(write_batch).context("failed to upsert direct throughput chunk state")?;
        Ok(())
    }

    pub fn prune_terminal_state(&self, older_than: DateTime<Utc>) -> Result<u64> {
        let batches = self.load_all_batches()?;
        let chunks = self.load_all_chunks()?;
        let groups = self.load_all_groups()?;
        let mut write_batch = WriteBatch::default();
        let mut deleted = 0_u64;

        for batch in batches.into_iter().filter(|batch| {
            matches!(batch.status.as_str(), "completed" | "failed" | "cancelled")
                && batch.terminal_at.is_some_and(|terminal_at| terminal_at <= older_than)
        }) {
            self.write_batch_delete_cf_and_legacy(
                &mut write_batch,
                BATCHES_CF,
                &batch_key(&batch.identity),
            );
            deleted = deleted.saturating_add(1);

            for chunk in chunks.iter().filter(|chunk| chunk.identity == batch.identity) {
                self.delete_chunk_state(&mut write_batch, chunk)?;
                deleted = deleted.saturating_add(1);
            }

            for group in groups.iter().filter(|group| group.identity == batch.identity) {
                self.delete_group_state(&mut write_batch, group)?;
                deleted = deleted.saturating_add(1);
            }
        }

        if deleted > 0 {
            self.db.write(write_batch).context("failed to prune terminal throughput state")?;
        }
        Ok(deleted)
    }

    fn write_entry_payload(
        &self,
        write_batch: &mut WriteBatch,
        entry: &ThroughputChangelogEntry,
    ) -> Result<()> {
        match &entry.payload {
            ThroughputChangelogPayload::StreamJobExecutionPlanned {
                handle_id,
                job_id,
                job_name,
                view_name,
                checkpoint_name,
                checkpoint_sequence,
                input_item_count,
                materialized_key_count,
                active_partitions,
                throughput_partition_count,
                owner_epoch,
                planned_at,
            } => {
                let existing = self.load_stream_job_runtime_state(handle_id)?;
                let state = LocalStreamJobRuntimeState {
                    handle_id: handle_id.clone(),
                    job_id: job_id.clone(),
                    job_name: job_name.clone(),
                    view_name: view_name.clone(),
                    checkpoint_name: checkpoint_name.clone(),
                    checkpoint_sequence: *checkpoint_sequence,
                    input_item_count: *input_item_count,
                    materialized_key_count: *materialized_key_count,
                    active_partitions: active_partitions.clone(),
                    throughput_partition_count: existing
                        .as_ref()
                        .map(|state| state.throughput_partition_count)
                        .filter(|count| *count > 0)
                        .unwrap_or(*throughput_partition_count),
                    source_kind: existing.as_ref().and_then(|state| state.source_kind.clone()),
                    source_name: existing.as_ref().and_then(|state| state.source_name.clone()),
                    source_cursors: existing
                        .as_ref()
                        .map(|state| state.source_cursors.clone())
                        .unwrap_or_default(),
                    source_partition_leases: existing
                        .as_ref()
                        .map(|state| state.source_partition_leases.clone())
                        .unwrap_or_default(),
                    dispatch_batches: existing
                        .as_ref()
                        .map(|state| state.dispatch_batches.clone())
                        .unwrap_or_default(),
                    applied_dispatch_batch_ids: existing
                        .as_ref()
                        .map(|state| state.applied_dispatch_batch_ids.clone())
                        .unwrap_or_default(),
                    dispatch_completed_at: existing
                        .as_ref()
                        .and_then(|state| state.dispatch_completed_at),
                    dispatch_cancelled_at: existing
                        .as_ref()
                        .and_then(|state| state.dispatch_cancelled_at),
                    stream_owner_epoch: *owner_epoch,
                    planned_at: *planned_at,
                    latest_checkpoint_at: existing
                        .as_ref()
                        .and_then(|state| state.latest_checkpoint_at),
                    evicted_window_count: existing
                        .as_ref()
                        .map(|state| state.evicted_window_count)
                        .unwrap_or_default(),
                    last_evicted_window_end: existing
                        .as_ref()
                        .and_then(|state| state.last_evicted_window_end),
                    last_evicted_at: existing.as_ref().and_then(|state| state.last_evicted_at),
                    view_runtime_stats: existing
                        .as_ref()
                        .map(|state| state.view_runtime_stats.clone())
                        .unwrap_or_default(),
                    pre_key_runtime_stats: existing
                        .as_ref()
                        .map(|state| state.pre_key_runtime_stats.clone())
                        .unwrap_or_default(),
                    hot_key_runtime_stats: existing
                        .as_ref()
                        .map(|state| state.hot_key_runtime_stats.clone())
                        .unwrap_or_default(),
                    owner_partition_runtime_stats: existing
                        .as_ref()
                        .map(|state| state.owner_partition_runtime_stats.clone())
                        .unwrap_or_default(),
                    checkpoint_partitions: existing
                        .as_ref()
                        .map(|state| state.checkpoint_partitions.clone())
                        .unwrap_or_default(),
                    terminal_status: existing
                        .as_ref()
                        .and_then(|state| state.terminal_status.clone()),
                    terminal_output: existing
                        .as_ref()
                        .and_then(|state| state.terminal_output.clone()),
                    terminal_error: existing
                        .as_ref()
                        .and_then(|state| state.terminal_error.clone()),
                    terminal_at: existing.as_ref().and_then(|state| state.terminal_at),
                    updated_at: *planned_at,
                };
                self.write_batch_delete_cf_and_legacy(
                    write_batch,
                    STREAM_JOBS_CF,
                    &legacy_stream_job_runtime_key(handle_id),
                );
                self.write_batch_put_cf_bytes(
                    write_batch,
                    STREAM_JOBS_CF,
                    &stream_job_runtime_key(handle_id),
                    encode_rocksdb_value(&state, "stream job runtime state")?,
                );
            }
            ThroughputChangelogPayload::StreamJobViewUpdated {
                handle_id,
                job_id,
                view_name,
                logical_key,
                output,
                checkpoint_sequence,
                updated_at,
            } => {
                let state = LocalStreamJobViewState {
                    handle_id: handle_id.clone(),
                    job_id: job_id.clone(),
                    view_name: view_name.clone(),
                    logical_key: logical_key.clone(),
                    output: output.clone(),
                    checkpoint_sequence: *checkpoint_sequence,
                    updated_at: *updated_at,
                };
                self.write_batch_delete_cf_and_legacy(
                    write_batch,
                    STREAM_JOBS_CF,
                    &legacy_stream_job_view_key(handle_id, view_name, logical_key),
                );
                self.write_batch_put_cf_bytes(
                    write_batch,
                    STREAM_JOBS_CF,
                    &stream_job_view_key(handle_id, view_name, logical_key),
                    encode_stream_job_view_state_value(&state)?,
                );
            }
            ThroughputChangelogPayload::StreamJobViewEvicted {
                handle_id,
                window_end,
                evicted_window_count,
                last_evicted_window_end,
                last_evicted_at,
                evicted_at,
                view_name,
                logical_key,
                ..
            } => {
                self.write_batch_delete_cf_and_legacy(
                    write_batch,
                    STREAM_JOBS_CF,
                    &legacy_stream_job_view_key(handle_id, view_name, logical_key),
                );
                write_batch.delete_cf(
                    self.cf(STREAM_JOBS_CF),
                    &stream_job_view_key(handle_id, view_name, logical_key),
                );
                if let Some(mut runtime_state) = self.load_stream_job_runtime_state(handle_id)? {
                    runtime_state.evicted_window_count = *evicted_window_count;
                    runtime_state.last_evicted_window_end = *last_evicted_window_end;
                    runtime_state.last_evicted_at = *last_evicted_at;
                    runtime_state.record_view_eviction(view_name, *window_end, Some(*evicted_at));
                    runtime_state.updated_at = last_evicted_at.unwrap_or(runtime_state.updated_at);
                    self.write_batch_delete_cf_and_legacy(
                        write_batch,
                        STREAM_JOBS_CF,
                        &legacy_stream_job_runtime_key(handle_id),
                    );
                    self.write_batch_put_cf_bytes(
                        write_batch,
                        STREAM_JOBS_CF,
                        &stream_job_runtime_key(handle_id),
                        encode_rocksdb_value(&runtime_state, "stream job runtime state")?,
                    );
                }
            }
            ThroughputChangelogPayload::StreamJobCheckpointReached {
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
                    &stream_job_checkpoint_key(handle_id, checkpoint_name, *stream_partition_id),
                    encode_rocksdb_value(&checkpoint_state, "stream job checkpoint state")?,
                );
                if let Some(mut runtime_state) = self.load_stream_job_runtime_state(handle_id)? {
                    runtime_state.job_id = job_id.clone();
                    runtime_state.checkpoint_name = checkpoint_name.clone();
                    runtime_state.checkpoint_sequence = *checkpoint_sequence;
                    runtime_state.stream_owner_epoch = *owner_epoch;
                    if let Some(cursor) = runtime_state
                        .source_cursors
                        .iter_mut()
                        .find(|cursor| cursor.source_partition_id == *stream_partition_id)
                    {
                        cursor.checkpoint_reached_at = Some(*reached_at);
                        cursor.updated_at = *reached_at;
                    }
                    runtime_state.latest_checkpoint_at = Some(*reached_at);
                    runtime_state.updated_at = *reached_at;
                    self.write_batch_delete_cf_and_legacy(
                        write_batch,
                        STREAM_JOBS_CF,
                        &legacy_stream_job_runtime_key(handle_id),
                    );
                    self.write_batch_put_cf_bytes(
                        write_batch,
                        STREAM_JOBS_CF,
                        &stream_job_runtime_key(handle_id),
                        encode_rocksdb_value(&runtime_state, "stream job runtime state")?,
                    );
                }
            }
            ThroughputChangelogPayload::StreamJobWorkflowSignaled {
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
            ThroughputChangelogPayload::StreamJobSourceLeaseAssigned {
                handle_id,
                job_id,
                source_partition_id,
                owner_partition_id,
                owner_epoch,
                lease_token,
                assigned_at,
            } => {
                let Some(mut runtime_state) = self.load_stream_job_runtime_state(handle_id)? else {
                    anyhow::bail!(
                        "stream job source lease arrived before runtime state existed for handle {handle_id}"
                    );
                };
                runtime_state.job_id = job_id.clone();
                runtime_state.stream_owner_epoch = *owner_epoch;
                if let Some(existing) = runtime_state
                    .source_partition_leases
                    .iter_mut()
                    .find(|lease| lease.source_partition_id == *source_partition_id)
                {
                    existing.owner_partition_id = *owner_partition_id;
                    existing.owner_epoch = *owner_epoch;
                    existing.lease_token = lease_token.clone();
                    existing.updated_at = *assigned_at;
                } else {
                    runtime_state.source_partition_leases.push(
                        crate::local_state::LocalStreamJobSourceLeaseState {
                            source_partition_id: *source_partition_id,
                            owner_partition_id: *owner_partition_id,
                            owner_epoch: *owner_epoch,
                            lease_token: lease_token.clone(),
                            updated_at: *assigned_at,
                        },
                    );
                    runtime_state
                        .source_partition_leases
                        .sort_by_key(|lease| lease.source_partition_id);
                }
                runtime_state.updated_at = *assigned_at;
                self.write_batch_delete_cf_and_legacy(
                    write_batch,
                    STREAM_JOBS_CF,
                    &legacy_stream_job_runtime_key(handle_id),
                );
                self.write_batch_put_cf_bytes(
                    write_batch,
                    STREAM_JOBS_CF,
                    &stream_job_runtime_key(handle_id),
                    encode_rocksdb_value(&runtime_state, "stream job runtime state")?,
                );
            }
            ThroughputChangelogPayload::StreamJobSourceProgressed {
                handle_id,
                job_id,
                source_partition_id,
                next_offset,
                checkpoint_sequence,
                checkpoint_target_offset,
                last_applied_offset,
                last_high_watermark,
                last_event_time_watermark,
                last_closed_window_end,
                pending_window_ends,
                dropped_late_event_count,
                last_dropped_late_offset,
                last_dropped_late_event_at,
                last_dropped_late_window_end,
                dropped_evicted_window_event_count,
                last_dropped_evicted_window_offset,
                last_dropped_evicted_window_event_at,
                last_dropped_evicted_window_end,
                source_owner_partition_id,
                lease_token,
                owner_epoch,
                progressed_at,
            } => {
                let Some(mut runtime_state) = self.load_stream_job_runtime_state(handle_id)? else {
                    anyhow::bail!(
                        "stream job source progress arrived before runtime state existed for handle {handle_id}"
                    );
                };
                runtime_state.job_id = job_id.clone();
                runtime_state.stream_owner_epoch = *owner_epoch;
                runtime_state.checkpoint_sequence = *checkpoint_sequence;
                if let Some(existing) = runtime_state
                    .source_partition_leases
                    .iter_mut()
                    .find(|lease| lease.source_partition_id == *source_partition_id)
                {
                    existing.owner_partition_id = *source_owner_partition_id;
                    existing.owner_epoch = *owner_epoch;
                    existing.lease_token = lease_token.clone();
                    existing.updated_at = *progressed_at;
                } else {
                    runtime_state.source_partition_leases.push(
                        crate::local_state::LocalStreamJobSourceLeaseState {
                            source_partition_id: *source_partition_id,
                            owner_partition_id: *source_owner_partition_id,
                            owner_epoch: *owner_epoch,
                            lease_token: lease_token.clone(),
                            updated_at: *progressed_at,
                        },
                    );
                    runtime_state
                        .source_partition_leases
                        .sort_by_key(|lease| lease.source_partition_id);
                }
                if let Some(existing) = runtime_state
                    .source_cursors
                    .iter_mut()
                    .find(|cursor| cursor.source_partition_id == *source_partition_id)
                {
                    existing.next_offset = *next_offset;
                    existing.initial_checkpoint_target_offset = *checkpoint_target_offset;
                    existing.last_applied_offset = *last_applied_offset;
                    existing.last_high_watermark = *last_high_watermark;
                    existing.last_event_time_watermark = *last_event_time_watermark;
                    existing.last_closed_window_end = *last_closed_window_end;
                    existing.pending_window_ends = pending_window_ends.clone();
                    existing.dropped_late_event_count = *dropped_late_event_count;
                    existing.last_dropped_late_offset = *last_dropped_late_offset;
                    existing.last_dropped_late_event_at = *last_dropped_late_event_at;
                    existing.last_dropped_late_window_end = *last_dropped_late_window_end;
                    existing.dropped_evicted_window_event_count =
                        *dropped_evicted_window_event_count;
                    existing.last_dropped_evicted_window_offset =
                        *last_dropped_evicted_window_offset;
                    existing.last_dropped_evicted_window_event_at =
                        *last_dropped_evicted_window_event_at;
                    existing.last_dropped_evicted_window_end = *last_dropped_evicted_window_end;
                    existing.updated_at = *progressed_at;
                } else {
                    runtime_state.source_cursors.push(LocalStreamJobSourceCursorState {
                        source_partition_id: *source_partition_id,
                        next_offset: *next_offset,
                        initial_checkpoint_target_offset: *checkpoint_target_offset,
                        last_applied_offset: *last_applied_offset,
                        last_high_watermark: *last_high_watermark,
                        last_event_time_watermark: *last_event_time_watermark,
                        last_closed_window_end: *last_closed_window_end,
                        pending_window_ends: pending_window_ends.clone(),
                        dropped_late_event_count: *dropped_late_event_count,
                        last_dropped_late_offset: *last_dropped_late_offset,
                        last_dropped_late_event_at: *last_dropped_late_event_at,
                        last_dropped_late_window_end: *last_dropped_late_window_end,
                        dropped_evicted_window_event_count: *dropped_evicted_window_event_count,
                        last_dropped_evicted_window_offset: *last_dropped_evicted_window_offset,
                        last_dropped_evicted_window_event_at: *last_dropped_evicted_window_event_at,
                        last_dropped_evicted_window_end: *last_dropped_evicted_window_end,
                        checkpoint_reached_at: None,
                        updated_at: *progressed_at,
                    });
                    runtime_state.source_cursors.sort_by_key(|cursor| cursor.source_partition_id);
                }
                runtime_state.updated_at = *progressed_at;
                self.write_batch_delete_cf_and_legacy(
                    write_batch,
                    STREAM_JOBS_CF,
                    &legacy_stream_job_runtime_key(handle_id),
                );
                self.write_batch_put_cf_bytes(
                    write_batch,
                    STREAM_JOBS_CF,
                    &stream_job_runtime_key(handle_id),
                    encode_rocksdb_value(&runtime_state, "stream job runtime state")?,
                );
            }
            ThroughputChangelogPayload::StreamJobTerminalized {
                handle_id,
                job_id,
                owner_epoch,
                status,
                output,
                error,
                terminal_at,
            } => {
                let Some(mut runtime_state) = self.load_stream_job_runtime_state(handle_id)? else {
                    anyhow::bail!(
                        "stream job terminalized before runtime state existed for handle {handle_id}"
                    );
                };
                runtime_state.job_id = job_id.clone();
                runtime_state.stream_owner_epoch = *owner_epoch;
                runtime_state.terminal_status = Some(status.clone());
                runtime_state.terminal_output = output.clone();
                runtime_state.terminal_error = error.clone();
                runtime_state.terminal_at = Some(*terminal_at);
                runtime_state.updated_at = *terminal_at;
                self.write_batch_delete_cf_and_legacy(
                    write_batch,
                    STREAM_JOBS_CF,
                    &legacy_stream_job_runtime_key(handle_id),
                );
                self.write_batch_put_cf_bytes(
                    write_batch,
                    STREAM_JOBS_CF,
                    &stream_job_runtime_key(handle_id),
                    encode_rocksdb_value(&runtime_state, "stream job runtime state")?,
                );
            }
            ThroughputChangelogPayload::BatchCreated {
                identity,
                task_queue,
                execution_policy,
                reducer,
                reducer_class,
                aggregation_tree_depth,
                fast_lane_enabled,
                aggregation_group_count,
                total_items,
                chunk_count,
            } => {
                let mut state =
                    self.load_batch_state(identity)?.unwrap_or_else(|| LocalBatchState {
                        identity: identity.clone(),
                        definition_id: String::new(),
                        definition_version: None,
                        artifact_hash: None,
                        task_queue: String::new(),
                        activity_capabilities: None,
                        execution_policy: None,
                        reducer: None,
                        reducer_class: default_reducer_class(),
                        aggregation_tree_depth: default_aggregation_tree_depth(),
                        fast_lane_enabled: false,
                        aggregation_group_count: 1,
                        total_items: 0,
                        chunk_count: 0,
                        terminal_chunk_count: 0,
                        succeeded_items: 0,
                        failed_items: 0,
                        cancelled_items: 0,
                        status: "scheduled".to_owned(),
                        last_report_id: None,
                        error: None,
                        reducer_output: None,
                        created_at: entry.occurred_at,
                        updated_at: entry.occurred_at,
                        terminal_at: None,
                    });
                state.task_queue = task_queue.clone();
                state.execution_policy = execution_policy.clone();
                state.reducer = reducer.clone();
                state.reducer_class = reducer_class.clone();
                state.aggregation_tree_depth = (*aggregation_tree_depth).max(1);
                state.fast_lane_enabled = *fast_lane_enabled;
                state.aggregation_group_count = *aggregation_group_count;
                state.total_items = *total_items;
                state.chunk_count = *chunk_count;
                state.updated_at = entry.occurred_at;
                self.write_batch_put_cf(
                    write_batch,
                    BATCHES_CF,
                    &batch_key(identity),
                    encode_rocksdb_value(&state, "batch state")?,
                );
            }
            ThroughputChangelogPayload::ChunkLeased {
                identity,
                chunk_id,
                chunk_index,
                attempt,
                group_id,
                item_count,
                max_attempts,
                retry_delay_ms,
                lease_epoch,
                owner_epoch,
                worker_id,
                lease_token,
                lease_expires_at,
            } => {
                let existing_chunk = self.load_chunk_state(identity, chunk_id)?;
                let mut batch_state =
                    self.load_batch_state(identity)?.unwrap_or_else(|| LocalBatchState {
                        identity: identity.clone(),
                        definition_id: String::new(),
                        definition_version: None,
                        artifact_hash: None,
                        task_queue: String::new(),
                        activity_capabilities: None,
                        execution_policy: None,
                        reducer: None,
                        reducer_class: default_reducer_class(),
                        aggregation_tree_depth: default_aggregation_tree_depth(),
                        fast_lane_enabled: false,
                        aggregation_group_count: 1,
                        total_items: 0,
                        chunk_count: 0,
                        terminal_chunk_count: 0,
                        succeeded_items: 0,
                        failed_items: 0,
                        cancelled_items: 0,
                        status: "scheduled".to_owned(),
                        last_report_id: None,
                        error: None,
                        reducer_output: None,
                        created_at: entry.occurred_at,
                        updated_at: entry.occurred_at,
                        terminal_at: None,
                    });
                if batch_state.status == "scheduled" {
                    batch_state.status = "running".to_owned();
                }
                batch_state.updated_at = entry.occurred_at;
                self.write_batch_put_cf(
                    write_batch,
                    BATCHES_CF,
                    &batch_key(identity),
                    encode_rocksdb_value(&batch_state, "leased batch state")?,
                );

                let state = LocalChunkState {
                    identity: identity.clone(),
                    chunk_id: chunk_id.clone(),
                    activity_type: existing_chunk
                        .as_ref()
                        .map(|chunk| chunk.activity_type.clone())
                        .unwrap_or_default(),
                    task_queue: batch_state.task_queue.clone(),
                    chunk_index: *chunk_index,
                    group_id: *group_id,
                    item_count: *item_count,
                    attempt: *attempt,
                    max_attempts: *max_attempts,
                    retry_delay_ms: *retry_delay_ms,
                    lease_epoch: *lease_epoch,
                    owner_epoch: *owner_epoch,
                    status: "started".to_owned(),
                    worker_id: Some(worker_id.clone()),
                    lease_token: Some(lease_token.clone()),
                    report_id: None,
                    result_handle: existing_chunk
                        .as_ref()
                        .map(|chunk| chunk.result_handle.clone())
                        .unwrap_or(Value::Null),
                    output: existing_chunk.as_ref().and_then(|chunk| chunk.output.clone()),
                    error: existing_chunk.as_ref().and_then(|chunk| chunk.error.clone()),
                    input_handle: existing_chunk
                        .as_ref()
                        .map(|chunk| chunk.input_handle.clone())
                        .unwrap_or(Value::Null),
                    items: existing_chunk
                        .as_ref()
                        .map(|chunk| chunk.items.clone())
                        .unwrap_or_default(),
                    cancellation_requested: existing_chunk
                        .as_ref()
                        .map(|chunk| chunk.cancellation_requested)
                        .unwrap_or(false),
                    cancellation_reason: existing_chunk
                        .as_ref()
                        .and_then(|chunk| chunk.cancellation_reason.clone()),
                    cancellation_metadata: existing_chunk
                        .as_ref()
                        .and_then(|chunk| chunk.cancellation_metadata.clone()),
                    scheduled_at: existing_chunk
                        .as_ref()
                        .map(|chunk| chunk.scheduled_at)
                        .unwrap_or(entry.occurred_at),
                    available_at: entry.occurred_at,
                    started_at: existing_chunk
                        .as_ref()
                        .and_then(|chunk| chunk.started_at)
                        .or(Some(entry.occurred_at)),
                    lease_expires_at: Some(*lease_expires_at),
                    completed_at: None,
                    updated_at: entry.occurred_at,
                };
                self.write_chunk_state(write_batch, existing_chunk.as_ref(), &state)?;
            }
            ThroughputChangelogPayload::ChunkApplied {
                identity,
                chunk_id,
                chunk_index,
                attempt,
                group_id,
                item_count,
                max_attempts,
                retry_delay_ms,
                lease_epoch,
                owner_epoch,
                report_id,
                status,
                available_at,
                result_handle,
                output,
                error,
                cancellation_reason,
                cancellation_metadata,
            } => {
                let mut batch_state =
                    self.load_batch_state(identity)?.unwrap_or_else(|| LocalBatchState {
                        identity: identity.clone(),
                        definition_id: String::new(),
                        definition_version: None,
                        artifact_hash: None,
                        task_queue: String::new(),
                        activity_capabilities: None,
                        execution_policy: None,
                        reducer: None,
                        reducer_class: default_reducer_class(),
                        aggregation_tree_depth: default_aggregation_tree_depth(),
                        fast_lane_enabled: false,
                        aggregation_group_count: 1,
                        total_items: 0,
                        chunk_count: 0,
                        terminal_chunk_count: 0,
                        succeeded_items: 0,
                        failed_items: 0,
                        cancelled_items: 0,
                        status: "running".to_owned(),
                        last_report_id: None,
                        error: None,
                        reducer_output: None,
                        created_at: entry.occurred_at,
                        updated_at: entry.occurred_at,
                        terminal_at: None,
                    });
                let existing_chunk = self.load_chunk_state(identity, chunk_id)?;
                let task_queue = existing_chunk
                    .as_ref()
                    .map(|chunk| chunk.task_queue.clone())
                    .or_else(|| Some(batch_state.task_queue.clone()))
                    .unwrap_or_default();
                let strip_collect_results_output =
                    batch_state.reducer.as_deref() == Some("collect_results");
                let previous_status = existing_chunk
                    .as_ref()
                    .map(|chunk| chunk.status.as_str())
                    .unwrap_or("scheduled");
                let state = LocalChunkState {
                    identity: identity.clone(),
                    chunk_id: chunk_id.clone(),
                    activity_type: existing_chunk
                        .as_ref()
                        .map(|chunk| chunk.activity_type.clone())
                        .unwrap_or_default(),
                    task_queue,
                    chunk_index: *chunk_index,
                    group_id: *group_id,
                    item_count: *item_count,
                    attempt: *attempt,
                    max_attempts: *max_attempts,
                    retry_delay_ms: *retry_delay_ms,
                    lease_epoch: *lease_epoch,
                    owner_epoch: *owner_epoch,
                    status: status.clone(),
                    worker_id: None,
                    lease_token: None,
                    report_id: Some(report_id.clone()),
                    result_handle: result_handle.clone().unwrap_or(Value::Null),
                    output: if strip_collect_results_output { None } else { output.clone() },
                    error: error.clone(),
                    input_handle: existing_chunk
                        .as_ref()
                        .map(|chunk| chunk.input_handle.clone())
                        .unwrap_or(Value::Null),
                    items: existing_chunk
                        .as_ref()
                        .map(|chunk| chunk.items.clone())
                        .unwrap_or_default(),
                    cancellation_requested: existing_chunk
                        .as_ref()
                        .map(|chunk| chunk.cancellation_requested)
                        .unwrap_or(false),
                    cancellation_reason: cancellation_reason.clone(),
                    cancellation_metadata: cancellation_metadata.clone(),
                    scheduled_at: existing_chunk
                        .as_ref()
                        .map(|chunk| chunk.scheduled_at)
                        .unwrap_or(entry.occurred_at),
                    available_at: *available_at,
                    started_at: existing_chunk.as_ref().and_then(|chunk| chunk.started_at),
                    lease_expires_at: None,
                    completed_at: matches!(status.as_str(), "completed" | "failed" | "cancelled")
                        .then_some(entry.occurred_at),
                    updated_at: entry.occurred_at,
                };
                self.write_chunk_state(write_batch, existing_chunk.as_ref(), &state)?;
                if previous_status == WorkflowBulkChunkStatus::Started.as_str() {
                    match status.as_str() {
                        "completed" => {
                            batch_state.succeeded_items =
                                batch_state.succeeded_items.saturating_add(*item_count);
                            batch_state.terminal_chunk_count =
                                batch_state.terminal_chunk_count.saturating_add(1);
                        }
                        "failed" => {
                            batch_state.failed_items =
                                batch_state.failed_items.saturating_add(*item_count);
                            batch_state.terminal_chunk_count =
                                batch_state.terminal_chunk_count.saturating_add(1);
                            batch_state.error = error.clone();
                        }
                        "cancelled" => {
                            batch_state.cancelled_items =
                                batch_state.cancelled_items.saturating_add(*item_count);
                            batch_state.terminal_chunk_count =
                                batch_state.terminal_chunk_count.saturating_add(1);
                            batch_state.error = error.clone();
                        }
                        _ => {}
                    }
                }
                if batch_state.status == "scheduled" {
                    batch_state.status = "running".to_owned();
                }
                batch_state.updated_at = entry.occurred_at;
                if matches!(status.as_str(), "completed" | "scheduled") && error.is_none() {
                    if !matches!(batch_state.status.as_str(), "failed" | "cancelled") {
                        batch_state.error = None;
                    }
                }
                if let Some(terminal_status) =
                    infer_ungrouped_batch_terminal_from_chunk_apply(&batch_state, status)
                {
                    batch_state.status = terminal_status.to_owned();
                    batch_state.last_report_id = Some(report_id.clone());
                    batch_state.terminal_chunk_count = batch_state.chunk_count;
                    batch_state.updated_at = entry.occurred_at;
                    batch_state.terminal_at = Some(entry.occurred_at);
                    if terminal_status == WorkflowBulkBatchStatus::Completed.as_str() {
                        batch_state.error = None;
                    }
                }
                self.write_batch_put_cf(
                    write_batch,
                    BATCHES_CF,
                    &batch_key(identity),
                    encode_rocksdb_value(&batch_state, "touched batch state")?,
                );
            }
            ThroughputChangelogPayload::ChunkRequeued {
                identity,
                chunk_id,
                chunk_index,
                attempt,
                group_id,
                item_count,
                max_attempts,
                retry_delay_ms,
                lease_epoch,
                owner_epoch,
                available_at,
            } => {
                let existing_chunk = self.load_chunk_state(identity, chunk_id)?;
                let task_queue = existing_chunk
                    .as_ref()
                    .map(|chunk| chunk.task_queue.clone())
                    .or_else(|| {
                        self.load_batch_state(identity).ok().flatten().map(|batch| batch.task_queue)
                    })
                    .unwrap_or_default();
                let state = LocalChunkState {
                    identity: identity.clone(),
                    chunk_id: chunk_id.clone(),
                    activity_type: existing_chunk
                        .as_ref()
                        .map(|chunk| chunk.activity_type.clone())
                        .unwrap_or_default(),
                    task_queue,
                    chunk_index: *chunk_index,
                    group_id: *group_id,
                    item_count: *item_count,
                    attempt: *attempt,
                    max_attempts: *max_attempts,
                    retry_delay_ms: *retry_delay_ms,
                    lease_epoch: *lease_epoch,
                    owner_epoch: *owner_epoch,
                    status: WorkflowBulkChunkStatus::Scheduled.as_str().to_owned(),
                    worker_id: None,
                    lease_token: None,
                    report_id: existing_chunk.as_ref().and_then(|chunk| chunk.report_id.clone()),
                    result_handle: Value::Null,
                    output: None,
                    error: existing_chunk.as_ref().and_then(|chunk| chunk.error.clone()),
                    input_handle: existing_chunk
                        .as_ref()
                        .map(|chunk| chunk.input_handle.clone())
                        .unwrap_or(Value::Null),
                    items: existing_chunk
                        .as_ref()
                        .map(|chunk| chunk.items.clone())
                        .unwrap_or_default(),
                    cancellation_requested: existing_chunk
                        .as_ref()
                        .map(|chunk| chunk.cancellation_requested)
                        .unwrap_or(false),
                    cancellation_reason: existing_chunk
                        .as_ref()
                        .and_then(|chunk| chunk.cancellation_reason.clone()),
                    cancellation_metadata: existing_chunk
                        .as_ref()
                        .and_then(|chunk| chunk.cancellation_metadata.clone()),
                    scheduled_at: existing_chunk
                        .as_ref()
                        .map(|chunk| chunk.scheduled_at)
                        .unwrap_or(entry.occurred_at),
                    available_at: *available_at,
                    started_at: existing_chunk.as_ref().and_then(|chunk| chunk.started_at),
                    lease_expires_at: None,
                    completed_at: None,
                    updated_at: entry.occurred_at,
                };
                self.write_chunk_state(write_batch, existing_chunk.as_ref(), &state)?;
                let mut batch_state =
                    self.load_batch_state(identity)?.unwrap_or_else(|| LocalBatchState {
                        identity: identity.clone(),
                        definition_id: String::new(),
                        definition_version: None,
                        artifact_hash: None,
                        task_queue: String::new(),
                        activity_capabilities: None,
                        execution_policy: None,
                        reducer: None,
                        reducer_class: default_reducer_class(),
                        aggregation_tree_depth: default_aggregation_tree_depth(),
                        fast_lane_enabled: false,
                        aggregation_group_count: 1,
                        total_items: 0,
                        chunk_count: 0,
                        terminal_chunk_count: 0,
                        succeeded_items: 0,
                        failed_items: 0,
                        cancelled_items: 0,
                        status: "running".to_owned(),
                        last_report_id: None,
                        error: None,
                        reducer_output: None,
                        created_at: entry.occurred_at,
                        updated_at: entry.occurred_at,
                        terminal_at: None,
                    });
                if batch_state.status == "scheduled" {
                    batch_state.status = "running".to_owned();
                }
                batch_state.updated_at = entry.occurred_at;
                self.write_batch_put_cf(
                    write_batch,
                    BATCHES_CF,
                    &batch_key(identity),
                    encode_rocksdb_value(&batch_state, "requeue-touched batch state")?,
                );
            }
            ThroughputChangelogPayload::BatchTerminal {
                identity,
                status,
                report_id,
                succeeded_items,
                failed_items,
                cancelled_items,
                error,
                terminal_at,
            } => {
                let mut batch_state =
                    self.load_batch_state(identity)?.unwrap_or_else(|| LocalBatchState {
                        identity: identity.clone(),
                        definition_id: String::new(),
                        definition_version: None,
                        artifact_hash: None,
                        task_queue: String::new(),
                        activity_capabilities: None,
                        execution_policy: None,
                        reducer: None,
                        reducer_class: default_reducer_class(),
                        aggregation_tree_depth: default_aggregation_tree_depth(),
                        fast_lane_enabled: false,
                        aggregation_group_count: 1,
                        total_items: succeeded_items
                            .saturating_add(*failed_items)
                            .saturating_add(*cancelled_items),
                        chunk_count: 0,
                        terminal_chunk_count: 0,
                        succeeded_items: 0,
                        failed_items: 0,
                        cancelled_items: 0,
                        status: status.clone(),
                        last_report_id: Some(report_id.clone()),
                        error: error.clone(),
                        reducer_output: None,
                        created_at: entry.occurred_at,
                        updated_at: entry.occurred_at,
                        terminal_at: Some(*terminal_at),
                    });
                batch_state.status = status.clone();
                batch_state.last_report_id = Some(report_id.clone());
                batch_state.succeeded_items = *succeeded_items;
                batch_state.failed_items = *failed_items;
                batch_state.cancelled_items = *cancelled_items;
                batch_state.terminal_chunk_count = batch_state.chunk_count;
                batch_state.error = error.clone();
                batch_state.updated_at = entry.occurred_at;
                batch_state.terminal_at = Some(*terminal_at);
                self.write_batch_put_cf(
                    write_batch,
                    BATCHES_CF,
                    &batch_key(identity),
                    encode_rocksdb_value(&batch_state, "terminal batch state")?,
                );
            }
            ThroughputChangelogPayload::GroupTerminal {
                identity,
                group_id,
                level,
                parent_group_id,
                status,
                succeeded_items,
                failed_items,
                cancelled_items,
                error,
                terminal_at,
            } => {
                let state = LocalGroupState {
                    identity: identity.clone(),
                    group_id: *group_id,
                    level: *level,
                    parent_group_id: *parent_group_id,
                    status: status.clone(),
                    succeeded_items: *succeeded_items,
                    failed_items: *failed_items,
                    cancelled_items: *cancelled_items,
                    error: error.clone(),
                    terminal_at: *terminal_at,
                };
                self.write_group_state(write_batch, &state)?;
            }
        }
        Ok(())
    }

    pub(crate) fn write_streams_entry_payload(
        &self,
        write_batch: &mut WriteBatch,
        entry: &StreamsChangelogEntry,
    ) -> Result<()> {
        match &entry.payload {
            StreamsChangelogPayload::StreamJobExecutionPlanned {
                handle_id,
                job_id,
                job_name,
                view_name,
                checkpoint_name,
                checkpoint_sequence,
                input_item_count,
                materialized_key_count,
                active_partitions,
                throughput_partition_count,
                owner_epoch,
                planned_at,
            } => {
                let existing = self.load_stream_job_runtime_state(handle_id)?;
                let state = LocalStreamJobRuntimeState {
                    handle_id: handle_id.clone(),
                    job_id: job_id.clone(),
                    job_name: job_name.clone(),
                    view_name: view_name.clone(),
                    checkpoint_name: checkpoint_name.clone(),
                    checkpoint_sequence: *checkpoint_sequence,
                    input_item_count: *input_item_count,
                    materialized_key_count: *materialized_key_count,
                    active_partitions: active_partitions.clone(),
                    throughput_partition_count: existing
                        .as_ref()
                        .map(|state| state.throughput_partition_count)
                        .filter(|count| *count > 0)
                        .unwrap_or(*throughput_partition_count),
                    source_kind: existing.as_ref().and_then(|state| state.source_kind.clone()),
                    source_name: existing.as_ref().and_then(|state| state.source_name.clone()),
                    source_cursors: existing
                        .as_ref()
                        .map(|state| state.source_cursors.clone())
                        .unwrap_or_default(),
                    source_partition_leases: existing
                        .as_ref()
                        .map(|state| state.source_partition_leases.clone())
                        .unwrap_or_default(),
                    dispatch_batches: existing
                        .as_ref()
                        .map(|state| state.dispatch_batches.clone())
                        .unwrap_or_default(),
                    applied_dispatch_batch_ids: existing
                        .as_ref()
                        .map(|state| state.applied_dispatch_batch_ids.clone())
                        .unwrap_or_default(),
                    dispatch_completed_at: existing
                        .as_ref()
                        .and_then(|state| state.dispatch_completed_at),
                    dispatch_cancelled_at: existing
                        .as_ref()
                        .and_then(|state| state.dispatch_cancelled_at),
                    stream_owner_epoch: *owner_epoch,
                    planned_at: *planned_at,
                    latest_checkpoint_at: existing
                        .as_ref()
                        .and_then(|state| state.latest_checkpoint_at),
                    evicted_window_count: existing
                        .as_ref()
                        .map(|state| state.evicted_window_count)
                        .unwrap_or_default(),
                    last_evicted_window_end: existing
                        .as_ref()
                        .and_then(|state| state.last_evicted_window_end),
                    last_evicted_at: existing.as_ref().and_then(|state| state.last_evicted_at),
                    view_runtime_stats: existing
                        .as_ref()
                        .map(|state| state.view_runtime_stats.clone())
                        .unwrap_or_default(),
                    pre_key_runtime_stats: existing
                        .as_ref()
                        .map(|state| state.pre_key_runtime_stats.clone())
                        .unwrap_or_default(),
                    hot_key_runtime_stats: existing
                        .as_ref()
                        .map(|state| state.hot_key_runtime_stats.clone())
                        .unwrap_or_default(),
                    owner_partition_runtime_stats: existing
                        .as_ref()
                        .map(|state| state.owner_partition_runtime_stats.clone())
                        .unwrap_or_default(),
                    checkpoint_partitions: existing
                        .as_ref()
                        .map(|state| state.checkpoint_partitions.clone())
                        .unwrap_or_default(),
                    terminal_status: existing
                        .as_ref()
                        .and_then(|state| state.terminal_status.clone()),
                    terminal_output: existing
                        .as_ref()
                        .and_then(|state| state.terminal_output.clone()),
                    terminal_error: existing
                        .as_ref()
                        .and_then(|state| state.terminal_error.clone()),
                    terminal_at: existing.as_ref().and_then(|state| state.terminal_at),
                    updated_at: *planned_at,
                };
                self.write_batch_delete_cf_and_legacy(
                    write_batch,
                    STREAM_JOBS_CF,
                    &legacy_stream_job_runtime_key(handle_id),
                );
                self.write_batch_put_cf_bytes(
                    write_batch,
                    STREAM_JOBS_CF,
                    &stream_job_runtime_key(handle_id),
                    encode_rocksdb_value(&state, "stream job runtime state")?,
                );
            }
            StreamsChangelogPayload::StreamJobViewUpdated {
                handle_id,
                job_id,
                view_name,
                logical_key,
                output,
                checkpoint_sequence,
                updated_at,
            } => {
                let state = LocalStreamJobViewState {
                    handle_id: handle_id.clone(),
                    job_id: job_id.clone(),
                    view_name: view_name.clone(),
                    logical_key: logical_key.clone(),
                    output: output.clone(),
                    checkpoint_sequence: *checkpoint_sequence,
                    updated_at: *updated_at,
                };
                self.write_batch_put_cf_bytes(
                    write_batch,
                    STREAM_JOBS_CF,
                    &stream_job_view_key(handle_id, view_name, logical_key),
                    encode_stream_job_view_state_value(&state)?,
                );
            }
            StreamsChangelogPayload::StreamJobViewBatchUpdated {
                handle_id,
                job_id,
                updates,
                checkpoint_sequence,
                updated_at,
            } => {
                for update in updates {
                    let state = LocalStreamJobViewState {
                        handle_id: handle_id.clone(),
                        job_id: job_id.clone(),
                        view_name: update.view_name.clone(),
                        logical_key: update.logical_key.clone(),
                        output: update.output.clone(),
                        checkpoint_sequence: *checkpoint_sequence,
                        updated_at: *updated_at,
                    };
                    self.write_batch_put_cf_bytes(
                        write_batch,
                        STREAM_JOBS_CF,
                        &stream_job_view_key(handle_id, &update.view_name, &update.logical_key),
                        encode_stream_job_view_state_value(&state)?,
                    );
                }
            }
            StreamsChangelogPayload::StreamJobViewEvicted {
                handle_id,
                window_end,
                evicted_window_count,
                last_evicted_window_end,
                last_evicted_at,
                evicted_at,
                view_name,
                logical_key,
                ..
            } => {
                self.write_batch_delete_cf_and_legacy(
                    write_batch,
                    STREAM_JOBS_CF,
                    &legacy_stream_job_view_key(handle_id, view_name, logical_key),
                );
                write_batch.delete_cf(
                    self.cf(STREAM_JOBS_CF),
                    &stream_job_view_key(handle_id, view_name, logical_key),
                );
                if let Some(mut runtime_state) = self.load_stream_job_runtime_state(handle_id)? {
                    runtime_state.evicted_window_count = *evicted_window_count;
                    runtime_state.last_evicted_window_end = *last_evicted_window_end;
                    runtime_state.last_evicted_at = *last_evicted_at;
                    runtime_state.record_view_eviction(view_name, *window_end, Some(*evicted_at));
                    runtime_state.updated_at = last_evicted_at.unwrap_or(runtime_state.updated_at);
                    self.write_batch_delete_cf_and_legacy(
                        write_batch,
                        STREAM_JOBS_CF,
                        &legacy_stream_job_runtime_key(handle_id),
                    );
                    self.write_batch_put_cf_bytes(
                        write_batch,
                        STREAM_JOBS_CF,
                        &stream_job_runtime_key(handle_id),
                        encode_rocksdb_value(&runtime_state, "stream job runtime state")?,
                    );
                }
            }
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
                    &stream_job_checkpoint_key(handle_id, checkpoint_name, *stream_partition_id),
                    encode_rocksdb_value(&checkpoint_state, "stream job checkpoint state")?,
                );
                if let Some(mut runtime_state) = self.load_stream_job_runtime_state(handle_id)? {
                    runtime_state.job_id = job_id.clone();
                    runtime_state.checkpoint_name = checkpoint_name.clone();
                    runtime_state.checkpoint_sequence = *checkpoint_sequence;
                    runtime_state.stream_owner_epoch = *owner_epoch;
                    if let Some(cursor) = runtime_state
                        .source_cursors
                        .iter_mut()
                        .find(|cursor| cursor.source_partition_id == *stream_partition_id)
                    {
                        cursor.checkpoint_reached_at = Some(*reached_at);
                        cursor.updated_at = *reached_at;
                    }
                    runtime_state.latest_checkpoint_at = Some(*reached_at);
                    runtime_state.updated_at = *reached_at;
                    self.write_batch_delete_cf_and_legacy(
                        write_batch,
                        STREAM_JOBS_CF,
                        &legacy_stream_job_runtime_key(handle_id),
                    );
                    self.write_batch_put_cf_bytes(
                        write_batch,
                        STREAM_JOBS_CF,
                        &stream_job_runtime_key(handle_id),
                        encode_rocksdb_value(&runtime_state, "stream job runtime state")?,
                    );
                }
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
            StreamsChangelogPayload::StreamJobSourceLeaseAssigned {
                handle_id,
                job_id,
                source_partition_id,
                owner_partition_id,
                owner_epoch,
                lease_token,
                assigned_at,
            } => {
                let Some(mut runtime_state) = self.load_stream_job_runtime_state(handle_id)? else {
                    anyhow::bail!(
                        "stream job source lease arrived before runtime state existed for handle {handle_id}"
                    );
                };
                runtime_state.job_id = job_id.clone();
                runtime_state.stream_owner_epoch = *owner_epoch;
                if let Some(existing) = runtime_state
                    .source_partition_leases
                    .iter_mut()
                    .find(|lease| lease.source_partition_id == *source_partition_id)
                {
                    existing.owner_partition_id = *owner_partition_id;
                    existing.owner_epoch = *owner_epoch;
                    existing.lease_token = lease_token.clone();
                    existing.updated_at = *assigned_at;
                } else {
                    runtime_state.source_partition_leases.push(
                        crate::local_state::LocalStreamJobSourceLeaseState {
                            source_partition_id: *source_partition_id,
                            owner_partition_id: *owner_partition_id,
                            owner_epoch: *owner_epoch,
                            lease_token: lease_token.clone(),
                            updated_at: *assigned_at,
                        },
                    );
                    runtime_state
                        .source_partition_leases
                        .sort_by_key(|lease| lease.source_partition_id);
                }
                runtime_state.updated_at = *assigned_at;
                self.write_batch_delete_cf_and_legacy(
                    write_batch,
                    STREAM_JOBS_CF,
                    &legacy_stream_job_runtime_key(handle_id),
                );
                self.write_batch_put_cf_bytes(
                    write_batch,
                    STREAM_JOBS_CF,
                    &stream_job_runtime_key(handle_id),
                    encode_rocksdb_value(&runtime_state, "stream job runtime state")?,
                );
            }
            StreamsChangelogPayload::StreamJobSourceProgressed {
                handle_id,
                job_id,
                source_partition_id,
                next_offset,
                checkpoint_sequence,
                checkpoint_target_offset,
                last_applied_offset,
                last_high_watermark,
                last_event_time_watermark,
                last_closed_window_end,
                pending_window_ends,
                dropped_late_event_count,
                last_dropped_late_offset,
                last_dropped_late_event_at,
                last_dropped_late_window_end,
                dropped_evicted_window_event_count,
                last_dropped_evicted_window_offset,
                last_dropped_evicted_window_event_at,
                last_dropped_evicted_window_end,
                source_owner_partition_id,
                lease_token,
                owner_epoch,
                progressed_at,
            } => {
                let Some(mut runtime_state) = self.load_stream_job_runtime_state(handle_id)? else {
                    anyhow::bail!(
                        "stream job source progress arrived before runtime state existed for handle {handle_id}"
                    );
                };
                runtime_state.job_id = job_id.clone();
                runtime_state.stream_owner_epoch = *owner_epoch;
                runtime_state.checkpoint_sequence = *checkpoint_sequence;
                if let Some(existing) = runtime_state
                    .source_partition_leases
                    .iter_mut()
                    .find(|lease| lease.source_partition_id == *source_partition_id)
                {
                    existing.owner_partition_id = *source_owner_partition_id;
                    existing.owner_epoch = *owner_epoch;
                    existing.lease_token = lease_token.clone();
                    existing.updated_at = *progressed_at;
                } else {
                    runtime_state.source_partition_leases.push(
                        crate::local_state::LocalStreamJobSourceLeaseState {
                            source_partition_id: *source_partition_id,
                            owner_partition_id: *source_owner_partition_id,
                            owner_epoch: *owner_epoch,
                            lease_token: lease_token.clone(),
                            updated_at: *progressed_at,
                        },
                    );
                    runtime_state
                        .source_partition_leases
                        .sort_by_key(|lease| lease.source_partition_id);
                }
                if let Some(existing) = runtime_state
                    .source_cursors
                    .iter_mut()
                    .find(|cursor| cursor.source_partition_id == *source_partition_id)
                {
                    existing.next_offset = *next_offset;
                    existing.initial_checkpoint_target_offset = *checkpoint_target_offset;
                    existing.last_applied_offset = *last_applied_offset;
                    existing.last_high_watermark = *last_high_watermark;
                    existing.last_event_time_watermark = *last_event_time_watermark;
                    existing.last_closed_window_end = *last_closed_window_end;
                    existing.pending_window_ends = pending_window_ends.clone();
                    existing.dropped_late_event_count = *dropped_late_event_count;
                    existing.last_dropped_late_offset = *last_dropped_late_offset;
                    existing.last_dropped_late_event_at = *last_dropped_late_event_at;
                    existing.last_dropped_late_window_end = *last_dropped_late_window_end;
                    existing.dropped_evicted_window_event_count =
                        *dropped_evicted_window_event_count;
                    existing.last_dropped_evicted_window_offset =
                        *last_dropped_evicted_window_offset;
                    existing.last_dropped_evicted_window_event_at =
                        *last_dropped_evicted_window_event_at;
                    existing.last_dropped_evicted_window_end = *last_dropped_evicted_window_end;
                    existing.updated_at = *progressed_at;
                } else {
                    runtime_state.source_cursors.push(LocalStreamJobSourceCursorState {
                        source_partition_id: *source_partition_id,
                        next_offset: *next_offset,
                        initial_checkpoint_target_offset: *checkpoint_target_offset,
                        last_applied_offset: *last_applied_offset,
                        last_high_watermark: *last_high_watermark,
                        last_event_time_watermark: *last_event_time_watermark,
                        last_closed_window_end: *last_closed_window_end,
                        pending_window_ends: pending_window_ends.clone(),
                        dropped_late_event_count: *dropped_late_event_count,
                        last_dropped_late_offset: *last_dropped_late_offset,
                        last_dropped_late_event_at: *last_dropped_late_event_at,
                        last_dropped_late_window_end: *last_dropped_late_window_end,
                        dropped_evicted_window_event_count: *dropped_evicted_window_event_count,
                        last_dropped_evicted_window_offset: *last_dropped_evicted_window_offset,
                        last_dropped_evicted_window_event_at: *last_dropped_evicted_window_event_at,
                        last_dropped_evicted_window_end: *last_dropped_evicted_window_end,
                        checkpoint_reached_at: None,
                        updated_at: *progressed_at,
                    });
                    runtime_state.source_cursors.sort_by_key(|cursor| cursor.source_partition_id);
                }
                runtime_state.updated_at = *progressed_at;
                self.write_batch_delete_cf_and_legacy(
                    write_batch,
                    STREAM_JOBS_CF,
                    &legacy_stream_job_runtime_key(handle_id),
                );
                self.write_batch_put_cf_bytes(
                    write_batch,
                    STREAM_JOBS_CF,
                    &stream_job_runtime_key(handle_id),
                    encode_rocksdb_value(&runtime_state, "stream job runtime state")?,
                );
            }
            StreamsChangelogPayload::StreamJobTerminalized {
                handle_id,
                job_id,
                owner_epoch,
                status,
                output,
                error,
                terminal_at,
            } => {
                let Some(mut runtime_state) = self.load_stream_job_runtime_state(handle_id)? else {
                    anyhow::bail!(
                        "stream job terminalized before runtime state existed for handle {handle_id}"
                    );
                };
                runtime_state.job_id = job_id.clone();
                runtime_state.stream_owner_epoch = *owner_epoch;
                runtime_state.terminal_status = Some(status.clone());
                runtime_state.terminal_output = output.clone();
                runtime_state.terminal_error = error.clone();
                runtime_state.terminal_at = Some(*terminal_at);
                runtime_state.updated_at = *terminal_at;
                self.write_batch_delete_cf_and_legacy(
                    write_batch,
                    STREAM_JOBS_CF,
                    &legacy_stream_job_runtime_key(handle_id),
                );
                self.write_batch_put_cf_bytes(
                    write_batch,
                    STREAM_JOBS_CF,
                    &stream_job_runtime_key(handle_id),
                    encode_rocksdb_value(&runtime_state, "stream job runtime state")?,
                );
            }
        }
        Ok(())
    }
}
