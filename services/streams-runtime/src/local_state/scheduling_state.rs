use super::*;

impl LocalThroughputState {
    pub(super) fn load_ready_chunk_entries(
        &self,
        tenant_id: &str,
        task_queue: &str,
        now: DateTime<Utc>,
    ) -> Result<Vec<ReadyChunkIndexEntry>> {
        self.load_ready_chunk_entries_scoped(Some(tenant_id), Some(task_queue), now)
    }

    fn load_ready_chunk_entries_scoped(
        &self,
        tenant_id: Option<&str>,
        task_queue: Option<&str>,
        now: DateTime<Utc>,
    ) -> Result<Vec<ReadyChunkIndexEntry>> {
        let prefix = ready_index_scope_prefix(tenant_id, task_queue);
        let mut entries = Vec::new();
        let mut stale_keys = Vec::new();
        for (key, value) in
            self.load_prefixed_entries(SCHEDULING_CF, &prefix, "ready chunk index")?
        {
            let indexed: ReadyChunkIndexEntry =
                decode_rocksdb_value(&value, "ready chunk index entry")?;
            if let Some(chunk) = self.load_chunk_state(&indexed.identity, &indexed.chunk_id)? {
                if chunk.status == WorkflowBulkChunkStatus::Scheduled.as_str() {
                    if chunk.available_at <= now {
                        entries.push(indexed);
                    } else {
                        break;
                    }
                    continue;
                }
            }
            stale_keys.push(key);
        }
        if !stale_keys.is_empty() {
            let mut write_batch = WriteBatch::default();
            for key in stale_keys {
                self.write_batch_delete_cf_and_legacy(&mut write_batch, SCHEDULING_CF, &key);
            }
            self.db.write(write_batch).context("failed to prune stale ready chunk index entry")?;
        }
        Ok(entries)
    }

    fn load_started_chunk_entries(
        &self,
        tenant_id: Option<&str>,
        task_queue: Option<&str>,
    ) -> Result<Vec<StartedChunkIndexEntry>> {
        let prefix = started_index_scope_prefix(tenant_id, task_queue);
        let mut entries = Vec::new();
        let mut stale_keys = Vec::new();
        for (key, value) in
            self.load_prefixed_entries(SCHEDULING_CF, &prefix, "started chunk index")?
        {
            let indexed: StartedChunkIndexEntry =
                decode_rocksdb_value(&value, "started chunk index entry")?;
            if let Some(chunk) = self.load_chunk_state(&indexed.identity, &indexed.chunk_id)? {
                if chunk.status == WorkflowBulkChunkStatus::Started.as_str() {
                    entries.push(indexed);
                    continue;
                }
            }
            stale_keys.push(key);
        }
        if !stale_keys.is_empty() {
            let mut write_batch = WriteBatch::default();
            for key in stale_keys {
                self.write_batch_delete_cf_and_legacy(&mut write_batch, SCHEDULING_CF, &key);
            }
            self.db
                .write(write_batch)
                .context("failed to prune stale started chunk index entry")?;
        }
        Ok(entries)
    }

    fn load_expired_started_chunk_entries(
        &self,
        now: DateTime<Utc>,
    ) -> Result<Vec<StartedChunkIndexEntry>> {
        let prefix = lease_expiry_index_prefix();
        let mut entries = Vec::new();
        let mut stale_keys = Vec::new();
        for (key, value) in
            self.load_prefixed_entries(SCHEDULING_CF, prefix, "lease expiry index")?
        {
            let indexed: StartedChunkIndexEntry =
                decode_rocksdb_value(&value, "lease expiry index entry")?;
            let Some(deadline) = indexed.lease_expires_at else {
                continue;
            };
            if deadline > now {
                break;
            }
            if let Some(chunk) = self.load_chunk_state(&indexed.identity, &indexed.chunk_id)? {
                if chunk.status == WorkflowBulkChunkStatus::Started.as_str()
                    && chunk.lease_expires_at.is_some_and(|expires_at| expires_at <= now)
                {
                    entries.push(indexed);
                    continue;
                }
            }
            stale_keys.push(key);
        }
        if !stale_keys.is_empty() {
            let mut write_batch = WriteBatch::default();
            for key in stale_keys {
                self.write_batch_delete_cf_and_legacy(&mut write_batch, SCHEDULING_CF, &key);
            }
            self.db.write(write_batch).context("failed to prune stale lease expiry index entry")?;
        }
        Ok(entries)
    }

    fn started_counts_by_batch(
        &self,
        tenant_id: Option<&str>,
        task_queue: Option<&str>,
        owned_partitions: Option<&HashSet<i32>>,
        partition_count: i32,
    ) -> Result<HashMap<String, usize>> {
        let mut counts = HashMap::new();
        for chunk in self.load_started_chunk_entries(tenant_id, task_queue)? {
            if !owned_partitions
                .map(|partitions| {
                    partitions.contains(&throughput_partition_id(
                        &chunk.identity.batch_id,
                        chunk.group_id,
                        partition_count,
                    ))
                })
                .unwrap_or(true)
            {
                continue;
            }
            *counts.entry(batch_key(&chunk.identity)).or_insert(0) += 1;
        }
        Ok(counts)
    }

    pub fn count_started_chunks(
        &self,
        tenant_id: Option<&str>,
        task_queue: Option<&str>,
        batch_id: Option<&str>,
        owned_partitions: Option<&HashSet<i32>>,
        partition_count: i32,
    ) -> Result<u64> {
        let mut count = 0_u64;
        for chunk in self.load_started_chunk_entries(tenant_id, task_queue)? {
            if batch_id.is_some_and(|value| chunk.identity.batch_id != value) {
                continue;
            }
            if owned_partitions
                .map(|partitions| {
                    partitions.contains(&throughput_partition_id(
                        &chunk.identity.batch_id,
                        chunk.group_id,
                        partition_count,
                    ))
                })
                .unwrap_or(false)
                || owned_partitions.is_none()
            {
                count = count.saturating_add(1);
            }
        }
        Ok(count)
    }

    pub fn lease_ready_chunks(
        &self,
        tenant_id: &str,
        task_queue: &str,
        worker_id: &str,
        now: DateTime<Utc>,
        lease_ttl: chrono::Duration,
        max_active_chunks_per_batch: Option<usize>,
        paused_batch_ids: &HashSet<String>,
        owned_partitions: Option<&HashSet<i32>>,
        partition_count: i32,
        max_chunks: usize,
    ) -> Result<Vec<LeasedChunkSnapshot>> {
        let _lease_guard = self.lease_lock.lock().expect("local throughput lease lock poisoned");
        let mut started_by_batch = self.started_counts_by_batch(
            Some(tenant_id),
            Some(task_queue),
            owned_partitions,
            partition_count,
        )?;
        self.lease_ready_chunks_scoped(
            Some(tenant_id),
            Some(task_queue),
            worker_id,
            now,
            lease_ttl,
            max_active_chunks_per_batch,
            paused_batch_ids,
            owned_partitions,
            partition_count,
            max_chunks,
            &mut started_by_batch,
            None,
        )
    }

    pub fn lease_ready_chunks_matching<F>(
        &self,
        worker_id: &str,
        now: DateTime<Utc>,
        lease_ttl: chrono::Duration,
        max_active_chunks_per_batch: Option<usize>,
        paused_batch_ids: &HashSet<String>,
        owned_partitions: Option<&HashSet<i32>>,
        partition_count: i32,
        max_chunks: usize,
        mut predicate: F,
    ) -> Result<Vec<LeasedChunkSnapshot>>
    where
        F: FnMut(&LeasedChunkSnapshot) -> bool,
    {
        let _lease_guard = self.lease_lock.lock().expect("local throughput lease lock poisoned");
        let mut started_by_batch =
            self.started_counts_by_batch(None, None, owned_partitions, partition_count)?;
        self.lease_ready_chunks_scoped(
            None,
            None,
            worker_id,
            now,
            lease_ttl,
            max_active_chunks_per_batch,
            paused_batch_ids,
            owned_partitions,
            partition_count,
            max_chunks,
            &mut started_by_batch,
            Some(&mut predicate),
        )
    }

    fn lease_ready_chunks_scoped(
        &self,
        tenant_id: Option<&str>,
        task_queue: Option<&str>,
        worker_id: &str,
        now: DateTime<Utc>,
        lease_ttl: chrono::Duration,
        max_active_chunks_per_batch: Option<usize>,
        paused_batch_ids: &HashSet<String>,
        owned_partitions: Option<&HashSet<i32>>,
        partition_count: i32,
        max_chunks: usize,
        started_by_batch: &mut HashMap<String, usize>,
        mut predicate: Option<&mut dyn FnMut(&LeasedChunkSnapshot) -> bool>,
    ) -> Result<Vec<LeasedChunkSnapshot>> {
        let mut leased_chunks = Vec::with_capacity(max_chunks.max(1));
        let mut write_batch = WriteBatch::default();
        for candidate in self.load_ready_chunk_entries_scoped(tenant_id, task_queue, now)? {
            if leased_chunks.len() >= max_chunks.max(1) {
                break;
            }
            if !owned_partitions
                .map(|partitions| {
                    partitions.contains(&throughput_partition_id(
                        &candidate.identity.batch_id,
                        candidate.group_id,
                        partition_count,
                    ))
                })
                .unwrap_or(true)
            {
                continue;
            }
            let Some(batch) = self.load_batch_state(&candidate.identity)? else {
                continue;
            };
            let batch_key = batch_key(&candidate.identity);
            if matches!(batch.status.as_str(), "completed" | "failed" | "cancelled")
                || batch.failed_items > 0
                || batch.cancelled_items > 0
            {
                continue;
            }
            if paused_batch_ids.contains(&candidate.identity.batch_id) {
                continue;
            }
            if max_active_chunks_per_batch.is_some_and(|limit| {
                started_by_batch.get(&batch_key).copied().unwrap_or_default() >= limit
            }) {
                continue;
            }
            let Some(existing_chunk) =
                self.load_chunk_state(&candidate.identity, &candidate.chunk_id)?
            else {
                continue;
            };
            if existing_chunk.status != WorkflowBulkChunkStatus::Scheduled.as_str()
                || existing_chunk.available_at > now
            {
                continue;
            }
            let mut leased_chunk = existing_chunk.clone();
            leased_chunk.status = WorkflowBulkChunkStatus::Started.as_str().to_owned();
            leased_chunk.worker_id = Some(worker_id.to_owned());
            leased_chunk.lease_epoch = leased_chunk.lease_epoch.saturating_add(1);
            leased_chunk.lease_token = Some(uuid::Uuid::now_v7().to_string());
            leased_chunk.started_at = leased_chunk.started_at.or(Some(now));
            leased_chunk.lease_expires_at = Some(now + lease_ttl);
            leased_chunk.updated_at = now;

            let snapshot = LeasedChunkSnapshot {
                identity: leased_chunk.identity.clone(),
                definition_id: batch.definition_id.clone(),
                definition_version: batch.definition_version,
                artifact_hash: batch.artifact_hash.clone(),
                chunk_id: leased_chunk.chunk_id.clone(),
                activity_type: leased_chunk.activity_type.clone(),
                activity_capabilities: batch.activity_capabilities.clone(),
                task_queue: leased_chunk.task_queue.clone(),
                chunk_index: leased_chunk.chunk_index,
                group_id: leased_chunk.group_id,
                attempt: leased_chunk.attempt,
                item_count: leased_chunk.item_count,
                max_attempts: leased_chunk.max_attempts,
                retry_delay_ms: leased_chunk.retry_delay_ms,
                items: leased_chunk.items.clone(),
                input_handle: leased_chunk.input_handle.clone(),
                omit_success_output: !bulk_reducer_requires_success_outputs(
                    batch.reducer.as_deref(),
                ),
                cancellation_requested: leased_chunk.cancellation_requested,
                lease_epoch: leased_chunk.lease_epoch,
                owner_epoch: leased_chunk.owner_epoch,
                worker_id: leased_chunk.worker_id.clone(),
                lease_token: leased_chunk.lease_token.clone(),
                scheduled_at: leased_chunk.scheduled_at,
                started_at: leased_chunk.started_at,
                lease_expires_at: leased_chunk.lease_expires_at,
                updated_at: leased_chunk.updated_at,
            };
            if predicate.as_mut().is_some_and(|predicate| !(predicate)(&snapshot)) {
                continue;
            }
            self.write_chunk_state(&mut write_batch, Some(&existing_chunk), &leased_chunk)?;
            *started_by_batch.entry(batch_key).or_default() += 1;
            leased_chunks.push(snapshot);
        }
        if !leased_chunks.is_empty() {
            self.db
                .write(write_batch)
                .context("failed to atomically lease ready throughput chunks from local state")?;
        }
        Ok(leased_chunks)
    }

    pub fn debug_lease_selection(
        &self,
        tenant_id: &str,
        task_queue: &str,
        now: DateTime<Utc>,
        max_active_chunks_per_batch: Option<usize>,
        paused_batch_ids: &HashSet<String>,
        owned_partitions: Option<&HashSet<i32>>,
        partition_count: i32,
    ) -> Result<LeaseSelectionDebug> {
        let started_by_batch = self.started_counts_by_batch(
            Some(tenant_id),
            Some(task_queue),
            owned_partitions,
            partition_count,
        )?;
        let mut debug = LeaseSelectionDebug::default();
        for candidate in self.load_ready_chunk_entries(tenant_id, task_queue, now)? {
            debug.indexed_candidates = debug.indexed_candidates.saturating_add(1);
            if !owned_partitions
                .map(|partitions| {
                    partitions.contains(&throughput_partition_id(
                        &candidate.identity.batch_id,
                        candidate.group_id,
                        partition_count,
                    ))
                })
                .unwrap_or(true)
            {
                continue;
            }
            debug.owned_candidates = debug.owned_candidates.saturating_add(1);

            let Some(batch) = self.load_batch_state(&candidate.identity)? else {
                debug.missing_batch = debug.missing_batch.saturating_add(1);
                continue;
            };
            if matches!(batch.status.as_str(), "completed" | "failed" | "cancelled") {
                debug.terminal_batch = debug.terminal_batch.saturating_add(1);
                continue;
            }
            if batch.failed_items > 0 || batch.cancelled_items > 0 {
                debug.batch_failed_or_cancelled = debug.batch_failed_or_cancelled.saturating_add(1);
                continue;
            }
            if paused_batch_ids.contains(&candidate.identity.batch_id) {
                debug.batch_paused = debug.batch_paused.saturating_add(1);
                continue;
            }
            if max_active_chunks_per_batch.is_some_and(|limit| {
                started_by_batch.get(&batch_key(&candidate.identity)).copied().unwrap_or_default()
                    >= limit
            }) {
                debug.batch_cap_blocked = debug.batch_cap_blocked.saturating_add(1);
                continue;
            }
            let Some(chunk) = self.load_chunk_state(&candidate.identity, &candidate.chunk_id)?
            else {
                debug.missing_chunk = debug.missing_chunk.saturating_add(1);
                continue;
            };
            if chunk.status != WorkflowBulkChunkStatus::Scheduled.as_str() {
                debug.chunk_not_scheduled = debug.chunk_not_scheduled.saturating_add(1);
                continue;
            }
            if chunk.available_at > now {
                debug.chunk_not_due = debug.chunk_not_due.saturating_add(1);
                continue;
            }
            debug.leaseable_candidates = debug.leaseable_candidates.saturating_add(1);
        }
        Ok(debug)
    }

    #[cfg(test)]
    pub fn has_ready_chunk(
        &self,
        tenant_id: &str,
        task_queue: &str,
        now: DateTime<Utc>,
        owned_partitions: Option<&HashSet<i32>>,
        partition_count: i32,
    ) -> Result<bool> {
        for candidate in self.load_ready_chunk_entries(tenant_id, task_queue, now)? {
            if !owned_partitions
                .map(|partitions| {
                    partitions.contains(&throughput_partition_id(
                        &candidate.identity.batch_id,
                        candidate.group_id,
                        partition_count,
                    ))
                })
                .unwrap_or(true)
            {
                continue;
            }
            let Some(batch) = self.load_batch_state(&candidate.identity)? else {
                continue;
            };
            if !matches!(batch.status.as_str(), "completed" | "failed" | "cancelled")
                && batch.failed_items == 0
                && batch.cancelled_items == 0
            {
                return Ok(true);
            }
        }
        Ok(false)
    }

    pub fn expired_started_chunks(
        &self,
        now: DateTime<Utc>,
        limit: usize,
        owned_partitions: Option<&HashSet<i32>>,
        partition_count: i32,
    ) -> Result<Vec<ExpiredChunkCandidate>> {
        Ok(self
            .load_expired_started_chunk_entries(now)?
            .into_iter()
            .filter(|chunk| {
                owned_partitions
                    .map(|partitions| {
                        partitions.contains(&throughput_partition_id(
                            &chunk.identity.batch_id,
                            chunk.group_id,
                            partition_count,
                        ))
                    })
                    .unwrap_or(true)
            })
            .take(limit)
            .map(|chunk| ExpiredChunkCandidate {
                identity: chunk.identity,
                chunk_id: chunk.chunk_id,
            })
            .collect())
    }

    pub(crate) fn prepare_report_validation(
        &self,
        report: &ThroughputChunkReport,
    ) -> Result<PreparedValidatedReport> {
        let identity = ThroughputBatchIdentity {
            tenant_id: report.tenant_id.clone(),
            instance_id: report.instance_id.clone(),
            run_id: report.run_id.clone(),
            batch_id: report.batch_id.clone(),
        };
        let Some(batch) = self.load_batch_state(&identity)? else {
            return Ok(PreparedValidatedReport::MissingBatch);
        };
        let chunk = self.load_chunk_state(&identity, &report.chunk_id)?;
        let validation = validate_loaded_report(
            Some(&batch),
            chunk.as_ref(),
            report.attempt,
            report.lease_epoch,
            &report.lease_token,
            report.owner_epoch,
            &report.report_id,
        );
        if validation != ReportValidation::Accept {
            return Ok(PreparedValidatedReport::Rejected(validation));
        }
        let chunk = chunk.expect("validated report missing chunk state");
        Ok(PreparedValidatedReport::Accepted(ValidatedReportChunk {
            chunk_index: chunk.chunk_index,
            group_id: chunk.group_id,
            status: chunk.status,
            attempt: chunk.attempt,
            available_at: chunk.available_at,
            error: chunk.error,
            item_count: chunk.item_count,
            max_attempts: chunk.max_attempts,
            retry_delay_ms: chunk.retry_delay_ms,
        }))
    }

    pub(crate) fn project_chunk_apply_after_validation(
        &self,
        report: &ThroughputChunkReport,
        validated_chunk: &ValidatedReportChunk,
    ) -> Result<Option<ProjectedChunkApply>> {
        let identity = ThroughputBatchIdentity {
            tenant_id: report.tenant_id.clone(),
            instance_id: report.instance_id.clone(),
            run_id: report.run_id.clone(),
            batch_id: report.batch_id.clone(),
        };
        let Some(batch) = self.load_batch_state(&identity)? else {
            return Ok(None);
        };
        let mut projected = ProjectedChunkApply {
            batch_definition_id: batch.definition_id.clone(),
            batch_definition_version: batch.definition_version,
            batch_artifact_hash: batch.artifact_hash.clone(),
            batch_task_queue: batch.task_queue.clone(),
            batch_execution_policy: batch.execution_policy.clone(),
            batch_reducer: batch.reducer.clone(),
            batch_reducer_class: batch.reducer_class.clone(),
            batch_aggregation_tree_depth: batch.aggregation_tree_depth.max(1),
            batch_fast_lane_enabled: batch.fast_lane_enabled,
            batch_total_items: batch.total_items,
            batch_chunk_count: batch.chunk_count,
            chunk_id: report.chunk_id.clone(),
            chunk_group_id: validated_chunk.group_id,
            chunk_status: validated_chunk.status.clone(),
            chunk_attempt: validated_chunk.attempt,
            chunk_available_at: validated_chunk.available_at,
            chunk_error: validated_chunk.error.clone(),
            chunk_item_count: validated_chunk.item_count,
            chunk_max_attempts: validated_chunk.max_attempts,
            chunk_retry_delay_ms: validated_chunk.retry_delay_ms,
        };

        match &report.payload {
            ThroughputChunkReportPayload::ChunkCompleted { .. } => {
                projected.chunk_status = WorkflowBulkChunkStatus::Completed.as_str().to_owned();
                projected.chunk_error = None;
            }
            ThroughputChunkReportPayload::ChunkFailed { error } => {
                if validated_chunk.attempt < validated_chunk.max_attempts {
                    projected.chunk_status = WorkflowBulkChunkStatus::Scheduled.as_str().to_owned();
                    projected.chunk_attempt = validated_chunk.attempt.saturating_add(1);
                    projected.chunk_available_at = report.occurred_at
                        + chrono::Duration::milliseconds(validated_chunk.retry_delay_ms as i64);
                    projected.chunk_error = Some(error.clone());
                } else {
                    projected.chunk_status = WorkflowBulkChunkStatus::Failed.as_str().to_owned();
                    projected.chunk_error = Some(error.clone());
                }
            }
            ThroughputChunkReportPayload::ChunkCancelled { reason, .. } => {
                projected.chunk_status = WorkflowBulkChunkStatus::Cancelled.as_str().to_owned();
                projected.chunk_error = Some(reason.clone());
            }
        }

        Ok(Some(projected))
    }

    pub fn project_requeue(
        &self,
        identity: &ThroughputBatchIdentity,
        chunk_id: &str,
        occurred_at: DateTime<Utc>,
    ) -> Result<Option<ProjectedChunkReschedule>> {
        let Some(chunk) = self.load_chunk_state(identity, chunk_id)? else {
            return Ok(None);
        };
        if chunk.status != WorkflowBulkChunkStatus::Started.as_str() {
            return Ok(None);
        }
        Ok(Some(ProjectedChunkReschedule {
            chunk_id: chunk.chunk_id,
            chunk_index: chunk.chunk_index,
            group_id: chunk.group_id,
            attempt: chunk.attempt,
            item_count: chunk.item_count,
            max_attempts: chunk.max_attempts,
            retry_delay_ms: chunk.retry_delay_ms,
            lease_epoch: chunk.lease_epoch,
            owner_epoch: chunk.owner_epoch,
            available_at: occurred_at,
            started_at: chunk.started_at,
        }))
    }

    pub(super) fn write_chunk_state(
        &self,
        write_batch: &mut WriteBatch,
        previous: Option<&LocalChunkState>,
        state: &LocalChunkState,
    ) -> Result<()> {
        if let Some(previous) = previous {
            self.delete_chunk_indices(write_batch, previous)?;
        }
        self.write_batch_put_cf(
            write_batch,
            CHUNKS_CF,
            &chunk_key(&state.identity, &state.chunk_id),
            encode_rocksdb_chunk_state(state)?,
        );
        self.write_batch_put_cf(
            write_batch,
            BATCH_INDEXES_CF,
            &batch_chunk_index_key(&state.identity, &state.chunk_id),
            encode_rocksdb_value(
                &BatchChunkIndexEntry {
                    identity: state.identity.clone(),
                    chunk_id: state.chunk_id.clone(),
                },
                "batch chunk index entry",
            )?,
        );
        if state.status == WorkflowBulkChunkStatus::Scheduled.as_str() {
            self.write_batch_put_cf(
                write_batch,
                SCHEDULING_CF,
                &ready_index_key(state),
                encode_rocksdb_value(
                    &ReadyChunkIndexEntry {
                        identity: state.identity.clone(),
                        chunk_id: state.chunk_id.clone(),
                        group_id: state.group_id,
                    },
                    "ready chunk index entry",
                )?,
            );
        }
        if state.status == WorkflowBulkChunkStatus::Started.as_str() {
            let indexed = StartedChunkIndexEntry {
                identity: state.identity.clone(),
                chunk_id: state.chunk_id.clone(),
                group_id: state.group_id,
                lease_expires_at: state.lease_expires_at,
                scheduled_at: state.scheduled_at,
                chunk_index: state.chunk_index,
            };
            self.write_batch_put_cf(
                write_batch,
                SCHEDULING_CF,
                &started_index_key(state),
                encode_rocksdb_value(&indexed, "started chunk index entry")?,
            );
            if state.lease_expires_at.is_some() {
                self.write_batch_put_cf(
                    write_batch,
                    SCHEDULING_CF,
                    &lease_expiry_index_key(state),
                    encode_rocksdb_value(&indexed, "lease expiry index entry")?,
                );
            }
        }
        Ok(())
    }

    pub(super) fn delete_chunk_state(
        &self,
        write_batch: &mut WriteBatch,
        state: &LocalChunkState,
    ) -> Result<()> {
        self.write_batch_delete_cf_and_legacy(
            write_batch,
            CHUNKS_CF,
            &chunk_key(&state.identity, &state.chunk_id),
        );
        self.delete_chunk_indices(write_batch, state)
    }

    fn delete_chunk_indices(
        &self,
        write_batch: &mut WriteBatch,
        state: &LocalChunkState,
    ) -> Result<()> {
        self.write_batch_delete_cf_and_legacy(
            write_batch,
            BATCH_INDEXES_CF,
            &batch_chunk_index_key(&state.identity, &state.chunk_id),
        );
        if state.status == WorkflowBulkChunkStatus::Scheduled.as_str() {
            self.write_batch_delete_cf_and_legacy(
                write_batch,
                SCHEDULING_CF,
                &ready_index_key(state),
            );
        }
        if state.status == WorkflowBulkChunkStatus::Started.as_str() {
            self.write_batch_delete_cf_and_legacy(
                write_batch,
                SCHEDULING_CF,
                &started_index_key(state),
            );
            if state.lease_expires_at.is_some() {
                self.write_batch_delete_cf_and_legacy(
                    write_batch,
                    SCHEDULING_CF,
                    &lease_expiry_index_key(state),
                );
            }
        }
        Ok(())
    }
}

fn ready_index_scope_prefix(tenant_id: Option<&str>, task_queue: Option<&str>) -> String {
    match (tenant_id, task_queue) {
        (Some(tenant_id), Some(task_queue)) => {
            format!("{READY_INDEX_PREFIX}{tenant_id}:{task_queue}:")
        }
        (Some(tenant_id), None) => format!("{READY_INDEX_PREFIX}{tenant_id}:"),
        (None, _) => READY_INDEX_PREFIX.to_owned(),
    }
}

pub(super) fn ready_index_key(state: &LocalChunkState) -> String {
    format!(
        "{}{}:{}:{}:{:020}:{}:{}:{}:{}:{:010}:{}",
        READY_INDEX_PREFIX,
        state.identity.tenant_id,
        state.task_queue,
        timestamp_sort_key(state.available_at),
        timestamp_sort_key(state.scheduled_at),
        state.identity.instance_id,
        state.identity.run_id,
        state.identity.batch_id,
        state.group_id,
        state.chunk_index,
        state.chunk_id
    )
}

fn started_index_scope_prefix(tenant_id: Option<&str>, task_queue: Option<&str>) -> String {
    match (tenant_id, task_queue) {
        (Some(tenant_id), Some(task_queue)) => {
            format!("{STARTED_INDEX_PREFIX}{tenant_id}:{task_queue}:")
        }
        (Some(tenant_id), None) => format!("{STARTED_INDEX_PREFIX}{tenant_id}:"),
        (None, _) => STARTED_INDEX_PREFIX.to_owned(),
    }
}

pub(super) fn started_index_key(state: &LocalChunkState) -> String {
    format!(
        "{}{}:{}:{}:{}:{}:{}:{:010}:{}",
        STARTED_INDEX_PREFIX,
        state.identity.tenant_id,
        state.task_queue,
        state.identity.instance_id,
        state.identity.run_id,
        state.identity.batch_id,
        state.group_id,
        state.chunk_index,
        state.chunk_id
    )
}

fn lease_expiry_index_prefix() -> &'static str {
    LEASE_EXPIRY_INDEX_PREFIX
}

pub(super) fn lease_expiry_index_key(state: &LocalChunkState) -> String {
    format!(
        "{}{}:{}:{}:{}:{}:{}:{:010}:{}",
        LEASE_EXPIRY_INDEX_PREFIX,
        timestamp_sort_key(state.lease_expires_at.unwrap_or(state.updated_at)),
        state.identity.tenant_id,
        state.task_queue,
        state.identity.instance_id,
        state.identity.run_id,
        state.identity.batch_id,
        state.group_id,
        state.chunk_id
    )
}
