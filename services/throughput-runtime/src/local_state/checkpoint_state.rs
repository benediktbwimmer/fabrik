use super::*;

impl LocalThroughputState {
    pub fn restore_from_latest_checkpoint_if_empty(&self) -> Result<bool> {
        if !self.is_empty()? {
            return Ok(false);
        }
        let latest_path = self.checkpoint_dir.join(LATEST_CHECKPOINT_FILE);
        if !latest_path.exists() {
            return Ok(false);
        }
        let bytes = fs::read(&latest_path).with_context(|| {
            format!("failed to read throughput checkpoint {}", latest_path.display())
        })?;
        let checkpoint: CheckpointFile = serde_json::from_slice(&bytes)
            .context("failed to deserialize throughput checkpoint")?;
        self.restore_from_checkpoint_if_empty(checkpoint)
    }

    pub fn next_start_offsets(&self, partitions: &[i32]) -> Result<HashMap<i32, i64>> {
        let mut offsets = HashMap::new();
        for partition in partitions {
            if let Some(offset) = self.last_applied_offset(*partition)? {
                offsets.insert(*partition, offset.saturating_add(1));
            }
        }
        Ok(offsets)
    }

    pub fn record_observed_high_watermark(&self, partition_id: i32, high_watermark: i64) {
        let mut meta = self.meta.lock().expect("local throughput meta lock poisoned");
        let entry = meta.observed_high_watermarks.entry(partition_id).or_insert(high_watermark);
        if high_watermark > *entry {
            *entry = high_watermark;
        }
    }

    pub fn record_changelog_apply_failure(&self) {
        let mut meta = self.meta.lock().expect("local throughput meta lock poisoned");
        meta.changelog_apply_failures = meta.changelog_apply_failures.saturating_add(1);
    }

    pub fn snapshot_checkpoint_value(&self) -> Result<Value> {
        serde_json::to_value(self.build_checkpoint()?)
            .context("failed to serialize throughput checkpoint value")
    }

    pub fn snapshot_partition_checkpoint_value(
        &self,
        partition_id: i32,
        partition_count: i32,
    ) -> Result<Value> {
        serde_json::to_value(self.build_partition_checkpoint(partition_id, partition_count)?)
            .context("failed to serialize partition-scoped throughput checkpoint value")
    }

    pub fn restore_from_checkpoint_value_if_empty(&self, value: Value) -> Result<bool> {
        let checkpoint: CheckpointFile =
            serde_json::from_value(value).context("failed to deserialize throughput checkpoint")?;
        self.restore_from_checkpoint_if_empty(checkpoint)
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub fn write_checkpoint(&self) -> Result<PathBuf> {
        let checkpoint = self.build_checkpoint()?;
        let payload =
            serde_json::to_vec_pretty(&checkpoint).context("failed to serialize checkpoint")?;
        let checkpoint_file = self
            .checkpoint_dir
            .join(format!("checkpoint-{}.json", checkpoint.created_at.timestamp_millis()));
        let temp_file = self.checkpoint_dir.join("latest.tmp");
        fs::write(&temp_file, payload)
            .with_context(|| format!("failed to write checkpoint file {}", temp_file.display()))?;
        fs::rename(&temp_file, &checkpoint_file).with_context(|| {
            format!("failed to finalize checkpoint file {}", checkpoint_file.display())
        })?;
        fs::copy(&checkpoint_file, self.checkpoint_dir.join(LATEST_CHECKPOINT_FILE)).with_context(
            || {
                format!(
                    "failed to update latest throughput checkpoint in {}",
                    self.checkpoint_dir.display()
                )
            },
        )?;
        self.prune_old_checkpoints()?;
        self.record_checkpoint_write(checkpoint.created_at);
        Ok(checkpoint_file)
    }

    pub fn record_checkpoint_failure(&self) {
        let mut meta = self.meta.lock().expect("local throughput meta lock poisoned");
        meta.checkpoint_failures = meta.checkpoint_failures.saturating_add(1);
    }

    pub fn record_checkpoint_write(&self, created_at: DateTime<Utc>) {
        let mut meta = self.meta.lock().expect("local throughput meta lock poisoned");
        meta.last_checkpoint_at = Some(created_at);
        meta.checkpoint_writes = meta.checkpoint_writes.saturating_add(1);
    }

    pub fn partition_is_caught_up(&self, partition_id: i32) -> Result<bool> {
        let applied = self.last_applied_offset(partition_id)?;
        let high_watermark = {
            let meta = self.meta.lock().expect("local throughput meta lock poisoned");
            meta.observed_high_watermarks.get(&partition_id).copied()
        };
        let Some(high_watermark) = high_watermark else {
            return Ok(true);
        };
        if high_watermark <= 0 {
            return Ok(true);
        }
        Ok(applied.unwrap_or(-1) >= high_watermark.saturating_sub(1))
    }

    pub fn debug_snapshot(&self) -> Result<LocalThroughputDebugSnapshot> {
        let meta = self.meta.lock().expect("local throughput meta lock poisoned");
        let last_applied_offsets = self.load_offsets()?;
        let observed_high_watermarks = meta.observed_high_watermarks.clone();
        let partition_lag = observed_high_watermarks
            .iter()
            .map(|(partition_id, high_watermark)| {
                let applied = last_applied_offsets.get(partition_id).copied().unwrap_or(-1);
                let lag = if *high_watermark <= 0 {
                    0
                } else {
                    high_watermark.saturating_sub(applied.saturating_add(1))
                };
                (*partition_id, lag.max(0))
            })
            .collect::<BTreeMap<_, _>>();
        let batches = self.load_all_batches()?;
        let chunks = self.load_all_chunks()?;
        let groups = self.load_all_groups()?;
        let batch_status_counts =
            batches.iter().fold(BTreeMap::new(), |mut counts: BTreeMap<String, u64>, batch| {
                *counts.entry(batch.status.clone()).or_default() += 1;
                counts
            });
        let batch_tree_depth_counts =
            batches.iter().fold(BTreeMap::new(), |mut counts: BTreeMap<u32, u64>, batch| {
                *counts.entry(batch.aggregation_tree_depth.max(1)).or_default() += 1;
                counts
            });
        let group_level_counts =
            groups.iter().fold(BTreeMap::new(), |mut counts: BTreeMap<u32, u64>, group| {
                *counts.entry(group.level).or_default() += 1;
                counts
            });
        let scheduled_chunk_count = chunks
            .iter()
            .filter(|chunk| chunk.status == WorkflowBulkChunkStatus::Scheduled.as_str())
            .count() as u64;
        let ready_chunk_count = chunks
            .iter()
            .filter(|chunk| {
                chunk.status == WorkflowBulkChunkStatus::Scheduled.as_str()
                    && chunk.available_at <= Utc::now()
            })
            .count() as u64;
        Ok(LocalThroughputDebugSnapshot {
            db_path: self.db_path.display().to_string(),
            checkpoint_dir: self.checkpoint_dir.display().to_string(),
            restored_from_checkpoint: meta.restored_from_checkpoint,
            last_checkpoint_at: meta.last_checkpoint_at,
            checkpoint_writes: meta.checkpoint_writes,
            checkpoint_failures: meta.checkpoint_failures,
            changelog_entries_applied: meta.changelog_entries_applied,
            changelog_apply_failures: meta.changelog_apply_failures,
            last_changelog_apply_at: meta.last_changelog_apply_at,
            legacy_default_cf_entries_migrated: meta.legacy_default_cf_entries_migrated,
            last_legacy_default_cf_migration_at: meta.last_legacy_default_cf_migration_at,
            batch_count: batches.len(),
            chunk_count: chunks.len(),
            scheduled_chunk_count,
            ready_chunk_count,
            started_chunk_count: self.count_started_chunks(None, None, None, None, 1)?,
            batch_status_counts,
            batch_tree_depth_counts,
            group_level_counts,
            last_applied_offsets,
            observed_high_watermarks,
            partition_lag,
        })
    }

    fn is_empty(&self) -> Result<bool> {
        if self.db.iterator(IteratorMode::Start).next().is_some() {
            return Ok(false);
        }
        for cf_name in [
            META_CF,
            BATCHES_CF,
            CHUNKS_CF,
            GROUPS_CF,
            BATCH_INDEXES_CF,
            SCHEDULING_CF,
            STREAM_JOBS_CF,
        ] {
            if self.db.iterator_cf(self.cf(cf_name), IteratorMode::Start).next().is_some() {
                return Ok(false);
            }
        }
        Ok(true)
    }

    pub(super) fn last_applied_offset(&self, partition_id: i32) -> Result<Option<i64>> {
        self.get_cf_with_legacy_fallback(
            META_CF,
            &offset_key(partition_id),
            "throughput changelog offset",
        )?
        .map(|value| {
            String::from_utf8(value)
                .context("throughput changelog offset is not utf-8")?
                .parse::<i64>()
                .context("throughput changelog offset is not numeric")
        })
        .transpose()
    }

    pub(super) fn is_mirrored_entry_pending(&self, entry_id: uuid::Uuid) -> Result<bool> {
        Ok(self
            .get_cf_with_legacy_fallback(
                META_CF,
                &mirrored_entry_key(entry_id),
                "mirrored throughput entry marker",
            )?
            .is_some())
    }

    fn load_offsets(&self) -> Result<BTreeMap<i32, i64>> {
        let mut offsets = BTreeMap::new();
        for (key, value) in
            self.load_prefixed_entries(META_CF, OFFSET_KEY_PREFIX, "throughput state offsets")?
        {
            if let Some(partition) = key.strip_prefix(OFFSET_KEY_PREFIX) {
                let partition = partition
                    .parse::<i32>()
                    .context("throughput offset partition is not numeric")?;
                let offset = String::from_utf8(value.to_vec())
                    .context("throughput offset value is not utf-8")?
                    .parse::<i64>()
                    .context("throughput offset value is not numeric")?;
                offsets.insert(partition, offset);
            }
        }
        Ok(offsets)
    }

    pub(super) fn load_mirrored_entry_ids(&self) -> Result<Vec<String>> {
        let mut entry_ids = Vec::new();
        for (key, _value) in self.load_prefixed_entries(
            META_CF,
            MIRRORED_ENTRY_KEY_PREFIX,
            "throughput mirrored-entry markers",
        )? {
            if let Some(entry_id) = key.strip_prefix(MIRRORED_ENTRY_KEY_PREFIX) {
                entry_ids.push(entry_id.to_owned());
            }
        }
        entry_ids.sort();
        Ok(entry_ids)
    }

    fn build_checkpoint(&self) -> Result<CheckpointFile> {
        Ok(CheckpointFile {
            created_at: Utc::now(),
            offsets: self.load_offsets()?,
            mirrored_entry_ids: self.load_mirrored_entry_ids()?,
            batches: self.load_all_batches()?,
            chunks: self.load_all_chunks()?,
            groups: self.load_all_groups()?,
            stream_job_views: self.load_all_stream_job_views()?,
        })
    }

    fn build_partition_checkpoint(
        &self,
        partition_id: i32,
        partition_count: i32,
    ) -> Result<CheckpointFile> {
        let created_at = Utc::now();
        let chunks = self
            .load_all_chunks()?
            .into_iter()
            .filter(|chunk| {
                throughput_partition_id(&chunk.identity.batch_id, chunk.group_id, partition_count)
                    == partition_id
            })
            .collect::<Vec<_>>();
        let groups = self
            .load_all_groups()?
            .into_iter()
            .filter(|group| {
                throughput_partition_id(&group.identity.batch_id, group.group_id, partition_count)
                    == partition_id
            })
            .collect::<Vec<_>>();
        let mut included_batches =
            chunks.iter().map(|chunk| batch_key(&chunk.identity)).collect::<HashSet<_>>();
        included_batches.extend(groups.iter().map(|group| batch_key(&group.identity)));
        let batches = self
            .load_all_batches()?
            .into_iter()
            .filter(|batch| {
                included_batches.contains(&batch_key(&batch.identity))
                    || (batch.chunk_count == 0
                        && throughput_partition_id(&batch.identity.batch_id, 0, partition_count)
                            == partition_id)
            })
            .collect::<Vec<_>>();
        Ok(CheckpointFile {
            created_at,
            offsets: self
                .load_offsets()?
                .into_iter()
                .filter(|(offset_partition_id, _)| *offset_partition_id == partition_id)
                .collect(),
            mirrored_entry_ids: Vec::new(),
            batches,
            chunks,
            groups,
            stream_job_views: Vec::new(),
        })
    }

    fn restore_from_checkpoint_if_empty(&self, checkpoint: CheckpointFile) -> Result<bool> {
        if !self.is_empty()? {
            return Ok(false);
        }
        let mut batch = WriteBatch::default();
        for (partition, offset) in &checkpoint.offsets {
            self.write_batch_put_cf(
                &mut batch,
                META_CF,
                &offset_key(*partition),
                offset.to_string().into_bytes(),
            );
        }
        for entry_id in &checkpoint.mirrored_entry_ids {
            self.write_batch_put_cf(
                &mut batch,
                META_CF,
                &mirrored_entry_key(entry_id),
                b"1".to_vec(),
            );
        }
        for state in &checkpoint.batches {
            self.write_batch_put_cf(
                &mut batch,
                BATCHES_CF,
                &batch_key(&state.identity),
                encode_rocksdb_value(state, "checkpoint batch state")?,
            );
        }
        for state in &checkpoint.chunks {
            self.write_chunk_state(&mut batch, None, state)?;
        }
        for state in &checkpoint.groups {
            self.write_group_state(&mut batch, state)?;
        }
        for state in &checkpoint.stream_job_views {
            self.write_batch_put_cf(
                &mut batch,
                STREAM_JOBS_CF,
                &stream_job_view_key(&state.handle_id, &state.view_name, &state.logical_key),
                encode_rocksdb_value(state, "checkpoint stream job view state")?,
            );
        }
        self.db.write(batch).context("failed to restore throughput checkpoint into state db")?;
        let mut meta = self.meta.lock().expect("local throughput meta lock poisoned");
        meta.restored_from_checkpoint = true;
        meta.last_checkpoint_at = Some(checkpoint.created_at);
        Ok(true)
    }

    #[cfg_attr(not(test), allow(dead_code))]
    fn prune_old_checkpoints(&self) -> Result<()> {
        let mut checkpoints = fs::read_dir(&self.checkpoint_dir)
            .with_context(|| {
                format!("failed to read checkpoint directory {}", self.checkpoint_dir.display())
            })?
            .filter_map(|entry| entry.ok().map(|value| value.path()))
            .filter(|path| {
                path.file_name()
                    .and_then(|name| name.to_str())
                    .is_some_and(|name| name.starts_with("checkpoint-") && name.ends_with(".json"))
            })
            .collect::<Vec<_>>();
        checkpoints.sort();
        if checkpoints.len() <= self.checkpoint_retention {
            return Ok(());
        }
        let stale_count = checkpoints.len() - self.checkpoint_retention;
        for stale in checkpoints.into_iter().take(stale_count) {
            fs::remove_file(&stale).with_context(|| {
                format!("failed to remove stale checkpoint {}", stale.display())
            })?;
        }
        Ok(())
    }
}
