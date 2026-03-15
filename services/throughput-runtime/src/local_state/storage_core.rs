use super::*;
use std::collections::BTreeMap;

impl LocalThroughputState {
    pub(crate) fn load_stream_job_runtime_state(
        &self,
        handle_id: &str,
    ) -> Result<Option<LocalStreamJobRuntimeState>> {
        self.get_cf_bytes_with_legacy_fallback(
            STREAM_JOBS_CF,
            &stream_job_runtime_key(handle_id),
            Some(&legacy_stream_job_runtime_key(handle_id)),
            "stream job runtime state",
        )?
        .map(|value| decode_rocksdb_value(&value, "stream job runtime state"))
        .transpose()
    }

    pub(crate) fn load_all_stream_job_runtime_states(
        &self,
    ) -> Result<Vec<LocalStreamJobRuntimeState>> {
        let mut states = BTreeMap::new();
        for (_key, value) in self.load_prefixed_entries_bytes(
            STREAM_JOBS_CF,
            STREAM_JOB_RUNTIME_BINARY_PREFIX,
            "stream job runtime states",
        )? {
            let state: LocalStreamJobRuntimeState =
                decode_rocksdb_value(&value, "stream job runtime state")?;
            states.insert(state.handle_id.clone(), state);
        }
        for (_key, value) in self.load_prefixed_entries(
            STREAM_JOBS_CF,
            STREAM_JOB_RUNTIME_KEY_PREFIX,
            "legacy stream job runtime states",
        )? {
            let state: LocalStreamJobRuntimeState =
                decode_rocksdb_value(&value, "stream job runtime state")?;
            states.entry(state.handle_id.clone()).or_insert(state);
        }
        Ok(states.into_values().collect())
    }

    pub(crate) fn load_stream_job_checkpoint_state(
        &self,
        handle_id: &str,
        checkpoint_name: &str,
        stream_partition_id: i32,
    ) -> Result<Option<LocalStreamJobCheckpointState>> {
        self.get_cf_bytes_with_legacy_fallback(
            STREAM_JOBS_CF,
            &stream_job_checkpoint_key(handle_id, checkpoint_name, stream_partition_id),
            Some(&legacy_stream_job_checkpoint_key(
                handle_id,
                checkpoint_name,
                stream_partition_id,
            )),
            "stream job checkpoint state",
        )?
        .map(|value| decode_rocksdb_value(&value, "stream job checkpoint state"))
        .transpose()
    }

    pub(crate) fn load_all_stream_job_checkpoints(
        &self,
    ) -> Result<Vec<LocalStreamJobCheckpointState>> {
        let mut checkpoints = BTreeMap::new();
        for (_key, value) in self.load_prefixed_entries_bytes(
            STREAM_JOBS_CF,
            STREAM_JOB_CHECKPOINT_BINARY_PREFIX,
            "stream job checkpoints",
        )? {
            let state: LocalStreamJobCheckpointState =
                decode_rocksdb_value(&value, "stream job checkpoint state")?;
            checkpoints.insert(
                (state.handle_id.clone(), state.checkpoint_name.clone(), state.stream_partition_id),
                state,
            );
        }
        for (_key, value) in self.load_prefixed_entries(
            STREAM_JOBS_CF,
            STREAM_JOB_CHECKPOINT_KEY_PREFIX,
            "legacy stream job checkpoints",
        )? {
            let state: LocalStreamJobCheckpointState =
                decode_rocksdb_value(&value, "stream job checkpoint state")?;
            checkpoints
                .entry((
                    state.handle_id.clone(),
                    state.checkpoint_name.clone(),
                    state.stream_partition_id,
                ))
                .or_insert(state);
        }
        Ok(checkpoints.into_values().collect())
    }

    pub(crate) fn load_stream_job_view_state(
        &self,
        handle_id: &str,
        view_name: &str,
        logical_key: &str,
    ) -> Result<Option<LocalStreamJobViewState>> {
        self.get_cf_bytes_with_legacy_fallback(
            STREAM_JOBS_CF,
            &stream_job_view_key(handle_id, view_name, logical_key),
            Some(&legacy_stream_job_view_key(handle_id, view_name, logical_key)),
            "stream job view state",
        )?
        .map(|value| decode_rocksdb_value(&value, "stream job view state"))
        .transpose()
    }

    pub(crate) fn load_all_stream_job_views(&self) -> Result<Vec<LocalStreamJobViewState>> {
        let mut views = BTreeMap::new();
        for (_key, value) in self.load_prefixed_entries_bytes(
            STREAM_JOBS_CF,
            STREAM_JOB_VIEW_BINARY_PREFIX,
            "stream job view states",
        )? {
            let state: LocalStreamJobViewState =
                decode_rocksdb_value(&value, "stream job view state")?;
            views.insert(
                (state.handle_id.clone(), state.view_name.clone(), state.logical_key.clone()),
                state,
            );
        }
        for (_key, value) in self.load_prefixed_entries(
            STREAM_JOBS_CF,
            STREAM_JOB_VIEW_KEY_PREFIX,
            "legacy stream job view states",
        )? {
            let state: LocalStreamJobViewState =
                decode_rocksdb_value(&value, "stream job view state")?;
            views
                .entry((
                    state.handle_id.clone(),
                    state.view_name.clone(),
                    state.logical_key.clone(),
                ))
                .or_insert(state);
        }
        Ok(views.into_values().collect())
    }

    pub(crate) fn load_stream_job_views_for_view(
        &self,
        handle_id: &str,
        view_name: &str,
    ) -> Result<Vec<LocalStreamJobViewState>> {
        let mut views = BTreeMap::new();
        for (_key, value) in self.load_prefixed_entries_bytes(
            STREAM_JOBS_CF,
            &stream_job_view_prefix(handle_id, view_name),
            "stream job view states",
        )? {
            let state: LocalStreamJobViewState =
                decode_rocksdb_value(&value, "stream job view state")?;
            views.insert(state.logical_key.clone(), state);
        }
        for (_key, value) in self.load_prefixed_entries(
            STREAM_JOBS_CF,
            &legacy_stream_job_view_prefix(handle_id, view_name),
            "legacy stream job view states",
        )? {
            let state: LocalStreamJobViewState =
                decode_rocksdb_value(&value, "stream job view state")?;
            views.entry(state.logical_key.clone()).or_insert(state);
        }
        Ok(views.into_values().collect())
    }

    pub(super) fn load_batch_state(
        &self,
        identity: &ThroughputBatchIdentity,
    ) -> Result<Option<LocalBatchState>> {
        self.get_cf_with_legacy_fallback(
            BATCHES_CF,
            &batch_key(identity),
            "throughput batch state",
        )?
        .map(|value| decode_rocksdb_value(&value, "throughput batch state"))
        .transpose()
    }

    pub(super) fn load_all_batches(&self) -> Result<Vec<LocalBatchState>> {
        let mut batches = Vec::new();
        for (_key, value) in
            self.load_prefixed_entries(BATCHES_CF, BATCH_KEY_PREFIX, "throughput state batches")?
        {
            batches.push(decode_rocksdb_value(&value, "throughput batch state")?);
        }
        Ok(batches)
    }

    pub(super) fn load_all_chunks(&self) -> Result<Vec<LocalChunkState>> {
        let mut chunks = Vec::new();
        for (_key, value) in
            self.load_prefixed_entries(CHUNKS_CF, CHUNK_KEY_PREFIX, "throughput state chunks")?
        {
            chunks.push(decode_rocksdb_chunk_state(&value)?);
        }
        Ok(chunks)
    }

    pub(super) fn load_group_state(
        &self,
        identity: &ThroughputBatchIdentity,
        group_id: u32,
    ) -> Result<Option<LocalGroupState>> {
        self.get_cf_with_legacy_fallback(
            GROUPS_CF,
            &group_key(identity, group_id),
            "throughput group state",
        )?
        .map(|value| decode_rocksdb_value(&value, "throughput group state"))
        .transpose()
    }

    pub(super) fn load_all_groups(&self) -> Result<Vec<LocalGroupState>> {
        let mut groups = Vec::new();
        for (_key, value) in
            self.load_prefixed_entries(GROUPS_CF, GROUP_KEY_PREFIX, "throughput state groups")?
        {
            groups.push(decode_rocksdb_value(&value, "throughput group state")?);
        }
        Ok(groups)
    }

    pub(super) fn load_chunks_for_batch(
        &self,
        identity: &ThroughputBatchIdentity,
    ) -> Result<Vec<LocalChunkState>> {
        let mut chunks = Vec::new();
        for index in self.load_batch_chunk_entries(identity)? {
            if let Some(chunk) = self.load_chunk_state(identity, &index.chunk_id)? {
                chunks.push(chunk);
            }
        }
        Ok(chunks)
    }

    pub(super) fn load_groups_for_batch(
        &self,
        identity: &ThroughputBatchIdentity,
    ) -> Result<Vec<LocalGroupState>> {
        let mut groups = Vec::new();
        for index in self.load_batch_group_entries(identity)? {
            if let Some(group) = self.load_group_state(identity, index.group_id)? {
                groups.push(group);
            }
        }
        Ok(groups)
    }

    pub(super) fn load_batch_chunk_entries(
        &self,
        identity: &ThroughputBatchIdentity,
    ) -> Result<Vec<BatchChunkIndexEntry>> {
        self.load_index_entries_by_prefix(&batch_chunk_index_prefix(identity))
    }

    pub(super) fn load_index_entries_by_prefix<T>(&self, prefix: &str) -> Result<Vec<T>>
    where
        T: DeserializeOwned,
    {
        let mut entries = Vec::new();
        for (_key, value) in
            self.load_prefixed_entries(BATCH_INDEXES_CF, prefix, "throughput secondary index")?
        {
            entries.push(decode_rocksdb_value(&value, "throughput secondary index entry")?);
        }
        Ok(entries)
    }

    pub(super) fn load_chunk_state(
        &self,
        identity: &ThroughputBatchIdentity,
        chunk_id: &str,
    ) -> Result<Option<LocalChunkState>> {
        self.get_cf_with_legacy_fallback(
            CHUNKS_CF,
            &chunk_key(identity, chunk_id),
            "throughput chunk state",
        )?
        .map(|value| decode_rocksdb_chunk_state(&value))
        .transpose()
    }
}
