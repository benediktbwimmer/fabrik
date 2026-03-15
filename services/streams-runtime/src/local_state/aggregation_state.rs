use super::*;

impl LocalThroughputState {
    pub(super) fn load_batch_group_entries(
        &self,
        identity: &ThroughputBatchIdentity,
    ) -> Result<Vec<BatchGroupIndexEntry>> {
        self.load_index_entries_by_prefix(&batch_group_index_prefix(identity))
    }

    pub(crate) fn reduce_batch_success_outputs_after_report(
        &self,
        identity: &ThroughputBatchIdentity,
        pending_report: Option<&ThroughputChunkReport>,
    ) -> Result<Option<Value>> {
        let Some(batch) = self.load_batch_state(identity)? else {
            return Ok(None);
        };
        let reducer = batch.reducer.as_deref();
        if bulk_reducer_summary_field_name(reducer).is_none() {
            return Ok(None);
        }

        if reducer == Some("count") {
            let count = if bulk_reducer_settles(reducer) {
                u64::from(batch.succeeded_items)
                    + u64::from(batch.failed_items)
                    + u64::from(batch.cancelled_items)
            } else {
                u64::from(batch.succeeded_items)
            };
            return Ok(Some(Value::from(count)));
        }

        let mut values = Vec::new();
        let mut errors = Vec::new();
        let mut chunks = self.load_chunks_for_batch(identity)?;
        chunks.sort_by_key(|chunk| chunk.chunk_index);
        for chunk in chunks {
            if let Some(report) = pending_report.filter(|report| report.chunk_id == chunk.chunk_id)
            {
                match &report.payload {
                    ThroughputChunkReportPayload::ChunkCompleted { output, .. }
                        if bulk_reducer_requires_success_outputs(reducer) =>
                    {
                        let output = output.as_ref().ok_or_else(|| {
                            anyhow::anyhow!(
                                "throughput batch {} reducer {} requires output for chunk {}",
                                identity.batch_id,
                                reducer.unwrap_or_default(),
                                chunk.chunk_id
                            )
                        })?;
                        values.extend(output.iter().cloned());
                    }
                    ThroughputChunkReportPayload::ChunkFailed { error }
                        if bulk_reducer_requires_error_outputs(reducer) =>
                    {
                        errors.push((error.clone(), chunk.item_count));
                    }
                    ThroughputChunkReportPayload::ChunkCancelled { reason, .. }
                        if bulk_reducer_requires_error_outputs(reducer) =>
                    {
                        errors.push((reason.clone(), chunk.item_count));
                    }
                    _ => {}
                }
                continue;
            }

            match chunk.status.as_str() {
                status
                    if status == WorkflowBulkChunkStatus::Completed.as_str()
                        && bulk_reducer_requires_success_outputs(reducer) =>
                {
                    let output = chunk.output.as_ref().ok_or_else(|| {
                        anyhow::anyhow!(
                            "throughput batch {} reducer {} missing completed output for chunk {}",
                            identity.batch_id,
                            reducer.unwrap_or_default(),
                            chunk.chunk_id
                        )
                    })?;
                    values.extend(output.iter().cloned());
                }
                status
                    if matches!(
                        status,
                        s if s == WorkflowBulkChunkStatus::Failed.as_str()
                            || s == WorkflowBulkChunkStatus::Cancelled.as_str()
                    ) && bulk_reducer_requires_error_outputs(reducer) =>
                {
                    if let Some(error) = chunk.error.as_ref() {
                        errors.push((error.clone(), chunk.item_count));
                    }
                }
                _ => {}
            }
        }

        if bulk_reducer_requires_error_outputs(reducer) {
            return bulk_reducer_reduce_errors(reducer, &errors)
                .context("failed to reduce throughput batch errors");
        }

        if values.is_empty() {
            return Ok(bulk_reducer_default_summary_value(reducer));
        }

        bulk_reducer_reduce_values(reducer, &values)
            .context("failed to reduce throughput batch success outputs")
    }

    pub(crate) fn group_barrier_batches(
        &self,
        owned_partitions: Option<&HashSet<i32>>,
        partition_count: i32,
    ) -> Result<Vec<ThroughputBatchIdentity>> {
        let mut identities = self
            .load_all_batches()?
            .into_iter()
            .filter(|batch| {
                batch.aggregation_group_count > 1
                    && !matches!(batch.status.as_str(), "completed" | "failed" | "cancelled")
                    && owned_partitions
                        .map(|partitions| {
                            partitions.contains(&throughput_partition_id(
                                &batch.identity.batch_id,
                                0,
                                partition_count,
                            ))
                        })
                        .unwrap_or(true)
            })
            .map(|batch| batch.identity)
            .collect::<Vec<_>>();
        identities.sort_by(|left, right| {
            left.tenant_id
                .cmp(&right.tenant_id)
                .then_with(|| left.instance_id.cmp(&right.instance_id))
                .then_with(|| left.run_id.cmp(&right.run_id))
                .then_with(|| left.batch_id.cmp(&right.batch_id))
        });
        Ok(identities)
    }

    pub(crate) fn project_batch_rollup_after_chunk_apply(
        &self,
        identity: &ThroughputBatchIdentity,
        report: &ThroughputChunkReport,
        chunk: &ProjectedChunkApply,
    ) -> Result<Option<ProjectedBatchRollup>> {
        let Some(batch) = self.load_batch_state(identity)? else {
            return Ok(None);
        };
        let mut rollup = ProjectedBatchRollup {
            batch_status: if batch.status == "scheduled" {
                "running".to_owned()
            } else {
                batch.status.clone()
            },
            batch_succeeded_items: batch.succeeded_items,
            batch_failed_items: batch.failed_items,
            batch_cancelled_items: batch.cancelled_items,
            batch_error: batch.error.clone(),
            batch_terminal: false,
            batch_terminal_deferred: false,
            terminal_at: None,
            group_terminal: None,
            parent_group_terminals: Vec::new(),
            grouped_batch_terminal: None,
            grouped_batch_summary: None,
        };
        let mut loaded_chunks: Option<Vec<LocalChunkState>> = None;

        match &report.payload {
            ThroughputChunkReportPayload::ChunkCompleted { .. } => {
                rollup.batch_succeeded_items =
                    rollup.batch_succeeded_items.saturating_add(chunk.chunk_item_count);
                loaded_chunks = Some(self.load_chunks_for_batch(identity)?);
                let chunks =
                    loaded_chunks.as_ref().expect("loaded report apply chunks missing after load");
                let all_chunks_terminal = chunks.iter().all(|candidate| {
                    if candidate.chunk_id == report.chunk_id {
                        true
                    } else {
                        matches!(candidate.status.as_str(), "completed" | "failed" | "cancelled")
                    }
                });
                if all_chunks_terminal
                    && batch.total_items > 0
                    && rollup.batch_succeeded_items >= batch.total_items
                {
                    rollup.batch_status = "completed".to_owned();
                    rollup.batch_terminal = true;
                    rollup.terminal_at = Some(report.occurred_at);
                    rollup.batch_error = None;
                }
            }
            ThroughputChunkReportPayload::ChunkFailed { error } => {
                if chunk.chunk_status == WorkflowBulkChunkStatus::Failed.as_str() {
                    rollup.batch_failed_items =
                        rollup.batch_failed_items.saturating_add(chunk.chunk_item_count);
                    rollup.batch_status = "failed".to_owned();
                    rollup.batch_error = Some(error.clone());
                    rollup.batch_terminal = true;
                    rollup.terminal_at = Some(report.occurred_at);
                }
            }
            ThroughputChunkReportPayload::ChunkCancelled { reason, .. } => {
                rollup.batch_cancelled_items =
                    rollup.batch_cancelled_items.saturating_add(chunk.chunk_item_count);
                rollup.batch_status = "cancelled".to_owned();
                rollup.batch_error = Some(reason.clone());
                rollup.batch_terminal = true;
                rollup.terminal_at = Some(report.occurred_at);
            }
        }

        if batch.aggregation_group_count > 1 && rollup.batch_terminal {
            rollup.batch_terminal = false;
            rollup.batch_terminal_deferred = true;
            rollup.terminal_at = None;
            rollup.batch_status = "running".to_owned();
        }
        if batch.aggregation_group_count > 1 {
            if loaded_chunks.is_none() {
                loaded_chunks = Some(self.load_chunks_for_batch(identity)?);
            }
            let chunks = loaded_chunks
                .as_ref()
                .expect("loaded grouped report apply chunks missing after load");
            let existing_group = self.load_group_state(identity, chunk.chunk_group_id)?;
            rollup.group_terminal = project_group_terminal_from_loaded(
                &batch,
                existing_group.as_ref(),
                chunks,
                chunk.chunk_group_id,
                Some((
                    report.chunk_id.as_str(),
                    chunk.chunk_status.as_str(),
                    chunk.chunk_error.as_deref(),
                )),
                report.occurred_at,
            );
            if let Some(group_terminal) = rollup.group_terminal.as_ref() {
                let groups = self.load_groups_for_batch(identity)?;
                rollup.parent_group_terminals = project_parent_group_terminals_from_loaded(
                    &batch,
                    &groups,
                    Some(group_terminal),
                    report.occurred_at,
                );
                let projected_groups = project_groups_for_batch_from_loaded(
                    chunks,
                    &groups,
                    Some(group_terminal),
                    &rollup.parent_group_terminals,
                );
                rollup.grouped_batch_terminal = project_batch_terminal_from_loaded(
                    &batch,
                    &projected_groups,
                    report.occurred_at,
                );
                if rollup.grouped_batch_terminal.is_none() {
                    rollup.grouped_batch_summary = summarize_projected_groups(&projected_groups);
                }
            }
        }

        Ok(Some(rollup))
    }

    pub(crate) fn project_batch_terminal_from_groups(
        &self,
        identity: &ThroughputBatchIdentity,
        occurred_at: DateTime<Utc>,
    ) -> Result<Option<ProjectedBatchTerminal>> {
        self.project_batch_terminal_from_groups_with_pending_group(identity, None, occurred_at)
    }

    pub(super) fn project_batch_terminal_from_groups_with_pending_group(
        &self,
        identity: &ThroughputBatchIdentity,
        pending_group: Option<&ProjectedGroupTerminal>,
        occurred_at: DateTime<Utc>,
    ) -> Result<Option<ProjectedBatchTerminal>> {
        let Some(batch) = self.load_batch_state(identity)? else {
            return Ok(None);
        };
        let groups = self.load_projected_groups_for_batch(identity, pending_group)?;
        Ok(project_batch_terminal_from_loaded(&batch, &groups, occurred_at))
    }

    pub(crate) fn project_group_terminal(
        &self,
        identity: &ThroughputBatchIdentity,
        group_id: u32,
        occurred_at: DateTime<Utc>,
    ) -> Result<Option<ProjectedGroupTerminal>> {
        self.project_group_terminal_after_apply(identity, group_id, None, occurred_at)
    }

    fn project_group_terminal_after_apply(
        &self,
        identity: &ThroughputBatchIdentity,
        group_id: u32,
        pending_chunk: Option<(&str, &str, Option<&str>)>,
        occurred_at: DateTime<Utc>,
    ) -> Result<Option<ProjectedGroupTerminal>> {
        let Some(batch) = self.load_batch_state(identity)? else {
            return Ok(None);
        };
        let existing_group = self.load_group_state(identity, group_id)?;
        let chunks = self.load_chunks_for_batch(identity)?.into_iter().collect::<Vec<_>>();
        Ok(project_group_terminal_from_loaded(
            &batch,
            existing_group.as_ref(),
            &chunks,
            group_id,
            pending_chunk,
            occurred_at,
        ))
    }

    fn load_projected_groups_for_batch(
        &self,
        identity: &ThroughputBatchIdentity,
        pending_group: Option<&ProjectedGroupTerminal>,
    ) -> Result<Vec<ProjectedGroupTerminal>> {
        let Some(batch) = self.load_batch_state(identity)? else {
            return Ok(Vec::new());
        };
        let chunks = self.load_chunks_for_batch(identity)?;
        let groups = self.load_groups_for_batch(identity)?;
        let parent_groups = project_parent_group_terminals_from_loaded(
            &batch,
            &groups,
            pending_group,
            pending_group.map(|group| group.terminal_at).unwrap_or_else(Utc::now),
        );
        Ok(project_groups_for_batch_from_loaded(&chunks, &groups, pending_group, &parent_groups))
    }

    pub(super) fn write_group_state(
        &self,
        write_batch: &mut WriteBatch,
        state: &LocalGroupState,
    ) -> Result<()> {
        self.write_batch_put_cf(
            write_batch,
            GROUPS_CF,
            &group_key(&state.identity, state.group_id),
            encode_rocksdb_value(state, "throughput group state")?,
        );
        self.write_batch_put_cf(
            write_batch,
            BATCH_INDEXES_CF,
            &batch_group_index_key(&state.identity, state.group_id),
            encode_rocksdb_value(
                &BatchGroupIndexEntry {
                    identity: state.identity.clone(),
                    group_id: state.group_id,
                },
                "batch group index entry",
            )?,
        );
        Ok(())
    }

    pub(super) fn delete_group_state(
        &self,
        write_batch: &mut WriteBatch,
        state: &LocalGroupState,
    ) -> Result<()> {
        self.write_batch_delete_cf_and_legacy(
            write_batch,
            GROUPS_CF,
            &group_key(&state.identity, state.group_id),
        );
        self.write_batch_delete_cf_and_legacy(
            write_batch,
            BATCH_INDEXES_CF,
            &batch_group_index_key(&state.identity, state.group_id),
        );
        Ok(())
    }
}

pub(super) fn derive_terminal_group_from_chunks(
    chunks: &[LocalChunkState],
    group_id: u32,
) -> Option<ProjectedGroupTerminal> {
    let mut saw_group = false;
    let mut succeeded_items = 0_u32;
    let mut failed_items = 0_u32;
    let mut cancelled_items = 0_u32;
    let mut error = None;
    let mut saw_nonterminal = false;
    let mut terminal_at = DateTime::<Utc>::UNIX_EPOCH;

    for chunk in chunks.iter().filter(|chunk| chunk.group_id == group_id) {
        saw_group = true;
        terminal_at = terminal_at.max(chunk.completed_at.unwrap_or(chunk.updated_at));
        match chunk.status.as_str() {
            "completed" => succeeded_items = succeeded_items.saturating_add(chunk.item_count),
            "failed" => {
                failed_items = failed_items.saturating_add(chunk.item_count);
                if error.is_none() {
                    error = chunk.error.clone();
                }
            }
            "cancelled" => {
                cancelled_items = cancelled_items.saturating_add(chunk.item_count);
                if error.is_none() {
                    error = chunk.error.clone();
                }
            }
            _ => saw_nonterminal = true,
        }
    }

    if !saw_group || (failed_items == 0 && cancelled_items == 0 && saw_nonterminal) {
        return None;
    }

    let status = if failed_items > 0 {
        "failed"
    } else if cancelled_items > 0 {
        "cancelled"
    } else if saw_nonterminal {
        return None;
    } else {
        "completed"
    };

    Some(ProjectedGroupTerminal {
        group_id,
        level: reduction_tree_node_level(group_id),
        parent_group_id: None,
        status: status.to_owned(),
        succeeded_items,
        failed_items,
        cancelled_items,
        error,
        terminal_at,
    })
}

fn project_group_terminal_from_loaded(
    batch: &LocalBatchState,
    existing_group: Option<&LocalGroupState>,
    chunks: &[LocalChunkState],
    group_id: u32,
    pending_chunk: Option<(&str, &str, Option<&str>)>,
    occurred_at: DateTime<Utc>,
) -> Option<ProjectedGroupTerminal> {
    if batch.aggregation_group_count <= 1 || existing_group.is_some() {
        return None;
    }
    let chunks = chunks.iter().filter(|chunk| chunk.group_id == group_id).collect::<Vec<_>>();
    if chunks.is_empty() {
        return None;
    }

    let mut succeeded_items = 0u32;
    let mut failed_items = 0u32;
    let mut cancelled_items = 0u32;
    let mut error = None;
    let mut saw_nonterminal = false;
    let mut saw_pending_chunk = pending_chunk.is_none();
    for chunk in &chunks {
        let (status, chunk_error) =
            if let Some((pending_chunk_id, pending_status, pending_error)) = pending_chunk {
                if chunk.chunk_id == pending_chunk_id {
                    saw_pending_chunk = true;
                    (pending_status, pending_error)
                } else {
                    (chunk.status.as_str(), chunk.error.as_deref())
                }
            } else {
                (chunk.status.as_str(), chunk.error.as_deref())
            };
        match status {
            "completed" => succeeded_items = succeeded_items.saturating_add(chunk.item_count),
            "failed" => {
                failed_items = failed_items.saturating_add(chunk.item_count);
                if error.is_none() {
                    error = chunk_error.map(ToOwned::to_owned);
                }
            }
            "cancelled" => {
                cancelled_items = cancelled_items.saturating_add(chunk.item_count);
                if error.is_none() {
                    error = chunk_error.map(ToOwned::to_owned);
                }
            }
            _ => saw_nonterminal = true,
        }
    }
    if !saw_pending_chunk || (failed_items == 0 && cancelled_items == 0 && saw_nonterminal) {
        return None;
    }
    let status = if failed_items > 0 {
        "failed"
    } else if cancelled_items > 0 {
        "cancelled"
    } else if saw_nonterminal {
        return None;
    } else {
        "completed"
    };
    Some(ProjectedGroupTerminal {
        group_id,
        level: 0,
        parent_group_id: reduction_tree_parent_group_id(
            group_id,
            batch.aggregation_group_count,
            batch.aggregation_tree_depth,
        ),
        status: status.to_owned(),
        succeeded_items,
        failed_items,
        cancelled_items,
        error,
        terminal_at: occurred_at,
    })
}

pub(super) fn project_parent_group_terminals_from_loaded(
    batch: &LocalBatchState,
    groups: &[LocalGroupState],
    pending_group: Option<&ProjectedGroupTerminal>,
    occurred_at: DateTime<Utc>,
) -> Vec<ProjectedGroupTerminal> {
    if reduction_tree_level_counts(batch.aggregation_group_count, batch.aggregation_tree_depth)
        .len()
        <= 1
    {
        return Vec::new();
    }

    let mut projected_groups = groups
        .iter()
        .map(|group| ProjectedGroupTerminal {
            group_id: group.group_id,
            level: group.level,
            parent_group_id: group.parent_group_id,
            status: group.status.clone(),
            succeeded_items: group.succeeded_items,
            failed_items: group.failed_items,
            cancelled_items: group.cancelled_items,
            error: group.error.clone(),
            terminal_at: group.terminal_at,
        })
        .collect::<Vec<_>>();
    if let Some(group) = pending_group {
        upsert_projected_group(&mut projected_groups, group.clone());
    }

    let mut candidates = pending_group
        .and_then(|group| {
            reduction_tree_parent_group_id(
                group.group_id,
                batch.aggregation_group_count,
                batch.aggregation_tree_depth,
            )
        })
        .into_iter()
        .collect::<Vec<_>>();
    let mut derived = Vec::new();

    while let Some(candidate_group_id) = candidates.pop() {
        if let Some(existing) =
            projected_groups.iter().find(|group| group.group_id == candidate_group_id).cloned()
        {
            if let Some(next_parent) = existing.parent_group_id {
                candidates.push(next_parent);
            }
            continue;
        }
        let Some(parent_group) =
            derive_parent_group_terminal(batch, &projected_groups, candidate_group_id, occurred_at)
        else {
            continue;
        };
        let next_parent = parent_group.parent_group_id;
        upsert_projected_group(&mut projected_groups, parent_group.clone());
        derived.push(parent_group);
        if let Some(next_parent) = next_parent {
            candidates.push(next_parent);
        }
    }

    derived.sort_by_key(|group| group.group_id);
    derived
}

pub(super) fn batch_group_index_prefix(identity: &ThroughputBatchIdentity) -> String {
    format!(
        "{BATCH_GROUP_INDEX_PREFIX}{}:{}:{}:{}:",
        identity.tenant_id, identity.instance_id, identity.run_id, identity.batch_id
    )
}

pub(super) fn batch_group_index_key(identity: &ThroughputBatchIdentity, group_id: u32) -> String {
    format!("{}{:010}", batch_group_index_prefix(identity), group_id)
}

fn derive_parent_group_terminal(
    batch: &LocalBatchState,
    groups: &[ProjectedGroupTerminal],
    parent_group_id: u32,
    occurred_at: DateTime<Utc>,
) -> Option<ProjectedGroupTerminal> {
    let child_group_ids = reduction_tree_child_group_ids(
        parent_group_id,
        batch.aggregation_group_count,
        batch.aggregation_tree_depth,
    );
    if child_group_ids.is_empty() {
        return None;
    }

    let children = child_group_ids
        .iter()
        .map(|child_group_id| {
            groups.iter().find(|group| group.group_id == *child_group_id).cloned()
        })
        .collect::<Option<Vec<_>>>()?;

    let mut succeeded_items = 0_u32;
    let mut failed_items = 0_u32;
    let mut cancelled_items = 0_u32;
    let mut error = None;
    let mut terminal_at = occurred_at;
    for child in children {
        succeeded_items = succeeded_items.saturating_add(child.succeeded_items);
        failed_items = failed_items.saturating_add(child.failed_items);
        cancelled_items = cancelled_items.saturating_add(child.cancelled_items);
        if error.is_none() {
            error = child.error.clone();
        }
        terminal_at = terminal_at.max(child.terminal_at);
    }

    let status = if failed_items > 0 {
        "failed"
    } else if cancelled_items > 0 {
        "cancelled"
    } else {
        "completed"
    };

    Some(ProjectedGroupTerminal {
        group_id: parent_group_id,
        level: reduction_tree_node_level(parent_group_id),
        parent_group_id: reduction_tree_parent_group_id(
            parent_group_id,
            batch.aggregation_group_count,
            batch.aggregation_tree_depth,
        ),
        status: status.to_owned(),
        succeeded_items,
        failed_items,
        cancelled_items,
        error,
        terminal_at,
    })
}

fn upsert_projected_group(groups: &mut Vec<ProjectedGroupTerminal>, group: ProjectedGroupTerminal) {
    if let Some(existing) = groups.iter_mut().find(|candidate| candidate.group_id == group.group_id)
    {
        *existing = group;
    } else {
        groups.push(group);
    }
}

fn project_groups_for_batch_from_loaded(
    chunks: &[LocalChunkState],
    groups: &[LocalGroupState],
    pending_group: Option<&ProjectedGroupTerminal>,
    parent_groups: &[ProjectedGroupTerminal],
) -> Vec<ProjectedGroupTerminal> {
    let mut projected_groups = groups
        .iter()
        .map(|group| ProjectedGroupTerminal {
            group_id: group.group_id,
            level: group.level,
            parent_group_id: group.parent_group_id,
            status: group.status.clone(),
            succeeded_items: group.succeeded_items,
            failed_items: group.failed_items,
            cancelled_items: group.cancelled_items,
            error: group.error.clone(),
            terminal_at: group.terminal_at,
        })
        .collect::<Vec<_>>();
    let derived_group_ids = chunks.iter().map(|chunk| chunk.group_id).collect::<HashSet<_>>();
    for group_id in derived_group_ids {
        if projected_groups.iter().any(|group| group.group_id == group_id) {
            continue;
        }
        if let Some(group) = derive_terminal_group_from_chunks(chunks, group_id) {
            projected_groups.push(group);
        }
    }
    if let Some(pending_group) = pending_group {
        if !projected_groups.iter().any(|group| group.group_id == pending_group.group_id) {
            projected_groups.push(pending_group.clone());
        }
    }
    for group in parent_groups {
        if !projected_groups.iter().any(|candidate| candidate.group_id == group.group_id) {
            projected_groups.push(group.clone());
        }
    }
    projected_groups.sort_by_key(|group| group.group_id);
    projected_groups
}

fn summarize_projected_groups(
    groups: &[ProjectedGroupTerminal],
) -> Option<ProjectedBatchGroupSummary> {
    let leaf_groups = groups.iter().filter(|group| group.level == 0).collect::<Vec<_>>();
    if leaf_groups.is_empty() {
        return None;
    }

    let mut succeeded_items = 0_u32;
    let mut failed_items = 0_u32;
    let mut cancelled_items = 0_u32;
    let mut error = None;
    let mut updated_at = DateTime::<Utc>::UNIX_EPOCH;
    for group in leaf_groups {
        succeeded_items = succeeded_items.saturating_add(group.succeeded_items);
        failed_items = failed_items.saturating_add(group.failed_items);
        cancelled_items = cancelled_items.saturating_add(group.cancelled_items);
        if error.is_none() {
            error = group.error.clone();
        }
        updated_at = updated_at.max(group.terminal_at);
    }

    Some(ProjectedBatchGroupSummary {
        succeeded_items,
        failed_items,
        cancelled_items,
        error,
        updated_at,
    })
}

fn project_batch_terminal_from_loaded(
    batch: &LocalBatchState,
    groups: &[ProjectedGroupTerminal],
    occurred_at: DateTime<Utc>,
) -> Option<ProjectedBatchTerminal> {
    if batch.aggregation_group_count <= 1
        || matches!(batch.status.as_str(), "completed" | "failed" | "cancelled")
        || groups.is_empty()
    {
        return None;
    }

    let level_counts =
        reduction_tree_level_counts(batch.aggregation_group_count, batch.aggregation_tree_depth);
    if level_counts.len() > 1 {
        let root_level = (level_counts.len() - 1) as u32;
        let root_group_id = reduction_tree_node_id(root_level, 0);
        if let Some(root) = groups.iter().find(|group| group.group_id == root_group_id) {
            return Some(ProjectedBatchTerminal {
                status: root.status.clone(),
                succeeded_items: root.succeeded_items,
                failed_items: root.failed_items,
                cancelled_items: root.cancelled_items,
                error: root.error.clone().or_else(|| batch.error.clone()),
                terminal_at: root.terminal_at.max(occurred_at),
            });
        }
    }

    let leaf_groups = groups.iter().filter(|group| group.level == 0).collect::<Vec<_>>();
    let mut groups_seen = HashSet::new();
    let mut succeeded_items = 0_u32;
    let mut failed_items = 0_u32;
    let mut cancelled_items = 0_u32;
    let mut batch_error = batch.error.clone();

    for group in leaf_groups {
        groups_seen.insert(group.group_id);
        succeeded_items = succeeded_items.saturating_add(group.succeeded_items);
        failed_items = failed_items.saturating_add(group.failed_items);
        cancelled_items = cancelled_items.saturating_add(group.cancelled_items);
        if batch_error.is_none() {
            batch_error = group.error.clone();
        }
    }

    let status = if failed_items > 0 {
        "failed".to_owned()
    } else if cancelled_items > 0 {
        "cancelled".to_owned()
    } else if u32::try_from(groups_seen.len()).unwrap_or_default() >= batch.aggregation_group_count
        && succeeded_items == batch.total_items
    {
        "completed".to_owned()
    } else {
        return None;
    };

    Some(ProjectedBatchTerminal {
        status,
        succeeded_items,
        failed_items,
        cancelled_items,
        error: batch_error,
        terminal_at: occurred_at,
    })
}
