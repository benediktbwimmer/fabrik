use super::*;

pub(super) fn legacy_cf_for_key(key: &str) -> Option<&'static str> {
    if key.starts_with(OFFSET_KEY_PREFIX) || key.starts_with(MIRRORED_ENTRY_KEY_PREFIX) {
        return Some(META_CF);
    }
    if key.starts_with(BATCH_KEY_PREFIX) {
        return Some(BATCHES_CF);
    }
    if key.starts_with(CHUNK_KEY_PREFIX) {
        return Some(CHUNKS_CF);
    }
    if key.starts_with(GROUP_KEY_PREFIX) {
        return Some(GROUPS_CF);
    }
    if key.starts_with(BATCH_CHUNK_INDEX_PREFIX) || key.starts_with(BATCH_GROUP_INDEX_PREFIX) {
        return Some(BATCH_INDEXES_CF);
    }
    if key.starts_with(READY_INDEX_PREFIX)
        || key.starts_with(STARTED_INDEX_PREFIX)
        || key.starts_with(LEASE_EXPIRY_INDEX_PREFIX)
    {
        return Some(SCHEDULING_CF);
    }
    if key.starts_with(STREAM_JOB_VIEW_KEY_PREFIX) {
        return Some(STREAM_JOBS_CF);
    }
    None
}

pub(super) fn offset_key(partition_id: i32) -> String {
    format!("{OFFSET_KEY_PREFIX}{partition_id}")
}

pub(super) fn mirrored_entry_key(entry_id: impl std::fmt::Display) -> String {
    format!("{MIRRORED_ENTRY_KEY_PREFIX}{entry_id}")
}

pub(super) fn batch_key(identity: &ThroughputBatchIdentity) -> String {
    format!(
        "{BATCH_KEY_PREFIX}{}:{}:{}:{}",
        identity.tenant_id, identity.instance_id, identity.run_id, identity.batch_id
    )
}

pub(super) fn chunk_key(identity: &ThroughputBatchIdentity, chunk_id: &str) -> String {
    format!(
        "{CHUNK_KEY_PREFIX}{}:{}:{}:{}:{}",
        identity.tenant_id, identity.instance_id, identity.run_id, identity.batch_id, chunk_id
    )
}

pub(super) fn group_key(identity: &ThroughputBatchIdentity, group_id: u32) -> String {
    format!(
        "{GROUP_KEY_PREFIX}{}:{}:{}:{}:{}",
        identity.tenant_id, identity.instance_id, identity.run_id, identity.batch_id, group_id
    )
}

pub(super) fn batch_chunk_index_prefix(identity: &ThroughputBatchIdentity) -> String {
    format!(
        "{BATCH_CHUNK_INDEX_PREFIX}{}:{}:{}:{}:",
        identity.tenant_id, identity.instance_id, identity.run_id, identity.batch_id
    )
}

pub(super) fn batch_chunk_index_key(identity: &ThroughputBatchIdentity, chunk_id: &str) -> String {
    format!("{}{}", batch_chunk_index_prefix(identity), chunk_id)
}

pub(super) fn timestamp_sort_key(timestamp: DateTime<Utc>) -> String {
    format!("{:020}", timestamp.timestamp_millis())
}

pub(super) fn stream_job_view_key(handle_id: &str, view_name: &str, logical_key: &str) -> String {
    format!("{STREAM_JOB_VIEW_KEY_PREFIX}{handle_id}:{view_name}:{logical_key}")
}

pub(super) fn throughput_partition_id(batch_id: &str, group_id: u32, partition_count: i32) -> i32 {
    fabrik_broker::partition_for_key(
        &fabrik_throughput::throughput_partition_key(batch_id, group_id),
        partition_count,
    )
}
