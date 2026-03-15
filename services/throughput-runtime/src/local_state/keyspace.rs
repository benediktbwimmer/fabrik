use super::*;

pub(super) const STREAM_JOB_VIEW_BINARY_PREFIX: &[u8] = b"sjv1\0";
pub(super) const STREAM_JOB_RUNTIME_BINARY_PREFIX: &[u8] = b"sjr1\0";
pub(super) const STREAM_JOB_CHECKPOINT_BINARY_PREFIX: &[u8] = b"sjc1\0";
pub(super) const STREAM_JOB_DISPATCH_APPLIED_BINARY_PREFIX: &[u8] = b"sjd1\0";

fn append_binary_key_component(buffer: &mut Vec<u8>, component: &str) {
    let bytes = component.as_bytes();
    let len = u16::try_from(bytes.len()).expect("stream job view key component exceeds u16");
    buffer.extend_from_slice(&len.to_be_bytes());
    buffer.extend_from_slice(bytes);
}

fn parse_binary_key_component(key: &[u8], offset: &mut usize) -> Option<String> {
    let len_bytes = key.get(*offset..(*offset + 2))?;
    let len = u16::from_be_bytes([len_bytes[0], len_bytes[1]]) as usize;
    *offset += 2;
    let value = key.get(*offset..(*offset + len))?;
    *offset += len;
    std::str::from_utf8(value).ok().map(str::to_owned)
}

pub(super) fn legacy_cf_for_key(key: &str) -> Option<&'static str> {
    if key.starts_with(OFFSET_KEY_PREFIX)
        || key.starts_with(MIRRORED_ENTRY_KEY_PREFIX)
        || key.starts_with(STREAMS_OFFSET_KEY_PREFIX)
        || key.starts_with(STREAMS_MIRRORED_ENTRY_KEY_PREFIX)
    {
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
    if key.starts_with(STREAM_JOB_RUNTIME_KEY_PREFIX)
        || key.starts_with(STREAM_JOB_CHECKPOINT_KEY_PREFIX)
    {
        return Some(STREAM_JOBS_CF);
    }
    None
}

pub(super) fn offset_key(plane: LocalChangelogPlane, partition_id: i32) -> String {
    match plane {
        LocalChangelogPlane::Throughput => format!("{OFFSET_KEY_PREFIX}{partition_id}"),
        LocalChangelogPlane::Streams => format!("{STREAMS_OFFSET_KEY_PREFIX}{partition_id}"),
    }
}

pub(super) fn mirrored_entry_key(
    plane: LocalChangelogPlane,
    entry_id: impl std::fmt::Display,
) -> String {
    match plane {
        LocalChangelogPlane::Throughput => format!("{MIRRORED_ENTRY_KEY_PREFIX}{entry_id}"),
        LocalChangelogPlane::Streams => format!("{STREAMS_MIRRORED_ENTRY_KEY_PREFIX}{entry_id}"),
    }
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

pub(super) fn legacy_stream_job_view_key(
    handle_id: &str,
    view_name: &str,
    logical_key: &str,
) -> String {
    format!("{STREAM_JOB_VIEW_KEY_PREFIX}{handle_id}:{view_name}:{logical_key}")
}

pub(super) fn legacy_stream_job_view_prefix(handle_id: &str, view_name: &str) -> String {
    format!("{STREAM_JOB_VIEW_KEY_PREFIX}{handle_id}:{view_name}:")
}

pub(super) fn legacy_stream_job_runtime_key(handle_id: &str) -> String {
    format!("{STREAM_JOB_RUNTIME_KEY_PREFIX}{handle_id}")
}

pub(super) fn legacy_stream_job_checkpoint_key(
    handle_id: &str,
    checkpoint_name: &str,
    stream_partition_id: i32,
) -> String {
    format!("{STREAM_JOB_CHECKPOINT_KEY_PREFIX}{handle_id}:{checkpoint_name}:{stream_partition_id}")
}

pub(super) fn stream_job_view_prefix(handle_id: &str, view_name: &str) -> Vec<u8> {
    let mut key = Vec::with_capacity(
        STREAM_JOB_VIEW_BINARY_PREFIX.len() + handle_id.len() + view_name.len() + 5,
    );
    key.extend_from_slice(STREAM_JOB_VIEW_BINARY_PREFIX);
    append_binary_key_component(&mut key, handle_id);
    append_binary_key_component(&mut key, view_name);
    key.push(0);
    key
}

pub(super) fn stream_job_view_key(handle_id: &str, view_name: &str, logical_key: &str) -> Vec<u8> {
    let mut key = stream_job_view_prefix(handle_id, view_name);
    key.extend_from_slice(logical_key.as_bytes());
    key
}

pub(super) fn parse_stream_job_view_key(key: &[u8]) -> Option<(String, String, String)> {
    if !key.starts_with(STREAM_JOB_VIEW_BINARY_PREFIX) {
        return None;
    }
    let mut offset = STREAM_JOB_VIEW_BINARY_PREFIX.len();
    let handle_id = parse_binary_key_component(key, &mut offset)?;
    let view_name = parse_binary_key_component(key, &mut offset)?;
    if *key.get(offset)? != 0 {
        return None;
    }
    offset += 1;
    let logical_key = std::str::from_utf8(key.get(offset..)?).ok()?.to_owned();
    Some((handle_id, view_name, logical_key))
}

pub(super) fn stream_job_runtime_key(handle_id: &str) -> Vec<u8> {
    let mut key = Vec::with_capacity(STREAM_JOB_RUNTIME_BINARY_PREFIX.len() + handle_id.len() + 2);
    key.extend_from_slice(STREAM_JOB_RUNTIME_BINARY_PREFIX);
    append_binary_key_component(&mut key, handle_id);
    key
}

pub(super) fn stream_job_dispatch_applied_prefix(handle_id: &str) -> Vec<u8> {
    let mut key =
        Vec::with_capacity(STREAM_JOB_DISPATCH_APPLIED_BINARY_PREFIX.len() + handle_id.len() + 3);
    key.extend_from_slice(STREAM_JOB_DISPATCH_APPLIED_BINARY_PREFIX);
    append_binary_key_component(&mut key, handle_id);
    key.push(0);
    key
}

pub(super) fn stream_job_dispatch_applied_key(handle_id: &str, batch_id: &str) -> Vec<u8> {
    let mut key = stream_job_dispatch_applied_prefix(handle_id);
    key.extend_from_slice(batch_id.as_bytes());
    key
}

pub(super) fn stream_job_checkpoint_key(
    handle_id: &str,
    checkpoint_name: &str,
    stream_partition_id: i32,
) -> Vec<u8> {
    let mut key = Vec::with_capacity(
        STREAM_JOB_CHECKPOINT_BINARY_PREFIX.len() + handle_id.len() + checkpoint_name.len() + 8,
    );
    key.extend_from_slice(STREAM_JOB_CHECKPOINT_BINARY_PREFIX);
    append_binary_key_component(&mut key, handle_id);
    append_binary_key_component(&mut key, checkpoint_name);
    key.extend_from_slice(&stream_partition_id.to_be_bytes());
    key
}

pub(super) fn throughput_partition_id(batch_id: &str, group_id: u32, partition_count: i32) -> i32 {
    fabrik_broker::partition_for_key(
        &fabrik_throughput::throughput_partition_key(batch_id, group_id),
        partition_count,
    )
}
