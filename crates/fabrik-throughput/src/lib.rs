use std::{
    fs,
    path::{Path, PathBuf},
};

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

pub const PG_V1_BACKEND: &str = "pg-v1";
pub const STREAM_V2_BACKEND: &str = "stream-v2";
pub const STREAM_V2_BACKEND_VERSION: &str = "2.0.0";
pub const DEFAULT_AGGREGATION_GROUP_COUNT: u32 = 1;
pub const DEFAULT_GROUP_ID: u32 = 0;
pub const INITIAL_OWNER_EPOCH: u64 = 1;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum ThroughputBackend {
    PgV1,
    StreamV2,
}

impl ThroughputBackend {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::PgV1 => PG_V1_BACKEND,
            Self::StreamV2 => STREAM_V2_BACKEND,
        }
    }

    pub fn default_version(&self) -> &'static str {
        match self {
            Self::PgV1 => "1.0.0",
            Self::StreamV2 => STREAM_V2_BACKEND_VERSION,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum PayloadHandle {
    Inline { key: String },
    Manifest { key: String, store: String },
}

impl PayloadHandle {
    pub fn inline_batch_input(batch_id: &str) -> Self {
        Self::Inline { key: format!("bulk-input:{batch_id}") }
    }

    pub fn inline_batch_result(batch_id: &str) -> Self {
        Self::Inline { key: format!("bulk-result:{batch_id}") }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ThroughputBatchIdentity {
    pub tenant_id: String,
    pub instance_id: String,
    pub run_id: String,
    pub batch_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CreateBatchCommand {
    pub dedupe_key: String,
    pub tenant_id: String,
    pub definition_id: String,
    pub definition_version: u32,
    pub artifact_hash: String,
    pub instance_id: String,
    pub run_id: String,
    pub batch_id: String,
    pub activity_type: String,
    pub task_queue: String,
    pub state: Option<String>,
    pub chunk_size: u32,
    pub max_attempts: u32,
    pub retry_delay_ms: u64,
    pub total_items: u32,
    pub aggregation_group_count: u32,
    pub throughput_backend: String,
    pub throughput_backend_version: String,
    pub input_handle: PayloadHandle,
    pub result_handle: PayloadHandle,
    pub items: Vec<Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ThroughputCommand {
    CreateBatch(CreateBatchCommand),
    CancelBatch { identity: ThroughputBatchIdentity, reason: String },
    TimeoutBatch { identity: ThroughputBatchIdentity, reason: String },
    ReplanGroups { identity: ThroughputBatchIdentity, aggregation_group_count: u32 },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ThroughputCommandEnvelope {
    pub command_id: Uuid,
    pub occurred_at: DateTime<Utc>,
    pub dedupe_key: String,
    pub partition_key: String,
    pub payload: ThroughputCommand,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ThroughputChunkReportPayload {
    ChunkCompleted { output: Vec<Value> },
    ChunkFailed { error: String },
    ChunkCancelled { reason: String, metadata: Option<Value> },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ThroughputChunkReport {
    pub report_id: String,
    pub tenant_id: String,
    pub instance_id: String,
    pub run_id: String,
    pub batch_id: String,
    pub chunk_id: String,
    pub chunk_index: u32,
    pub group_id: u32,
    pub attempt: u32,
    pub lease_epoch: u64,
    pub owner_epoch: u64,
    pub worker_id: String,
    pub worker_build_id: String,
    pub lease_token: String,
    pub occurred_at: DateTime<Utc>,
    pub payload: ThroughputChunkReportPayload,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ThroughputReportEnvelope {
    pub report_id: String,
    pub occurred_at: DateTime<Utc>,
    pub partition_key: String,
    pub payload: ThroughputChunkReport,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ThroughputChangelogPayload {
    BatchCreated {
        identity: ThroughputBatchIdentity,
        task_queue: String,
        aggregation_group_count: u32,
        total_items: u32,
        chunk_count: u32,
    },
    ChunkLeased {
        identity: ThroughputBatchIdentity,
        chunk_id: String,
        chunk_index: u32,
        attempt: u32,
        group_id: u32,
        item_count: u32,
        max_attempts: u32,
        retry_delay_ms: u64,
        lease_epoch: u64,
        owner_epoch: u64,
        worker_id: String,
        lease_expires_at: DateTime<Utc>,
    },
    ChunkApplied {
        identity: ThroughputBatchIdentity,
        chunk_id: String,
        chunk_index: u32,
        attempt: u32,
        group_id: u32,
        item_count: u32,
        max_attempts: u32,
        retry_delay_ms: u64,
        lease_epoch: u64,
        owner_epoch: u64,
        report_id: String,
        status: String,
        available_at: DateTime<Utc>,
        error: Option<String>,
    },
    ChunkRequeued {
        identity: ThroughputBatchIdentity,
        chunk_id: String,
        chunk_index: u32,
        attempt: u32,
        group_id: u32,
        item_count: u32,
        max_attempts: u32,
        retry_delay_ms: u64,
        lease_epoch: u64,
        owner_epoch: u64,
        available_at: DateTime<Utc>,
    },
    BatchTerminal {
        identity: ThroughputBatchIdentity,
        status: String,
        report_id: String,
        succeeded_items: u32,
        failed_items: u32,
        cancelled_items: u32,
        error: Option<String>,
        terminal_at: DateTime<Utc>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ThroughputChangelogEntry {
    pub entry_id: Uuid,
    pub occurred_at: DateTime<Utc>,
    pub partition_key: String,
    pub payload: ThroughputChangelogPayload,
}

pub fn throughput_partition_key(batch_id: &str, group_id: u32) -> String {
    format!("{batch_id}:{group_id}")
}

pub fn effective_aggregation_group_count(requested_group_count: u32, chunk_count: u32) -> u32 {
    requested_group_count.max(1).min(chunk_count.max(1))
}

pub fn group_id_for_chunk_index(chunk_index: u32, aggregation_group_count: u32) -> u32 {
    if aggregation_group_count <= 1 {
        DEFAULT_GROUP_ID
    } else {
        chunk_index % aggregation_group_count
    }
}

pub const LOCAL_FILESYSTEM_STORE: &str = "localfs";

#[derive(Debug, Clone)]
pub struct FilesystemPayloadStore {
    root: PathBuf,
}

impl FilesystemPayloadStore {
    pub fn new(root: impl Into<PathBuf>) -> Result<Self> {
        let root = root.into();
        fs::create_dir_all(&root)
            .with_context(|| format!("failed to create throughput payload root {}", root.display()))?;
        Ok(Self { root })
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    pub fn write_value(&self, key: &str, value: &Value) -> Result<PayloadHandle> {
        let path = self.path_for_key(key);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).with_context(|| {
                format!("failed to create payload parent directory {}", parent.display())
            })?;
        }
        fs::write(
            &path,
            serde_json::to_vec(value).context("failed to serialize payload value")?,
        )
        .with_context(|| format!("failed to write payload file {}", path.display()))?;
        Ok(PayloadHandle::Manifest {
            key: key.to_owned(),
            store: LOCAL_FILESYSTEM_STORE.to_owned(),
        })
    }

    pub fn read_value(&self, handle: &PayloadHandle) -> Result<Value> {
        let path = self.path_from_handle(handle)?;
        let bytes =
            fs::read(&path).with_context(|| format!("failed to read payload file {}", path.display()))?;
        serde_json::from_slice(&bytes).context("failed to deserialize payload value")
    }

    pub fn read_items(&self, handle: &PayloadHandle) -> Result<Vec<Value>> {
        let value = self.read_value(handle)?;
        match value {
            Value::Array(items) => Ok(items),
            other => anyhow::bail!("payload handle did not resolve to an array: {other}"),
        }
    }

    fn path_from_handle(&self, handle: &PayloadHandle) -> Result<PathBuf> {
        match handle {
            PayloadHandle::Manifest { key, store } if store == LOCAL_FILESYSTEM_STORE => {
                Ok(self.path_for_key(key))
            }
            PayloadHandle::Manifest { store, .. } => {
                anyhow::bail!("unsupported payload store {store}")
            }
            PayloadHandle::Inline { .. } => anyhow::bail!("inline payload handle has no manifest path"),
        }
    }

    fn path_for_key(&self, key: &str) -> PathBuf {
        self.root.join(format!("{key}.json"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    #[test]
    fn filesystem_payload_store_round_trips_arrays() -> Result<()> {
        let root = std::env::temp_dir().join(format!("fabrik-throughput-payload-{}", Uuid::now_v7()));
        let store = FilesystemPayloadStore::new(&root)?;
        let handle = store.write_value(
            "batches/batch-a/chunks/chunk-a/input",
            &Value::Array(vec![Value::from(1), Value::from("two")]),
        )?;
        let items = store.read_items(&handle)?;
        assert_eq!(items, vec![Value::from(1), Value::from("two")]);
        let _ = fs::remove_dir_all(root);
        Ok(())
    }
}
