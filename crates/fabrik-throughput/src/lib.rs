use std::{
    fs,
    path::{Path, PathBuf},
};

use anyhow::{Context, Result};
use aws_config::BehaviorVersion;
use aws_sdk_s3::{
    Client as S3Client,
    config::{Builder as S3ConfigBuilder, Credentials, Region},
    primitives::ByteStream,
};
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
    ChunkCompleted {
        result_handle: Value,
        #[serde(default)]
        output: Option<Vec<Value>>,
    },
    ChunkFailed {
        error: String,
    },
    ChunkCancelled {
        reason: String,
        metadata: Option<Value>,
    },
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
        lease_token: String,
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
        result_handle: Option<Value>,
        output: Option<Vec<Value>>,
        error: Option<String>,
        cancellation_reason: Option<String>,
        cancellation_metadata: Option<Value>,
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
    GroupTerminal {
        identity: ThroughputBatchIdentity,
        group_id: u32,
        status: String,
        succeeded_items: u32,
        failed_items: u32,
        cancelled_items: u32,
        error: Option<String>,
        terminal_at: DateTime<Utc>,
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
pub const S3_STORE: &str = "s3";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PayloadStoreKind {
    LocalFilesystem,
    S3,
}

impl PayloadStoreKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::LocalFilesystem => LOCAL_FILESYSTEM_STORE,
            Self::S3 => S3_STORE,
        }
    }

    pub fn parse(raw: &str) -> Result<Self> {
        match raw {
            LOCAL_FILESYSTEM_STORE => Ok(Self::LocalFilesystem),
            S3_STORE => Ok(Self::S3),
            _ => anyhow::bail!("unsupported throughput payload store kind {raw}"),
        }
    }
}

#[derive(Debug, Clone)]
pub struct PayloadStoreConfig {
    pub default_store: PayloadStoreKind,
    pub local_dir: String,
    pub s3_bucket: Option<String>,
    pub s3_region: String,
    pub s3_endpoint: Option<String>,
    pub s3_access_key_id: Option<String>,
    pub s3_secret_access_key: Option<String>,
    pub s3_force_path_style: bool,
    pub s3_key_prefix: String,
}

#[derive(Debug, Clone)]
pub struct FilesystemPayloadStore {
    root: PathBuf,
}

impl FilesystemPayloadStore {
    pub fn new(root: impl Into<PathBuf>) -> Result<Self> {
        let root = root.into();
        fs::create_dir_all(&root).with_context(|| {
            format!("failed to create throughput payload root {}", root.display())
        })?;
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
        fs::write(&path, serde_json::to_vec(value).context("failed to serialize payload value")?)
            .with_context(|| format!("failed to write payload file {}", path.display()))?;
        Ok(PayloadHandle::Manifest {
            key: key.to_owned(),
            store: LOCAL_FILESYSTEM_STORE.to_owned(),
        })
    }

    pub fn read_value(&self, handle: &PayloadHandle) -> Result<Value> {
        let path = self.path_from_handle(handle)?;
        let bytes = fs::read(&path)
            .with_context(|| format!("failed to read payload file {}", path.display()))?;
        serde_json::from_slice(&bytes).context("failed to deserialize payload value")
    }

    pub fn read_items(&self, handle: &PayloadHandle) -> Result<Vec<Value>> {
        let value = self.read_value(handle)?;
        match value {
            Value::Array(items) => Ok(items),
            other => anyhow::bail!("payload handle did not resolve to an array: {other}"),
        }
    }

    pub fn list_keys(&self, prefix: &str) -> Result<Vec<String>> {
        let mut keys = Vec::new();
        self.collect_keys(&self.root, prefix, &mut keys)?;
        keys.sort();
        Ok(keys)
    }

    pub fn delete_key(&self, key: &str) -> Result<()> {
        let path = self.path_for_key(key);
        if path.exists() {
            fs::remove_file(&path)
                .with_context(|| format!("failed to remove payload file {}", path.display()))?;
        }
        Ok(())
    }

    fn path_from_handle(&self, handle: &PayloadHandle) -> Result<PathBuf> {
        match handle {
            PayloadHandle::Manifest { key, store } if store == LOCAL_FILESYSTEM_STORE => {
                Ok(self.path_for_key(key))
            }
            PayloadHandle::Manifest { store, .. } => {
                anyhow::bail!("unsupported payload store {store}")
            }
            PayloadHandle::Inline { .. } => {
                anyhow::bail!("inline payload handle has no manifest path")
            }
        }
    }

    fn path_for_key(&self, key: &str) -> PathBuf {
        self.root.join(format!("{key}.json"))
    }

    fn collect_keys(&self, dir: &Path, prefix: &str, keys: &mut Vec<String>) -> Result<()> {
        if !dir.exists() {
            return Ok(());
        }
        for entry in fs::read_dir(dir)
            .with_context(|| format!("failed to read payload directory {}", dir.display()))?
        {
            let entry = entry
                .with_context(|| format!("failed to read payload entry in {}", dir.display()))?;
            let path = entry.path();
            if path.is_dir() {
                self.collect_keys(&path, prefix, keys)?;
                continue;
            }
            if path.extension().and_then(|value| value.to_str()) != Some("json") {
                continue;
            }
            let relative = path
                .strip_prefix(&self.root)
                .with_context(|| {
                    format!(
                        "failed to strip payload root {} from {}",
                        self.root.display(),
                        path.display()
                    )
                })?
                .to_string_lossy()
                .replace('\\', "/");
            let key = relative.strip_suffix(".json").unwrap_or(&relative).to_owned();
            if key.starts_with(prefix) {
                keys.push(key);
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct S3PayloadStore {
    client: S3Client,
    bucket: String,
    key_prefix: String,
}

impl S3PayloadStore {
    pub async fn new(
        bucket: impl Into<String>,
        region: impl Into<String>,
        endpoint: Option<String>,
        access_key_id: Option<String>,
        secret_access_key: Option<String>,
        force_path_style: bool,
        key_prefix: impl Into<String>,
    ) -> Result<Self> {
        let bucket = bucket.into();
        let mut loader =
            aws_config::defaults(BehaviorVersion::latest()).region(Region::new(region.into()));
        if let (Some(access_key_id), Some(secret_access_key)) = (access_key_id, secret_access_key) {
            loader = loader.credentials_provider(Credentials::new(
                access_key_id,
                secret_access_key,
                None,
                None,
                "fabrik-throughput",
            ));
        }
        let shared_config = loader.load().await;
        let mut builder = S3ConfigBuilder::from(&shared_config);
        if let Some(endpoint) = endpoint {
            builder = builder.endpoint_url(endpoint);
        }
        if force_path_style {
            builder = builder.force_path_style(true);
        }
        let client = S3Client::from_conf(builder.build());
        Ok(Self { client, bucket, key_prefix: normalize_prefix(key_prefix.into()) })
    }

    pub async fn write_value(&self, key: &str, value: &Value) -> Result<PayloadHandle> {
        let bytes = serde_json::to_vec(value).context("failed to serialize payload value")?;
        self.client
            .put_object()
            .bucket(&self.bucket)
            .key(self.object_key(key))
            .content_type("application/json")
            .body(ByteStream::from(bytes))
            .send()
            .await
            .context("failed to write payload object to s3")?;
        Ok(PayloadHandle::Manifest { key: key.to_owned(), store: S3_STORE.to_owned() })
    }

    pub async fn read_value(&self, handle: &PayloadHandle) -> Result<Value> {
        let key = self.key_from_handle(handle)?;
        let response = self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await
            .context("failed to read payload object from s3")?;
        let bytes = response
            .body
            .collect()
            .await
            .context("failed to collect payload object body")?
            .into_bytes();
        serde_json::from_slice(&bytes).context("failed to deserialize payload value")
    }

    pub async fn list_keys(&self, prefix: &str) -> Result<Vec<String>> {
        let mut keys = Vec::new();
        let full_prefix = self.object_prefix(prefix);
        let mut continuation_token = None;
        loop {
            let mut request =
                self.client.list_objects_v2().bucket(&self.bucket).prefix(&full_prefix);
            if let Some(token) = continuation_token.as_deref() {
                request = request.continuation_token(token);
            }
            let response =
                request.send().await.context("failed to list payload objects from s3")?;
            for object in response.contents() {
                let Some(key) = object.key() else {
                    continue;
                };
                if let Some(logical_key) = self.logical_key_from_object_key(key) {
                    keys.push(logical_key);
                }
            }
            if response.is_truncated().unwrap_or(false) {
                continuation_token = response.next_continuation_token().map(ToOwned::to_owned);
            } else {
                break;
            }
        }
        keys.sort();
        Ok(keys)
    }

    pub async fn delete_key(&self, key: &str) -> Result<()> {
        self.client
            .delete_object()
            .bucket(&self.bucket)
            .key(self.object_key(key))
            .send()
            .await
            .context("failed to delete payload object from s3")?;
        Ok(())
    }

    fn key_from_handle(&self, handle: &PayloadHandle) -> Result<String> {
        match handle {
            PayloadHandle::Manifest { key, store } if store == S3_STORE => Ok(self.object_key(key)),
            PayloadHandle::Manifest { store, .. } => {
                anyhow::bail!("payload handle store {store} does not match configured s3 store")
            }
            PayloadHandle::Inline { .. } => anyhow::bail!("inline payload handle has no s3 object"),
        }
    }

    fn object_key(&self, key: &str) -> String {
        if self.key_prefix.is_empty() {
            format!("{key}.json")
        } else {
            format!("{}/{key}.json", self.key_prefix)
        }
    }

    fn object_prefix(&self, prefix: &str) -> String {
        if self.key_prefix.is_empty() {
            prefix.to_owned()
        } else if prefix.is_empty() {
            format!("{}/", self.key_prefix)
        } else {
            format!("{}/{}", self.key_prefix, prefix)
        }
    }

    fn logical_key_from_object_key(&self, object_key: &str) -> Option<String> {
        let without_root = if self.key_prefix.is_empty() {
            object_key
        } else {
            object_key.strip_prefix(&format!("{}/", self.key_prefix))?
        };
        let logical = without_root.strip_suffix(".json")?;
        Some(logical.to_owned())
    }
}

#[derive(Debug, Clone)]
pub struct PayloadStore {
    default_store: PayloadStoreKind,
    filesystem: Option<FilesystemPayloadStore>,
    s3: Option<S3PayloadStore>,
}

impl PayloadStore {
    pub async fn from_config(config: PayloadStoreConfig) -> Result<Self> {
        let filesystem = Some(FilesystemPayloadStore::new(&config.local_dir)?);
        let s3 = match config.default_store {
            PayloadStoreKind::S3 => {
                let bucket = config
                    .s3_bucket
                    .clone()
                    .filter(|value| !value.trim().is_empty())
                    .ok_or_else(|| anyhow::anyhow!("THROUGHPUT_PAYLOAD_S3_BUCKET is required when THROUGHPUT_PAYLOAD_STORE=s3"))?;
                Some(
                    S3PayloadStore::new(
                        bucket,
                        config.s3_region,
                        config.s3_endpoint,
                        config.s3_access_key_id,
                        config.s3_secret_access_key,
                        config.s3_force_path_style,
                        config.s3_key_prefix,
                    )
                    .await?,
                )
            }
            PayloadStoreKind::LocalFilesystem => {
                if let Some(bucket) = config.s3_bucket.filter(|value| !value.trim().is_empty()) {
                    Some(
                        S3PayloadStore::new(
                            bucket,
                            config.s3_region,
                            config.s3_endpoint,
                            config.s3_access_key_id,
                            config.s3_secret_access_key,
                            config.s3_force_path_style,
                            config.s3_key_prefix,
                        )
                        .await?,
                    )
                } else {
                    None
                }
            }
        };
        Ok(Self { default_store: config.default_store, filesystem, s3 })
    }

    pub fn default_store_kind(&self) -> PayloadStoreKind {
        self.default_store
    }

    pub async fn write_value(&self, key: &str, value: &Value) -> Result<PayloadHandle> {
        match self.default_store {
            PayloadStoreKind::LocalFilesystem => self
                .filesystem
                .as_ref()
                .context("filesystem payload store not configured")?
                .write_value(key, value),
            PayloadStoreKind::S3 => {
                self.s3
                    .as_ref()
                    .context("s3 payload store not configured")?
                    .write_value(key, value)
                    .await
            }
        }
    }

    pub async fn read_value(&self, handle: &PayloadHandle) -> Result<Value> {
        match handle {
            PayloadHandle::Manifest { store, .. } if store == LOCAL_FILESYSTEM_STORE => self
                .filesystem
                .as_ref()
                .context("filesystem payload store not configured")?
                .read_value(handle),
            PayloadHandle::Manifest { store, .. } if store == S3_STORE => {
                self.s3
                    .as_ref()
                    .context("s3 payload store not configured")?
                    .read_value(handle)
                    .await
            }
            PayloadHandle::Manifest { store, .. } => {
                anyhow::bail!("unsupported payload store {store}")
            }
            PayloadHandle::Inline { .. } => {
                anyhow::bail!("inline payload handle has no manifest payload")
            }
        }
    }

    pub async fn read_items(&self, handle: &PayloadHandle) -> Result<Vec<Value>> {
        let value = self.read_value(handle).await?;
        match value {
            Value::Array(items) => Ok(items),
            other => anyhow::bail!("payload handle did not resolve to an array: {other}"),
        }
    }

    pub async fn list_keys(&self, prefix: &str) -> Result<Vec<String>> {
        match self.default_store {
            PayloadStoreKind::LocalFilesystem => self
                .filesystem
                .as_ref()
                .context("filesystem payload store not configured")?
                .list_keys(prefix),
            PayloadStoreKind::S3 => {
                self.s3.as_ref().context("s3 payload store not configured")?.list_keys(prefix).await
            }
        }
    }

    pub async fn delete_key(&self, key: &str) -> Result<()> {
        match self.default_store {
            PayloadStoreKind::LocalFilesystem => self
                .filesystem
                .as_ref()
                .context("filesystem payload store not configured")?
                .delete_key(key),
            PayloadStoreKind::S3 => {
                self.s3.as_ref().context("s3 payload store not configured")?.delete_key(key).await
            }
        }
    }
}

fn normalize_prefix(prefix: String) -> String {
    prefix.trim_matches('/').to_owned()
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    #[test]
    fn filesystem_payload_store_round_trips_arrays() -> Result<()> {
        let root =
            std::env::temp_dir().join(format!("fabrik-throughput-payload-{}", Uuid::now_v7()));
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

    #[test]
    fn payload_store_kind_parses_supported_values() -> Result<()> {
        assert_eq!(
            PayloadStoreKind::parse(LOCAL_FILESYSTEM_STORE)?,
            PayloadStoreKind::LocalFilesystem
        );
        assert_eq!(PayloadStoreKind::parse(S3_STORE)?, PayloadStoreKind::S3);
        Ok(())
    }
}
