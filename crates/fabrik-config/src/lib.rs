use std::{collections::BTreeMap, env};

use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HttpServiceConfig {
    pub name: String,
    pub port: u16,
    pub log_filter: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GrpcServiceConfig {
    pub name: String,
    pub port: u16,
    pub log_filter: String,
}

impl GrpcServiceConfig {
    pub fn from_env(
        env_prefix: &str,
        default_name: &str,
        default_port: u16,
    ) -> Result<Self, ConfigError> {
        let port_key = format!("{env_prefix}_PORT");
        let port = match env::var(&port_key) {
            Ok(raw) => raw
                .parse::<u16>()
                .map_err(|_| ConfigError::InvalidPort { key: port_key.clone(), value: raw })?,
            Err(env::VarError::NotPresent) => default_port,
            Err(err) => {
                return Err(ConfigError::UnreadableEnv { key: port_key, source: err });
            }
        };

        let log_filter = env::var("RUST_LOG").unwrap_or_else(|_| "info".to_owned());

        Ok(Self { name: default_name.to_owned(), port, log_filter })
    }
}

impl HttpServiceConfig {
    pub fn from_env(
        env_prefix: &str,
        default_name: &str,
        default_port: u16,
    ) -> Result<Self, ConfigError> {
        let port_key = format!("{env_prefix}_PORT");
        let port = match env::var(&port_key) {
            Ok(raw) => raw
                .parse::<u16>()
                .map_err(|_| ConfigError::InvalidPort { key: port_key.clone(), value: raw })?,
            Err(env::VarError::NotPresent) => default_port,
            Err(err) => {
                return Err(ConfigError::UnreadableEnv { key: port_key, source: err });
            }
        };

        let log_filter = env::var("RUST_LOG").unwrap_or_else(|_| "info".to_owned());

        Ok(Self { name: default_name.to_owned(), port, log_filter })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RedpandaConfig {
    pub brokers: String,
    pub workflow_events_topic: String,
    pub workflow_events_partitions: i32,
    pub throughput_commands_topic: String,
    pub throughput_reports_topic: String,
    pub throughput_changelog_topic: String,
    pub throughput_projections_topic: String,
    pub throughput_partitions: i32,
}

impl RedpandaConfig {
    pub fn from_env() -> Result<Self, ConfigError> {
        Ok(Self {
            brokers: read_string_with_default("REDPANDA_BROKERS", "localhost:29092")?,
            workflow_events_topic: read_string_with_default(
                "WORKFLOW_EVENTS_TOPIC",
                "workflow-events",
            )?,
            workflow_events_partitions: read_i32_with_default("WORKFLOW_EVENTS_PARTITIONS", 4)?,
            throughput_commands_topic: read_string_with_default(
                "THROUGHPUT_COMMANDS_TOPIC",
                "throughput-commands",
            )?,
            throughput_reports_topic: read_string_with_default(
                "THROUGHPUT_REPORTS_TOPIC",
                "throughput-reports",
            )?,
            throughput_changelog_topic: read_string_with_default(
                "THROUGHPUT_CHANGELOG_TOPIC",
                "throughput-changelog",
            )?,
            throughput_projections_topic: read_string_with_default(
                "THROUGHPUT_PROJECTIONS_TOPIC",
                "throughput-projections",
            )?,
            throughput_partitions: read_i32_with_default("THROUGHPUT_PARTITIONS", 4)?,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PostgresConfig {
    pub url: String,
}

impl PostgresConfig {
    pub fn from_env() -> Result<Self, ConfigError> {
        Ok(Self {
            url: read_string_with_default(
                "POSTGRES_URL",
                "postgres://fabrik:fabrik@localhost:55433/fabrik",
            )?,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueryRuntimeConfig {
    pub default_page_size: usize,
    pub max_page_size: usize,
    pub throughput_payload_store: ThroughputPayloadStoreConfig,
    pub history_retention_days: Option<u64>,
    pub run_retention_days: Option<u64>,
    pub activity_retention_days: Option<u64>,
    pub signal_retention_days: Option<u64>,
    pub snapshot_retention_days: Option<u64>,
    pub retention_sweep_interval_seconds: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MatchingRuntimeConfig {
    pub lease_ttl_seconds: u64,
    pub heartbeat_publish_interval_ms: u64,
    pub sweep_interval_ms: u64,
    pub max_rebuild_tasks: i64,
    pub activity_result_apply_batch_max_items: usize,
    pub activity_result_apply_batch_max_bytes: usize,
    pub activity_result_apply_flush_interval_ms: u64,
    pub result_apply_per_run_coalescing_cap: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ThroughputRuntimeConfig {
    pub lease_ttl_seconds: u64,
    pub sweep_interval_ms: u64,
    pub poll_max_tasks: usize,
    pub report_apply_batch_size: usize,
    pub changelog_publish_batch_size: usize,
    pub projection_publish_batch_size: usize,
    pub max_active_chunks_per_batch: usize,
    pub max_active_chunks_per_tenant: usize,
    pub max_active_chunks_per_task_queue: usize,
    pub local_state_dir: String,
    pub checkpoint_dir: String,
    pub checkpoint_interval_seconds: u64,
    pub checkpoint_retention: usize,
    pub checkpoint_key_prefix: String,
    pub terminal_state_retention_seconds: u64,
    pub terminal_gc_interval_seconds: u64,
    pub restore_idle_timeout_ms: u64,
    pub payload_store: ThroughputPayloadStoreConfig,
    pub inline_chunk_input_threshold_bytes: usize,
    pub inline_chunk_output_threshold_bytes: usize,
    pub grouping_chunk_threshold: usize,
    pub target_chunks_per_group: usize,
    pub max_aggregation_groups: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ThroughputPayloadStoreConfig {
    pub kind: ThroughputPayloadStoreKind,
    pub local_dir: String,
    pub s3_bucket: Option<String>,
    pub s3_region: String,
    pub s3_endpoint: Option<String>,
    pub s3_access_key_id: Option<String>,
    pub s3_secret_access_key: Option<String>,
    pub s3_force_path_style: bool,
    pub s3_key_prefix: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ThroughputPayloadStoreKind {
    LocalFilesystem,
    S3,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ThroughputOwnershipConfig {
    pub static_partition_ids: Option<Vec<i32>>,
    pub runtime_capacity: usize,
    pub member_heartbeat_ttl_seconds: u64,
    pub assignment_poll_interval_seconds: u64,
    pub rebalance_interval_seconds: u64,
    pub lease_ttl_seconds: u64,
    pub renew_interval_seconds: u64,
    pub partition_id_offset: i32,
}

impl ThroughputRuntimeConfig {
    pub fn from_env() -> Result<Self, ConfigError> {
        Ok(Self {
            lease_ttl_seconds: read_u64_with_default("THROUGHPUT_LEASE_TTL_SECONDS", 30)?,
            sweep_interval_ms: read_u64_with_default("THROUGHPUT_SWEEP_INTERVAL_MS", 500)?,
            poll_max_tasks: read_usize_with_default("THROUGHPUT_POLL_MAX_TASKS", 32)?,
            report_apply_batch_size: read_usize_with_default(
                "THROUGHPUT_REPORT_APPLY_BATCH_SIZE",
                64,
            )?,
            changelog_publish_batch_size: read_usize_with_default(
                "THROUGHPUT_CHANGELOG_PUBLISH_BATCH_SIZE",
                128,
            )?,
            projection_publish_batch_size: read_usize_with_default(
                "THROUGHPUT_PROJECTION_PUBLISH_BATCH_SIZE",
                128,
            )?,
            max_active_chunks_per_batch: read_usize_with_default(
                "THROUGHPUT_MAX_ACTIVE_CHUNKS_PER_BATCH",
                256,
            )?,
            max_active_chunks_per_tenant: read_usize_with_default(
                "THROUGHPUT_MAX_ACTIVE_CHUNKS_PER_TENANT",
                4_096,
            )?,
            max_active_chunks_per_task_queue: read_usize_with_default(
                "THROUGHPUT_MAX_ACTIVE_CHUNKS_PER_TASK_QUEUE",
                2_048,
            )?,
            local_state_dir: read_string_with_default(
                "THROUGHPUT_LOCAL_STATE_DIR",
                "/tmp/fabrik-throughput/state",
            )?,
            checkpoint_dir: read_string_with_default(
                "THROUGHPUT_CHECKPOINT_DIR",
                "/tmp/fabrik-throughput/checkpoints",
            )?,
            checkpoint_interval_seconds: read_u64_with_default(
                "THROUGHPUT_CHECKPOINT_INTERVAL_SECONDS",
                30,
            )?,
            checkpoint_retention: read_usize_with_default("THROUGHPUT_CHECKPOINT_RETENTION", 5)?,
            checkpoint_key_prefix: read_string_with_default(
                "THROUGHPUT_CHECKPOINT_KEY_PREFIX",
                "checkpoints",
            )?,
            terminal_state_retention_seconds: read_u64_with_default(
                "THROUGHPUT_TERMINAL_STATE_RETENTION_SECONDS",
                3600,
            )?,
            terminal_gc_interval_seconds: read_u64_with_default(
                "THROUGHPUT_TERMINAL_GC_INTERVAL_SECONDS",
                60,
            )?,
            restore_idle_timeout_ms: read_u64_with_default(
                "THROUGHPUT_RESTORE_IDLE_TIMEOUT_MS",
                1_000,
            )?,
            payload_store: ThroughputPayloadStoreConfig::from_env()?,
            inline_chunk_input_threshold_bytes: read_usize_with_default(
                "THROUGHPUT_INLINE_CHUNK_INPUT_THRESHOLD_BYTES",
                32 * 1024,
            )?,
            inline_chunk_output_threshold_bytes: read_usize_with_default(
                "THROUGHPUT_INLINE_CHUNK_OUTPUT_THRESHOLD_BYTES",
                32 * 1024,
            )?,
            grouping_chunk_threshold: read_usize_with_default(
                "THROUGHPUT_GROUPING_CHUNK_THRESHOLD",
                128,
            )?,
            target_chunks_per_group: read_usize_with_default(
                "THROUGHPUT_TARGET_CHUNKS_PER_GROUP",
                64,
            )?,
            max_aggregation_groups: read_usize_with_default(
                "THROUGHPUT_MAX_AGGREGATION_GROUPS",
                32,
            )?,
        })
    }
}

impl ThroughputOwnershipConfig {
    pub fn from_env() -> Result<Self, ConfigError> {
        let static_partition_ids = match env::var("THROUGHPUT_OWNERSHIP_PARTITIONS") {
            Ok(raw) => Some(parse_partition_ids(&raw)?),
            Err(env::VarError::NotPresent) => match env::var("THROUGHPUT_OWNERSHIP_PARTITION_ID") {
                Ok(raw) => Some(vec![raw.parse::<i32>().map_err(|_| ConfigError::InvalidI32 {
                    key: "THROUGHPUT_OWNERSHIP_PARTITION_ID".to_owned(),
                    value: raw,
                })?]),
                Err(env::VarError::NotPresent) => None,
                Err(source) => {
                    return Err(ConfigError::UnreadableEnv {
                        key: "THROUGHPUT_OWNERSHIP_PARTITION_ID".to_owned(),
                        source,
                    });
                }
            },
            Err(source) => {
                return Err(ConfigError::UnreadableEnv {
                    key: "THROUGHPUT_OWNERSHIP_PARTITIONS".to_owned(),
                    source,
                });
            }
        };
        Ok(Self {
            static_partition_ids,
            runtime_capacity: read_usize_with_default("THROUGHPUT_RUNTIME_CAPACITY", 4)?,
            member_heartbeat_ttl_seconds: read_u64_with_default(
                "THROUGHPUT_OWNERSHIP_MEMBER_HEARTBEAT_TTL_SECONDS",
                15,
            )?,
            assignment_poll_interval_seconds: read_u64_with_default(
                "THROUGHPUT_OWNERSHIP_ASSIGNMENT_POLL_INTERVAL_SECONDS",
                2,
            )?,
            rebalance_interval_seconds: read_u64_with_default(
                "THROUGHPUT_OWNERSHIP_REBALANCE_INTERVAL_SECONDS",
                5,
            )?,
            lease_ttl_seconds: read_u64_with_default("THROUGHPUT_OWNERSHIP_LEASE_TTL_SECONDS", 15)?,
            renew_interval_seconds: read_u64_with_default(
                "THROUGHPUT_OWNERSHIP_RENEW_INTERVAL_SECONDS",
                5,
            )?,
            partition_id_offset: read_i32_with_default(
                "THROUGHPUT_OWNERSHIP_PARTITION_ID_OFFSET",
                1_000_000,
            )?,
        })
    }
}

impl MatchingRuntimeConfig {
    pub fn from_env() -> Result<Self, ConfigError> {
        Ok(Self {
            lease_ttl_seconds: read_u64_with_default("MATCHING_LEASE_TTL_SECONDS", 30)?,
            heartbeat_publish_interval_ms: read_u64_with_default(
                "MATCHING_HEARTBEAT_PUBLISH_INTERVAL_MS",
                30_000,
            )?,
            sweep_interval_ms: read_u64_with_default("MATCHING_SWEEP_INTERVAL_MS", 500)?,
            max_rebuild_tasks: read_i64_with_default("MATCHING_MAX_REBUILD_TASKS", 10_000)?,
            activity_result_apply_batch_max_items: read_usize_with_default(
                "MATCHING_ACTIVITY_RESULT_BATCH_MAX_ITEMS",
                256,
            )?,
            activity_result_apply_batch_max_bytes: read_usize_with_default(
                "MATCHING_ACTIVITY_RESULT_BATCH_MAX_BYTES",
                1_048_576,
            )?,
            activity_result_apply_flush_interval_ms: read_u64_with_default(
                "MATCHING_ACTIVITY_RESULT_BATCH_FLUSH_INTERVAL_MS",
                5,
            )?,
            result_apply_per_run_coalescing_cap: read_usize_with_default(
                "MATCHING_RESULT_APPLY_PER_RUN_COALESCING_CAP",
                256,
            )?,
        })
    }
}

impl QueryRuntimeConfig {
    pub fn from_env() -> Result<Self, ConfigError> {
        Ok(Self {
            default_page_size: read_usize_with_default("QUERY_DEFAULT_PAGE_SIZE", 100)?,
            max_page_size: read_usize_with_default("QUERY_MAX_PAGE_SIZE", 500)?,
            throughput_payload_store: ThroughputPayloadStoreConfig::from_env()?,
            history_retention_days: read_optional_u64("QUERY_HISTORY_RETENTION_DAYS")?,
            run_retention_days: read_optional_u64("QUERY_RUN_RETENTION_DAYS")?,
            activity_retention_days: read_optional_u64("QUERY_ACTIVITY_RETENTION_DAYS")?,
            signal_retention_days: read_optional_u64("QUERY_SIGNAL_RETENTION_DAYS")?,
            snapshot_retention_days: read_optional_u64("QUERY_SNAPSHOT_RETENTION_DAYS")?,
            retention_sweep_interval_seconds: read_u64_with_default(
                "QUERY_RETENTION_SWEEP_INTERVAL_SECONDS",
                300,
            )?,
        })
    }
}

impl ThroughputPayloadStoreConfig {
    pub fn from_env() -> Result<Self, ConfigError> {
        Ok(Self {
            kind: read_payload_store_kind_with_default(
                "THROUGHPUT_PAYLOAD_STORE",
                ThroughputPayloadStoreKind::LocalFilesystem,
            )?,
            local_dir: read_string_with_default(
                "THROUGHPUT_PAYLOAD_STORE_DIR",
                "/tmp/fabrik-throughput/payloads",
            )?,
            s3_bucket: read_optional_string("THROUGHPUT_PAYLOAD_S3_BUCKET")?,
            s3_region: read_string_with_default("THROUGHPUT_PAYLOAD_S3_REGION", "us-east-1")?,
            s3_endpoint: read_optional_string("THROUGHPUT_PAYLOAD_S3_ENDPOINT")?,
            s3_access_key_id: read_optional_string("THROUGHPUT_PAYLOAD_S3_ACCESS_KEY_ID")?,
            s3_secret_access_key: read_optional_string("THROUGHPUT_PAYLOAD_S3_SECRET_ACCESS_KEY")?,
            s3_force_path_style: read_bool_with_default(
                "THROUGHPUT_PAYLOAD_S3_FORCE_PATH_STYLE",
                false,
            )?,
            s3_key_prefix: read_string_with_default(
                "THROUGHPUT_PAYLOAD_S3_KEY_PREFIX",
                "throughput",
            )?,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecutorRuntimeConfig {
    pub cache_capacity: usize,
    pub snapshot_interval_events: u64,
    pub continue_as_new_event_threshold: Option<u64>,
    pub continue_as_new_activity_attempt_threshold: Option<u64>,
    pub continue_as_new_run_age_seconds: Option<u64>,
    pub max_mailbox_items_per_turn: usize,
    pub max_transitions_per_turn: usize,
    pub throughput_default_backend: String,
    pub throughput_task_queue_backends: BTreeMap<String, String>,
}

impl ExecutorRuntimeConfig {
    pub fn from_env() -> Result<Self, ConfigError> {
        Ok(Self {
            cache_capacity: read_usize_with_default("EXECUTOR_CACHE_CAPACITY", 10_000)?,
            snapshot_interval_events: read_u64_with_default(
                "EXECUTOR_SNAPSHOT_INTERVAL_EVENTS",
                50,
            )?,
            continue_as_new_event_threshold: read_optional_u64(
                "EXECUTOR_CONTINUE_AS_NEW_EVENT_THRESHOLD",
            )?,
            continue_as_new_activity_attempt_threshold: read_optional_u64(
                "EXECUTOR_CONTINUE_AS_NEW_ACTIVITY_ATTEMPT_THRESHOLD",
            )?,
            continue_as_new_run_age_seconds: read_optional_u64(
                "EXECUTOR_CONTINUE_AS_NEW_RUN_AGE_SECONDS",
            )?,
            max_mailbox_items_per_turn: read_usize_with_default(
                "EXECUTOR_MAX_MAILBOX_ITEMS_PER_TURN",
                64,
            )?,
            max_transitions_per_turn: read_usize_with_default(
                "EXECUTOR_MAX_TRANSITIONS_PER_TURN",
                10_000,
            )?,
            throughput_default_backend: read_throughput_backend_with_default(
                "EXECUTOR_THROUGHPUT_DEFAULT_BACKEND",
                "pg-v1",
            )?,
            throughput_task_queue_backends: read_task_queue_backend_overrides(
                "EXECUTOR_THROUGHPUT_TASK_QUEUE_BACKENDS",
            )?,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OwnershipConfig {
    pub static_partition_ids: Option<Vec<i32>>,
    pub executor_capacity: usize,
    pub member_heartbeat_ttl_seconds: u64,
    pub assignment_poll_interval_seconds: u64,
    pub rebalance_interval_seconds: u64,
    pub lease_ttl_seconds: u64,
    pub renew_interval_seconds: u64,
}

impl OwnershipConfig {
    pub fn from_env() -> Result<Self, ConfigError> {
        let static_partition_ids = match env::var("WORKFLOW_PARTITIONS") {
            Ok(raw) => Some(parse_partition_ids(&raw)?),
            Err(env::VarError::NotPresent) => match env::var("WORKFLOW_PARTITION_ID") {
                Ok(raw) => Some(vec![raw.parse::<i32>().map_err(|_| ConfigError::InvalidI32 {
                    key: "WORKFLOW_PARTITION_ID".to_owned(),
                    value: raw,
                })?]),
                Err(env::VarError::NotPresent) => None,
                Err(source) => {
                    return Err(ConfigError::UnreadableEnv {
                        key: "WORKFLOW_PARTITION_ID".to_owned(),
                        source,
                    });
                }
            },
            Err(source) => {
                return Err(ConfigError::UnreadableEnv {
                    key: "WORKFLOW_PARTITIONS".to_owned(),
                    source,
                });
            }
        };
        Ok(Self {
            static_partition_ids,
            executor_capacity: read_usize_with_default("EXECUTOR_CAPACITY", 4)?,
            member_heartbeat_ttl_seconds: read_u64_with_default(
                "OWNERSHIP_MEMBER_HEARTBEAT_TTL_SECONDS",
                15,
            )?,
            assignment_poll_interval_seconds: read_u64_with_default(
                "OWNERSHIP_ASSIGNMENT_POLL_INTERVAL_SECONDS",
                2,
            )?,
            rebalance_interval_seconds: read_u64_with_default(
                "OWNERSHIP_REBALANCE_INTERVAL_SECONDS",
                5,
            )?,
            lease_ttl_seconds: read_u64_with_default("OWNERSHIP_LEASE_TTL_SECONDS", 15)?,
            renew_interval_seconds: read_u64_with_default("OWNERSHIP_RENEW_INTERVAL_SECONDS", 5)?,
        })
    }
}

fn parse_partition_ids(raw: &str) -> Result<Vec<i32>, ConfigError> {
    let mut partitions = Vec::new();
    for token in raw.split(',') {
        let trimmed = token.trim();
        if trimmed.is_empty() {
            continue;
        }
        let partition = trimmed.parse::<i32>().map_err(|_| ConfigError::InvalidI32 {
            key: "WORKFLOW_PARTITIONS".to_owned(),
            value: trimmed.to_owned(),
        })?;
        partitions.push(partition);
    }
    if partitions.is_empty() {
        return Err(ConfigError::InvalidI32 {
            key: "WORKFLOW_PARTITIONS".to_owned(),
            value: raw.to_owned(),
        });
    }
    partitions.sort_unstable();
    partitions.dedup();
    Ok(partitions)
}

fn read_string_with_default(key: &str, default: &str) -> Result<String, ConfigError> {
    match env::var(key) {
        Ok(value) => Ok(value),
        Err(env::VarError::NotPresent) => Ok(default.to_owned()),
        Err(source) => Err(ConfigError::UnreadableEnv { key: key.to_owned(), source }),
    }
}

fn read_usize_with_default(key: &str, default: usize) -> Result<usize, ConfigError> {
    match env::var(key) {
        Ok(raw) => raw
            .parse::<usize>()
            .map_err(|_| ConfigError::InvalidUsize { key: key.to_owned(), value: raw }),
        Err(env::VarError::NotPresent) => Ok(default),
        Err(source) => Err(ConfigError::UnreadableEnv { key: key.to_owned(), source }),
    }
}

fn read_u64_with_default(key: &str, default: u64) -> Result<u64, ConfigError> {
    match env::var(key) {
        Ok(raw) => raw
            .parse::<u64>()
            .map_err(|_| ConfigError::InvalidU64 { key: key.to_owned(), value: raw }),
        Err(env::VarError::NotPresent) => Ok(default),
        Err(source) => Err(ConfigError::UnreadableEnv { key: key.to_owned(), source }),
    }
}

fn read_i32_with_default(key: &str, default: i32) -> Result<i32, ConfigError> {
    match env::var(key) {
        Ok(raw) => raw
            .parse::<i32>()
            .map_err(|_| ConfigError::InvalidI32 { key: key.to_owned(), value: raw }),
        Err(env::VarError::NotPresent) => Ok(default),
        Err(source) => Err(ConfigError::UnreadableEnv { key: key.to_owned(), source }),
    }
}

fn read_i64_with_default(key: &str, default: i64) -> Result<i64, ConfigError> {
    match env::var(key) {
        Ok(raw) => raw
            .parse::<i64>()
            .map_err(|_| ConfigError::InvalidI64 { key: key.to_owned(), value: raw }),
        Err(env::VarError::NotPresent) => Ok(default),
        Err(source) => Err(ConfigError::UnreadableEnv { key: key.to_owned(), source }),
    }
}

fn read_optional_u64(key: &str) -> Result<Option<u64>, ConfigError> {
    match env::var(key) {
        Ok(raw) if raw.trim().is_empty() => Ok(None),
        Ok(raw) => raw
            .parse::<u64>()
            .map(Some)
            .map_err(|_| ConfigError::InvalidU64 { key: key.to_owned(), value: raw }),
        Err(env::VarError::NotPresent) => Ok(None),
        Err(source) => Err(ConfigError::UnreadableEnv { key: key.to_owned(), source }),
    }
}

fn read_optional_string(key: &str) -> Result<Option<String>, ConfigError> {
    match env::var(key) {
        Ok(raw) if raw.trim().is_empty() => Ok(None),
        Ok(raw) => Ok(Some(raw)),
        Err(env::VarError::NotPresent) => Ok(None),
        Err(source) => Err(ConfigError::UnreadableEnv { key: key.to_owned(), source }),
    }
}

fn read_throughput_backend_with_default(key: &str, default: &str) -> Result<String, ConfigError> {
    match env::var(key) {
        Ok(raw) => validate_throughput_backend(key, raw),
        Err(env::VarError::NotPresent) => Ok(default.to_owned()),
        Err(source) => Err(ConfigError::UnreadableEnv { key: key.to_owned(), source }),
    }
}

fn validate_throughput_backend(key: &str, raw: String) -> Result<String, ConfigError> {
    match raw.trim() {
        "pg-v1" | "stream-v2" => Ok(raw.trim().to_owned()),
        _ => Err(ConfigError::InvalidThroughputBackend { key: key.to_owned(), value: raw }),
    }
}

fn read_task_queue_backend_overrides(key: &str) -> Result<BTreeMap<String, String>, ConfigError> {
    let Some(raw) = read_optional_string(key)? else {
        return Ok(BTreeMap::new());
    };

    let mut overrides = BTreeMap::new();
    for entry in raw.split(',') {
        let trimmed = entry.trim();
        if trimmed.is_empty() {
            continue;
        }
        let Some((task_queue, backend)) = trimmed.split_once('=') else {
            return Err(ConfigError::InvalidTaskQueueBackendMapping {
                key: key.to_owned(),
                value: trimmed.to_owned(),
            });
        };
        let task_queue = task_queue.trim();
        if task_queue.is_empty() {
            return Err(ConfigError::InvalidTaskQueueBackendMapping {
                key: key.to_owned(),
                value: trimmed.to_owned(),
            });
        }
        overrides.insert(
            task_queue.to_owned(),
            validate_throughput_backend(key, backend.trim().to_owned())?,
        );
    }
    Ok(overrides)
}

fn read_bool_with_default(key: &str, default: bool) -> Result<bool, ConfigError> {
    match env::var(key) {
        Ok(raw) => match raw.trim().to_ascii_lowercase().as_str() {
            "1" | "true" | "yes" | "on" => Ok(true),
            "0" | "false" | "no" | "off" => Ok(false),
            _ => Err(ConfigError::InvalidBool { key: key.to_owned(), value: raw }),
        },
        Err(env::VarError::NotPresent) => Ok(default),
        Err(source) => Err(ConfigError::UnreadableEnv { key: key.to_owned(), source }),
    }
}

fn read_payload_store_kind_with_default(
    key: &str,
    default: ThroughputPayloadStoreKind,
) -> Result<ThroughputPayloadStoreKind, ConfigError> {
    match env::var(key) {
        Ok(raw) => match raw.trim() {
            "localfs" => Ok(ThroughputPayloadStoreKind::LocalFilesystem),
            "s3" => Ok(ThroughputPayloadStoreKind::S3),
            _ => Err(ConfigError::InvalidPayloadStoreKind { key: key.to_owned(), value: raw }),
        },
        Err(env::VarError::NotPresent) => Ok(default),
        Err(source) => Err(ConfigError::UnreadableEnv { key: key.to_owned(), source }),
    }
}

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("failed to read environment variable {key}: {source}")]
    UnreadableEnv { key: String, source: env::VarError },
    #[error("environment variable {key} must be a valid u16 port, got {value}")]
    InvalidPort { key: String, value: String },
    #[error("environment variable {key} must be a valid usize, got {value}")]
    InvalidUsize { key: String, value: String },
    #[error("environment variable {key} must be a valid u64, got {value}")]
    InvalidU64 { key: String, value: String },
    #[error("environment variable {key} must be a valid i32, got {value}")]
    InvalidI32 { key: String, value: String },
    #[error("environment variable {key} must be a valid i64, got {value}")]
    InvalidI64 { key: String, value: String },
    #[error("environment variable {key} must be a valid bool, got {value}")]
    InvalidBool { key: String, value: String },
    #[error("environment variable {key} must be one of localfs or s3, got {value}")]
    InvalidPayloadStoreKind { key: String, value: String },
    #[error("environment variable {key} must be pg-v1 or stream-v2, got {value}")]
    InvalidThroughputBackend { key: String, value: String },
    #[error(
        "environment variable {key} must be a comma-separated list of task-queue=backend mappings, got {value}"
    )]
    InvalidTaskQueueBackendMapping { key: String, value: String },
}

#[cfg(test)]
mod tests {
    use super::HttpServiceConfig;

    #[test]
    fn falls_back_to_default_port() {
        let config = HttpServiceConfig::from_env("FABRIK_TEST", "test-service", 4100).unwrap();
        assert_eq!(config.port, 4100);
        assert_eq!(config.name, "test-service");
    }
}
