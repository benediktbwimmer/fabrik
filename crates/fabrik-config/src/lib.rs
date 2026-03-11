use std::env;

use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HttpServiceConfig {
    pub name: String,
    pub port: u16,
    pub log_filter: String,
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
    pub history_retention_days: Option<u64>,
    pub run_retention_days: Option<u64>,
    pub effect_retention_days: Option<u64>,
    pub signal_retention_days: Option<u64>,
    pub snapshot_retention_days: Option<u64>,
    pub retention_sweep_interval_seconds: u64,
}

impl QueryRuntimeConfig {
    pub fn from_env() -> Result<Self, ConfigError> {
        Ok(Self {
            default_page_size: read_usize_with_default("QUERY_DEFAULT_PAGE_SIZE", 100)?,
            max_page_size: read_usize_with_default("QUERY_MAX_PAGE_SIZE", 500)?,
            history_retention_days: read_optional_u64("QUERY_HISTORY_RETENTION_DAYS")?,
            run_retention_days: read_optional_u64("QUERY_RUN_RETENTION_DAYS")?,
            effect_retention_days: read_optional_u64("QUERY_EFFECT_RETENTION_DAYS")?,
            signal_retention_days: read_optional_u64("QUERY_SIGNAL_RETENTION_DAYS")?,
            snapshot_retention_days: read_optional_u64("QUERY_SNAPSHOT_RETENTION_DAYS")?,
            retention_sweep_interval_seconds: read_u64_with_default(
                "QUERY_RETENTION_SWEEP_INTERVAL_SECONDS",
                300,
            )?,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecutorRuntimeConfig {
    pub cache_capacity: usize,
    pub snapshot_interval_events: u64,
    pub continue_as_new_event_threshold: Option<u64>,
    pub continue_as_new_effect_attempt_threshold: Option<u64>,
    pub continue_as_new_run_age_seconds: Option<u64>,
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
            continue_as_new_effect_attempt_threshold: read_optional_u64(
                "EXECUTOR_CONTINUE_AS_NEW_EFFECT_ATTEMPT_THRESHOLD",
            )?,
            continue_as_new_run_age_seconds: read_optional_u64(
                "EXECUTOR_CONTINUE_AS_NEW_RUN_AGE_SECONDS",
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
