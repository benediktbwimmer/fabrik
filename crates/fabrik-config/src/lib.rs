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
}

impl RedpandaConfig {
    pub fn from_env() -> Result<Self, ConfigError> {
        Ok(Self {
            brokers: read_string_with_default("REDPANDA_BROKERS", "localhost:29092")?,
            workflow_events_topic: read_string_with_default(
                "WORKFLOW_EVENTS_TOPIC",
                "workflow-events",
            )?,
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

fn read_string_with_default(key: &str, default: &str) -> Result<String, ConfigError> {
    match env::var(key) {
        Ok(value) => Ok(value),
        Err(env::VarError::NotPresent) => Ok(default.to_owned()),
        Err(source) => Err(ConfigError::UnreadableEnv { key: key.to_owned(), source }),
    }
}

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("failed to read environment variable {key}: {source}")]
    UnreadableEnv { key: String, source: env::VarError },
    #[error("environment variable {key} must be a valid u16 port, got {value}")]
    InvalidPort { key: String, value: String },
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
