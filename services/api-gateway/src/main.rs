use anyhow::Result;
use fabrik_config::HttpServiceConfig;
use fabrik_service::{ServiceInfo, init_tracing, run_http_service};
use tracing::info;

#[tokio::main]
async fn main() -> Result<()> {
    let config = HttpServiceConfig::from_env("API_GATEWAY", "api-gateway", 3000)?;
    init_tracing(&config.log_filter);
    info!(port = config.port, "starting api gateway");

    run_http_service(
        ServiceInfo::new(config.name, "edge-api", env!("CARGO_PKG_VERSION")),
        config.port,
    )
    .await
}
