use std::net::{Ipv4Addr, SocketAddr};

use anyhow::Result;
use axum::{Json, Router, routing::get};
use serde::Serialize;
use tokio::signal;
use tracing::info;
use tracing_subscriber::EnvFilter;

#[derive(Debug, Clone)]
pub struct ServiceInfo {
    pub name: String,
    pub role: String,
    pub version: String,
}

impl ServiceInfo {
    pub fn new(
        name: impl Into<String>,
        role: impl Into<String>,
        version: impl Into<String>,
    ) -> Self {
        Self { name: name.into(), role: role.into(), version: version.into() }
    }
}

#[derive(Debug, Serialize)]
struct HealthResponse {
    status: &'static str,
}

#[derive(Debug, Serialize)]
struct MetadataResponse {
    name: String,
    role: String,
    version: String,
}

pub fn init_tracing(default_filter: &str) {
    let filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(default_filter));

    tracing_subscriber::fmt().with_env_filter(filter).with_target(false).compact().init();
}

pub fn default_router<S>(info: ServiceInfo) -> Router<S>
where
    S: Clone + Send + Sync + 'static,
{
    let metadata = info.clone();
    Router::new().route("/healthz", get(|| async { Json(HealthResponse { status: "ok" }) })).route(
        "/metadata",
        get(move || {
            let metadata = metadata.clone();
            async move {
                Json(MetadataResponse {
                    name: metadata.name,
                    role: metadata.role,
                    version: metadata.version,
                })
            }
        }),
    )
}

pub async fn run_http_service(info: ServiceInfo, port: u16) -> Result<()> {
    serve(default_router(info), port).await
}

pub async fn serve(app: Router, port: u16) -> Result<()> {
    let addr = SocketAddr::from((Ipv4Addr::UNSPECIFIED, port));
    info!(%addr, "service listening");
    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).with_graceful_shutdown(shutdown_signal()).await?;
    Ok(())
}

pub async fn shutdown_signal() {
    let ctrl_c = async {
        signal::ctrl_c().await.expect("failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("failed to install SIGTERM handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }
}
