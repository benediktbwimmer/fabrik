use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::Duration,
};

use anyhow::Result;
use axum::{Json, routing::get};
use chrono::Utc;
use fabrik_broker::{JsonTopicConfig, build_json_consumer_from_offsets, decode_json_record};
use fabrik_config::{HttpServiceConfig, PostgresConfig, QueryRuntimeConfig, RedpandaConfig};
use fabrik_service::{ServiceInfo, default_router, init_tracing, serve};
use fabrik_store::{ThroughputProjectionEvent, WorkflowStore};
use fabrik_throughput::{PayloadHandle, PayloadStore, PayloadStoreConfig, PayloadStoreKind};
use futures_util::StreamExt;
use serde::Serialize;
use tracing::error;

const PROJECTION_APPLY_BATCH_SIZE: usize = 128;
const PROJECTION_APPLY_BATCH_WAIT_MS: u64 = 2;

#[derive(Clone)]
struct AppState {
    store: WorkflowStore,
    payload_store: PayloadStore,
    projector_id: String,
    debug: Arc<Mutex<ProjectorDebugState>>,
}

#[derive(Debug, Clone, Serialize, Default)]
struct ProjectorDebugState {
    applied_events: u64,
    apply_failures: u64,
    manifest_writes: u64,
    last_applied_at: Option<chrono::DateTime<chrono::Utc>>,
    last_failure: Option<String>,
}

#[derive(Debug)]
struct BufferedProjectionRecord {
    partition_id: i32,
    offset: i64,
    event: ThroughputProjectionEvent,
}

#[tokio::main]
async fn main() -> Result<()> {
    let config = HttpServiceConfig::from_env("THROUGHPUT_PROJECTOR", "throughput-projector", 3007)?;
    let postgres = PostgresConfig::from_env()?;
    let redpanda = RedpandaConfig::from_env()?;
    let query = QueryRuntimeConfig::from_env()?;
    init_tracing(&config.log_filter);

    let store = WorkflowStore::connect(&postgres.url).await?;
    store.init().await?;
    let payload_store = PayloadStore::from_config(build_payload_store_config(&query)).await?;
    let projector_id = "throughput-projector".to_owned();
    let debug = Arc::new(Mutex::new(ProjectorDebugState::default()));
    let state = AppState {
        store: store.clone(),
        payload_store,
        projector_id: projector_id.clone(),
        debug: debug.clone(),
    };

    tokio::spawn(run_projection_consumer(
        state.clone(),
        JsonTopicConfig::new(
            redpanda.brokers,
            redpanda.throughput_projections_topic,
            redpanda.throughput_partitions,
        ),
    ));

    let app = default_router::<AppState>(ServiceInfo::new(
        config.name,
        "throughput-projector",
        env!("CARGO_PKG_VERSION"),
    ))
    .route("/debug/throughput-projector", get(get_debug))
    .with_state(state);
    serve(app, config.port).await
}

async fn get_debug(
    axum::extract::State(state): axum::extract::State<AppState>,
) -> Json<ProjectorDebugState> {
    Json(state.debug.lock().expect("throughput projector debug lock poisoned").clone())
}

async fn run_projection_consumer(state: AppState, config: JsonTopicConfig) {
    loop {
        let offsets =
            match state.store.load_throughput_projection_offsets(&state.projector_id).await {
                Ok(offsets) => offsets,
                Err(error) => {
                    error!(error = %error, "failed to load throughput projection offsets");
                    tokio::time::sleep(Duration::from_millis(500)).await;
                    continue;
                }
            };
        let mut consumer = match build_json_consumer_from_offsets(
            &config,
            &state.projector_id,
            &offsets,
            &config.all_partition_ids(),
        )
        .await
        {
            Ok(consumer) => consumer,
            Err(error) => {
                error!(error = %error, "failed to build throughput projection consumer");
                tokio::time::sleep(Duration::from_millis(500)).await;
                continue;
            }
        };
        while let Some(message) = consumer.next().await {
            let Some(first_record) = decode_projection_record(message) else {
                break;
            };
            let mut batch = vec![first_record];
            loop {
                if batch.len() >= PROJECTION_APPLY_BATCH_SIZE {
                    break;
                }
                let Ok(Some(message)) = tokio::time::timeout(
                    Duration::from_millis(PROJECTION_APPLY_BATCH_WAIT_MS),
                    consumer.next(),
                )
                .await
                else {
                    break;
                };
                let Some(record) = decode_projection_record(message) else {
                    break;
                };
                batch.push(record);
            }
            if let Err(error) = apply_projection_batch(&state, &batch).await {
                {
                    let mut debug =
                        state.debug.lock().expect("throughput projector debug lock poisoned");
                    debug.apply_failures = debug.apply_failures.saturating_add(1);
                    debug.last_failure = Some(error.to_string());
                }
                error!(error = %error, "failed to apply throughput projection event");
                tokio::time::sleep(Duration::from_millis(250)).await;
                break;
            }
            if let Err(error) = commit_projection_batch_offsets(&state, &batch).await {
                error!(error = %error, "failed to commit throughput projection offset");
                break;
            }
            let mut debug = state.debug.lock().expect("throughput projector debug lock poisoned");
            debug.applied_events = debug.applied_events.saturating_add(batch.len() as u64);
            debug.last_applied_at = Some(Utc::now());
        }
    }
}

fn decode_projection_record(
    message: Result<fabrik_broker::ConsumedJsonRecord>,
) -> Option<BufferedProjectionRecord> {
    let record = match message {
        Ok(record) => record,
        Err(error) => {
            error!(error = %error, "failed to read throughput projection message");
            return None;
        }
    };
    let event: ThroughputProjectionEvent = match decode_json_record(&record.record) {
        Ok(event) => event,
        Err(error) => {
            error!(error = %error, "failed to decode throughput projection event");
            return None;
        }
    };
    Some(BufferedProjectionRecord {
        partition_id: record.partition_id,
        offset: record.record.offset,
        event,
    })
}

async fn apply_projection_batch(
    state: &AppState,
    batch: &[BufferedProjectionRecord],
) -> Result<()> {
    for record in batch {
        apply_projection_event(state, &record.event).await?;
    }
    Ok(())
}

async fn commit_projection_batch_offsets(
    state: &AppState,
    batch: &[BufferedProjectionRecord],
) -> Result<()> {
    let mut offsets: HashMap<i32, i64> = HashMap::new();
    for record in batch {
        offsets
            .entry(record.partition_id)
            .and_modify(|offset| *offset = (*offset).max(record.offset))
            .or_insert(record.offset);
    }
    let updated_at = Utc::now();
    for (partition_id, offset) in offsets {
        state
            .store
            .commit_throughput_projection_offset(
                &state.projector_id,
                partition_id,
                offset,
                updated_at,
            )
            .await?;
    }
    Ok(())
}

async fn apply_projection_event(state: &AppState, event: &ThroughputProjectionEvent) -> Result<()> {
    match event {
        ThroughputProjectionEvent::UpsertBatch { batch } => {
            state.store.upsert_throughput_projection_batch(batch).await?
        }
        ThroughputProjectionEvent::UpsertChunk { chunk } => {
            state.store.upsert_throughput_projection_chunk(chunk).await?
        }
        ThroughputProjectionEvent::UpdateBatchState { update } => {
            state.store.update_throughput_projection_batch_state(update).await?;
            if update.terminal_at.is_some() {
                sync_batch_result_manifest(
                    state,
                    &update.tenant_id,
                    &update.instance_id,
                    &update.run_id,
                    &update.batch_id,
                )
                .await?;
            }
        }
        ThroughputProjectionEvent::UpdateChunkState { update } => {
            state.store.update_throughput_projection_chunk_state(update).await?;
        }
    }
    Ok(())
}

async fn sync_batch_result_manifest(
    state: &AppState,
    tenant_id: &str,
    instance_id: &str,
    run_id: &str,
    batch_id: &str,
) -> Result<()> {
    let batch = match state
        .store
        .get_bulk_batch_query_view(tenant_id, instance_id, run_id, batch_id)
        .await?
    {
        Some(batch) => batch,
        None => return Ok(()),
    };
    let chunks = state
        .store
        .list_bulk_chunks_for_batch_page_query_view(
            tenant_id,
            instance_id,
            run_id,
            batch_id,
            i64::MAX,
            0,
        )
        .await?;
    let manifest = serde_json::json!({
        "kind": "chunk-result-manifest",
        "batchId": batch_id,
        "chunks": chunks
            .into_iter()
            .filter(|chunk| chunk.output.is_some() || !chunk.result_handle.is_null())
            .map(|chunk| serde_json::json!({
                "chunkId": chunk.chunk_id,
                "chunkIndex": chunk.chunk_index,
                "resultHandle": chunk.result_handle,
            }))
            .collect::<Vec<_>>(),
    });
    if let Ok(handle) = serde_json::from_value::<PayloadHandle>(batch.result_handle.clone()) {
        let key = match handle {
            PayloadHandle::Manifest { key, .. } | PayloadHandle::ManifestSlice { key, .. } => key,
            PayloadHandle::Inline { .. } => return Ok(()),
        };
        state.payload_store.write_value(&key, &manifest).await?;
        let mut debug = state.debug.lock().expect("throughput projector debug lock poisoned");
        debug.manifest_writes = debug.manifest_writes.saturating_add(1);
    }
    Ok(())
}

fn build_payload_store_config(query: &QueryRuntimeConfig) -> PayloadStoreConfig {
    PayloadStoreConfig {
        default_store: match query.throughput_payload_store.kind {
            fabrik_config::ThroughputPayloadStoreKind::LocalFilesystem => {
                PayloadStoreKind::LocalFilesystem
            }
            fabrik_config::ThroughputPayloadStoreKind::S3 => PayloadStoreKind::S3,
        },
        local_dir: query.throughput_payload_store.local_dir.clone(),
        s3_bucket: query.throughput_payload_store.s3_bucket.clone(),
        s3_region: query.throughput_payload_store.s3_region.clone(),
        s3_endpoint: query.throughput_payload_store.s3_endpoint.clone(),
        s3_access_key_id: query.throughput_payload_store.s3_access_key_id.clone(),
        s3_secret_access_key: query.throughput_payload_store.s3_secret_access_key.clone(),
        s3_force_path_style: query.throughput_payload_store.s3_force_path_style,
        s3_key_prefix: query.throughput_payload_store.s3_key_prefix.clone(),
    }
}
