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
use fabrik_throughput::{
    PayloadHandle, PayloadStore, PayloadStoreConfig, PayloadStoreKind, StreamsProjectionEvent,
};
use futures_util::StreamExt;
use serde::Serialize;
use tracing::error;

const PROJECTION_APPLY_BATCH_SIZE: usize = 128;
const PROJECTION_APPLY_BATCH_WAIT_MS: u64 = 2;
const STREAMS_PROJECTOR_SERVICE_NAME: &str = "streams-projector";
const STREAMS_PROJECTOR_DEBUG_ROUTE: &str = "/debug/streams-projector";
const LEGACY_THROUGHPUT_PROJECTOR_DEBUG_ROUTE: &str = "/debug/throughput-projector";
const LEGACY_THROUGHPUT_PROJECTOR_ID: &str = "throughput-projector";
const STREAMS_PROJECTOR_ID: &str = "streams-projector";

#[derive(Clone)]
struct AppState {
    store: WorkflowStore,
    payload_store: PayloadStore,
    throughput_projector_id: String,
    streams_projector_id: String,
    debug: Arc<Mutex<ProjectorDebugState>>,
}

#[derive(Debug, Clone, Serialize, Default)]
struct ProjectorDebugState {
    applied_events: u64,
    throughput_applied_events: u64,
    streams_applied_events: u64,
    apply_failures: u64,
    manifest_writes: u64,
    last_applied_at: Option<chrono::DateTime<chrono::Utc>>,
    last_throughput_applied_at: Option<chrono::DateTime<chrono::Utc>>,
    last_streams_applied_at: Option<chrono::DateTime<chrono::Utc>>,
    last_failure: Option<String>,
}

#[derive(Debug)]
struct BufferedProjectionRecord {
    partition_id: i32,
    offset: i64,
    event: BufferedProjectionEvent,
}

#[derive(Debug)]
enum BufferedProjectionEvent {
    Throughput(ThroughputProjectionEvent),
    Streams(StreamsProjectionEvent),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ProjectionPlane {
    Throughput,
    Streams,
}

#[tokio::main]
async fn main() -> Result<()> {
    let config = HttpServiceConfig::from_env_aliases(
        &["STREAMS_PROJECTOR", "THROUGHPUT_PROJECTOR"],
        STREAMS_PROJECTOR_SERVICE_NAME,
        3007,
    )?;
    let postgres = PostgresConfig::from_env()?;
    let redpanda = RedpandaConfig::from_env()?;
    let query = QueryRuntimeConfig::from_env()?;
    init_tracing(&config.log_filter);

    let store = WorkflowStore::connect(&postgres.url).await?;
    store.init().await?;
    let payload_store = PayloadStore::from_config(build_payload_store_config(&query)).await?;
    let debug = Arc::new(Mutex::new(ProjectorDebugState::default()));
    let state = AppState {
        store: store.clone(),
        payload_store,
        throughput_projector_id: LEGACY_THROUGHPUT_PROJECTOR_ID.to_owned(),
        streams_projector_id: STREAMS_PROJECTOR_ID.to_owned(),
        debug: debug.clone(),
    };

    let throughput_config = JsonTopicConfig::new(
        redpanda.brokers.clone(),
        redpanda.throughput_projections_topic,
        redpanda.throughput_partitions,
    );
    let streams_config = JsonTopicConfig::new(
        redpanda.brokers,
        redpanda.streams_projections_topic,
        redpanda.throughput_partitions,
    );

    tokio::spawn(run_projection_consumer(
        state.clone(),
        throughput_config.clone(),
        ProjectionPlane::Throughput,
    ));
    if streams_config.topic_name != throughput_config.topic_name {
        tokio::spawn(run_projection_consumer(
            state.clone(),
            streams_config,
            ProjectionPlane::Streams,
        ));
    }

    let app = default_router::<AppState>(ServiceInfo::new(
        config.name,
        STREAMS_PROJECTOR_SERVICE_NAME,
        env!("CARGO_PKG_VERSION"),
    ))
    .route(STREAMS_PROJECTOR_DEBUG_ROUTE, get(get_debug))
    .route(LEGACY_THROUGHPUT_PROJECTOR_DEBUG_ROUTE, get(get_debug))
    .with_state(state);
    serve(app, config.port).await
}

async fn get_debug(
    axum::extract::State(state): axum::extract::State<AppState>,
) -> Json<ProjectorDebugState> {
    Json(state.debug.lock().expect("streams projector debug lock poisoned").clone())
}

fn projector_id_for_plane<'a>(state: &'a AppState, plane: ProjectionPlane) -> &'a str {
    match plane {
        ProjectionPlane::Throughput => &state.throughput_projector_id,
        ProjectionPlane::Streams => &state.streams_projector_id,
    }
}

async fn run_projection_consumer(state: AppState, config: JsonTopicConfig, plane: ProjectionPlane) {
    loop {
        let projector_id = projector_id_for_plane(&state, plane);
        let offsets_result = match plane {
            ProjectionPlane::Throughput => {
                state.store.load_throughput_projection_offsets(projector_id).await
            }
            ProjectionPlane::Streams => {
                state.store.load_streams_projection_offsets(projector_id).await
            }
        };
        let offsets = match offsets_result {
            Ok(offsets) => offsets,
            Err(error) => {
                error!(error = %error, "failed to load streams projection offsets");
                tokio::time::sleep(Duration::from_millis(500)).await;
                continue;
            }
        };
        let start_offsets = next_unread_offsets(&offsets);
        let mut consumer = match build_json_consumer_from_offsets(
            &config,
            projector_id,
            &start_offsets,
            &config.all_partition_ids(),
        )
        .await
        {
            Ok(consumer) => consumer,
            Err(error) => {
                error!(error = %error, "failed to build streams projection consumer");
                tokio::time::sleep(Duration::from_millis(500)).await;
                continue;
            }
        };
        while let Some(message) = consumer.next().await {
            let Some(first_record) = decode_projection_record(message, plane) else {
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
                let Some(record) = decode_projection_record(message, plane) else {
                    break;
                };
                batch.push(record);
            }
            if let Err(error) = apply_projection_batch(&state, &batch).await {
                {
                    let mut debug =
                        state.debug.lock().expect("streams projector debug lock poisoned");
                    debug.apply_failures = debug.apply_failures.saturating_add(1);
                    debug.last_failure = Some(error.to_string());
                }
                error!(error = %error, "failed to apply streams projection event");
                tokio::time::sleep(Duration::from_millis(250)).await;
                break;
            }
            if let Err(error) = commit_projection_batch_offsets(&state, &batch, plane).await {
                error!(error = %error, "failed to commit streams projection offset");
                break;
            }
            let mut debug = state.debug.lock().expect("streams projector debug lock poisoned");
            debug.applied_events = debug.applied_events.saturating_add(batch.len() as u64);
            debug.last_applied_at = Some(Utc::now());
            match plane {
                ProjectionPlane::Throughput => {
                    debug.throughput_applied_events =
                        debug.throughput_applied_events.saturating_add(batch.len() as u64);
                    debug.last_throughput_applied_at = Some(Utc::now());
                }
                ProjectionPlane::Streams => {
                    debug.streams_applied_events =
                        debug.streams_applied_events.saturating_add(batch.len() as u64);
                    debug.last_streams_applied_at = Some(Utc::now());
                }
            }
        }
    }
}

fn decode_projection_record(
    message: Result<fabrik_broker::ConsumedJsonRecord>,
    plane: ProjectionPlane,
) -> Option<BufferedProjectionRecord> {
    let record = match message {
        Ok(record) => record,
        Err(error) => {
            error!(error = %error, "failed to read streams projection message");
            return None;
        }
    };
    let event = match plane {
        ProjectionPlane::Throughput => {
            match decode_json_record::<ThroughputProjectionEvent>(&record.record) {
                Ok(event) => BufferedProjectionEvent::Throughput(event),
                Err(error) => {
                    error!(error = %error, "failed to decode throughput projection event");
                    return None;
                }
            }
        }
        ProjectionPlane::Streams => {
            match decode_json_record::<StreamsProjectionEvent>(&record.record) {
                Ok(event) => BufferedProjectionEvent::Streams(event),
                Err(error) => {
                    error!(error = %error, "failed to decode streams projection event");
                    return None;
                }
            }
        }
    };
    Some(BufferedProjectionRecord {
        partition_id: record.partition_id,
        offset: record.record.offset,
        event,
    })
}

fn next_unread_offsets(offsets: &HashMap<i32, i64>) -> HashMap<i32, i64> {
    offsets.iter().map(|(partition_id, offset)| (*partition_id, offset.saturating_add(1))).collect()
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
    plane: ProjectionPlane,
) -> Result<()> {
    let mut offsets: HashMap<i32, i64> = HashMap::new();
    for record in batch {
        offsets
            .entry(record.partition_id)
            .and_modify(|offset| *offset = (*offset).max(record.offset))
            .or_insert(record.offset);
    }
    let updated_at = Utc::now();
    let projector_id = projector_id_for_plane(state, plane);
    for (partition_id, offset) in offsets {
        match plane {
            ProjectionPlane::Throughput => {
                state
                    .store
                    .commit_throughput_projection_offset(
                        projector_id,
                        partition_id,
                        offset,
                        updated_at,
                    )
                    .await?;
            }
            ProjectionPlane::Streams => {
                state
                    .store
                    .commit_streams_projection_offset(
                        projector_id,
                        partition_id,
                        offset,
                        updated_at,
                    )
                    .await?;
            }
        }
    }
    Ok(())
}

async fn apply_projection_event(state: &AppState, event: &BufferedProjectionEvent) -> Result<()> {
    match event {
        BufferedProjectionEvent::Throughput(event) => match event {
            ThroughputProjectionEvent::UpsertBatch { batch } => {
                state.store.upsert_throughput_projection_batch(batch).await?
            }
            ThroughputProjectionEvent::UpsertChunk { chunk } => {
                state.store.upsert_throughput_projection_chunk(chunk).await?
            }
            ThroughputProjectionEvent::UpsertStreamJobView { view } => {
                state.store.upsert_stream_job_view_query(view).await?
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
        },
        BufferedProjectionEvent::Streams(event) => match event {
            StreamsProjectionEvent::UpsertStreamJobView { view } => {
                state
                    .store
                    .upsert_stream_job_view_query(&fabrik_store::StreamJobViewRecord {
                        tenant_id: view.tenant_id.clone(),
                        instance_id: view.instance_id.clone(),
                        run_id: view.run_id.clone(),
                        job_id: view.job_id.clone(),
                        handle_id: view.handle_id.clone(),
                        view_name: view.view_name.clone(),
                        logical_key: view.logical_key.clone(),
                        output: view.output.clone(),
                        checkpoint_sequence: view.checkpoint_sequence,
                        updated_at: view.updated_at,
                    })
                    .await?;
            }
        },
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
        let mut debug = state.debug.lock().expect("streams projector debug lock poisoned");
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

#[cfg(test)]
mod tests {
    use super::next_unread_offsets;
    use std::collections::HashMap;

    #[test]
    fn next_unread_offsets_advance_past_last_applied_record() {
        let offsets = HashMap::from([(0, 23), (1, 8), (2, i64::MAX)]);

        let next_offsets = next_unread_offsets(&offsets);

        assert_eq!(next_offsets.get(&0), Some(&24));
        assert_eq!(next_offsets.get(&1), Some(&9));
        assert_eq!(next_offsets.get(&2), Some(&i64::MAX));
    }
}
