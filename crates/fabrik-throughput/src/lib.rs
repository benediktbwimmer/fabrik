use std::{
    collections::BTreeMap,
    fs,
    path::{Path, PathBuf},
};

mod bridge;

use anyhow::{Context, Result};
use aws_config::BehaviorVersion;
use aws_sdk_s3::{
    Client as S3Client,
    config::{Builder as S3ConfigBuilder, Credentials, Region},
    primitives::ByteStream,
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::Value;
use sha2::{Digest, Sha256};
use uuid::Uuid;

pub use bridge::*;

pub const STREAM_V2_BACKEND: &str = "stream-v2";
pub const CORE_ECHO_ACTIVITY: &str = "core.echo";
pub const CORE_NOOP_ACTIVITY: &str = "core.noop";
pub const CORE_ACCEPT_ACTIVITY: &str = "core.accept";
pub const BENCHMARK_ECHO_ACTIVITY: &str = "benchmark.echo";
pub const BENCHMARK_FAST_COUNT_ACTIVITY: &str = "benchmark.fast_count";
pub const BENCHMARK_COMPACT_INPUT_KIND: &str = "benchmark_compact";
pub const BENCHMARK_COMPACT_INPUT_HANDLE_PREFIX: &str = "benchmark-compact";
pub const STREAM_V2_BACKEND_VERSION: &str = "2.0.0";
pub const ADMISSION_POLICY_VERSION: &str = "2026-03-13.1";
pub const BULK_REDUCER_ALL_SUCCEEDED: &str = "all_succeeded";
pub const BULK_REDUCER_ALL_SETTLED: &str = "all_settled";
pub const BULK_REDUCER_COUNT: &str = "count";
pub const BULK_REDUCER_SUM: &str = "sum";
pub const BULK_REDUCER_MIN: &str = "min";
pub const BULK_REDUCER_MAX: &str = "max";
pub const BULK_REDUCER_AVG: &str = "avg";
pub const BULK_REDUCER_HISTOGRAM: &str = "histogram";
pub const BULK_REDUCER_SAMPLE_ERRORS: &str = "sample_errors";
pub const BULK_REDUCER_COLLECT_RESULTS: &str = "collect_results";
pub const BULK_REDUCER_ERROR_SAMPLE_LIMIT: usize = 8;
pub const DEFAULT_AGGREGATION_GROUP_COUNT: u32 = 1;
pub const DEFAULT_GROUP_ID: u32 = 0;
pub const INITIAL_OWNER_EPOCH: u64 = 1;
pub const TINY_WORKFLOW_MAX_ITEMS: usize = 32;
pub const STREAMS_KERNEL_V1_CONTRACT: &str = "streams_kernel_v1";
pub const STREAMS_KERNEL_V2_CONTRACT: &str = "streams_kernel_v2";
pub const STREAM_RUNTIME_KEYED_ROLLUP: &str = "keyed_rollup";
pub const STREAM_RUNTIME_AGGREGATE_V2: &str = "aggregate_v2";
pub const STREAM_JOB_KEYED_ROLLUP: &str = "keyed-rollup";
pub const STREAM_SOURCE_BOUNDED_INPUT: &str = "bounded_input";
pub const STREAM_SOURCE_TOPIC: &str = "topic";
pub const STREAM_OPERATOR_MAP: &str = "map";
pub const STREAM_OPERATOR_FILTER: &str = "filter";
pub const STREAM_OPERATOR_ROUTE: &str = "route";
pub const STREAM_OPERATOR_KEY_BY: &str = "key_by";
pub const STREAM_OPERATOR_WINDOW: &str = "window";
pub const STREAM_OPERATOR_REDUCE: &str = "reduce";
pub const STREAM_OPERATOR_AGGREGATE: &str = "aggregate";
pub const STREAM_OPERATOR_DEDUPE: &str = "dedupe";
pub const STREAM_OPERATOR_MATERIALIZE: &str = "materialize";
pub const STREAM_OPERATOR_EMIT_CHECKPOINT: &str = "emit_checkpoint";
pub const STREAM_OPERATOR_SIGNAL_WORKFLOW: &str = "signal_workflow";
pub const STREAM_OPERATOR_SINK: &str = "sink";
pub const STREAM_REDUCER_COUNT: &str = "count";
pub const STREAM_REDUCER_SUM: &str = "sum";
pub const STREAM_REDUCER_MIN: &str = "min";
pub const STREAM_REDUCER_MAX: &str = "max";
pub const STREAM_REDUCER_AVG: &str = "avg";
pub const STREAM_REDUCER_HISTOGRAM: &str = "histogram";
pub const STREAM_REDUCER_THRESHOLD: &str = "threshold";
pub const STREAM_CHECKPOINT_POLICY_NAMED: &str = "named_checkpoints";
pub const STREAM_CHECKPOINT_DELIVERY_WORKFLOW_AWAITABLE: &str = "workflow_awaitable";
pub const STREAM_QUERY_MODE_BY_KEY: &str = "by_key";
pub const STREAM_QUERY_MODE_PREFIX_SCAN: &str = "prefix_scan";
pub const STREAM_CONSISTENCY_STRONG: &str = "strong";
pub const STREAM_CONSISTENCY_EVENTUAL: &str = "eventual";
pub const STREAM_STATE_KIND_KEYED: &str = "keyed";
pub const STREAM_STATE_KIND_WINDOW: &str = "window";
pub const STREAM_STATE_KIND_DEDUPE: &str = "dedupe";
const REDUCTION_GROUP_LEVEL_SHIFT: u32 = 24;
const REDUCTION_GROUP_SLOT_MASK: u32 = (1 << REDUCTION_GROUP_LEVEL_SHIFT) - 1;

fn vec_is_empty<T>(values: &[T]) -> bool {
    values.is_empty()
}

fn btree_map_is_empty<K, V>(values: &BTreeMap<K, V>) -> bool {
    values.is_empty()
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StreamArtifactEntrypoint {
    pub module: String,
    pub export: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StreamArtifactSourceLocation {
    pub file: String,
    pub line: u32,
    pub column: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub struct CompiledStreamSource {
    pub kind: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub binding: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub config: Option<Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub struct CompiledStreamOperator {
    pub kind: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub operator_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(default, skip_serializing_if = "vec_is_empty")]
    pub inputs: Vec<String>,
    #[serde(default, skip_serializing_if = "vec_is_empty")]
    pub outputs: Vec<String>,
    #[serde(default, skip_serializing_if = "vec_is_empty")]
    pub state_ids: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub config: Option<Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub struct CompiledStreamState {
    pub id: String,
    pub kind: String,
    #[serde(default, skip_serializing_if = "vec_is_empty")]
    pub key_fields: Vec<String>,
    #[serde(default, skip_serializing_if = "vec_is_empty")]
    pub value_fields: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retention_seconds: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub config: Option<Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub struct CompiledStreamView {
    pub name: String,
    pub consistency: String,
    pub query_mode: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub view_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key_field: Option<String>,
    #[serde(default)]
    pub value_fields: Vec<String>,
    #[serde(default, skip_serializing_if = "vec_is_empty")]
    pub supported_consistencies: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retention_seconds: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub struct CompiledStreamQuery {
    pub name: String,
    pub view_name: String,
    pub consistency: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub query_id: Option<String>,
    #[serde(default, skip_serializing_if = "vec_is_empty")]
    pub arg_fields: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub struct CompiledStreamJob {
    pub name: String,
    pub runtime: String,
    pub source: CompiledStreamSource,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key_by: Option<String>,
    #[serde(default, skip_serializing_if = "vec_is_empty")]
    pub states: Vec<CompiledStreamState>,
    #[serde(default)]
    pub operators: Vec<CompiledStreamOperator>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub checkpoint_policy: Option<Value>,
    #[serde(default)]
    pub views: Vec<CompiledStreamView>,
    #[serde(default)]
    pub queries: Vec<CompiledStreamQuery>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub classification: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata: Option<Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub struct CompiledKeyedRollupKernel {
    pub key_field: String,
    pub value_field: String,
    pub output_field: String,
    pub view_name: String,
    pub query_name: String,
    pub checkpoint_name: String,
    pub checkpoint_sequence: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub struct CompiledStreamJobArtifact {
    pub definition_id: String,
    pub definition_version: u32,
    pub compiler_version: String,
    #[serde(default = "default_streams_runtime_contract")]
    pub runtime_contract: String,
    pub source_language: String,
    pub entrypoint: StreamArtifactEntrypoint,
    #[serde(default, skip_serializing_if = "vec_is_empty")]
    pub source_files: Vec<String>,
    #[serde(default, skip_serializing_if = "btree_map_is_empty")]
    pub source_map: BTreeMap<String, StreamArtifactSourceLocation>,
    pub job: CompiledStreamJob,
    pub artifact_hash: String,
}

fn default_streams_runtime_contract() -> String {
    STREAMS_KERNEL_V1_CONTRACT.to_owned()
}

fn json_object<'a>(value: &'a Value, subject: &str) -> Result<&'a serde_json::Map<String, Value>> {
    value.as_object().with_context(|| format!("stream {subject} must be an object"))
}

fn json_string_field<'a>(
    object: &'a serde_json::Map<String, Value>,
    field: &str,
    subject: &str,
) -> Result<&'a str> {
    object
        .get(field)
        .and_then(Value::as_str)
        .with_context(|| format!("stream {subject}.{field} must be a string"))
}

fn json_i64_field(
    object: &serde_json::Map<String, Value>,
    field: &str,
    subject: &str,
) -> Result<i64> {
    object
        .get(field)
        .and_then(Value::as_i64)
        .with_context(|| format!("stream {subject}.{field} must be an integer"))
}

impl CompiledStreamJob {
    pub fn validate_well_formed(&self) -> Result<()> {
        if self.name.trim().is_empty() {
            anyhow::bail!("compiled stream job.name cannot be empty");
        }
        if self.runtime.trim().is_empty() {
            anyhow::bail!("compiled stream job.runtime cannot be empty");
        }
        if self.source.kind.trim().is_empty() {
            anyhow::bail!("compiled stream job.source.kind cannot be empty");
        }
        if self.views.iter().any(|view| view.name.trim().is_empty()) {
            anyhow::bail!("compiled stream job views must have non-empty names");
        }
        if self.queries.iter().any(|query| query.name.trim().is_empty()) {
            anyhow::bail!("compiled stream job queries must have non-empty names");
        }
        if self.states.iter().any(|state| state.id.trim().is_empty()) {
            anyhow::bail!("compiled stream job states must have non-empty ids");
        }
        if self.states.iter().any(|state| state.kind.trim().is_empty()) {
            anyhow::bail!("compiled stream job states must have non-empty kinds");
        }
        if self
            .operators
            .iter()
            .filter_map(|operator| operator.operator_id.as_deref())
            .any(|id| id.trim().is_empty())
        {
            anyhow::bail!("compiled stream job operator ids must be non-empty when present");
        }
        Ok(())
    }

    pub fn validate_supported_contract(&self) -> Result<()> {
        self.validate_well_formed()?;
        if self.runtime == STREAM_RUNTIME_KEYED_ROLLUP {
            self.keyed_rollup_kernel()?;
        }
        Ok(())
    }

    pub fn keyed_rollup_kernel(&self) -> Result<Option<CompiledKeyedRollupKernel>> {
        if self.runtime != STREAM_RUNTIME_KEYED_ROLLUP {
            return Ok(None);
        }

        if self.name != STREAM_JOB_KEYED_ROLLUP {
            anyhow::bail!(
                "stream runtime {STREAM_RUNTIME_KEYED_ROLLUP} requires job.name={STREAM_JOB_KEYED_ROLLUP}"
            );
        }
        if self.source.kind != STREAM_SOURCE_BOUNDED_INPUT {
            anyhow::bail!(
                "stream runtime {STREAM_RUNTIME_KEYED_ROLLUP} requires source.kind={STREAM_SOURCE_BOUNDED_INPUT}"
            );
        }

        let key_field =
            self.key_by.clone().context("stream runtime keyed_rollup requires key_by")?;
        if self.operators.len() != 2 {
            anyhow::bail!("stream runtime keyed_rollup requires exactly 2 operators");
        }

        let reduce = &self.operators[0];
        if reduce.kind != STREAM_OPERATOR_REDUCE {
            anyhow::bail!("stream runtime keyed_rollup requires operator[0]=reduce");
        }
        let reduce_config = json_object(
            reduce
                .config
                .as_ref()
                .context("stream runtime keyed_rollup reduce operator requires config")?,
            "runtime keyed_rollup reduce.config",
        )?;
        if json_string_field(reduce_config, "reducer", "runtime keyed_rollup reduce.config")?
            != STREAM_REDUCER_SUM
        {
            anyhow::bail!("stream runtime keyed_rollup only supports reduce.config.reducer=sum");
        }
        let value_field =
            json_string_field(reduce_config, "valueField", "runtime keyed_rollup reduce.config")?
                .to_owned();
        let output_field =
            reduce_config.get("outputField").and_then(Value::as_str).unwrap_or("value").to_owned();

        let checkpoint = &self.operators[1];
        if checkpoint.kind != STREAM_OPERATOR_EMIT_CHECKPOINT {
            anyhow::bail!("stream runtime keyed_rollup requires operator[1]=emit_checkpoint");
        }
        let checkpoint_name = checkpoint
            .name
            .clone()
            .context("stream runtime keyed_rollup emit_checkpoint requires a name")?;
        let checkpoint_config = json_object(
            checkpoint
                .config
                .as_ref()
                .context("stream runtime keyed_rollup emit_checkpoint requires config")?,
            "runtime keyed_rollup emit_checkpoint.config",
        )?;
        let checkpoint_sequence = json_i64_field(
            checkpoint_config,
            "sequence",
            "runtime keyed_rollup emit_checkpoint.config",
        )?;

        if self.views.len() != 1 {
            anyhow::bail!("stream runtime keyed_rollup requires exactly 1 materialized view");
        }
        let view = &self.views[0];
        if view.consistency != STREAM_CONSISTENCY_STRONG {
            anyhow::bail!("stream runtime keyed_rollup requires a strong materialized view");
        }
        if view.query_mode != STREAM_QUERY_MODE_BY_KEY {
            anyhow::bail!("stream runtime keyed_rollup requires query_mode=by_key");
        }
        if view.key_field.as_deref() != Some(key_field.as_str()) {
            anyhow::bail!("stream runtime keyed_rollup requires view.key_field to match key_by");
        }

        if self.queries.len() != 1 {
            anyhow::bail!("stream runtime keyed_rollup requires exactly 1 query");
        }
        let query = &self.queries[0];
        if query.view_name != view.name {
            anyhow::bail!("stream runtime keyed_rollup query must target the declared view");
        }
        if query.consistency != STREAM_CONSISTENCY_STRONG {
            anyhow::bail!("stream runtime keyed_rollup query consistency must be strong");
        }

        let checkpoint_policy = json_object(
            self.checkpoint_policy
                .as_ref()
                .context("stream runtime keyed_rollup requires checkpoint_policy")?,
            "runtime keyed_rollup checkpoint_policy",
        )?;
        if json_string_field(checkpoint_policy, "kind", "runtime keyed_rollup checkpoint_policy")?
            != STREAM_CHECKPOINT_POLICY_NAMED
        {
            anyhow::bail!(
                "stream runtime keyed_rollup requires checkpoint_policy.kind={STREAM_CHECKPOINT_POLICY_NAMED}"
            );
        }
        let checkpoints = checkpoint_policy.get("checkpoints").and_then(Value::as_array).context(
            "stream runtime keyed_rollup checkpoint_policy.checkpoints must be an array",
        )?;
        if checkpoints.len() != 1 {
            anyhow::bail!("stream runtime keyed_rollup requires exactly 1 named checkpoint");
        }
        let declared_checkpoint =
            json_object(&checkpoints[0], "runtime keyed_rollup checkpoint_policy.checkpoints[0]")?;
        if json_string_field(
            declared_checkpoint,
            "delivery",
            "runtime keyed_rollup checkpoint_policy.checkpoints[0]",
        )? != STREAM_CHECKPOINT_DELIVERY_WORKFLOW_AWAITABLE
        {
            anyhow::bail!(
                "stream runtime keyed_rollup requires checkpoint delivery=workflow_awaitable"
            );
        }
        let declared_checkpoint_name = json_string_field(
            declared_checkpoint,
            "name",
            "runtime keyed_rollup checkpoint_policy.checkpoints[0]",
        )?;
        let declared_checkpoint_sequence = json_i64_field(
            declared_checkpoint,
            "sequence",
            "runtime keyed_rollup checkpoint_policy.checkpoints[0]",
        )?;
        if declared_checkpoint_name != checkpoint_name {
            anyhow::bail!(
                "stream runtime keyed_rollup checkpoint policy must match emit_checkpoint name"
            );
        }
        if declared_checkpoint_sequence != checkpoint_sequence {
            anyhow::bail!(
                "stream runtime keyed_rollup checkpoint policy must match emit_checkpoint sequence"
            );
        }

        Ok(Some(CompiledKeyedRollupKernel {
            key_field,
            value_field,
            output_field,
            view_name: view.name.clone(),
            query_name: query.name.clone(),
            checkpoint_name,
            checkpoint_sequence,
        }))
    }
}

impl CompiledStreamJobArtifact {
    pub fn validate_persistable(&self) -> Result<()> {
        if self.definition_id.trim().is_empty() {
            anyhow::bail!("stream artifact definition_id cannot be empty");
        }
        if self.definition_version == 0 {
            anyhow::bail!("stream artifact definition_version must be greater than 0");
        }
        if self.runtime_contract != STREAMS_KERNEL_V1_CONTRACT
            && self.runtime_contract != STREAMS_KERNEL_V2_CONTRACT
        {
            anyhow::bail!(
                "stream artifact runtime_contract must be {STREAMS_KERNEL_V1_CONTRACT} or {STREAMS_KERNEL_V2_CONTRACT}"
            );
        }
        if self.entrypoint.module.trim().is_empty() || self.entrypoint.export.trim().is_empty() {
            anyhow::bail!("stream artifact entrypoint must include module and export");
        }
        self.job.validate_well_formed()?;
        if self.hash() != self.artifact_hash {
            anyhow::bail!("stream artifact hash does not match payload");
        }
        Ok(())
    }

    pub fn hash(&self) -> String {
        let mut clone = self.clone();
        clone.artifact_hash.clear();
        let encoded =
            serde_json::to_vec(&clone).expect("compiled stream job artifact serialization failed");
        let digest = Sha256::digest(encoded);
        format!("{digest:x}")
    }

    pub fn validate(&self) -> Result<()> {
        self.validate_persistable()?;
        if self.runtime_contract != STREAMS_KERNEL_V1_CONTRACT {
            anyhow::bail!("stream artifact runtime_contract must be {STREAMS_KERNEL_V1_CONTRACT}");
        }
        self.job.validate_supported_contract()?;
        Ok(())
    }
}

fn default_throughput_bridge_protocol_version() -> String {
    THROUGHPUT_BRIDGE_PROTOCOL_VERSION.to_owned()
}

fn default_throughput_bridge_operation_kind() -> String {
    ThroughputBridgeOperationKind::BulkRun.as_str().to_owned()
}

pub fn throughput_bridge_request_id(
    tenant_id: &str,
    instance_id: &str,
    run_id: &str,
    batch_id: &str,
) -> String {
    format!("throughput-bridge:{tenant_id}:{instance_id}:{run_id}:{batch_id}")
}

pub fn throughput_start_dedupe_key(source_event_id: Uuid, batch_id: &str) -> String {
    format!("throughput-start:{source_event_id}:{batch_id}")
}

pub fn throughput_start_command_id(source_event_id: Uuid, batch_id: &str) -> Uuid {
    Uuid::new_v5(
        &Uuid::NAMESPACE_URL,
        format!("unified-throughput-start:{source_event_id}:{batch_id}").as_bytes(),
    )
}

pub fn throughput_terminal_callback_dedupe_key(
    tenant_id: &str,
    instance_id: &str,
    run_id: &str,
    batch_id: &str,
    status: &str,
) -> String {
    format!("throughput-terminal:{tenant_id}:{instance_id}:{run_id}:{batch_id}:{status}")
}

pub fn throughput_terminal_callback_event_id(dedupe_key: &str) -> Uuid {
    Uuid::new_v5(&Uuid::NAMESPACE_URL, format!("throughput-terminal-event:{dedupe_key}").as_bytes())
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum WorkflowAdmissionMode {
    InlineFastStart,
    DurableWorkflow,
}

impl WorkflowAdmissionMode {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::InlineFastStart => "inline_fast_start",
            Self::DurableWorkflow => "durable_workflow",
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum ThroughputExecutionPath {
    NativeStreamV2,
}

impl ThroughputExecutionPath {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::NativeStreamV2 => "native_stream_v2",
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum FastPathRejectionReason {
    NotTinyWorkflow,
    UnsupportedReducer,
    UnsupportedActivity,
    RetriesRequired,
    UnsupportedBackend,
    RequiresInteractiveFeatures,
}

impl FastPathRejectionReason {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::NotTinyWorkflow => "not_tiny_workflow",
            Self::UnsupportedReducer => "unsupported_reducer",
            Self::UnsupportedActivity => "unsupported_activity",
            Self::RetriesRequired => "retries_required",
            Self::UnsupportedBackend => "unsupported_backend",
            Self::RequiresInteractiveFeatures => "requires_interactive_features",
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum VectorizedBulkActivityCapability {
    None,
    PayloadlessTransport,
    TinyInlineCompletion,
}

impl VectorizedBulkActivityCapability {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::None => "none",
            Self::PayloadlessTransport => "payloadless_transport",
            Self::TinyInlineCompletion => "tiny_inline_completion",
        }
    }
}

fn is_false(value: &bool) -> bool {
    !*value
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct ActivityExecutionCapabilities {
    #[serde(default, skip_serializing_if = "is_false")]
    pub payloadless_transport: bool,
    #[serde(default, skip_serializing_if = "is_false")]
    pub tiny_inline_completion: bool,
    #[serde(default, skip_serializing_if = "is_false")]
    pub omit_success_output_short_circuit: bool,
}

impl ActivityExecutionCapabilities {
    pub fn normalized(mut self) -> Self {
        if self.tiny_inline_completion {
            self.payloadless_transport = true;
        }
        self
    }

    pub fn is_empty(&self) -> bool {
        !self.payloadless_transport
            && !self.tiny_inline_completion
            && !self.omit_success_output_short_circuit
    }

    pub fn merged_with_builtin(activity_type: &str, declared: Option<&Self>) -> Self {
        let mut capabilities = builtin_activity_execution_capabilities(activity_type);
        if let Some(declared) = declared {
            capabilities.payloadless_transport |= declared.payloadless_transport;
            capabilities.tiny_inline_completion |= declared.tiny_inline_completion;
            capabilities.omit_success_output_short_circuit |=
                declared.omit_success_output_short_circuit;
        }
        capabilities.normalized()
    }
}

pub fn builtin_activity_execution_capabilities(
    activity_type: &str,
) -> ActivityExecutionCapabilities {
    match activity_type {
        BENCHMARK_FAST_COUNT_ACTIVITY => ActivityExecutionCapabilities {
            payloadless_transport: true,
            tiny_inline_completion: true,
            omit_success_output_short_circuit: false,
        },
        BENCHMARK_ECHO_ACTIVITY
        | CORE_ECHO_ACTIVITY
        | CORE_NOOP_ACTIVITY
        | CORE_ACCEPT_ACTIVITY => ActivityExecutionCapabilities {
            payloadless_transport: true,
            tiny_inline_completion: false,
            omit_success_output_short_circuit: true,
        },
        _ => ActivityExecutionCapabilities::default(),
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct WorkerActivityManifestEntry {
    pub activity_type: String,
    #[serde(default, skip_serializing_if = "ActivityExecutionCapabilities::is_empty")]
    pub capabilities: ActivityExecutionCapabilities,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct WorkerActivityManifest {
    pub schema_version: u32,
    pub task_queue: Option<String>,
    #[serde(default)]
    pub activities: Vec<WorkerActivityManifestEntry>,
}

#[derive(Debug, Clone, Deserialize)]
struct WorkerPackageManifest {
    task_queue: Option<String>,
    activity_manifest_path: Option<String>,
}

#[derive(Debug, Clone, Default)]
pub struct ActivityCapabilityRegistry {
    by_task_queue: BTreeMap<String, BTreeMap<String, ActivityExecutionCapabilities>>,
}

impl ActivityCapabilityRegistry {
    pub fn is_empty(&self) -> bool {
        self.by_task_queue.is_empty()
    }

    pub fn entry_count(&self) -> usize {
        self.by_task_queue.values().map(BTreeMap::len).sum()
    }

    pub fn lookup(
        &self,
        task_queue: &str,
        activity_type: &str,
    ) -> Option<&ActivityExecutionCapabilities> {
        self.by_task_queue
            .get(task_queue)
            .and_then(|activities| activities.get(activity_type))
            .or_else(|| {
                self.by_task_queue.get("").and_then(|activities| activities.get(activity_type))
            })
    }

    pub fn insert(
        &mut self,
        task_queue: Option<&str>,
        activity_type: &str,
        capabilities: ActivityExecutionCapabilities,
    ) {
        if capabilities.is_empty() {
            return;
        }
        self.by_task_queue
            .entry(task_queue.unwrap_or_default().to_owned())
            .or_default()
            .entry(activity_type.to_owned())
            .and_modify(|existing| {
                existing.payloadless_transport |= capabilities.payloadless_transport;
                existing.tiny_inline_completion |= capabilities.tiny_inline_completion;
                existing.omit_success_output_short_circuit |=
                    capabilities.omit_success_output_short_circuit;
                *existing = existing.clone().normalized();
            })
            .or_insert_with(|| capabilities.normalized());
    }
}

pub fn load_activity_capability_registry_from_worker_packages(
    workers_dir: &Path,
) -> Result<ActivityCapabilityRegistry> {
    let mut registry = ActivityCapabilityRegistry::default();
    if !workers_dir.exists() {
        return Ok(registry);
    }
    for entry in fs::read_dir(workers_dir)
        .with_context(|| format!("failed to read worker packages dir {}", workers_dir.display()))?
    {
        let entry = entry?;
        let package_path = entry.path().join("worker-package.json");
        if !package_path.exists() {
            continue;
        }
        let package: WorkerPackageManifest = serde_json::from_slice(
            &fs::read(&package_path)
                .with_context(|| format!("failed to read {}", package_path.display()))?,
        )
        .with_context(|| format!("failed to parse {}", package_path.display()))?;
        let Some(activity_manifest_path) = package.activity_manifest_path.as_deref() else {
            continue;
        };
        let activity_manifest_path = PathBuf::from(activity_manifest_path);
        let manifest: WorkerActivityManifest = serde_json::from_slice(
            &fs::read(&activity_manifest_path)
                .with_context(|| format!("failed to read {}", activity_manifest_path.display()))?,
        )
        .with_context(|| format!("failed to parse {}", activity_manifest_path.display()))?;
        let task_queue = manifest
            .task_queue
            .as_deref()
            .or(package.task_queue.as_deref())
            .filter(|value| !value.is_empty());
        for activity in manifest.activities {
            registry.insert(task_queue, &activity.activity_type, activity.capabilities);
        }
    }
    Ok(registry)
}

pub fn load_activity_capability_registry_from_env(
    env_var: &str,
) -> Result<ActivityCapabilityRegistry> {
    let Some(workers_dir) = std::env::var_os(env_var) else {
        return Ok(ActivityCapabilityRegistry::default());
    };
    load_activity_capability_registry_from_worker_packages(Path::new(&workers_dir))
}

pub fn resolve_activity_capabilities(
    registry: Option<&ActivityCapabilityRegistry>,
    task_queue: &str,
    activity_type: &str,
    declared: Option<&ActivityExecutionCapabilities>,
) -> Option<ActivityExecutionCapabilities> {
    let mut resolved = registry
        .and_then(|registry| registry.lookup(task_queue, activity_type))
        .cloned()
        .unwrap_or_default();
    if let Some(declared) = declared {
        resolved.payloadless_transport |= declared.payloadless_transport;
        resolved.tiny_inline_completion |= declared.tiny_inline_completion;
        resolved.omit_success_output_short_circuit |= declared.omit_success_output_short_circuit;
    }
    let resolved = resolved.normalized();
    (!resolved.is_empty()).then_some(resolved)
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TinyWorkflowRoutingDecision {
    pub admission_mode: WorkflowAdmissionMode,
    pub execution_mode: TinyWorkflowExecutionMode,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum ThroughputBackend {
    StreamV2,
}

impl ThroughputBackend {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::StreamV2 => STREAM_V2_BACKEND,
        }
    }

    pub fn default_version(&self) -> &'static str {
        match self {
            Self::StreamV2 => STREAM_V2_BACKEND_VERSION,
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum BulkReducerClass {
    Legacy,
    Mergeable,
}

impl BulkReducerClass {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Legacy => "legacy",
            Self::Mergeable => "mergeable",
        }
    }
}

pub fn bulk_reducer_name(reducer: Option<&str>) -> &str {
    reducer.unwrap_or(BULK_REDUCER_COLLECT_RESULTS)
}

pub fn bulk_reducer_class(reducer: Option<&str>) -> BulkReducerClass {
    match bulk_reducer_name(reducer) {
        BULK_REDUCER_ALL_SUCCEEDED
        | BULK_REDUCER_ALL_SETTLED
        | BULK_REDUCER_COUNT
        | BULK_REDUCER_SUM
        | BULK_REDUCER_MIN
        | BULK_REDUCER_MAX
        | BULK_REDUCER_AVG
        | BULK_REDUCER_HISTOGRAM
        | BULK_REDUCER_SAMPLE_ERRORS => BulkReducerClass::Mergeable,
        _ => BulkReducerClass::Legacy,
    }
}

pub fn bulk_reducer_is_mergeable(reducer: Option<&str>) -> bool {
    bulk_reducer_class(reducer) == BulkReducerClass::Mergeable
}

pub fn bulk_reducer_materializes_results(reducer: Option<&str>) -> bool {
    matches!(bulk_reducer_name(reducer), BULK_REDUCER_COLLECT_RESULTS | "collect_settled_results")
}

pub fn bulk_reducer_summary_field_name(reducer: Option<&str>) -> Option<&'static str> {
    match bulk_reducer_name(reducer) {
        BULK_REDUCER_COUNT => Some(BULK_REDUCER_COUNT),
        BULK_REDUCER_SUM => Some(BULK_REDUCER_SUM),
        BULK_REDUCER_MIN => Some(BULK_REDUCER_MIN),
        BULK_REDUCER_MAX => Some(BULK_REDUCER_MAX),
        BULK_REDUCER_AVG => Some(BULK_REDUCER_AVG),
        BULK_REDUCER_HISTOGRAM => Some(BULK_REDUCER_HISTOGRAM),
        BULK_REDUCER_SAMPLE_ERRORS => Some(BULK_REDUCER_SAMPLE_ERRORS),
        _ => None,
    }
}

pub fn bulk_reducer_requires_success_outputs(reducer: Option<&str>) -> bool {
    bulk_reducer_materializes_results(reducer)
        || matches!(
            bulk_reducer_name(reducer),
            BULK_REDUCER_SUM
                | BULK_REDUCER_MIN
                | BULK_REDUCER_MAX
                | BULK_REDUCER_AVG
                | BULK_REDUCER_HISTOGRAM
        )
}

pub fn bulk_reducer_requires_error_outputs(reducer: Option<&str>) -> bool {
    matches!(bulk_reducer_name(reducer), BULK_REDUCER_SAMPLE_ERRORS)
}

pub fn activity_can_short_circuit_omitted_success_output(
    activity_type: &str,
    declared_capabilities: Option<&ActivityExecutionCapabilities>,
) -> bool {
    ActivityExecutionCapabilities::merged_with_builtin(activity_type, declared_capabilities)
        .omit_success_output_short_circuit
}

pub fn can_use_payloadless_bulk_transport(
    activity_type: &str,
    declared_capabilities: Option<&ActivityExecutionCapabilities>,
    reducer: Option<&str>,
    max_attempts: u32,
    items: &[Value],
) -> bool {
    if vectorized_bulk_activity_capability(activity_type, declared_capabilities)
        == VectorizedBulkActivityCapability::TinyInlineCompletion
    {
        return true;
    }
    if vectorized_bulk_activity_capability(activity_type, declared_capabilities)
        != VectorizedBulkActivityCapability::PayloadlessTransport
        || max_attempts > 1
    {
        return false;
    }
    if bulk_reducer_requires_success_outputs(reducer)
        || bulk_reducer_requires_error_outputs(reducer)
    {
        return false;
    }
    items.iter().all(|item| {
        let Some(object) = item.as_object() else {
            return false;
        };
        !object.get("cancel").and_then(Value::as_bool).unwrap_or(false)
            && object.get("fail_until_attempt").and_then(Value::as_u64).unwrap_or_default() == 0
            && !object.contains_key("reducer_value")
    })
}

pub fn can_complete_payloadless_bulk_chunk(
    activity_type: &str,
    declared_capabilities: Option<&ActivityExecutionCapabilities>,
    omit_success_output: bool,
    item_count: u32,
    has_inline_items: bool,
    has_input_handle: bool,
    cancellation_requested: bool,
) -> bool {
    if cancellation_requested || !omit_success_output || item_count == 0 {
        return false;
    }
    if vectorized_bulk_activity_capability(activity_type, declared_capabilities)
        == VectorizedBulkActivityCapability::TinyInlineCompletion
    {
        return true;
    }
    vectorized_bulk_activity_capability(activity_type, declared_capabilities)
        == VectorizedBulkActivityCapability::PayloadlessTransport
        && !has_inline_items
        && !has_input_handle
}

pub fn can_use_payloadless_benchmark_transport(
    activity_type: &str,
    declared_capabilities: Option<&ActivityExecutionCapabilities>,
    reducer: Option<&str>,
    max_attempts: u32,
    items: &[Value],
) -> bool {
    can_use_payloadless_bulk_transport(
        activity_type,
        declared_capabilities,
        reducer,
        max_attempts,
        items,
    )
}

pub fn can_complete_payloadless_benchmark_chunk(
    activity_type: &str,
    declared_capabilities: Option<&ActivityExecutionCapabilities>,
    omit_success_output: bool,
    item_count: u32,
    has_inline_items: bool,
    has_input_handle: bool,
    cancellation_requested: bool,
) -> bool {
    can_complete_payloadless_bulk_chunk(
        activity_type,
        declared_capabilities,
        omit_success_output,
        item_count,
        has_inline_items,
        has_input_handle,
        cancellation_requested,
    )
}

pub fn can_inline_stream_v2_microbatch(
    activity_type: &str,
    declared_capabilities: Option<&ActivityExecutionCapabilities>,
    reducer: Option<&str>,
    max_attempts: u32,
    items: &[Value],
    total_items: usize,
    chunk_count: u32,
) -> bool {
    chunk_count == 1
        && total_items > 0
        && total_items <= TINY_WORKFLOW_MAX_ITEMS
        && matches!(
            reducer.unwrap_or(BULK_REDUCER_COLLECT_RESULTS),
            BULK_REDUCER_ALL_SETTLED | BULK_REDUCER_COUNT
        )
        && can_use_payloadless_bulk_transport(
            activity_type,
            declared_capabilities,
            reducer,
            max_attempts,
            items,
        )
}

pub fn can_inline_durable_tiny_fanout(
    activity_type: &str,
    declared_capabilities: Option<&ActivityExecutionCapabilities>,
    reducer: Option<&str>,
    max_attempts: u32,
    items: &[Value],
) -> bool {
    !items.is_empty()
        && items.len() <= TINY_WORKFLOW_MAX_ITEMS
        && matches!(
            reducer.unwrap_or(BULK_REDUCER_COLLECT_RESULTS),
            BULK_REDUCER_ALL_SETTLED | BULK_REDUCER_COUNT
        )
        && can_use_payloadless_bulk_transport(
            activity_type,
            declared_capabilities,
            reducer,
            max_attempts,
            items,
        )
}

pub fn vectorized_bulk_activity_capability(
    activity_type: &str,
    declared_capabilities: Option<&ActivityExecutionCapabilities>,
) -> VectorizedBulkActivityCapability {
    let capabilities =
        ActivityExecutionCapabilities::merged_with_builtin(activity_type, declared_capabilities);
    if capabilities.tiny_inline_completion {
        VectorizedBulkActivityCapability::TinyInlineCompletion
    } else if capabilities.payloadless_transport {
        VectorizedBulkActivityCapability::PayloadlessTransport
    } else {
        VectorizedBulkActivityCapability::None
    }
}

pub fn tiny_workflow_routing_decision(
    activity_type: &str,
    declared_capabilities: Option<&ActivityExecutionCapabilities>,
    reducer: Option<&str>,
    throughput_backend: Option<&str>,
    max_attempts: u32,
    items: &[Value],
    total_items: usize,
    chunk_count: u32,
) -> std::result::Result<TinyWorkflowRoutingDecision, FastPathRejectionReason> {
    if max_attempts > 1 {
        return Err(FastPathRejectionReason::RetriesRequired);
    }
    if !matches!(bulk_reducer_name(reducer), BULK_REDUCER_ALL_SETTLED | BULK_REDUCER_COUNT) {
        return Err(FastPathRejectionReason::UnsupportedReducer);
    }
    if vectorized_bulk_activity_capability(activity_type, declared_capabilities)
        == VectorizedBulkActivityCapability::None
    {
        return Err(FastPathRejectionReason::UnsupportedActivity);
    }
    if total_items == 0 || total_items > TINY_WORKFLOW_MAX_ITEMS {
        return Err(FastPathRejectionReason::NotTinyWorkflow);
    }
    if let Some(backend) = throughput_backend {
        if backend != STREAM_V2_BACKEND {
            return Err(FastPathRejectionReason::UnsupportedBackend);
        }
        if chunk_count != 1
            || !can_use_payloadless_bulk_transport(
                activity_type,
                declared_capabilities,
                reducer,
                max_attempts,
                items,
            )
        {
            return Err(FastPathRejectionReason::NotTinyWorkflow);
        }
        return Ok(TinyWorkflowRoutingDecision {
            admission_mode: WorkflowAdmissionMode::InlineFastStart,
            execution_mode: TinyWorkflowExecutionMode::Throughput,
        });
    }
    if !can_use_payloadless_bulk_transport(
        activity_type,
        declared_capabilities,
        reducer,
        max_attempts,
        items,
    ) {
        return Err(FastPathRejectionReason::NotTinyWorkflow);
    }
    Ok(TinyWorkflowRoutingDecision {
        admission_mode: WorkflowAdmissionMode::InlineFastStart,
        execution_mode: TinyWorkflowExecutionMode::Durable,
    })
}

pub fn benchmark_echo_item_requires_output(item: &Value) -> bool {
    item.get("reducer_value").is_some()
}

pub fn execute_benchmark_echo(attempt: u32, input: &Value) -> Result<Value> {
    let fail_until_attempt =
        input.get("fail_until_attempt").and_then(Value::as_u64).unwrap_or_default() as u32;
    if input.get("cancel").and_then(Value::as_bool).unwrap_or(false) {
        anyhow::bail!("activity cancelled");
    }
    if attempt <= fail_until_attempt {
        anyhow::bail!("benchmark configured failure on attempt {attempt}");
    }
    if let Some(reducer_value) = input.get("reducer_value") {
        return Ok(reducer_value.clone());
    }
    Ok(input.clone())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BenchmarkCompactInputMeta {
    pub total_items: u32,
    pub payload_size: Option<u32>,
    pub chunk_size: Option<u32>,
}

pub fn benchmark_compact_input_spec(total_items: u32) -> Value {
    serde_json::json!({
        "kind": BENCHMARK_COMPACT_INPUT_KIND,
        "count": total_items,
    })
}

pub fn benchmark_compact_input_spec_with_payload(total_items: u32, payload_size: u32) -> Value {
    serde_json::json!({
        "kind": BENCHMARK_COMPACT_INPUT_KIND,
        "count": total_items,
        "payload_size": payload_size,
    })
}

pub fn parse_benchmark_compact_input_meta(value: &Value) -> Option<BenchmarkCompactInputMeta> {
    let object = value.as_object()?;
    if object.get("kind").and_then(Value::as_str) != Some(BENCHMARK_COMPACT_INPUT_KIND) {
        return None;
    }
    Some(BenchmarkCompactInputMeta {
        total_items: object
            .get("count")
            .and_then(Value::as_u64)
            .and_then(|count| u32::try_from(count).ok())?,
        payload_size: object
            .get("payload_size")
            .and_then(Value::as_u64)
            .and_then(|size| u32::try_from(size).ok()),
        chunk_size: object
            .get("chunk_size")
            .and_then(Value::as_u64)
            .and_then(|size| u32::try_from(size).ok()),
    })
}

pub fn parse_benchmark_compact_input_spec(value: &Value) -> Option<u32> {
    parse_benchmark_compact_input_meta(value).map(|meta| meta.total_items)
}

pub fn benchmark_compact_input_handle(batch_id: &str, total_items: u32) -> PayloadHandle {
    benchmark_compact_input_handle_with_meta(batch_id, total_items, None, None)
}

pub fn benchmark_compact_input_handle_with_meta(
    batch_id: &str,
    total_items: u32,
    payload_size: Option<u32>,
    chunk_size: Option<u32>,
) -> PayloadHandle {
    let mut key = format!("{BENCHMARK_COMPACT_INPUT_HANDLE_PREFIX}:{batch_id}:{total_items}");
    if let Some(payload_size) = payload_size {
        key.push_str(&format!(":p{payload_size}"));
    }
    if let Some(chunk_size) = chunk_size {
        key.push_str(&format!(":c{chunk_size}"));
    }
    PayloadHandle::Inline { key }
}

pub fn parse_benchmark_compact_input_meta_from_handle(
    handle: &PayloadHandle,
) -> Option<BenchmarkCompactInputMeta> {
    let PayloadHandle::Inline { key } = handle else {
        return None;
    };
    let rest = key.strip_prefix(&format!("{BENCHMARK_COMPACT_INPUT_HANDLE_PREFIX}:"))?;
    let mut parts = rest.split(':');
    let _batch_id = parts.next()?;
    let total_items = parts.next()?.parse::<u32>().ok()?;
    let mut payload_size = None;
    let mut chunk_size = None;
    for part in parts {
        if let Some(value) = part.strip_prefix('p') {
            payload_size = value.parse::<u32>().ok();
        } else if let Some(value) = part.strip_prefix('c') {
            chunk_size = value.parse::<u32>().ok();
        }
    }
    Some(BenchmarkCompactInputMeta { total_items, payload_size, chunk_size })
}

pub fn parse_benchmark_compact_total_items_from_handle(handle: &PayloadHandle) -> Option<u32> {
    parse_benchmark_compact_input_meta_from_handle(handle).map(|meta| meta.total_items)
}

pub fn synthesize_benchmark_echo_items(
    meta: BenchmarkCompactInputMeta,
    chunk_index: u32,
    item_count: u32,
) -> Vec<Value> {
    let chunk_size = meta.chunk_size.unwrap_or(item_count.max(1));
    let start = usize::try_from(chunk_index)
        .unwrap_or_default()
        .saturating_mul(usize::try_from(chunk_size).unwrap_or_default().max(1));
    let payload =
        "x".repeat(usize::try_from(meta.payload_size.unwrap_or_default()).unwrap_or_default());
    (0..usize::try_from(item_count).unwrap_or_default())
        .map(|offset| {
            serde_json::json!({
                "index": start.saturating_add(offset),
                "payload": payload,
                "fail_until_attempt": 0,
                "cancel": false,
            })
        })
        .collect()
}

pub fn bulk_reducer_settles(reducer: Option<&str>) -> bool {
    matches!(
        bulk_reducer_name(reducer),
        BULK_REDUCER_ALL_SETTLED | BULK_REDUCER_COUNT | BULK_REDUCER_SAMPLE_ERRORS
    )
}

pub fn bulk_reducer_supports_stream_v2(reducer: Option<&str>) -> bool {
    matches!(
        bulk_reducer_name(reducer),
        BULK_REDUCER_ALL_SUCCEEDED
            | BULK_REDUCER_ALL_SETTLED
            | BULK_REDUCER_COUNT
            | BULK_REDUCER_SUM
            | BULK_REDUCER_MIN
            | BULK_REDUCER_MAX
            | BULK_REDUCER_AVG
            | BULK_REDUCER_HISTOGRAM
            | BULK_REDUCER_SAMPLE_ERRORS
    )
}

pub fn bulk_reducer_default_summary_value(reducer: Option<&str>) -> Option<Value> {
    match bulk_reducer_name(reducer) {
        BULK_REDUCER_COUNT => Some(Value::from(0_u64)),
        BULK_REDUCER_SUM => Some(Value::from(0.0)),
        BULK_REDUCER_MIN | BULK_REDUCER_MAX | BULK_REDUCER_AVG => Some(Value::Null),
        BULK_REDUCER_HISTOGRAM => Some(Value::Object(serde_json::Map::new())),
        BULK_REDUCER_SAMPLE_ERRORS => Some(serde_json::json!({
            "sample": [],
            "total": 0,
            "truncated": false,
        })),
        _ => None,
    }
}

pub fn bulk_reducer_reduce_values(
    reducer: Option<&str>,
    values: &[Value],
) -> Result<Option<Value>> {
    let reducer = bulk_reducer_name(reducer);
    let Some(field_name) = bulk_reducer_summary_field_name(Some(reducer)) else {
        return Ok(None);
    };
    if field_name == BULK_REDUCER_COUNT {
        return Ok(Some(Value::from(
            u64::try_from(values.len()).context("reducer value count exceeds u64")?,
        )));
    }
    if field_name == BULK_REDUCER_HISTOGRAM {
        let mut buckets = BTreeMap::<String, u64>::new();
        for value in values {
            *buckets.entry(histogram_bucket_key(value)?).or_default() += 1;
        }
        return Ok(Some(Value::Object(serde_json::Map::from_iter(
            buckets.into_iter().map(|(bucket, count)| (bucket, Value::from(count))),
        ))));
    }

    let numbers = values
        .iter()
        .map(|value| {
            value.as_f64().ok_or_else(|| {
                anyhow::anyhow!("bulk reducer {reducer} requires numeric outputs, got {value}")
            })
        })
        .collect::<Result<Vec<_>>>()?;

    let reduced = match reducer {
        BULK_REDUCER_SUM => Value::from(numbers.iter().copied().sum::<f64>()),
        BULK_REDUCER_MIN => {
            numbers.iter().copied().reduce(f64::min).map(Value::from).unwrap_or(Value::Null)
        }
        BULK_REDUCER_MAX => {
            numbers.iter().copied().reduce(f64::max).map(Value::from).unwrap_or(Value::Null)
        }
        BULK_REDUCER_AVG => {
            if numbers.is_empty() {
                Value::Null
            } else {
                Value::from(numbers.iter().copied().sum::<f64>() / numbers.len() as f64)
            }
        }
        _ => return Ok(None),
    };
    Ok(Some(reduced))
}

pub fn bulk_reducer_reduce_errors(
    reducer: Option<&str>,
    errors: &[(String, u32)],
) -> Result<Option<Value>> {
    if bulk_reducer_name(reducer) != BULK_REDUCER_SAMPLE_ERRORS {
        return Ok(None);
    }

    let mut sample = Vec::new();
    let mut total = 0_u64;
    for (message, weight) in errors {
        total = total.saturating_add(u64::from(*weight));
        if sample.len() < BULK_REDUCER_ERROR_SAMPLE_LIMIT {
            sample.push(Value::String(message.clone()));
        }
    }

    Ok(Some(serde_json::json!({
        "sample": sample,
        "total": total,
        "truncated": usize::try_from(total).unwrap_or(usize::MAX) > BULK_REDUCER_ERROR_SAMPLE_LIMIT,
    })))
}

fn histogram_bucket_key(value: &Value) -> Result<String> {
    Ok(match value {
        Value::Null => "null".to_owned(),
        Value::Bool(boolean) => boolean.to_string(),
        Value::Number(number) => number.to_string(),
        Value::String(string) => string.clone(),
        other => serde_json::to_string(other).context("failed to serialize histogram bucket")?,
    })
}

pub fn throughput_execution_mode(_backend: &str) -> &'static str {
    "throughput"
}

pub fn throughput_routing_reason(
    backend: &str,
    execution_policy: Option<&str>,
    reducer: Option<&str>,
) -> &'static str {
    if stream_v2_fast_lane_enabled(backend, execution_policy, reducer) {
        "stream_v2_fast_lane"
    } else {
        "stream_v2_selected"
    }
}

pub fn throughput_reducer_version(_reducer: Option<&str>) -> &'static str {
    "builtin/v1"
}

pub fn throughput_reducer_execution_path(
    backend: &str,
    execution_policy: Option<&str>,
    reducer: Option<&str>,
) -> &'static str {
    if stream_v2_fast_lane_enabled(backend, execution_policy, reducer) {
        "mergeable_fast_lane"
    } else if bulk_reducer_is_mergeable(reducer) {
        "mergeable_owner_apply"
    } else if bulk_reducer_materializes_results(reducer) {
        "materialized_results"
    } else {
        "summary_only"
    }
}

pub fn stream_v2_fast_lane_enabled(
    backend: &str,
    execution_policy: Option<&str>,
    reducer: Option<&str>,
) -> bool {
    backend == STREAM_V2_BACKEND
        && execution_policy.unwrap_or("default") == "eager"
        && bulk_reducer_is_mergeable(reducer)
        && bulk_reducer_supports_stream_v2(reducer)
}

pub fn planned_reduction_tree_depth(
    chunk_count: u32,
    aggregation_group_count: u32,
    reducer: Option<&str>,
) -> u32 {
    if !bulk_reducer_is_mergeable(reducer) || chunk_count <= 1 || aggregation_group_count <= 1 {
        return 1;
    }
    if aggregation_group_count >= 16 && chunk_count >= aggregation_group_count.saturating_mul(32) {
        3
    } else {
        2
    }
}

pub fn reduction_tree_level_counts(
    aggregation_group_count: u32,
    aggregation_tree_depth: u32,
) -> Vec<u32> {
    let leaf_count = aggregation_group_count.max(1);
    let requested_depth = aggregation_tree_depth.max(1) as usize;
    let mut counts = vec![leaf_count];
    while counts.len() < requested_depth {
        let previous = *counts.last().expect("reduction tree counts missing prior level");
        let next = if counts.len() + 1 == requested_depth {
            1
        } else if previous <= 1 {
            1
        } else {
            let sqrt = (previous as f64).sqrt().ceil() as u32;
            sqrt.max(2).min(previous.saturating_sub(1))
        };
        counts.push(next.max(1));
        if previous <= 1 {
            break;
        }
    }
    counts
}

pub fn reduction_tree_node_id(level: u32, slot: u32) -> u32 {
    if level == 0 {
        slot
    } else {
        (level << REDUCTION_GROUP_LEVEL_SHIFT) | (slot & REDUCTION_GROUP_SLOT_MASK)
    }
}

pub fn reduction_tree_node_level(group_id: u32) -> u32 {
    group_id >> REDUCTION_GROUP_LEVEL_SHIFT
}

pub fn reduction_tree_node_slot(group_id: u32) -> u32 {
    if reduction_tree_node_level(group_id) == 0 {
        group_id
    } else {
        group_id & REDUCTION_GROUP_SLOT_MASK
    }
}

pub fn reduction_tree_parent_group_id(
    group_id: u32,
    aggregation_group_count: u32,
    aggregation_tree_depth: u32,
) -> Option<u32> {
    let level_counts = reduction_tree_level_counts(aggregation_group_count, aggregation_tree_depth);
    let level = usize::try_from(reduction_tree_node_level(group_id)).ok()?;
    if level + 1 >= level_counts.len() {
        return None;
    }
    let child_count = level_counts[level];
    let parent_count = level_counts[level + 1];
    let slot = reduction_tree_node_slot(group_id);
    if child_count == 0 || parent_count == 0 {
        return None;
    }
    let parent_slot = ((u64::from(slot) * u64::from(parent_count)) / u64::from(child_count)) as u32;
    Some(reduction_tree_node_id((level + 1) as u32, parent_slot.min(parent_count - 1)))
}

pub fn reduction_tree_child_group_ids(
    parent_group_id: u32,
    aggregation_group_count: u32,
    aggregation_tree_depth: u32,
) -> Vec<u32> {
    let level_counts = reduction_tree_level_counts(aggregation_group_count, aggregation_tree_depth);
    let parent_level = match usize::try_from(reduction_tree_node_level(parent_group_id)) {
        Ok(level) => level,
        Err(_) => return Vec::new(),
    };
    if parent_level == 0 || parent_level >= level_counts.len() {
        return Vec::new();
    }
    let child_level = parent_level - 1;
    let child_count = level_counts[child_level];
    let parent_count = level_counts[parent_level];
    let parent_slot = reduction_tree_node_slot(parent_group_id);
    (0..child_count)
        .filter(|child_slot| {
            (((u64::from(*child_slot) * u64::from(parent_count)) / u64::from(child_count)) as u32)
                == parent_slot
        })
        .map(|child_slot| reduction_tree_node_id(child_level as u32, child_slot))
        .collect()
}

pub fn encode_cbor<T: Serialize>(value: &T, subject: &str) -> Result<Vec<u8>> {
    serde_cbor::to_vec(value).with_context(|| format!("failed to encode {subject} as CBOR"))
}

pub fn decode_cbor<T: DeserializeOwned>(bytes: &[u8], subject: &str) -> Result<T> {
    serde_cbor::from_slice(bytes).with_context(|| format!("failed to decode {subject} from CBOR"))
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum PayloadHandle {
    Inline { key: String },
    Manifest { key: String, store: String },
    ManifestSlice { key: String, store: String, start: usize, len: usize },
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
pub struct CollectResultsChunkSegmentRef {
    pub chunk_id: String,
    pub chunk_index: u32,
    pub item_count: u32,
    pub result_handle: PayloadHandle,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CollectResultsBatchManifest {
    pub kind: String,
    pub chunks: Vec<CollectResultsChunkSegmentRef>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ThroughputBatchIdentity {
    pub tenant_id: String,
    pub instance_id: String,
    pub run_id: String,
    pub batch_id: String,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TinyWorkflowExecutionMode {
    Throughput,
    Durable,
}

impl TinyWorkflowExecutionMode {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Throughput => "throughput",
            Self::Durable => "durable",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TinyWorkflowStartItem {
    pub instance_id: String,
    pub run_id: String,
    pub request_id: Option<String>,
    pub trigger_event_id: Uuid,
    pub accepted_at: DateTime<Utc>,
    pub input: Value,
    pub memo: Option<Value>,
    pub search_attributes: Option<Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TinyWorkflowStartCommand {
    pub dedupe_key: String,
    pub tenant_id: String,
    pub definition_id: String,
    pub definition_version: u32,
    pub artifact_hash: String,
    pub workflow_task_queue: String,
    pub execution_mode: TinyWorkflowExecutionMode,
    pub item: TinyWorkflowStartItem,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TinyWorkflowStartBatchCommand {
    pub dedupe_key: String,
    pub tenant_id: String,
    pub definition_id: String,
    pub definition_version: u32,
    pub artifact_hash: String,
    pub workflow_task_queue: String,
    pub execution_mode: TinyWorkflowExecutionMode,
    pub items: Vec<TinyWorkflowStartItem>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct StartThroughputRunCommand {
    pub dedupe_key: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub bridge_request_id: String,
    #[serde(default = "default_throughput_bridge_protocol_version")]
    pub bridge_protocol_version: String,
    #[serde(default = "default_throughput_bridge_operation_kind")]
    pub bridge_operation_kind: String,
    pub tenant_id: String,
    pub definition_id: String,
    pub definition_version: Option<u32>,
    pub artifact_hash: Option<String>,
    pub instance_id: String,
    pub run_id: String,
    pub batch_id: String,
    pub activity_type: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub activity_capabilities: Option<ActivityExecutionCapabilities>,
    pub task_queue: String,
    pub state: Option<String>,
    pub chunk_size: u32,
    pub max_attempts: u32,
    pub retry_delay_ms: u64,
    pub total_items: u32,
    pub aggregation_group_count: u32,
    pub execution_policy: Option<String>,
    pub reducer: Option<String>,
    pub throughput_backend: String,
    pub throughput_backend_version: String,
    pub routing_reason: String,
    pub admission_policy_version: String,
    pub input_handle: PayloadHandle,
    pub result_handle: PayloadHandle,
}

impl StartThroughputRunCommand {
    pub fn resolved_bridge_request_id(&self) -> String {
        if self.bridge_request_id.is_empty() {
            throughput_bridge_request_id(
                &self.tenant_id,
                &self.instance_id,
                &self.run_id,
                &self.batch_id,
            )
        } else {
            self.bridge_request_id.clone()
        }
    }

    pub fn resolved_bridge_protocol_version(&self) -> &str {
        if self.bridge_protocol_version.trim().is_empty() {
            THROUGHPUT_BRIDGE_PROTOCOL_VERSION
        } else {
            self.bridge_protocol_version.as_str()
        }
    }

    pub fn resolved_bridge_operation_kind(&self) -> ThroughputBridgeOperationKind {
        ThroughputBridgeOperationKind::parse(&self.bridge_operation_kind)
            .unwrap_or(ThroughputBridgeOperationKind::BulkRun)
    }
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub activity_capabilities: Option<ActivityExecutionCapabilities>,
    pub task_queue: String,
    pub state: Option<String>,
    pub chunk_size: u32,
    pub max_attempts: u32,
    pub retry_delay_ms: u64,
    pub total_items: u32,
    pub aggregation_group_count: u32,
    pub execution_policy: Option<String>,
    pub reducer: Option<String>,
    pub throughput_backend: String,
    pub throughput_backend_version: String,
    pub routing_reason: String,
    pub admission_policy_version: String,
    pub input_handle: PayloadHandle,
    pub result_handle: PayloadHandle,
    pub items: Vec<Value>,
}

impl CreateBatchCommand {
    pub fn to_start_throughput_run(&self) -> StartThroughputRunCommand {
        StartThroughputRunCommand {
            dedupe_key: self.dedupe_key.clone(),
            bridge_request_id: throughput_bridge_request_id(
                &self.tenant_id,
                &self.instance_id,
                &self.run_id,
                &self.batch_id,
            ),
            bridge_protocol_version: THROUGHPUT_BRIDGE_PROTOCOL_VERSION.to_owned(),
            bridge_operation_kind: ThroughputBridgeOperationKind::BulkRun.as_str().to_owned(),
            tenant_id: self.tenant_id.clone(),
            definition_id: self.definition_id.clone(),
            definition_version: Some(self.definition_version),
            artifact_hash: Some(self.artifact_hash.clone()),
            instance_id: self.instance_id.clone(),
            run_id: self.run_id.clone(),
            batch_id: self.batch_id.clone(),
            activity_type: self.activity_type.clone(),
            activity_capabilities: self.activity_capabilities.clone(),
            task_queue: self.task_queue.clone(),
            state: self.state.clone(),
            chunk_size: self.chunk_size,
            max_attempts: self.max_attempts,
            retry_delay_ms: self.retry_delay_ms,
            total_items: self.total_items,
            aggregation_group_count: self.aggregation_group_count,
            execution_policy: self.execution_policy.clone(),
            reducer: self.reducer.clone(),
            throughput_backend: self.throughput_backend.clone(),
            throughput_backend_version: self.throughput_backend_version.clone(),
            routing_reason: self.routing_reason.clone(),
            admission_policy_version: self.admission_policy_version.clone(),
            input_handle: self.input_handle.clone(),
            result_handle: self.result_handle.clone(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ThroughputCommand {
    StartThroughputRun(StartThroughputRunCommand),
    CreateBatch(CreateBatchCommand),
    ScheduleStreamJob(ScheduleStreamJobCommand),
    CancelStreamJob(CancelStreamJobCommand),
    TinyWorkflowStart(TinyWorkflowStartCommand),
    TinyWorkflowStartBatch(TinyWorkflowStartBatchCommand),
    CancelBatch { identity: ThroughputBatchIdentity, reason: String },
    TimeoutBatch { identity: ThroughputBatchIdentity, reason: String },
    ReplanGroups { identity: ThroughputBatchIdentity, aggregation_group_count: u32 },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ScheduleStreamJobCommand {
    pub tenant_id: String,
    pub stream_instance_id: String,
    pub stream_run_id: String,
    pub job_id: String,
    pub handle_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CancelStreamJobCommand {
    pub tenant_id: String,
    pub stream_instance_id: String,
    pub stream_run_id: String,
    pub job_id: String,
    pub handle_id: String,
    pub reason: Option<String>,
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
    StreamJobExecutionPlanned {
        handle_id: String,
        job_id: String,
        job_name: String,
        view_name: String,
        checkpoint_name: String,
        checkpoint_sequence: i64,
        input_item_count: u64,
        materialized_key_count: u64,
        active_partitions: Vec<i32>,
        owner_epoch: u64,
        planned_at: DateTime<Utc>,
    },
    StreamJobViewUpdated {
        handle_id: String,
        job_id: String,
        view_name: String,
        logical_key: String,
        output: Value,
        checkpoint_sequence: i64,
        updated_at: DateTime<Utc>,
    },
    StreamJobCheckpointReached {
        handle_id: String,
        job_id: String,
        checkpoint_name: String,
        checkpoint_sequence: i64,
        stream_partition_id: i32,
        owner_epoch: u64,
        reached_at: DateTime<Utc>,
    },
    StreamJobTerminalized {
        handle_id: String,
        job_id: String,
        owner_epoch: u64,
        status: String,
        output: Option<Value>,
        error: Option<String>,
        terminal_at: DateTime<Utc>,
    },
    BatchCreated {
        identity: ThroughputBatchIdentity,
        task_queue: String,
        execution_policy: Option<String>,
        reducer: Option<String>,
        reducer_class: String,
        aggregation_tree_depth: u32,
        fast_lane_enabled: bool,
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
        #[serde(default)]
        level: u32,
        #[serde(default)]
        parent_group_id: Option<u32>,
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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct StreamsViewRecord {
    pub tenant_id: String,
    pub instance_id: String,
    pub run_id: String,
    pub job_id: String,
    pub handle_id: String,
    pub view_name: String,
    pub logical_key: String,
    pub output: Value,
    pub checkpoint_sequence: i64,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum StreamsProjectionEvent {
    UpsertStreamJobView { view: StreamsViewRecord },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum StreamsChangelogPayload {
    StreamJobExecutionPlanned {
        handle_id: String,
        job_id: String,
        job_name: String,
        view_name: String,
        checkpoint_name: String,
        checkpoint_sequence: i64,
        input_item_count: u64,
        materialized_key_count: u64,
        active_partitions: Vec<i32>,
        owner_epoch: u64,
        planned_at: DateTime<Utc>,
    },
    StreamJobViewUpdated {
        handle_id: String,
        job_id: String,
        view_name: String,
        logical_key: String,
        output: Value,
        checkpoint_sequence: i64,
        updated_at: DateTime<Utc>,
    },
    StreamJobCheckpointReached {
        handle_id: String,
        job_id: String,
        checkpoint_name: String,
        checkpoint_sequence: i64,
        stream_partition_id: i32,
        owner_epoch: u64,
        reached_at: DateTime<Utc>,
    },
    StreamJobTerminalized {
        handle_id: String,
        job_id: String,
        owner_epoch: u64,
        status: String,
        output: Option<Value>,
        error: Option<String>,
        terminal_at: DateTime<Utc>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct StreamsChangelogEntry {
    pub entry_id: Uuid,
    pub occurred_at: DateTime<Utc>,
    pub partition_key: String,
    pub payload: StreamsChangelogPayload,
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
        let value =
            serde_json::from_slice(&bytes).context("failed to deserialize payload value")?;
        materialize_payload_handle_value(handle, value)
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
            PayloadHandle::ManifestSlice { key, store, .. } if store == LOCAL_FILESYSTEM_STORE => {
                Ok(self.path_for_key(key))
            }
            PayloadHandle::Manifest { store, .. } => {
                anyhow::bail!("unsupported payload store {store}")
            }
            PayloadHandle::ManifestSlice { store, .. } => {
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
        let value =
            serde_json::from_slice(&bytes).context("failed to deserialize payload value")?;
        materialize_payload_handle_value(handle, value)
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
            PayloadHandle::ManifestSlice { key, store, .. } if store == S3_STORE => {
                Ok(self.object_key(key))
            }
            PayloadHandle::Manifest { store, .. } => {
                anyhow::bail!("payload handle store {store} does not match configured s3 store")
            }
            PayloadHandle::ManifestSlice { store, .. } => {
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
            PayloadHandle::Manifest { store, .. } | PayloadHandle::ManifestSlice { store, .. }
                if store == LOCAL_FILESYSTEM_STORE =>
            {
                self.filesystem
                    .as_ref()
                    .context("filesystem payload store not configured")?
                    .read_value(handle)
            }
            PayloadHandle::Manifest { store, .. } | PayloadHandle::ManifestSlice { store, .. }
                if store == S3_STORE =>
            {
                self.s3
                    .as_ref()
                    .context("s3 payload store not configured")?
                    .read_value(handle)
                    .await
            }
            PayloadHandle::Manifest { store, .. } | PayloadHandle::ManifestSlice { store, .. } => {
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

fn materialize_payload_handle_value(handle: &PayloadHandle, value: Value) -> Result<Value> {
    match handle {
        PayloadHandle::ManifestSlice { start, len, .. } => match value {
            Value::Array(items) => {
                let end = start.saturating_add(*len);
                if *start > items.len() || end > items.len() {
                    anyhow::bail!(
                        "payload slice {}..{} is out of bounds for {} items",
                        start,
                        end,
                        items.len()
                    );
                }
                Ok(Value::Array(items.into_iter().skip(*start).take(*len).collect()))
            }
            other => anyhow::bail!("payload slice handle did not resolve to an array: {other}"),
        },
        _ => Ok(value),
    }
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

    #[test]
    fn native_start_throughput_run_command_round_trips() -> Result<()> {
        let command = ThroughputCommandEnvelope {
            command_id: Uuid::now_v7(),
            occurred_at: Utc::now(),
            dedupe_key: "throughput-start:test".to_owned(),
            partition_key: "batch-a:0".to_owned(),
            payload: ThroughputCommand::StartThroughputRun(StartThroughputRunCommand {
                dedupe_key: "throughput-start:test".to_owned(),
                bridge_request_id: throughput_bridge_request_id(
                    "tenant-a",
                    "instance-a",
                    "run-a",
                    "batch-a",
                ),
                bridge_protocol_version: THROUGHPUT_BRIDGE_PROTOCOL_VERSION.to_owned(),
                bridge_operation_kind: ThroughputBridgeOperationKind::BulkRun.as_str().to_owned(),
                tenant_id: "tenant-a".to_owned(),
                definition_id: "demo".to_owned(),
                definition_version: Some(7),
                artifact_hash: Some("artifact-a".to_owned()),
                instance_id: "instance-a".to_owned(),
                run_id: "run-a".to_owned(),
                batch_id: "batch-a".to_owned(),
                activity_type: "benchmark.echo".to_owned(),
                activity_capabilities: None,
                task_queue: "bulk".to_owned(),
                state: Some("join".to_owned()),
                chunk_size: 16,
                max_attempts: 3,
                retry_delay_ms: 1000,
                total_items: 32,
                aggregation_group_count: 2,
                execution_policy: Some("parallel".to_owned()),
                reducer: Some("count".to_owned()),
                throughput_backend: "stream-v2".to_owned(),
                throughput_backend_version: "2.0.0".to_owned(),
                routing_reason: "stream_v2_selected".to_owned(),
                admission_policy_version: ADMISSION_POLICY_VERSION.to_owned(),
                input_handle: PayloadHandle::Manifest {
                    key: "batches/batch-a/input".to_owned(),
                    store: LOCAL_FILESYSTEM_STORE.to_owned(),
                },
                result_handle: PayloadHandle::Inline { key: "bulk-result:batch-a".to_owned() },
            }),
        };

        let encoded = serde_json::to_value(&command)?;
        let round_tripped: ThroughputCommandEnvelope = serde_json::from_value(encoded.clone())?;
        assert_eq!(round_tripped, command);
        assert!(encoded.to_string().contains("\"start_throughput_run\""));
        assert!(!encoded.to_string().contains("\"items\""));
        Ok(())
    }

    #[test]
    fn schedule_stream_job_command_round_trips() -> Result<()> {
        let command = ThroughputCommandEnvelope {
            command_id: Uuid::now_v7(),
            occurred_at: Utc::now(),
            dedupe_key: "stream-job-submit:handle-a".to_owned(),
            partition_key: throughput_partition_key("job-a", 0),
            payload: ThroughputCommand::ScheduleStreamJob(ScheduleStreamJobCommand {
                tenant_id: "tenant-a".to_owned(),
                stream_instance_id: "stream-a".to_owned(),
                stream_run_id: "run-a".to_owned(),
                job_id: "job-a".to_owned(),
                handle_id: "handle-a".to_owned(),
            }),
        };

        let encoded = serde_json::to_value(&command)?;
        let round_tripped: ThroughputCommandEnvelope = serde_json::from_value(encoded.clone())?;
        assert_eq!(round_tripped, command);
        assert!(encoded.to_string().contains("\"schedule_stream_job\""));
        Ok(())
    }

    #[test]
    fn compiled_stream_artifact_validates_keyed_rollup_kernel_contract() -> Result<()> {
        let mut artifact = CompiledStreamJobArtifact {
            definition_id: "payments-rollup".to_owned(),
            definition_version: 3,
            compiler_version: "0.1.0".to_owned(),
            runtime_contract: STREAMS_KERNEL_V1_CONTRACT.to_owned(),
            source_language: "typescript".to_owned(),
            entrypoint: StreamArtifactEntrypoint {
                module: "examples/typescript-stream-jobs/keyed-rollup-stream-job.ts".to_owned(),
                export: "keyedRollupStreamJob".to_owned(),
            },
            source_files: vec![
                "examples/typescript-stream-jobs/keyed-rollup-stream-job.ts".to_owned(),
            ],
            source_map: BTreeMap::new(),
            job: CompiledStreamJob {
                name: STREAM_JOB_KEYED_ROLLUP.to_owned(),
                runtime: STREAM_RUNTIME_KEYED_ROLLUP.to_owned(),
                source: CompiledStreamSource {
                    kind: STREAM_SOURCE_BOUNDED_INPUT.to_owned(),
                    name: None,
                    binding: None,
                    config: None,
                },
                key_by: Some("accountId".to_owned()),
                states: vec![],
                operators: vec![
                    CompiledStreamOperator {
                        kind: STREAM_OPERATOR_REDUCE.to_owned(),
                        operator_id: None,
                        name: Some("sum-account-totals".to_owned()),
                        inputs: vec![],
                        outputs: vec![],
                        state_ids: vec![],
                        config: Some(serde_json::json!({
                            "reducer": STREAM_REDUCER_SUM,
                            "valueField": "amount",
                            "outputField": "totalAmount",
                        })),
                    },
                    CompiledStreamOperator {
                        kind: STREAM_OPERATOR_EMIT_CHECKPOINT.to_owned(),
                        operator_id: None,
                        name: Some("hourly-rollup-ready".to_owned()),
                        inputs: vec![],
                        outputs: vec![],
                        state_ids: vec![],
                        config: Some(serde_json::json!({
                            "sequence": 1,
                        })),
                    },
                ],
                checkpoint_policy: Some(serde_json::json!({
                    "kind": STREAM_CHECKPOINT_POLICY_NAMED,
                    "checkpoints": [
                        {
                            "name": "hourly-rollup-ready",
                            "delivery": STREAM_CHECKPOINT_DELIVERY_WORKFLOW_AWAITABLE,
                            "sequence": 1,
                        }
                    ],
                })),
                views: vec![CompiledStreamView {
                    name: "accountTotals".to_owned(),
                    consistency: STREAM_CONSISTENCY_STRONG.to_owned(),
                    query_mode: STREAM_QUERY_MODE_BY_KEY.to_owned(),
                    view_id: None,
                    key_field: Some("accountId".to_owned()),
                    value_fields: vec![
                        "accountId".to_owned(),
                        "totalAmount".to_owned(),
                        "asOfCheckpoint".to_owned(),
                    ],
                    supported_consistencies: vec![],
                    retention_seconds: None,
                }],
                queries: vec![CompiledStreamQuery {
                    name: "accountTotals".to_owned(),
                    view_name: "accountTotals".to_owned(),
                    consistency: STREAM_CONSISTENCY_STRONG.to_owned(),
                    query_id: None,
                    arg_fields: vec![],
                }],
                classification: None,
                metadata: None,
            },
            artifact_hash: String::new(),
        };
        artifact.artifact_hash = artifact.hash();

        artifact.validate()?;
        assert_eq!(
            artifact.job.keyed_rollup_kernel()?,
            Some(CompiledKeyedRollupKernel {
                key_field: "accountId".to_owned(),
                value_field: "amount".to_owned(),
                output_field: "totalAmount".to_owned(),
                view_name: "accountTotals".to_owned(),
                query_name: "accountTotals".to_owned(),
                checkpoint_name: "hourly-rollup-ready".to_owned(),
                checkpoint_sequence: 1,
            })
        );
        Ok(())
    }

    #[test]
    fn compiled_stream_artifact_rejects_mismatched_keyed_rollup_checkpoint_policy() {
        let mut artifact = CompiledStreamJobArtifact {
            definition_id: "payments-rollup".to_owned(),
            definition_version: 3,
            compiler_version: "0.1.0".to_owned(),
            runtime_contract: STREAMS_KERNEL_V1_CONTRACT.to_owned(),
            source_language: "typescript".to_owned(),
            entrypoint: StreamArtifactEntrypoint {
                module: "examples/typescript-stream-jobs/keyed-rollup-stream-job.ts".to_owned(),
                export: "keyedRollupStreamJob".to_owned(),
            },
            source_files: vec![],
            source_map: BTreeMap::new(),
            job: CompiledStreamJob {
                name: STREAM_JOB_KEYED_ROLLUP.to_owned(),
                runtime: STREAM_RUNTIME_KEYED_ROLLUP.to_owned(),
                source: CompiledStreamSource {
                    kind: STREAM_SOURCE_BOUNDED_INPUT.to_owned(),
                    name: None,
                    binding: None,
                    config: None,
                },
                key_by: Some("accountId".to_owned()),
                states: vec![],
                operators: vec![
                    CompiledStreamOperator {
                        kind: STREAM_OPERATOR_REDUCE.to_owned(),
                        operator_id: None,
                        name: Some("sum-account-totals".to_owned()),
                        inputs: vec![],
                        outputs: vec![],
                        state_ids: vec![],
                        config: Some(serde_json::json!({
                            "reducer": STREAM_REDUCER_SUM,
                            "valueField": "amount",
                        })),
                    },
                    CompiledStreamOperator {
                        kind: STREAM_OPERATOR_EMIT_CHECKPOINT.to_owned(),
                        operator_id: None,
                        name: Some("hourly-rollup-ready".to_owned()),
                        inputs: vec![],
                        outputs: vec![],
                        state_ids: vec![],
                        config: Some(serde_json::json!({
                            "sequence": 1,
                        })),
                    },
                ],
                checkpoint_policy: Some(serde_json::json!({
                    "kind": STREAM_CHECKPOINT_POLICY_NAMED,
                    "checkpoints": [
                        {
                            "name": "wrong-checkpoint",
                            "delivery": STREAM_CHECKPOINT_DELIVERY_WORKFLOW_AWAITABLE,
                            "sequence": 1,
                        }
                    ],
                })),
                views: vec![CompiledStreamView {
                    name: "accountTotals".to_owned(),
                    consistency: STREAM_CONSISTENCY_STRONG.to_owned(),
                    query_mode: STREAM_QUERY_MODE_BY_KEY.to_owned(),
                    view_id: None,
                    key_field: Some("accountId".to_owned()),
                    value_fields: vec![],
                    supported_consistencies: vec![],
                    retention_seconds: None,
                }],
                queries: vec![CompiledStreamQuery {
                    name: "accountTotals".to_owned(),
                    view_name: "accountTotals".to_owned(),
                    consistency: STREAM_CONSISTENCY_STRONG.to_owned(),
                    query_id: None,
                    arg_fields: vec![],
                }],
                classification: None,
                metadata: None,
            },
            artifact_hash: String::new(),
        };
        artifact.artifact_hash = artifact.hash();

        let error = artifact.validate().expect_err("artifact should reject mismatched checkpoint");
        assert!(error.to_string().contains("checkpoint policy must match emit_checkpoint name"));
    }

    #[test]
    fn compiled_stream_artifact_allows_persistable_kernel_v2_shape() -> Result<()> {
        let mut artifact = CompiledStreamJobArtifact {
            definition_id: "fraud-detector".to_owned(),
            definition_version: 1,
            compiler_version: "0.2.0".to_owned(),
            runtime_contract: STREAMS_KERNEL_V2_CONTRACT.to_owned(),
            source_language: "typescript".to_owned(),
            entrypoint: StreamArtifactEntrypoint {
                module: "examples/typescript-stream-jobs/fraud-detector.ts".to_owned(),
                export: "fraudDetector".to_owned(),
            },
            source_files: vec!["examples/typescript-stream-jobs/fraud-detector.ts".to_owned()],
            source_map: BTreeMap::new(),
            job: CompiledStreamJob {
                name: "fraud-detector".to_owned(),
                runtime: STREAM_RUNTIME_AGGREGATE_V2.to_owned(),
                source: CompiledStreamSource {
                    kind: STREAM_SOURCE_TOPIC.to_owned(),
                    name: Some("payments".to_owned()),
                    binding: Some("payments".to_owned()),
                    config: Some(serde_json::json!({
                        "topic": "payments"
                    })),
                },
                key_by: Some("accountId".to_owned()),
                states: vec![
                    CompiledStreamState {
                        id: "risk-aggregate".to_owned(),
                        kind: STREAM_STATE_KIND_KEYED.to_owned(),
                        key_fields: vec!["accountId".to_owned()],
                        value_fields: vec!["avgRisk".to_owned()],
                        retention_seconds: Some(3600),
                        config: Some(serde_json::json!({
                            "reducer": STREAM_REDUCER_AVG
                        })),
                    },
                    CompiledStreamState {
                        id: "minute-window".to_owned(),
                        kind: STREAM_STATE_KIND_WINDOW.to_owned(),
                        key_fields: vec!["accountId".to_owned(), "windowStart".to_owned()],
                        value_fields: vec!["avgRisk".to_owned()],
                        retention_seconds: Some(3600),
                        config: Some(serde_json::json!({
                            "mode": "tumbling",
                            "size": "1m"
                        })),
                    },
                ],
                operators: vec![
                    CompiledStreamOperator {
                        kind: STREAM_OPERATOR_FILTER.to_owned(),
                        operator_id: Some("filter-valid".to_owned()),
                        name: Some("filter-valid".to_owned()),
                        inputs: vec!["source:payments".to_owned()],
                        outputs: vec!["filtered".to_owned()],
                        state_ids: vec![],
                        config: Some(serde_json::json!({
                            "predicate": "amount > 0"
                        })),
                    },
                    CompiledStreamOperator {
                        kind: STREAM_OPERATOR_WINDOW.to_owned(),
                        operator_id: Some("minute-window".to_owned()),
                        name: Some("minute-window".to_owned()),
                        inputs: vec!["filtered".to_owned()],
                        outputs: vec!["windowed".to_owned()],
                        state_ids: vec!["minute-window".to_owned()],
                        config: Some(serde_json::json!({
                            "mode": "tumbling",
                            "size": "1m"
                        })),
                    },
                    CompiledStreamOperator {
                        kind: STREAM_OPERATOR_AGGREGATE.to_owned(),
                        operator_id: Some("avg-risk".to_owned()),
                        name: Some("avg-risk".to_owned()),
                        inputs: vec!["windowed".to_owned()],
                        outputs: vec!["risk-view".to_owned()],
                        state_ids: vec!["risk-aggregate".to_owned()],
                        config: Some(serde_json::json!({
                            "reducer": STREAM_REDUCER_AVG,
                            "valueField": "risk"
                        })),
                    },
                    CompiledStreamOperator {
                        kind: STREAM_OPERATOR_MATERIALIZE.to_owned(),
                        operator_id: Some("materialize-risk".to_owned()),
                        name: Some("materialize-risk".to_owned()),
                        inputs: vec!["risk-view".to_owned()],
                        outputs: vec!["riskScores".to_owned()],
                        state_ids: vec!["risk-aggregate".to_owned()],
                        config: Some(serde_json::json!({
                            "view": "riskScores"
                        })),
                    },
                ],
                checkpoint_policy: Some(serde_json::json!({
                    "kind": STREAM_CHECKPOINT_POLICY_NAMED,
                    "checkpoints": [
                        {
                            "name": "minute-closed",
                            "delivery": STREAM_CHECKPOINT_DELIVERY_WORKFLOW_AWAITABLE,
                            "sequence": 1
                        }
                    ],
                })),
                views: vec![CompiledStreamView {
                    name: "riskScores".to_owned(),
                    consistency: STREAM_CONSISTENCY_STRONG.to_owned(),
                    query_mode: STREAM_QUERY_MODE_BY_KEY.to_owned(),
                    view_id: Some("risk-scores".to_owned()),
                    key_field: Some("accountId".to_owned()),
                    value_fields: vec!["accountId".to_owned(), "avgRisk".to_owned()],
                    supported_consistencies: vec![
                        STREAM_CONSISTENCY_STRONG.to_owned(),
                        STREAM_CONSISTENCY_EVENTUAL.to_owned(),
                    ],
                    retention_seconds: Some(3600),
                }],
                queries: vec![
                    CompiledStreamQuery {
                        name: "riskScoresByKey".to_owned(),
                        view_name: "riskScores".to_owned(),
                        consistency: STREAM_CONSISTENCY_STRONG.to_owned(),
                        query_id: Some("risk-scores-by-key".to_owned()),
                        arg_fields: vec!["accountId".to_owned()],
                    },
                    CompiledStreamQuery {
                        name: "riskScoresScan".to_owned(),
                        view_name: "riskScores".to_owned(),
                        consistency: STREAM_CONSISTENCY_EVENTUAL.to_owned(),
                        query_id: Some("risk-scores-scan".to_owned()),
                        arg_fields: vec!["prefix".to_owned()],
                    },
                ],
                classification: Some("fast_lane".to_owned()),
                metadata: Some(serde_json::json!({
                    "kernel": "aggregate_v2"
                })),
            },
            artifact_hash: String::new(),
        };
        artifact.artifact_hash = artifact.hash();

        artifact.validate_persistable()?;
        let error = artifact
            .validate()
            .expect_err("kernel v2 artifact should not validate as runnable yet");
        assert!(error.to_string().contains(STREAMS_KERNEL_V1_CONTRACT));
        Ok(())
    }

    #[test]
    fn cancel_stream_job_command_round_trips() -> Result<()> {
        let command = ThroughputCommandEnvelope {
            command_id: Uuid::now_v7(),
            occurred_at: Utc::now(),
            dedupe_key: "stream-job-cancel:handle-a".to_owned(),
            partition_key: throughput_partition_key("job-a", 0),
            payload: ThroughputCommand::CancelStreamJob(CancelStreamJobCommand {
                tenant_id: "tenant-a".to_owned(),
                stream_instance_id: "stream-a".to_owned(),
                stream_run_id: "run-a".to_owned(),
                job_id: "job-a".to_owned(),
                handle_id: "handle-a".to_owned(),
                reason: Some("user requested cancel".to_owned()),
            }),
        };

        let encoded = serde_json::to_value(&command)?;
        let round_tripped: ThroughputCommandEnvelope = serde_json::from_value(encoded.clone())?;
        assert_eq!(round_tripped, command);
        assert!(encoded.to_string().contains("\"cancel_stream_job\""));
        Ok(())
    }

    #[test]
    fn legacy_create_batch_converts_to_native_start_command() {
        let legacy = CreateBatchCommand {
            dedupe_key: "throughput-start:test".to_owned(),
            tenant_id: "tenant-a".to_owned(),
            definition_id: "demo".to_owned(),
            definition_version: 3,
            artifact_hash: "artifact-a".to_owned(),
            instance_id: "instance-a".to_owned(),
            run_id: "run-a".to_owned(),
            batch_id: "batch-a".to_owned(),
            activity_type: "benchmark.echo".to_owned(),
            activity_capabilities: None,
            task_queue: "bulk".to_owned(),
            state: Some("join".to_owned()),
            chunk_size: 16,
            max_attempts: 2,
            retry_delay_ms: 500,
            total_items: 10,
            aggregation_group_count: 1,
            execution_policy: Some("parallel".to_owned()),
            reducer: Some("count".to_owned()),
            throughput_backend: "stream-v2".to_owned(),
            throughput_backend_version: "2.0.0".to_owned(),
            routing_reason: "bridge".to_owned(),
            admission_policy_version: ADMISSION_POLICY_VERSION.to_owned(),
            input_handle: PayloadHandle::Inline { key: "bulk-input:batch-a".to_owned() },
            result_handle: PayloadHandle::Inline { key: "bulk-result:batch-a".to_owned() },
            items: vec![Value::from(1)],
        };

        let native = legacy.to_start_throughput_run();
        assert_eq!(native.definition_version, Some(3));
        assert_eq!(native.artifact_hash.as_deref(), Some("artifact-a"));
        assert_eq!(native.total_items, 10);
        assert_eq!(
            native.bridge_request_id,
            throughput_bridge_request_id("tenant-a", "instance-a", "run-a", "batch-a")
        );
        assert_eq!(native.bridge_protocol_version, THROUGHPUT_BRIDGE_PROTOCOL_VERSION);
        assert_eq!(native.bridge_operation_kind, ThroughputBridgeOperationKind::BulkRun.as_str());
        assert_eq!(native.input_handle, legacy.input_handle);
    }

    #[test]
    fn bridge_request_and_terminal_dedupe_helpers_are_stable() {
        let bridge_request_id =
            throughput_bridge_request_id("tenant-a", "instance-a", "run-a", "batch-a");
        assert_eq!(bridge_request_id, "throughput-bridge:tenant-a:instance-a:run-a:batch-a");

        let callback_dedupe = throughput_terminal_callback_dedupe_key(
            "tenant-a",
            "instance-a",
            "run-a",
            "batch-a",
            "completed",
        );
        assert_eq!(
            callback_dedupe,
            "throughput-terminal:tenant-a:instance-a:run-a:batch-a:completed"
        );
        assert_eq!(
            throughput_terminal_callback_event_id(&callback_dedupe),
            throughput_terminal_callback_event_id(&callback_dedupe)
        );
    }

    #[test]
    fn reduction_tree_helpers_plan_three_level_layout() {
        assert_eq!(reduction_tree_level_counts(16, 3), vec![16, 4, 1]);
        let parent = reduction_tree_parent_group_id(7, 16, 3).expect("leaf parent should exist");
        assert_eq!(reduction_tree_node_level(parent), 1);
        assert_eq!(reduction_tree_child_group_ids(parent, 16, 3), vec![4, 5, 6, 7]);
        let root =
            reduction_tree_parent_group_id(parent, 16, 3).expect("intermediate root should exist");
        assert_eq!(reduction_tree_node_level(root), 2);
        assert_eq!(reduction_tree_child_group_ids(root, 16, 3).len(), 4);
        assert!(reduction_tree_parent_group_id(root, 16, 3).is_none());
    }

    #[test]
    fn reducer_support_flags_distinguish_streaming_capability_from_output_requirements() {
        assert!(bulk_reducer_is_mergeable(Some(BULK_REDUCER_SUM)));
        assert!(bulk_reducer_requires_success_outputs(Some(BULK_REDUCER_SUM)));
        assert!(bulk_reducer_supports_stream_v2(Some(BULK_REDUCER_SUM)));
        assert!(bulk_reducer_supports_stream_v2(Some(BULK_REDUCER_COUNT)));
        assert!(bulk_reducer_requires_success_outputs(Some(BULK_REDUCER_HISTOGRAM)));
        assert!(!bulk_reducer_requires_success_outputs(Some(BULK_REDUCER_SAMPLE_ERRORS)));
        assert!(bulk_reducer_requires_error_outputs(Some(BULK_REDUCER_SAMPLE_ERRORS)));
        assert!(bulk_reducer_supports_stream_v2(Some(BULK_REDUCER_HISTOGRAM)));
        assert!(bulk_reducer_supports_stream_v2(Some(BULK_REDUCER_SAMPLE_ERRORS)));
        assert!(bulk_reducer_settles(Some(BULK_REDUCER_SAMPLE_ERRORS)));
    }

    #[test]
    fn core_activities_are_fast_lane_eligible_when_outputs_can_be_omitted() {
        let items = vec![serde_json::json!({"value": 1}), serde_json::json!({"value": 2})];

        assert!(activity_can_short_circuit_omitted_success_output(CORE_ECHO_ACTIVITY, None,));
        assert!(activity_can_short_circuit_omitted_success_output(CORE_NOOP_ACTIVITY, None,));
        assert!(activity_can_short_circuit_omitted_success_output(CORE_ACCEPT_ACTIVITY, None,));
        assert!(can_use_payloadless_bulk_transport(
            CORE_ECHO_ACTIVITY,
            None,
            Some(BULK_REDUCER_COUNT),
            1,
            &items,
        ));
        assert!(can_inline_durable_tiny_fanout(
            CORE_ACCEPT_ACTIVITY,
            None,
            Some(BULK_REDUCER_ALL_SETTLED),
            1,
            &items,
        ));
        assert_eq!(
            tiny_workflow_routing_decision(
                CORE_NOOP_ACTIVITY,
                None,
                Some(BULK_REDUCER_COUNT),
                None,
                1,
                &items,
                items.len(),
                1,
            )
            .expect("core.noop should route through tiny workflow fast lane")
            .admission_mode,
            WorkflowAdmissionMode::InlineFastStart
        );
    }

    #[test]
    fn fast_count_remains_bulk_only_for_single_activity_short_circuiting() {
        assert!(!activity_can_short_circuit_omitted_success_output(
            BENCHMARK_FAST_COUNT_ACTIVITY,
            None,
        ));
        assert!(can_complete_payloadless_bulk_chunk(
            BENCHMARK_FAST_COUNT_ACTIVITY,
            None,
            true,
            128,
            true,
            false,
            false,
        ));
    }

    #[test]
    fn declared_capabilities_enable_arbitrary_activity_fast_paths() {
        let items = vec![serde_json::json!({"value": 1}), serde_json::json!({"value": 2})];
        let capabilities = ActivityExecutionCapabilities {
            payloadless_transport: true,
            tiny_inline_completion: false,
            omit_success_output_short_circuit: false,
        };

        assert!(can_use_payloadless_bulk_transport(
            "custom.echo",
            Some(&capabilities),
            Some(BULK_REDUCER_COUNT),
            1,
            &items,
        ));
        assert!(can_inline_durable_tiny_fanout(
            "custom.echo",
            Some(&capabilities),
            Some(BULK_REDUCER_ALL_SETTLED),
            1,
            &items,
        ));
        assert_eq!(
            tiny_workflow_routing_decision(
                "custom.echo",
                Some(&capabilities),
                Some(BULK_REDUCER_COUNT),
                None,
                1,
                &items,
                items.len(),
                1,
            )
            .expect("declared custom activity should route through tiny workflow fast lane")
            .admission_mode,
            WorkflowAdmissionMode::InlineFastStart
        );
    }

    #[test]
    fn numeric_reducers_reduce_success_outputs() -> Result<()> {
        let values = vec![Value::from(1), Value::from(4), Value::from(7)];

        assert_eq!(
            bulk_reducer_reduce_values(Some(BULK_REDUCER_SUM), &values)?,
            Some(Value::from(12.0))
        );
        assert_eq!(
            bulk_reducer_reduce_values(Some(BULK_REDUCER_MIN), &values)?,
            Some(Value::from(1.0))
        );
        assert_eq!(
            bulk_reducer_reduce_values(Some(BULK_REDUCER_MAX), &values)?,
            Some(Value::from(7.0))
        );
        assert_eq!(
            bulk_reducer_reduce_values(Some(BULK_REDUCER_AVG), &values)?,
            Some(Value::from(4.0))
        );
        assert_eq!(bulk_reducer_reduce_values(Some(BULK_REDUCER_MIN), &[])?, Some(Value::Null));
        assert_eq!(
            bulk_reducer_reduce_values(Some(BULK_REDUCER_SUM), &[])?,
            Some(Value::from(0.0))
        );
        assert_eq!(
            bulk_reducer_reduce_values(
                Some(BULK_REDUCER_HISTOGRAM),
                &[Value::from("a"), Value::from("b"), Value::from("a")]
            )?,
            Some(serde_json::json!({"a": 2, "b": 1}))
        );
        assert_eq!(
            bulk_reducer_reduce_errors(
                Some(BULK_REDUCER_SAMPLE_ERRORS),
                &[("boom".to_owned(), 3), ("cancelled".to_owned(), 1)]
            )?,
            Some(serde_json::json!({
                "sample": ["boom", "cancelled"],
                "total": 4,
                "truncated": false,
            }))
        );
        Ok(())
    }
}
