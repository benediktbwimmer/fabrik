use std::collections::{BTreeMap, BTreeSet};

use anyhow::Context;
use chrono::{DateTime, Utc};
use fabrik_events::{EventEnvelope, WorkflowEvent};
use fabrik_throughput::{
    BULK_REDUCER_AVG, BULK_REDUCER_MAX, BULK_REDUCER_MIN, BULK_REDUCER_SUM,
    DEFAULT_AGGREGATION_GROUP_COUNT, PayloadHandle, ThroughputBackend, bulk_reducer_reduce_values,
    bulk_reducer_requires_success_outputs, bulk_reducer_summary_field_name,
};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Number, Value};
use sha2::{Digest, Sha256};
use thiserror::Error;

use crate::{ExecutionEmission, RetryPolicy, StepConfig, parse_timer_ref};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CompiledWorkflowArtifact {
    pub definition_id: String,
    pub definition_version: u32,
    pub compiler_version: String,
    pub source_language: String,
    pub entrypoint: ArtifactEntrypoint,
    #[serde(default)]
    pub source_files: Vec<String>,
    #[serde(default)]
    pub source_map: BTreeMap<String, SourceLocation>,
    #[serde(default)]
    pub helpers: BTreeMap<String, HelperFunction>,
    #[serde(default)]
    pub queries: BTreeMap<String, CompiledQueryHandler>,
    #[serde(default)]
    pub signals: BTreeMap<String, CompiledSignalHandler>,
    #[serde(default)]
    pub updates: BTreeMap<String, CompiledUpdateHandler>,
    pub workflow: CompiledWorkflow,
    pub artifact_hash: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ArtifactEntrypoint {
    pub module: String,
    pub export: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SourceLocation {
    pub file: String,
    pub line: u32,
    pub column: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CompiledWorkflow {
    pub initial_state: String,
    pub states: BTreeMap<String, CompiledStateNode>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub params: Vec<CompiledWorkflowParam>,
    #[serde(default, skip_serializing_if = "BTreeSet::is_empty")]
    pub non_cancellable_states: BTreeSet<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CompiledWorkflowParam {
    pub name: String,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub rest: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default: Option<Expression>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CompiledQueryHandler {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub arg_name: Option<String>,
    pub expr: Expression,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CompiledSignalHandler {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub arg_name: Option<String>,
    pub initial_state: String,
    pub states: BTreeMap<String, CompiledStateNode>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CompiledUpdateHandler {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub arg_name: Option<String>,
    pub initial_state: String,
    pub states: BTreeMap<String, CompiledStateNode>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum CompiledStateNode {
    Assign {
        actions: Vec<Assignment>,
        next: String,
    },
    Choice {
        condition: Expression,
        then_next: String,
        else_next: String,
    },
    Step {
        handler: String,
        input: Expression,
        #[serde(skip_serializing_if = "Option::is_none")]
        next: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        task_queue: Option<Expression>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        retry: Option<RetryPolicy>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        config: Option<StepConfig>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        schedule_to_start_timeout_ms: Option<u64>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        start_to_close_timeout_ms: Option<u64>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        heartbeat_timeout_ms: Option<u64>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        output_var: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        on_error: Option<ErrorTransition>,
    },
    FanOut {
        activity_type: String,
        items: Expression,
        next: String,
        handle_var: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        task_queue: Option<Expression>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        reducer: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        retry: Option<RetryPolicy>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        config: Option<StepConfig>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        schedule_to_start_timeout_ms: Option<u64>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        start_to_close_timeout_ms: Option<u64>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        heartbeat_timeout_ms: Option<u64>,
    },
    StartBulkActivity {
        activity_type: String,
        items: Expression,
        next: String,
        handle_var: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        task_queue: Option<Expression>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        execution_policy: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        reducer: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        throughput_backend: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        chunk_size: Option<u32>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        retry: Option<RetryPolicy>,
    },
    WaitForBulkActivity {
        bulk_ref_var: String,
        next: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        output_var: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        on_error: Option<ErrorTransition>,
    },
    WaitForAllActivities {
        fanout_ref_var: String,
        next: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        output_var: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        on_error: Option<ErrorTransition>,
    },
    StartChild {
        child_definition_id: String,
        input: Expression,
        next: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        handle_var: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        workflow_id: Option<Expression>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        task_queue: Option<Expression>,
        parent_close_policy: ParentClosePolicy,
    },
    SignalChild {
        child_ref_var: String,
        signal_name: String,
        payload: Expression,
        next: String,
    },
    CancelChild {
        child_ref_var: String,
        reason: Option<Expression>,
        next: String,
    },
    SignalExternal {
        target_instance_id: Expression,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        target_run_id: Option<Expression>,
        signal_name: String,
        payload: Expression,
        next: String,
    },
    CancelExternal {
        target_instance_id: Expression,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        target_run_id: Option<Expression>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        reason: Option<Expression>,
        next: String,
    },
    WaitForChild {
        child_ref_var: String,
        next: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        output_var: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        on_error: Option<ErrorTransition>,
    },
    WaitForEvent {
        event_type: String,
        next: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        output_var: Option<String>,
    },
    WaitForCondition {
        condition: Expression,
        next: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        timeout_ref: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        timeout_next: Option<String>,
    },
    WaitForTimer {
        #[serde(default)]
        timer_ref: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        timer_expr: Option<Expression>,
        next: String,
    },
    Succeed {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        output: Option<Expression>,
    },
    Fail {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        reason: Option<Expression>,
    },
    ContinueAsNew {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        input: Option<Expression>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ErrorTransition {
    pub next: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error_var: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Assignment {
    pub target: String,
    pub expr: Expression,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Expression {
    Literal {
        value: Value,
    },
    Identifier {
        name: String,
    },
    Member {
        object: Box<Expression>,
        property: String,
    },
    Index {
        object: Box<Expression>,
        index: Box<Expression>,
    },
    Binary {
        op: BinaryOp,
        left: Box<Expression>,
        right: Box<Expression>,
    },
    Unary {
        op: UnaryOp,
        expr: Box<Expression>,
    },
    Logical {
        op: LogicalOp,
        left: Box<Expression>,
        right: Box<Expression>,
    },
    Conditional {
        condition: Box<Expression>,
        then_expr: Box<Expression>,
        else_expr: Box<Expression>,
    },
    Array {
        items: Vec<Expression>,
    },
    ArrayFind {
        array: Box<Expression>,
        item_name: String,
        predicate: Box<Expression>,
    },
    ArrayMap {
        array: Box<Expression>,
        item_name: String,
        expr: Box<Expression>,
    },
    ArrayReduce {
        array: Box<Expression>,
        accumulator_name: String,
        item_name: String,
        initial: Box<Expression>,
        expr: Box<Expression>,
    },
    Object {
        fields: BTreeMap<String, Expression>,
    },
    ObjectMerge {
        left: Box<Expression>,
        right: Box<Expression>,
    },
    Call {
        callee: String,
        args: Vec<Expression>,
    },
    WorkflowInfo,
    SideEffect {
        marker_id: String,
        expr: Box<Expression>,
    },
    Version {
        change_id: String,
        min_supported: u32,
        max_supported: u32,
    },
    Now,
    Uuid {
        scope: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum BinaryOp {
    Add,
    Subtract,
    Multiply,
    Divide,
    Remainder,
    In,
    Equal,
    NotEqual,
    LessThan,
    LessThanOrEqual,
    GreaterThan,
    GreaterThanOrEqual,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum UnaryOp {
    Not,
    Negate,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum LogicalOp {
    And,
    Or,
    Coalesce,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct HelperFunction {
    pub params: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub statements: Vec<HelperStatement>,
    pub body: Expression,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum HelperStatement {
    Assign { target: String, expr: Expression },
    AssignIndex { target: String, index: Expression, expr: Expression },
    ForRange { index_var: String, start: Expression, end: Expression, body: Vec<HelperStatement> },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
pub struct ArtifactExecutionState {
    #[serde(default)]
    pub bindings: BTreeMap<String, Value>,
    #[serde(default)]
    pub workflow_info: Option<Value>,
    #[serde(default)]
    pub markers: BTreeMap<String, Value>,
    #[serde(default)]
    pub version_markers: BTreeMap<String, u32>,
    #[serde(default)]
    pub active_signal: Option<ActiveSignalState>,
    #[serde(default)]
    pub active_update: Option<ActiveUpdateState>,
    #[serde(default)]
    pub pending_workflow_cancellation: Option<String>,
    #[serde(default)]
    pub condition_timers: BTreeSet<String>,
    #[serde(skip)]
    pub turn_context: Option<ExecutionTurnContext>,
    #[serde(skip)]
    pub pending_markers: Vec<(String, Value)>,
    #[serde(skip)]
    pub pending_version_markers: Vec<(String, u32)>,
}

impl ArtifactExecutionState {
    pub(crate) fn compact_for_persistence(
        &mut self,
        persisted_input: Option<&Value>,
        keep_input_binding: bool,
    ) {
        if keep_input_binding {
            return;
        }
        if self
            .bindings
            .get("input")
            .zip(persisted_input)
            .is_some_and(|(bound, input)| bound == input)
        {
            self.bindings.remove("input");
        }
    }

    pub(crate) fn expand_after_persistence(&mut self, persisted_input: Option<&Value>) {
        if !self.bindings.contains_key("input") {
            if let Some(input) = persisted_input {
                self.bindings.insert("input".to_owned(), input.clone());
            }
        }
    }

    pub(crate) fn waits_on_bulk_activity(&self, wait_state: &str) -> bool {
        self.bulk_wait_binding(wait_state).is_some()
    }

    fn bulk_wait_binding(&self, wait_state: &str) -> Option<BulkActivityExecutionState> {
        self.bindings.values().find_map(|value| {
            let bulk = decode_bulk_state(value)?;
            (bulk.wait_state == wait_state).then_some(bulk)
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ActiveUpdateState {
    pub update_id: String,
    pub update_name: String,
    pub return_state: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ActiveSignalState {
    pub signal_id: String,
    pub signal_name: String,
    pub return_state: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct FanOutExecutionState {
    pub origin_state: String,
    pub wait_state: String,
    pub task_queue: String,
    #[serde(default)]
    pub total_count: usize,
    #[serde(default)]
    pub pending_count: usize,
    #[serde(default)]
    pub succeeded_count: usize,
    #[serde(default)]
    pub failed_count: usize,
    #[serde(default)]
    pub cancelled_count: usize,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub inputs: Vec<Value>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub pending_activity_ids: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub results: Vec<Option<Value>>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub failed_activity_errors: BTreeMap<usize, String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub first_failed_index: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub first_failed_error: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reducer: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub settlement_bitmap: Vec<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct BulkActivityExecutionState {
    pub batch_id: String,
    pub origin_state: String,
    pub wait_state: String,
    pub activity_type: String,
    pub task_queue: String,
    pub execution_policy: Option<String>,
    pub reducer: Option<String>,
    pub total_items: u32,
    pub chunk_size: u32,
    pub max_attempts: u32,
    pub retry_delay_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ParentClosePolicy {
    Terminate,
    RequestCancel,
    Abandon,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecutionTurnContext {
    pub event_id: uuid::Uuid,
    pub occurred_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CompiledExecutionPlan {
    pub workflow_version: u32,
    pub final_state: String,
    pub emissions: Vec<ExecutionEmission>,
    pub execution_state: ArtifactExecutionState,
    pub context: Option<Value>,
    pub output: Option<Value>,
}

impl CompiledWorkflowArtifact {
    pub fn synthetic_turn_context(label: &str) -> ExecutionTurnContext {
        ExecutionTurnContext {
            event_id: uuid::Uuid::new_v5(&uuid::Uuid::NAMESPACE_URL, label.as_bytes()),
            occurred_at: DateTime::from_timestamp_millis(0).expect("unix epoch is valid"),
        }
    }

    pub fn new(
        definition_id: impl Into<String>,
        definition_version: u32,
        compiler_version: impl Into<String>,
        entrypoint: ArtifactEntrypoint,
        workflow: CompiledWorkflow,
    ) -> Self {
        let mut artifact = Self {
            definition_id: definition_id.into(),
            definition_version,
            compiler_version: compiler_version.into(),
            source_language: "typescript".to_owned(),
            entrypoint,
            source_files: Vec::new(),
            source_map: BTreeMap::new(),
            helpers: BTreeMap::new(),
            queries: BTreeMap::new(),
            signals: BTreeMap::new(),
            updates: BTreeMap::new(),
            workflow,
            artifact_hash: String::new(),
        };
        artifact.artifact_hash = artifact.hash();
        artifact
    }

    pub fn is_wait_state(&self, state_id: &str) -> Result<bool, CompiledWorkflowError> {
        let state = self
            .workflow
            .states
            .get(state_id)
            .ok_or_else(|| CompiledWorkflowError::UnknownState(state_id.to_owned()))?;
        Ok(matches!(
            state,
            CompiledStateNode::WaitForEvent { .. }
                | CompiledStateNode::WaitForCondition { .. }
                | CompiledStateNode::WaitForTimer { .. }
                | CompiledStateNode::WaitForAllActivities { .. }
        ))
    }

    pub fn is_non_cancellable_state(&self, state_id: &str) -> bool {
        self.workflow.non_cancellable_states.contains(state_id)
    }

    pub fn pending_workflow_cancellation_ready<'a>(
        &self,
        current_state: &str,
        execution_state: &'a ArtifactExecutionState,
    ) -> Option<&'a str> {
        execution_state
            .pending_workflow_cancellation
            .as_deref()
            .filter(|_| !self.is_non_cancellable_state(current_state))
    }

    pub fn expected_signal_type(
        &self,
        state_id: &str,
    ) -> Result<Option<&str>, CompiledWorkflowError> {
        let state = self
            .state_by_id(state_id)
            .ok_or_else(|| CompiledWorkflowError::UnknownState(state_id.to_owned()))?;
        Ok(match state {
            CompiledStateNode::WaitForEvent { event_type, .. } => Some(event_type.as_str()),
            _ => None,
        })
    }

    pub fn has_query(&self, query_name: &str) -> bool {
        self.queries.contains_key(query_name)
    }

    pub fn has_signal_handler(&self, signal_name: &str) -> bool {
        self.signals.contains_key(signal_name)
    }

    pub fn has_update(&self, update_name: &str) -> bool {
        self.updates.contains_key(update_name)
    }

    pub fn evaluate_query(
        &self,
        query_name: &str,
        args: &Value,
        mut execution_state: ArtifactExecutionState,
    ) -> Result<Value, CompiledWorkflowError> {
        let handler = self
            .queries
            .get(query_name)
            .ok_or_else(|| CompiledWorkflowError::UnknownQuery(query_name.to_owned()))?;
        if let Some(arg_name) = &handler.arg_name {
            execution_state.bindings.insert(arg_name.clone(), args.clone());
        } else {
            execution_state.bindings.insert("args".to_owned(), args.clone());
        }
        evaluate_expression(&handler.expr, &mut execution_state, &self.helpers)
    }

    pub fn hash(&self) -> String {
        let mut clone = self.clone();
        clone.artifact_hash.clear();
        let value = serde_json::to_value(&clone).expect("compiled artifact serialization failed");
        let encoded = serde_json::to_vec(&canonicalize_value(value))
            .expect("compiled artifact serialization failed");
        let digest = Sha256::digest(encoded);
        format!("{digest:x}")
    }

    fn state_by_id(&self, state_id: &str) -> Option<&CompiledStateNode> {
        self.workflow
            .states
            .get(state_id)
            .or_else(|| self.signals.values().find_map(|handler| handler.states.get(state_id)))
            .or_else(|| self.updates.values().find_map(|handler| handler.states.get(state_id)))
    }

    fn states_for<'a>(
        &'a self,
        state_id: &str,
        execution_state: &ArtifactExecutionState,
    ) -> Option<&'a BTreeMap<String, CompiledStateNode>> {
        if let Some(active_signal) = &execution_state.active_signal {
            if let Some(handler) = self.signals.get(&active_signal.signal_name) {
                if handler.states.contains_key(state_id) {
                    return Some(&handler.states);
                }
            }
        }
        if let Some(active_update) = &execution_state.active_update {
            if let Some(handler) = self.updates.get(&active_update.update_name) {
                if handler.states.contains_key(state_id) {
                    return Some(&handler.states);
                }
            }
        }
        if self.workflow.states.contains_key(state_id) {
            return Some(&self.workflow.states);
        }
        self.updates
            .values()
            .find(|handler| handler.states.contains_key(state_id))
            .map(|handler| &handler.states)
    }

    pub fn validate(&self) -> Result<(), CompiledWorkflowError> {
        if self.workflow.initial_state.is_empty() {
            return Err(CompiledWorkflowError::MissingInitialState);
        }
        if !self.workflow.states.contains_key(&self.workflow.initial_state) {
            return Err(CompiledWorkflowError::UnknownState(self.workflow.initial_state.clone()));
        }
        for (name, state) in &self.workflow.states {
            for next in state.next_states() {
                if !self.workflow.states.contains_key(next) {
                    return Err(CompiledWorkflowError::UnknownTransition {
                        state: name.clone(),
                        next: next.to_owned(),
                    });
                }
            }
        }
        for (name, handler) in &self.updates {
            if !handler.states.contains_key(&handler.initial_state) {
                return Err(CompiledWorkflowError::UnknownUpdateInitialState(name.clone()));
            }
            for (state_name, state) in &handler.states {
                for next in state.next_states() {
                    if !handler.states.contains_key(next) {
                        return Err(CompiledWorkflowError::UnknownUpdateTransition {
                            update: name.clone(),
                            state: state_name.clone(),
                            next: next.to_owned(),
                        });
                    }
                }
            }
        }
        for (name, handler) in &self.signals {
            if !handler.states.contains_key(&handler.initial_state) {
                return Err(CompiledWorkflowError::UnknownSignalInitialState(name.clone()));
            }
            for (state_name, state) in &handler.states {
                for next in state.next_states() {
                    if !handler.states.contains_key(next) {
                        return Err(CompiledWorkflowError::UnknownSignalTransition {
                            signal: name.clone(),
                            state: state_name.clone(),
                            next: next.to_owned(),
                        });
                    }
                }
            }
        }
        if self.hash() != self.artifact_hash {
            return Err(CompiledWorkflowError::ArtifactHashMismatch);
        }
        Ok(())
    }

    pub fn execute_trigger(
        &self,
        input: &Value,
    ) -> Result<CompiledExecutionPlan, CompiledWorkflowError> {
        self.execute_trigger_with_turn(input, Self::synthetic_turn_context("trigger"))
    }

    pub fn execute_trigger_with_turn(
        &self,
        input: &Value,
        turn_context: ExecutionTurnContext,
    ) -> Result<CompiledExecutionPlan, CompiledWorkflowError> {
        let mut execution_state = ArtifactExecutionState::default();
        self.bind_workflow_input(input, &mut execution_state, turn_context)?;
        self.execute_from_state(&self.workflow.initial_state, execution_state, true)
    }

    pub fn execute_trigger_with_state_and_turn(
        &self,
        input: &Value,
        mut execution_state: ArtifactExecutionState,
        turn_context: ExecutionTurnContext,
    ) -> Result<CompiledExecutionPlan, CompiledWorkflowError> {
        self.bind_workflow_input(input, &mut execution_state, turn_context)?;
        self.execute_from_state(&self.workflow.initial_state, execution_state, true)
    }

    fn bind_workflow_input(
        &self,
        input: &Value,
        execution_state: &mut ArtifactExecutionState,
        turn_context: ExecutionTurnContext,
    ) -> Result<(), CompiledWorkflowError> {
        execution_state.bindings.insert("input".to_owned(), input.clone());
        execution_state.turn_context = Some(turn_context);

        let args = match input {
            Value::Array(items) => items.clone(),
            value => vec![value.clone()],
        };
        for (index, param) in self.workflow.params.iter().enumerate() {
            let value = if param.rest {
                Value::Array(args.iter().skip(index).cloned().collect())
            } else {
                match args.get(index) {
                    Some(value) => value.clone(),
                    None => match &param.default {
                        Some(default) => {
                            evaluate_expression(default, execution_state, &self.helpers)?
                        }
                        None => Value::Null,
                    },
                }
            };
            execution_state.bindings.insert(param.name.clone(), value);
        }
        Ok(())
    }

    pub fn execute_after_signal(
        &self,
        wait_state: &str,
        signal_type: &str,
        payload: &Value,
        execution_state: ArtifactExecutionState,
    ) -> Result<CompiledExecutionPlan, CompiledWorkflowError> {
        self.execute_after_signal_with_turn(
            wait_state,
            signal_type,
            payload,
            execution_state,
            Self::synthetic_turn_context("signal"),
        )
    }

    pub fn execute_after_signal_with_turn(
        &self,
        wait_state: &str,
        signal_type: &str,
        payload: &Value,
        mut execution_state: ArtifactExecutionState,
        turn_context: ExecutionTurnContext,
    ) -> Result<CompiledExecutionPlan, CompiledWorkflowError> {
        execution_state.turn_context = Some(turn_context);
        let states = self
            .states_for(wait_state, &execution_state)
            .ok_or_else(|| CompiledWorkflowError::UnknownState(wait_state.to_owned()))?;
        let state = states
            .get(wait_state)
            .ok_or_else(|| CompiledWorkflowError::UnknownState(wait_state.to_owned()))?;
        match state {
            CompiledStateNode::WaitForEvent { event_type, next, output_var }
                if event_type == signal_type =>
            {
                if let Some(output_var) = output_var {
                    execution_state.bindings.insert(output_var.clone(), payload.clone());
                }
                self.execute_from_state_in_graph(states, next, execution_state, false)
            }
            CompiledStateNode::WaitForCondition { .. } => {
                Err(CompiledWorkflowError::NotWaitingOnSignal(wait_state.to_owned()))
            }
            CompiledStateNode::WaitForEvent { event_type, .. } => {
                Err(CompiledWorkflowError::UnexpectedSignal {
                    expected: event_type.clone(),
                    received: signal_type.to_owned(),
                })
            }
            _ => Err(CompiledWorkflowError::NotWaitingOnSignal(wait_state.to_owned())),
        }
    }

    pub fn execute_signal_handler_with_turn(
        &self,
        current_state: &str,
        signal_id: &str,
        signal_name: &str,
        payload: &Value,
        mut execution_state: ArtifactExecutionState,
        turn_context: ExecutionTurnContext,
    ) -> Result<CompiledExecutionPlan, CompiledWorkflowError> {
        if execution_state.active_signal.is_some() {
            return Err(CompiledWorkflowError::SignalAlreadyActive(signal_name.to_owned()));
        }
        if execution_state.active_update.is_some() {
            return Err(CompiledWorkflowError::UpdateAlreadyActive(signal_name.to_owned()));
        }
        let handler = self
            .signals
            .get(signal_name)
            .ok_or_else(|| CompiledWorkflowError::UnknownSignalHandler(signal_name.to_owned()))?;
        execution_state.turn_context = Some(turn_context);
        execution_state.active_signal = Some(ActiveSignalState {
            signal_id: signal_id.to_owned(),
            signal_name: signal_name.to_owned(),
            return_state: current_state.to_owned(),
        });
        if let Some(arg_name) = &handler.arg_name {
            execution_state.bindings.insert(arg_name.clone(), payload.clone());
        } else {
            execution_state.bindings.insert("args".to_owned(), payload.clone());
        }
        self.execute_from_state_in_graph(
            &handler.states,
            &handler.initial_state,
            execution_state,
            false,
        )
    }

    pub fn execute_update_with_turn(
        &self,
        current_state: &str,
        update_id: &str,
        update_name: &str,
        payload: &Value,
        mut execution_state: ArtifactExecutionState,
        turn_context: ExecutionTurnContext,
    ) -> Result<CompiledExecutionPlan, CompiledWorkflowError> {
        let handler = self
            .updates
            .get(update_name)
            .ok_or_else(|| CompiledWorkflowError::UnknownUpdate(update_name.to_owned()))?;
        if execution_state.active_update.is_some() {
            return Err(CompiledWorkflowError::UpdateAlreadyActive(update_name.to_owned()));
        }
        execution_state.turn_context = Some(turn_context);
        execution_state.active_update = Some(ActiveUpdateState {
            update_id: update_id.to_owned(),
            update_name: update_name.to_owned(),
            return_state: current_state.to_owned(),
        });
        if let Some(arg_name) = &handler.arg_name {
            execution_state.bindings.insert(arg_name.clone(), payload.clone());
        } else {
            execution_state.bindings.insert("args".to_owned(), payload.clone());
        }
        let plan = self.execute_from_state_in_graph(
            &handler.states,
            &handler.initial_state,
            execution_state,
            false,
        )?;
        Ok(plan)
    }

    pub fn execute_after_timer(
        &self,
        wait_state: &str,
        timer_id: &str,
        execution_state: ArtifactExecutionState,
    ) -> Result<CompiledExecutionPlan, CompiledWorkflowError> {
        self.execute_after_timer_with_turn(
            wait_state,
            timer_id,
            execution_state,
            Self::synthetic_turn_context("timer"),
        )
    }

    pub fn execute_after_timer_with_turn(
        &self,
        wait_state: &str,
        timer_id: &str,
        mut execution_state: ArtifactExecutionState,
        turn_context: ExecutionTurnContext,
    ) -> Result<CompiledExecutionPlan, CompiledWorkflowError> {
        execution_state.turn_context = Some(turn_context);
        let states = self
            .states_for(wait_state, &execution_state)
            .ok_or_else(|| CompiledWorkflowError::UnknownState(wait_state.to_owned()))?;
        let state = states
            .get(wait_state)
            .ok_or_else(|| CompiledWorkflowError::UnknownState(wait_state.to_owned()))?;
        match state {
            CompiledStateNode::WaitForTimer { next, .. } if wait_state == timer_id => {
                self.execute_from_state_in_graph(states, next, execution_state, false)
            }
            CompiledStateNode::WaitForCondition { timeout_next, .. } if wait_state == timer_id => {
                execution_state.condition_timers.remove(wait_state);
                let Some(next) = timeout_next else {
                    return Err(CompiledWorkflowError::UnexpectedTimer {
                        expected: wait_state.to_owned(),
                        received: timer_id.to_owned(),
                    });
                };
                self.execute_from_state_in_graph(states, next, execution_state, false)
            }
            CompiledStateNode::WaitForTimer { .. } => Err(CompiledWorkflowError::UnexpectedTimer {
                expected: wait_state.to_owned(),
                received: timer_id.to_owned(),
            }),
            _ => Err(CompiledWorkflowError::NotWaitingOnTimer(wait_state.to_owned())),
        }
    }

    pub fn execute_after_child_completion_with_turn(
        &self,
        wait_state: &str,
        child_id: &str,
        output: &Value,
        mut execution_state: ArtifactExecutionState,
        turn_context: ExecutionTurnContext,
    ) -> Result<CompiledExecutionPlan, CompiledWorkflowError> {
        execution_state.turn_context = Some(turn_context);
        let states = self
            .states_for(wait_state, &execution_state)
            .ok_or_else(|| CompiledWorkflowError::UnknownState(wait_state.to_owned()))?;
        let state = states
            .get(wait_state)
            .ok_or_else(|| CompiledWorkflowError::UnknownState(wait_state.to_owned()))?;
        match state {
            CompiledStateNode::WaitForChild { child_ref_var, next, output_var, .. } => {
                let bound_child_id = execution_state
                    .bindings
                    .get(child_ref_var)
                    .and_then(extract_child_id)
                    .ok_or_else(|| {
                        CompiledWorkflowError::UnknownChildReference(child_ref_var.clone())
                    })?;
                if bound_child_id != child_id {
                    return Err(CompiledWorkflowError::UnexpectedChild {
                        expected: bound_child_id,
                        received: child_id.to_owned(),
                    });
                }
                if let Some(output_var) = output_var {
                    execution_state.bindings.insert(output_var.clone(), output.clone());
                }
                self.execute_from_state_in_graph(states, next, execution_state, false)
            }
            _ => Err(CompiledWorkflowError::NotWaitingOnChild(wait_state.to_owned())),
        }
    }

    pub fn execute_after_child_failure_with_turn(
        &self,
        wait_state: &str,
        child_id: &str,
        error: &str,
        mut execution_state: ArtifactExecutionState,
        turn_context: ExecutionTurnContext,
    ) -> Result<CompiledExecutionPlan, CompiledWorkflowError> {
        execution_state.turn_context = Some(turn_context);
        let states = self
            .states_for(wait_state, &execution_state)
            .ok_or_else(|| CompiledWorkflowError::UnknownState(wait_state.to_owned()))?;
        let state = states
            .get(wait_state)
            .ok_or_else(|| CompiledWorkflowError::UnknownState(wait_state.to_owned()))?;
        match state {
            CompiledStateNode::WaitForChild { child_ref_var, on_error, .. } => {
                let bound_child_id = execution_state
                    .bindings
                    .get(child_ref_var)
                    .and_then(extract_child_id)
                    .ok_or_else(|| {
                        CompiledWorkflowError::UnknownChildReference(child_ref_var.clone())
                    })?;
                if bound_child_id != child_id {
                    return Err(CompiledWorkflowError::UnexpectedChild {
                        expected: bound_child_id,
                        received: child_id.to_owned(),
                    });
                }
                if let Some(on_error) = on_error {
                    if let Some(error_var) = &on_error.error_var {
                        execution_state
                            .bindings
                            .insert(error_var.clone(), Value::String(error.to_owned()));
                    }
                    return self.execute_from_state_in_graph(
                        states,
                        &on_error.next,
                        execution_state,
                        false,
                    );
                }
                if let Some(active_update) = &execution_state.active_update {
                    let return_state = active_update.return_state.clone();
                    let update_id = active_update.update_id.clone();
                    let update_name = active_update.update_name.clone();
                    execution_state.active_update = None;
                    return Ok(CompiledExecutionPlan {
                        workflow_version: self.definition_version,
                        final_state: return_state.clone(),
                        emissions: vec![ExecutionEmission {
                            event: WorkflowEvent::WorkflowUpdateRejected {
                                update_id,
                                update_name,
                                error: error.to_owned(),
                            },
                            state: Some(return_state.clone()),
                        }],
                        execution_state,
                        context: Some(Value::String(error.to_owned())),
                        output: Some(Value::String(error.to_owned())),
                    });
                }
                execution_state.active_signal = None;
                Ok(CompiledExecutionPlan {
                    workflow_version: self.definition_version,
                    final_state: wait_state.to_owned(),
                    emissions: vec![ExecutionEmission {
                        event: WorkflowEvent::WorkflowFailed { reason: error.to_owned() },
                        state: Some(wait_state.to_owned()),
                    }],
                    execution_state,
                    context: Some(Value::String(error.to_owned())),
                    output: Some(Value::String(error.to_owned())),
                })
            }
            _ => Err(CompiledWorkflowError::NotWaitingOnChild(wait_state.to_owned())),
        }
    }

    pub fn execute_after_bulk_completion_with_turn(
        &self,
        wait_state: &str,
        batch_id: &str,
        summary: &Value,
        mut execution_state: ArtifactExecutionState,
        turn_context: ExecutionTurnContext,
    ) -> Result<CompiledExecutionPlan, CompiledWorkflowError> {
        execution_state.turn_context = Some(turn_context);
        let states = self
            .states_for(wait_state, &execution_state)
            .ok_or_else(|| CompiledWorkflowError::UnknownState(wait_state.to_owned()))?;
        let state = states
            .get(wait_state)
            .ok_or_else(|| CompiledWorkflowError::UnknownState(wait_state.to_owned()))?;
        match state {
            CompiledStateNode::WaitForBulkActivity { bulk_ref_var, next, output_var, .. } => {
                let bulk = self.bulk_binding(wait_state, bulk_ref_var, &execution_state)?;
                if bulk.batch_id != batch_id {
                    return Err(CompiledWorkflowError::UnexpectedBulkBatch {
                        expected: bulk.batch_id,
                        received: batch_id.to_owned(),
                    });
                }
                let reduced = reduce_bulk_terminal_payload(summary, &bulk);
                execution_state.bindings.remove(bulk_ref_var);
                if let Some(output_var) = output_var {
                    execution_state.bindings.insert(output_var.clone(), reduced);
                }
                self.execute_from_state_in_graph(states, next, execution_state, false)
            }
            _ => Err(CompiledWorkflowError::NotWaitingOnBulk(wait_state.to_owned())),
        }
    }

    pub fn execute_after_bulk_failure_with_turn(
        &self,
        wait_state: &str,
        batch_id: &str,
        error: &Value,
        mut execution_state: ArtifactExecutionState,
        turn_context: ExecutionTurnContext,
    ) -> Result<CompiledExecutionPlan, CompiledWorkflowError> {
        execution_state.turn_context = Some(turn_context);
        let states = self
            .states_for(wait_state, &execution_state)
            .ok_or_else(|| CompiledWorkflowError::UnknownState(wait_state.to_owned()))?;
        let state = states
            .get(wait_state)
            .ok_or_else(|| CompiledWorkflowError::UnknownState(wait_state.to_owned()))?;
        match state {
            CompiledStateNode::WaitForBulkActivity {
                bulk_ref_var,
                next,
                output_var,
                on_error,
                ..
            } => {
                let bulk = self.bulk_binding(wait_state, bulk_ref_var, &execution_state)?;
                if bulk.batch_id != batch_id {
                    return Err(CompiledWorkflowError::UnexpectedBulkBatch {
                        expected: bulk.batch_id,
                        received: batch_id.to_owned(),
                    });
                }
                if bulk_reducer_settles(&bulk) {
                    let reduced = reduce_bulk_terminal_payload(error, &bulk);
                    execution_state.bindings.remove(bulk_ref_var);
                    if let Some(output_var) = output_var {
                        execution_state.bindings.insert(output_var.clone(), reduced);
                    }
                    return self.execute_from_state_in_graph(states, next, execution_state, false);
                }
                execution_state.bindings.remove(bulk_ref_var);
                if let Some(on_error) = on_error {
                    if let Some(error_var) = &on_error.error_var {
                        execution_state.bindings.insert(error_var.clone(), error.clone());
                    }
                    return self.execute_from_state_in_graph(
                        states,
                        &on_error.next,
                        execution_state,
                        false,
                    );
                }
                let reason = match error {
                    Value::String(reason) => reason.clone(),
                    other => serde_json::to_string(other)
                        .unwrap_or_else(|_| "bulk batch failed".to_owned()),
                };
                if let Some(active_update) = execution_state.active_update.take() {
                    let return_state = active_update.return_state;
                    return Ok(CompiledExecutionPlan {
                        workflow_version: self.definition_version,
                        final_state: return_state.clone(),
                        emissions: vec![ExecutionEmission {
                            event: WorkflowEvent::WorkflowUpdateRejected {
                                update_id: active_update.update_id,
                                update_name: active_update.update_name,
                                error: reason.clone(),
                            },
                            state: Some(return_state),
                        }],
                        execution_state,
                        context: Some(error.clone()),
                        output: Some(error.clone()),
                    });
                }
                execution_state.active_signal = None;
                Ok(CompiledExecutionPlan {
                    workflow_version: self.definition_version,
                    final_state: wait_state.to_owned(),
                    emissions: vec![ExecutionEmission {
                        event: WorkflowEvent::WorkflowFailed { reason },
                        state: Some(wait_state.to_owned()),
                    }],
                    execution_state,
                    context: Some(error.clone()),
                    output: Some(error.clone()),
                })
            }
            _ => Err(CompiledWorkflowError::NotWaitingOnBulk(wait_state.to_owned())),
        }
    }

    pub fn execute_after_step_completion(
        &self,
        step_state: &str,
        step_id: &str,
        output: &Value,
        execution_state: ArtifactExecutionState,
    ) -> Result<CompiledExecutionPlan, CompiledWorkflowError> {
        self.execute_after_step_completion_with_turn(
            step_state,
            step_id,
            output,
            execution_state,
            Self::synthetic_turn_context("step_completion"),
        )
    }

    pub fn try_execute_after_step_terminal_batch(
        &self,
        step_state: &str,
        events: &[EventEnvelope<WorkflowEvent>],
        execution_state: ArtifactExecutionState,
    ) -> Result<Option<(CompiledExecutionPlan, usize)>, CompiledWorkflowError> {
        self.try_execute_after_step_terminal_batch_with_turn(step_state, events, execution_state)
    }

    pub fn try_execute_after_step_terminal_batch_with_turn(
        &self,
        step_state: &str,
        events: &[EventEnvelope<WorkflowEvent>],
        mut execution_state: ArtifactExecutionState,
    ) -> Result<Option<(CompiledExecutionPlan, usize)>, CompiledWorkflowError> {
        if events.is_empty() {
            return Ok(None);
        }
        let states = self
            .states_for(step_state, &execution_state)
            .ok_or_else(|| CompiledWorkflowError::UnknownState(step_state.to_owned()))?;
        let state = states
            .get(step_state)
            .ok_or_else(|| CompiledWorkflowError::UnknownState(step_state.to_owned()))?;
        let CompiledStateNode::WaitForAllActivities { fanout_ref_var, next, output_var, .. } =
            state
        else {
            return Ok(None);
        };

        let mut fanout = self.fanout_binding(step_state, fanout_ref_var, &execution_state)?;
        if !fanout_reducer_settles(&fanout) {
            return Ok(None);
        }

        let mut applied = 0usize;
        let mut last_turn_context = None;
        for event in events {
            let Some((activity_id, value, completion)) = fanout_terminal_update(event) else {
                break;
            };
            let Some((origin_state, fanout_index)) = parse_fanout_activity_id(activity_id) else {
                break;
            };
            if fanout.origin_state != origin_state
                || fanout_index >= fanout_total_count(&fanout)
                || !fanout_slot_is_pending(&fanout, fanout_index)
            {
                break;
            }
            fanout_mark_completed(&mut fanout, fanout_index, value, completion);
            last_turn_context = Some(ExecutionTurnContext {
                event_id: event.event_id,
                occurred_at: event.occurred_at,
            });
            applied += 1;
            if fanout.pending_count == 0 {
                break;
            }
        }

        if applied == 0 {
            return Ok(None);
        }

        execution_state.turn_context = last_turn_context;
        if fanout.pending_count == 0 {
            execution_state.bindings.remove(fanout_ref_var);
            if let Some(output_var) = output_var {
                execution_state
                    .bindings
                    .insert(output_var.clone(), reduce_fanout_terminal_payload(&fanout));
            }
            let plan = self.execute_from_state_in_graph(states, next, execution_state, false)?;
            return Ok(Some((plan, applied)));
        }

        execution_state.bindings.insert(
            fanout_ref_var.clone(),
            serde_json::to_value(&fanout).expect("fanout state serializes"),
        );
        Ok(Some((
            CompiledExecutionPlan {
                workflow_version: self.definition_version,
                final_state: step_state.to_owned(),
                emissions: Vec::new(),
                execution_state,
                context: None,
                output: None,
            },
            applied,
        )))
    }

    pub fn execute_after_step_completion_with_turn(
        &self,
        step_state: &str,
        step_id: &str,
        output: &Value,
        mut execution_state: ArtifactExecutionState,
        turn_context: ExecutionTurnContext,
    ) -> Result<CompiledExecutionPlan, CompiledWorkflowError> {
        execution_state.turn_context = Some(turn_context);
        let states = self
            .states_for(step_state, &execution_state)
            .ok_or_else(|| CompiledWorkflowError::UnknownState(step_state.to_owned()))?;
        let state = states
            .get(step_state)
            .ok_or_else(|| CompiledWorkflowError::UnknownState(step_state.to_owned()))?;
        match state {
            CompiledStateNode::Step { next, output_var, .. } if step_state == step_id => {
                if let Some(output_var) = output_var {
                    execution_state.bindings.insert(output_var.clone(), output.clone());
                }
                let next = next.as_ref().ok_or_else(|| {
                    CompiledWorkflowError::MissingContinuation(step_state.to_owned())
                })?;
                self.execute_from_state_in_graph(states, next, execution_state, false)
            }
            CompiledStateNode::WaitForAllActivities {
                fanout_ref_var, next, output_var, ..
            } => {
                let mut fanout =
                    self.fanout_binding(step_state, fanout_ref_var, &execution_state)?;
                let Some((origin_state, fanout_index)) = parse_fanout_activity_id(step_id) else {
                    return Err(CompiledWorkflowError::InvalidFanOutActivityId(step_id.to_owned()));
                };
                if fanout.origin_state != origin_state
                    || fanout_index >= fanout_total_count(&fanout)
                    || !fanout_slot_is_pending(&fanout, fanout_index)
                {
                    return Err(CompiledWorkflowError::UnexpectedFanOutActivity {
                        state: step_state.to_owned(),
                        activity_id: step_id.to_owned(),
                    });
                }
                fanout_mark_completed(
                    &mut fanout,
                    fanout_index,
                    output.clone(),
                    CompletionKind::Succeeded,
                );
                if fanout.pending_count == 0 {
                    execution_state.bindings.remove(fanout_ref_var);
                    if let Some(output_var) = output_var {
                        execution_state
                            .bindings
                            .insert(output_var.clone(), reduce_fanout_terminal_payload(&fanout));
                    }
                    self.execute_from_state_in_graph(states, next, execution_state, false)
                } else {
                    execution_state.bindings.insert(
                        fanout_ref_var.clone(),
                        serde_json::to_value(&fanout).expect("fanout state serializes"),
                    );
                    Ok(CompiledExecutionPlan {
                        workflow_version: self.definition_version,
                        final_state: step_state.to_owned(),
                        emissions: Vec::new(),
                        execution_state,
                        context: None,
                        output: None,
                    })
                }
            }
            CompiledStateNode::Step { .. } => Err(CompiledWorkflowError::UnexpectedStep {
                expected: step_state.to_owned(),
                received: step_id.to_owned(),
            }),
            _ => Err(CompiledWorkflowError::NotWaitingOnStep(step_state.to_owned())),
        }
    }

    pub fn execute_after_step_failure(
        &self,
        step_state: &str,
        step_id: &str,
        error: &str,
        execution_state: ArtifactExecutionState,
    ) -> Result<CompiledExecutionPlan, CompiledWorkflowError> {
        self.execute_after_step_terminal_with_turn(
            step_state,
            step_id,
            error,
            execution_state,
            CompletionKind::Failed,
            Self::synthetic_turn_context("step_failure"),
        )
    }

    pub fn execute_after_step_failure_with_turn(
        &self,
        step_state: &str,
        step_id: &str,
        error: &str,
        execution_state: ArtifactExecutionState,
        turn_context: ExecutionTurnContext,
    ) -> Result<CompiledExecutionPlan, CompiledWorkflowError> {
        self.execute_after_step_terminal_with_turn(
            step_state,
            step_id,
            error,
            execution_state,
            CompletionKind::Failed,
            turn_context,
        )
    }

    pub fn execute_after_step_cancellation_with_turn(
        &self,
        step_state: &str,
        step_id: &str,
        reason: &str,
        execution_state: ArtifactExecutionState,
        turn_context: ExecutionTurnContext,
    ) -> Result<CompiledExecutionPlan, CompiledWorkflowError> {
        self.execute_after_step_terminal_with_turn(
            step_state,
            step_id,
            reason,
            execution_state,
            CompletionKind::Cancelled,
            turn_context,
        )
    }

    fn execute_after_step_terminal_with_turn(
        &self,
        step_state: &str,
        step_id: &str,
        error: &str,
        mut execution_state: ArtifactExecutionState,
        completion: CompletionKind,
        turn_context: ExecutionTurnContext,
    ) -> Result<CompiledExecutionPlan, CompiledWorkflowError> {
        execution_state.turn_context = Some(turn_context);
        let states = self
            .states_for(step_state, &execution_state)
            .ok_or_else(|| CompiledWorkflowError::UnknownState(step_state.to_owned()))?;
        let state = states
            .get(step_state)
            .ok_or_else(|| CompiledWorkflowError::UnknownState(step_state.to_owned()))?;
        match state {
            CompiledStateNode::Step { on_error: Some(on_error), .. } if step_state == step_id => {
                if let Some(error_var) = &on_error.error_var {
                    execution_state
                        .bindings
                        .insert(error_var.clone(), Value::String(error.to_owned()));
                }
                self.execute_from_state_in_graph(states, &on_error.next, execution_state, false)
            }
            CompiledStateNode::Step { on_error: None, .. } if step_state == step_id => {
                if let Some(active_update) = execution_state.active_update.take() {
                    let return_state = active_update.return_state;
                    return Ok(CompiledExecutionPlan {
                        workflow_version: self.definition_version,
                        final_state: return_state.clone(),
                        emissions: vec![ExecutionEmission {
                            event: WorkflowEvent::WorkflowUpdateRejected {
                                update_id: active_update.update_id,
                                update_name: active_update.update_name,
                                error: error.to_owned(),
                            },
                            state: Some(return_state),
                        }],
                        execution_state,
                        context: Some(Value::String(error.to_owned())),
                        output: Some(Value::String(error.to_owned())),
                    });
                }
                execution_state.active_signal = None;
                Ok(CompiledExecutionPlan {
                    workflow_version: self.definition_version,
                    final_state: step_state.to_owned(),
                    emissions: vec![ExecutionEmission {
                        event: WorkflowEvent::WorkflowFailed { reason: error.to_owned() },
                        state: Some(step_state.to_owned()),
                    }],
                    execution_state,
                    context: Some(Value::String(error.to_owned())),
                    output: Some(Value::String(error.to_owned())),
                })
            }
            CompiledStateNode::WaitForAllActivities {
                fanout_ref_var,
                next,
                output_var,
                on_error,
            } => {
                let mut fanout =
                    self.fanout_binding(step_state, fanout_ref_var, &execution_state)?;
                let Some((origin_state, fanout_index)) = parse_fanout_activity_id(step_id) else {
                    return Err(CompiledWorkflowError::InvalidFanOutActivityId(step_id.to_owned()));
                };
                if fanout.origin_state != origin_state
                    || fanout_index >= fanout_total_count(&fanout)
                    || !fanout_slot_is_pending(&fanout, fanout_index)
                {
                    return Err(CompiledWorkflowError::UnexpectedFanOutActivity {
                        state: step_state.to_owned(),
                        activity_id: step_id.to_owned(),
                    });
                }
                if fanout_reducer_settles(&fanout) {
                    fanout_mark_completed(
                        &mut fanout,
                        fanout_index,
                        Value::String(error.to_owned()),
                        completion,
                    );
                    if fanout.pending_count == 0 {
                        execution_state.bindings.remove(fanout_ref_var);
                        if let Some(output_var) = output_var {
                            execution_state.bindings.insert(
                                output_var.clone(),
                                reduce_fanout_terminal_payload(&fanout),
                            );
                        }
                        return self.execute_from_state_in_graph(
                            states,
                            next,
                            execution_state,
                            false,
                        );
                    }
                    execution_state.bindings.insert(
                        fanout_ref_var.clone(),
                        serde_json::to_value(&fanout).expect("fanout state serializes"),
                    );
                    return Ok(CompiledExecutionPlan {
                        workflow_version: self.definition_version,
                        final_state: step_state.to_owned(),
                        emissions: Vec::new(),
                        execution_state,
                        context: None,
                        output: None,
                    });
                }
                execution_state.bindings.remove(fanout_ref_var);
                if let Some(on_error) = on_error {
                    if let Some(error_var) = &on_error.error_var {
                        execution_state
                            .bindings
                            .insert(error_var.clone(), Value::String(error.to_owned()));
                    }
                    return self.execute_from_state_in_graph(
                        states,
                        &on_error.next,
                        execution_state,
                        false,
                    );
                }
                if let Some(active_update) = execution_state.active_update.take() {
                    let return_state = active_update.return_state;
                    return Ok(CompiledExecutionPlan {
                        workflow_version: self.definition_version,
                        final_state: return_state.clone(),
                        emissions: vec![ExecutionEmission {
                            event: WorkflowEvent::WorkflowUpdateRejected {
                                update_id: active_update.update_id,
                                update_name: active_update.update_name,
                                error: error.to_owned(),
                            },
                            state: Some(return_state),
                        }],
                        execution_state,
                        context: Some(Value::String(error.to_owned())),
                        output: Some(Value::String(error.to_owned())),
                    });
                }
                execution_state.active_signal = None;
                Ok(CompiledExecutionPlan {
                    workflow_version: self.definition_version,
                    final_state: step_state.to_owned(),
                    emissions: vec![ExecutionEmission {
                        event: WorkflowEvent::WorkflowFailed { reason: error.to_owned() },
                        state: Some(step_state.to_owned()),
                    }],
                    execution_state,
                    context: Some(Value::String(error.to_owned())),
                    output: Some(Value::String(error.to_owned())),
                })
            }
            CompiledStateNode::Step { .. } => Err(CompiledWorkflowError::UnexpectedStep {
                expected: step_state.to_owned(),
                received: step_id.to_owned(),
            }),
            _ => Err(CompiledWorkflowError::NotWaitingOnStep(step_state.to_owned())),
        }
    }

    pub fn step_retry(
        &self,
        step_state: &str,
        execution_state: &ArtifactExecutionState,
    ) -> Result<Option<&RetryPolicy>, CompiledWorkflowError> {
        let state = self
            .state_by_id(step_state)
            .ok_or_else(|| CompiledWorkflowError::UnknownState(step_state.to_owned()))?;
        match state {
            CompiledStateNode::Step { retry, .. } => Ok(retry.as_ref()),
            CompiledStateNode::WaitForAllActivities { fanout_ref_var, .. } => {
                let fanout = self.fanout_binding(step_state, fanout_ref_var, execution_state)?;
                match self.state_by_id(&fanout.origin_state) {
                    Some(CompiledStateNode::FanOut { retry, .. }) => Ok(retry.as_ref()),
                    _ => Err(CompiledWorkflowError::NotWaitingOnFanOut(step_state.to_owned())),
                }
            }
            _ => Err(CompiledWorkflowError::NotWaitingOnStep(step_state.to_owned())),
        }
    }

    pub fn step_details(
        &self,
        step_state: &str,
        execution_state: &ArtifactExecutionState,
    ) -> Result<(String, Option<StepConfig>, Value), CompiledWorkflowError> {
        let state = self
            .state_by_id(step_state)
            .or_else(|| {
                parse_fanout_activity_id(step_state)
                    .and_then(|(state_id, _)| self.state_by_id(&state_id))
            })
            .ok_or_else(|| CompiledWorkflowError::UnknownState(step_state.to_owned()))?;
        match state {
            CompiledStateNode::Step { handler, input, config, .. } => {
                let mut execution_state = execution_state.clone();
                let input = evaluate_expression(input, &mut execution_state, &self.helpers)?;
                Ok((handler.clone(), config.clone(), input))
            }
            CompiledStateNode::FanOut { activity_type, config, items, .. } => {
                let Some((origin_state, index)) = parse_fanout_activity_id(step_state) else {
                    return Err(CompiledWorkflowError::InvalidFanOutActivityId(
                        step_state.to_owned(),
                    ));
                };
                let (_, fanout) = self
                    .find_fanout_member(execution_state, &origin_state, index)
                    .ok_or_else(|| CompiledWorkflowError::UnexpectedFanOutActivity {
                        state: origin_state.clone(),
                        activity_id: step_state.to_owned(),
                    })?;
                let input = fanout.inputs.get(index).cloned().map(Ok).unwrap_or_else(|| {
                    let mut replay_state = execution_state.clone();
                    let evaluated = evaluate_expression(items, &mut replay_state, &self.helpers)?;
                    let Value::Array(items) = evaluated else {
                        return Err(CompiledWorkflowError::InvalidFanOutItems {
                            state: origin_state.clone(),
                        });
                    };
                    Ok(items.get(index).cloned().unwrap_or(Value::Null))
                })?;
                Ok((activity_type.clone(), config.clone(), input))
            }
            _ => Err(CompiledWorkflowError::NotWaitingOnStep(step_state.to_owned())),
        }
    }

    pub fn step_descriptor(
        &self,
        step_state: &str,
    ) -> Result<(String, Option<StepConfig>), CompiledWorkflowError> {
        let state = self
            .state_by_id(step_state)
            .or_else(|| {
                parse_fanout_activity_id(step_state)
                    .and_then(|(state_id, _)| self.state_by_id(&state_id))
            })
            .ok_or_else(|| CompiledWorkflowError::UnknownState(step_state.to_owned()))?;
        match state {
            CompiledStateNode::Step { handler, config, .. } => {
                Ok((handler.clone(), config.clone()))
            }
            CompiledStateNode::FanOut { activity_type, config, .. } => {
                Ok((activity_type.clone(), config.clone()))
            }
            _ => Err(CompiledWorkflowError::NotWaitingOnStep(step_state.to_owned())),
        }
    }

    pub fn step_timeouts(
        &self,
        step_state: &str,
    ) -> Result<(Option<u64>, Option<u64>, Option<u64>), CompiledWorkflowError> {
        let state = self
            .state_by_id(step_state)
            .or_else(|| {
                parse_fanout_activity_id(step_state)
                    .and_then(|(state_id, _)| self.state_by_id(&state_id))
            })
            .ok_or_else(|| CompiledWorkflowError::UnknownState(step_state.to_owned()))?;
        match state {
            CompiledStateNode::Step {
                schedule_to_start_timeout_ms,
                start_to_close_timeout_ms,
                heartbeat_timeout_ms,
                ..
            }
            | CompiledStateNode::FanOut {
                schedule_to_start_timeout_ms,
                start_to_close_timeout_ms,
                heartbeat_timeout_ms,
                ..
            } => Ok((
                *schedule_to_start_timeout_ms,
                *start_to_close_timeout_ms,
                *heartbeat_timeout_ms,
            )),
            _ => Err(CompiledWorkflowError::NotWaitingOnStep(step_state.to_owned())),
        }
    }

    fn fanout_binding(
        &self,
        state_id: &str,
        binding: &str,
        execution_state: &ArtifactExecutionState,
    ) -> Result<FanOutExecutionState, CompiledWorkflowError> {
        execution_state.bindings.get(binding).and_then(decode_fanout_state).ok_or_else(|| {
            CompiledWorkflowError::MissingFanOutBinding {
                state: state_id.to_owned(),
                binding: binding.to_owned(),
            }
        })
    }

    fn bulk_binding(
        &self,
        state_id: &str,
        binding: &str,
        execution_state: &ArtifactExecutionState,
    ) -> Result<BulkActivityExecutionState, CompiledWorkflowError> {
        execution_state.bindings.get(binding).and_then(decode_bulk_state).ok_or_else(|| {
            CompiledWorkflowError::MissingBulkBinding {
                state: state_id.to_owned(),
                binding: binding.to_owned(),
            }
        })
    }

    fn find_fanout_member(
        &self,
        execution_state: &ArtifactExecutionState,
        origin_state: &str,
        index: usize,
    ) -> Option<(String, FanOutExecutionState)> {
        execution_state.bindings.iter().find_map(|(binding, value)| {
            let fanout = decode_fanout_state(value)?;
            (fanout.origin_state == origin_state && index < fanout_total_count(&fanout))
                .then_some((binding.clone(), fanout))
        })
    }

    pub fn reschedule_state_for_activity(
        &self,
        activity_id: &str,
        execution_state: &ArtifactExecutionState,
    ) -> Option<String> {
        let (origin_state, index) = parse_fanout_activity_id(activity_id)?;
        self.find_fanout_member(execution_state, &origin_state, index)
            .map(|(_, fanout)| fanout.wait_state)
    }

    pub fn waits_on_bulk_activity(
        &self,
        wait_state: &str,
        execution_state: &ArtifactExecutionState,
    ) -> bool {
        let Some(states) = self.states_for(wait_state, execution_state) else {
            return false;
        };
        let Some(CompiledStateNode::WaitForBulkActivity { bulk_ref_var, .. }) =
            states.get(wait_state)
        else {
            return false;
        };
        self.bulk_binding(wait_state, bulk_ref_var, execution_state)
            .map(|bulk| bulk.wait_state == wait_state)
            .unwrap_or(false)
    }

    pub fn rehydrate_bulk_wait_context(
        &self,
        wait_state: &str,
        execution_state: &ArtifactExecutionState,
    ) -> Option<Value> {
        let states = self.states_for(wait_state, execution_state)?;
        let bulk = execution_state.bulk_wait_binding(wait_state)?;
        let CompiledStateNode::StartBulkActivity { items, .. } = states.get(&bulk.origin_state)?
        else {
            return None;
        };
        let mut execution_state = execution_state.clone();
        evaluate_expression(items, &mut execution_state, &self.helpers).ok()
    }

    fn execute_from_state(
        &self,
        start_state: &str,
        execution_state: ArtifactExecutionState,
        emit_started: bool,
    ) -> Result<CompiledExecutionPlan, CompiledWorkflowError> {
        self.execute_from_state_in_graph(
            &self.workflow.states,
            start_state,
            execution_state,
            emit_started,
        )
    }

    fn execute_from_state_in_graph(
        &self,
        states: &BTreeMap<String, CompiledStateNode>,
        start_state: &str,
        mut execution_state: ArtifactExecutionState,
        emit_started: bool,
    ) -> Result<CompiledExecutionPlan, CompiledWorkflowError> {
        let mut current_state = start_state.to_owned();
        let mut emissions = Vec::new();
        let mut context = execution_state.bindings.get("input").cloned();
        let mut output = None;
        let mut visited = 0usize;

        if emit_started {
            emissions.push(ExecutionEmission {
                event: WorkflowEvent::WorkflowStarted,
                state: Some(current_state.clone()),
            });
        }

        loop {
            if self.pending_workflow_cancellation_ready(&current_state, &execution_state).is_some()
            {
                return Ok(CompiledExecutionPlan {
                    workflow_version: self.definition_version,
                    final_state: current_state,
                    emissions,
                    execution_state,
                    context,
                    output,
                });
            }
            visited += 1;
            if visited > self.workflow.states.len() * 8 {
                return Err(CompiledWorkflowError::LoopDetected(current_state));
            }

            let state = states
                .get(&current_state)
                .ok_or_else(|| CompiledWorkflowError::UnknownState(current_state.clone()))?;

            match state {
                CompiledStateNode::Assign { actions, next } => {
                    for action in actions {
                        let value =
                            evaluate_expression(&action.expr, &mut execution_state, &self.helpers)?;
                        execution_state.bindings.insert(action.target.clone(), value);
                    }
                    emit_pending_markers(&mut emissions, &mut execution_state, &current_state);
                    current_state = next.clone();
                    if self
                        .pending_workflow_cancellation_ready(&current_state, &execution_state)
                        .is_some()
                    {
                        return Ok(CompiledExecutionPlan {
                            workflow_version: self.definition_version,
                            final_state: current_state,
                            emissions,
                            execution_state,
                            context,
                            output,
                        });
                    }
                }
                CompiledStateNode::Choice { condition, then_next, else_next } => {
                    let value =
                        evaluate_expression(condition, &mut execution_state, &self.helpers)?;
                    emit_pending_markers(&mut emissions, &mut execution_state, &current_state);
                    current_state =
                        if truthy(&value) { then_next.clone() } else { else_next.clone() };
                    if self
                        .pending_workflow_cancellation_ready(&current_state, &execution_state)
                        .is_some()
                    {
                        return Ok(CompiledExecutionPlan {
                            workflow_version: self.definition_version,
                            final_state: current_state,
                            emissions,
                            execution_state,
                            context,
                            output,
                        });
                    }
                }
                CompiledStateNode::Step {
                    input: step_input,
                    task_queue,
                    config,
                    schedule_to_start_timeout_ms,
                    start_to_close_timeout_ms,
                    heartbeat_timeout_ms,
                    ..
                } => {
                    let input =
                        evaluate_expression(step_input, &mut execution_state, &self.helpers)?;
                    context = Some(input.clone());
                    emit_pending_markers(&mut emissions, &mut execution_state, &current_state);
                    let activity_type = self.step_descriptor(&current_state)?.0;
                    let task_queue = task_queue
                        .as_ref()
                        .map(|expr| evaluate_expression(expr, &mut execution_state, &self.helpers))
                        .transpose()?
                        .and_then(|value| stringify_value(&value))
                        .unwrap_or_else(|| "default".to_owned());
                    emissions.push(ExecutionEmission {
                        event: WorkflowEvent::ActivityTaskScheduled {
                            activity_id: current_state.clone(),
                            activity_type,
                            task_queue,
                            attempt: 1,
                            input,
                            config: config.as_ref().map(|config| {
                                serde_json::to_value(config).expect("step config serializes")
                            }),
                            state: Some(current_state.clone()),
                            schedule_to_start_timeout_ms: *schedule_to_start_timeout_ms,
                            start_to_close_timeout_ms: *start_to_close_timeout_ms,
                            heartbeat_timeout_ms: *heartbeat_timeout_ms,
                        },
                        state: Some(current_state.clone()),
                    });
                    return Ok(CompiledExecutionPlan {
                        workflow_version: self.definition_version,
                        final_state: current_state,
                        emissions,
                        execution_state,
                        context,
                        output,
                    });
                }
                CompiledStateNode::FanOut {
                    activity_type,
                    items,
                    next,
                    handle_var,
                    task_queue,
                    reducer,
                    config,
                    schedule_to_start_timeout_ms,
                    start_to_close_timeout_ms,
                    heartbeat_timeout_ms,
                    ..
                } => {
                    let items = evaluate_expression(items, &mut execution_state, &self.helpers)?;
                    let Value::Array(items) = items else {
                        return Err(CompiledWorkflowError::InvalidFanOutItems {
                            state: current_state.clone(),
                        });
                    };
                    let task_queue = task_queue
                        .as_ref()
                        .map(|expr| evaluate_expression(expr, &mut execution_state, &self.helpers))
                        .transpose()?
                        .and_then(|value| stringify_value(&value))
                        .unwrap_or_else(|| "default".to_owned());
                    emit_pending_markers(&mut emissions, &mut execution_state, &current_state);

                    if items.is_empty() {
                        let wait_state = states
                            .get(next)
                            .ok_or_else(|| CompiledWorkflowError::UnknownState(next.clone()))?;
                        if let CompiledStateNode::WaitForAllActivities {
                            fanout_ref_var,
                            next: after_wait,
                            output_var,
                            ..
                        } = wait_state
                        {
                            execution_state.bindings.remove(fanout_ref_var);
                            if let Some(output_var) = output_var {
                                execution_state.bindings.insert(
                                    output_var.clone(),
                                    empty_fanout_terminal_payload(reducer.as_deref()),
                                );
                            }
                            current_state = after_wait.clone();
                            if self
                                .pending_workflow_cancellation_ready(
                                    &current_state,
                                    &execution_state,
                                )
                                .is_some()
                            {
                                return Ok(CompiledExecutionPlan {
                                    workflow_version: self.definition_version,
                                    final_state: current_state,
                                    emissions,
                                    execution_state,
                                    context,
                                    output,
                                });
                            }
                            continue;
                        }
                        return Err(CompiledWorkflowError::NotWaitingOnFanOut(next.clone()));
                    }

                    let reducer_name = reducer.as_deref().unwrap_or("collect_results");
                    let uses_materialized_results =
                        bulk_reducer_requires_success_outputs(Some(reducer_name));
                    let mut results = uses_materialized_results
                        .then(|| Vec::with_capacity(items.len()))
                        .unwrap_or_default();
                    for (index, item) in items.iter().enumerate() {
                        let activity_id = build_fanout_activity_id(&current_state, index);
                        if uses_materialized_results {
                            results.push(None);
                        }
                        emissions.push(ExecutionEmission {
                            event: WorkflowEvent::ActivityTaskScheduled {
                                activity_id,
                                activity_type: activity_type.clone(),
                                task_queue: task_queue.clone(),
                                attempt: 1,
                                input: item.clone(),
                                config: config.as_ref().map(|config| {
                                    serde_json::to_value(config)
                                        .expect("fanout activity config serializes")
                                }),
                                state: Some(next.clone()),
                                schedule_to_start_timeout_ms: *schedule_to_start_timeout_ms,
                                start_to_close_timeout_ms: *start_to_close_timeout_ms,
                                heartbeat_timeout_ms: *heartbeat_timeout_ms,
                            },
                            state: Some(next.clone()),
                        });
                    }
                    execution_state.bindings.insert(
                        handle_var.clone(),
                        serde_json::to_value(FanOutExecutionState {
                            origin_state: current_state.clone(),
                            wait_state: next.clone(),
                            task_queue,
                            total_count: items.len(),
                            pending_count: items.len(),
                            succeeded_count: 0,
                            failed_count: 0,
                            cancelled_count: 0,
                            inputs: uses_materialized_results
                                .then_some(items.clone())
                                .unwrap_or_default(),
                            pending_activity_ids: Vec::new(),
                            results,
                            failed_activity_errors: BTreeMap::new(),
                            first_failed_index: None,
                            first_failed_error: None,
                            reducer: reducer.clone(),
                            settlement_bitmap: if uses_materialized_results {
                                Vec::new()
                            } else {
                                vec![0; items.len().div_ceil(64)]
                            },
                        })
                        .expect("fanout execution state serializes"),
                    );
                    context = Some(Value::Array(items));
                    return Ok(CompiledExecutionPlan {
                        workflow_version: self.definition_version,
                        final_state: next.clone(),
                        emissions,
                        execution_state,
                        context,
                        output,
                    });
                }
                CompiledStateNode::StartBulkActivity {
                    activity_type,
                    items,
                    next,
                    handle_var,
                    task_queue,
                    execution_policy,
                    reducer,
                    throughput_backend,
                    chunk_size,
                    retry,
                } => {
                    let items = evaluate_expression(items, &mut execution_state, &self.helpers)?;
                    let Value::Array(items) = items else {
                        return Err(CompiledWorkflowError::InvalidBulkItems {
                            state: current_state.clone(),
                        });
                    };
                    let task_queue = task_queue
                        .as_ref()
                        .map(|expr| evaluate_expression(expr, &mut execution_state, &self.helpers))
                        .transpose()?
                        .and_then(|value| stringify_value(&value))
                        .unwrap_or_else(|| "default".to_owned());
                    let chunk_size = chunk_size.unwrap_or(256).clamp(1, MAX_BULK_CHUNK_SIZE);
                    let max_attempts =
                        retry.as_ref().map(|policy| policy.max_attempts).unwrap_or(1);
                    let retry_delay_ms = retry
                        .as_ref()
                        .map(|policy| {
                            parse_timer_ref(&policy.delay)
                                .map(|duration| duration.num_milliseconds().max(0) as u64)
                        })
                        .transpose()
                        .map_err(|error| CompiledWorkflowError::InvalidBulkRetryDelay {
                            state: current_state.clone(),
                            details: error.to_string(),
                        })?
                        .unwrap_or_default();
                    let batch_id = build_bulk_batch_id(
                        execution_state.turn_context.as_ref().ok_or_else(|| {
                            CompiledWorkflowError::MissingTurnContext(
                                "ctx.bulkActivity()".to_owned(),
                            )
                        })?,
                        &current_state,
                    );
                    validate_bulk_items(&current_state, &items, chunk_size as usize)?;
                    if items.is_empty() {
                        let wait_state = states
                            .get(next)
                            .ok_or_else(|| CompiledWorkflowError::UnknownState(next.clone()))?;
                        if let CompiledStateNode::WaitForBulkActivity {
                            bulk_ref_var,
                            next: after_wait,
                            output_var,
                            ..
                        } = wait_state
                        {
                            execution_state.bindings.remove(bulk_ref_var);
                            if let Some(output_var) = output_var {
                                execution_state.bindings.insert(
                                    output_var.clone(),
                                    bulk_success_summary(
                                        &batch_id,
                                        reducer.as_deref(),
                                        0,
                                        0,
                                        0,
                                        0,
                                        0,
                                    ),
                                );
                            }
                            current_state = after_wait.clone();
                            continue;
                        }
                        return Err(CompiledWorkflowError::NotWaitingOnBulk(next.clone()));
                    }
                    let total_items = items_len_to_u32(items.len())?;
                    let throughput_backend = throughput_backend.clone().unwrap_or_default();
                    let input_handle =
                        serde_json::to_value(PayloadHandle::inline_batch_input(&batch_id))
                            .expect("bulk input handle serializes");
                    let result_handle =
                        serde_json::to_value(PayloadHandle::inline_batch_result(&batch_id))
                            .expect("bulk result handle serializes");
                    emit_pending_markers(&mut emissions, &mut execution_state, &current_state);
                    emissions.push(ExecutionEmission {
                        event: WorkflowEvent::BulkActivityBatchScheduled {
                            batch_id: batch_id.clone(),
                            activity_type: activity_type.clone(),
                            task_queue: task_queue.clone(),
                            items: items.clone(),
                            input_handle,
                            result_handle,
                            chunk_size,
                            max_attempts,
                            retry_delay_ms,
                            aggregation_group_count: DEFAULT_AGGREGATION_GROUP_COUNT,
                            execution_policy: execution_policy.clone(),
                            reducer: reducer.clone(),
                            throughput_backend_version: if throughput_backend
                                == ThroughputBackend::StreamV2.as_str()
                            {
                                ThroughputBackend::StreamV2.default_version().to_owned()
                            } else if throughput_backend == ThroughputBackend::PgV1.as_str() {
                                ThroughputBackend::PgV1.default_version().to_owned()
                            } else {
                                String::new()
                            },
                            throughput_backend,
                            state: Some(next.clone()),
                        },
                        state: Some(next.clone()),
                    });
                    execution_state.bindings.insert(
                        handle_var.clone(),
                        serde_json::to_value(BulkActivityExecutionState {
                            batch_id,
                            origin_state: current_state.clone(),
                            wait_state: next.clone(),
                            activity_type: activity_type.clone(),
                            task_queue,
                            execution_policy: execution_policy.clone(),
                            reducer: reducer.clone(),
                            total_items,
                            chunk_size,
                            max_attempts,
                            retry_delay_ms,
                        })
                        .expect("bulk execution state serializes"),
                    );
                    context = Some(Value::Array(items));
                    return Ok(CompiledExecutionPlan {
                        workflow_version: self.definition_version,
                        final_state: next.clone(),
                        emissions,
                        execution_state,
                        context,
                        output,
                    });
                }
                CompiledStateNode::WaitForBulkActivity { .. } => {
                    return Ok(CompiledExecutionPlan {
                        workflow_version: self.definition_version,
                        final_state: current_state,
                        emissions,
                        execution_state,
                        context,
                        output,
                    });
                }
                CompiledStateNode::WaitForAllActivities { .. } => {
                    return Ok(CompiledExecutionPlan {
                        workflow_version: self.definition_version,
                        final_state: current_state,
                        emissions,
                        execution_state,
                        context,
                        output,
                    });
                }
                CompiledStateNode::StartChild {
                    child_definition_id,
                    input: child_input,
                    next,
                    handle_var,
                    workflow_id,
                    task_queue,
                    parent_close_policy,
                } => {
                    let input =
                        evaluate_expression(child_input, &mut execution_state, &self.helpers)?;
                    let child_id = build_child_id(
                        execution_state.turn_context.as_ref().ok_or_else(|| {
                            CompiledWorkflowError::MissingTurnContext("ctx.startChild()".to_owned())
                        })?,
                        &current_state,
                    );
                    let workflow_id = workflow_id
                        .as_ref()
                        .map(|expr| evaluate_expression(expr, &mut execution_state, &self.helpers))
                        .transpose()?
                        .and_then(|value| stringify_value(&value))
                        .unwrap_or_else(|| format!("child-{child_id}"));
                    let task_queue = task_queue
                        .as_ref()
                        .map(|expr| evaluate_expression(expr, &mut execution_state, &self.helpers))
                        .transpose()?
                        .and_then(|value| stringify_value(&value));
                    if let Some(handle_var) = handle_var {
                        execution_state.bindings.insert(
                            handle_var.clone(),
                            serde_json::json!({
                                "child_id": child_id,
                                "workflow_id": workflow_id,
                            }),
                        );
                    }
                    context = Some(input.clone());
                    emit_pending_markers(&mut emissions, &mut execution_state, &current_state);
                    emissions.push(ExecutionEmission {
                        event: WorkflowEvent::ChildWorkflowStartRequested {
                            child_id,
                            child_workflow_id: workflow_id,
                            child_definition_id: child_definition_id.clone(),
                            input,
                            task_queue,
                            parent_close_policy: parent_close_policy.as_event_value().to_owned(),
                        },
                        state: Some(next.clone()),
                    });
                    current_state = next.clone();
                    if self
                        .pending_workflow_cancellation_ready(&current_state, &execution_state)
                        .is_some()
                    {
                        return Ok(CompiledExecutionPlan {
                            workflow_version: self.definition_version,
                            final_state: current_state,
                            emissions,
                            execution_state,
                            context,
                            output,
                        });
                    }
                }
                CompiledStateNode::SignalChild { child_ref_var, signal_name, payload, next } => {
                    let child_id = execution_state
                        .bindings
                        .get(child_ref_var)
                        .ok_or_else(|| {
                            CompiledWorkflowError::UnknownChildReference(child_ref_var.clone())
                        })?
                        .get("child_id")
                        .and_then(Value::as_str)
                        .ok_or_else(|| {
                            CompiledWorkflowError::UnknownChildReference(child_ref_var.clone())
                        })?
                        .to_owned();
                    let payload =
                        evaluate_expression(payload, &mut execution_state, &self.helpers)?;
                    emit_pending_markers(&mut emissions, &mut execution_state, &current_state);
                    emissions.push(ExecutionEmission {
                        event: WorkflowEvent::ChildWorkflowSignalRequested {
                            child_id,
                            signal_name: signal_name.clone(),
                            payload,
                        },
                        state: Some(next.clone()),
                    });
                    current_state = next.clone();
                }
                CompiledStateNode::CancelChild { child_ref_var, reason, next } => {
                    let child_id = execution_state
                        .bindings
                        .get(child_ref_var)
                        .ok_or_else(|| {
                            CompiledWorkflowError::UnknownChildReference(child_ref_var.clone())
                        })?
                        .get("child_id")
                        .and_then(Value::as_str)
                        .ok_or_else(|| {
                            CompiledWorkflowError::UnknownChildReference(child_ref_var.clone())
                        })?
                        .to_owned();
                    let reason = reason
                        .as_ref()
                        .map(|expr| evaluate_expression(expr, &mut execution_state, &self.helpers))
                        .transpose()?
                        .and_then(|value| stringify_value(&value))
                        .unwrap_or_else(|| "cancel requested by parent".to_owned());
                    emit_pending_markers(&mut emissions, &mut execution_state, &current_state);
                    emissions.push(ExecutionEmission {
                        event: WorkflowEvent::ChildWorkflowCancellationRequested {
                            child_id,
                            reason,
                        },
                        state: Some(next.clone()),
                    });
                    current_state = next.clone();
                }
                CompiledStateNode::SignalExternal {
                    target_instance_id,
                    target_run_id,
                    signal_name,
                    payload,
                    next,
                } => {
                    let target_instance_id = evaluate_expression(
                        target_instance_id,
                        &mut execution_state,
                        &self.helpers,
                    )?
                    .as_str()
                    .map(str::to_owned)
                    .ok_or_else(|| {
                        CompiledWorkflowError::InvalidExternalWorkflowTarget(current_state.clone())
                    })?;
                    let target_run_id = target_run_id
                        .as_ref()
                        .map(|expr| evaluate_expression(expr, &mut execution_state, &self.helpers))
                        .transpose()?
                        .and_then(|value| stringify_value(&value));
                    let payload =
                        evaluate_expression(payload, &mut execution_state, &self.helpers)?;
                    emit_pending_markers(&mut emissions, &mut execution_state, &current_state);
                    emissions.push(ExecutionEmission {
                        event: WorkflowEvent::ExternalWorkflowSignalRequested {
                            target_instance_id,
                            target_run_id,
                            signal_name: signal_name.clone(),
                            payload,
                        },
                        state: Some(next.clone()),
                    });
                    current_state = next.clone();
                }
                CompiledStateNode::CancelExternal {
                    target_instance_id,
                    target_run_id,
                    reason,
                    next,
                } => {
                    let target_instance_id = evaluate_expression(
                        target_instance_id,
                        &mut execution_state,
                        &self.helpers,
                    )?
                    .as_str()
                    .map(str::to_owned)
                    .ok_or_else(|| {
                        CompiledWorkflowError::InvalidExternalWorkflowTarget(current_state.clone())
                    })?;
                    let target_run_id = target_run_id
                        .as_ref()
                        .map(|expr| evaluate_expression(expr, &mut execution_state, &self.helpers))
                        .transpose()?
                        .and_then(|value| stringify_value(&value));
                    let reason = reason
                        .as_ref()
                        .map(|expr| evaluate_expression(expr, &mut execution_state, &self.helpers))
                        .transpose()?
                        .and_then(|value| stringify_value(&value))
                        .unwrap_or_else(|| "cancel requested by external handle".to_owned());
                    emit_pending_markers(&mut emissions, &mut execution_state, &current_state);
                    emissions.push(ExecutionEmission {
                        event: WorkflowEvent::ExternalWorkflowCancellationRequested {
                            target_instance_id,
                            target_run_id,
                            reason,
                        },
                        state: Some(next.clone()),
                    });
                    current_state = next.clone();
                }
                CompiledStateNode::WaitForChild { .. } => {
                    return Ok(CompiledExecutionPlan {
                        workflow_version: self.definition_version,
                        final_state: current_state,
                        emissions,
                        execution_state,
                        context,
                        output,
                    });
                }
                CompiledStateNode::WaitForEvent { .. } => {
                    return Ok(CompiledExecutionPlan {
                        workflow_version: self.definition_version,
                        final_state: current_state,
                        emissions,
                        execution_state,
                        context,
                        output,
                    });
                }
                CompiledStateNode::WaitForCondition { condition, next, timeout_ref, .. } => {
                    let ready = truthy(&evaluate_expression(
                        condition,
                        &mut execution_state,
                        &self.helpers,
                    )?);
                    if ready {
                        execution_state.condition_timers.remove(&current_state);
                        current_state = next.clone();
                        if self
                            .pending_workflow_cancellation_ready(&current_state, &execution_state)
                            .is_some()
                        {
                            return Ok(CompiledExecutionPlan {
                                workflow_version: self.definition_version,
                                final_state: current_state,
                                emissions,
                                execution_state,
                                context,
                                output,
                            });
                        }
                        continue;
                    }
                    if let Some(timeout_ref) = timeout_ref {
                        if !execution_state.condition_timers.contains(&current_state) {
                            let fire_at = Utc::now()
                                + crate::parse_timer_ref(timeout_ref).map_err(|source| {
                                    CompiledWorkflowError::InvalidTimer {
                                        state: current_state.clone(),
                                        timer_ref: timeout_ref.clone(),
                                        details: source.to_string(),
                                    }
                                })?;
                            execution_state.condition_timers.insert(current_state.clone());
                            emissions.push(ExecutionEmission {
                                event: WorkflowEvent::TimerScheduled {
                                    timer_id: current_state.clone(),
                                    fire_at,
                                },
                                state: Some(current_state.clone()),
                            });
                        }
                    }
                    return Ok(CompiledExecutionPlan {
                        workflow_version: self.definition_version,
                        final_state: current_state,
                        emissions,
                        execution_state,
                        context,
                        output,
                    });
                }
                CompiledStateNode::WaitForTimer { timer_ref, timer_expr, .. } => {
                    emit_pending_markers(&mut emissions, &mut execution_state, &current_state);
                    let timer_delay = evaluate_timer_delay(
                        timer_ref,
                        timer_expr.as_ref(),
                        &current_state,
                        &mut execution_state,
                        &self.helpers,
                    )?;
                    let fire_at = Utc::now() + timer_delay;
                    emissions.push(ExecutionEmission {
                        event: WorkflowEvent::TimerScheduled {
                            timer_id: current_state.clone(),
                            fire_at,
                        },
                        state: Some(current_state.clone()),
                    });
                    return Ok(CompiledExecutionPlan {
                        workflow_version: self.definition_version,
                        final_state: current_state,
                        emissions,
                        execution_state,
                        context,
                        output,
                    });
                }
                CompiledStateNode::Succeed { output: expression } => {
                    let value = expression
                        .as_ref()
                        .map(|expression| {
                            evaluate_expression(expression, &mut execution_state, &self.helpers)
                        })
                        .transpose()?
                        .or_else(|| context.clone())
                        .unwrap_or(Value::Null);
                    emit_pending_markers(&mut emissions, &mut execution_state, &current_state);
                    output = Some(value.clone());
                    context = Some(value.clone());
                    if let Some(active_update) = execution_state.active_update.take() {
                        let return_state = active_update.return_state;
                        let completion = ExecutionEmission {
                            event: WorkflowEvent::WorkflowUpdateCompleted {
                                update_id: active_update.update_id,
                                update_name: active_update.update_name,
                                output: value.clone(),
                            },
                            state: Some(return_state.clone()),
                        };
                        if self
                            .pending_workflow_cancellation_ready(&return_state, &execution_state)
                            .is_some()
                        {
                            emissions.push(completion.clone());
                            current_state = return_state.clone();
                        } else if matches!(
                            self.workflow.states.get(&return_state),
                            Some(CompiledStateNode::WaitForCondition { .. })
                        ) {
                            let mut resumed = self.execute_from_state_in_graph(
                                &self.workflow.states,
                                &return_state,
                                execution_state,
                                false,
                            )?;
                            let mut prefixed = vec![completion];
                            prefixed.append(&mut resumed.emissions);
                            resumed.emissions = prefixed;
                            return Ok(resumed);
                        }
                        if current_state != return_state {
                            emissions.push(completion);
                            current_state = return_state;
                        }
                    } else if let Some(active_signal) = execution_state.active_signal.take() {
                        current_state = active_signal.return_state;
                        if self
                            .pending_workflow_cancellation_ready(&current_state, &execution_state)
                            .is_some()
                        {
                            return Ok(CompiledExecutionPlan {
                                workflow_version: self.definition_version,
                                final_state: current_state,
                                emissions,
                                execution_state,
                                context,
                                output,
                            });
                        }
                        if matches!(
                            self.workflow.states.get(&current_state),
                            Some(CompiledStateNode::WaitForCondition { .. })
                        ) {
                            return self.execute_from_state_in_graph(
                                &self.workflow.states,
                                &current_state,
                                execution_state,
                                false,
                            );
                        }
                    } else {
                        emissions.push(ExecutionEmission {
                            event: WorkflowEvent::WorkflowCompleted { output: value },
                            state: Some(current_state.clone()),
                        });
                    }
                    return Ok(CompiledExecutionPlan {
                        workflow_version: self.definition_version,
                        final_state: current_state,
                        emissions,
                        execution_state,
                        context,
                        output,
                    });
                }
                CompiledStateNode::Fail { reason } => {
                    let reason = reason
                        .as_ref()
                        .map(|expression| {
                            evaluate_expression(expression, &mut execution_state, &self.helpers)
                        })
                        .transpose()?
                        .and_then(|value| stringify_value(&value))
                        .unwrap_or_else(|| format!("workflow entered fail state {current_state}"));
                    emit_pending_markers(&mut emissions, &mut execution_state, &current_state);
                    output = Some(Value::String(reason.clone()));
                    context = Some(Value::String(reason.clone()));
                    if let Some(active_update) = execution_state.active_update.take() {
                        let return_state = active_update.return_state;
                        emissions.push(ExecutionEmission {
                            event: WorkflowEvent::WorkflowUpdateRejected {
                                update_id: active_update.update_id,
                                update_name: active_update.update_name,
                                error: reason.clone(),
                            },
                            state: Some(return_state.clone()),
                        });
                        current_state = return_state;
                    } else {
                        execution_state.active_signal = None;
                        emissions.push(ExecutionEmission {
                            event: WorkflowEvent::WorkflowFailed { reason },
                            state: Some(current_state.clone()),
                        });
                    }
                    return Ok(CompiledExecutionPlan {
                        workflow_version: self.definition_version,
                        final_state: current_state,
                        emissions,
                        execution_state,
                        context,
                        output,
                    });
                }
                CompiledStateNode::ContinueAsNew { input } => {
                    let continued_input = input
                        .as_ref()
                        .map(|expression| {
                            evaluate_expression(expression, &mut execution_state, &self.helpers)
                        })
                        .transpose()?
                        .or_else(|| context.clone())
                        .unwrap_or(Value::Null);
                    emit_pending_markers(&mut emissions, &mut execution_state, &current_state);
                    let new_run_id = format!("run-{}", uuid::Uuid::now_v7());
                    emissions.push(ExecutionEmission {
                        event: WorkflowEvent::WorkflowContinuedAsNew {
                            new_run_id,
                            input: continued_input.clone(),
                        },
                        state: Some(current_state.clone()),
                    });
                    return Ok(CompiledExecutionPlan {
                        workflow_version: self.definition_version,
                        final_state: current_state,
                        emissions,
                        execution_state,
                        context: Some(continued_input),
                        output,
                    });
                }
            }
        }
    }
}

impl CompiledStateNode {
    fn next_states(&self) -> Vec<&str> {
        match self {
            Self::Assign { next, .. } => vec![next.as_str()],
            Self::Choice { then_next, else_next, .. } => {
                vec![then_next.as_str(), else_next.as_str()]
            }
            Self::Step { next, on_error, .. } => next
                .iter()
                .map(String::as_str)
                .chain(on_error.iter().map(|transition| transition.next.as_str()))
                .collect(),
            Self::FanOut { next, .. } => vec![next.as_str()],
            Self::StartBulkActivity { next, .. } => vec![next.as_str()],
            Self::WaitForBulkActivity { next, on_error, .. } => std::iter::once(next.as_str())
                .chain(on_error.iter().map(|transition| transition.next.as_str()))
                .collect(),
            Self::WaitForAllActivities { next, on_error, .. } => std::iter::once(next.as_str())
                .chain(on_error.iter().map(|transition| transition.next.as_str()))
                .collect(),
            Self::StartChild { next, .. }
            | Self::SignalChild { next, .. }
            | Self::CancelChild { next, .. }
            | Self::SignalExternal { next, .. }
            | Self::CancelExternal { next, .. }
            | Self::WaitForEvent { next, .. }
            | Self::WaitForTimer { next, .. }
            | Self::WaitForChild { next, .. } => vec![next.as_str()],
            Self::WaitForCondition { next, timeout_next, .. } => std::iter::once(next.as_str())
                .chain(timeout_next.iter().map(String::as_str))
                .collect(),
            Self::Succeed { .. } | Self::Fail { .. } | Self::ContinueAsNew { .. } => Vec::new(),
        }
    }
}

impl ParentClosePolicy {
    fn as_event_value(&self) -> &'static str {
        match self {
            Self::Terminate => "TERMINATE",
            Self::RequestCancel => "REQUEST_CANCEL",
            Self::Abandon => "ABANDON",
        }
    }
}

const FANOUT_ACTIVITY_SEPARATOR: &str = "::";
const MAX_BULK_ITEMS_PER_BATCH: usize = 100_000;
const MAX_BULK_ITEM_BYTES: usize = 256 * 1024;
const MAX_BULK_TOTAL_INPUT_BYTES: usize = 8 * 1024 * 1024;
const MAX_BULK_CHUNK_SIZE: u32 = 1024;
const MAX_BULK_CHUNK_INPUT_BYTES: usize = 1024 * 1024;

fn build_child_id(turn_context: &ExecutionTurnContext, state_id: &str) -> String {
    uuid::Uuid::new_v5(
        &uuid::Uuid::NAMESPACE_URL,
        format!("{}:{state_id}:child", turn_context.event_id).as_bytes(),
    )
    .to_string()
}

fn extract_child_id(value: &Value) -> Option<String> {
    value.get("child_id").and_then(Value::as_str).map(str::to_owned)
}

fn build_bulk_batch_id(turn_context: &ExecutionTurnContext, state_id: &str) -> String {
    uuid::Uuid::new_v5(
        &uuid::Uuid::NAMESPACE_URL,
        format!("{}:{state_id}:bulk", turn_context.event_id).as_bytes(),
    )
    .to_string()
}

fn bulk_success_summary(
    batch_id: &str,
    reducer: Option<&str>,
    total_items: u32,
    succeeded_items: u32,
    failed_items: u32,
    cancelled_items: u32,
    chunk_count: u32,
) -> Value {
    let mut summary = serde_json::json!({
        "batchId": batch_id,
        "status": "completed",
        "totalItems": total_items,
        "succeededItems": succeeded_items,
        "failedItems": failed_items,
        "cancelledItems": cancelled_items,
        "chunkCount": chunk_count,
        "resultHandle": { "batchId": batch_id },
    });
    if let Some(field_name) = bulk_reducer_summary_field_name(reducer) {
        summary[field_name] = match field_name {
            "count" | BULK_REDUCER_SUM => Value::from(0.0),
            BULK_REDUCER_MIN | BULK_REDUCER_MAX | BULK_REDUCER_AVG => Value::Null,
            _ => Value::Null,
        };
    }
    summary
}

fn bulk_reducer<'a>(bulk: &'a BulkActivityExecutionState) -> &'a str {
    bulk.reducer.as_deref().unwrap_or("collect_results")
}

fn fanout_reducer<'a>(fanout: &'a FanOutExecutionState) -> &'a str {
    fanout.reducer.as_deref().unwrap_or("collect_results")
}

fn fanout_reducer_settles(fanout: &FanOutExecutionState) -> bool {
    matches!(fanout_reducer(fanout), "all_settled" | "count" | "collect_settled_results")
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CompletionKind {
    Succeeded,
    Failed,
    Cancelled,
}

fn fanout_total_count(fanout: &FanOutExecutionState) -> usize {
    if fanout.total_count > 0 {
        return fanout.total_count;
    }
    fanout.inputs.len().max(fanout.results.len()).max(fanout.pending_activity_ids.len()).max(
        fanout
            .failed_activity_errors
            .last_key_value()
            .map(|(index, _)| index.saturating_add(1))
            .unwrap_or(0),
    )
}

fn fanout_uses_counter_state(fanout: &FanOutExecutionState) -> bool {
    !fanout.settlement_bitmap.is_empty()
}

fn fanout_materializes_settled_results(fanout: &FanOutExecutionState) -> bool {
    fanout_reducer(fanout) == "collect_settled_results"
}

fn fanout_settled_result_value(completion: CompletionKind, value: Value) -> Value {
    match completion {
        CompletionKind::Succeeded => serde_json::json!({
            "status": "fulfilled",
            "value": value,
        }),
        CompletionKind::Failed | CompletionKind::Cancelled => serde_json::json!({
            "status": "rejected",
            "reason": value,
        }),
    }
}

fn fanout_bitmap_contains(fanout: &FanOutExecutionState, index: usize) -> bool {
    let Some(word) = fanout.settlement_bitmap.get(index / 64) else {
        return false;
    };
    let mask = 1u64 << (index % 64);
    word & mask != 0
}

fn fanout_bitmap_mark(fanout: &mut FanOutExecutionState, index: usize) {
    if let Some(word) = fanout.settlement_bitmap.get_mut(index / 64) {
        *word |= 1u64 << (index % 64);
    }
}

fn fanout_legacy_summary_counts(fanout: &FanOutExecutionState) -> (usize, usize, usize) {
    if fanout_materializes_settled_results(fanout) {
        return fanout.results.iter().fold((0usize, 0usize, 0usize), |mut counts, value| {
            match value
                .as_ref()
                .and_then(Value::as_object)
                .and_then(|entry| entry.get("status"))
                .and_then(Value::as_str)
            {
                Some("fulfilled") => counts.0 += 1,
                Some("rejected") => counts.1 += 1,
                _ => {}
            }
            counts
        });
    }
    let succeeded = fanout.results.iter().filter(|value| value.is_some()).count();
    let failed = fanout.failed_activity_errors.len();
    (succeeded, failed, 0)
}

fn fanout_summary_counts(fanout: &FanOutExecutionState) -> (usize, usize, usize) {
    if fanout_uses_counter_state(fanout) {
        return (fanout.succeeded_count, fanout.failed_count, fanout.cancelled_count);
    }
    fanout_legacy_summary_counts(fanout)
}

fn fanout_mark_completed(
    fanout: &mut FanOutExecutionState,
    index: usize,
    value: Value,
    completion: CompletionKind,
) {
    let error_message = (!matches!(completion, CompletionKind::Succeeded))
        .then(|| value.as_str().unwrap_or("activity failed").to_owned());
    fanout.pending_activity_ids.retain(|activity_id| {
        activity_id != &build_fanout_activity_id(&fanout.origin_state, index)
    });
    fanout.pending_count = fanout_pending_count(fanout).saturating_sub(1);
    if fanout_uses_counter_state(fanout) {
        fanout_bitmap_mark(fanout, index);
        match completion {
            CompletionKind::Succeeded => {
                fanout.succeeded_count = fanout.succeeded_count.saturating_add(1)
            }
            CompletionKind::Failed => fanout.failed_count = fanout.failed_count.saturating_add(1),
            CompletionKind::Cancelled => {
                fanout.cancelled_count = fanout.cancelled_count.saturating_add(1)
            }
        }
        if !matches!(completion, CompletionKind::Succeeded)
            && fanout.first_failed_index.map_or(true, |current| index < current)
        {
            fanout.first_failed_index = Some(index);
            fanout.first_failed_error = error_message.clone();
        }
    } else {
        if index < fanout.results.len() {
            fanout.results[index] = if fanout_materializes_settled_results(fanout) {
                Some(fanout_settled_result_value(completion, value.clone()))
            } else {
                matches!(completion, CompletionKind::Succeeded).then_some(value)
            };
        }
    }
    if matches!(completion, CompletionKind::Succeeded) {
        fanout.failed_activity_errors.remove(&index);
    } else {
        let error_message = error_message.expect("error message is present for failures");
        if !fanout_uses_counter_state(fanout) && !fanout_materializes_settled_results(fanout) {
            fanout.failed_activity_errors.insert(index, error_message);
        }
    }
}

fn fanout_slot_is_pending(fanout: &FanOutExecutionState, index: usize) -> bool {
    if index >= fanout_total_count(fanout) {
        return false;
    }
    if !fanout.pending_activity_ids.is_empty() {
        let activity_id = build_fanout_activity_id(&fanout.origin_state, index);
        return fanout.pending_activity_ids.iter().any(|pending_id| pending_id == &activity_id);
    }
    if !fanout.settlement_bitmap.is_empty() {
        return !fanout_bitmap_contains(fanout, index);
    }
    fanout.results.get(index).is_some_and(|slot| slot.is_none())
        && !fanout.failed_activity_errors.contains_key(&index)
}

fn fanout_pending_count(fanout: &FanOutExecutionState) -> usize {
    if fanout.pending_count > 0 {
        return fanout.pending_count;
    }
    if fanout_uses_counter_state(fanout) {
        return fanout_total_count(fanout).saturating_sub(
            fanout.settlement_bitmap.iter().map(|word| word.count_ones() as usize).sum::<usize>(),
        );
    }
    if !fanout.pending_activity_ids.is_empty() {
        return fanout.pending_activity_ids.len();
    }
    fanout
        .results
        .iter()
        .enumerate()
        .filter(|(index, _)| fanout_slot_is_pending(fanout, *index))
        .count()
}

fn reduce_fanout_terminal_payload(fanout: &FanOutExecutionState) -> Value {
    match fanout_reducer(fanout) {
        "collect_results" => Value::Array(
            fanout.results.iter().cloned().map(|value| value.unwrap_or(Value::Null)).collect(),
        ),
        "collect_settled_results" => Value::Array(
            fanout.results.iter().cloned().map(|value| value.unwrap_or(Value::Null)).collect(),
        ),
        reducer => {
            let (succeeded, failed, cancelled) = fanout_summary_counts(fanout);
            let terminal_status = if failed > 0 {
                "failed"
            } else if cancelled > 0 {
                "cancelled"
            } else {
                "completed"
            };
            let mut summary = serde_json::json!({
                "status": if fanout_reducer_settles(fanout) { "settled" } else { terminal_status },
                "terminalStatus": terminal_status,
                "totalItems": fanout_total_count(fanout),
                "succeededItems": succeeded,
                "failedItems": failed,
                "cancelledItems": cancelled,
            });
            if reducer == "count" {
                summary["count"] = Value::from(
                    u64::try_from(succeeded + failed + cancelled)
                        .expect("fanout completion count exceeds u64"),
                );
            } else if terminal_status == "completed"
                && let Some(field_name) = bulk_reducer_summary_field_name(Some(reducer))
            {
                let values =
                    fanout.results.iter().filter_map(|value| value.clone()).collect::<Vec<_>>();
                if let Some(reduced) = bulk_reducer_reduce_values(Some(reducer), &values)
                    .context("failed to reduce fanout terminal payload")
                    .expect("fanout reducer outputs are valid")
                {
                    summary[field_name] = reduced;
                }
            }
            summary
        }
    }
}

fn empty_fanout_terminal_payload(reducer: Option<&str>) -> Value {
    let reducer = reducer.unwrap_or("collect_results");
    if matches!(reducer, "collect_results" | "collect_settled_results") {
        return Value::Array(Vec::new());
    }
    let mut summary = serde_json::json!({
        "status": "settled",
        "terminalStatus": "completed",
        "totalItems": 0,
        "succeededItems": 0,
        "failedItems": 0,
        "cancelledItems": 0,
    });
    if reducer == "count" {
        summary["count"] = Value::from(0_u64);
    } else if let Some(field_name) = bulk_reducer_summary_field_name(Some(reducer)) {
        summary[field_name] = match reducer {
            BULK_REDUCER_SUM => Value::from(0.0),
            BULK_REDUCER_MIN | BULK_REDUCER_MAX | BULK_REDUCER_AVG => Value::Null,
            _ => Value::Null,
        };
    }
    summary
}

fn fanout_terminal_update(
    event: &EventEnvelope<WorkflowEvent>,
) -> Option<(&str, Value, CompletionKind)> {
    match &event.payload {
        WorkflowEvent::ActivityTaskCompleted { activity_id, output, .. } => {
            Some((activity_id.as_str(), output.clone(), CompletionKind::Succeeded))
        }
        WorkflowEvent::ActivityTaskFailed { activity_id, error, .. } => {
            Some((activity_id.as_str(), Value::String(error.clone()), CompletionKind::Failed))
        }
        WorkflowEvent::ActivityTaskCancelled { activity_id, reason, .. } => {
            Some((activity_id.as_str(), Value::String(reason.clone()), CompletionKind::Cancelled))
        }
        _ => None,
    }
}

fn bulk_reducer_settles(bulk: &BulkActivityExecutionState) -> bool {
    matches!(bulk_reducer(bulk), "all_settled" | "count")
}

fn reduce_bulk_terminal_payload(payload: &Value, bulk: &BulkActivityExecutionState) -> Value {
    let batch_id =
        payload.get("batchId").and_then(Value::as_str).unwrap_or(&bulk.batch_id).to_owned();
    let terminal_status =
        payload.get("status").and_then(Value::as_str).unwrap_or("completed").to_owned();
    let total_items =
        payload.get("totalItems").and_then(Value::as_u64).unwrap_or(u64::from(bulk.total_items))
            as u32;
    let succeeded_items = payload.get("succeededItems").and_then(Value::as_u64).unwrap_or(0) as u32;
    let failed_items = payload.get("failedItems").and_then(Value::as_u64).unwrap_or(0) as u32;
    let cancelled_items = payload.get("cancelledItems").and_then(Value::as_u64).unwrap_or(0) as u32;
    let chunk_count = payload.get("chunkCount").and_then(Value::as_u64).unwrap_or(0) as u32;

    let mut summary = serde_json::json!({
        "batchId": batch_id,
        "status": if bulk_reducer_settles(bulk) { "settled" } else { terminal_status.as_str() },
        "terminalStatus": terminal_status,
        "totalItems": total_items,
        "succeededItems": succeeded_items,
        "failedItems": failed_items,
        "cancelledItems": cancelled_items,
        "chunkCount": chunk_count,
    });

    if let Some(message) = payload.get("message").cloned() {
        summary["message"] = message;
    }

    match bulk_reducer(bulk) {
        "collect_results" => {
            summary["resultHandle"] = serde_json::json!({ "batchId": bulk.batch_id });
        }
        "count" => {
            summary["count"] = Value::from(
                u64::from(succeeded_items) + u64::from(failed_items) + u64::from(cancelled_items),
            );
        }
        reducer => {
            if terminal_status == "completed"
                && let Some(field_name) = bulk_reducer_summary_field_name(Some(reducer))
                && let Some(reduced) = payload.get("reducerOutput")
            {
                summary[field_name] = reduced.clone();
            }
        }
    }

    summary
}

fn validate_bulk_items(
    state: &str,
    items: &[Value],
    chunk_size: usize,
) -> Result<(), CompiledWorkflowError> {
    if items.len() > MAX_BULK_ITEMS_PER_BATCH {
        return Err(CompiledWorkflowError::BulkItemLimitExceeded {
            state: state.to_owned(),
            count: items.len(),
            max: MAX_BULK_ITEMS_PER_BATCH,
        });
    }

    let mut total_bytes = 0usize;
    let mut current_chunk_bytes = 0usize;
    for (index, item) in items.iter().enumerate() {
        let item_bytes = serde_json::to_vec(item).map(|bytes| bytes.len()).unwrap_or_default();
        if item_bytes > MAX_BULK_ITEM_BYTES {
            return Err(CompiledWorkflowError::BulkItemTooLarge {
                state: state.to_owned(),
                index,
                bytes: item_bytes,
                max: MAX_BULK_ITEM_BYTES,
            });
        }
        total_bytes = total_bytes.saturating_add(item_bytes);
        if total_bytes > MAX_BULK_TOTAL_INPUT_BYTES {
            return Err(CompiledWorkflowError::BulkInputTooLarge {
                state: state.to_owned(),
                bytes: total_bytes,
                max: MAX_BULK_TOTAL_INPUT_BYTES,
            });
        }

        if index % chunk_size == 0 {
            current_chunk_bytes = 0;
        }
        current_chunk_bytes = current_chunk_bytes.saturating_add(item_bytes);
        if current_chunk_bytes > MAX_BULK_CHUNK_INPUT_BYTES {
            return Err(CompiledWorkflowError::BulkChunkTooLarge {
                state: state.to_owned(),
                chunk_index: index / chunk_size,
                bytes: current_chunk_bytes,
                max: MAX_BULK_CHUNK_INPUT_BYTES,
            });
        }
    }

    Ok(())
}

fn build_fanout_activity_id(state_id: &str, index: usize) -> String {
    format!("{state_id}{FANOUT_ACTIVITY_SEPARATOR}{index}")
}

fn parse_fanout_activity_id(activity_id: &str) -> Option<(String, usize)> {
    let (state_id, index) = activity_id.rsplit_once(FANOUT_ACTIVITY_SEPARATOR)?;
    Some((state_id.to_owned(), index.parse::<usize>().ok()?))
}

fn decode_fanout_state(value: &Value) -> Option<FanOutExecutionState> {
    serde_json::from_value(value.clone()).ok()
}

fn decode_bulk_state(value: &Value) -> Option<BulkActivityExecutionState> {
    serde_json::from_value(value.clone()).ok()
}

fn items_len_to_u32(len: usize) -> Result<u32, CompiledWorkflowError> {
    u32::try_from(len).map_err(|_| CompiledWorkflowError::BulkItemsTooLarge { count: len })
}

pub fn evaluate_expression(
    expression: &Expression,
    execution_state: &mut ArtifactExecutionState,
    helpers: &BTreeMap<String, HelperFunction>,
) -> Result<Value, CompiledWorkflowError> {
    let bindings = &execution_state.bindings;
    match expression {
        Expression::Literal { value } => Ok(value.clone()),
        Expression::Identifier { name } => Ok(bindings.get(name).cloned().unwrap_or(Value::Null)),
        Expression::Member { object, property } => {
            match evaluate_expression(object, execution_state, helpers)? {
                Value::Array(items) if property == "length" => {
                    Ok(Value::Number(Number::from(items.len())))
                }
                Value::Object(map) => Ok(map.get(property).cloned().unwrap_or(Value::Null)),
                _ => Ok(Value::Null),
            }
        }
        Expression::Index { object, index } => {
            let object = evaluate_expression(object, execution_state, helpers)?;
            let index = evaluate_expression(index, execution_state, helpers)?;
            match (object, index) {
                (Value::Array(items), Value::Number(index)) => {
                    let index = index
                        .as_u64()
                        .map(|value| value as usize)
                        .or_else(|| {
                            index.as_f64().and_then(|value| {
                                if value.is_finite() && value >= 0.0 {
                                    Some(value.trunc() as usize)
                                } else {
                                    None
                                }
                            })
                        })
                        .unwrap_or_default();
                    Ok(items.get(index).cloned().unwrap_or(Value::Null))
                }
                (Value::Object(map), Value::String(index)) => {
                    Ok(map.get(&index).cloned().unwrap_or(Value::Null))
                }
                _ => Ok(Value::Null),
            }
        }
        Expression::Binary { op, left, right } => {
            let left = evaluate_expression(left, execution_state, helpers)?;
            let right = evaluate_expression(right, execution_state, helpers)?;
            evaluate_binary(op, left, right)
        }
        Expression::Unary { op, expr } => {
            let value = evaluate_expression(expr, execution_state, helpers)?;
            match op {
                UnaryOp::Not => Ok(Value::Bool(!truthy(&value))),
                UnaryOp::Negate => Ok(number_value(-numeric(&value)?)),
            }
        }
        Expression::Logical { op, left, right } => {
            let left = evaluate_expression(left, execution_state, helpers)?;
            match op {
                LogicalOp::And if !truthy(&left) => Ok(left),
                LogicalOp::Or if truthy(&left) => Ok(left),
                LogicalOp::Coalesce if !left.is_null() => Ok(left),
                _ => evaluate_expression(right, execution_state, helpers),
            }
        }
        Expression::Conditional { condition, then_expr, else_expr } => {
            if truthy(&evaluate_expression(condition, execution_state, helpers)?) {
                evaluate_expression(then_expr, execution_state, helpers)
            } else {
                evaluate_expression(else_expr, execution_state, helpers)
            }
        }
        Expression::Array { items } => items
            .iter()
            .map(|item| evaluate_expression(item, execution_state, helpers))
            .collect::<Result<Vec<_>, _>>()
            .map(Value::Array),
        Expression::ArrayFind { array, item_name, predicate } => {
            let array = evaluate_expression(array, execution_state, helpers)?;
            let Value::Array(items) = array else {
                return Ok(Value::Null);
            };
            let previous = execution_state.bindings.get(item_name).cloned();
            for item in items {
                execution_state.bindings.insert(item_name.clone(), item.clone());
                if truthy(&evaluate_expression(predicate, execution_state, helpers)?) {
                    match previous {
                        Some(ref value) => {
                            execution_state.bindings.insert(item_name.clone(), value.clone());
                        }
                        None => {
                            execution_state.bindings.remove(item_name);
                        }
                    }
                    return Ok(item);
                }
            }
            match previous {
                Some(value) => {
                    execution_state.bindings.insert(item_name.clone(), value);
                }
                None => {
                    execution_state.bindings.remove(item_name);
                }
            }
            Ok(Value::Null)
        }
        Expression::ArrayMap { array, item_name, expr } => {
            let array = evaluate_expression(array, execution_state, helpers)?;
            let Value::Array(items) = array else {
                return Ok(Value::Array(Vec::new()));
            };
            let previous = execution_state.bindings.get(item_name).cloned();
            let mut mapped = Vec::with_capacity(items.len());
            for item in items {
                execution_state.bindings.insert(item_name.clone(), item);
                mapped.push(evaluate_expression(expr, execution_state, helpers)?);
            }
            match previous {
                Some(value) => {
                    execution_state.bindings.insert(item_name.clone(), value);
                }
                None => {
                    execution_state.bindings.remove(item_name);
                }
            }
            Ok(Value::Array(mapped))
        }
        Expression::ArrayReduce { array, accumulator_name, item_name, initial, expr } => {
            let array = evaluate_expression(array, execution_state, helpers)?;
            let Value::Array(items) = array else {
                return evaluate_expression(initial, execution_state, helpers);
            };
            let previous_accumulator = execution_state.bindings.get(accumulator_name).cloned();
            let previous_item = execution_state.bindings.get(item_name).cloned();
            let mut accumulator = evaluate_expression(initial, execution_state, helpers)?;
            for item in items {
                execution_state.bindings.insert(accumulator_name.clone(), accumulator);
                execution_state.bindings.insert(item_name.clone(), item);
                accumulator = evaluate_expression(expr, execution_state, helpers)?;
            }
            match previous_accumulator {
                Some(value) => {
                    execution_state.bindings.insert(accumulator_name.clone(), value);
                }
                None => {
                    execution_state.bindings.remove(accumulator_name);
                }
            }
            match previous_item {
                Some(value) => {
                    execution_state.bindings.insert(item_name.clone(), value);
                }
                None => {
                    execution_state.bindings.remove(item_name);
                }
            }
            Ok(accumulator)
        }
        Expression::Object { fields } => {
            let mut object = Map::new();
            for (key, value) in fields {
                object.insert(key.clone(), evaluate_expression(value, execution_state, helpers)?);
            }
            Ok(Value::Object(object))
        }
        Expression::ObjectMerge { left, right } => {
            let mut merged = match evaluate_expression(left, execution_state, helpers)? {
                Value::Object(map) => map,
                _ => Map::new(),
            };
            if let Value::Object(right_map) = evaluate_expression(right, execution_state, helpers)?
            {
                for (key, value) in right_map {
                    merged.insert(key, value);
                }
            }
            Ok(Value::Object(merged))
        }
        Expression::Call { callee, args } => {
            if callee == "__temporal_is_cancellation" {
                if args.len() != 1 {
                    return Err(CompiledWorkflowError::HelperArityMismatch {
                        helper: callee.clone(),
                        expected: 1,
                        received: args.len(),
                    });
                }
                let value = evaluate_expression(&args[0], execution_state, helpers)?;
                return Ok(Value::Bool(is_cancellation_value(&value)));
            }
            if callee == "__builtin_object_keys" {
                if args.len() != 1 {
                    return Err(CompiledWorkflowError::HelperArityMismatch {
                        helper: callee.clone(),
                        expected: 1,
                        received: args.len(),
                    });
                }
                return Ok(match evaluate_expression(&args[0], execution_state, helpers)? {
                    Value::Object(map) => {
                        Value::Array(map.keys().cloned().map(Value::String).collect::<Vec<_>>())
                    }
                    _ => Value::Array(Vec::new()),
                });
            }
            if callee == "__builtin_array_join" {
                if args.len() != 2 {
                    return Err(CompiledWorkflowError::HelperArityMismatch {
                        helper: callee.clone(),
                        expected: 2,
                        received: args.len(),
                    });
                }
                let array = evaluate_expression(&args[0], execution_state, helpers)?;
                let separator = evaluate_expression(&args[1], execution_state, helpers)?;
                let separator = separator
                    .as_str()
                    .map(str::to_owned)
                    .unwrap_or_else(|| join_stringify_value(&separator));
                let Value::Array(items) = array else {
                    return Ok(Value::String(String::new()));
                };
                let joined = items.iter().map(join_fragment).collect::<Vec<_>>().join(&separator);
                return Ok(Value::String(joined));
            }
            if callee == "__builtin_array_map_number" {
                if args.len() != 1 {
                    return Err(CompiledWorkflowError::HelperArityMismatch {
                        helper: callee.clone(),
                        expected: 1,
                        received: args.len(),
                    });
                }
                let array = evaluate_expression(&args[0], execution_state, helpers)?;
                let Value::Array(items) = array else {
                    return Ok(Value::Array(Vec::new()));
                };
                let mapped = items
                    .into_iter()
                    .map(|item| number_value(numeric(&item).unwrap_or(0.0)))
                    .collect::<Vec<_>>();
                return Ok(Value::Array(mapped));
            }
            if callee == "__builtin_string" {
                if args.len() != 1 {
                    return Err(CompiledWorkflowError::HelperArityMismatch {
                        helper: callee.clone(),
                        expected: 1,
                        received: args.len(),
                    });
                }
                let value = evaluate_expression(&args[0], execution_state, helpers)?;
                return Ok(Value::String(join_stringify_value(&value)));
            }
            if callee == "__builtin_math_floor" {
                if args.len() != 1 {
                    return Err(CompiledWorkflowError::HelperArityMismatch {
                        helper: callee.clone(),
                        expected: 1,
                        received: args.len(),
                    });
                }
                let value = evaluate_expression(&args[0], execution_state, helpers)?;
                return Ok(number_value(numeric(&value)?.floor()));
            }
            if callee == "__builtin_math_min" {
                if args.len() != 2 {
                    return Err(CompiledWorkflowError::HelperArityMismatch {
                        helper: callee.clone(),
                        expected: 2,
                        received: args.len(),
                    });
                }
                let left = evaluate_expression(&args[0], execution_state, helpers)?;
                let right = evaluate_expression(&args[1], execution_state, helpers)?;
                return Ok(number_value(numeric(&left)?.min(numeric(&right)?)));
            }
            if callee == "__builtin_array_fill" {
                if args.len() != 2 {
                    return Err(CompiledWorkflowError::HelperArityMismatch {
                        helper: callee.clone(),
                        expected: 2,
                        received: args.len(),
                    });
                }
                let length = evaluate_expression(&args[0], execution_state, helpers)?;
                let fill = evaluate_expression(&args[1], execution_state, helpers)?;
                let length = numeric(&length)?.max(0.0).trunc() as usize;
                return Ok(Value::Array(vec![fill; length]));
            }
            if callee == "__builtin_array_append" {
                if args.len() != 2 {
                    return Err(CompiledWorkflowError::HelperArityMismatch {
                        helper: callee.clone(),
                        expected: 2,
                        received: args.len(),
                    });
                }
                let array = evaluate_expression(&args[0], execution_state, helpers)?;
                let value = evaluate_expression(&args[1], execution_state, helpers)?;
                let Value::Array(mut items) = array else {
                    return Ok(Value::Array(vec![value]));
                };
                items.push(value);
                return Ok(Value::Array(items));
            }
            if callee == "__builtin_array_shift_head" {
                if args.len() != 1 {
                    return Err(CompiledWorkflowError::HelperArityMismatch {
                        helper: callee.clone(),
                        expected: 1,
                        received: args.len(),
                    });
                }
                let array = evaluate_expression(&args[0], execution_state, helpers)?;
                let Value::Array(items) = array else {
                    return Ok(Value::Null);
                };
                return Ok(items.into_iter().next().unwrap_or(Value::Null));
            }
            if callee == "__builtin_array_shift_tail" {
                if args.len() != 1 {
                    return Err(CompiledWorkflowError::HelperArityMismatch {
                        helper: callee.clone(),
                        expected: 1,
                        received: args.len(),
                    });
                }
                let array = evaluate_expression(&args[0], execution_state, helpers)?;
                let Value::Array(items) = array else {
                    return Ok(Value::Array(Vec::new()));
                };
                return Ok(Value::Array(items.into_iter().skip(1).collect()));
            }
            if callee == "__builtin_object_omit" {
                if args.len() != 2 {
                    return Err(CompiledWorkflowError::HelperArityMismatch {
                        helper: callee.clone(),
                        expected: 2,
                        received: args.len(),
                    });
                }
                let object = evaluate_expression(&args[0], execution_state, helpers)?;
                let key = evaluate_expression(&args[1], execution_state, helpers)?;
                let key =
                    key.as_str().map(str::to_owned).unwrap_or_else(|| join_stringify_value(&key));
                let Value::Object(mut map) = object else {
                    return Ok(Value::Object(Map::new()));
                };
                map.remove(&key);
                return Ok(Value::Object(map));
            }
            if callee == "__builtin_object_set" {
                if args.len() != 3 {
                    return Err(CompiledWorkflowError::HelperArityMismatch {
                        helper: callee.clone(),
                        expected: 3,
                        received: args.len(),
                    });
                }
                let object = evaluate_expression(&args[0], execution_state, helpers)?;
                let key = evaluate_expression(&args[1], execution_state, helpers)?;
                let value = evaluate_expression(&args[2], execution_state, helpers)?;
                let key =
                    key.as_str().map(str::to_owned).unwrap_or_else(|| join_stringify_value(&key));
                let Value::Object(mut map) = object else {
                    let mut map = Map::new();
                    map.insert(key, value);
                    return Ok(Value::Object(map));
                };
                map.insert(key, value);
                return Ok(Value::Object(map));
            }
            if callee == "__builtin_array_sort_default"
                || callee == "__builtin_array_sort_numeric_asc"
            {
                if args.len() != 1 {
                    return Err(CompiledWorkflowError::HelperArityMismatch {
                        helper: callee.clone(),
                        expected: 1,
                        received: args.len(),
                    });
                }
                let array = evaluate_expression(&args[0], execution_state, helpers)?;
                let Value::Array(mut items) = array else {
                    return Ok(Value::Array(Vec::new()));
                };
                if callee == "__builtin_array_sort_numeric_asc" {
                    items.sort_by(|left, right| {
                        let left = numeric(left).unwrap_or(0.0);
                        let right = numeric(right).unwrap_or(0.0);
                        left.partial_cmp(&right).unwrap_or(std::cmp::Ordering::Equal)
                    });
                } else {
                    items.sort_by_key(join_fragment);
                }
                return Ok(Value::Array(items));
            }
            let helper = helpers
                .get(callee)
                .ok_or_else(|| CompiledWorkflowError::UnknownHelper(callee.clone()))?;
            if helper.params.len() != args.len() {
                return Err(CompiledWorkflowError::HelperArityMismatch {
                    helper: callee.clone(),
                    expected: helper.params.len(),
                    received: args.len(),
                });
            }
            let mut scoped = bindings.clone();
            for (param, arg) in helper.params.iter().zip(args) {
                scoped.insert(param.clone(), evaluate_expression(arg, execution_state, helpers)?);
            }
            let mut scoped_state = execution_state.clone();
            scoped_state.bindings = scoped;
            execute_helper_statements(&helper.statements, &mut scoped_state, helpers)?;
            let result = evaluate_expression(&helper.body, &mut scoped_state, helpers)?;
            execution_state.markers = scoped_state.markers;
            execution_state.version_markers = scoped_state.version_markers;
            execution_state.pending_markers = scoped_state.pending_markers;
            execution_state.pending_version_markers = scoped_state.pending_version_markers;
            Ok(result)
        }
        Expression::WorkflowInfo => {
            Ok(execution_state.workflow_info.clone().unwrap_or(Value::Null))
        }
        Expression::SideEffect { marker_id, expr } => {
            if let Some(existing) = execution_state.markers.get(marker_id) {
                return Ok(existing.clone());
            }

            let value = evaluate_expression(expr, execution_state, helpers)?;
            execution_state.markers.insert(marker_id.clone(), value.clone());
            execution_state.pending_markers.push((marker_id.clone(), value.clone()));
            Ok(value)
        }
        Expression::Version { change_id, min_supported, max_supported } => {
            if min_supported > max_supported {
                return Err(CompiledWorkflowError::InvalidVersionRange {
                    change_id: change_id.clone(),
                    min_supported: *min_supported,
                    max_supported: *max_supported,
                });
            }
            if let Some(existing) = execution_state.version_markers.get(change_id) {
                return Ok(number_value(*existing as f64));
            }

            let version = *max_supported;
            execution_state.version_markers.insert(change_id.clone(), version);
            execution_state.pending_version_markers.push((change_id.clone(), version));
            Ok(number_value(version as f64))
        }
        Expression::Now => Ok(number_value(
            execution_state
                .turn_context
                .as_ref()
                .map(|context| context.occurred_at.timestamp_millis() as f64)
                .unwrap_or_default(),
        )),
        Expression::Uuid { scope } => {
            let turn_context = execution_state.turn_context.as_ref().ok_or_else(|| {
                CompiledWorkflowError::MissingTurnContext("ctx.uuid()".to_owned())
            })?;
            let value = uuid::Uuid::new_v5(
                &uuid::Uuid::NAMESPACE_URL,
                format!("{}:{scope}", turn_context.event_id).as_bytes(),
            );
            Ok(Value::String(value.to_string()))
        }
    }
}

fn join_fragment(value: &Value) -> String {
    match value {
        Value::Null => String::new(),
        Value::String(text) => text.clone(),
        Value::Bool(flag) => flag.to_string(),
        Value::Number(number) => number.to_string(),
        Value::Array(_) | Value::Object(_) => join_stringify_value(value),
    }
}

fn join_stringify_value(value: &Value) -> String {
    match value {
        Value::String(text) => text.clone(),
        other => other.to_string(),
    }
}

fn evaluate_timer_delay(
    timer_ref: &str,
    timer_expr: Option<&Expression>,
    state: &str,
    execution_state: &mut ArtifactExecutionState,
    helpers: &BTreeMap<String, HelperFunction>,
) -> Result<chrono::TimeDelta, CompiledWorkflowError> {
    let timer_text = if let Some(timer_expr) = timer_expr {
        let value = evaluate_expression(timer_expr, execution_state, helpers)?;
        match value {
            Value::Number(number) => {
                if let Some(ms) = number.as_i64() {
                    format!("{ms}ms")
                } else if let Some(ms) = number.as_u64() {
                    format!("{ms}ms")
                } else {
                    return Err(CompiledWorkflowError::InvalidTimer {
                        state: state.to_owned(),
                        timer_ref: number.to_string(),
                        details: "numeric timer value must be an integer millisecond duration"
                            .to_owned(),
                    });
                }
            }
            Value::String(text) => text,
            other => {
                return Err(CompiledWorkflowError::InvalidTimer {
                    state: state.to_owned(),
                    timer_ref: other.to_string(),
                    details: "timer expression must evaluate to a string or integer milliseconds"
                        .to_owned(),
                });
            }
        }
    } else {
        timer_ref.to_owned()
    };
    crate::parse_timer_ref(&timer_text).map_err(|source| CompiledWorkflowError::InvalidTimer {
        state: state.to_owned(),
        timer_ref: timer_text,
        details: source.to_string(),
    })
}

fn execute_helper_statements(
    statements: &[HelperStatement],
    execution_state: &mut ArtifactExecutionState,
    helpers: &BTreeMap<String, HelperFunction>,
) -> Result<(), CompiledWorkflowError> {
    for statement in statements {
        match statement {
            HelperStatement::Assign { target, expr } => {
                let value = evaluate_expression(expr, execution_state, helpers)?;
                execution_state.bindings.insert(target.clone(), value);
            }
            HelperStatement::AssignIndex { target, index, expr } => {
                let current = execution_state.bindings.get(target).cloned().unwrap_or(Value::Null);
                let index = evaluate_expression(index, execution_state, helpers)?;
                let value = evaluate_expression(expr, execution_state, helpers)?;
                let updated = assign_index_value(current, index, value);
                execution_state.bindings.insert(target.clone(), updated);
            }
            HelperStatement::ForRange { index_var, start, end, body } => {
                let start =
                    numeric(&evaluate_expression(start, execution_state, helpers)?)?.trunc() as i64;
                let end =
                    numeric(&evaluate_expression(end, execution_state, helpers)?)?.trunc() as i64;
                let previous = execution_state.bindings.get(index_var).cloned();
                for current in start..end {
                    execution_state
                        .bindings
                        .insert(index_var.clone(), Value::Number(Number::from(current)));
                    execute_helper_statements(body, execution_state, helpers)?;
                }
                if let Some(previous) = previous {
                    execution_state.bindings.insert(index_var.clone(), previous);
                } else {
                    execution_state.bindings.remove(index_var);
                }
            }
        }
    }
    Ok(())
}

fn assign_index_value(current: Value, index: Value, value: Value) -> Value {
    match (current, index) {
        (Value::Array(mut items), Value::Number(index)) => {
            let index = index.as_u64().map(|value| value as usize).unwrap_or_default();
            if index >= items.len() {
                items.resize(index + 1, Value::Null);
            }
            items[index] = value;
            Value::Array(items)
        }
        (Value::Object(mut map), Value::String(key)) => {
            map.insert(key, value);
            Value::Object(map)
        }
        (Value::Null, Value::String(key)) => {
            let mut map = Map::new();
            map.insert(key, value);
            Value::Object(map)
        }
        (Value::Null, Value::Number(index)) => {
            let index = index.as_u64().map(|value| value as usize).unwrap_or_default();
            let mut items = vec![Value::Null; index + 1];
            items[index] = value;
            Value::Array(items)
        }
        (other, _) => other,
    }
}

fn is_cancellation_value(value: &Value) -> bool {
    match value {
        Value::String(message) => message.to_ascii_lowercase().contains("cancel"),
        Value::Object(object) => {
            let field_matches = |field: &str, expected: &[&str]| {
                object.get(field).and_then(Value::as_str).is_some_and(|value| {
                    let lower = value.to_ascii_lowercase();
                    expected.iter().any(|candidate| lower == *candidate)
                })
            };
            object.get("cancelled").and_then(Value::as_bool).unwrap_or(false)
                || field_matches("status", &["cancelled", "canceled"])
                || field_matches("terminalStatus", &["cancelled", "canceled"])
                || field_matches(
                    "type",
                    &["cancelledfailure", "canceledfailure", "cancellationerror", "cancellederror"],
                )
                || field_matches(
                    "name",
                    &["cancelledfailure", "canceledfailure", "cancellationerror", "cancellederror"],
                )
                || field_matches(
                    "errorType",
                    &["cancelledfailure", "canceledfailure", "cancellationerror", "cancellederror"],
                )
                || object
                    .get("message")
                    .or_else(|| object.get("reason"))
                    .is_some_and(is_cancellation_value)
        }
        _ => false,
    }
}

fn emit_pending_markers(
    emissions: &mut Vec<ExecutionEmission>,
    execution_state: &mut ArtifactExecutionState,
    state: &str,
) {
    for (marker_id, value) in execution_state.pending_markers.drain(..) {
        emissions.push(ExecutionEmission {
            event: WorkflowEvent::MarkerRecorded { marker_id, value },
            state: Some(state.to_owned()),
        });
    }
    for (change_id, version) in execution_state.pending_version_markers.drain(..) {
        emissions.push(ExecutionEmission {
            event: WorkflowEvent::VersionMarkerRecorded { change_id, version },
            state: Some(state.to_owned()),
        });
    }
}

fn evaluate_binary(
    op: &BinaryOp,
    left: Value,
    right: Value,
) -> Result<Value, CompiledWorkflowError> {
    match op {
        BinaryOp::Add => match (left, right) {
            (Value::String(left), Value::String(right)) => {
                Ok(Value::String(format!("{left}{right}")))
            }
            (Value::String(left), right) => {
                Ok(Value::String(format!("{left}{}", stringify_value(&right).unwrap_or_default())))
            }
            (left, Value::String(right)) => Ok(Value::String(format!(
                "{}{}",
                stringify_value(&left).unwrap_or_default(),
                right
            ))),
            (left, right) => Ok(number_value(numeric(&left)? + numeric(&right)?)),
        },
        BinaryOp::Subtract => Ok(number_value(numeric(&left)? - numeric(&right)?)),
        BinaryOp::Multiply => Ok(number_value(numeric(&left)? * numeric(&right)?)),
        BinaryOp::Divide => Ok(number_value(numeric(&left)? / numeric(&right)?)),
        BinaryOp::Remainder => Ok(number_value(numeric(&left)? % numeric(&right)?)),
        BinaryOp::In => Ok(Value::Bool(match (left, right) {
            (Value::String(key), Value::Object(map)) => map.contains_key(&key),
            (Value::Number(index), Value::Array(items)) => {
                index.as_u64().is_some_and(|index| (index as usize) < items.len())
            }
            _ => false,
        })),
        BinaryOp::Equal => Ok(Value::Bool(left == right)),
        BinaryOp::NotEqual => Ok(Value::Bool(left != right)),
        BinaryOp::LessThan => Ok(Value::Bool(numeric(&left)? < numeric(&right)?)),
        BinaryOp::LessThanOrEqual => Ok(Value::Bool(numeric(&left)? <= numeric(&right)?)),
        BinaryOp::GreaterThan => Ok(Value::Bool(numeric(&left)? > numeric(&right)?)),
        BinaryOp::GreaterThanOrEqual => Ok(Value::Bool(numeric(&left)? >= numeric(&right)?)),
    }
}

fn truthy(value: &Value) -> bool {
    match value {
        Value::Null => false,
        Value::Bool(value) => *value,
        Value::Number(number) => number.as_f64().unwrap_or_default() != 0.0,
        Value::String(string) => !string.is_empty(),
        Value::Array(items) => !items.is_empty(),
        Value::Object(map) => !map.is_empty(),
    }
}

fn numeric(value: &Value) -> Result<f64, CompiledWorkflowError> {
    match value {
        Value::Number(number) => Ok(number.as_f64().unwrap_or_default()),
        Value::Bool(value) => Ok(if *value { 1.0 } else { 0.0 }),
        Value::Null => Ok(0.0),
        Value::String(value) => value
            .parse::<f64>()
            .map_err(|_| CompiledWorkflowError::InvalidNumericValue(value.clone())),
        other => Err(CompiledWorkflowError::InvalidNumericValue(other.to_string())),
    }
}

fn number_value(value: f64) -> Value {
    if value.is_finite() && value.fract() == 0.0 {
        if value >= 0.0 && value <= u64::MAX as f64 {
            return Value::Number(Number::from(value as u64));
        }
        if value >= i64::MIN as f64 && value <= i64::MAX as f64 {
            return Value::Number(Number::from(value as i64));
        }
    }

    Value::Number(Number::from_f64(value).unwrap_or_else(|| Number::from(0)))
}

fn stringify_value(value: &Value) -> Option<String> {
    match value {
        Value::Null => None,
        Value::String(value) => Some(value.clone()),
        other => Some(other.to_string()),
    }
}

fn canonicalize_value(value: Value) -> Value {
    match value {
        Value::Array(items) => Value::Array(items.into_iter().map(canonicalize_value).collect()),
        Value::Object(map) => {
            let canonical = map
                .into_iter()
                .map(|(key, value)| (key, canonicalize_value(value)))
                .collect::<BTreeMap<_, _>>();
            Value::Object(canonical.into_iter().collect())
        }
        other => other,
    }
}

#[derive(Debug, Error, PartialEq)]
pub enum CompiledWorkflowError {
    #[error("compiled artifact is missing an initial state")]
    MissingInitialState,
    #[error("compiled workflow references unknown state {0}")]
    UnknownState(String),
    #[error("compiled workflow state {state} references missing next state {next}")]
    UnknownTransition { state: String, next: String },
    #[error("compiled update handler {0} is missing an initial state")]
    UnknownUpdateInitialState(String),
    #[error("compiled update handler {update} state {state} references missing next state {next}")]
    UnknownUpdateTransition { update: String, state: String, next: String },
    #[error("compiled signal handler {0} is missing an initial state")]
    UnknownSignalInitialState(String),
    #[error("compiled signal handler {signal} state {state} references missing next state {next}")]
    UnknownSignalTransition { signal: String, state: String, next: String },
    #[error("compiled artifact hash does not match its contents")]
    ArtifactHashMismatch,
    #[error("compiled workflow loop detected while executing state {0}")]
    LoopDetected(String),
    #[error("compiled workflow state {state} has invalid timer_ref {timer_ref}: {details}")]
    InvalidTimer { state: String, timer_ref: String, details: String },
    #[error("compiled workflow state {0} is not waiting on a signal")]
    NotWaitingOnSignal(String),
    #[error("unexpected signal received, expected {expected}, received {received}")]
    UnexpectedSignal { expected: String, received: String },
    #[error("compiled workflow state {0} is not waiting on a timer")]
    NotWaitingOnTimer(String),
    #[error("unexpected timer fired, expected {expected}, received {received}")]
    UnexpectedTimer { expected: String, received: String },
    #[error("compiled workflow state {0} is not waiting on a step")]
    NotWaitingOnStep(String),
    #[error("unexpected step completion, expected {expected}, received {received}")]
    UnexpectedStep { expected: String, received: String },
    #[error("compiled workflow state {0} is not waiting on a fan-out join")]
    NotWaitingOnFanOut(String),
    #[error("compiled workflow state {state} evaluated fan-out items to a non-array value")]
    InvalidFanOutItems { state: String },
    #[error("compiled workflow state {state} is missing fan-out binding {binding}")]
    MissingFanOutBinding { state: String, binding: String },
    #[error("compiled workflow state {state} received unexpected fan-out activity {activity_id}")]
    UnexpectedFanOutActivity { state: String, activity_id: String },
    #[error("compiled workflow activity id {0} is not a valid fan-out member id")]
    InvalidFanOutActivityId(String),
    #[error("compiled workflow state {0} is not waiting on a bulk activity")]
    NotWaitingOnBulk(String),
    #[error("compiled workflow state {state} evaluated bulk items to a non-array value")]
    InvalidBulkItems { state: String },
    #[error("compiled workflow state {state} is missing bulk binding {binding}")]
    MissingBulkBinding { state: String, binding: String },
    #[error("unexpected bulk batch completion, expected {expected}, received {received}")]
    UnexpectedBulkBatch { expected: String, received: String },
    #[error("compiled workflow state {state} has invalid bulk retry delay: {details}")]
    InvalidBulkRetryDelay { state: String, details: String },
    #[error("compiled workflow bulk activity item count {count} exceeds u32")]
    BulkItemsTooLarge { count: usize },
    #[error(
        "compiled workflow state {state} exceeded bulk item count limit {max} with {count} items"
    )]
    BulkItemLimitExceeded { state: String, count: usize, max: usize },
    #[error(
        "compiled workflow state {state} bulk item {index} serialized to {bytes} bytes, over limit {max}"
    )]
    BulkItemTooLarge { state: String, index: usize, bytes: usize, max: usize },
    #[error(
        "compiled workflow state {state} bulk input serialized to {bytes} bytes, over limit {max}"
    )]
    BulkInputTooLarge { state: String, bytes: usize, max: usize },
    #[error(
        "compiled workflow state {state} bulk chunk {chunk_index} serialized to {bytes} bytes, over limit {max}"
    )]
    BulkChunkTooLarge { state: String, chunk_index: usize, bytes: usize, max: usize },
    #[error("compiled workflow state {0} is not waiting on a child")]
    NotWaitingOnChild(String),
    #[error("unexpected child completion, expected {expected}, received {received}")]
    UnexpectedChild { expected: String, received: String },
    #[error("compiled workflow step {0} is missing a continuation")]
    MissingContinuation(String),
    #[error("unknown query handler {0}")]
    UnknownQuery(String),
    #[error("unknown update handler {0}")]
    UnknownUpdate(String),
    #[error("unknown signal handler {0}")]
    UnknownSignalHandler(String),
    #[error("signal {0} cannot start while another signal is active")]
    SignalAlreadyActive(String),
    #[error("update {0} cannot start while another update is active")]
    UpdateAlreadyActive(String),
    #[error("unknown child workflow reference binding {0}")]
    UnknownChildReference(String),
    #[error("compiled workflow state {0} evaluated external workflow target to a non-string value")]
    InvalidExternalWorkflowTarget(String),
    #[error("unknown helper function {0}")]
    UnknownHelper(String),
    #[error("helper {helper} expected {expected} args, received {received}")]
    HelperArityMismatch { helper: String, expected: usize, received: usize },
    #[error(
        "version expression for change {change_id} has invalid range min={min_supported} max={max_supported}"
    )]
    InvalidVersionRange { change_id: String, min_supported: u32, max_supported: u32 },
    #[error("compiled workflow expression {0} requires execution turn context")]
    MissingTurnContext(String),
    #[error("value {0} cannot be treated as a number")]
    InvalidNumericValue(String),
}

#[cfg(test)]
mod tests {
    use chrono::Utc;
    use serde_json::json;

    use super::*;

    fn demo_artifact() -> CompiledWorkflowArtifact {
        let workflow = CompiledWorkflow {
            initial_state: "branch".to_owned(),
            states: BTreeMap::from([
                (
                    "branch".to_owned(),
                    CompiledStateNode::Choice {
                        condition: Expression::Identifier { name: "approved".to_owned() },
                        then_next: "step".to_owned(),
                        else_next: "fail".to_owned(),
                    },
                ),
                (
                    "step".to_owned(),
                    CompiledStateNode::Step {
                        handler: "core.echo".to_owned(),
                        input: Expression::Object {
                            fields: BTreeMap::from([(
                                "message".to_owned(),
                                Expression::Identifier { name: "input".to_owned() },
                            )]),
                        },
                        next: Some("done".to_owned()),
                        task_queue: None,
                        retry: None,
                        config: None,
                        schedule_to_start_timeout_ms: None,
                        start_to_close_timeout_ms: None,
                        heartbeat_timeout_ms: None,
                        output_var: Some("result".to_owned()),
                        on_error: Some(ErrorTransition {
                            next: "fail".to_owned(),
                            error_var: Some("err".to_owned()),
                        }),
                    },
                ),
                (
                    "done".to_owned(),
                    CompiledStateNode::Succeed {
                        output: Some(Expression::Identifier { name: "result".to_owned() }),
                    },
                ),
                (
                    "fail".to_owned(),
                    CompiledStateNode::Fail {
                        reason: Some(Expression::Literal { value: json!("nope") }),
                    },
                ),
            ]),
            params: Vec::new(),
            non_cancellable_states: BTreeSet::new(),
        };
        let mut artifact = CompiledWorkflowArtifact::new(
            "demo",
            1,
            "test",
            ArtifactEntrypoint { module: "workflow.ts".to_owned(), export: "workflow".to_owned() },
            workflow,
        );
        artifact.helpers.insert(
            "always".to_owned(),
            HelperFunction {
                params: vec!["value".to_owned()],
                statements: Vec::new(),
                body: Expression::Identifier { name: "value".to_owned() },
            },
        );
        artifact.artifact_hash = artifact.hash();
        artifact
    }

    fn update_child_artifact() -> CompiledWorkflowArtifact {
        let workflow = CompiledWorkflow {
            initial_state: "wait_signal".to_owned(),
            states: BTreeMap::from([(
                "wait_signal".to_owned(),
                CompiledStateNode::WaitForEvent {
                    event_type: "ready".to_owned(),
                    next: "done".to_owned(),
                    output_var: None,
                },
            )]),
            params: Vec::new(),
            non_cancellable_states: BTreeSet::new(),
        };
        let updates = BTreeMap::from([(
            "approve".to_owned(),
            CompiledUpdateHandler {
                arg_name: Some("args".to_owned()),
                initial_state: "start_child".to_owned(),
                states: BTreeMap::from([
                    (
                        "start_child".to_owned(),
                        CompiledStateNode::StartChild {
                            child_definition_id: "childWorkflow".to_owned(),
                            input: Expression::Identifier { name: "args".to_owned() },
                            next: "await_child".to_owned(),
                            handle_var: Some("child".to_owned()),
                            workflow_id: None,
                            task_queue: None,
                            parent_close_policy: ParentClosePolicy::RequestCancel,
                        },
                    ),
                    (
                        "await_child".to_owned(),
                        CompiledStateNode::WaitForChild {
                            child_ref_var: "child".to_owned(),
                            next: "finish".to_owned(),
                            output_var: Some("childResult".to_owned()),
                            on_error: None,
                        },
                    ),
                    (
                        "finish".to_owned(),
                        CompiledStateNode::Succeed {
                            output: Some(Expression::Identifier { name: "childResult".to_owned() }),
                        },
                    ),
                ]),
            },
        )]);
        let mut artifact = CompiledWorkflowArtifact::new(
            "demo-update-child",
            1,
            "test",
            ArtifactEntrypoint { module: "workflow.ts".to_owned(), export: "workflow".to_owned() },
            workflow,
        );
        artifact.updates = updates;
        artifact.artifact_hash = artifact.hash();
        artifact
    }

    fn signal_handler_artifact() -> CompiledWorkflowArtifact {
        let workflow = CompiledWorkflow {
            initial_state: "idle".to_owned(),
            states: BTreeMap::from([(
                "idle".to_owned(),
                CompiledStateNode::WaitForTimer {
                    timer_ref: "1s".to_owned(),
                    timer_expr: None,
                    next: "done".to_owned(),
                },
            )]),
            params: Vec::new(),
            non_cancellable_states: BTreeSet::new(),
        };
        let signals = BTreeMap::from([(
            "approved".to_owned(),
            CompiledSignalHandler {
                arg_name: Some("payload".to_owned()),
                initial_state: "assign".to_owned(),
                states: BTreeMap::from([
                    (
                        "assign".to_owned(),
                        CompiledStateNode::Assign {
                            actions: vec![Assignment {
                                target: "approvedValue".to_owned(),
                                expr: Expression::Identifier { name: "payload".to_owned() },
                            }],
                            next: "finish".to_owned(),
                        },
                    ),
                    ("finish".to_owned(), CompiledStateNode::Succeed { output: None }),
                ]),
            },
        )]);
        let mut artifact = CompiledWorkflowArtifact::new(
            "demo-signal-handler",
            1,
            "test",
            ArtifactEntrypoint { module: "workflow.ts".to_owned(), export: "workflow".to_owned() },
            workflow,
        );
        artifact.signals = signals;
        artifact.artifact_hash = artifact.hash();
        artifact
    }

    #[test]
    fn executes_dynamic_timer_expressions() {
        let workflow = CompiledWorkflow {
            initial_state: "idle".to_owned(),
            states: BTreeMap::from([
                (
                    "idle".to_owned(),
                    CompiledStateNode::WaitForTimer {
                        timer_ref: String::new(),
                        timer_expr: Some(Expression::Binary {
                            op: BinaryOp::Multiply,
                            left: Box::new(Expression::Identifier { name: "seconds".to_owned() }),
                            right: Box::new(Expression::Literal { value: json!(1000) }),
                        }),
                        next: "done".to_owned(),
                    },
                ),
                (
                    "done".to_owned(),
                    CompiledStateNode::Succeed {
                        output: Some(Expression::Literal { value: json!("done") }),
                    },
                ),
            ]),
            params: vec![CompiledWorkflowParam {
                name: "seconds".to_owned(),
                rest: false,
                default: None,
            }],
            non_cancellable_states: BTreeSet::new(),
        };
        let artifact = CompiledWorkflowArtifact::new(
            "dynamic-timer",
            1,
            "test",
            ArtifactEntrypoint { module: "workflow.ts".to_owned(), export: "workflow".to_owned() },
            workflow,
        );

        let plan = artifact
            .execute_trigger_with_turn(
                &json!([2]),
                CompiledWorkflowArtifact::synthetic_turn_context("dynamic-timer"),
            )
            .unwrap();

        assert!(matches!(
            plan.emissions.last(),
            Some(ExecutionEmission { event: WorkflowEvent::TimerScheduled { .. }, .. })
        ));
    }

    fn signal_condition_artifact() -> CompiledWorkflowArtifact {
        let workflow = CompiledWorkflow {
            initial_state: "wait_ready".to_owned(),
            states: BTreeMap::from([
                (
                    "wait_ready".to_owned(),
                    CompiledStateNode::WaitForCondition {
                        condition: Expression::Identifier { name: "ready".to_owned() },
                        next: "done".to_owned(),
                        timeout_ref: None,
                        timeout_next: None,
                    },
                ),
                (
                    "done".to_owned(),
                    CompiledStateNode::Succeed {
                        output: Some(Expression::Identifier { name: "payload".to_owned() }),
                    },
                ),
            ]),
            params: Vec::new(),
            non_cancellable_states: BTreeSet::new(),
        };
        let signals = BTreeMap::from([(
            "approved".to_owned(),
            CompiledSignalHandler {
                arg_name: Some("value".to_owned()),
                initial_state: "apply".to_owned(),
                states: BTreeMap::from([
                    (
                        "apply".to_owned(),
                        CompiledStateNode::Assign {
                            actions: vec![
                                Assignment {
                                    target: "ready".to_owned(),
                                    expr: Expression::Literal { value: Value::Bool(true) },
                                },
                                Assignment {
                                    target: "payload".to_owned(),
                                    expr: Expression::Identifier { name: "value".to_owned() },
                                },
                            ],
                            next: "finish".to_owned(),
                        },
                    ),
                    ("finish".to_owned(), CompiledStateNode::Succeed { output: None }),
                ]),
            },
        )]);
        let mut artifact = CompiledWorkflowArtifact::new(
            "demo-signal-condition",
            1,
            "test",
            ArtifactEntrypoint { module: "workflow.ts".to_owned(), export: "workflow".to_owned() },
            workflow,
        );
        artifact.signals = signals;
        artifact.artifact_hash = artifact.hash();
        artifact
    }

    fn fanout_artifact_with_reducer(
        enable_retry: bool,
        reducer: Option<&str>,
    ) -> CompiledWorkflowArtifact {
        let workflow = CompiledWorkflow {
            initial_state: "dispatch".to_owned(),
            states: BTreeMap::from([
                (
                    "dispatch".to_owned(),
                    CompiledStateNode::FanOut {
                        activity_type: "benchmark.echo".to_owned(),
                        items: Expression::Member {
                            object: Box::new(Expression::Identifier { name: "input".to_owned() }),
                            property: "items".to_owned(),
                        },
                        next: "join".to_owned(),
                        handle_var: "fanout".to_owned(),
                        task_queue: None,
                        reducer: reducer.map(str::to_owned),
                        retry: enable_retry.then_some(RetryPolicy {
                            max_attempts: 2,
                            delay: "1s".to_owned(),
                            non_retryable_error_types: Vec::new(),
                        }),
                        config: None,
                        schedule_to_start_timeout_ms: None,
                        start_to_close_timeout_ms: None,
                        heartbeat_timeout_ms: None,
                    },
                ),
                (
                    "join".to_owned(),
                    CompiledStateNode::WaitForAllActivities {
                        fanout_ref_var: "fanout".to_owned(),
                        next: "done".to_owned(),
                        output_var: Some("results".to_owned()),
                        on_error: Some(ErrorTransition {
                            next: "fail".to_owned(),
                            error_var: Some("error".to_owned()),
                        }),
                    },
                ),
                (
                    "done".to_owned(),
                    CompiledStateNode::Succeed {
                        output: Some(Expression::Identifier { name: "results".to_owned() }),
                    },
                ),
                (
                    "fail".to_owned(),
                    CompiledStateNode::Fail {
                        reason: Some(Expression::Identifier { name: "error".to_owned() }),
                    },
                ),
            ]),
            params: Vec::new(),
            non_cancellable_states: BTreeSet::new(),
        };
        let mut artifact = CompiledWorkflowArtifact::new(
            "fanout-demo",
            1,
            "test",
            ArtifactEntrypoint { module: "workflow.ts".to_owned(), export: "workflow".to_owned() },
            workflow,
        );
        artifact.artifact_hash = artifact.hash();
        artifact
    }

    fn fanout_artifact(enable_retry: bool) -> CompiledWorkflowArtifact {
        fanout_artifact_with_reducer(enable_retry, None)
    }

    fn terminal_event(
        artifact: &CompiledWorkflowArtifact,
        payload: WorkflowEvent,
        occurred_at_ms: i64,
    ) -> EventEnvelope<WorkflowEvent> {
        let mut event = EventEnvelope::new(
            payload.event_type(),
            fabrik_events::WorkflowIdentity::new(
                "tenant-a",
                artifact.definition_id.clone(),
                artifact.definition_version,
                artifact.artifact_hash.clone(),
                "instance-a",
                "run-a",
                "test",
            ),
            payload,
        );
        event.occurred_at = DateTime::<Utc>::from_timestamp_millis(occurred_at_ms).unwrap();
        event
    }

    fn bulk_artifact_with_reducer(reducer: &str) -> CompiledWorkflowArtifact {
        let workflow = CompiledWorkflow {
            initial_state: "dispatch".to_owned(),
            states: BTreeMap::from([
                (
                    "dispatch".to_owned(),
                    CompiledStateNode::StartBulkActivity {
                        activity_type: "benchmark.echo".to_owned(),
                        items: Expression::Member {
                            object: Box::new(Expression::Identifier { name: "input".to_owned() }),
                            property: "items".to_owned(),
                        },
                        next: "join".to_owned(),
                        handle_var: "bulk".to_owned(),
                        task_queue: None,
                        execution_policy: Some("eager".to_owned()),
                        reducer: Some(reducer.to_owned()),
                        throughput_backend: None,
                        chunk_size: Some(2),
                        retry: Some(RetryPolicy {
                            max_attempts: 2,
                            delay: "1s".to_owned(),
                            non_retryable_error_types: Vec::new(),
                        }),
                    },
                ),
                (
                    "join".to_owned(),
                    CompiledStateNode::WaitForBulkActivity {
                        bulk_ref_var: "bulk".to_owned(),
                        next: "done".to_owned(),
                        output_var: Some("summary".to_owned()),
                        on_error: Some(ErrorTransition {
                            next: "fail".to_owned(),
                            error_var: Some("error".to_owned()),
                        }),
                    },
                ),
                (
                    "done".to_owned(),
                    CompiledStateNode::Succeed {
                        output: Some(Expression::Identifier { name: "summary".to_owned() }),
                    },
                ),
                (
                    "fail".to_owned(),
                    CompiledStateNode::Fail {
                        reason: Some(Expression::Identifier { name: "error".to_owned() }),
                    },
                ),
            ]),
            params: Vec::new(),
            non_cancellable_states: BTreeSet::new(),
        };
        let mut artifact = CompiledWorkflowArtifact::new(
            "bulk-demo",
            1,
            "test",
            ArtifactEntrypoint { module: "workflow.ts".to_owned(), export: "workflow".to_owned() },
            workflow,
        );
        artifact.artifact_hash = artifact.hash();
        artifact
    }

    fn bulk_artifact() -> CompiledWorkflowArtifact {
        bulk_artifact_with_reducer("collect_results")
    }

    #[test]
    fn validates_hash() {
        let artifact = demo_artifact();
        assert_eq!(artifact.validate(), Ok(()));
    }

    #[test]
    fn executes_step_path() {
        let artifact = demo_artifact();
        let mut plan = artifact.execute_trigger(&json!({"ok": true})).unwrap();
        plan.execution_state.bindings.insert("approved".to_owned(), Value::Bool(true));
        let plan = artifact.execute_from_state("branch", plan.execution_state, false).unwrap();
        assert!(matches!(
            plan.emissions.last(),
            Some(ExecutionEmission { event: WorkflowEvent::ActivityTaskScheduled { activity_id, .. }, .. })
            if activity_id == "step"
        ));
    }

    #[test]
    fn execute_trigger_binds_workflow_params_and_defaults() {
        let artifact = CompiledWorkflowArtifact::new(
            "workflow-params",
            1,
            "test",
            ArtifactEntrypoint { module: "workflow.ts".to_owned(), export: "workflow".to_owned() },
            CompiledWorkflow {
                initial_state: "done".to_owned(),
                states: BTreeMap::from([(
                    "done".to_owned(),
                    CompiledStateNode::Succeed {
                        output: Some(Expression::Object {
                            fields: BTreeMap::from([
                                (
                                    "name".to_owned(),
                                    Expression::Identifier { name: "name".to_owned() },
                                ),
                                (
                                    "punctuation".to_owned(),
                                    Expression::Identifier { name: "punctuation".to_owned() },
                                ),
                                (
                                    "input".to_owned(),
                                    Expression::Identifier { name: "input".to_owned() },
                                ),
                            ]),
                        }),
                    },
                )]),
                params: vec![
                    CompiledWorkflowParam { name: "name".to_owned(), rest: false, default: None },
                    CompiledWorkflowParam {
                        name: "punctuation".to_owned(),
                        rest: false,
                        default: Some(Expression::Literal { value: json!("!") }),
                    },
                ],
                non_cancellable_states: BTreeSet::new(),
            },
        );

        let plan = artifact.execute_trigger(&json!(["fiona"])).unwrap();

        assert!(matches!(
            plan.emissions.last(),
            Some(ExecutionEmission {
                event: WorkflowEvent::WorkflowCompleted { output },
                ..
            }) if output == &json!({
                "name": "fiona",
                "punctuation": "!",
                "input": ["fiona"],
            })
        ));
    }

    #[test]
    fn execute_trigger_binds_rest_workflow_params() {
        let artifact = CompiledWorkflowArtifact::new(
            "workflow-rest-params",
            1,
            "test",
            ArtifactEntrypoint { module: "workflow.ts".to_owned(), export: "workflow".to_owned() },
            CompiledWorkflow {
                initial_state: "done".to_owned(),
                states: BTreeMap::from([(
                    "done".to_owned(),
                    CompiledStateNode::Succeed {
                        output: Some(Expression::Object {
                            fields: BTreeMap::from([
                                (
                                    "prefix".to_owned(),
                                    Expression::Identifier { name: "prefix".to_owned() },
                                ),
                                (
                                    "names".to_owned(),
                                    Expression::Identifier { name: "names".to_owned() },
                                ),
                            ]),
                        }),
                    },
                )]),
                params: vec![
                    CompiledWorkflowParam { name: "prefix".to_owned(), rest: false, default: None },
                    CompiledWorkflowParam { name: "names".to_owned(), rest: true, default: None },
                ],
                non_cancellable_states: BTreeSet::new(),
            },
        );

        let plan = artifact.execute_trigger(&json!(["team", "a", "b"])).unwrap();

        assert!(matches!(
            plan.emissions.last(),
            Some(ExecutionEmission {
                event: WorkflowEvent::WorkflowCompleted { output },
                ..
            }) if output == &json!({
                "prefix": "team",
                "names": ["a", "b"],
            })
        ));
    }

    #[test]
    fn resumes_after_step_completion() {
        let artifact = demo_artifact();
        let plan = artifact
            .execute_after_step_completion(
                "step",
                "step",
                &json!({"done": true}),
                ArtifactExecutionState::default(),
            )
            .unwrap();
        assert!(matches!(
            plan.emissions.last(),
            Some(ExecutionEmission { event: WorkflowEvent::WorkflowCompleted { .. }, .. })
        ));
    }

    #[test]
    fn evaluates_array_index_from_arithmetic_result() {
        let mut state = ArtifactExecutionState::default();
        let value = evaluate_expression(
            &Expression::Index {
                object: Box::new(Expression::Literal { value: json!(["a", "b"]) }),
                index: Box::new(Expression::Binary {
                    op: BinaryOp::Add,
                    left: Box::new(Expression::Literal { value: json!(0) }),
                    right: Box::new(Expression::Literal { value: json!(1) }),
                }),
            },
            &mut state,
            &BTreeMap::new(),
        )
        .unwrap();

        assert_eq!(value, json!("b"));
    }

    #[test]
    fn evaluates_now_and_uuid_from_turn_context() {
        let turn_context = ExecutionTurnContext {
            event_id: uuid::Uuid::parse_str("11111111-2222-3333-4444-555555555555").unwrap(),
            occurred_at: DateTime::from_timestamp_millis(1_700_000_000_123).unwrap(),
        };
        let mut state = ArtifactExecutionState {
            bindings: BTreeMap::new(),
            workflow_info: None,
            markers: BTreeMap::new(),
            version_markers: BTreeMap::new(),
            active_signal: None,
            active_update: None,
            pending_workflow_cancellation: None,
            condition_timers: BTreeSet::new(),
            turn_context: Some(turn_context.clone()),
            pending_markers: Vec::new(),
            pending_version_markers: Vec::new(),
        };

        let now = evaluate_expression(&Expression::Now, &mut state, &BTreeMap::new()).unwrap();
        let uuid = evaluate_expression(
            &Expression::Uuid { scope: "callsite-1".to_owned() },
            &mut state,
            &BTreeMap::new(),
        )
        .unwrap();

        assert_eq!(now, json!(1_700_000_000_123i64));
        assert_eq!(
            uuid,
            json!(
                uuid::Uuid::new_v5(
                    &uuid::Uuid::NAMESPACE_URL,
                    format!("{}:callsite-1", turn_context.event_id).as_bytes(),
                )
                .to_string()
            )
        );
    }

    #[test]
    fn evaluates_builtin_object_keys_and_array_join() {
        let mut state = ArtifactExecutionState::default();
        let keys = evaluate_expression(
            &Expression::Call {
                callee: "__builtin_object_keys".to_owned(),
                args: vec![Expression::Literal { value: json!({"alpha": 1, "beta": 2}) }],
            },
            &mut state,
            &BTreeMap::new(),
        )
        .unwrap();
        let numeric_keys = evaluate_expression(
            &Expression::Call {
                callee: "__builtin_array_sort_numeric_asc".to_owned(),
                args: vec![Expression::Call {
                    callee: "__builtin_array_map_number".to_owned(),
                    args: vec![Expression::Literal { value: json!(["10", "2", "1"]) }],
                }],
            },
            &mut state,
            &BTreeMap::new(),
        )
        .unwrap();
        let contains_alpha = evaluate_expression(
            &Expression::Binary {
                op: BinaryOp::In,
                left: Box::new(Expression::Literal { value: json!("alpha") }),
                right: Box::new(Expression::Literal { value: json!({"alpha": 1, "beta": 2}) }),
            },
            &mut state,
            &BTreeMap::new(),
        )
        .unwrap();
        let omitted = evaluate_expression(
            &Expression::Call {
                callee: "__builtin_object_omit".to_owned(),
                args: vec![
                    Expression::Literal { value: json!({"alpha": 1, "beta": 2}) },
                    Expression::Literal { value: json!("alpha") },
                ],
            },
            &mut state,
            &BTreeMap::new(),
        )
        .unwrap();
        let inserted = evaluate_expression(
            &Expression::Call {
                callee: "__builtin_object_set".to_owned(),
                args: vec![
                    Expression::Literal { value: json!({"beta": 2}) },
                    Expression::Literal { value: json!("alpha") },
                    Expression::Literal { value: json!(1) },
                ],
            },
            &mut state,
            &BTreeMap::new(),
        )
        .unwrap();
        let reduced = evaluate_expression(
            &Expression::ArrayReduce {
                array: Box::new(Expression::Literal { value: json!([1, 2, 3]) }),
                accumulator_name: "sum".to_owned(),
                item_name: "value".to_owned(),
                initial: Box::new(Expression::Literal { value: json!(0) }),
                expr: Box::new(Expression::Binary {
                    op: BinaryOp::Add,
                    left: Box::new(Expression::Identifier { name: "sum".to_owned() }),
                    right: Box::new(Expression::Identifier { name: "value".to_owned() }),
                }),
            },
            &mut state,
            &BTreeMap::new(),
        )
        .unwrap();
        let appended = evaluate_expression(
            &Expression::Call {
                callee: "__builtin_array_append".to_owned(),
                args: vec![
                    Expression::Literal { value: json!([1, 2]) },
                    Expression::Literal { value: json!(3) },
                ],
            },
            &mut state,
            &BTreeMap::new(),
        )
        .unwrap();
        let joined = evaluate_expression(
            &Expression::Call {
                callee: "__builtin_array_join".to_owned(),
                args: vec![
                    Expression::Literal { value: json!(["a", null, "c"]) },
                    Expression::Literal { value: json!("\n") },
                ],
            },
            &mut state,
            &BTreeMap::new(),
        )
        .unwrap();

        assert_eq!(keys, json!(["alpha", "beta"]));
        assert_eq!(numeric_keys, json!([1, 2, 10]));
        assert_eq!(contains_alpha, json!(true));
        assert_eq!(omitted, json!({"beta": 2}));
        assert_eq!(inserted, json!({"alpha": 1, "beta": 2}));
        assert_eq!(reduced, json!(6));
        assert_eq!(appended, json!([1, 2, 3]));
        assert_eq!(joined, json!("a\n\nc"));
    }

    #[test]
    fn evaluates_workflow_info_from_execution_state() {
        let mut state = ArtifactExecutionState {
            workflow_info: Some(json!({
                "workflowId": "workflow-1",
                "runId": "run-1",
                "parent": { "workflowId": "parent-1", "runId": "parent-run-1" },
                "historyLength": 3,
                "continueAsNewSuggested": false
            })),
            ..ArtifactExecutionState::default()
        };

        let value =
            evaluate_expression(&Expression::WorkflowInfo, &mut state, &BTreeMap::new()).unwrap();

        assert_eq!(value["workflowId"], json!("workflow-1"));
        assert_eq!(value["parent"]["workflowId"], json!("parent-1"));
        assert_eq!(value["historyLength"], json!(3));
    }

    #[test]
    fn records_side_effect_markers_once_per_callsite() {
        let turn_context = ExecutionTurnContext {
            event_id: uuid::Uuid::parse_str("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee").unwrap(),
            occurred_at: DateTime::from_timestamp_millis(1_700_000_123_000).unwrap(),
        };
        let mut state = ArtifactExecutionState {
            bindings: BTreeMap::new(),
            workflow_info: None,
            markers: BTreeMap::new(),
            version_markers: BTreeMap::new(),
            active_signal: None,
            active_update: None,
            pending_workflow_cancellation: None,
            condition_timers: BTreeSet::new(),
            turn_context: Some(turn_context),
            pending_markers: Vec::new(),
            pending_version_markers: Vec::new(),
        };
        let expression = Expression::SideEffect {
            marker_id: "marker_callsite".to_owned(),
            expr: Box::new(Expression::Object {
                fields: BTreeMap::from([
                    ("now".to_owned(), Expression::Now),
                    ("uuid".to_owned(), Expression::Uuid { scope: "callsite".to_owned() }),
                ]),
            }),
        };

        let first = evaluate_expression(&expression, &mut state, &BTreeMap::new()).unwrap();
        let second = evaluate_expression(&expression, &mut state, &BTreeMap::new()).unwrap();

        assert_eq!(first, second);
        assert_eq!(state.markers.len(), 1);
        assert_eq!(state.pending_markers.len(), 1);
    }

    #[test]
    fn records_version_markers_once_per_change_id() {
        let mut state = ArtifactExecutionState::default();
        let expression = Expression::Version {
            change_id: "feature-x".to_owned(),
            min_supported: 1,
            max_supported: 3,
        };

        let first = evaluate_expression(&expression, &mut state, &BTreeMap::new()).unwrap();
        let second = evaluate_expression(&expression, &mut state, &BTreeMap::new()).unwrap();

        assert_eq!(first, json!(3));
        assert_eq!(second, json!(3));
        assert_eq!(state.version_markers.get("feature-x"), Some(&3));
        assert_eq!(state.pending_version_markers, vec![("feature-x".to_owned(), 3)]);

        let mut emissions = Vec::new();
        emit_pending_markers(&mut emissions, &mut state, "dispatch");
        assert!(state.pending_version_markers.is_empty());
        assert!(matches!(
            emissions.as_slice(),
            [ExecutionEmission {
                event: WorkflowEvent::VersionMarkerRecorded { change_id, version },
                ..
            }] if change_id == "feature-x" && *version == 3
        ));
    }

    #[test]
    fn update_start_child_waits_on_child_state_without_duplicate_acceptance_event() {
        let artifact = update_child_artifact();
        let plan = artifact
            .execute_update_with_turn(
                "wait_signal",
                "upd-1",
                "approve",
                &json!({"id": "approval-1"}),
                ArtifactExecutionState::default(),
                ExecutionTurnContext {
                    event_id: uuid::Uuid::now_v7(),
                    occurred_at: DateTime::from_timestamp_millis(1_700_000_000_000).unwrap(),
                },
            )
            .unwrap();

        assert_eq!(plan.final_state, "await_child");
        assert_eq!(plan.emissions.len(), 1);
        assert!(matches!(
            plan.emissions.first(),
            Some(ExecutionEmission {
                event: WorkflowEvent::ChildWorkflowStartRequested { .. },
                state,
            }) if state.as_deref() == Some("await_child")
        ));
    }

    #[test]
    fn child_completion_finishes_active_update_and_returns_to_caller_state() {
        let artifact = update_child_artifact();
        let plan = artifact
            .execute_update_with_turn(
                "wait_signal",
                "upd-1",
                "approve",
                &json!({"id": "approval-1"}),
                ArtifactExecutionState::default(),
                ExecutionTurnContext {
                    event_id: uuid::Uuid::parse_str("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")
                        .unwrap(),
                    occurred_at: DateTime::from_timestamp_millis(1_700_000_000_000).unwrap(),
                },
            )
            .unwrap();
        let child_id =
            plan.execution_state.bindings["child"]["child_id"].as_str().unwrap().to_owned();

        let resumed = artifact
            .execute_after_child_completion_with_turn(
                "await_child",
                &child_id,
                &json!({"ok": true}),
                plan.execution_state,
                ExecutionTurnContext {
                    event_id: uuid::Uuid::now_v7(),
                    occurred_at: DateTime::from_timestamp_millis(1_700_000_000_100).unwrap(),
                },
            )
            .unwrap();

        assert_eq!(resumed.final_state, "wait_signal");
        assert!(matches!(
            resumed.emissions.last(),
            Some(ExecutionEmission {
                event: WorkflowEvent::WorkflowUpdateCompleted { .. },
                state,
            }) if state.as_deref() == Some("wait_signal")
        ));
    }

    #[test]
    fn child_failure_can_follow_wait_for_child_error_transition() {
        let artifact = CompiledWorkflowArtifact::new(
            "child-error-recovery",
            1,
            "test",
            ArtifactEntrypoint { module: "workflow.ts".to_owned(), export: "workflow".to_owned() },
            CompiledWorkflow {
                initial_state: "await_child".to_owned(),
                states: BTreeMap::from([
                    (
                        "await_child".to_owned(),
                        CompiledStateNode::WaitForChild {
                            child_ref_var: "child".to_owned(),
                            next: "done".to_owned(),
                            output_var: Some("childResult".to_owned()),
                            on_error: Some(ErrorTransition {
                                next: "recover".to_owned(),
                                error_var: Some("childError".to_owned()),
                            }),
                        },
                    ),
                    (
                        "recover".to_owned(),
                        CompiledStateNode::Succeed {
                            output: Some(Expression::Identifier { name: "childError".to_owned() }),
                        },
                    ),
                    ("done".to_owned(), CompiledStateNode::Succeed { output: None }),
                ]),
                params: Vec::new(),
                non_cancellable_states: BTreeSet::new(),
            },
        );
        let mut execution_state = ArtifactExecutionState::default();
        execution_state.bindings.insert(
            "child".to_owned(),
            json!({
                "child_id": "child-1",
                "workflow_id": "child-workflow"
            }),
        );

        let resumed = artifact
            .execute_after_child_failure_with_turn(
                "await_child",
                "child-1",
                "child workflow cancelled",
                execution_state,
                ExecutionTurnContext { event_id: uuid::Uuid::now_v7(), occurred_at: Utc::now() },
            )
            .unwrap();

        assert_eq!(resumed.final_state, "recover");
        assert_eq!(
            resumed.execution_state.bindings.get("childError"),
            Some(&Value::String("child workflow cancelled".to_owned()))
        );
        assert!(matches!(
            resumed.emissions.last(),
            Some(ExecutionEmission {
                event: WorkflowEvent::WorkflowCompleted { output },
                ..
            }) if output == &Value::String("child workflow cancelled".to_owned())
        ));
    }

    #[test]
    fn builtin_is_cancellation_matches_temporal_like_errors() {
        let helper = HelperFunction {
            params: vec!["error".to_owned()],
            statements: Vec::new(),
            body: Expression::Call {
                callee: "__temporal_is_cancellation".to_owned(),
                args: vec![Expression::Identifier { name: "error".to_owned() }],
            },
        };
        let helpers = BTreeMap::from([("isCancellation".to_owned(), helper)]);
        let mut state = ArtifactExecutionState::default();
        state.bindings.insert("error".to_owned(), json!({"type": "CancelledFailure"}));
        assert_eq!(
            evaluate_expression(
                &Expression::Call {
                    callee: "isCancellation".to_owned(),
                    args: vec![Expression::Identifier { name: "error".to_owned() }],
                },
                &mut state,
                &helpers,
            )
            .unwrap(),
            Value::Bool(true)
        );

        state.bindings.insert("error".to_owned(), Value::String("boom".to_owned()));
        assert_eq!(
            evaluate_expression(
                &Expression::Call {
                    callee: "isCancellation".to_owned(),
                    args: vec![Expression::Identifier { name: "error".to_owned() }],
                },
                &mut state,
                &helpers,
            )
            .unwrap(),
            Value::Bool(false)
        );
    }

    #[test]
    fn evaluates_block_bodied_helpers_with_for_range() {
        let helper = HelperFunction {
            params: vec!["total".to_owned(), "n".to_owned()],
            statements: vec![
                HelperStatement::Assign {
                    target: "base".to_owned(),
                    expr: Expression::Call {
                        callee: "__builtin_math_floor".to_owned(),
                        args: vec![Expression::Binary {
                            op: BinaryOp::Divide,
                            left: Box::new(Expression::Identifier { name: "total".to_owned() }),
                            right: Box::new(Expression::Identifier { name: "n".to_owned() }),
                        }],
                    },
                },
                HelperStatement::Assign {
                    target: "remainder".to_owned(),
                    expr: Expression::Binary {
                        op: BinaryOp::Remainder,
                        left: Box::new(Expression::Identifier { name: "total".to_owned() }),
                        right: Box::new(Expression::Identifier { name: "n".to_owned() }),
                    },
                },
                HelperStatement::Assign {
                    target: "partitions".to_owned(),
                    expr: Expression::Call {
                        callee: "__builtin_array_fill".to_owned(),
                        args: vec![
                            Expression::Identifier { name: "n".to_owned() },
                            Expression::Identifier { name: "base".to_owned() },
                        ],
                    },
                },
                HelperStatement::ForRange {
                    index_var: "i".to_owned(),
                    start: Expression::Literal { value: json!(0) },
                    end: Expression::Identifier { name: "remainder".to_owned() },
                    body: vec![HelperStatement::AssignIndex {
                        target: "partitions".to_owned(),
                        index: Expression::Identifier { name: "i".to_owned() },
                        expr: Expression::Binary {
                            op: BinaryOp::Add,
                            left: Box::new(Expression::Index {
                                object: Box::new(Expression::Identifier {
                                    name: "partitions".to_owned(),
                                }),
                                index: Box::new(Expression::Identifier { name: "i".to_owned() }),
                            }),
                            right: Box::new(Expression::Literal { value: json!(1) }),
                        },
                    }],
                },
            ],
            body: Expression::Identifier { name: "partitions".to_owned() },
        };
        let helpers = BTreeMap::from([("divideIntoPartitions".to_owned(), helper)]);
        let mut state = ArtifactExecutionState::default();
        let value = evaluate_expression(
            &Expression::Call {
                callee: "divideIntoPartitions".to_owned(),
                args: vec![
                    Expression::Literal { value: json!(10) },
                    Expression::Literal { value: json!(3) },
                ],
            },
            &mut state,
            &helpers,
        )
        .unwrap();

        assert_eq!(value, json!([4, 3, 3]));
    }

    #[test]
    fn signal_handler_executes_and_returns_to_workflow_state() {
        let artifact = signal_handler_artifact();
        let plan = artifact
            .execute_signal_handler_with_turn(
                "idle",
                "sig-1",
                "approved",
                &json!({"ok": true}),
                ArtifactExecutionState::default(),
                ExecutionTurnContext { event_id: uuid::Uuid::now_v7(), occurred_at: Utc::now() },
            )
            .unwrap();

        assert_eq!(plan.final_state, "idle");
        assert!(plan.emissions.is_empty());
        assert_eq!(plan.execution_state.bindings.get("approvedValue"), Some(&json!({"ok": true})));
        assert!(plan.execution_state.active_signal.is_none());
    }

    #[test]
    fn signal_handler_re_evaluates_wait_for_condition() {
        let artifact = signal_condition_artifact();
        let plan = artifact
            .execute_signal_handler_with_turn(
                "wait_ready",
                "sig-1",
                "approved",
                &json!({"ok": true}),
                ArtifactExecutionState::default(),
                ExecutionTurnContext { event_id: uuid::Uuid::now_v7(), occurred_at: Utc::now() },
            )
            .unwrap();

        assert_eq!(plan.final_state, "done");
        assert!(matches!(
            plan.emissions.last(),
            Some(ExecutionEmission {
                event: WorkflowEvent::WorkflowCompleted { output },
                ..
            }) if output == &json!({"ok": true})
        ));
        assert!(plan.execution_state.active_signal.is_none());
    }

    #[test]
    fn fanout_join_returns_ordered_results_after_out_of_order_completions() {
        let artifact = fanout_artifact(false);
        let plan = artifact
            .execute_trigger_with_turn(
                &json!({
                    "items": [
                        {"value": 1},
                        {"value": 2},
                        {"value": 3}
                    ]
                }),
                ExecutionTurnContext {
                    event_id: uuid::Uuid::parse_str("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")
                        .unwrap(),
                    occurred_at: DateTime::from_timestamp_millis(1_700_000_000_000).unwrap(),
                },
            )
            .unwrap();

        assert_eq!(plan.final_state, "join");
        assert_eq!(plan.emissions.len(), 4);

        let plan = artifact
            .execute_after_step_completion_with_turn(
                "join",
                "dispatch::2",
                &json!({"value": 30}),
                plan.execution_state,
                ExecutionTurnContext {
                    event_id: uuid::Uuid::now_v7(),
                    occurred_at: DateTime::from_timestamp_millis(1_700_000_000_100).unwrap(),
                },
            )
            .unwrap();
        assert_eq!(plan.final_state, "join");
        assert!(plan.emissions.is_empty());

        let plan = artifact
            .execute_after_step_completion_with_turn(
                "join",
                "dispatch::0",
                &json!({"value": 10}),
                plan.execution_state,
                ExecutionTurnContext {
                    event_id: uuid::Uuid::now_v7(),
                    occurred_at: DateTime::from_timestamp_millis(1_700_000_000_200).unwrap(),
                },
            )
            .unwrap();
        assert_eq!(plan.final_state, "join");
        assert!(plan.emissions.is_empty());

        let plan = artifact
            .execute_after_step_completion_with_turn(
                "join",
                "dispatch::1",
                &json!({"value": 20}),
                plan.execution_state,
                ExecutionTurnContext {
                    event_id: uuid::Uuid::now_v7(),
                    occurred_at: DateTime::from_timestamp_millis(1_700_000_000_300).unwrap(),
                },
            )
            .unwrap();

        assert_eq!(plan.final_state, "done");
        assert!(matches!(
            plan.emissions.last(),
            Some(ExecutionEmission {
                event: WorkflowEvent::WorkflowCompleted { output },
                ..
            }) if output == &json!([
                {"value": 10},
                {"value": 20},
                {"value": 30}
            ])
        ));
    }

    #[test]
    fn fanout_failure_uses_join_error_transition() {
        let artifact = fanout_artifact(false);
        let plan = artifact
            .execute_trigger_with_turn(
                &json!({"items": [{"value": 1}, {"value": 2}]}),
                ExecutionTurnContext {
                    event_id: uuid::Uuid::now_v7(),
                    occurred_at: DateTime::from_timestamp_millis(1_700_000_000_000).unwrap(),
                },
            )
            .unwrap();

        let failed = artifact
            .execute_after_step_failure_with_turn(
                "join",
                "dispatch::1",
                "boom",
                plan.execution_state,
                ExecutionTurnContext {
                    event_id: uuid::Uuid::now_v7(),
                    occurred_at: DateTime::from_timestamp_millis(1_700_000_000_100).unwrap(),
                },
            )
            .unwrap();

        assert_eq!(failed.final_state, "fail");
        assert!(matches!(
            failed.emissions.last(),
            Some(ExecutionEmission {
                event: WorkflowEvent::WorkflowFailed { reason },
                ..
            }) if reason == "boom"
        ));
    }

    #[test]
    fn fanout_all_settled_waits_for_remaining_activities_before_completing() {
        let artifact = fanout_artifact_with_reducer(false, Some("all_settled"));
        let plan = artifact
            .execute_trigger_with_turn(
                &json!({"items": [{"value": 1}, {"value": 2}, {"value": 3}]}),
                ExecutionTurnContext {
                    event_id: uuid::Uuid::now_v7(),
                    occurred_at: DateTime::from_timestamp_millis(1_700_000_000_000).unwrap(),
                },
            )
            .unwrap();

        let waiting = artifact
            .execute_after_step_failure_with_turn(
                "join",
                "dispatch::2",
                "later error",
                plan.execution_state,
                ExecutionTurnContext {
                    event_id: uuid::Uuid::now_v7(),
                    occurred_at: DateTime::from_timestamp_millis(1_700_000_000_100).unwrap(),
                },
            )
            .unwrap();
        assert_eq!(waiting.final_state, "join");
        assert!(waiting.emissions.is_empty());

        let waiting = artifact
            .execute_after_step_completion_with_turn(
                "join",
                "dispatch::1",
                &json!({"value": 20}),
                waiting.execution_state,
                ExecutionTurnContext {
                    event_id: uuid::Uuid::now_v7(),
                    occurred_at: DateTime::from_timestamp_millis(1_700_000_000_200).unwrap(),
                },
            )
            .unwrap();
        assert_eq!(waiting.final_state, "join");
        assert!(waiting.emissions.is_empty());

        let settled = artifact
            .execute_after_step_failure_with_turn(
                "join",
                "dispatch::0",
                "first error",
                waiting.execution_state,
                ExecutionTurnContext {
                    event_id: uuid::Uuid::now_v7(),
                    occurred_at: DateTime::from_timestamp_millis(1_700_000_000_300).unwrap(),
                },
            )
            .unwrap();

        assert_eq!(settled.final_state, "done");
        assert!(matches!(
            settled.emissions.last(),
            Some(ExecutionEmission {
                event: WorkflowEvent::WorkflowCompleted { output },
                ..
            }) if output["status"] == json!("settled")
                && output["terminalStatus"] == json!("failed")
                && output["succeededItems"] == json!(1)
                && output["failedItems"] == json!(2)
        ));
    }

    #[test]
    fn fanout_all_settled_batch_apply_completes_in_one_transition() {
        let artifact = fanout_artifact_with_reducer(false, Some("all_settled"));
        let plan = artifact
            .execute_trigger_with_turn(
                &json!({"items": [{"value": 1}, {"value": 2}, {"value": 3}]}),
                ExecutionTurnContext {
                    event_id: uuid::Uuid::now_v7(),
                    occurred_at: DateTime::from_timestamp_millis(1_700_000_000_000).unwrap(),
                },
            )
            .unwrap();

        let events = vec![
            terminal_event(
                &artifact,
                WorkflowEvent::ActivityTaskFailed {
                    activity_id: "dispatch::2".to_owned(),
                    attempt: 1,
                    error: "later error".to_owned(),
                    worker_id: "worker-a".to_owned(),
                    worker_build_id: "build-a".to_owned(),
                },
                1_700_000_000_100,
            ),
            terminal_event(
                &artifact,
                WorkflowEvent::ActivityTaskCompleted {
                    activity_id: "dispatch::1".to_owned(),
                    attempt: 1,
                    output: json!({"value": 20}),
                    worker_id: "worker-a".to_owned(),
                    worker_build_id: "build-a".to_owned(),
                },
                1_700_000_000_200,
            ),
            terminal_event(
                &artifact,
                WorkflowEvent::ActivityTaskFailed {
                    activity_id: "dispatch::0".to_owned(),
                    attempt: 1,
                    error: "first error".to_owned(),
                    worker_id: "worker-a".to_owned(),
                    worker_build_id: "build-a".to_owned(),
                },
                1_700_000_000_300,
            ),
        ];

        let Some((settled, applied)) = artifact
            .try_execute_after_step_terminal_batch("join", &events, plan.execution_state)
            .unwrap()
        else {
            panic!("expected fanout batch fast path");
        };

        assert_eq!(applied, 3);
        assert_eq!(settled.final_state, "done");
        assert!(matches!(
            settled.emissions.last(),
            Some(ExecutionEmission {
                event: WorkflowEvent::WorkflowCompleted { output },
                ..
            }) if output["status"] == json!("settled")
                && output["terminalStatus"] == json!("failed")
                && output["succeededItems"] == json!(1)
                && output["failedItems"] == json!(2)
        ));
    }

    #[test]
    fn fanout_all_settled_intermediate_completion_drops_context_and_failure_map() {
        let artifact = fanout_artifact_with_reducer(false, Some("all_settled"));
        let plan = artifact
            .execute_trigger_with_turn(
                &json!({"items": [{"value": 1}, {"value": 2}]}),
                ExecutionTurnContext {
                    event_id: uuid::Uuid::now_v7(),
                    occurred_at: DateTime::from_timestamp_millis(1_700_000_000_000).unwrap(),
                },
            )
            .unwrap();

        let waiting = artifact
            .execute_after_step_failure_with_turn(
                "join",
                "dispatch::1",
                "later error",
                plan.execution_state,
                ExecutionTurnContext {
                    event_id: uuid::Uuid::now_v7(),
                    occurred_at: DateTime::from_timestamp_millis(1_700_000_000_100).unwrap(),
                },
            )
            .unwrap();

        assert!(waiting.context.is_none());
        let fanout = artifact.fanout_binding("join", "fanout", &waiting.execution_state).unwrap();
        assert!(fanout.failed_activity_errors.is_empty());
        assert_eq!(fanout.first_failed_index, Some(1));
        assert_eq!(fanout.first_failed_error.as_deref(), Some("later error"));
    }

    #[test]
    fn fanout_count_reducer_emits_terminal_count_summary() {
        let artifact = fanout_artifact_with_reducer(false, Some("count"));
        let plan = artifact
            .execute_trigger_with_turn(
                &json!({"items": [{"value": 1}, {"value": 2}, {"value": 3}]}),
                ExecutionTurnContext {
                    event_id: uuid::Uuid::now_v7(),
                    occurred_at: DateTime::from_timestamp_millis(1_700_000_000_000).unwrap(),
                },
            )
            .unwrap();

        let waiting = artifact
            .execute_after_step_completion_with_turn(
                "join",
                "dispatch::2",
                &json!({"value": 30}),
                plan.execution_state,
                ExecutionTurnContext {
                    event_id: uuid::Uuid::now_v7(),
                    occurred_at: DateTime::from_timestamp_millis(1_700_000_000_100).unwrap(),
                },
            )
            .unwrap();
        let settled = artifact
            .execute_after_step_failure_with_turn(
                "join",
                "dispatch::0",
                "boom",
                waiting.execution_state,
                ExecutionTurnContext {
                    event_id: uuid::Uuid::now_v7(),
                    occurred_at: DateTime::from_timestamp_millis(1_700_000_000_200).unwrap(),
                },
            )
            .unwrap();
        let settled = artifact
            .execute_after_step_completion_with_turn(
                "join",
                "dispatch::1",
                &json!({"value": 20}),
                settled.execution_state,
                ExecutionTurnContext {
                    event_id: uuid::Uuid::now_v7(),
                    occurred_at: DateTime::from_timestamp_millis(1_700_000_000_300).unwrap(),
                },
            )
            .unwrap();

        assert!(matches!(
            settled.emissions.last(),
            Some(ExecutionEmission {
                event: WorkflowEvent::WorkflowCompleted { output },
                ..
            }) if output["status"] == json!("settled")
                && output["count"] == json!(3)
                && output["succeededItems"] == json!(2)
                && output["failedItems"] == json!(1)
        ));
    }

    #[test]
    fn fanout_sum_reducer_emits_terminal_sum_summary() {
        let artifact = fanout_artifact_with_reducer(false, Some("sum"));
        let plan = artifact
            .execute_trigger_with_turn(
                &json!({"items": [1, 2, 3]}),
                ExecutionTurnContext {
                    event_id: uuid::Uuid::now_v7(),
                    occurred_at: DateTime::from_timestamp_millis(1_700_000_000_000).unwrap(),
                },
            )
            .unwrap();

        let waiting = artifact
            .execute_after_step_completion_with_turn(
                "join",
                "dispatch::0",
                &json!(10),
                plan.execution_state,
                ExecutionTurnContext {
                    event_id: uuid::Uuid::now_v7(),
                    occurred_at: DateTime::from_timestamp_millis(1_700_000_000_100).unwrap(),
                },
            )
            .unwrap();
        let waiting = artifact
            .execute_after_step_completion_with_turn(
                "join",
                "dispatch::1",
                &json!(20),
                waiting.execution_state,
                ExecutionTurnContext {
                    event_id: uuid::Uuid::now_v7(),
                    occurred_at: DateTime::from_timestamp_millis(1_700_000_000_200).unwrap(),
                },
            )
            .unwrap();
        let completed = artifact
            .execute_after_step_completion_with_turn(
                "join",
                "dispatch::2",
                &json!(30),
                waiting.execution_state,
                ExecutionTurnContext {
                    event_id: uuid::Uuid::now_v7(),
                    occurred_at: DateTime::from_timestamp_millis(1_700_000_000_300).unwrap(),
                },
            )
            .unwrap();

        assert!(matches!(
            completed.emissions.last(),
            Some(ExecutionEmission {
                event: WorkflowEvent::WorkflowCompleted { output },
                ..
            }) if output["terminalStatus"] == json!("completed")
                && output["sum"] == json!(60.0)
                && output.get("resultHandle").is_none()
        ));
    }

    #[test]
    fn fanout_count_reducer_avoids_storing_inputs_and_reconstructs_retry_input() {
        let artifact = fanout_artifact_with_reducer(true, Some("count"));
        let plan = artifact
            .execute_trigger_with_turn(
                &json!({"items": [{"value": 1}, {"value": 2}]}),
                ExecutionTurnContext {
                    event_id: uuid::Uuid::now_v7(),
                    occurred_at: DateTime::from_timestamp_millis(1_700_000_000_000).unwrap(),
                },
            )
            .unwrap();

        let fanout = artifact.fanout_binding("join", "fanout", &plan.execution_state).unwrap();
        assert_eq!(fanout.total_count, 2);
        assert!(fanout.inputs.is_empty());
        assert!(fanout.results.is_empty());
        assert_eq!(fanout.pending_count, 2);
        assert_eq!(fanout.settlement_bitmap.len(), 1);

        let retry = artifact.step_retry("join", &plan.execution_state).unwrap().unwrap();
        assert_eq!(retry.max_attempts, 2);

        let (activity_type, _config, input) =
            artifact.step_details("dispatch::1", &plan.execution_state).unwrap();
        assert_eq!(activity_type, "benchmark.echo");
        assert_eq!(input, json!({"value": 2}));
    }

    #[test]
    fn fanout_wait_state_resolves_retry_and_member_input() {
        let artifact = fanout_artifact(true);
        let plan = artifact
            .execute_trigger_with_turn(
                &json!({"items": [{"value": 1}, {"value": 2}]}),
                ExecutionTurnContext {
                    event_id: uuid::Uuid::now_v7(),
                    occurred_at: DateTime::from_timestamp_millis(1_700_000_000_000).unwrap(),
                },
            )
            .unwrap();

        let retry = artifact.step_retry("join", &plan.execution_state).unwrap().unwrap();
        assert_eq!(retry.max_attempts, 2);

        let (activity_type, _config, input) =
            artifact.step_details("dispatch::1", &plan.execution_state).unwrap();
        assert_eq!(activity_type, "benchmark.echo");
        assert_eq!(input, json!({"value": 2}));
        assert_eq!(
            artifact.reschedule_state_for_activity("dispatch::1", &plan.execution_state).as_deref(),
            Some("join")
        );
    }

    #[test]
    fn fanout_collect_settled_results_emits_ordered_settled_array() {
        let artifact = fanout_artifact_with_reducer(false, Some("collect_settled_results"));
        let plan = artifact
            .execute_trigger_with_turn(
                &json!({"items": [{"value": 1}, {"value": 2}, {"value": 3}]}),
                ExecutionTurnContext {
                    event_id: uuid::Uuid::now_v7(),
                    occurred_at: DateTime::from_timestamp_millis(1_700_000_000_000).unwrap(),
                },
            )
            .unwrap();

        let waiting = artifact
            .execute_after_step_completion_with_turn(
                "join",
                "dispatch::2",
                &json!({"value": 30}),
                plan.execution_state,
                ExecutionTurnContext {
                    event_id: uuid::Uuid::now_v7(),
                    occurred_at: DateTime::from_timestamp_millis(1_700_000_000_100).unwrap(),
                },
            )
            .unwrap();
        let waiting = artifact
            .execute_after_step_failure_with_turn(
                "join",
                "dispatch::0",
                "boom",
                waiting.execution_state,
                ExecutionTurnContext {
                    event_id: uuid::Uuid::now_v7(),
                    occurred_at: DateTime::from_timestamp_millis(1_700_000_000_200).unwrap(),
                },
            )
            .unwrap();
        let settled = artifact
            .execute_after_step_completion_with_turn(
                "join",
                "dispatch::1",
                &json!({"value": 20}),
                waiting.execution_state,
                ExecutionTurnContext {
                    event_id: uuid::Uuid::now_v7(),
                    occurred_at: DateTime::from_timestamp_millis(1_700_000_000_300).unwrap(),
                },
            )
            .unwrap();

        assert!(matches!(
            settled.emissions.last(),
            Some(ExecutionEmission {
                event: WorkflowEvent::WorkflowCompleted { output },
                ..
            }) if output == &json!([
                {"status": "rejected", "reason": "boom"},
                {"status": "fulfilled", "value": {"value": 20}},
                {"status": "fulfilled", "value": {"value": 30}}
            ])
        ));
    }

    #[test]
    fn fanout_schedule_emits_task_queue_timeout_and_retry_metadata() {
        let mut artifact = fanout_artifact_with_reducer(true, Some("collect_results"));
        if let Some(CompiledStateNode::FanOut {
            task_queue,
            start_to_close_timeout_ms,
            heartbeat_timeout_ms,
            schedule_to_start_timeout_ms,
            ..
        }) = artifact.workflow.states.get_mut("dispatch")
        {
            *task_queue = Some(Expression::Literal { value: json!("payments") });
            *schedule_to_start_timeout_ms = Some(1_000);
            *start_to_close_timeout_ms = Some(30_000);
            *heartbeat_timeout_ms = Some(5_000);
        } else {
            panic!("dispatch should be fanout");
        }
        artifact.artifact_hash = artifact.hash();

        let plan = artifact
            .execute_trigger_with_turn(
                &json!({"items": [{"value": 1}, {"value": 2}]}),
                ExecutionTurnContext {
                    event_id: uuid::Uuid::now_v7(),
                    occurred_at: DateTime::from_timestamp_millis(1_700_000_000_000).unwrap(),
                },
            )
            .unwrap();

        assert!(plan.emissions.iter().any(|emission| {
            matches!(
                emission,
                ExecutionEmission {
                    event: WorkflowEvent::ActivityTaskScheduled {
                        task_queue,
                        schedule_to_start_timeout_ms,
                        start_to_close_timeout_ms,
                        heartbeat_timeout_ms,
                        ..
                    },
                    ..
                } if task_queue == "payments"
                    && schedule_to_start_timeout_ms == &Some(1_000)
                    && start_to_close_timeout_ms == &Some(30_000)
                    && heartbeat_timeout_ms == &Some(5_000)
            )
        }));

        let retry = artifact.step_retry("join", &plan.execution_state).unwrap().unwrap();
        assert_eq!(retry.max_attempts, 2);
        assert_eq!(retry.delay, "1s");
    }

    #[test]
    fn bulk_empty_input_short_circuits_without_emitting_schedule_event() {
        let artifact = bulk_artifact();
        let plan = artifact
            .execute_trigger_with_turn(
                &json!({"items": []}),
                ExecutionTurnContext {
                    event_id: uuid::Uuid::now_v7(),
                    occurred_at: DateTime::from_timestamp_millis(1_700_000_000_000).unwrap(),
                },
            )
            .unwrap();

        assert_eq!(plan.final_state, "done");
        assert_eq!(plan.emissions.len(), 2);
        let output = match &plan.emissions[1].event {
            WorkflowEvent::WorkflowCompleted { output } => output,
            other => panic!("expected workflow completion, got {other:?}"),
        };
        let batch_id = output["batchId"].as_str().unwrap();
        assert_eq!(
            output,
            &serde_json::json!({
                "batchId": batch_id,
                "status": "completed",
                "totalItems": 0,
                "succeededItems": 0,
                "failedItems": 0,
                "cancelledItems": 0,
                "chunkCount": 0,
                "resultHandle": { "batchId": batch_id },
            })
        );
    }

    #[test]
    fn bulk_wait_state_resumes_with_summary() {
        let artifact = bulk_artifact();
        let plan = artifact
            .execute_trigger_with_turn(
                &json!({"items": [{"value": 1}, {"value": 2}, {"value": 3}]}),
                ExecutionTurnContext {
                    event_id: uuid::Uuid::parse_str("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")
                        .unwrap(),
                    occurred_at: DateTime::from_timestamp_millis(1_700_000_000_000).unwrap(),
                },
            )
            .unwrap();

        let batch_id = plan
            .emissions
            .iter()
            .find_map(|emission| match &emission.event {
                WorkflowEvent::BulkActivityBatchScheduled { batch_id, .. } => {
                    Some(batch_id.clone())
                }
                _ => None,
            })
            .expect("expected bulk schedule event");
        let resumed = artifact
            .execute_after_bulk_completion_with_turn(
                "join",
                &batch_id,
                &serde_json::json!({
                    "batchId": batch_id,
                    "status": "completed",
                    "totalItems": 3,
                    "succeededItems": 3,
                    "failedItems": 0,
                    "cancelledItems": 0,
                    "chunkCount": 2,
                    "resultHandle": { "batchId": batch_id },
                }),
                plan.execution_state,
                ExecutionTurnContext {
                    event_id: uuid::Uuid::now_v7(),
                    occurred_at: DateTime::from_timestamp_millis(1_700_000_000_100).unwrap(),
                },
            )
            .unwrap();

        assert_eq!(resumed.final_state, "done");
        assert!(matches!(
            resumed.emissions.last(),
            Some(ExecutionEmission {
                event: WorkflowEvent::WorkflowCompleted { output },
                ..
            }) if output["succeededItems"] == json!(3)
        ));
    }

    #[test]
    fn bulk_failure_uses_error_transition() {
        let artifact = bulk_artifact();
        let plan = artifact
            .execute_trigger_with_turn(
                &json!({"items": [{"value": 1}, {"value": 2}]}),
                ExecutionTurnContext {
                    event_id: uuid::Uuid::parse_str("bbbbbbbb-cccc-dddd-eeee-ffffffffffff")
                        .unwrap(),
                    occurred_at: DateTime::from_timestamp_millis(1_700_000_000_000).unwrap(),
                },
            )
            .unwrap();

        let batch_id = plan
            .emissions
            .iter()
            .find_map(|emission| match &emission.event {
                WorkflowEvent::BulkActivityBatchScheduled { batch_id, .. } => {
                    Some(batch_id.clone())
                }
                _ => None,
            })
            .expect("expected bulk schedule event");
        let failed = artifact
            .execute_after_bulk_failure_with_turn(
                "join",
                &batch_id,
                &json!({
                    "batchId": batch_id,
                    "status": "failed",
                    "message": "boom",
                    "totalItems": 2,
                    "succeededItems": 0,
                    "failedItems": 2,
                    "cancelledItems": 0,
                    "chunkCount": 1,
                }),
                plan.execution_state,
                ExecutionTurnContext {
                    event_id: uuid::Uuid::now_v7(),
                    occurred_at: DateTime::from_timestamp_millis(1_700_000_000_100).unwrap(),
                },
            )
            .unwrap();

        assert_eq!(failed.final_state, "fail");
        assert!(matches!(
            failed.emissions.last(),
            Some(ExecutionEmission {
                event: WorkflowEvent::WorkflowFailed { reason },
                ..
            }) if reason.contains("boom")
        ));
    }

    #[test]
    fn bulk_all_settled_reducer_treats_failure_as_settled_success() {
        let artifact = bulk_artifact_with_reducer("all_settled");
        let plan = artifact
            .execute_trigger_with_turn(
                &json!({"items": [{"value": 1}, {"value": 2}]}),
                ExecutionTurnContext {
                    event_id: uuid::Uuid::now_v7(),
                    occurred_at: DateTime::from_timestamp_millis(1_700_000_000_000).unwrap(),
                },
            )
            .unwrap();
        let batch_id = plan
            .emissions
            .iter()
            .find_map(|emission| match &emission.event {
                WorkflowEvent::BulkActivityBatchScheduled { batch_id, .. } => {
                    Some(batch_id.clone())
                }
                _ => None,
            })
            .expect("expected bulk schedule event");

        let settled = artifact
            .execute_after_bulk_failure_with_turn(
                "join",
                &batch_id,
                &json!({
                    "batchId": batch_id,
                    "status": "failed",
                    "message": "boom",
                    "totalItems": 2,
                    "succeededItems": 1,
                    "failedItems": 1,
                    "cancelledItems": 0,
                    "chunkCount": 1,
                }),
                plan.execution_state,
                ExecutionTurnContext {
                    event_id: uuid::Uuid::now_v7(),
                    occurred_at: DateTime::from_timestamp_millis(1_700_000_000_100).unwrap(),
                },
            )
            .unwrap();

        assert!(matches!(
            settled.emissions.last(),
            Some(ExecutionEmission {
                event: WorkflowEvent::WorkflowCompleted { output },
                ..
            }) if output["status"] == json!("settled")
                && output["terminalStatus"] == json!("failed")
                && output["failedItems"] == json!(1)
        ));
    }

    #[test]
    fn bulk_count_reducer_emits_terminal_count_summary() {
        let artifact = bulk_artifact_with_reducer("count");
        let plan = artifact
            .execute_trigger_with_turn(
                &json!({"items": [{"value": 1}, {"value": 2}, {"value": 3}]}),
                ExecutionTurnContext {
                    event_id: uuid::Uuid::now_v7(),
                    occurred_at: DateTime::from_timestamp_millis(1_700_000_000_000).unwrap(),
                },
            )
            .unwrap();
        let batch_id = plan
            .emissions
            .iter()
            .find_map(|emission| match &emission.event {
                WorkflowEvent::BulkActivityBatchScheduled { batch_id, .. } => {
                    Some(batch_id.clone())
                }
                _ => None,
            })
            .expect("expected bulk schedule event");

        let resumed = artifact
            .execute_after_bulk_completion_with_turn(
                "join",
                &batch_id,
                &json!({
                    "batchId": batch_id,
                    "status": "completed",
                    "totalItems": 3,
                    "succeededItems": 2,
                    "failedItems": 1,
                    "cancelledItems": 0,
                    "chunkCount": 2,
                }),
                plan.execution_state,
                ExecutionTurnContext {
                    event_id: uuid::Uuid::now_v7(),
                    occurred_at: DateTime::from_timestamp_millis(1_700_000_000_100).unwrap(),
                },
            )
            .unwrap();

        assert!(matches!(
            resumed.emissions.last(),
            Some(ExecutionEmission {
                event: WorkflowEvent::WorkflowCompleted { output },
                ..
            }) if output["status"] == json!("settled")
                && output["count"] == json!(3)
                && output.get("resultHandle").is_none()
        ));
    }

    #[test]
    fn bulk_avg_reducer_uses_reducer_output_summary() {
        let artifact = bulk_artifact_with_reducer("avg");
        let plan = artifact
            .execute_trigger_with_turn(
                &json!({"items": [{"value": 1}, {"value": 2}, {"value": 3}]}),
                ExecutionTurnContext {
                    event_id: uuid::Uuid::now_v7(),
                    occurred_at: DateTime::from_timestamp_millis(1_700_000_000_000).unwrap(),
                },
            )
            .unwrap();
        let batch_id = plan
            .emissions
            .iter()
            .find_map(|emission| match &emission.event {
                WorkflowEvent::BulkActivityBatchScheduled { batch_id, .. } => {
                    Some(batch_id.clone())
                }
                _ => None,
            })
            .expect("expected bulk schedule event");

        let resumed = artifact
            .execute_after_bulk_completion_with_turn(
                "join",
                &batch_id,
                &json!({
                    "batchId": batch_id,
                    "status": "completed",
                    "totalItems": 3,
                    "succeededItems": 3,
                    "failedItems": 0,
                    "cancelledItems": 0,
                    "chunkCount": 2,
                    "reducerOutput": 20.0,
                }),
                plan.execution_state,
                ExecutionTurnContext {
                    event_id: uuid::Uuid::now_v7(),
                    occurred_at: DateTime::from_timestamp_millis(1_700_000_000_100).unwrap(),
                },
            )
            .unwrap();

        assert!(matches!(
            resumed.emissions.last(),
            Some(ExecutionEmission {
                event: WorkflowEvent::WorkflowCompleted { output },
                ..
            }) if output["terminalStatus"] == json!("completed")
                && output["avg"] == json!(20.0)
                && output.get("resultHandle").is_none()
        ));
    }

    #[test]
    fn pending_cancellation_pauses_at_non_cancellable_boundary() {
        let artifact = CompiledWorkflowArtifact::new(
            "non-cancellable-boundary",
            1,
            "test",
            ArtifactEntrypoint { module: "workflow.ts".to_owned(), export: "workflow".to_owned() },
            CompiledWorkflow {
                initial_state: "shielded_step".to_owned(),
                states: BTreeMap::from([
                    (
                        "shielded_step".to_owned(),
                        CompiledStateNode::Step {
                            handler: "benchmark.echo".to_owned(),
                            input: Expression::Literal {
                                value: Value::String("inside".to_owned()),
                            },
                            next: Some("after_scope".to_owned()),
                            task_queue: None,
                            retry: None,
                            config: None,
                            schedule_to_start_timeout_ms: None,
                            start_to_close_timeout_ms: None,
                            heartbeat_timeout_ms: None,
                            output_var: None,
                            on_error: None,
                        },
                    ),
                    (
                        "after_scope".to_owned(),
                        CompiledStateNode::Assign {
                            actions: vec![Assignment {
                                target: "done".to_owned(),
                                expr: Expression::Literal { value: Value::Bool(true) },
                            }],
                            next: "outside_step".to_owned(),
                        },
                    ),
                    (
                        "outside_step".to_owned(),
                        CompiledStateNode::Step {
                            handler: "benchmark.echo".to_owned(),
                            input: Expression::Literal {
                                value: Value::String("outside".to_owned()),
                            },
                            next: Some("finish".to_owned()),
                            task_queue: None,
                            retry: None,
                            config: None,
                            schedule_to_start_timeout_ms: None,
                            start_to_close_timeout_ms: None,
                            heartbeat_timeout_ms: None,
                            output_var: None,
                            on_error: None,
                        },
                    ),
                    ("finish".to_owned(), CompiledStateNode::Succeed { output: None }),
                ]),
                params: Vec::new(),
                non_cancellable_states: BTreeSet::from(["shielded_step".to_owned()]),
            },
        );

        let mut execution_state = ArtifactExecutionState::default();
        execution_state.pending_workflow_cancellation = Some("cancel later".to_owned());

        let plan = artifact
            .execute_after_step_completion_with_turn(
                "shielded_step",
                "shielded_step",
                &Value::String("ok".to_owned()),
                execution_state,
                CompiledWorkflowArtifact::synthetic_turn_context("non-cancellable-boundary"),
            )
            .unwrap();

        assert_eq!(plan.final_state, "after_scope");
        assert_eq!(
            plan.execution_state.pending_workflow_cancellation.as_deref(),
            Some("cancel later")
        );
        assert!(!plan.emissions.iter().any(|emission| matches!(
            emission.event,
            WorkflowEvent::ActivityTaskScheduled {
                ref activity_id, ..
            } if activity_id == "outside_step"
        )));
    }

    #[test]
    fn wait_for_condition_timeout_schedules_timer_and_returns_false_on_fire() {
        let artifact = CompiledWorkflowArtifact::new(
            "condition-timeout",
            1,
            "test",
            ArtifactEntrypoint { module: "workflow.ts".to_owned(), export: "workflow".to_owned() },
            CompiledWorkflow {
                initial_state: "wait_ready".to_owned(),
                states: BTreeMap::from([
                    (
                        "wait_ready".to_owned(),
                        CompiledStateNode::WaitForCondition {
                            condition: Expression::Identifier { name: "approved".to_owned() },
                            next: "assign_true".to_owned(),
                            timeout_ref: Some("5s".to_owned()),
                            timeout_next: Some("assign_false".to_owned()),
                        },
                    ),
                    (
                        "assign_true".to_owned(),
                        CompiledStateNode::Assign {
                            actions: vec![Assignment {
                                target: "result".to_owned(),
                                expr: Expression::Literal { value: Value::Bool(true) },
                            }],
                            next: "done".to_owned(),
                        },
                    ),
                    (
                        "assign_false".to_owned(),
                        CompiledStateNode::Assign {
                            actions: vec![Assignment {
                                target: "result".to_owned(),
                                expr: Expression::Literal { value: Value::Bool(false) },
                            }],
                            next: "done".to_owned(),
                        },
                    ),
                    (
                        "done".to_owned(),
                        CompiledStateNode::Succeed {
                            output: Some(Expression::Identifier { name: "result".to_owned() }),
                        },
                    ),
                ]),
                params: Vec::new(),
                non_cancellable_states: BTreeSet::new(),
            },
        );

        let waiting = artifact
            .execute_trigger_with_turn(
                &json!({"approved": false}),
                ExecutionTurnContext {
                    event_id: uuid::Uuid::now_v7(),
                    occurred_at: DateTime::from_timestamp_millis(1_700_000_000_000).unwrap(),
                },
            )
            .unwrap();

        assert_eq!(waiting.final_state, "wait_ready");
        assert!(matches!(
            waiting.emissions.first(),
            Some(ExecutionEmission { event: WorkflowEvent::WorkflowStarted, .. })
        ));
        assert!(waiting.emissions.iter().any(|emission| matches!(
            emission.event,
            WorkflowEvent::TimerScheduled {
                ref timer_id, ..
            } if timer_id == "wait_ready"
        )));
        assert!(waiting.execution_state.condition_timers.contains("wait_ready"));

        let timed_out = artifact
            .execute_after_timer_with_turn(
                "wait_ready",
                "wait_ready",
                waiting.execution_state,
                ExecutionTurnContext {
                    event_id: uuid::Uuid::now_v7(),
                    occurred_at: DateTime::from_timestamp_millis(1_700_000_005_000).unwrap(),
                },
            )
            .unwrap();

        assert!(timed_out.execution_state.condition_timers.is_empty());
        assert!(matches!(
            timed_out.emissions.last(),
            Some(ExecutionEmission {
                event: WorkflowEvent::WorkflowCompleted { output },
                ..
            }) if output == &json!(false)
        ));
    }
}
