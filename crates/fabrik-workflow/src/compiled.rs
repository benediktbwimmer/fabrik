use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use fabrik_events::WorkflowEvent;
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
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CompiledQueryHandler {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub arg_name: Option<String>,
    pub expr: Expression,
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
        retry: Option<RetryPolicy>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        config: Option<StepConfig>,
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
        retry: Option<RetryPolicy>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        config: Option<StepConfig>,
    },
    StartBulkActivity {
        activity_type: String,
        items: Expression,
        next: String,
        handle_var: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        task_queue: Option<Expression>,
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
    WaitForChild {
        child_ref_var: String,
        next: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        output_var: Option<String>,
    },
    WaitForEvent {
        event_type: String,
        next: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        output_var: Option<String>,
    },
    WaitForTimer {
        timer_ref: String,
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
    Object {
        fields: BTreeMap<String, Expression>,
    },
    Call {
        callee: String,
        args: Vec<Expression>,
    },
    SideEffect {
        marker_id: String,
        expr: Box<Expression>,
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
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct HelperFunction {
    pub params: Vec<String>,
    pub body: Expression,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
pub struct ArtifactExecutionState {
    #[serde(default)]
    pub bindings: BTreeMap<String, Value>,
    #[serde(default)]
    pub markers: BTreeMap<String, Value>,
    #[serde(default)]
    pub active_update: Option<ActiveUpdateState>,
    #[serde(skip)]
    pub turn_context: Option<ExecutionTurnContext>,
    #[serde(skip)]
    pub pending_markers: Vec<(String, Value)>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ActiveUpdateState {
    pub update_id: String,
    pub update_name: String,
    pub return_state: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct FanOutExecutionState {
    pub origin_state: String,
    pub wait_state: String,
    pub task_queue: String,
    pub inputs: Vec<Value>,
    pub pending_activity_ids: Vec<String>,
    pub results: Vec<Option<Value>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct BulkActivityExecutionState {
    pub batch_id: String,
    pub origin_state: String,
    pub wait_state: String,
    pub activity_type: String,
    pub task_queue: String,
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
                | CompiledStateNode::WaitForTimer { .. }
                | CompiledStateNode::WaitForAllActivities { .. }
        ))
    }

    pub fn expected_signal_type(
        &self,
        state_id: &str,
    ) -> Result<Option<&str>, CompiledWorkflowError> {
        let state = self
            .workflow
            .states
            .get(state_id)
            .ok_or_else(|| CompiledWorkflowError::UnknownState(state_id.to_owned()))?;
        Ok(match state {
            CompiledStateNode::WaitForEvent { event_type, .. } => Some(event_type.as_str()),
            _ => None,
        })
    }

    pub fn has_query(&self, query_name: &str) -> bool {
        self.queries.contains_key(query_name)
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
            .or_else(|| self.updates.values().find_map(|handler| handler.states.get(state_id)))
    }

    fn states_for<'a>(
        &'a self,
        state_id: &str,
        execution_state: &ArtifactExecutionState,
    ) -> Option<&'a BTreeMap<String, CompiledStateNode>> {
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
        execution_state.bindings.insert("input".to_owned(), input.clone());
        execution_state.turn_context = Some(turn_context);
        self.execute_from_state(&self.workflow.initial_state, execution_state, true)
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
            CompiledStateNode::WaitForEvent { event_type, .. } => {
                Err(CompiledWorkflowError::UnexpectedSignal {
                    expected: event_type.clone(),
                    received: signal_type.to_owned(),
                })
            }
            _ => Err(CompiledWorkflowError::NotWaitingOnSignal(wait_state.to_owned())),
        }
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
            CompiledStateNode::WaitForChild { child_ref_var, next, output_var } => {
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
        let state = self
            .state_by_id(wait_state)
            .ok_or_else(|| CompiledWorkflowError::UnknownState(wait_state.to_owned()))?;
        match state {
            CompiledStateNode::WaitForChild { child_ref_var, .. } => {
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
                execution_state.bindings.remove(bulk_ref_var);
                if let Some(output_var) = output_var {
                    execution_state.bindings.insert(output_var.clone(), summary.clone());
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
            CompiledStateNode::WaitForBulkActivity { bulk_ref_var, on_error, .. } => {
                let bulk = self.bulk_binding(wait_state, bulk_ref_var, &execution_state)?;
                if bulk.batch_id != batch_id {
                    return Err(CompiledWorkflowError::UnexpectedBulkBatch {
                        expected: bulk.batch_id,
                        received: batch_id.to_owned(),
                    });
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
                    || fanout_index >= fanout.results.len()
                    || !fanout.pending_activity_ids.iter().any(|activity_id| activity_id == step_id)
                {
                    return Err(CompiledWorkflowError::UnexpectedFanOutActivity {
                        state: step_state.to_owned(),
                        activity_id: step_id.to_owned(),
                    });
                }
                fanout.pending_activity_ids.retain(|activity_id| activity_id != step_id);
                fanout.results[fanout_index] = Some(output.clone());
                if fanout.pending_activity_ids.is_empty() {
                    let ordered_results = Value::Array(
                        fanout
                            .results
                            .iter()
                            .cloned()
                            .map(|value| value.unwrap_or(Value::Null))
                            .collect(),
                    );
                    execution_state.bindings.remove(fanout_ref_var);
                    if let Some(output_var) = output_var {
                        execution_state.bindings.insert(output_var.clone(), ordered_results);
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
                        context: Some(output.clone()),
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
        self.execute_after_step_failure_with_turn(
            step_state,
            step_id,
            error,
            execution_state,
            Self::synthetic_turn_context("step_failure"),
        )
    }

    pub fn execute_after_step_failure_with_turn(
        &self,
        step_state: &str,
        step_id: &str,
        error: &str,
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
            CompiledStateNode::WaitForAllActivities { fanout_ref_var, on_error, .. } => {
                let fanout = self.fanout_binding(step_state, fanout_ref_var, &execution_state)?;
                let Some((origin_state, fanout_index)) = parse_fanout_activity_id(step_id) else {
                    return Err(CompiledWorkflowError::InvalidFanOutActivityId(step_id.to_owned()));
                };
                if fanout.origin_state != origin_state
                    || fanout_index >= fanout.results.len()
                    || !fanout.pending_activity_ids.iter().any(|activity_id| activity_id == step_id)
                {
                    return Err(CompiledWorkflowError::UnexpectedFanOutActivity {
                        state: step_state.to_owned(),
                        activity_id: step_id.to_owned(),
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
            CompiledStateNode::FanOut { activity_type, config, .. } => {
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
                Ok((
                    activity_type.clone(),
                    config.clone(),
                    fanout.inputs.get(index).cloned().unwrap_or(Value::Null),
                ))
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
            (fanout.origin_state == origin_state && index < fanout.inputs.len())
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
                }
                CompiledStateNode::Choice { condition, then_next, else_next } => {
                    let value =
                        evaluate_expression(condition, &mut execution_state, &self.helpers)?;
                    emit_pending_markers(&mut emissions, &mut execution_state, &current_state);
                    current_state =
                        if truthy(&value) { then_next.clone() } else { else_next.clone() };
                }
                CompiledStateNode::Step { input: step_input, .. } => {
                    let input =
                        evaluate_expression(step_input, &mut execution_state, &self.helpers)?;
                    context = Some(input.clone());
                    emit_pending_markers(&mut emissions, &mut execution_state, &current_state);
                    let (activity_type, config) = self.step_descriptor(&current_state)?;
                    emissions.push(ExecutionEmission {
                        event: WorkflowEvent::ActivityTaskScheduled {
                            activity_id: current_state.clone(),
                            activity_type,
                            task_queue: "default".to_owned(),
                            attempt: 1,
                            input,
                            config: config.map(|config| {
                                serde_json::to_value(config).expect("step config serializes")
                            }),
                            state: Some(current_state.clone()),
                            schedule_to_start_timeout_ms: None,
                            start_to_close_timeout_ms: None,
                            heartbeat_timeout_ms: None,
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
                    config,
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
                                execution_state
                                    .bindings
                                    .insert(output_var.clone(), Value::Array(Vec::new()));
                            }
                            current_state = after_wait.clone();
                            continue;
                        }
                        return Err(CompiledWorkflowError::NotWaitingOnFanOut(next.clone()));
                    }

                    let mut pending_activity_ids = Vec::with_capacity(items.len());
                    let mut results = Vec::with_capacity(items.len());
                    for (index, item) in items.iter().enumerate() {
                        let activity_id = build_fanout_activity_id(&current_state, index);
                        pending_activity_ids.push(activity_id.clone());
                        results.push(None);
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
                                schedule_to_start_timeout_ms: None,
                                start_to_close_timeout_ms: None,
                                heartbeat_timeout_ms: None,
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
                            inputs: items.clone(),
                            pending_activity_ids,
                            results,
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
                    let max_attempts = retry.as_ref().map(|policy| policy.max_attempts).unwrap_or(1);
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
                                    bulk_success_summary(&batch_id, 0, 0, 0, 0, 0),
                                );
                            }
                            current_state = after_wait.clone();
                            continue;
                        }
                        return Err(CompiledWorkflowError::NotWaitingOnBulk(next.clone()));
                    }
                    let total_items = items_len_to_u32(items.len())?;
                    emit_pending_markers(&mut emissions, &mut execution_state, &current_state);
                    emissions.push(ExecutionEmission {
                        event: WorkflowEvent::BulkActivityBatchScheduled {
                            batch_id: batch_id.clone(),
                            activity_type: activity_type.clone(),
                            task_queue: task_queue.clone(),
                            items: items.clone(),
                            chunk_size,
                            max_attempts,
                            retry_delay_ms,
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
                CompiledStateNode::WaitForTimer { timer_ref, .. } => {
                    emit_pending_markers(&mut emissions, &mut execution_state, &current_state);
                    let fire_at = Utc::now()
                        + crate::parse_timer_ref(timer_ref).map_err(|source| {
                            CompiledWorkflowError::InvalidTimer {
                                state: current_state.clone(),
                                timer_ref: timer_ref.clone(),
                                details: source.to_string(),
                            }
                        })?;
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
                        emissions.push(ExecutionEmission {
                            event: WorkflowEvent::WorkflowUpdateCompleted {
                                update_id: active_update.update_id,
                                update_name: active_update.update_name,
                                output: value.clone(),
                            },
                            state: Some(return_state.clone()),
                        });
                        current_state = return_state;
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
            | Self::WaitForEvent { next, .. }
            | Self::WaitForTimer { next, .. }
            | Self::WaitForChild { next, .. } => {
                vec![next.as_str()]
            }
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
    total_items: u32,
    succeeded_items: u32,
    failed_items: u32,
    cancelled_items: u32,
    chunk_count: u32,
) -> Value {
    serde_json::json!({
        "batchId": batch_id,
        "status": "completed",
        "totalItems": total_items,
        "succeededItems": succeeded_items,
        "failedItems": failed_items,
        "cancelledItems": cancelled_items,
        "chunkCount": chunk_count,
        "resultHandle": { "batchId": batch_id },
    })
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
        Expression::Object { fields } => {
            let mut object = Map::new();
            for (key, value) in fields {
                object.insert(key.clone(), evaluate_expression(value, execution_state, helpers)?);
            }
            Ok(Value::Object(object))
        }
        Expression::Call { callee, args } => {
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
            let result = evaluate_expression(&helper.body, &mut scoped_state, helpers)?;
            execution_state.markers = scoped_state.markers;
            execution_state.pending_markers = scoped_state.pending_markers;
            Ok(result)
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
    #[error("compiled workflow state {state} exceeded bulk item count limit {max} with {count} items")]
    BulkItemLimitExceeded { state: String, count: usize, max: usize },
    #[error("compiled workflow state {state} bulk item {index} serialized to {bytes} bytes, over limit {max}")]
    BulkItemTooLarge { state: String, index: usize, bytes: usize, max: usize },
    #[error("compiled workflow state {state} bulk input serialized to {bytes} bytes, over limit {max}")]
    BulkInputTooLarge { state: String, bytes: usize, max: usize },
    #[error("compiled workflow state {state} bulk chunk {chunk_index} serialized to {bytes} bytes, over limit {max}")]
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
    #[error("update {0} cannot start while another update is active")]
    UpdateAlreadyActive(String),
    #[error("unknown child workflow reference binding {0}")]
    UnknownChildReference(String),
    #[error("unknown helper function {0}")]
    UnknownHelper(String),
    #[error("helper {helper} expected {expected} args, received {received}")]
    HelperArityMismatch { helper: String, expected: usize, received: usize },
    #[error("compiled workflow expression {0} requires execution turn context")]
    MissingTurnContext(String),
    #[error("value {0} cannot be treated as a number")]
    InvalidNumericValue(String),
}

#[cfg(test)]
mod tests {
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
                        retry: None,
                        config: None,
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

    fn fanout_artifact(enable_retry: bool) -> CompiledWorkflowArtifact {
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
                        retry: enable_retry
                            .then_some(RetryPolicy { max_attempts: 2, delay: "1s".to_owned() }),
                        config: None,
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

    fn bulk_artifact() -> CompiledWorkflowArtifact {
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
                        chunk_size: Some(2),
                        retry: Some(RetryPolicy {
                            max_attempts: 2,
                            delay: "1s".to_owned(),
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
            markers: BTreeMap::new(),
            active_update: None,
            turn_context: Some(turn_context.clone()),
            pending_markers: Vec::new(),
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
    fn records_side_effect_markers_once_per_callsite() {
        let turn_context = ExecutionTurnContext {
            event_id: uuid::Uuid::parse_str("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee").unwrap(),
            occurred_at: DateTime::from_timestamp_millis(1_700_000_123_000).unwrap(),
        };
        let mut state = ArtifactExecutionState {
            bindings: BTreeMap::new(),
            markers: BTreeMap::new(),
            active_update: None,
            turn_context: Some(turn_context),
            pending_markers: Vec::new(),
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
                WorkflowEvent::BulkActivityBatchScheduled { batch_id, .. } => Some(batch_id.clone()),
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
                WorkflowEvent::BulkActivityBatchScheduled { batch_id, .. } => Some(batch_id.clone()),
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
}
