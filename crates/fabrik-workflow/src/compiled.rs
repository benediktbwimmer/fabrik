use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use fabrik_events::WorkflowEvent;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Number, Value};
use sha2::{Digest, Sha256};
use thiserror::Error;

use crate::{ExecutionEmission, RetryPolicy, StepConfig};

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
    Effect {
        connector: String,
        input: Expression,
        next: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        timeout: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        retry: Option<RetryPolicy>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        config: Option<StepConfig>,
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
    #[serde(skip)]
    pub turn_context: Option<ExecutionTurnContext>,
    #[serde(skip)]
    pub pending_markers: Vec<(String, Value)>,
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
            CompiledStateNode::WaitForEvent { .. } | CompiledStateNode::WaitForTimer { .. }
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

    pub fn hash(&self) -> String {
        let mut clone = self.clone();
        clone.artifact_hash.clear();
        let value = serde_json::to_value(&clone).expect("compiled artifact serialization failed");
        let encoded = serde_json::to_vec(&canonicalize_value(value))
            .expect("compiled artifact serialization failed");
        let digest = Sha256::digest(encoded);
        format!("{digest:x}")
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
        let state = self
            .workflow
            .states
            .get(wait_state)
            .ok_or_else(|| CompiledWorkflowError::UnknownState(wait_state.to_owned()))?;
        match state {
            CompiledStateNode::WaitForEvent { event_type, next, output_var }
                if event_type == signal_type =>
            {
                if let Some(output_var) = output_var {
                    execution_state.bindings.insert(output_var.clone(), payload.clone());
                }
                self.execute_from_state(next, execution_state, false)
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
        let state = self
            .workflow
            .states
            .get(wait_state)
            .ok_or_else(|| CompiledWorkflowError::UnknownState(wait_state.to_owned()))?;
        match state {
            CompiledStateNode::WaitForTimer { next, .. } if wait_state == timer_id => {
                self.execute_from_state(next, execution_state, false)
            }
            CompiledStateNode::WaitForTimer { .. } => Err(CompiledWorkflowError::UnexpectedTimer {
                expected: wait_state.to_owned(),
                received: timer_id.to_owned(),
            }),
            _ => Err(CompiledWorkflowError::NotWaitingOnTimer(wait_state.to_owned())),
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
        let state = self
            .workflow
            .states
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
                self.execute_from_state(next, execution_state, false)
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
        let state = self
            .workflow
            .states
            .get(step_state)
            .ok_or_else(|| CompiledWorkflowError::UnknownState(step_state.to_owned()))?;
        match state {
            CompiledStateNode::Step { on_error: Some(on_error), .. } if step_state == step_id => {
                if let Some(error_var) = &on_error.error_var {
                    execution_state
                        .bindings
                        .insert(error_var.clone(), Value::String(error.to_owned()));
                }
                self.execute_from_state(&on_error.next, execution_state, false)
            }
            CompiledStateNode::Step { on_error: None, .. } if step_state == step_id => {
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
    ) -> Result<Option<&RetryPolicy>, CompiledWorkflowError> {
        let state = self
            .workflow
            .states
            .get(step_state)
            .ok_or_else(|| CompiledWorkflowError::UnknownState(step_state.to_owned()))?;
        match state {
            CompiledStateNode::Step { retry, .. } => Ok(retry.as_ref()),
            _ => Err(CompiledWorkflowError::NotWaitingOnStep(step_state.to_owned())),
        }
    }

    pub fn effect_retry(
        &self,
        effect_state: &str,
    ) -> Result<Option<&RetryPolicy>, CompiledWorkflowError> {
        let state = self
            .workflow
            .states
            .get(effect_state)
            .ok_or_else(|| CompiledWorkflowError::UnknownState(effect_state.to_owned()))?;
        match state {
            CompiledStateNode::Effect { retry, .. } => Ok(retry.as_ref()),
            _ => Err(CompiledWorkflowError::NotWaitingOnEffect(effect_state.to_owned())),
        }
    }

    pub fn effect_timeout(
        &self,
        effect_state: &str,
    ) -> Result<Option<&str>, CompiledWorkflowError> {
        let state = self
            .workflow
            .states
            .get(effect_state)
            .ok_or_else(|| CompiledWorkflowError::UnknownState(effect_state.to_owned()))?;
        match state {
            CompiledStateNode::Effect { timeout, .. } => Ok(timeout.as_deref()),
            _ => Err(CompiledWorkflowError::NotWaitingOnEffect(effect_state.to_owned())),
        }
    }

    pub fn step_details(
        &self,
        step_state: &str,
        execution_state: &ArtifactExecutionState,
    ) -> Result<(String, Option<StepConfig>, Value), CompiledWorkflowError> {
        let state = self
            .workflow
            .states
            .get(step_state)
            .ok_or_else(|| CompiledWorkflowError::UnknownState(step_state.to_owned()))?;
        match state {
            CompiledStateNode::Step { handler, input, config, .. } => {
                let mut execution_state = execution_state.clone();
                let input = evaluate_expression(input, &mut execution_state, &self.helpers)?;
                Ok((handler.clone(), config.clone(), input))
            }
            _ => Err(CompiledWorkflowError::NotWaitingOnStep(step_state.to_owned())),
        }
    }

    pub fn step_descriptor(
        &self,
        step_state: &str,
    ) -> Result<(String, Option<StepConfig>), CompiledWorkflowError> {
        let state = self
            .workflow
            .states
            .get(step_state)
            .ok_or_else(|| CompiledWorkflowError::UnknownState(step_state.to_owned()))?;
        match state {
            CompiledStateNode::Step { handler, config, .. } => {
                Ok((handler.clone(), config.clone()))
            }
            _ => Err(CompiledWorkflowError::NotWaitingOnStep(step_state.to_owned())),
        }
    }

    pub fn effect_descriptor(
        &self,
        effect_state: &str,
    ) -> Result<(String, Option<StepConfig>), CompiledWorkflowError> {
        let state = self
            .workflow
            .states
            .get(effect_state)
            .ok_or_else(|| CompiledWorkflowError::UnknownState(effect_state.to_owned()))?;
        match state {
            CompiledStateNode::Effect { connector, config, .. } => {
                Ok((connector.clone(), config.clone()))
            }
            _ => Err(CompiledWorkflowError::NotWaitingOnEffect(effect_state.to_owned())),
        }
    }

    pub fn effect_details(
        &self,
        effect_state: &str,
        execution_state: &ArtifactExecutionState,
    ) -> Result<(String, Option<StepConfig>, Value), CompiledWorkflowError> {
        let state = self
            .workflow
            .states
            .get(effect_state)
            .ok_or_else(|| CompiledWorkflowError::UnknownState(effect_state.to_owned()))?;
        match state {
            CompiledStateNode::Effect { connector, input, config, .. } => {
                let mut execution_state = execution_state.clone();
                let input = evaluate_expression(input, &mut execution_state, &self.helpers)?;
                Ok((connector.clone(), config.clone(), input))
            }
            _ => Err(CompiledWorkflowError::NotWaitingOnEffect(effect_state.to_owned())),
        }
    }

    pub fn execute_after_effect_completion(
        &self,
        effect_state: &str,
        effect_id: &str,
        output: &Value,
        execution_state: ArtifactExecutionState,
    ) -> Result<CompiledExecutionPlan, CompiledWorkflowError> {
        self.execute_after_effect_completion_with_turn(
            effect_state,
            effect_id,
            output,
            execution_state,
            Self::synthetic_turn_context("effect_completion"),
        )
    }

    pub fn execute_after_effect_completion_with_turn(
        &self,
        effect_state: &str,
        effect_id: &str,
        output: &Value,
        mut execution_state: ArtifactExecutionState,
        turn_context: ExecutionTurnContext,
    ) -> Result<CompiledExecutionPlan, CompiledWorkflowError> {
        execution_state.turn_context = Some(turn_context);
        let state = self
            .workflow
            .states
            .get(effect_state)
            .ok_or_else(|| CompiledWorkflowError::UnknownState(effect_state.to_owned()))?;
        match state {
            CompiledStateNode::Effect { next, output_var, .. } if effect_state == effect_id => {
                if let Some(output_var) = output_var {
                    execution_state.bindings.insert(output_var.clone(), output.clone());
                }
                self.execute_from_state(next, execution_state, false)
            }
            CompiledStateNode::Effect { .. } => Err(CompiledWorkflowError::UnexpectedEffect {
                expected: effect_state.to_owned(),
                received: effect_id.to_owned(),
            }),
            _ => Err(CompiledWorkflowError::NotWaitingOnEffect(effect_state.to_owned())),
        }
    }

    pub fn execute_after_effect_failure(
        &self,
        effect_state: &str,
        effect_id: &str,
        error: &str,
        execution_state: ArtifactExecutionState,
    ) -> Result<CompiledExecutionPlan, CompiledWorkflowError> {
        self.execute_after_effect_failure_with_turn(
            effect_state,
            effect_id,
            error,
            execution_state,
            Self::synthetic_turn_context("effect_failure"),
        )
    }

    pub fn execute_after_effect_failure_with_turn(
        &self,
        effect_state: &str,
        effect_id: &str,
        error: &str,
        mut execution_state: ArtifactExecutionState,
        turn_context: ExecutionTurnContext,
    ) -> Result<CompiledExecutionPlan, CompiledWorkflowError> {
        execution_state.turn_context = Some(turn_context);
        let state = self
            .workflow
            .states
            .get(effect_state)
            .ok_or_else(|| CompiledWorkflowError::UnknownState(effect_state.to_owned()))?;
        match state {
            CompiledStateNode::Effect { on_error: Some(on_error), .. }
                if effect_state == effect_id =>
            {
                if let Some(error_var) = &on_error.error_var {
                    execution_state
                        .bindings
                        .insert(error_var.clone(), Value::String(error.to_owned()));
                }
                self.execute_from_state(&on_error.next, execution_state, false)
            }
            CompiledStateNode::Effect { on_error: None, .. } if effect_state == effect_id => {
                Ok(CompiledExecutionPlan {
                    workflow_version: self.definition_version,
                    final_state: effect_state.to_owned(),
                    emissions: vec![ExecutionEmission {
                        event: WorkflowEvent::WorkflowFailed { reason: error.to_owned() },
                        state: Some(effect_state.to_owned()),
                    }],
                    execution_state,
                    context: Some(Value::String(error.to_owned())),
                    output: Some(Value::String(error.to_owned())),
                })
            }
            CompiledStateNode::Effect { .. } => Err(CompiledWorkflowError::UnexpectedEffect {
                expected: effect_state.to_owned(),
                received: effect_id.to_owned(),
            }),
            _ => Err(CompiledWorkflowError::NotWaitingOnEffect(effect_state.to_owned())),
        }
    }

    fn execute_from_state(
        &self,
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

            let state = self
                .workflow
                .states
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
                    emissions.push(ExecutionEmission {
                        event: WorkflowEvent::StepScheduled {
                            step_id: current_state.clone(),
                            attempt: 1,
                            input,
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
                CompiledStateNode::Effect { connector, input: effect_input, .. } => {
                    let input =
                        evaluate_expression(effect_input, &mut execution_state, &self.helpers)?;
                    context = Some(input.clone());
                    emit_pending_markers(&mut emissions, &mut execution_state, &current_state);
                    emissions.push(ExecutionEmission {
                        event: WorkflowEvent::EffectRequested {
                            effect_id: current_state.clone(),
                            connector: connector.clone(),
                            attempt: 1,
                            input,
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
                    emissions.push(ExecutionEmission {
                        event: WorkflowEvent::WorkflowCompleted { output: value },
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
                    emissions.push(ExecutionEmission {
                        event: WorkflowEvent::WorkflowFailed { reason },
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
            Self::Effect { next, on_error, .. } => std::iter::once(next.as_str())
                .chain(on_error.iter().map(|transition| transition.next.as_str()))
                .collect(),
            Self::WaitForEvent { next, .. } | Self::WaitForTimer { next, .. } => {
                vec![next.as_str()]
            }
            Self::Succeed { .. } | Self::Fail { .. } | Self::ContinueAsNew { .. } => Vec::new(),
        }
    }
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
    #[error("compiled workflow state {0} is not waiting on an effect")]
    NotWaitingOnEffect(String),
    #[error("unexpected effect completion, expected {expected}, received {received}")]
    UnexpectedEffect { expected: String, received: String },
    #[error("compiled workflow step {0} is missing a continuation")]
    MissingContinuation(String),
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
            Some(ExecutionEmission { event: WorkflowEvent::StepScheduled { step_id, .. }, .. })
            if step_id == "step"
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

    fn effect_artifact() -> CompiledWorkflowArtifact {
        let workflow = CompiledWorkflow {
            initial_state: "effect".to_owned(),
            states: BTreeMap::from([
                (
                    "effect".to_owned(),
                    CompiledStateNode::Effect {
                        connector: "core.echo".to_owned(),
                        input: Expression::Identifier { name: "input".to_owned() },
                        next: "done".to_owned(),
                        timeout: Some("30s".to_owned()),
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
                        reason: Some(Expression::Identifier { name: "err".to_owned() }),
                    },
                ),
            ]),
        };
        CompiledWorkflowArtifact::new(
            "effect-demo",
            1,
            "test",
            ArtifactEntrypoint { module: "workflow.ts".to_owned(), export: "workflow".to_owned() },
            workflow,
        )
    }

    #[test]
    fn executes_effect_path() {
        let artifact = effect_artifact();
        let plan = artifact.execute_trigger(&json!({"message": "hello"})).unwrap();
        assert!(matches!(
            plan.emissions.last(),
            Some(ExecutionEmission {
                event: WorkflowEvent::EffectRequested { effect_id, connector, .. },
                ..
            }) if effect_id == "effect" && connector == "core.echo"
        ));
    }

    #[test]
    fn resumes_after_effect_completion() {
        let artifact = effect_artifact();
        let plan = artifact
            .execute_after_effect_completion(
                "effect",
                "effect",
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
    fn resumes_after_effect_failure() {
        let artifact = effect_artifact();
        let plan = artifact
            .execute_after_effect_failure(
                "effect",
                "effect",
                "boom",
                ArtifactExecutionState::default(),
            )
            .unwrap();
        assert!(matches!(
            plan.emissions.last(),
            Some(ExecutionEmission { event: WorkflowEvent::WorkflowFailed { reason }, .. })
                if reason == "boom"
        ));
    }
}
