mod compiled;

use std::collections::{BTreeMap, HashSet};

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use fabrik_events::{EventEnvelope, WorkflowEvent};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use thiserror::Error;
use uuid::Uuid;

pub use compiled::{
    ActiveUpdateState, ArtifactEntrypoint, ArtifactExecutionState, CompiledExecutionPlan,
    CompiledQueryHandler, CompiledStateNode, CompiledUpdateHandler, CompiledWorkflow,
    CompiledWorkflowArtifact, CompiledWorkflowError, ErrorTransition, ExecutionTurnContext,
    Expression, HelperFunction, ParentClosePolicy, SourceLocation,
};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum ReplaySource {
    RunStart,
    SnapshotTail {
        snapshot_event_count: i64,
        snapshot_last_event_id: Uuid,
        snapshot_last_event_type: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ReplayCheckpoint {
    pub run_id: String,
    pub current_state: Option<String>,
    pub status: WorkflowStatus,
    pub context: Option<Value>,
    pub output: Option<Value>,
    pub event_count: i64,
    pub last_event_id: Uuid,
    pub last_event_type: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ReplayTransitionTraceEntry {
    pub event_id: Uuid,
    pub event_type: String,
    pub occurred_at: DateTime<Utc>,
    pub checkpoint_before: Option<ReplayCheckpoint>,
    pub checkpoint_after: ReplayCheckpoint,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ReplayFieldMismatch {
    pub field: String,
    pub expected: Value,
    pub actual: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ReplayDivergenceKind {
    ProjectionMismatch,
    SnapshotMismatch,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ReplayDivergence {
    pub kind: ReplayDivergenceKind,
    pub event_id: Option<Uuid>,
    pub event_type: Option<String>,
    pub message: String,
    pub fields: Vec<ReplayFieldMismatch>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ReplayTrace {
    pub source: ReplaySource,
    pub final_state: WorkflowInstanceState,
    pub transitions: Vec<ReplayTransitionTraceEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct WorkflowDefinition {
    pub id: String,
    pub version: u32,
    pub initial_state: String,
    pub states: BTreeMap<String, StateNode>,
}

pub fn artifact_hash(definition: &WorkflowDefinition) -> String {
    let encoded = serde_json::to_vec(definition).expect("workflow definition serialization failed");
    let digest = Sha256::digest(encoded);
    format!("{digest:x}")
}

impl WorkflowDefinition {
    pub fn validate(&self) -> Result<(), WorkflowValidationError> {
        if !self.states.contains_key(&self.initial_state) {
            return Err(WorkflowValidationError::MissingState(self.initial_state.clone()));
        }

        for state in self.states.values() {
            for target in state.next_states() {
                if !self.states.contains_key(target) {
                    return Err(WorkflowValidationError::MissingState(target.to_owned()));
                }
            }

            if let StateNode::Step { handler, retry, config, .. } = state {
                if let Some(retry) = retry {
                    if retry.max_attempts == 0 {
                        return Err(WorkflowValidationError::InvalidRetryAttempts);
                    }

                    parse_timer_ref(&retry.delay)
                        .map_err(WorkflowValidationError::InvalidRetryDelay)?;
                }

                validate_step_config(handler, config.as_ref())?;
            }
        }

        Ok(())
    }

    pub fn execute_trigger(
        &self,
        input: &Value,
    ) -> Result<WorkflowExecutionPlan, WorkflowExecutionError> {
        self.execute_from_state(&self.initial_state, input, true)
    }

    pub fn execute_after_timer(
        &self,
        wait_state: &str,
        timer_id: &str,
        input: &Value,
    ) -> Result<WorkflowExecutionPlan, WorkflowExecutionError> {
        self.validate().map_err(WorkflowExecutionError::InvalidDefinition)?;

        let state = self
            .states
            .get(wait_state)
            .ok_or_else(|| WorkflowExecutionError::UnknownState(wait_state.to_owned()))?;

        match state {
            StateNode::WaitForTimer { next, .. } if wait_state == timer_id => {
                self.execute_from_state(next, input, false)
            }
            StateNode::WaitForTimer { .. } => Err(WorkflowExecutionError::UnexpectedTimer {
                expected: wait_state.to_owned(),
                received: timer_id.to_owned(),
            }),
            _ => Err(WorkflowExecutionError::NotWaitingOnTimer(wait_state.to_owned())),
        }
    }

    pub fn execute_after_signal(
        &self,
        wait_state: &str,
        signal_type: &str,
        input: &Value,
    ) -> Result<WorkflowExecutionPlan, WorkflowExecutionError> {
        self.validate().map_err(WorkflowExecutionError::InvalidDefinition)?;

        let state = self
            .states
            .get(wait_state)
            .ok_or_else(|| WorkflowExecutionError::UnknownState(wait_state.to_owned()))?;

        match state {
            StateNode::WaitForEvent { event_type, next } if event_type == signal_type => {
                self.execute_from_state(next, input, false)
            }
            StateNode::WaitForEvent { event_type, .. } => {
                Err(WorkflowExecutionError::UnexpectedSignal {
                    expected: event_type.clone(),
                    received: signal_type.to_owned(),
                })
            }
            _ => Err(WorkflowExecutionError::NotWaitingOnSignal(wait_state.to_owned())),
        }
    }

    pub fn execute_after_step_completion(
        &self,
        step_state: &str,
        step_id: &str,
        output: &Value,
    ) -> Result<WorkflowExecutionPlan, WorkflowExecutionError> {
        self.validate().map_err(WorkflowExecutionError::InvalidDefinition)?;

        let state = self
            .states
            .get(step_state)
            .ok_or_else(|| WorkflowExecutionError::UnknownState(step_state.to_owned()))?;

        match state {
            StateNode::Step { next, .. } if step_state == step_id => match next {
                Some(next_state) => self.execute_from_state(next_state, output, false),
                None => Ok(WorkflowExecutionPlan {
                    workflow_version: self.version,
                    final_state: step_state.to_owned(),
                    emissions: vec![ExecutionEmission {
                        event: WorkflowEvent::WorkflowCompleted { output: output.clone() },
                        state: Some(step_state.to_owned()),
                    }],
                }),
            },
            StateNode::Step { .. } => Err(WorkflowExecutionError::UnexpectedStep {
                expected: step_state.to_owned(),
                received: step_id.to_owned(),
            }),
            _ => Err(WorkflowExecutionError::NotWaitingOnStep(step_state.to_owned())),
        }
    }

    pub fn schedule_retry(
        &self,
        step_state: &str,
        last_attempt: u32,
    ) -> Result<Option<RetrySchedule>, WorkflowExecutionError> {
        self.validate().map_err(WorkflowExecutionError::InvalidDefinition)?;

        let state = self
            .states
            .get(step_state)
            .ok_or_else(|| WorkflowExecutionError::UnknownState(step_state.to_owned()))?;

        let retry = match state {
            StateNode::Step { retry, .. } => retry,
            _ => return Err(WorkflowExecutionError::NotWaitingOnStep(step_state.to_owned())),
        };

        let Some(retry) = retry else {
            return Ok(None);
        };

        if last_attempt >= retry.max_attempts {
            return Ok(None);
        }

        let fire_at = Utc::now()
            + parse_timer_ref(&retry.delay).map_err(|source| {
                WorkflowExecutionError::InvalidRetry { state: step_state.to_owned(), source }
            })?;

        Ok(Some(RetrySchedule { attempt: last_attempt + 1, fire_at }))
    }

    pub fn step_handler(&self, step_id: &str) -> Result<&str, WorkflowExecutionError> {
        let state = self
            .states
            .get(step_id)
            .ok_or_else(|| WorkflowExecutionError::UnknownState(step_id.to_owned()))?;

        match state {
            StateNode::Step { handler, .. } => Ok(handler),
            _ => Err(WorkflowExecutionError::NotWaitingOnStep(step_id.to_owned())),
        }
    }

    pub fn step_config(
        &self,
        step_id: &str,
    ) -> Result<Option<&StepConfig>, WorkflowExecutionError> {
        let state = self
            .states
            .get(step_id)
            .ok_or_else(|| WorkflowExecutionError::UnknownState(step_id.to_owned()))?;

        match state {
            StateNode::Step { config, .. } => Ok(config.as_ref()),
            _ => Err(WorkflowExecutionError::NotWaitingOnStep(step_id.to_owned())),
        }
    }

    pub fn is_wait_state(&self, state_id: &str) -> Result<bool, WorkflowExecutionError> {
        let state = self
            .states
            .get(state_id)
            .ok_or_else(|| WorkflowExecutionError::UnknownState(state_id.to_owned()))?;
        Ok(matches!(state, StateNode::WaitForEvent { .. } | StateNode::WaitForTimer { .. }))
    }

    pub fn expected_signal_type(
        &self,
        state_id: &str,
    ) -> Result<Option<&str>, WorkflowExecutionError> {
        let state = self
            .states
            .get(state_id)
            .ok_or_else(|| WorkflowExecutionError::UnknownState(state_id.to_owned()))?;
        Ok(match state {
            StateNode::WaitForEvent { event_type, .. } => Some(event_type.as_str()),
            _ => None,
        })
    }

    fn execute_from_state(
        &self,
        start_state: &str,
        input: &Value,
        emit_started: bool,
    ) -> Result<WorkflowExecutionPlan, WorkflowExecutionError> {
        self.validate().map_err(WorkflowExecutionError::InvalidDefinition)?;

        let current_state = start_state.to_owned();
        let mut emissions = Vec::new();
        let last_output = input.clone();
        let mut visited = 0usize;

        if emit_started {
            emissions.push(ExecutionEmission {
                event: WorkflowEvent::WorkflowStarted,
                state: Some(current_state.clone()),
            });
        }

        loop {
            visited += 1;
            if visited > self.states.len() * 4 {
                return Err(WorkflowExecutionError::LoopDetected(current_state));
            }

            let state = self
                .states
                .get(&current_state)
                .ok_or_else(|| WorkflowExecutionError::UnknownState(current_state.clone()))?;

            match state {
                StateNode::Step { .. } => {
                    let handler = self.step_handler(&current_state)?.to_owned();
                    let config = self.step_config(&current_state)?.cloned().map(|config| {
                        serde_json::to_value(config).expect("step config serializes")
                    });
                    emissions.push(ExecutionEmission {
                        event: WorkflowEvent::ActivityTaskScheduled {
                            activity_id: current_state.clone(),
                            activity_type: handler,
                            task_queue: "default".to_owned(),
                            attempt: 1,
                            input: last_output.clone(),
                            config,
                            state: Some(current_state.clone()),
                            schedule_to_start_timeout_ms: None,
                            start_to_close_timeout_ms: None,
                            heartbeat_timeout_ms: None,
                        },
                        state: Some(current_state.clone()),
                    });
                    return Ok(WorkflowExecutionPlan {
                        workflow_version: self.version,
                        final_state: current_state,
                        emissions,
                    });
                }
                StateNode::Succeed => {
                    emissions.push(ExecutionEmission {
                        event: WorkflowEvent::WorkflowCompleted { output: last_output.clone() },
                        state: Some(current_state.clone()),
                    });
                    return Ok(WorkflowExecutionPlan {
                        workflow_version: self.version,
                        final_state: current_state,
                        emissions,
                    });
                }
                StateNode::Fail => {
                    emissions.push(ExecutionEmission {
                        event: WorkflowEvent::WorkflowFailed {
                            reason: format!("workflow entered fail state {current_state}"),
                        },
                        state: Some(current_state.clone()),
                    });
                    return Ok(WorkflowExecutionPlan {
                        workflow_version: self.version,
                        final_state: current_state,
                        emissions,
                    });
                }
                StateNode::WaitForEvent { .. } => {
                    return Ok(WorkflowExecutionPlan {
                        workflow_version: self.version,
                        final_state: current_state,
                        emissions,
                    });
                }
                StateNode::WaitForTimer { timer_ref, .. } => {
                    let fire_at = Utc::now()
                        + parse_timer_ref(timer_ref).map_err(|source| {
                            WorkflowExecutionError::InvalidTimer {
                                state: current_state.clone(),
                                source,
                            }
                        })?;
                    emissions.push(ExecutionEmission {
                        event: WorkflowEvent::TimerScheduled {
                            timer_id: current_state.clone(),
                            fire_at,
                        },
                        state: Some(current_state.clone()),
                    });
                    return Ok(WorkflowExecutionPlan {
                        workflow_version: self.version,
                        final_state: current_state,
                        emissions,
                    });
                }
            }
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum StateNode {
    Step {
        handler: String,
        next: Option<String>,
        #[serde(default)]
        retry: Option<RetryPolicy>,
        #[serde(default)]
        config: Option<StepConfig>,
    },
    WaitForEvent {
        event_type: String,
        next: String,
    },
    WaitForTimer {
        timer_ref: String,
        next: String,
    },
    Succeed,
    Fail,
}

impl StateNode {
    fn next_states(&self) -> impl Iterator<Item = &str> {
        let next = match self {
            Self::Step { next, .. } => next.as_deref(),
            Self::WaitForEvent { next, .. } => Some(next.as_str()),
            Self::WaitForTimer { next, .. } => Some(next.as_str()),
            Self::Succeed | Self::Fail => None,
        };

        next.into_iter()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RetryPolicy {
    pub max_attempts: u32,
    pub delay: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum StepConfig {
    HttpRequest(HttpRequestConfig),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct HttpRequestConfig {
    pub method: String,
    pub url: String,
    #[serde(default)]
    pub headers: BTreeMap<String, String>,
    #[serde(default)]
    pub body: Option<Value>,
    #[serde(default)]
    pub body_from_input: bool,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum WorkflowValidationError {
    #[error("workflow references unknown state {0}")]
    MissingState(String),
    #[error("step retry policy max_attempts must be at least 1")]
    InvalidRetryAttempts,
    #[error("step retry policy delay is invalid: {0}")]
    InvalidRetryDelay(TimerParseError),
    #[error("http.request steps require http_request config")]
    MissingHttpRequestConfig,
    #[error("http.request steps require a non-empty method")]
    InvalidHttpRequestMethod,
    #[error("http.request steps require a non-empty url")]
    InvalidHttpRequestUrl,
    #[error("non-http steps do not accept http_request config")]
    UnexpectedHttpRequestConfig,
}

#[derive(Debug, Clone, PartialEq)]
pub struct WorkflowExecutionPlan {
    pub workflow_version: u32,
    pub final_state: String,
    pub emissions: Vec<ExecutionEmission>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ExecutionEmission {
    pub event: WorkflowEvent,
    pub state: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RetrySchedule {
    pub attempt: u32,
    pub fire_at: DateTime<Utc>,
}

#[derive(Debug, Error, PartialEq)]
pub enum WorkflowExecutionError {
    #[error("workflow definition is invalid: {0}")]
    InvalidDefinition(#[from] WorkflowValidationError),
    #[error("workflow references unknown runtime state {0}")]
    UnknownState(String),
    #[error("workflow loop detected while executing state {0}")]
    LoopDetected(String),
    #[error("timer state {state} has invalid timer_ref: {source}")]
    InvalidTimer { state: String, source: TimerParseError },
    #[error("retry policy for step state {state} is invalid: {source}")]
    InvalidRetry { state: String, source: TimerParseError },
    #[error("workflow state {0} is not waiting on a timer")]
    NotWaitingOnTimer(String),
    #[error("unexpected timer fired, expected {expected}, received {received}")]
    UnexpectedTimer { expected: String, received: String },
    #[error("workflow state {0} is not waiting on a signal")]
    NotWaitingOnSignal(String),
    #[error("unexpected signal received, expected {expected}, received {received}")]
    UnexpectedSignal { expected: String, received: String },
    #[error("workflow state {0} is not waiting on a step")]
    NotWaitingOnStep(String),
    #[error("unexpected step completion, expected {expected}, received {received}")]
    UnexpectedStep { expected: String, received: String },
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum StepExecutionError {
    #[error("unsupported handler {0}")]
    UnsupportedHandler(String),
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum TimerParseError {
    #[error("timer_ref must not be empty")]
    Empty,
    #[error("timer_ref must end with s, m, or h, got {0}")]
    InvalidUnit(String),
    #[error("timer_ref amount is invalid: {0}")]
    InvalidAmount(String),
}

pub fn execute_handler(handler: &str, input: &Value) -> Result<Value, StepExecutionError> {
    match handler {
        "core.echo" => Ok(input.clone()),
        "core.accept" => Ok(serde_json::json!({
            "accepted": true,
            "input": input,
        })),
        "core.noop" => Ok(Value::Null),
        other => Err(StepExecutionError::UnsupportedHandler(other.to_owned())),
    }
}

fn validate_step_config(
    handler: &str,
    config: Option<&StepConfig>,
) -> Result<(), WorkflowValidationError> {
    match (handler, config) {
        ("http.request", Some(StepConfig::HttpRequest(config))) => {
            if config.method.trim().is_empty() {
                return Err(WorkflowValidationError::InvalidHttpRequestMethod);
            }
            if config.url.trim().is_empty() {
                return Err(WorkflowValidationError::InvalidHttpRequestUrl);
            }
            Ok(())
        }
        ("http.request", None) => Err(WorkflowValidationError::MissingHttpRequestConfig),
        (_, Some(StepConfig::HttpRequest(_))) => {
            Err(WorkflowValidationError::UnexpectedHttpRequestConfig)
        }
        _ => Ok(()),
    }
}

pub fn parse_timer_ref(timer_ref: &str) -> Result<chrono::TimeDelta, TimerParseError> {
    if timer_ref.is_empty() {
        return Err(TimerParseError::Empty);
    }

    let (amount, unit) = timer_ref.split_at(timer_ref.len() - 1);
    let amount =
        amount.parse::<i64>().map_err(|_| TimerParseError::InvalidAmount(timer_ref.to_owned()))?;

    match unit {
        "s" => Ok(chrono::TimeDelta::seconds(amount)),
        "m" => Ok(chrono::TimeDelta::minutes(amount)),
        "h" => Ok(chrono::TimeDelta::hours(amount)),
        _ => Err(TimerParseError::InvalidUnit(timer_ref.to_owned())),
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct WorkflowInstanceState {
    pub tenant_id: String,
    pub instance_id: String,
    pub run_id: String,
    pub definition_id: String,
    pub definition_version: Option<u32>,
    pub artifact_hash: Option<String>,
    #[serde(default = "default_workflow_task_queue")]
    pub workflow_task_queue: String,
    #[serde(default)]
    pub sticky_workflow_build_id: Option<String>,
    #[serde(default)]
    pub sticky_workflow_poller_id: Option<String>,
    pub current_state: Option<String>,
    pub context: Option<Value>,
    #[serde(default)]
    pub artifact_execution: Option<ArtifactExecutionState>,
    pub status: WorkflowStatus,
    pub input: Option<Value>,
    pub output: Option<Value>,
    pub event_count: i64,
    pub last_event_id: Uuid,
    pub last_event_type: String,
    pub updated_at: DateTime<Utc>,
}

impl WorkflowInstanceState {
    pub fn apply_event(&mut self, event: &EventEnvelope<WorkflowEvent>) {
        let was_terminal = self.status.is_terminal();
        self.event_count += 1;
        self.last_event_id = event.event_id;
        self.last_event_type = event.event_type.clone();
        self.updated_at = event.occurred_at;
        self.definition_id = event.definition_id.clone();
        self.run_id = event.run_id.clone();
        self.definition_version = Some(event.definition_version);
        self.artifact_hash = Some(event.artifact_hash.clone());
        if let Some(task_queue) = event.metadata.get("workflow_task_queue") {
            self.workflow_task_queue = task_queue.clone();
        }
        if let Some(build_id) = event.metadata.get("workflow_build_id") {
            self.sticky_workflow_build_id = Some(build_id.clone());
        }
        if let Some(poller_id) = event.metadata.get("workflow_poller_id") {
            self.sticky_workflow_poller_id = Some(poller_id.clone());
        }
        if !was_terminal {
            self.current_state =
                event.metadata.get("state").cloned().or_else(|| self.current_state.clone());
        }

        if was_terminal {
            if matches!(
                event.payload,
                WorkflowEvent::WorkflowUpdateCompleted { .. }
                    | WorkflowEvent::WorkflowUpdateRejected { .. }
            ) {
                if let Some(execution) = self.artifact_execution.as_mut() {
                    execution.active_update = None;
                }
            }
            return;
        }

        match &event.payload {
            WorkflowEvent::WorkflowTriggered { input } => {
                self.status = WorkflowStatus::Triggered;
                self.input = Some(input.clone());
                self.context = Some(input.clone());
            }
            WorkflowEvent::WorkflowStarted
            | WorkflowEvent::WorkflowArtifactPinned
            | WorkflowEvent::MarkerRecorded { .. }
            | WorkflowEvent::SignalQueued { .. }
            | WorkflowEvent::WorkflowUpdateRequested { .. }
            | WorkflowEvent::WorkflowCancellationRequested { .. }
            | WorkflowEvent::ChildWorkflowStartRequested { .. }
            | WorkflowEvent::ChildWorkflowStarted { .. } => {
                self.status = WorkflowStatus::Running;
            }
            WorkflowEvent::WorkflowContinuedAsNew { input, .. } => {
                self.status = WorkflowStatus::Running;
                self.current_state = None;
                self.output = None;
                self.context = Some(input.clone());
            }
            WorkflowEvent::SignalReceived { payload, .. }
            | WorkflowEvent::WorkflowUpdateAccepted { payload, .. } => {
                self.status = WorkflowStatus::Running;
                self.context = Some(payload.clone());
            }
            WorkflowEvent::ActivityTaskScheduled { .. }
            | WorkflowEvent::ActivityTaskStarted { .. }
            | WorkflowEvent::ActivityTaskHeartbeatRecorded { .. }
            | WorkflowEvent::ActivityTaskCancellationRequested { .. }
            | WorkflowEvent::TimerScheduled { .. }
            | WorkflowEvent::TimerFired { .. } => {
                self.status = WorkflowStatus::Running;
            }
            WorkflowEvent::ActivityTaskCompleted { output, .. } => {
                self.status = WorkflowStatus::Running;
                self.context = Some(output.clone());
            }
            WorkflowEvent::WorkflowUpdateCompleted { output, .. }
            | WorkflowEvent::ChildWorkflowCompleted { output, .. } => {
                self.status = WorkflowStatus::Running;
                self.context = Some(output.clone());
            }
            WorkflowEvent::WorkflowUpdateRejected { error, .. }
            | WorkflowEvent::ChildWorkflowFailed { error, .. }
            | WorkflowEvent::ChildWorkflowCancelled { reason: error, .. }
            | WorkflowEvent::ChildWorkflowTerminated { reason: error, .. } => {
                self.status = WorkflowStatus::Running;
                self.context = Some(Value::String(error.clone()));
            }
            WorkflowEvent::ActivityTaskFailed { error, .. }
            | WorkflowEvent::ActivityTaskCancelled { reason: error, .. } => {
                self.status = WorkflowStatus::Failed;
                self.output = Some(Value::String(error.clone()));
                self.context = Some(Value::String(error.clone()));
            }
            WorkflowEvent::ActivityTaskTimedOut { .. } => {
                self.status = WorkflowStatus::Failed;
                self.output = Some(Value::String("activity timed out".to_owned()));
                self.context = Some(Value::String("activity timed out".to_owned()));
            }
            WorkflowEvent::WorkflowFailed { reason: error } => {
                self.status = WorkflowStatus::Failed;
                self.output = Some(Value::String(error.clone()));
                self.context = Some(Value::String(error.clone()));
            }
            WorkflowEvent::WorkflowCancelled { reason } => {
                self.status = WorkflowStatus::Cancelled;
                self.output = Some(Value::String(reason.clone()));
                self.context = Some(Value::String(reason.clone()));
            }
            WorkflowEvent::WorkflowCompleted { output } => {
                self.status = WorkflowStatus::Completed;
                self.output = Some(output.clone());
                self.context = Some(output.clone());
            }
            WorkflowEvent::WorkflowTerminated { reason } => {
                self.status = WorkflowStatus::Terminated;
                self.output = Some(Value::String(reason.clone()));
                self.context = Some(Value::String(reason.clone()));
            }
        }
    }
}

impl TryFrom<&EventEnvelope<WorkflowEvent>> for WorkflowInstanceState {
    type Error = WorkflowProjectionError;

    fn try_from(event: &EventEnvelope<WorkflowEvent>) -> Result<Self, Self::Error> {
        match &event.payload {
            WorkflowEvent::WorkflowTriggered { .. }
            | WorkflowEvent::WorkflowStarted
            | WorkflowEvent::WorkflowArtifactPinned
            | WorkflowEvent::MarkerRecorded { .. }
            | WorkflowEvent::SignalQueued { .. } => {}
            _ => return Err(WorkflowProjectionError::MissingWorkflowId(event.event_type.clone())),
        }

        let mut state = Self {
            tenant_id: event.tenant_id.clone(),
            instance_id: event.instance_id.clone(),
            run_id: event.run_id.clone(),
            definition_id: event.definition_id.clone(),
            definition_version: None,
            artifact_hash: None,
            workflow_task_queue: event
                .metadata
                .get("workflow_task_queue")
                .cloned()
                .unwrap_or_else(default_workflow_task_queue),
            sticky_workflow_build_id: event.metadata.get("workflow_build_id").cloned(),
            sticky_workflow_poller_id: event.metadata.get("workflow_poller_id").cloned(),
            current_state: None,
            context: None,
            artifact_execution: None,
            status: WorkflowStatus::Triggered,
            input: None,
            output: None,
            event_count: 0,
            last_event_id: event.event_id,
            last_event_type: event.event_type.clone(),
            updated_at: event.occurred_at,
        };
        state.apply_event(event);
        Ok(state)
    }
}

fn default_workflow_task_queue() -> String {
    "default".to_owned()
}

pub fn replay_history(history: &[EventEnvelope<WorkflowEvent>]) -> Result<WorkflowInstanceState> {
    Ok(replay_history_trace(history)?.final_state)
}

pub fn replay_history_trace(history: &[EventEnvelope<WorkflowEvent>]) -> Result<ReplayTrace> {
    replay_history_trace_from_state(history, None, ReplaySource::RunStart)
}

pub fn replay_history_trace_from_snapshot(
    history_tail: &[EventEnvelope<WorkflowEvent>],
    snapshot_state: &WorkflowInstanceState,
    snapshot_event_count: i64,
    snapshot_last_event_id: Uuid,
    snapshot_last_event_type: &str,
) -> Result<ReplayTrace> {
    replay_history_trace_from_state(
        history_tail,
        Some(snapshot_state.clone()),
        ReplaySource::SnapshotTail {
            snapshot_event_count,
            snapshot_last_event_id,
            snapshot_last_event_type: snapshot_last_event_type.to_owned(),
        },
    )
}

fn replay_history_trace_from_state(
    history: &[EventEnvelope<WorkflowEvent>],
    mut state: Option<WorkflowInstanceState>,
    source: ReplaySource,
) -> Result<ReplayTrace> {
    let mut transitions = Vec::with_capacity(history.len());

    for event in history {
        let before = state.as_ref().map(replay_checkpoint);
        match state.as_mut() {
            Some(current) => current.apply_event(event),
            None => state = Some(WorkflowInstanceState::try_from(event)?),
        }
        let after = replay_checkpoint(
            state.as_ref().context("workflow history failed to materialize state")?,
        );
        transitions.push(ReplayTransitionTraceEntry {
            event_id: event.event_id,
            event_type: event.event_type.clone(),
            occurred_at: event.occurred_at,
            checkpoint_before: before,
            checkpoint_after: after,
        });
    }

    Ok(ReplayTrace {
        source,
        final_state: state.context("workflow history did not produce a replayable state")?,
        transitions,
    })
}

pub fn replay_compiled_history(
    history: &[EventEnvelope<WorkflowEvent>],
    artifact: &CompiledWorkflowArtifact,
) -> Result<WorkflowInstanceState> {
    Ok(replay_compiled_history_trace(history, artifact)?.final_state)
}

pub fn replay_compiled_history_trace(
    history: &[EventEnvelope<WorkflowEvent>],
    artifact: &CompiledWorkflowArtifact,
) -> Result<ReplayTrace> {
    let mut seen_dedupe_keys = HashSet::new();
    let trigger = history
        .iter()
        .find_map(|event| match &event.payload {
            WorkflowEvent::WorkflowTriggered { input } => Some((event, input.clone())),
            _ => None,
        })
        .context("compiled workflow history is missing WorkflowTriggered")?;
    let mut replayed = WorkflowInstanceState::try_from(trigger.0)?;
    let mut execution = artifact.execute_trigger_with_turn(
        &trigger.1,
        ExecutionTurnContext { event_id: trigger.0.event_id, occurred_at: trigger.0.occurred_at },
    )?;
    replayed.current_state = Some(execution.final_state.clone());
    replayed.context = execution.context.clone();
    replayed.output = execution.output.clone();
    replayed.artifact_execution = Some(execution.execution_state.clone());
    let mut transitions = vec![ReplayTransitionTraceEntry {
        event_id: trigger.0.event_id,
        event_type: trigger.0.event_type.clone(),
        occurred_at: trigger.0.occurred_at,
        checkpoint_before: None,
        checkpoint_after: replay_checkpoint(&replayed),
    }];
    for event in history.iter().skip(1) {
        let before = replay_checkpoint(&replayed);
        let was_terminal = replayed.status.is_terminal();
        replayed.apply_event(event);
        if was_terminal {
            transitions.push(ReplayTransitionTraceEntry {
                event_id: event.event_id,
                event_type: event.event_type.clone(),
                occurred_at: event.occurred_at,
                checkpoint_before: Some(before),
                checkpoint_after: replay_checkpoint(&replayed),
            });
            continue;
        }
        let skip_semantic = should_skip_replay_event(event, &mut seen_dedupe_keys);
        let mut advanced = false;
        if !skip_semantic {
            match &event.payload {
                WorkflowEvent::SignalReceived { signal_type, payload, .. } => {
                    execution = artifact.execute_after_signal_with_turn(
                        replayed.current_state.as_deref().unwrap_or_default(),
                        signal_type,
                        payload,
                        replayed.artifact_execution.clone().unwrap_or_default(),
                        ExecutionTurnContext {
                            event_id: event.event_id,
                            occurred_at: event.occurred_at,
                        },
                    )?;
                    advanced = true;
                }
                WorkflowEvent::WorkflowUpdateAccepted { update_id, update_name, payload } => {
                    execution = artifact.execute_update_with_turn(
                        replayed.current_state.as_deref().unwrap_or_default(),
                        update_id,
                        update_name,
                        payload,
                        replayed.artifact_execution.clone().unwrap_or_default(),
                        ExecutionTurnContext {
                            event_id: event.event_id,
                            occurred_at: event.occurred_at,
                        },
                    )?;
                    advanced = true;
                }
                WorkflowEvent::TimerFired { timer_id } => {
                    if !is_internal_control_timer(timer_id) {
                        execution = artifact.execute_after_timer_with_turn(
                            replayed.current_state.as_deref().unwrap_or_default(),
                            timer_id,
                            replayed.artifact_execution.clone().unwrap_or_default(),
                            ExecutionTurnContext {
                                event_id: event.event_id,
                                occurred_at: event.occurred_at,
                            },
                        )?;
                        advanced = true;
                    }
                }
                WorkflowEvent::ActivityTaskCompleted { activity_id, output, .. } => {
                    execution = artifact.execute_after_step_completion_with_turn(
                        replayed.current_state.as_deref().unwrap_or_default(),
                        activity_id,
                        output,
                        replayed.artifact_execution.clone().unwrap_or_default(),
                        ExecutionTurnContext {
                            event_id: event.event_id,
                            occurred_at: event.occurred_at,
                        },
                    )?;
                    advanced = true;
                }
                WorkflowEvent::ActivityTaskFailed { activity_id, error, .. } => {
                    execution = artifact.execute_after_step_failure_with_turn(
                        replayed.current_state.as_deref().unwrap_or_default(),
                        activity_id,
                        error,
                        replayed.artifact_execution.clone().unwrap_or_default(),
                        ExecutionTurnContext {
                            event_id: event.event_id,
                            occurred_at: event.occurred_at,
                        },
                    )?;
                    advanced = true;
                }
                WorkflowEvent::ActivityTaskTimedOut { activity_id, .. } => {
                    execution = artifact.execute_after_step_failure_with_turn(
                        replayed.current_state.as_deref().unwrap_or_default(),
                        activity_id,
                        "activity timed out",
                        replayed.artifact_execution.clone().unwrap_or_default(),
                        ExecutionTurnContext {
                            event_id: event.event_id,
                            occurred_at: event.occurred_at,
                        },
                    )?;
                    advanced = true;
                }
                WorkflowEvent::ActivityTaskCancelled { activity_id, reason, .. } => {
                    execution = artifact.execute_after_step_failure_with_turn(
                        replayed.current_state.as_deref().unwrap_or_default(),
                        activity_id,
                        reason,
                        replayed.artifact_execution.clone().unwrap_or_default(),
                        ExecutionTurnContext {
                            event_id: event.event_id,
                            occurred_at: event.occurred_at,
                        },
                    )?;
                    advanced = true;
                }
                WorkflowEvent::ChildWorkflowCompleted { child_id, output, .. } => {
                    execution = artifact.execute_after_child_completion_with_turn(
                        replayed.current_state.as_deref().unwrap_or_default(),
                        child_id,
                        output,
                        replayed.artifact_execution.clone().unwrap_or_default(),
                        ExecutionTurnContext {
                            event_id: event.event_id,
                            occurred_at: event.occurred_at,
                        },
                    )?;
                    advanced = true;
                }
                WorkflowEvent::ChildWorkflowFailed { child_id, error, .. }
                | WorkflowEvent::ChildWorkflowCancelled { child_id, reason: error, .. }
                | WorkflowEvent::ChildWorkflowTerminated { child_id, reason: error, .. } => {
                    execution = artifact.execute_after_child_failure_with_turn(
                        replayed.current_state.as_deref().unwrap_or_default(),
                        child_id,
                        error,
                        replayed.artifact_execution.clone().unwrap_or_default(),
                        ExecutionTurnContext {
                            event_id: event.event_id,
                            occurred_at: event.occurred_at,
                        },
                    )?;
                    advanced = true;
                }
                _ => {}
            }
        }
        if advanced {
            apply_compiled_execution(&mut replayed, &execution);
        }
        transitions.push(ReplayTransitionTraceEntry {
            event_id: event.event_id,
            event_type: event.event_type.clone(),
            occurred_at: event.occurred_at,
            checkpoint_before: Some(before),
            checkpoint_after: replay_checkpoint(&replayed),
        });
    }

    Ok(ReplayTrace { source: ReplaySource::RunStart, final_state: replayed, transitions })
}

pub fn replay_compiled_history_trace_from_snapshot(
    history_tail: &[EventEnvelope<WorkflowEvent>],
    snapshot_state: &WorkflowInstanceState,
    artifact: &CompiledWorkflowArtifact,
    snapshot_event_count: i64,
    snapshot_last_event_id: Uuid,
    snapshot_last_event_type: &str,
) -> Result<ReplayTrace> {
    let mut replayed = snapshot_state.clone();
    let mut transitions = Vec::with_capacity(history_tail.len());
    let mut seen_dedupe_keys = HashSet::new();

    for event in history_tail {
        let before = replay_checkpoint(&replayed);
        let was_terminal = replayed.status.is_terminal();
        replayed.apply_event(event);
        if was_terminal {
            transitions.push(ReplayTransitionTraceEntry {
                event_id: event.event_id,
                event_type: event.event_type.clone(),
                occurred_at: event.occurred_at,
                checkpoint_before: Some(before),
                checkpoint_after: replay_checkpoint(&replayed),
            });
            continue;
        }
        if !should_skip_replay_event(event, &mut seen_dedupe_keys) {
            match &event.payload {
                WorkflowEvent::SignalReceived { signal_type, payload, .. } => {
                    let execution = artifact.execute_after_signal_with_turn(
                        replayed.current_state.as_deref().unwrap_or_default(),
                        signal_type,
                        payload,
                        replayed.artifact_execution.clone().unwrap_or_default(),
                        ExecutionTurnContext {
                            event_id: event.event_id,
                            occurred_at: event.occurred_at,
                        },
                    )?;
                    apply_compiled_execution(&mut replayed, &execution);
                }
                WorkflowEvent::WorkflowUpdateAccepted { update_id, update_name, payload } => {
                    let execution = artifact.execute_update_with_turn(
                        replayed.current_state.as_deref().unwrap_or_default(),
                        update_id,
                        update_name,
                        payload,
                        replayed.artifact_execution.clone().unwrap_or_default(),
                        ExecutionTurnContext {
                            event_id: event.event_id,
                            occurred_at: event.occurred_at,
                        },
                    )?;
                    apply_compiled_execution(&mut replayed, &execution);
                }
                WorkflowEvent::TimerFired { timer_id } => {
                    if !is_internal_control_timer(timer_id) {
                        let execution = artifact.execute_after_timer_with_turn(
                            replayed.current_state.as_deref().unwrap_or_default(),
                            timer_id,
                            replayed.artifact_execution.clone().unwrap_or_default(),
                            ExecutionTurnContext {
                                event_id: event.event_id,
                                occurred_at: event.occurred_at,
                            },
                        )?;
                        apply_compiled_execution(&mut replayed, &execution);
                    }
                }
                WorkflowEvent::ActivityTaskCompleted { activity_id, output, .. } => {
                    let execution = artifact.execute_after_step_completion_with_turn(
                        replayed.current_state.as_deref().unwrap_or_default(),
                        activity_id,
                        output,
                        replayed.artifact_execution.clone().unwrap_or_default(),
                        ExecutionTurnContext {
                            event_id: event.event_id,
                            occurred_at: event.occurred_at,
                        },
                    )?;
                    apply_compiled_execution(&mut replayed, &execution);
                }
                WorkflowEvent::ActivityTaskFailed { activity_id, error, .. } => {
                    let execution = artifact.execute_after_step_failure_with_turn(
                        replayed.current_state.as_deref().unwrap_or_default(),
                        activity_id,
                        error,
                        replayed.artifact_execution.clone().unwrap_or_default(),
                        ExecutionTurnContext {
                            event_id: event.event_id,
                            occurred_at: event.occurred_at,
                        },
                    )?;
                    apply_compiled_execution(&mut replayed, &execution);
                }
                WorkflowEvent::ActivityTaskTimedOut { activity_id, .. } => {
                    let execution = artifact.execute_after_step_failure_with_turn(
                        replayed.current_state.as_deref().unwrap_or_default(),
                        activity_id,
                        "activity timed out",
                        replayed.artifact_execution.clone().unwrap_or_default(),
                        ExecutionTurnContext {
                            event_id: event.event_id,
                            occurred_at: event.occurred_at,
                        },
                    )?;
                    apply_compiled_execution(&mut replayed, &execution);
                }
                WorkflowEvent::ActivityTaskCancelled { activity_id, reason, .. } => {
                    let execution = artifact.execute_after_step_failure_with_turn(
                        replayed.current_state.as_deref().unwrap_or_default(),
                        activity_id,
                        reason,
                        replayed.artifact_execution.clone().unwrap_or_default(),
                        ExecutionTurnContext {
                            event_id: event.event_id,
                            occurred_at: event.occurred_at,
                        },
                    )?;
                    apply_compiled_execution(&mut replayed, &execution);
                }
                WorkflowEvent::ChildWorkflowCompleted { child_id, output, .. } => {
                    let execution = artifact.execute_after_child_completion_with_turn(
                        replayed.current_state.as_deref().unwrap_or_default(),
                        child_id,
                        output,
                        replayed.artifact_execution.clone().unwrap_or_default(),
                        ExecutionTurnContext {
                            event_id: event.event_id,
                            occurred_at: event.occurred_at,
                        },
                    )?;
                    apply_compiled_execution(&mut replayed, &execution);
                }
                WorkflowEvent::ChildWorkflowFailed { child_id, error, .. }
                | WorkflowEvent::ChildWorkflowCancelled { child_id, reason: error, .. }
                | WorkflowEvent::ChildWorkflowTerminated { child_id, reason: error, .. } => {
                    let execution = artifact.execute_after_child_failure_with_turn(
                        replayed.current_state.as_deref().unwrap_or_default(),
                        child_id,
                        error,
                        replayed.artifact_execution.clone().unwrap_or_default(),
                        ExecutionTurnContext {
                            event_id: event.event_id,
                            occurred_at: event.occurred_at,
                        },
                    )?;
                    apply_compiled_execution(&mut replayed, &execution);
                }
                _ => {}
            }
        }
        transitions.push(ReplayTransitionTraceEntry {
            event_id: event.event_id,
            event_type: event.event_type.clone(),
            occurred_at: event.occurred_at,
            checkpoint_before: Some(before),
            checkpoint_after: replay_checkpoint(&replayed),
        });
    }

    Ok(ReplayTrace {
        source: ReplaySource::SnapshotTail {
            snapshot_event_count,
            snapshot_last_event_id,
            snapshot_last_event_type: snapshot_last_event_type.to_owned(),
        },
        final_state: replayed,
        transitions,
    })
}

fn apply_compiled_execution(
    replayed: &mut WorkflowInstanceState,
    execution: &CompiledExecutionPlan,
) {
    replayed.current_state = Some(execution.final_state.clone());
    replayed.context = execution.context.clone();
    replayed.output = execution.output.clone();
    replayed.artifact_execution = Some(execution.execution_state.clone());
}

fn should_skip_replay_event(
    event: &EventEnvelope<WorkflowEvent>,
    seen_dedupe_keys: &mut HashSet<String>,
) -> bool {
    event.dedupe_key.as_ref().is_some_and(|dedupe_key| !seen_dedupe_keys.insert(dedupe_key.clone()))
}

fn is_internal_control_timer(timer_id: &str) -> bool {
    timer_id.starts_with("retry:")
}

fn replay_checkpoint(state: &WorkflowInstanceState) -> ReplayCheckpoint {
    ReplayCheckpoint {
        run_id: state.run_id.clone(),
        current_state: state.current_state.clone(),
        status: state.status.clone(),
        context: state.context.clone(),
        output: state.output.clone(),
        event_count: state.event_count,
        last_event_id: state.last_event_id,
        last_event_type: state.last_event_type.clone(),
    }
}

pub fn projection_mismatches(
    expected: &WorkflowInstanceState,
    actual: &WorkflowInstanceState,
) -> Vec<ReplayFieldMismatch> {
    let mut mismatches = Vec::new();
    push_mismatch(&mut mismatches, "tenant_id", &expected.tenant_id, &actual.tenant_id);
    push_mismatch(&mut mismatches, "instance_id", &expected.instance_id, &actual.instance_id);
    push_mismatch(&mut mismatches, "run_id", &expected.run_id, &actual.run_id);
    push_mismatch(&mut mismatches, "definition_id", &expected.definition_id, &actual.definition_id);
    push_mismatch(
        &mut mismatches,
        "definition_version",
        &expected.definition_version,
        &actual.definition_version,
    );
    push_mismatch(&mut mismatches, "artifact_hash", &expected.artifact_hash, &actual.artifact_hash);
    push_mismatch(&mut mismatches, "current_state", &expected.current_state, &actual.current_state);
    push_mismatch(&mut mismatches, "context", &expected.context, &actual.context);
    push_mismatch(&mut mismatches, "status", &expected.status, &actual.status);
    push_mismatch(&mut mismatches, "input", &expected.input, &actual.input);
    push_mismatch(&mut mismatches, "output", &expected.output, &actual.output);
    push_mismatch(
        &mut mismatches,
        "artifact_execution",
        &expected.artifact_execution,
        &actual.artifact_execution,
    );
    push_mismatch(&mut mismatches, "event_count", &expected.event_count, &actual.event_count);
    push_mismatch(&mut mismatches, "last_event_id", &expected.last_event_id, &actual.last_event_id);
    push_mismatch(
        &mut mismatches,
        "last_event_type",
        &expected.last_event_type,
        &actual.last_event_type,
    );
    mismatches
}

pub fn first_transition_divergence(
    expected: &[ReplayTransitionTraceEntry],
    actual: &[ReplayTransitionTraceEntry],
) -> Option<ReplayDivergence> {
    for (expected_entry, actual_entry) in expected.iter().zip(actual.iter()) {
        let fields =
            checkpoint_mismatches(&expected_entry.checkpoint_after, &actual_entry.checkpoint_after);
        if !fields.is_empty() {
            return Some(ReplayDivergence {
                kind: ReplayDivergenceKind::SnapshotMismatch,
                event_id: Some(actual_entry.event_id),
                event_type: Some(actual_entry.event_type.clone()),
                message: format!(
                    "snapshot-backed replay diverged after event {} ({})",
                    actual_entry.event_id, actual_entry.event_type
                ),
                fields,
            });
        }
    }

    if expected.len() != actual.len() {
        let extra = if actual.len() > expected.len() {
            actual.get(expected.len())
        } else {
            expected.get(actual.len())
        };
        return Some(ReplayDivergence {
            kind: ReplayDivergenceKind::SnapshotMismatch,
            event_id: extra.map(|entry| entry.event_id),
            event_type: extra.map(|entry| entry.event_type.clone()),
            message: "snapshot-backed replay produced a different transition count".to_owned(),
            fields: vec![ReplayFieldMismatch {
                field: "transition_count".to_owned(),
                expected: Value::from(expected.len() as u64),
                actual: Value::from(actual.len() as u64),
            }],
        });
    }

    None
}

fn checkpoint_mismatches(
    expected: &ReplayCheckpoint,
    actual: &ReplayCheckpoint,
) -> Vec<ReplayFieldMismatch> {
    let mut mismatches = Vec::new();
    push_mismatch(&mut mismatches, "run_id", &expected.run_id, &actual.run_id);
    push_mismatch(&mut mismatches, "current_state", &expected.current_state, &actual.current_state);
    push_mismatch(&mut mismatches, "status", &expected.status, &actual.status);
    push_mismatch(&mut mismatches, "context", &expected.context, &actual.context);
    push_mismatch(&mut mismatches, "output", &expected.output, &actual.output);
    push_mismatch(&mut mismatches, "event_count", &expected.event_count, &actual.event_count);
    push_mismatch(&mut mismatches, "last_event_id", &expected.last_event_id, &actual.last_event_id);
    push_mismatch(
        &mut mismatches,
        "last_event_type",
        &expected.last_event_type,
        &actual.last_event_type,
    );
    mismatches
}

fn push_mismatch<T>(
    mismatches: &mut Vec<ReplayFieldMismatch>,
    field: &str,
    expected: &T,
    actual: &T,
) where
    T: Serialize + PartialEq,
{
    if expected != actual {
        mismatches.push(ReplayFieldMismatch {
            field: field.to_owned(),
            expected: serde_json::to_value(expected).expect("expected value serializes"),
            actual: serde_json::to_value(actual).expect("actual value serializes"),
        });
    }
}

pub fn same_projection(left: &WorkflowInstanceState, right: &WorkflowInstanceState) -> bool {
    left.tenant_id == right.tenant_id
        && left.instance_id == right.instance_id
        && left.run_id == right.run_id
        && left.definition_id == right.definition_id
        && left.definition_version == right.definition_version
        && left.artifact_hash == right.artifact_hash
        && left.current_state == right.current_state
        && left.context == right.context
        && left.status == right.status
        && left.input == right.input
        && left.output == right.output
        && left.event_count == right.event_count
        && left.last_event_id == right.last_event_id
        && left.last_event_type == right.last_event_type
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum WorkflowStatus {
    Triggered,
    Running,
    Completed,
    Failed,
    Cancelled,
    Terminated,
}

impl WorkflowStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Triggered => "triggered",
            Self::Running => "running",
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
            Self::Terminated => "terminated",
        }
    }

    pub fn is_terminal(&self) -> bool {
        matches!(self, Self::Completed | Self::Failed | Self::Cancelled | Self::Terminated)
    }
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum WorkflowProjectionError {
    #[error("event {0} cannot initialize workflow state without a workflow id")]
    MissingWorkflowId(String),
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use chrono::Utc;
    use fabrik_events::{EventEnvelope, WorkflowEvent};
    use serde_json::{Value, json};
    use uuid::Uuid;

    use crate::compiled::{
        ArtifactEntrypoint, CompiledStateNode, CompiledWorkflow, CompiledWorkflowArtifact,
        Expression,
    };

    use super::{
        ActiveUpdateState, ArtifactExecutionState, ExecutionEmission, HttpRequestConfig,
        ReplaySource, RetryPolicy, StateNode, StepConfig, WorkflowDefinition,
        WorkflowInstanceState, WorkflowProjectionError, WorkflowStatus, WorkflowValidationError,
        replay_compiled_history_trace, replay_compiled_history_trace_from_snapshot,
        replay_history_trace, replay_history_trace_from_snapshot,
    };

    fn test_event(
        event_type: &str,
        run_id: &str,
        payload: WorkflowEvent,
        metadata: &[(&str, &str)],
    ) -> EventEnvelope<WorkflowEvent> {
        let mut event_metadata = BTreeMap::new();
        for (key, value) in metadata {
            event_metadata.insert((*key).to_owned(), (*value).to_owned());
        }

        EventEnvelope {
            schema_version: 1,
            event_id: Uuid::now_v7(),
            event_type: event_type.to_owned(),
            occurred_at: Utc::now(),
            tenant_id: "tenant-a".to_owned(),
            definition_id: "demo".to_owned(),
            definition_version: 1,
            artifact_hash: "artifact-1".to_owned(),
            instance_id: "instance-1".to_owned(),
            run_id: run_id.to_owned(),
            partition_key: format!("tenant-a:instance-1:{run_id}"),
            producer: "test".to_owned(),
            causation_id: None,
            correlation_id: None,
            dedupe_key: None,
            metadata: event_metadata,
            payload,
        }
    }

    fn test_event_with_dedupe(
        event_type: &str,
        run_id: &str,
        payload: WorkflowEvent,
        metadata: &[(&str, &str)],
        dedupe_key: Option<&str>,
    ) -> EventEnvelope<WorkflowEvent> {
        let mut event = test_event(event_type, run_id, payload, metadata);
        event.dedupe_key = dedupe_key.map(str::to_owned);
        event
    }

    fn compiled_test_artifact() -> CompiledWorkflowArtifact {
        let mut states = BTreeMap::new();
        states.insert(
            "wait_signal".to_owned(),
            CompiledStateNode::WaitForEvent {
                event_type: "external.approved".to_owned(),
                next: "wait_timer".to_owned(),
                output_var: Some("approval".to_owned()),
            },
        );
        states.insert(
            "wait_timer".to_owned(),
            CompiledStateNode::WaitForTimer { timer_ref: "5s".to_owned(), next: "echo".to_owned() },
        );
        states.insert(
            "echo".to_owned(),
            CompiledStateNode::Step {
                handler: "core.echo".to_owned(),
                input: Expression::Object {
                    fields: BTreeMap::from([
                        (
                            "approval".to_owned(),
                            Expression::Identifier { name: "approval".to_owned() },
                        ),
                        (
                            "originalInput".to_owned(),
                            Expression::Identifier { name: "input".to_owned() },
                        ),
                    ]),
                },
                next: Some("done".to_owned()),
                retry: None,
                config: None,
                output_var: Some("echoed".to_owned()),
                on_error: None,
            },
        );
        states.insert(
            "done".to_owned(),
            CompiledStateNode::Succeed {
                output: Some(Expression::Object {
                    fields: BTreeMap::from([
                        (
                            "approval".to_owned(),
                            Expression::Identifier { name: "approval".to_owned() },
                        ),
                        ("input".to_owned(), Expression::Identifier { name: "input".to_owned() }),
                        ("echoed".to_owned(), Expression::Identifier { name: "echoed".to_owned() }),
                    ]),
                }),
            },
        );

        CompiledWorkflowArtifact::new(
            "compiled-demo".to_owned(),
            1,
            "test-compiler",
            ArtifactEntrypoint {
                module: "workflow.ts".to_owned(),
                export: "liveValidation".to_owned(),
            },
            CompiledWorkflow { initial_state: "wait_signal".to_owned(), states },
        )
    }

    #[test]
    fn validation_rejects_unknown_next_state() {
        let mut states = BTreeMap::new();
        states.insert(
            "start".to_owned(),
            StateNode::WaitForEvent {
                event_type: "trigger.received".to_owned(),
                next: "missing".to_owned(),
            },
        );

        let workflow = WorkflowDefinition {
            id: "demo".to_owned(),
            version: 1,
            initial_state: "start".to_owned(),
            states,
        };

        assert_eq!(
            workflow.validate(),
            Err(WorkflowValidationError::MissingState("missing".to_owned()))
        );
    }

    #[test]
    fn projection_initializes_from_trigger_event() {
        let event = EventEnvelope {
            schema_version: 1,
            event_id: Uuid::now_v7(),
            event_type: "WorkflowTriggered".to_owned(),
            occurred_at: Utc::now(),
            tenant_id: "tenant-a".to_owned(),
            definition_id: "demo".to_owned(),
            definition_version: 1,
            artifact_hash: "artifact-1".to_owned(),
            instance_id: "instance-1".to_owned(),
            run_id: "run-1".to_owned(),
            partition_key: "tenant-a:instance-1:run-1".to_owned(),
            producer: "test".to_owned(),
            causation_id: None,
            correlation_id: None,
            dedupe_key: None,
            metadata: BTreeMap::new(),
            payload: WorkflowEvent::WorkflowTriggered { input: json!({"hello": "world"}) },
        };

        let state = WorkflowInstanceState::try_from(&event).unwrap();
        assert_eq!(state.definition_id, "demo");
        assert_eq!(state.instance_id, "instance-1");
        assert_eq!(state.run_id, "run-1");
        assert_eq!(state.status, WorkflowStatus::Triggered);
        assert_eq!(state.event_count, 1);
    }

    #[test]
    fn projection_rejects_non_initial_events_without_initial_identity() {
        let event = EventEnvelope {
            schema_version: 1,
            event_id: Uuid::now_v7(),
            event_type: "WorkflowCompleted".to_owned(),
            occurred_at: Utc::now(),
            tenant_id: "tenant-a".to_owned(),
            definition_id: "demo".to_owned(),
            definition_version: 1,
            artifact_hash: "artifact-1".to_owned(),
            instance_id: "instance-1".to_owned(),
            run_id: "run-1".to_owned(),
            partition_key: "tenant-a:instance-1:run-1".to_owned(),
            producer: "test".to_owned(),
            causation_id: None,
            correlation_id: None,
            dedupe_key: None,
            metadata: BTreeMap::new(),
            payload: WorkflowEvent::WorkflowCompleted { output: json!({"ok": true}) },
        };

        assert_eq!(
            WorkflowInstanceState::try_from(&event),
            Err(WorkflowProjectionError::MissingWorkflowId("WorkflowCompleted".to_owned()))
        );
    }

    #[test]
    fn projection_applies_continue_as_new_marker() {
        let triggered = EventEnvelope {
            schema_version: 1,
            event_id: Uuid::now_v7(),
            event_type: "WorkflowTriggered".to_owned(),
            occurred_at: Utc::now(),
            tenant_id: "tenant-a".to_owned(),
            definition_id: "demo".to_owned(),
            definition_version: 1,
            artifact_hash: "artifact-1".to_owned(),
            instance_id: "instance-1".to_owned(),
            run_id: "run-1".to_owned(),
            partition_key: "tenant-a:instance-1:run-1".to_owned(),
            producer: "test".to_owned(),
            causation_id: None,
            correlation_id: None,
            dedupe_key: None,
            metadata: BTreeMap::new(),
            payload: WorkflowEvent::WorkflowTriggered { input: json!({"hello": "world"}) },
        };

        let mut state = WorkflowInstanceState::try_from(&triggered).unwrap();
        let continued = EventEnvelope {
            schema_version: 1,
            event_id: Uuid::now_v7(),
            event_type: "WorkflowContinuedAsNew".to_owned(),
            occurred_at: Utc::now(),
            tenant_id: "tenant-a".to_owned(),
            definition_id: "demo".to_owned(),
            definition_version: 1,
            artifact_hash: "artifact-1".to_owned(),
            instance_id: "instance-1".to_owned(),
            run_id: "run-1".to_owned(),
            partition_key: "tenant-a:instance-1:run-1".to_owned(),
            producer: "test".to_owned(),
            causation_id: Some(triggered.event_id),
            correlation_id: Some(triggered.event_id),
            dedupe_key: None,
            metadata: BTreeMap::new(),
            payload: WorkflowEvent::WorkflowContinuedAsNew {
                new_run_id: "run-2".to_owned(),
                input: json!({"carried": true}),
            },
        };

        state.apply_event(&continued);
        assert_eq!(state.run_id, "run-1");
        assert_eq!(state.current_state, None);
        assert_eq!(state.context, Some(json!({"carried": true})));
        assert_eq!(state.status, WorkflowStatus::Running);
    }

    #[test]
    fn replay_trace_records_all_history_events() {
        let triggered = test_event(
            "WorkflowTriggered",
            "run-1",
            WorkflowEvent::WorkflowTriggered { input: json!({"hello": "world"}) },
            &[],
        );
        let started = test_event(
            "WorkflowStarted",
            "run-1",
            WorkflowEvent::WorkflowStarted,
            &[("state", "wait")],
        );
        let completed = test_event(
            "WorkflowCompleted",
            "run-1",
            WorkflowEvent::WorkflowCompleted { output: json!({"ok": true}) },
            &[("state", "done")],
        );

        let trace = replay_history_trace(&[triggered, started, completed]).unwrap();
        assert_eq!(trace.source, ReplaySource::RunStart);
        assert_eq!(trace.transitions.len(), 3);
        assert_eq!(trace.transitions[0].checkpoint_before, None);
        assert_eq!(trace.transitions[1].checkpoint_after.current_state, Some("wait".to_owned()));
        assert_eq!(trace.final_state.current_state, Some("done".to_owned()));
        assert_eq!(trace.final_state.status, WorkflowStatus::Completed);
    }

    #[test]
    fn replay_trace_from_snapshot_replays_only_tail() {
        let triggered = test_event(
            "WorkflowTriggered",
            "run-1",
            WorkflowEvent::WorkflowTriggered { input: json!({"hello": "world"}) },
            &[],
        );
        let started = test_event(
            "WorkflowStarted",
            "run-1",
            WorkflowEvent::WorkflowStarted,
            &[("state", "wait")],
        );
        let completed = test_event(
            "WorkflowCompleted",
            "run-1",
            WorkflowEvent::WorkflowCompleted { output: json!({"ok": true}) },
            &[("state", "done")],
        );

        let full_history = vec![triggered, started, completed.clone()];
        let snapshot_state = replay_history_trace(&full_history[..2]).unwrap().final_state;
        let trace = replay_history_trace_from_snapshot(
            &[completed],
            &snapshot_state,
            snapshot_state.event_count,
            snapshot_state.last_event_id,
            &snapshot_state.last_event_type,
        )
        .unwrap();

        assert!(matches!(trace.source, ReplaySource::SnapshotTail { .. }));
        assert_eq!(trace.transitions.len(), 1);
        assert_eq!(trace.final_state.current_state, Some("done".to_owned()));
        assert_eq!(trace.final_state.status, WorkflowStatus::Completed);
    }

    #[test]
    fn projection_tracks_cancellation_request_and_terminal_cancel() {
        let triggered = test_event(
            "WorkflowTriggered",
            "run-1",
            WorkflowEvent::WorkflowTriggered { input: json!({"hello": "world"}) },
            &[],
        );
        let cancellation_requested = test_event(
            "WorkflowCancellationRequested",
            "run-1",
            WorkflowEvent::WorkflowCancellationRequested {
                reason: "operator requested cancellation".to_owned(),
            },
            &[],
        );
        let cancelled = test_event(
            "WorkflowCancelled",
            "run-1",
            WorkflowEvent::WorkflowCancelled {
                reason: "operator requested cancellation".to_owned(),
            },
            &[],
        );

        let mut state = WorkflowInstanceState::try_from(&triggered).unwrap();
        state.apply_event(&cancellation_requested);
        assert_eq!(state.status, WorkflowStatus::Running);

        state.apply_event(&cancelled);
        assert_eq!(state.status, WorkflowStatus::Cancelled);
        assert_eq!(state.output, Some(Value::String("operator requested cancellation".to_owned())));
    }

    #[test]
    fn projection_does_not_reopen_after_terminal_child_reflection() {
        let triggered = test_event(
            "WorkflowTriggered",
            "run-1",
            WorkflowEvent::WorkflowTriggered { input: json!({"hello": "world"}) },
            &[],
        );
        let terminated = test_event(
            "WorkflowTerminated",
            "run-1",
            WorkflowEvent::WorkflowTerminated { reason: "done".to_owned() },
            &[],
        );
        let child_cancelled = test_event(
            "ChildWorkflowCancelled",
            "run-1",
            WorkflowEvent::ChildWorkflowCancelled {
                child_id: "child-1".to_owned(),
                child_run_id: "child-run-1".to_owned(),
                reason: "parent close".to_owned(),
            },
            &[],
        );

        let mut state = WorkflowInstanceState::try_from(&triggered).unwrap();
        state.apply_event(&terminated);
        state.apply_event(&child_cancelled);

        assert_eq!(state.status, WorkflowStatus::Terminated);
        assert_eq!(state.output, Some(Value::String("done".to_owned())));
    }

    #[test]
    fn terminal_projection_clears_active_update_on_post_terminal_rejection() {
        let triggered = test_event(
            "WorkflowTriggered",
            "run-1",
            WorkflowEvent::WorkflowTriggered { input: json!({"hello": "world"}) },
            &[],
        );
        let terminated = test_event(
            "WorkflowTerminated",
            "run-1",
            WorkflowEvent::WorkflowTerminated { reason: "done".to_owned() },
            &[],
        );
        let update_rejected = test_event(
            "WorkflowUpdateRejected",
            "run-1",
            WorkflowEvent::WorkflowUpdateRejected {
                update_id: "upd-1".to_owned(),
                update_name: "approve".to_owned(),
                error: "closed".to_owned(),
            },
            &[],
        );

        let mut state = WorkflowInstanceState::try_from(&triggered).unwrap();
        state.artifact_execution = Some(ArtifactExecutionState {
            active_update: Some(ActiveUpdateState {
                update_id: "upd-1".to_owned(),
                update_name: "approve".to_owned(),
                return_state: "wait".to_owned(),
            }),
            ..Default::default()
        });

        state.apply_event(&terminated);
        state.apply_event(&update_rejected);

        assert_eq!(state.status, WorkflowStatus::Terminated);
        assert_eq!(state.last_event_type, "WorkflowUpdateRejected");
        assert!(
            state
                .artifact_execution
                .as_ref()
                .and_then(|execution| execution.active_update.as_ref())
                .is_none()
        );
    }

    #[test]
    fn compiled_replay_from_snapshot_preserves_bindings() {
        let artifact = compiled_test_artifact();
        let run_id = "run-compiled";
        let input = json!({"request_id": "live-2"});
        let approval = json!({"approved": true});
        let echoed = json!({
            "approval": approval.clone(),
            "originalInput": input.clone(),
        });

        let triggered = test_event(
            "WorkflowTriggered",
            run_id,
            WorkflowEvent::WorkflowTriggered { input: input.clone() },
            &[],
        );
        let pinned = test_event(
            "WorkflowArtifactPinned",
            run_id,
            WorkflowEvent::WorkflowArtifactPinned,
            &[],
        );
        let started = test_event(
            "WorkflowStarted",
            run_id,
            WorkflowEvent::WorkflowStarted,
            &[("state", "wait_signal")],
        );
        let signal = test_event(
            "SignalReceived",
            run_id,
            WorkflowEvent::SignalReceived {
                signal_id: "sig-1".to_owned(),
                signal_type: "external.approved".to_owned(),
                payload: approval.clone(),
            },
            &[],
        );
        let timer_scheduled = test_event(
            "TimerScheduled",
            run_id,
            WorkflowEvent::TimerScheduled {
                timer_id: "wait_timer".to_owned(),
                fire_at: Utc::now(),
            },
            &[("state", "wait_timer")],
        );
        let timer_fired = test_event_with_dedupe(
            "TimerFired",
            run_id,
            WorkflowEvent::TimerFired { timer_id: "wait_timer".to_owned() },
            &[("state", "wait_timer")],
            Some("timer:wait_timer"),
        );
        let activity_scheduled = test_event(
            "ActivityTaskScheduled",
            run_id,
            WorkflowEvent::ActivityTaskScheduled {
                activity_id: "echo".to_owned(),
                activity_type: "core.echo".to_owned(),
                task_queue: "default".to_owned(),
                attempt: 1,
                input: echoed.clone(),
                config: None,
                state: Some("echo".to_owned()),
                schedule_to_start_timeout_ms: None,
                start_to_close_timeout_ms: None,
                heartbeat_timeout_ms: None,
            },
            &[("state", "echo")],
        );
        let activity_completed = test_event(
            "ActivityTaskCompleted",
            run_id,
            WorkflowEvent::ActivityTaskCompleted {
                activity_id: "echo".to_owned(),
                attempt: 1,
                output: echoed.clone(),
                worker_id: "worker-1".to_owned(),
                worker_build_id: "build-1".to_owned(),
            },
            &[("state", "echo")],
        );
        let completed = test_event(
            "WorkflowCompleted",
            run_id,
            WorkflowEvent::WorkflowCompleted {
                output: json!({
                    "approval": approval.clone(),
                    "input": input.clone(),
                    "echoed": echoed.clone(),
                }),
            },
            &[("state", "done")],
        );

        let history = vec![
            triggered.clone(),
            pinned,
            started,
            signal,
            timer_scheduled.clone(),
            timer_fired.clone(),
            activity_scheduled,
            activity_completed,
            completed,
        ];

        let snapshot_state =
            replay_compiled_history_trace(&history[..5], &artifact).unwrap().final_state;
        let trace = replay_compiled_history_trace_from_snapshot(
            &history[5..],
            &snapshot_state,
            &artifact,
            snapshot_state.event_count,
            timer_scheduled.event_id,
            &timer_scheduled.event_type,
        )
        .unwrap();

        assert_eq!(
            trace.final_state.output,
            Some(json!({
                "approval": approval,
                "input": input,
                "echoed": echoed,
            }))
        );
        let bindings = &trace
            .final_state
            .artifact_execution
            .as_ref()
            .expect("compiled bindings should exist")
            .bindings;
        assert_eq!(bindings.get("approval"), Some(&json!({"approved": true})));
        assert_eq!(bindings.get("input"), Some(&json!({"request_id": "live-2"})));
    }

    #[test]
    fn compiled_replay_skips_duplicate_timer_fires_by_dedupe_key() {
        let artifact = compiled_test_artifact();
        let run_id = "run-dedupe";
        let input = json!({"request_id": "live-3"});
        let approval = json!({"approved": true});
        let echoed = json!({
            "approval": approval.clone(),
            "originalInput": input.clone(),
        });

        let history = vec![
            test_event(
                "WorkflowTriggered",
                run_id,
                WorkflowEvent::WorkflowTriggered { input: input.clone() },
                &[],
            ),
            test_event(
                "WorkflowArtifactPinned",
                run_id,
                WorkflowEvent::WorkflowArtifactPinned,
                &[],
            ),
            test_event(
                "WorkflowStarted",
                run_id,
                WorkflowEvent::WorkflowStarted,
                &[("state", "wait_signal")],
            ),
            test_event(
                "SignalReceived",
                run_id,
                WorkflowEvent::SignalReceived {
                    signal_id: "sig-1".to_owned(),
                    signal_type: "external.approved".to_owned(),
                    payload: approval.clone(),
                },
                &[],
            ),
            test_event(
                "TimerScheduled",
                run_id,
                WorkflowEvent::TimerScheduled {
                    timer_id: "wait_timer".to_owned(),
                    fire_at: Utc::now(),
                },
                &[("state", "wait_timer")],
            ),
            test_event_with_dedupe(
                "TimerFired",
                run_id,
                WorkflowEvent::TimerFired { timer_id: "wait_timer".to_owned() },
                &[("state", "wait_timer")],
                Some("timer:wait_timer"),
            ),
            test_event_with_dedupe(
                "TimerFired",
                run_id,
                WorkflowEvent::TimerFired { timer_id: "wait_timer".to_owned() },
                &[("state", "wait_timer")],
                Some("timer:wait_timer"),
            ),
            test_event(
                "ActivityTaskScheduled",
                run_id,
                WorkflowEvent::ActivityTaskScheduled {
                    activity_id: "echo".to_owned(),
                    activity_type: "core.echo".to_owned(),
                    task_queue: "default".to_owned(),
                    attempt: 1,
                    input: echoed.clone(),
                    config: None,
                    state: Some("echo".to_owned()),
                    schedule_to_start_timeout_ms: None,
                    start_to_close_timeout_ms: None,
                    heartbeat_timeout_ms: None,
                },
                &[("state", "echo")],
            ),
            test_event(
                "ActivityTaskCompleted",
                run_id,
                WorkflowEvent::ActivityTaskCompleted {
                    activity_id: "echo".to_owned(),
                    attempt: 1,
                    output: echoed.clone(),
                    worker_id: "worker-1".to_owned(),
                    worker_build_id: "build-1".to_owned(),
                },
                &[("state", "echo")],
            ),
            test_event(
                "WorkflowCompleted",
                run_id,
                WorkflowEvent::WorkflowCompleted {
                    output: json!({
                        "approval": approval,
                        "input": input,
                        "echoed": echoed,
                    }),
                },
                &[("state", "done")],
            ),
        ];

        let trace = replay_compiled_history_trace(&history, &artifact).unwrap();
        assert_eq!(trace.final_state.status, WorkflowStatus::Completed);
        assert_eq!(trace.transitions.len(), 10);
        assert_eq!(trace.final_state.event_count, 10);
        assert_eq!(trace.final_state.current_state, Some("done".to_owned()));
    }

    #[test]
    fn executes_sync_step_workflow() {
        let mut states = BTreeMap::new();
        states.insert(
            "first".to_owned(),
            StateNode::Step {
                handler: "core.echo".to_owned(),
                next: Some("done".to_owned()),
                retry: None,
                config: None,
            },
        );
        states.insert("done".to_owned(), StateNode::Succeed);

        let workflow = WorkflowDefinition {
            id: "demo".to_owned(),
            version: 3,
            initial_state: "first".to_owned(),
            states,
        };

        let plan = workflow.execute_trigger(&json!({"message": "hi"})).unwrap();
        assert_eq!(plan.workflow_version, 3);
        assert_eq!(plan.final_state, "first");
        assert!(matches!(
            plan.emissions.last(),
            Some(ExecutionEmission {
                event: WorkflowEvent::ActivityTaskScheduled { activity_id: step_id, attempt, .. },
                ..
            }) if step_id == "first" && *attempt == 1
        ));
    }

    #[test]
    fn execute_trigger_stops_on_wait_state() {
        let mut states = BTreeMap::new();
        states.insert(
            "wait".to_owned(),
            StateNode::WaitForEvent {
                event_type: "external.signal".to_owned(),
                next: "done".to_owned(),
            },
        );
        states.insert("done".to_owned(), StateNode::Succeed);

        let workflow = WorkflowDefinition {
            id: "demo".to_owned(),
            version: 1,
            initial_state: "wait".to_owned(),
            states,
        };

        let plan = workflow.execute_trigger(&json!({})).unwrap();
        assert_eq!(plan.final_state, "wait");
        assert_eq!(plan.emissions.len(), 1);
        assert!(matches!(&plan.emissions[0].event, WorkflowEvent::WorkflowStarted));
    }

    #[test]
    fn execute_trigger_schedules_timer() {
        let mut states = BTreeMap::new();
        states.insert(
            "wait".to_owned(),
            StateNode::WaitForTimer { timer_ref: "5s".to_owned(), next: "done".to_owned() },
        );
        states.insert("done".to_owned(), StateNode::Succeed);

        let workflow = WorkflowDefinition {
            id: "timer-demo".to_owned(),
            version: 1,
            initial_state: "wait".to_owned(),
            states,
        };

        let plan = workflow.execute_trigger(&json!({"message": "hi"})).unwrap();
        assert_eq!(plan.final_state, "wait");
        assert!(matches!(
            plan.emissions.last(),
            Some(ExecutionEmission {
                event: WorkflowEvent::TimerScheduled { timer_id, .. },
                ..
            }) if timer_id == "wait"
        ));
    }

    #[test]
    fn execute_after_timer_resumes_next_state() {
        let mut states = BTreeMap::new();
        states.insert(
            "wait".to_owned(),
            StateNode::WaitForTimer { timer_ref: "1s".to_owned(), next: "done".to_owned() },
        );
        states.insert("done".to_owned(), StateNode::Succeed);

        let workflow = WorkflowDefinition {
            id: "timer-demo".to_owned(),
            version: 1,
            initial_state: "wait".to_owned(),
            states,
        };

        let plan =
            workflow.execute_after_timer("wait", "wait", &json!({"message": "resume"})).unwrap();
        assert_eq!(plan.final_state, "done");
        assert!(matches!(
            plan.emissions.last(),
            Some(ExecutionEmission { event: WorkflowEvent::WorkflowCompleted { .. }, .. })
        ));
    }

    #[test]
    fn execute_after_signal_resumes_next_state() {
        let mut states = BTreeMap::new();
        states.insert(
            "wait".to_owned(),
            StateNode::WaitForEvent {
                event_type: "external.approved".to_owned(),
                next: "done".to_owned(),
            },
        );
        states.insert("done".to_owned(), StateNode::Succeed);

        let workflow = WorkflowDefinition {
            id: "signal-demo".to_owned(),
            version: 2,
            initial_state: "wait".to_owned(),
            states,
        };

        let plan = workflow
            .execute_after_signal("wait", "external.approved", &json!({"approved": true}))
            .unwrap();
        assert_eq!(plan.final_state, "done");
        assert!(matches!(
            plan.emissions.last(),
            Some(ExecutionEmission { event: WorkflowEvent::WorkflowCompleted { .. }, .. })
        ));
    }

    #[test]
    fn execute_after_step_completion_advances_workflow() {
        let mut states = BTreeMap::new();
        states.insert(
            "echo".to_owned(),
            StateNode::Step {
                handler: "core.echo".to_owned(),
                next: Some("done".to_owned()),
                retry: None,
                config: None,
            },
        );
        states.insert("done".to_owned(), StateNode::Succeed);

        let workflow = WorkflowDefinition {
            id: "step-demo".to_owned(),
            version: 1,
            initial_state: "echo".to_owned(),
            states,
        };

        let plan = workflow
            .execute_after_step_completion("echo", "echo", &json!({"message": "ok"}))
            .unwrap();
        assert_eq!(plan.final_state, "done");
        assert!(matches!(
            plan.emissions.last(),
            Some(ExecutionEmission {
                event: WorkflowEvent::WorkflowCompleted { output },
                ..
            }) if output == &json!({"message": "ok"})
        ));
    }

    #[test]
    fn schedule_retry_returns_next_attempt() {
        let mut states = BTreeMap::new();
        states.insert(
            "effect".to_owned(),
            StateNode::Step {
                handler: "core.echo".to_owned(),
                next: Some("done".to_owned()),
                retry: Some(RetryPolicy { max_attempts: 3, delay: "5s".to_owned() }),
                config: None,
            },
        );
        states.insert("done".to_owned(), StateNode::Succeed);

        let workflow = WorkflowDefinition {
            id: "retry-demo".to_owned(),
            version: 1,
            initial_state: "effect".to_owned(),
            states,
        };

        let retry = workflow.schedule_retry("effect", 1).unwrap().unwrap();
        assert_eq!(retry.attempt, 2);
    }

    #[test]
    fn validates_retry_policy() {
        let mut states = BTreeMap::new();
        states.insert(
            "effect".to_owned(),
            StateNode::Step {
                handler: "core.echo".to_owned(),
                next: Some("done".to_owned()),
                retry: Some(RetryPolicy { max_attempts: 0, delay: "5s".to_owned() }),
                config: None,
            },
        );
        states.insert("done".to_owned(), StateNode::Succeed);

        let workflow = WorkflowDefinition {
            id: "retry-demo".to_owned(),
            version: 1,
            initial_state: "effect".to_owned(),
            states,
        };

        assert_eq!(workflow.validate(), Err(WorkflowValidationError::InvalidRetryAttempts));
    }

    #[test]
    fn validates_http_step_config() {
        let mut states = BTreeMap::new();
        states.insert(
            "request".to_owned(),
            StateNode::Step {
                handler: "http.request".to_owned(),
                next: Some("done".to_owned()),
                retry: None,
                config: Some(StepConfig::HttpRequest(HttpRequestConfig {
                    method: "POST".to_owned(),
                    url: "http://localhost:9999/echo".to_owned(),
                    headers: BTreeMap::new(),
                    body: None,
                    body_from_input: true,
                })),
            },
        );
        states.insert("done".to_owned(), StateNode::Succeed);

        let workflow = WorkflowDefinition {
            id: "http-demo".to_owned(),
            version: 1,
            initial_state: "request".to_owned(),
            states,
        };

        assert_eq!(workflow.validate(), Ok(()));
        assert!(matches!(
            workflow.step_config("request").unwrap(),
            Some(StepConfig::HttpRequest(_))
        ));
    }

    #[test]
    fn rejects_missing_http_step_config() {
        let mut states = BTreeMap::new();
        states.insert(
            "request".to_owned(),
            StateNode::Step {
                handler: "http.request".to_owned(),
                next: Some("done".to_owned()),
                retry: None,
                config: None,
            },
        );
        states.insert("done".to_owned(), StateNode::Succeed);

        let workflow = WorkflowDefinition {
            id: "http-demo".to_owned(),
            version: 1,
            initial_state: "request".to_owned(),
            states,
        };

        assert_eq!(workflow.validate(), Err(WorkflowValidationError::MissingHttpRequestConfig));
    }
}
