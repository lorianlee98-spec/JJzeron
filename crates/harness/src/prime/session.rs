use std::collections::HashMap;
use std::collections::VecDeque;
use std::path::Path;
use std::time::{Duration, Instant};

use base64::Engine as _;
use serde_json::{Value, json};
use tokio::sync::mpsc;
use uuid::Uuid;
use zeron_proto::{AgentEvent, DoneStatus, HarnessId, RunRequest, ToolCall, UserInputQuestion};

use crate::process::Child;
use crate::{HarnessError, RunControls, StderrTail};

use super::client::PrimeClient;
use super::events::{EventMapper, replay_messages, without_image_data};
use super::{KILL_GRACE, parse_commands, reasoning_name};

type EventTx = mpsc::Sender<Result<AgentEvent, HarnessError>>;
enum CommandKind {
    Initial,
    Steer,
    Goal,
    Completion(u64),
    Quiescence(u64),
}

type CommandResult = (CommandKind, String, Result<Value, HarnessError>);

struct PendingControl {
    text: String,
    started: bool,
    error: Option<String>,
    may_start_turn: bool,
}

fn session_command_may_start_turn(text: &str) -> Option<bool> {
    if text.contains(['\n', '\r', '\u{2028}', '\u{2029}']) {
        return None;
    }
    match zeron_proto::invocation::leading_command(text) {
        Some(("compact" | "refine" | "autonomous", _)) => Some(false),
        Some(("goal", args)) => Some(!matches!(
            args.trim(),
            "" | "status" | "pause" | "clear" | "stop"
        )),
        _ => None,
    }
}

async fn emit(tx: &EventTx, event: AgentEvent) -> bool {
    tx.send(Ok(event)).await.is_ok()
}

async fn refresh_context_usage(client: &PrimeClient, tx: &EventTx) -> bool {
    let stats = match tokio::time::timeout(
        Duration::from_secs(5),
        client.request("get_session_stats", json!({})),
    )
    .await
    {
        Ok(Ok(stats)) => stats,
        Ok(Err(error)) => {
            tracing::warn!(%error, "Prime context usage refresh failed");
            return false;
        }
        Err(_) => {
            tracing::warn!("Prime context usage refresh timed out");
            return false;
        }
    };
    if !stats.is_object() {
        tracing::warn!("Prime get_session_stats returned invalid data");
        return false;
    }
    let usage = &stats["contextUsage"];
    let (tokens, window) = if usage.is_null() {
        (None, None)
    } else if let (Some(window), tokens) = (
        usage["contextWindow"].as_u64().filter(|window| *window > 0),
        usage["tokens"].as_u64(),
    ) && (usage["tokens"].is_null() || tokens.is_some())
    {
        (tokens, Some(window))
    } else {
        tracing::warn!("Prime get_session_stats returned invalid contextUsage");
        return false;
    };
    emit(tx, AgentEvent::ContextUsageSnapshot { tokens, window }).await
}

fn selected_model(value: &str) -> Result<(String, String), HarnessError> {
    value
        .split_once('/')
        .filter(|(provider, model)| !provider.is_empty() && !model.is_empty())
        .map(|(provider, model)| (provider.into(), model.into()))
        .ok_or_else(|| HarnessError::Protocol(format!("Invalid Prime model selector: {value}")))
}

async fn inline_images(paths: &[String]) -> Vec<Value> {
    let mut images = Vec::new();
    for path in paths {
        let mime = match Path::new(path)
            .extension()
            .and_then(|extension| extension.to_str())
            .map(str::to_ascii_lowercase)
            .as_deref()
        {
            Some("png") => "image/png",
            Some("jpg" | "jpeg") => "image/jpeg",
            Some("gif") => "image/gif",
            Some("webp") => "image/webp",
            _ => continue,
        };
        if let Ok(bytes) = tokio::fs::read(path).await {
            if bytes.len() <= 5 * 1024 * 1024 {
                images.push(json!({
                    "type": "image",
                    "mimeType": mime,
                    "data": base64::engine::general_purpose::STANDARD.encode(bytes),
                }));
            }
        }
    }
    images
}

async fn setup(
    client: &PrimeClient,
    request: &RunRequest,
    tx: &EventTx,
) -> Result<String, HarnessError> {
    let state = tokio::time::timeout(
        Duration::from_secs(30),
        client.request("get_state", json!({})),
    )
    .await
    .map_err(|_| HarnessError::Protocol("Prime RPC get_state timed out".into()))??;
    let session_file = state
        .get("sessionFile")
        .and_then(Value::as_str)
        .filter(|path| !path.is_empty())
        .ok_or_else(|| HarnessError::Protocol("Prime RPC session has no persistent file".into()))?
        .to_owned();
    let native_model = &state["model"];
    let current_model = match (
        native_model["provider"].as_str(),
        native_model["id"].as_str(),
    ) {
        (Some(provider), Some(id)) => format!("{provider}/{id}"),
        _ => "default".to_owned(),
    };
    let selected = match request.model.as_deref() {
        Some("default") | None => current_model.clone(),
        Some(model) if model == current_model => current_model.clone(),
        Some(model) => {
            let (provider, model_id) = selected_model(model)?;
            client
                .request(
                    "set_model",
                    json!({ "provider": provider, "modelId": model_id }),
                )
                .await?;
            model.to_owned()
        }
    };
    if let Some(level) = request
        .reasoning
        .filter(|level| super::REASONING.contains(level))
        .map(reasoning_name)
        && state["thinkingLevel"].as_str() != Some(level)
    {
        client
            .request("set_thinking_level", json!({ "level": level }))
            .await?;
    }
    let commands = client.request("get_commands", json!({})).await?;
    let _ = emit(
        tx,
        AgentEvent::SessionStarted {
            harness: HarnessId::Prime,
            model: selected,
            tools: Vec::new(),
            cwd: request.cwd.clone(),
            session_id: session_file.clone(),
            assistant_message_id: Uuid::new_v4().to_string(),
        },
    )
    .await;
    let _ = refresh_context_usage(client, tx).await;
    // The RPC state is authoritative for a resumed session whose goal was
    // created before this event subscription. Mark this initial projection so
    // consumers can distinguish it from exact stdout notifications.
    let _ = emit(
        tx,
        AgentEvent::PrimeEvent {
            event: json!({ "type": "goal_update", "goal": state["goal"], "source": "get_state" }),
        },
    )
    .await;
    if let Ok(commands) = parse_commands(&commands) {
        let _ = emit(tx, AgentEvent::AvailableCommands { commands }).await;
    }
    Ok(session_file)
}

pub(super) async fn run_session(
    mut child: Option<Child>,
    client: PrimeClient,
    mut incoming: mpsc::UnboundedReceiver<Value>,
    stderr: StderrTail,
    request: RunRequest,
    mut controls: RunControls,
    tx: EventTx,
) {
    let session_file = match setup(&client, &request, &tx).await {
        Ok(path) => path,
        Err(error) => {
            let _ = emit(
                &tx,
                AgentEvent::Done {
                    status: DoneStatus::Errored,
                    result: None,
                    error: Some(error.to_string()),
                    session_id: None,
                },
            )
            .await;
            if child.is_none() && request.resume.is_none() && client.is_daemon() {
                let _ =
                    tokio::time::timeout(Duration::from_secs(5), client.request("kill", json!({})))
                        .await;
            }
            if let Some(child) = child.as_mut() {
                crate::shutdown_child(child, KILL_GRACE).await;
            }
            return;
        }
    };

    let mut mapper = EventMapper::new();
    let mut children = Children::default();
    let mut active = true;
    let mut done_current = false;
    let mut completion_generation = 0;
    let mut completion_pending = false;
    let mut transport_error = None;
    let mut interrupted = false;
    let mut steering_open = true;
    let mut steering_pending = false;
    let mut pending_controls = VecDeque::new();
    let mut tick = tokio::time::interval(Duration::from_millis(200));
    let images = inline_images(&request.attachments).await;
    let initial_prompt = request.prompt.clone();
    if let Some(may_start_turn) = session_command_may_start_turn(&initial_prompt) {
        pending_controls.push_back(PendingControl {
            text: initial_prompt.clone(),
            started: false,
            error: None,
            may_start_turn,
        });
    }
    // The prompt may request extension UI input before it acknowledges
    // acceptance, so event handling must run while the request is pending.
    let (command_tx, mut command_rx) = mpsc::unbounded_channel::<CommandResult>();
    {
        let client = client.clone();
        let command_tx = command_tx.clone();
        tokio::spawn(async move {
            let result = client
                .request(
                    "prompt",
                    json!({ "message": initial_prompt, "images": images }),
                )
                .await;
            let _ = command_tx.send((CommandKind::Initial, initial_prompt, result));
        });
    }

    loop {
        tokio::select! {
            Some((kind, text, result)) = command_rx.recv() => {
                match (kind, result) {
                    (CommandKind::Steer, Ok(_)) => {
                        steering_pending = false;
                        if !active {
                            active = true;
                            done_current = false;
                        }
                        if !emit(&tx, AgentEvent::Steered { assistant_message_id: None, next_assistant_message_id: None }).await { break; }
                        if client.is_daemon() {
                            start_completion(&client, &command_tx, &mut completion_generation, &mut completion_pending);
                        }
                    }
                    (CommandKind::Goal, Ok(_)) | (CommandKind::Initial, Ok(_)) => {
                        if client.is_daemon() {
                            start_completion(&client, &command_tx, &mut completion_generation, &mut completion_pending);
                        }
                    }
                    (CommandKind::Completion(generation), Ok(_)) if generation == completion_generation => {
                        completion_pending = false;
                        let terminal = terminal_result(&client).await;
                        let (status, error) = match terminal {
                            Ok(result) => result,
                            Err(error) => (DoneStatus::Errored, Some(error.to_string())),
                        };
                        let background = client.request("get_session_summary", json!({})).await.ok()
                            .filter(|summary| summary["isSessionActive"] == true);
                        active = false;
                        done_current = true;
                        if !emit(&tx, AgentEvent::Done { status, result: None, error, session_id: Some(session_file.clone()) }).await { break; }
                        if let Some(summary) = background
                            && !emit(&tx, AgentEvent::PrimeEvent { event: json!({
                                "type":"lifecycle_update",
                                "phase":"background",
                                "hasRunningRlmChildren":summary["hasRunningRlmChildren"],
                                "isBashRunning":summary["isBashRunning"],
                                "unfinishedActionCount":summary["unfinishedActionCount"],
                            }) }).await { break; }
                        mapper = EventMapper::new();
                        if !steering_open { break; }
                        let client = client.clone();
                        let command_tx = command_tx.clone();
                        tokio::spawn(async move {
                            let result = client.request("wait_for_headless_completion", json!({"waitForRlmQuiescence":true})).await;
                            let _ = command_tx.send((CommandKind::Quiescence(generation), String::new(), result));
                        });
                    }
                    (CommandKind::Quiescence(generation), Ok(_)) if generation == completion_generation => {
                        if !emit(&tx, AgentEvent::PrimeEvent { event: json!({"type":"lifecycle_update","phase":"quiescent"}) }).await { break; }
                    }
                    (CommandKind::Quiescence(generation), Err(error)) if generation == completion_generation => {
                        if !emit(&tx, AgentEvent::Error { message: error.to_string() }).await { break; }
                    }
                    (CommandKind::Completion(generation), Err(error)) if generation == completion_generation => {
                        completion_pending = false;
                        if !emit(&tx, AgentEvent::Done { status: DoneStatus::Errored, result: None, error: Some(error.to_string()), session_id: Some(session_file.clone()) }).await { break; }
                        active = false;
                        done_current = true;
                        if !steering_open { break; }
                    }
                    (CommandKind::Completion(_), _) | (CommandKind::Quiescence(_), _) => {}
                    (CommandKind::Goal, Err(error)) => {
                        if !emit(&tx, AgentEvent::Error { message: error.to_string() }).await { break; }
                    }
                    (CommandKind::Steer, Err(error)) if session_command_may_start_turn(&text).is_some() => {
                        if let Some(index) = pending_controls.iter().position(|command| command.text == text) {
                            pending_controls.remove(index);
                        }
                        if !emit(&tx, AgentEvent::Steered { assistant_message_id: None, next_assistant_message_id: None }).await
                            || !emit(&tx, AgentEvent::Done { status: DoneStatus::Errored, result: None, error: Some(error.to_string()), session_id: Some(session_file.clone()) }).await
                        { break; }
                        active = false;
                        done_current = true;
                    }
                    (_, Err(error)) => {
                        let _ = emit(&tx, AgentEvent::Done { status: DoneStatus::Errored, result: None, error: Some(error.to_string()), session_id: Some(session_file.clone()) }).await;
                        done_current = true;
                        break;
                    }
                }
            }
            message = incoming.recv() => {
                let Some(message) = message else { break; };
                // Preserve the native notification for subscribers while the
                // image part takes ownership of any inline media bytes.
                if message.get("type").and_then(Value::as_str) != Some("_transport_closed")
                    && !emit(&tx, AgentEvent::PrimeEvent { event: without_image_data(&message) }).await
                {
                    break;
                }
                if message["type"] == "compaction_end" && message["result"].is_object() {
                    if !refresh_context_usage(&client, &tx).await {
                        let _ = emit(&tx, AgentEvent::ContextUsageSnapshot { tokens: None, window: None }).await;
                    }
                } else if message["type"] == "message_end" && message["message"]["role"] == "assistant" {
                    let _ = refresh_context_usage(&client, &tx).await;
                }
                match message.get("type").and_then(Value::as_str) {
                    Some("_transport_closed") => {
                        transport_error = message["error"].as_str().map(str::to_owned);
                        let _ = emit(&tx, AgentEvent::PrimeEvent { event: json!({
                            "type":"lifecycle_update", "phase":"disconnected"
                        }) }).await;
                        break;
                    }
                    Some("agent_start") => {
                        if client.is_daemon() && done_current && !completion_pending {
                            start_completion(&client, &command_tx, &mut completion_generation, &mut completion_pending);
                        }
                        active = true;
                        done_current = false;
                    }
                    Some("agent_end") => {
                        if client.is_daemon() { continue; }
                        active = false;
                        done_current = true;
                        let error = mapper.failure().map(str::to_owned);
                        let status = if error.is_some() { DoneStatus::Errored } else { DoneStatus::Completed };
                        if !emit(&tx, AgentEvent::Done { status, result: None, error, session_id: Some(session_file.clone()) }).await { break; }
                        mapper = EventMapper::new();
                        if !steering_open { break; }
                    }
                    Some("message_end") if message["message"]["customType"] == "session_slash_command_result" => {
                        let result = &message["message"];
                        if let Some(command) = pending_controls.iter_mut().find(|command| result["details"]["command"]["text"] == command.text) {
                            if result["details"]["success"] == false {
                                command.error = Some(result["details"]["error"].as_str().unwrap_or("Prime command failed").into());
                            } else if result["display"] == true
                                && let Some(content) = result["content"].as_str()
                                && !emit(&tx, AgentEvent::TextDelta { text: content.into() }).await
                            { break; }
                        }
                    }
                    Some("message_end") if message["message"]["customType"] == "autonomous_status" => {
                        if pending_controls.iter().any(|command| command.text.starts_with("/autonomous"))
                            && let Some(content) = message["message"]["content"].as_str()
                            && !emit(&tx, AgentEvent::TextDelta { text: content.into() }).await
                        { break; }
                    }
                    Some("session_action_update") => {
                        let label = message["actions"]["active"]["label"].as_str();
                        if let Some(command) = pending_controls.front_mut() {
                            if label == Some(command.text.as_str()) {
                                command.started = true;
                            } else if command.started {
                                let command = pending_controls.pop_front().unwrap();
                                let next_action = message["actions"]["active"]["kind"].as_str();
                                let has_goal_turn = command.may_start_turn && command.error.is_none()
                                    && (next_action == Some("turn") || message["actions"]["queuedCount"].as_u64().is_some_and(|count| count > 0));
                                if !has_goal_turn && !client.is_daemon() {
                                    active = false;
                                    done_current = true;
                                    let status = if command.error.is_some() { DoneStatus::Errored } else { DoneStatus::Completed };
                                    if !emit(&tx, AgentEvent::Done { status, result: None, error: command.error, session_id: Some(session_file.clone()) }).await { break; }
                                    if !steering_open { break; }
                                }
                            }
                        }
                    }
                    Some("rlm_child_update") => {
                        for event in children.update(&message["child"], &client, &request.cwd).await {
                            if !emit(&tx, event).await { break; }
                        }
                    }
                    Some("session_attached" | "session_resynced" | "session_replaced") => {
                        if let Some(snapshot) = message["snapshot"]["children"].as_array() {
                            for child in snapshot.iter().filter(|child| matches!(child["status"].as_str(), Some("queued" | "running"))) {
                                for event in children.update(child, &client, &request.cwd).await {
                                    if !emit(&tx, event).await { break; }
                                }
                            }
                        }
                    }
                    Some("observed_session_event") | Some("observed_session_closed") => {
                        for event in children.observed(&message) {
                            if !emit(&tx, event).await { break; }
                        }
                    }
                    Some("extension_ui_request") => {
                        // The bridge must not block the event reader while an
                        // extension waits for a user decision.
                        let client = client.clone();
                        let question = &controls.request_input;
                        let Some(id) = message.get("id").and_then(Value::as_str).map(str::to_owned) else { continue; };
                        let method = message.get("method").and_then(Value::as_str).unwrap_or_default();
                        if matches!(method, "select" | "confirm" | "input" | "editor") {
                            let options = if method == "confirm" { vec!["Yes".into(), "No".into()] } else {
                                message.get("options").and_then(Value::as_array).into_iter().flatten().filter_map(Value::as_str).map(str::to_owned).collect()
                            };
                            let prompt = UserInputQuestion { id: id.clone(), header: message["title"].as_str().unwrap_or("Prime Agent").into(), question: message["message"].as_str().or_else(|| message["title"].as_str()).unwrap_or("Prime Agent requests input").into(), options, multi_select: false };
                            let receiver = question(vec![prompt]);
                            let method = method.to_owned();
                            tokio::spawn(async move {
                                let answers = receiver.await.unwrap_or_default();
                                let answer = answers.first().and_then(|answer| answer.labels.first());
                                let response = match (method.as_str(), answer) {
                                    ("confirm", Some(value)) => json!({"type":"extension_ui_response","id":id,"confirmed":value == "Yes"}),
                                    (_, Some(value)) => json!({"type":"extension_ui_response","id":id,"value":value}),
                                    _ => json!({"type":"extension_ui_response","id":id,"cancelled":true}),
                                };
                                let _ = client.send(response);
                            });
                        }
                    }
                    _ => {
                        for event in mapper.map(&message) {
                            if !matches!(event, AgentEvent::UserMessage { .. }) && !emit(&tx, event).await { break; }
                        }
                    }
                }
            }
            next = controls.steering.recv(), if steering_open && !interrupted && !steering_pending => {
                match next {
                    Some(next) => {
                        let goal_command = next.message_id.is_none();
                        if !goal_command && let Some(may_start_turn) = session_command_may_start_turn(&next.prompt) {
                            pending_controls.push_back(PendingControl { text: next.prompt.clone(), started: false, error: None, may_start_turn });
                        }
                        // Prime's prompt with streamingBehavior handles both a
                        // live turn and the idle edge without a state race.
                        if !goal_command { steering_pending = true; }
                        let client = client.clone();
                        let command_tx = command_tx.clone();
                        tokio::spawn(async move {
                            let text = next.prompt;
                            let result = client.request("prompt", json!({ "message": text, "streamingBehavior": "steer" })).await;
                            let kind = if goal_command { CommandKind::Goal } else { CommandKind::Steer };
                            let _ = command_tx.send((kind, text, result));
                        });
                    }
                    None => {
                        steering_open = false;
                        if !active { break; }
                    }
                }
            }
            _ = controls.interrupt.cancelled(), if !interrupted => {
                interrupted = true;
                let _ = tokio::time::timeout(Duration::from_secs(5), client.request("abort", json!({}))).await;
                let _ = emit(&tx, AgentEvent::Done { status: DoneStatus::Interrupted, result: None, error: None, session_id: Some(session_file.clone()) }).await;
                done_current = true;
                break;
            }
            _ = tick.tick() => {
                for event in children.settle_due() {
                    if !emit(&tx, event).await { break; }
                }
            }
            _ = tx.closed() => break,
        }
        for watch_key in children.finished_observations() {
            let client = client.clone();
            tokio::spawn(async move {
                let _ = client
                    .request("unobserve", json!({ "activeSessionId": watch_key }))
                    .await;
            });
        }
    }

    if !tx.is_closed() {
        for event in children.finish_open() {
            let _ = emit(&tx, event).await;
        }
        if active && !done_current && !interrupted {
            let error = if let Some(child) = child.as_mut() {
                crate::crash_message(
                    "prime-agent --mode rpc",
                    child.try_wait().ok().flatten(),
                    &stderr,
                )
            } else {
                transport_error
                    .unwrap_or_else(|| "Prime daemon session ended before completion".into())
            };
            let _ = emit(
                &tx,
                AgentEvent::Done {
                    status: DoneStatus::Errored,
                    result: None,
                    error: Some(error),
                    session_id: Some(session_file),
                },
            )
            .await;
        }
    }
    if let Some(child) = child.as_mut() {
        crate::shutdown_child(child, KILL_GRACE).await;
    }
}

fn start_completion(
    client: &PrimeClient,
    command_tx: &mpsc::UnboundedSender<CommandResult>,
    generation: &mut u64,
    pending: &mut bool,
) {
    *generation += 1;
    *pending = true;
    let current = *generation;
    let client = client.clone();
    let command_tx = command_tx.clone();
    tokio::spawn(async move {
        let result = client
            .request("wait_for_headless_completion", json!({}))
            .await;
        let _ = command_tx.send((CommandKind::Completion(current), String::new(), result));
    });
}

async fn terminal_result(
    client: &PrimeClient,
) -> Result<(DoneStatus, Option<String>), HarnessError> {
    let data = client.request("get_messages", json!({})).await?;
    let messages = data["messages"].as_array().ok_or_else(|| {
        HarnessError::Protocol("Prime daemon returned no terminal transcript".into())
    })?;
    for message in messages.iter().rev() {
        if message["role"] == "assistant" {
            return Ok(match message["stopReason"].as_str() {
                Some("error") => (
                    DoneStatus::Errored,
                    Some(
                        message["errorMessage"]
                            .as_str()
                            .unwrap_or("Prime model request failed")
                            .into(),
                    ),
                ),
                Some("aborted") => (DoneStatus::Interrupted, None),
                _ => (DoneStatus::Completed, None),
            });
        }
        if message["customType"] == "session_slash_command_result" {
            return Ok(if message["details"]["success"] == false {
                (
                    DoneStatus::Errored,
                    Some(
                        message["details"]["error"]
                            .as_str()
                            .unwrap_or("Prime command failed")
                            .into(),
                    ),
                )
            } else {
                (DoneStatus::Completed, None)
            });
        }
    }
    Ok((DoneStatus::Completed, None))
}

#[derive(Default)]
struct Children {
    by_id: HashMap<String, ChildState>,
}

struct ChildState {
    watch_key: String,
    status: String,
    error: Option<String>,
    observed: bool,
    unobserved: bool,
    settled: bool,
    terminal_since: Option<Instant>,
    mapper: EventMapper,
}

fn child_tool_id(child_id: &str) -> String {
    format!("prime-child:{child_id}")
}

fn tagged(child_id: &str, event: AgentEvent) -> AgentEvent {
    AgentEvent::Subagent {
        parent_tool_use_id: child_tool_id(child_id),
        event: Box::new(event),
    }
}

fn child_done(child_id: &str, state: &mut ChildState) -> AgentEvent {
    state.settled = true;
    let status = match state.status.as_str() {
        "error" => DoneStatus::Errored,
        "cancelled" => DoneStatus::Interrupted,
        _ => DoneStatus::Completed,
    };
    tagged(
        child_id,
        AgentEvent::Done {
            status,
            result: None,
            error: state
                .error
                .clone()
                .or_else(|| state.mapper.failure().map(str::to_owned)),
            session_id: None,
        },
    )
}

impl Children {
    async fn update(&mut self, child: &Value, client: &PrimeClient, cwd: &str) -> Vec<AgentEvent> {
        let Some(id) = child
            .get("id")
            .and_then(Value::as_str)
            .filter(|id| !id.is_empty())
        else {
            return Vec::new();
        };
        let status = child
            .get("status")
            .and_then(Value::as_str)
            .unwrap_or("running");
        let mut events = Vec::new();
        if !self.by_id.contains_key(id) {
            let label = child
                .get("sessionName")
                .and_then(Value::as_str)
                .or_else(|| child.get("label").and_then(Value::as_str))
                .unwrap_or("Prime subagent");
            let model = child
                .get("model")
                .and_then(Value::as_str)
                .unwrap_or_default();
            let watch_key = child
                .get("activeSessionId")
                .and_then(Value::as_str)
                .unwrap_or(id)
                .to_owned();
            events.push(AgentEvent::ToolCall {
                id: child_tool_id(id),
                call: ToolCall::Unknown {
                    name: format!("Agent: {label}"),
                    input: (!model.is_empty()).then(|| json!({ "model": model })),
                },
            });
            events.push(tagged(
                id,
                AgentEvent::SessionStarted {
                    harness: HarnessId::Prime,
                    model: model.into(),
                    tools: Vec::new(),
                    cwd: cwd.into(),
                    session_id: watch_key.clone(),
                    assistant_message_id: Uuid::new_v4().to_string(),
                },
            ));
            self.by_id.insert(
                id.into(),
                ChildState {
                    watch_key: watch_key.clone(),
                    status: status.into(),
                    error: None,
                    observed: false,
                    unobserved: false,
                    settled: false,
                    terminal_since: None,
                    mapper: EventMapper::new(),
                },
            );
        }
        if let Some(state) = self.by_id.get_mut(id) {
            // The queued snapshot precedes child-session publication. In a
            // daemon the later snapshot also supplies its activeSessionId.
            if let Some(active_id) = child.get("activeSessionId").and_then(Value::as_str) {
                if state.watch_key != active_id {
                    state.watch_key = active_id.into();
                    state.observed = false;
                }
            }
            state.status = status.into();
            state.error = child
                .get("error")
                .and_then(Value::as_str)
                .map(str::to_owned);
        }
        let watch_key = self
            .by_id
            .get(id)
            .filter(|state| status != "queued" && !state.observed && !state.settled)
            .map(|state| state.watch_key.clone());
        if let Some(watch_key) = watch_key {
            match tokio::time::timeout(
                Duration::from_secs(2),
                client.request("observe", json!({ "activeSessionId": watch_key })),
            )
            .await
            {
                Ok(Ok(data)) => {
                    if let Some(state) = self.by_id.get_mut(id) {
                        state.observed = true;
                    }
                    if let Some(messages) = data.get("messages").and_then(Value::as_array) {
                        events.extend(
                            replay_messages(messages)
                                .into_iter()
                                .map(|event| tagged(id, event)),
                        );
                    }
                }
                // A child may still be publishing when its first running
                // snapshot arrives. A later snapshot retries observation.
                Ok(Err(_)) | Err(_) => {}
            }
        }
        if let Some(state) = self.by_id.get_mut(id) {
            if matches!(status, "done" | "error" | "cancelled") && !state.settled {
                state.terminal_since.get_or_insert_with(Instant::now);
                if !state.observed {
                    events.push(child_done(id, state));
                }
            }
        }
        events
    }

    fn observed(&mut self, message: &Value) -> Vec<AgentEvent> {
        let Some(watch_key) = message.get("activeSessionId").and_then(Value::as_str) else {
            return Vec::new();
        };
        // ponytail: linear lookup is cheap for a session's child roster;
        // add a watch-key index if very large rosters make it measurable.
        let Some((id, state)) = self
            .by_id
            .iter_mut()
            .find(|(_, state)| state.watch_key == watch_key)
        else {
            return Vec::new();
        };
        if state.settled || !state.observed {
            return Vec::new();
        }
        match message.get("type").and_then(Value::as_str) {
            Some("observed_session_event") => {
                let event = &message["event"];
                let mut mapped: Vec<_> = state
                    .mapper
                    .map(event)
                    .into_iter()
                    .map(|event| tagged(id, event))
                    .collect();
                if event.get("type").and_then(Value::as_str) == Some("agent_end")
                    && state.terminal_since.is_some()
                {
                    mapped.push(child_done(id, state));
                }
                mapped
            }
            Some("observed_session_closed") => {
                if let Some(error) = message.get("error").and_then(Value::as_str) {
                    state.status = "error".into();
                    state.error = Some(error.into());
                }
                vec![child_done(id, state)]
            }
            _ => Vec::new(),
        }
    }

    fn settle_due(&mut self) -> Vec<AgentEvent> {
        self.by_id
            .iter_mut()
            .filter_map(|(id, state)| {
                if !state.settled
                    && state
                        .terminal_since
                        .is_some_and(|at| at.elapsed() >= Duration::from_millis(500))
                {
                    Some(child_done(id, state))
                } else {
                    None
                }
            })
            .collect()
    }

    fn finished_observations(&mut self) -> Vec<String> {
        self.by_id
            .values_mut()
            .filter_map(|state| {
                if state.settled && state.observed && !state.unobserved {
                    state.unobserved = true;
                    Some(state.watch_key.clone())
                } else {
                    None
                }
            })
            .collect()
    }

    fn finish_open(&mut self) -> Vec<AgentEvent> {
        self.by_id
            .iter_mut()
            .filter_map(|(id, state)| {
                (!state.settled).then(|| {
                    state.status = "cancelled".into();
                    child_done(id, state)
                })
            })
            .collect()
    }
}
