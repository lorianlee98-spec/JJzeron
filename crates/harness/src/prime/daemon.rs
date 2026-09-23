//! Prime's resident daemon JSONL RPC. Its completion commands are the native
//! lifecycle authority; CLI RPC does not expose them.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use serde_json::{Value, json};
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncWrite, AsyncWriteExt, BufReader};
use tokio::sync::{mpsc, oneshot};
use uuid::Uuid;

use crate::HarnessError;

type Pending = Arc<Mutex<HashMap<String, oneshot::Sender<Result<Value, String>>>>>;

#[derive(Clone)]
pub(super) struct DaemonClient {
    client_id: String,
    protocol_version: u64,
    root_id: Arc<Mutex<Option<String>>>,
    observed: Arc<Mutex<HashSet<String>>>,
    next_id: Arc<AtomicU64>,
    pending: Pending,
    writer: mpsc::UnboundedSender<Value>,
}

impl DaemonClient {
    pub(super) async fn connect(
        cwd: &str,
        resume: Option<&str>,
    ) -> Result<(Self, mpsc::UnboundedReceiver<Value>), HarnessError> {
        #[cfg(unix)]
        let socket = {
            let path = std::env::temp_dir()
                .join(format!("prime-agent-{}", unsafe { libc::getuid() }))
                .join("daemon.sock");
            tokio::net::UnixStream::connect(path).await?
        };
        #[cfg(windows)]
        let socket = tokio::net::windows::named_pipe::ClientOptions::new()
            .open(r"\\.\pipe\prime-agent-daemon")?;
        let (client, incoming) = Self::from_stream(socket).await?;
        let mut create = json!({
            "lifecycle": "resident",
            "config": { "cwd": cwd, "executionMode": "rpc" },
        });
        if let Some(path) = resume {
            create["sessionPath"] = Value::String(path.into());
        }
        let created = client.request_raw("create", create).await?;
        let root_id = created["activeSessionId"]
            .as_str()
            .ok_or_else(|| {
                HarnessError::Protocol("Prime daemon create returned no activeSessionId".into())
            })?
            .to_owned();
        *client.root_id.lock().unwrap() = Some(root_id.clone());
        let attached = client
            .request_raw(
                "attach",
                json!({
                    "activeSessionId": root_id,
                    "clientId": client.client_id,
                    "supportsExtensionUi": true,
                    "capabilities": ["attach_snapshot", "event_sequence", "extension_ui"],
                }),
            )
            .await;
        if let Err(error) = attached {
            if resume.is_none() {
                let _ = tokio::time::timeout(
                    std::time::Duration::from_secs(5),
                    client.request("kill", json!({})),
                )
                .await;
            }
            return Err(error);
        }
        Ok((client, incoming))
    }

    async fn from_stream<S>(
        stream: S,
    ) -> Result<(Self, mpsc::UnboundedReceiver<Value>), HarnessError>
    where
        S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        let (read, mut write) = tokio::io::split(stream);
        let mut lines = BufReader::new(read).lines();
        let hello = tokio::time::timeout(std::time::Duration::from_secs(5), lines.next_line())
            .await
            .map_err(|_| HarnessError::Protocol("Prime daemon handshake timed out".into()))??
            .ok_or_else(|| HarnessError::Protocol("Prime daemon closed before handshake".into()))?;
        let hello: Value = serde_json::from_str(&hello).map_err(|error| {
            HarnessError::Protocol(format!("Invalid Prime daemon handshake: {error}"))
        })?;
        let protocol_version = hello["protocol"]["version"].as_u64().unwrap_or(0);
        if hello["type"] != "daemon_hello"
            || hello["protocol"]["name"] != "prime-agent.daemon"
            || protocol_version < 7
            || !hello["serverCapabilities"]
                .as_array()
                .is_some_and(|capabilities| {
                    capabilities
                        .iter()
                        .any(|capability| capability == "rlm_quiescence_barrier")
                })
        {
            return Err(HarnessError::Protocol(
                "Prime daemon lacks the native RLM completion protocol; update Prime Agent".into(),
            ));
        }

        let (writer, mut outgoing) = mpsc::unbounded_channel::<Value>();
        tokio::spawn(async move {
            while let Some(command) = outgoing.recv().await {
                if write
                    .write_all(command.to_string().as_bytes())
                    .await
                    .is_err()
                    || write.write_all(b"\n").await.is_err()
                    || write.flush().await.is_err()
                {
                    break;
                }
            }
        });

        let (incoming_tx, incoming) = mpsc::unbounded_channel();
        let pending: Pending = Arc::default();
        let root_id = Arc::new(Mutex::new(None::<String>));
        let observed = Arc::new(Mutex::new(HashSet::<String>::new()));
        let client_id = format!("jjzeron:{}", Uuid::new_v4());
        let reader_pending = Arc::clone(&pending);
        let reader_root = Arc::clone(&root_id);
        let reader_observed = Arc::clone(&observed);
        let ack_writer = writer.downgrade();
        let ack_client_id = client_id.clone();
        tokio::spawn(async move {
            let failure = loop {
                match lines.next_line().await {
                    Ok(Some(line)) if line.is_empty() => continue,
                    Ok(Some(line)) => {
                        let value: Value = match serde_json::from_str(&line) {
                            Ok(value) => value,
                            Err(error) => break format!("Prime daemon sent invalid JSON: {error}"),
                        };
                        if value["type"] == "response" {
                            if let Some(id) = value["id"].as_str()
                                && let Some(waiter) = reader_pending.lock().unwrap().remove(id)
                            {
                                let result = if value["success"] == true {
                                    Ok(value.get("data").cloned().unwrap_or(Value::Null))
                                } else {
                                    Err(value["error"]
                                        .as_str()
                                        .unwrap_or("Prime daemon request failed")
                                        .into())
                                };
                                let _ = waiter.send(result);
                                if let Some(writer) = ack_writer.upgrade() {
                                    let _ = writer.send(json!({
                                        "type":"command",
                                        "id":format!("ack-{id}"),
                                        "clientId":ack_client_id,
                                        "protocol":{"name":"prime-agent.daemon","version":protocol_version},
                                        "command":{"type":"ack_result","commandId":id},
                                    }));
                                }
                            }
                            continue;
                        }
                        let root = reader_root.lock().unwrap().clone();
                        let session_id = value["activeSessionId"].as_str();
                        let event = match value["type"].as_str() {
                            Some("session_event") if session_id == root.as_deref() => {
                                value.get("event").cloned()
                            }
                            Some("session_event")
                                if session_id.is_some_and(|id| {
                                    reader_observed.lock().unwrap().contains(id)
                                }) =>
                            {
                                Some(
                                    json!({"type":"observed_session_event", "activeSessionId":session_id, "event":value["event"]}),
                                )
                            }
                            Some("session_closed") if session_id == root.as_deref() => {
                                let _ = incoming_tx.send(value.clone());
                                Some(
                                    json!({"type":"_transport_closed", "error":"Prime daemon session closed"}),
                                )
                            }
                            Some("session_closed")
                                if session_id.is_some_and(|id| {
                                    reader_observed.lock().unwrap().contains(id)
                                }) =>
                            {
                                Some(
                                    json!({"type":"observed_session_closed", "activeSessionId":session_id}),
                                )
                            }
                            Some("extension_ui_request") if session_id == root.as_deref() => {
                                let mut event = value["payload"].clone();
                                if let Some(object) = event.as_object_mut() {
                                    object.insert("type".into(), json!("extension_ui_request"));
                                    object.insert("id".into(), value["id"].clone());
                                    object.insert("method".into(), value["method"].clone());
                                }
                                Some(event)
                            }
                            Some("extension_error") if session_id == root.as_deref() => Some(value),
                            Some(
                                "session_status"
                                | "session_replaced"
                                | "session_resynced"
                                | "session_attached"
                                | "session_detached"
                                | "side_question_event",
                            ) if session_id == root.as_deref() => Some(value),
                            Some("heartbeats_changed" | "roster_update" | "daemon_closing") => {
                                Some(value)
                            }
                            _ => None,
                        };
                        if let Some(event) = event
                            && incoming_tx.send(event).is_err()
                        {
                            break "Prime daemon event consumer closed".into();
                        }
                    }
                    Ok(None) => break "Prime daemon connection closed".into(),
                    Err(error) => break format!("Prime daemon connection failed: {error}"),
                }
            };
            for (_, waiter) in reader_pending.lock().unwrap().drain() {
                let _ = waiter.send(Err(failure.clone()));
            }
            let _ = incoming_tx.send(json!({"type":"_transport_closed", "error":failure}));
        });

        Ok((
            Self {
                client_id,
                protocol_version,
                root_id,
                observed,
                next_id: Arc::new(AtomicU64::new(0)),
                pending,
                writer,
            },
            incoming,
        ))
    }

    async fn request_raw(&self, kind: &str, mut fields: Value) -> Result<Value, HarnessError> {
        let id = format!("jj-{}", self.next_id.fetch_add(1, Ordering::Relaxed));
        let object = fields.as_object_mut().ok_or_else(|| {
            HarnessError::Protocol("Prime daemon request fields must be an object".into())
        })?;
        object.insert("type".into(), Value::String(kind.into()));
        let command = json!({
            "type": "command",
            "id": id,
            "clientId": self.client_id,
            "protocol": { "name": "prime-agent.daemon", "version": self.protocol_version },
            "command": fields,
        });
        let (tx, rx) = oneshot::channel();
        self.pending.lock().unwrap().insert(id.clone(), tx);
        if self.writer.send(command).is_err() {
            self.pending.lock().unwrap().remove(&id);
            return Err(HarnessError::Protocol("Prime daemon socket closed".into()));
        }
        match rx.await {
            Ok(Ok(data)) => Ok(data),
            Ok(Err(error)) => Err(HarnessError::Protocol(format!(
                "Prime daemon {kind}: {error}"
            ))),
            Err(_) => Err(HarnessError::Protocol(format!(
                "Prime daemon closed during {kind}"
            ))),
        }
    }

    pub(super) async fn request(
        &self,
        kind: &str,
        mut fields: Value,
    ) -> Result<Value, HarnessError> {
        let root = self.root_id.lock().unwrap().clone().ok_or_else(|| {
            HarnessError::Protocol("Prime daemon session has no active ID".into())
        })?;
        let object = fields.as_object_mut().ok_or_else(|| {
            HarnessError::Protocol("Prime daemon request fields must be an object".into())
        })?;
        let target = object
            .get("activeSessionId")
            .and_then(Value::as_str)
            .unwrap_or(&root)
            .to_owned();
        object.insert("activeSessionId".into(), Value::String(target.clone()));
        match kind {
            "observe" => {
                self.observed.lock().unwrap().insert(target.clone());
                object.insert("clientId".into(), Value::String(self.client_id.clone()));
                object.insert(
                    "capabilities".into(),
                    json!(["attach_snapshot", "event_sequence"]),
                );
                match self.request_raw("attach", fields).await {
                    Ok(data) => Ok(json!({"messages": data["snapshot"]["messages"]})),
                    Err(error) => {
                        self.observed.lock().unwrap().remove(&target);
                        Err(error)
                    }
                }
            }
            "unobserve" => {
                self.observed.lock().unwrap().remove(&target);
                self.request_raw("detach", fields).await
            }
            "get_state" => self.request_raw("get_connection_state", fields).await,
            "get_session_summary" => self.request_raw("get_state", fields).await,
            _ => self.request_raw(kind, fields).await,
        }
    }

    pub(super) fn send(&self, value: Value) -> Result<(), HarnessError> {
        if value["type"] != "extension_ui_response" {
            return Err(HarnessError::Protocol(
                "Unsupported Prime daemon notification".into(),
            ));
        }
        let id = value["id"]
            .as_str()
            .ok_or_else(|| HarnessError::Protocol("Prime extension UI response has no ID".into()))?
            .to_owned();
        let response = if let Some(confirmed) = value["confirmed"].as_bool() {
            json!({"confirmed": confirmed})
        } else if let Some(answer) = value["value"].as_str() {
            json!({"value": answer})
        } else {
            json!({"cancelled": true})
        };
        let client = self.clone();
        tokio::spawn(async move {
            let _ = client
                .request(
                    "extension_ui_response",
                    json!({"requestId":id,"response":response}),
                )
                .await;
        });
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::prime::client::PrimeClient;
    use crate::prime::session;
    use crate::{CancellationToken, RunControls, StderrTail};
    use tokio::sync::oneshot;
    use zeron_proto::{AgentEvent, DoneStatus, RunRequest, SandboxLevel};

    #[tokio::test]
    async fn retrying_agent_end_cannot_finish_the_prime_response() {
        let (socket, peer) = tokio::io::duplex(64 * 1024);
        let (read, mut write) = tokio::io::split(peer);
        write.write_all(b"{\"type\":\"daemon_hello\",\"protocol\":{\"name\":\"prime-agent.daemon\",\"version\":7},\"serverCapabilities\":[\"rlm_quiescence_barrier\"]}\n").await.unwrap();
        let (client, incoming) = DaemonClient::from_stream(socket).await.unwrap();
        *client.root_id.lock().unwrap() = Some("root".into());
        let (release_retry, retry_released) = oneshot::channel();
        tokio::spawn(async move {
            let mut lines = BufReader::new(read).lines();
            let mut retry_released = Some(retry_released);
            let mut weak_waits = 0;
            let mut strong_waits = 0;
            let mut pending_strong = None::<String>;
            while let Ok(Some(line)) = lines.next_line().await {
                let command: Value = serde_json::from_str(&line).unwrap();
                let id = &command["id"];
                let kind = command["command"]["type"].as_str().unwrap();
                if kind == "ack_result" {
                    continue;
                }
                let mut events = Vec::new();
                let data = match kind {
                    "get_connection_state" => {
                        json!({"sessionFile":"/tmp/jj-prime-test.jsonl","model":{"provider":"test","id":"model"},"thinkingLevel":"high","goal":null})
                    }
                    "get_state" => {
                        json!({"isSessionActive":weak_waits == 1,"hasRunningRlmChildren":weak_waits == 1,"unfinishedActionCount":0})
                    }
                    "get_commands" => json!({"commands":[]}),
                    "get_session_stats" => {
                        json!({"contextUsage":{"tokens":100,"contextWindow":200000}})
                    }
                    "prompt" => {
                        events.extend([
                            json!({"type":"agent_start"}),
                            json!({"type":"message_end","message":{"role":"assistant","stopReason":"error","errorMessage":"transient provider error"}}),
                            json!({"type":"agent_end"}),
                            json!({"type":"auto_retry_start","attempt":1,"maxAttempts":3}),
                        ]);
                        Value::Null
                    }
                    "wait_for_headless_completion"
                        if command["command"]["waitForRlmQuiescence"] != true =>
                    {
                        weak_waits += 1;
                        if weak_waits == 1 {
                            retry_released.take().unwrap().await.unwrap();
                            events.extend([
                                json!({"type":"agent_start"}),
                                json!({"type":"message_end","message":{"role":"assistant","stopReason":"stop","content":[{"type":"text","text":"recovered"}]}}),
                                json!({"type":"auto_retry_end","attempt":1,"success":true}),
                                json!({"type":"agent_end"}),
                            ]);
                        } else {
                            events.extend([
                                json!({"type":"message_end","message":{"role":"assistant","stopReason":"stop","content":[{"type":"text","text":"child received"}]}}),
                                json!({"type":"agent_end"}),
                            ]);
                        }
                        Value::Null
                    }
                    "wait_for_headless_completion" => {
                        strong_waits += 1;
                        if strong_waits == 1 {
                            pending_strong = id.as_str().map(str::to_owned);
                            let frame = json!({"type":"session_event","activeSessionId":"root","event":{"type":"agent_start"}});
                            write
                                .write_all(format!("{frame}\n").as_bytes())
                                .await
                                .unwrap();
                            continue;
                        }
                        Value::Null
                    }
                    "get_messages" => {
                        json!({"messages":[{"role":"assistant","stopReason":"stop","content":[{"type":"text","text":"recovered"}]}]})
                    }
                    _ => panic!("unexpected Prime daemon command: {kind}"),
                };
                for event in events {
                    let frame =
                        json!({"type":"session_event","activeSessionId":"root","event":event});
                    write
                        .write_all(format!("{frame}\n").as_bytes())
                        .await
                        .unwrap();
                }
                let response =
                    json!({"type":"response","id":id,"command":kind,"success":true,"data":data});
                write
                    .write_all(format!("{response}\n").as_bytes())
                    .await
                    .unwrap();
                if weak_waits == 2
                    && kind == "wait_for_headless_completion"
                    && let Some(strong_id) = pending_strong.take()
                {
                    let response = json!({"type":"response","id":strong_id,"command":"wait_for_headless_completion","success":true,"data":null});
                    write
                        .write_all(format!("{response}\n").as_bytes())
                        .await
                        .unwrap();
                }
            }
        });

        let (_steer, steering) = mpsc::channel(1);
        let controls = RunControls {
            steering,
            interrupt: CancellationToken::new(),
            request_input: Box::new(|_| {
                let (tx, rx) = oneshot::channel();
                let _ = tx.send(Vec::new());
                rx
            }),
        };
        let request = RunRequest {
            prompt: "run".into(),
            harness: Some(zeron_proto::HarnessId::Prime),
            model: Some("default".into()),
            reasoning: None,
            model_options: Default::default(),
            cwd: "/tmp".into(),
            sandbox: SandboxLevel::WorkspaceWrite,
            auto_approve: true,
            resume: None,
            attachments: Vec::new(),
            worktree: None,
        };
        let (tx, mut rx) = mpsc::channel(64);
        tokio::spawn(session::run_session(
            None,
            PrimeClient::Daemon(client),
            incoming,
            StderrTail::default(),
            request,
            controls,
            tx,
        ));
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            loop {
                let event = rx.recv().await.unwrap().unwrap();
                assert!(!matches!(event, AgentEvent::Done { .. }));
                if matches!(event, AgentEvent::PrimeEvent { event } if event["type"] == "auto_retry_start") {
                    break;
                }
            }
        }).await.unwrap();
        while let Ok(event) = rx.try_recv() {
            assert!(!matches!(event.unwrap(), AgentEvent::Done { .. }));
        }
        release_retry.send(()).unwrap();
        let mut ends = 1;
        let mut dones = 0;
        let mut resumed = false;
        let mut quiescent = false;
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            loop {
                match rx.recv().await.unwrap().unwrap() {
                    AgentEvent::PrimeEvent { event } if event["type"] == "agent_end" => ends += 1,
                    AgentEvent::PrimeEvent { event } if event["type"] == "agent_start" => {
                        resumed = true
                    }
                    AgentEvent::PrimeEvent { event }
                        if event["type"] == "lifecycle_update" && event["phase"] == "quiescent" =>
                    {
                        quiescent = true
                    }
                    AgentEvent::Done { status, .. } => {
                        assert!(matches!(status, DoneStatus::Completed));
                        dones += 1;
                    }
                    _ => {}
                }
                if dones == 2 && quiescent {
                    break;
                }
            }
        })
        .await
        .unwrap();
        assert_eq!(ends, 3);
        assert_eq!(dones, 2);
        assert!(resumed);
    }
}
