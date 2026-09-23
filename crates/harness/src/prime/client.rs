//! Prime Agent's JSONL RPC transport. Responses can overtake one another;
//! session events are delivered separately in their original stdout order.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use serde_json::{Value, json};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::sync::{mpsc, oneshot};

use crate::HarnessError;
use crate::process::{ChildStdin, ChildStdout};

type Pending = Arc<Mutex<HashMap<String, oneshot::Sender<Result<Value, String>>>>>;

#[derive(Clone)]
pub(super) struct PrimeClient {
    next_id: Arc<AtomicU64>,
    pending: Pending,
    writer: mpsc::UnboundedSender<Value>,
}

impl PrimeClient {
    pub(super) fn new(
        stdin: ChildStdin,
        stdout: ChildStdout,
    ) -> (Self, mpsc::UnboundedReceiver<Value>) {
        let (writer, mut outgoing) = mpsc::unbounded_channel::<Value>();
        // ponytail: this queue is unbounded so events cannot block RPC
        // responses; split the lanes if sustained output becomes a memory cost.
        let (incoming_tx, incoming) = mpsc::unbounded_channel();
        let pending: Pending = Arc::default();

        tokio::spawn(async move {
            let mut stdin = stdin;
            while let Some(value) = outgoing.recv().await {
                let line = value.to_string();
                if stdin.write_all(line.as_bytes()).await.is_err()
                    || stdin.write_all(b"\n").await.is_err()
                    || stdin.flush().await.is_err()
                {
                    break;
                }
            }
        });

        let reader_pending = Arc::clone(&pending);
        tokio::spawn(async move {
            let mut lines = BufReader::new(stdout).lines();
            let failure = loop {
                match lines.next_line().await {
                    Ok(Some(line)) if line.is_empty() => continue,
                    Ok(Some(line)) => match serde_json::from_str::<Value>(&line) {
                        Ok(value)
                            if value.get("type").and_then(Value::as_str) == Some("response") =>
                        {
                            let Some(id) = value.get("id").and_then(Value::as_str) else {
                                continue;
                            };
                            let response =
                                if value.get("success").and_then(Value::as_bool) == Some(true) {
                                    Ok(value.get("data").cloned().unwrap_or(Value::Null))
                                } else {
                                    Err(value
                                        .get("error")
                                        .and_then(Value::as_str)
                                        .unwrap_or("Prime RPC request failed")
                                        .to_owned())
                                };
                            if let Some(waiter) = reader_pending.lock().unwrap().remove(id) {
                                let _ = waiter.send(response);
                            }
                        }
                        Ok(value) => {
                            if incoming_tx.send(value).is_err() {
                                break "Prime RPC consumer closed".to_owned();
                            }
                        }
                        Err(error) => break format!("Prime RPC sent invalid JSON: {error}"),
                    },
                    Ok(None) => break "Prime RPC process closed stdout".to_owned(),
                    Err(error) => break format!("Prime RPC stdout failed: {error}"),
                }
            };
            for (_, waiter) in reader_pending.lock().unwrap().drain() {
                let _ = waiter.send(Err(failure.clone()));
            }
            let _ = incoming_tx.send(json!({ "type": "_transport_closed", "error": failure }));
        });

        (
            Self {
                next_id: Arc::new(AtomicU64::new(0)),
                pending,
                writer,
            },
            incoming,
        )
    }

    pub(super) async fn request(
        &self,
        kind: &str,
        mut fields: Value,
    ) -> Result<Value, HarnessError> {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed).to_string();
        let object = fields.as_object_mut().ok_or_else(|| {
            HarnessError::Protocol("Prime RPC request fields must be an object".into())
        })?;
        object.insert("id".into(), Value::String(id.clone()));
        object.insert("type".into(), Value::String(kind.to_owned()));
        let (tx, rx) = oneshot::channel();
        self.pending.lock().unwrap().insert(id.clone(), tx);
        if self.writer.send(fields).is_err() {
            self.pending.lock().unwrap().remove(&id);
            return Err(HarnessError::Protocol(format!(
                "Prime RPC stdin closed before {kind}"
            )));
        }
        match rx.await {
            Ok(Ok(data)) => Ok(data),
            Ok(Err(error)) => Err(HarnessError::Protocol(format!("Prime RPC {kind}: {error}"))),
            Err(_) => Err(HarnessError::Protocol(format!(
                "Prime RPC closed during {kind}"
            ))),
        }
    }

    pub(super) fn send(&self, value: Value) -> Result<(), HarnessError> {
        self.writer
            .send(value)
            .map_err(|_| HarnessError::Protocol("Prime RPC stdin closed".into()))
    }
}
