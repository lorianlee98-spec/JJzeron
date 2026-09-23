//! Prime's native RPC events and persisted messages → Zeron's event stream.

use serde_json::Value;
use zeron_proto::{AgentEvent, ToolCall};

const OUTPUT_CAP: usize = 16 * 1024;

#[derive(Default)]
pub(super) struct EventMapper {
    failure: Option<String>,
    active_bash: Option<BashRun>,
    next_bash_id: u64,
}

struct BashRun {
    id: String,
    output: String,
    truncated: bool,
}

impl EventMapper {
    pub(super) fn new() -> Self {
        Self::default()
    }

    pub(super) fn failure(&self) -> Option<&str> {
        self.failure.as_deref()
    }

    pub(super) fn map(&mut self, event: &Value) -> Vec<AgentEvent> {
        match field(event, "type") {
            Some("extension_error") => {
                let path = field(event, "extensionPath").unwrap_or("extension");
                let error = field(event, "error").unwrap_or("Unknown extension error");
                vec![AgentEvent::Error {
                    message: cap_text(&format!("Prime extension {path}: {error}")),
                }]
            }
            Some("message_start") if field(&event["message"], "role") == Some("user") => {
                message_text(&event["message"]["content"])
                    .map(|text| AgentEvent::UserMessage { text })
                    .into_iter()
                    .collect()
            }
            Some("message_update") => match field(&event["assistantMessageEvent"], "type") {
                Some("text_delta") => field(&event["assistantMessageEvent"], "delta")
                    .filter(|delta| !delta.is_empty())
                    .map(|text| AgentEvent::TextDelta { text: text.into() })
                    .into_iter()
                    .collect(),
                Some("thinking_delta") => field(&event["assistantMessageEvent"], "delta")
                    .filter(|delta| !delta.is_empty())
                    .map(|text| AgentEvent::ReasoningDelta { text: text.into() })
                    .into_iter()
                    .collect(),
                _ => Vec::new(),
            },
            Some("message_end") if field(&event["message"], "role") == Some("assistant") => {
                if field(&event["message"], "stopReason") == Some("error") {
                    let message = field(&event["message"], "errorMessage")
                        .filter(|text| !text.is_empty())
                        .unwrap_or("Prime model request failed")
                        .to_owned();
                    // Prime may retry this agent_end. The native completion
                    // barrier decides whether the error is terminal.
                    self.failure = Some(message);
                    Vec::new()
                } else {
                    self.failure = None;
                    Vec::new()
                }
            }
            Some("tool_execution_start") => tool_event(event, "args")
                .map(|(id, call)| AgentEvent::ToolCall { id, call })
                .into_iter()
                .collect(),
            Some("tool_execution_end") => field(event, "toolCallId")
                .map(|id| {
                    tool_result_events(
                        id,
                        &event["result"],
                        event["isError"].as_bool().unwrap_or(false),
                    )
                })
                .unwrap_or_default(),
            Some("bash_start") => {
                self.next_bash_id += 1;
                let id = field(event, "runId")
                    .map(|id| format!("prime-bash-{id}"))
                    .unwrap_or_else(|| format!("prime-bash-{}", self.next_bash_id));
                self.active_bash = Some(BashRun {
                    id: id.clone(),
                    output: String::new(),
                    truncated: false,
                });
                vec![AgentEvent::ToolCall {
                    id,
                    call: ToolCall::Exec {
                        command: field(event, "command").unwrap_or_default().into(),
                    },
                }]
            }
            Some("bash_output") => {
                if let (Some(run), Some(chunk)) = (&mut self.active_bash, field(event, "chunk")) {
                    append_bounded(&mut run.output, chunk, &mut run.truncated);
                }
                Vec::new()
            }
            Some("bash_end") => {
                let Some(run) = self.active_bash.take() else {
                    return Vec::new();
                };
                let output = if run.output.is_empty() {
                    field(event, "errorMessage").map(cap_text)
                } else if run.truncated || event["truncated"].as_bool() == Some(true) {
                    Some(format!("{}\n… [truncated]", run.output))
                } else {
                    Some(run.output)
                };
                vec![AgentEvent::ToolResult {
                    id: run.id,
                    is_error: event["cancelled"].as_bool() == Some(true)
                        || event["exitCode"].as_i64().is_some_and(|code| code != 0)
                        || field(event, "errorMessage").is_some(),
                    output,
                    diff: None,
                }]
            }
            _ => Vec::new(),
        }
    }
}

/// `observe` returns complete messages first; live notifications start after that
/// snapshot. Each completed block is appended once, without replaying the
/// incremental deltas that produced it.
pub(super) fn replay_messages(messages: &[Value]) -> Vec<AgentEvent> {
    let mut events = Vec::new();
    for message in messages {
        match field(message, "role") {
            Some("user") => {
                if let Some(text) = message_text(&message["content"]) {
                    events.push(AgentEvent::UserMessage { text });
                }
            }
            Some("assistant") => {
                if let Some(blocks) = message["content"].as_array() {
                    for block in blocks {
                        match field(block, "type") {
                            Some("text") => {
                                if let Some(text) = field(block, "text").filter(|s| !s.is_empty()) {
                                    events.push(AgentEvent::TextDelta { text: text.into() });
                                }
                            }
                            Some("thinking") => {
                                if let Some(text) =
                                    field(block, "thinking").filter(|s| !s.is_empty())
                                {
                                    events.push(AgentEvent::ReasoningDelta { text: text.into() });
                                }
                            }
                            Some("toolCall") => {
                                if let Some(id) = field(block, "id") {
                                    events.push(AgentEvent::ToolCall {
                                        id: id.into(),
                                        call: typed_tool(
                                            field(block, "name").unwrap_or("tool"),
                                            &block["arguments"],
                                        ),
                                    });
                                }
                            }
                            _ => {}
                        }
                    }
                }
            }
            Some("toolResult") => {
                if let Some(id) = field(message, "toolCallId") {
                    events.extend(tool_result_events(
                        id,
                        &message["content"],
                        message["isError"].as_bool().unwrap_or(false),
                    ));
                }
            }
            _ => {}
        }
    }
    events
}

fn field<'a>(value: &'a Value, key: &str) -> Option<&'a str> {
    value.get(key).and_then(Value::as_str)
}

fn tool_event(event: &Value, args_key: &str) -> Option<(String, ToolCall)> {
    let id = field(event, "toolCallId")?;
    let name = field(event, "toolName").unwrap_or("tool");
    Some((id.into(), typed_tool(name, &event[args_key])))
}

fn typed_tool(name: &str, args: &Value) -> ToolCall {
    match name {
        "ipython" => ToolCall::Exec {
            command: field(args, "code").unwrap_or_default().into(),
        },
        "bash" => ToolCall::Exec {
            command: field(args, "command").unwrap_or_default().into(),
        },
        _ => ToolCall::Unknown {
            name: name.into(),
            input: (!args.is_null()).then(|| args.clone()),
        },
    }
}

fn message_text(content: &Value) -> Option<String> {
    match content {
        Value::String(text) if !text.is_empty() => Some(text.clone()),
        Value::Array(blocks) => join_text_blocks(blocks),
        _ => None,
    }
}

fn result_text(result: &Value) -> Option<String> {
    if let Some(text) = result.as_str().or_else(|| field(result, "output")) {
        return (!text.is_empty()).then(|| cap_text(text));
    }
    result
        .as_array()
        .or_else(|| result.get("content").and_then(Value::as_array))
        .and_then(|blocks| join_text_blocks(blocks))
        .map(|text| cap_text(&text))
}

fn tool_result_events(id: &str, result: &Value, is_error: bool) -> Vec<AgentEvent> {
    let mut events = vec![AgentEvent::ToolResult {
        id: id.into(),
        is_error,
        output: result_text(result),
        diff: None,
    }];
    let content = result.get("content").unwrap_or(result);
    events.extend(crate::tool_images::events(id, content));
    events
}

/// Prime events remain subscribable, but large image bytes are represented by
/// the durable image part rather than copied into every journal subscriber.
pub(super) fn without_image_data(event: &Value) -> Value {
    fn scrub(value: &mut Value) {
        match value {
            Value::Array(items) => items.iter_mut().for_each(scrub),
            Value::Object(fields) => {
                if fields.get("type").and_then(Value::as_str) == Some("image") {
                    if fields.remove("data").is_some() {
                        fields.insert("dataOmitted".into(), Value::Bool(true));
                    }
                }
                if matches!(
                    fields.get("type").and_then(Value::as_str),
                    Some("input_image" | "inputImage")
                ) {
                    for key in ["image_url", "imageUrl"] {
                        if fields
                            .get(key)
                            .and_then(Value::as_str)
                            .is_some_and(|url| url.starts_with("data:image/"))
                        {
                            fields.remove(key);
                            fields.insert("dataOmitted".into(), Value::Bool(true));
                        }
                    }
                }
                for child in fields.values_mut() {
                    scrub(child);
                }
            }
            _ => {}
        }
    }
    let mut event = event.clone();
    scrub(&mut event);
    event
}

fn join_text_blocks(blocks: &[Value]) -> Option<String> {
    let text = blocks
        .iter()
        .filter(|block| field(block, "type") == Some("text"))
        .filter_map(|block| field(block, "text"))
        .filter(|text| !text.is_empty())
        .collect::<Vec<_>>()
        .join("\n");
    (!text.is_empty()).then_some(text)
}

fn cap_text(text: &str) -> String {
    if text.len() <= OUTPUT_CAP {
        return text.into();
    }
    let mut end = OUTPUT_CAP;
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}\n… [truncated]", &text[..end])
}

fn append_bounded(output: &mut String, chunk: &str, truncated: &mut bool) {
    let remaining = OUTPUT_CAP.saturating_sub(output.len());
    if chunk.len() <= remaining {
        output.push_str(chunk);
    } else {
        let mut end = remaining;
        while !chunk.is_char_boundary(end) {
            end -= 1;
        }
        output.push_str(&chunk[..end]);
        *truncated = true;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn maps_live_and_replayed_prime_transcript_without_duplicate_deltas() {
        let mut mapper = EventMapper::new();
        assert_eq!(
            mapper.map(&json!({"type":"message_update","assistantMessageEvent":{"type":"text_delta","delta":"Hello"}})),
            vec![AgentEvent::TextDelta { text: "Hello".into() }]
        );
        assert_eq!(
            mapper.map(&json!({"type":"tool_execution_start","toolCallId":"call-1","toolName":"ipython","args":{"code":"print(1)"}})),
            vec![AgentEvent::ToolCall { id: "call-1".into(), call: ToolCall::Exec { command: "print(1)".into() } }]
        );
        assert_eq!(
            mapper.map(&json!({"type":"tool_execution_end","toolCallId":"call-1","result":{"content":[{"type":"text","text":"1"}]},"isError":false})),
            vec![AgentEvent::ToolResult { id: "call-1".into(), is_error: false, output: Some("1".into()), diff: None }]
        );
        assert!(mapper.map(&json!({"type":"message_update","assistantMessageEvent":{"type":"text_end","content":"Hello"}})).is_empty());
        assert!(mapper.map(&json!({"type":"message_end","message":{"role":"assistant","stopReason":"error","errorMessage":"provider unavailable"}})).is_empty());
        assert_eq!(mapper.failure(), Some("provider unavailable"));
        assert!(mapper
            .map(&json!({"type":"message_end","message":{"role":"assistant","stopReason":"stop"}}))
            .is_empty());
        assert_eq!(mapper.failure(), None);
        assert_eq!(
            mapper.map(&json!({"type":"extension_error","extensionPath":"/project/extension.ts","error":"hook failed"})),
            vec![AgentEvent::Error { message: "Prime extension /project/extension.ts: hook failed".into() }]
        );
        assert_eq!(
            mapper.map(&json!({"type":"bash_start","runId":"run-1","command":"echo ok"})),
            vec![AgentEvent::ToolCall {
                id: "prime-bash-run-1".into(),
                call: ToolCall::Exec {
                    command: "echo ok".into()
                }
            }]
        );
        assert!(
            mapper
                .map(&json!({"type":"bash_output","chunk":"ok\n"}))
                .is_empty()
        );
        assert_eq!(
            mapper.map(&json!({"type":"bash_end","runId":"run-1","exitCode":0,"cancelled":false})),
            vec![AgentEvent::ToolResult {
                id: "prime-bash-run-1".into(),
                is_error: false,
                output: Some("ok\n".into()),
                diff: None
            }]
        );

        let replay = replay_messages(&[
            json!({"role":"user","content":"Calculate"}),
            json!({"role":"assistant","content":[{"type":"thinking","thinking":"Compute"},{"type":"text","text":"One"},{"type":"toolCall","id":"call-1","name":"ipython","arguments":{"code":"print(1)"}}]}),
            json!({"role":"toolResult","toolCallId":"call-1","content":[{"type":"text","text":"1"}],"isError":false}),
        ]);
        assert_eq!(
            replay,
            vec![
                AgentEvent::UserMessage {
                    text: "Calculate".into()
                },
                AgentEvent::ReasoningDelta {
                    text: "Compute".into()
                },
                AgentEvent::TextDelta { text: "One".into() },
                AgentEvent::ToolCall {
                    id: "call-1".into(),
                    call: ToolCall::Exec {
                        command: "print(1)".into()
                    }
                },
                AgentEvent::ToolResult {
                    id: "call-1".into(),
                    is_error: false,
                    output: Some("1".into()),
                    diff: None
                },
            ]
        );
    }

    #[test]
    fn tool_result_images_survive_live_and_replay_without_raw_media_publication() {
        let notification = json!({
            "type": "tool_execution_end",
            "toolCallId": "call-image",
            "result": {"content": [
                {"type": "text", "text": "Screenshot"},
                {"type": "image", "mimeType": "image/png", "data": "aGVsbG8="}
            ]},
            "isError": false
        });
        let events = EventMapper::new().map(&notification);
        assert!(
            matches!(&events[0], AgentEvent::ToolResult { output: Some(text), .. } if text == "Screenshot")
        );
        assert!(
            matches!(&events[1], AgentEvent::InlineImage { id, data, .. } if id == "call-image:image:1" && data == "aGVsbG8=")
        );
        let public = without_image_data(&notification);
        assert!(public["result"]["content"][1]["data"].is_null());
        assert_eq!(public["result"]["content"][1]["dataOmitted"], true);
        let input_image = without_image_data(&json!({
            "type":"tool_execution_end",
            "result":{"content":[{"type":"input_image","image_url":"data:image/png;base64,aGVsbG8="}]}
        }));
        assert!(input_image["result"]["content"][0]["image_url"].is_null());
        assert_eq!(input_image["result"]["content"][0]["dataOmitted"], true);

        let replay = replay_messages(&[json!({
            "role": "toolResult",
            "toolCallId": "call-image",
            "content": notification["result"]["content"],
            "isError": false
        })]);
        assert_eq!(replay, events);
    }
}
