//! Codex's typed thread items omit programmatic tool outputs. Their native
//! rollout retains those outputs on both fresh and resumed app-server threads.

use std::io::{Read, Seek, SeekFrom};
use std::path::PathBuf;

use serde_json::Value;
use zeron_proto::AgentEvent;

const MAX_READ: u64 = 64 * 1024 * 1024;
const MAX_LINE: usize = 40 * 1024 * 1024;

pub(super) struct RolloutImages {
    path: PathBuf,
    offset: u64,
    pending: Vec<u8>,
}

impl RolloutImages {
    pub(super) fn new(path: &str, resumed: bool) -> Option<Self> {
        let path = PathBuf::from(path);
        if !path.is_absolute() || path.extension().is_none_or(|ext| ext != "jsonl") {
            return None;
        }
        let offset = if resumed {
            std::fs::metadata(&path)
                .map(|metadata| metadata.len())
                .unwrap_or(0)
        } else {
            0
        };
        Some(Self {
            path,
            offset,
            pending: Vec::new(),
        })
    }

    pub(super) fn poll(&mut self) -> std::io::Result<Vec<AgentEvent>> {
        let mut file = match std::fs::File::open(&self.path) {
            Ok(file) => file,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(error) => return Err(error),
        };
        if file.metadata()?.len() < self.offset {
            self.offset = 0;
            self.pending.clear();
        }
        file.seek(SeekFrom::Start(self.offset))?;
        let mut appended = Vec::new();
        file.take(MAX_READ).read_to_end(&mut appended)?;
        self.offset += appended.len() as u64;
        self.pending.extend_from_slice(&appended);
        let Some(end) = self.pending.iter().rposition(|byte| *byte == b'\n') else {
            if self.pending.len() > MAX_LINE {
                self.pending.clear();
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "Codex rollout item exceeds the image limit",
                ));
            }
            return Ok(Vec::new());
        };
        let complete = self.pending.drain(..=end).collect::<Vec<_>>();
        if self.pending.len() > MAX_LINE {
            self.pending.clear();
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Codex rollout item exceeds the image limit",
            ));
        }
        let mut events = Vec::new();
        for line in complete.split(|byte| *byte == b'\n') {
            if line.len() > MAX_LINE {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "Codex rollout item exceeds the image limit",
                ));
            }
            let Ok(record) = serde_json::from_slice::<Value>(line) else {
                continue;
            };
            if record["type"] != "response_item" {
                continue;
            }
            let payload = &record["payload"];
            if payload["type"] != "custom_tool_call_output"
                && payload["type"] != "function_call_output"
            {
                continue;
            }
            let Some(call_id) = payload["call_id"].as_str() else {
                continue;
            };
            events.extend(crate::tool_images::events(call_id, &payload["output"]));
        }
        Ok(events)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    #[test]
    fn follows_only_new_complete_native_outputs() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("rollout.jsonl");
        let mut source = std::fs::File::create(&path).unwrap();
        writeln!(source, "{{\"type\":\"response_item\",\"payload\":{{\"type\":\"custom_tool_call_output\",\"call_id\":\"old\",\"output\":[{{\"type\":\"input_image\",\"image_url\":\"data:image/png;base64,aGVsbG8=\"}}]}}}}\n").unwrap();
        let mut tail = RolloutImages::new(path.to_str().unwrap(), true).unwrap();
        write!(source, "{{\"type\":\"response_item\",\"payload\":{{\"type\":\"custom_tool_call_output\",\"call_id\":\"new\",\"output\":[{{\"type\":\"input_image\",\"image_url\":\"data:image/png;base64,d29ybGQ=\"}}]}}}}").unwrap();
        source.flush().unwrap();
        assert!(tail.poll().unwrap().is_empty());
        writeln!(source).unwrap();
        source.flush().unwrap();
        let images = tail.poll().unwrap();
        assert!(matches!(&images[0], AgentEvent::InlineImage { id, .. } if id == "new:image:0"));
        assert!(tail.poll().unwrap().is_empty());
    }

    #[tokio::test]
    async fn forwarded_mcp_image_is_not_rendered_twice_from_outer_call() {
        use sha2::{Digest, Sha256};

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("rollout.jsonl");
        std::fs::write(&path, b"{\"type\":\"response_item\",\"payload\":{\"type\":\"custom_tool_call_output\",\"call_id\":\"outer\",\"output\":[{\"type\":\"input_image\",\"image_url\":\"data:image/png;base64,aGVsbG8=\"}]}}\n").unwrap();
        let mut tail = Some(RolloutImages::new(path.to_str().unwrap(), false).unwrap());
        let mut nested = std::collections::HashSet::from([Sha256::digest(b"aGVsbG8=").into()]);
        let (tx, mut rx) = tokio::sync::mpsc::channel(2);
        assert!(super::super::forward_rollout_images(&mut tail, &mut nested, &tx).await);
        assert!(rx.try_recv().is_err());
        assert!(nested.is_empty());
    }
}
