//! Prime Agent's native JSONL RPC driver. Prime owns its configuration,
//! extensions, skills, Python kernel, and session files; Zeron only drives it.

mod client;
mod events;
mod session;

use std::path::{Path, PathBuf};
use std::time::Duration;

use async_trait::async_trait;
use futures::StreamExt;
use futures::stream::BoxStream;
use serde_json::{Value, json};
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::sync::mpsc;
use zeron_proto::invocation::{
    Skill, SkillCommand, valid_invocation_name, valid_skill_command_name,
};
use zeron_proto::{
    AgentEvent, HarnessId, Model, ReasoningLevel, RunRequest, SlashCommand, SteeringMode,
};

use crate::process::{Child, Command, Stdio};
use crate::{Harness, HarnessError, RunControls, StderrTail};

use self::client::PrimeClient;

const REASONING: &[ReasoningLevel] = &[
    ReasoningLevel::Minimal,
    ReasoningLevel::Low,
    ReasoningLevel::Medium,
    ReasoningLevel::High,
    ReasoningLevel::XHigh,
    ReasoningLevel::Max,
];
const PROBE_TIMEOUT: Duration = Duration::from_secs(30);
const KILL_GRACE: Duration = Duration::from_secs(3);

fn reasoning_name(level: ReasoningLevel) -> &'static str {
    match level {
        ReasoningLevel::Minimal => "minimal",
        ReasoningLevel::Low => "low",
        ReasoningLevel::Medium => "medium",
        ReasoningLevel::High => "high",
        ReasoningLevel::XHigh => "xhigh",
        ReasoningLevel::Max => "max",
        _ => "off",
    }
}

pub struct PrimeHarness {
    executable: Option<PathBuf>,
}

impl Default for PrimeHarness {
    fn default() -> Self {
        Self::new()
    }
}

impl PrimeHarness {
    pub fn new() -> Self {
        Self { executable: None }
    }

    /// An explicit local CLI is useful for a controlled protocol peer in tests.
    pub fn with_executable(mut self, executable: impl Into<PathBuf>) -> Self {
        self.executable = Some(executable.into());
        self
    }

    fn resolve_executable(&self) -> Result<PathBuf, HarnessError> {
        if let Some(path) = self
            .executable
            .as_ref()
            .cloned()
            .or_else(|| std::env::var_os("PRIME_AGENT_EXECUTABLE").map(PathBuf::from))
        {
            return crate::executable::validate_native_override(&path);
        }
        crate::executable::find_on_paths("prime-agent", Vec::new()).ok_or_else(|| {
            HarnessError::NotInstalled(
                "prime-agent (install Prime Agent or set PRIME_AGENT_EXECUTABLE)".into(),
            )
        })
    }

    fn spawn(
        &self,
        cwd: &Path,
        resume: Option<&str>,
        no_session: bool,
    ) -> Result<
        (
            Child,
            PrimeClient,
            mpsc::UnboundedReceiver<Value>,
            StderrTail,
        ),
        HarnessError,
    > {
        let executable = self.resolve_executable()?;
        let mut cmd = Command::new(&executable);
        cmd.arg("--mode").arg("rpc");
        if no_session {
            cmd.arg("--no-session");
        }
        if let Some(path) = resume {
            cmd.arg("--resume").arg(path);
        }
        cmd.current_dir(cwd);
        crate::compose_child_path(&mut cmd, &executable);
        cmd.stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);
        #[cfg(unix)]
        {
            use std::os::unix::process::CommandExt;
            cmd.as_std_mut().process_group(0);
        }
        let mut child = cmd.spawn().map_err(|error| {
            if error.kind() == std::io::ErrorKind::NotFound {
                HarnessError::NotInstalled(crate::executable::binary_hint(&executable))
            } else {
                HarnessError::Io(error)
            }
        })?;
        let stdin = child
            .stdin
            .take()
            .ok_or_else(|| HarnessError::Protocol("Prime RPC has no stdin".into()))?;
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| HarnessError::Protocol("Prime RPC has no stdout".into()))?;
        let stderr_tail = StderrTail::default();
        if let Some(stderr) = child.stderr.take() {
            let tail = stderr_tail.clone();
            tokio::spawn(async move {
                let mut lines = BufReader::new(stderr).lines();
                while let Ok(Some(line)) = lines.next_line().await {
                    tail.push(&line);
                }
            });
        }
        let (client, incoming) = PrimeClient::new(stdin, stdout);
        Ok((child, client, incoming, stderr_tail))
    }

    async fn probe(&self, cwd: &Path, command: &str) -> Result<Value, HarnessError> {
        // No session file is needed for discovery. Prime still loads the real
        // global/project settings, packages, skills, and extensions.
        let (mut child, client, _incoming, stderr) = self.spawn(cwd, None, true)?;
        let result = tokio::time::timeout(PROBE_TIMEOUT, client.request(command, json!({}))).await;
        crate::shutdown_child(&mut child, KILL_GRACE).await;
        match result {
            Ok(Ok(data)) => Ok(data),
            Ok(Err(error)) => Err(error),
            Err(_) => Err(HarnessError::Protocol(format!(
                "Prime RPC {command} timed out; {}",
                stderr.snapshot().unwrap_or_default()
            ))),
        }
    }
}

fn model_id(model: &Value) -> Option<String> {
    let provider = model.get("provider")?.as_str()?;
    let id = model.get("id")?.as_str()?;
    Some(format!("{provider}/{id}"))
}

fn parse_models(data: &Value) -> Result<Vec<Model>, HarnessError> {
    let entries = data
        .get("models")
        .and_then(Value::as_array)
        .ok_or_else(|| HarnessError::Protocol("Prime RPC returned no model list".into()))?;
    Ok(entries
        .iter()
        .filter_map(|entry| {
            let id = model_id(entry)?;
            let provider = entry.get("provider")?.as_str()?;
            let name = entry
                .get("name")
                .and_then(Value::as_str)
                .unwrap_or_else(|| entry.get("id").and_then(Value::as_str).unwrap_or("model"));
            Some(Model {
                id,
                label: format!("{name} ({provider})"),
                description: None,
                reasoning_levels: if entry.get("reasoning").and_then(Value::as_bool) == Some(true) {
                    REASONING
                        .iter()
                        .copied()
                        .filter(|level| {
                            entry.pointer(&format!("/thinkingLevelMap/{}", reasoning_name(*level)))
                                != Some(&Value::Null)
                        })
                        .collect()
                } else {
                    Vec::new()
                },
                options: Vec::new(),
            })
        })
        .collect())
}

fn commands_in(data: &Value) -> Result<&[Value], HarnessError> {
    data.get("commands")
        .and_then(Value::as_array)
        .map(Vec::as_slice)
        .ok_or_else(|| HarnessError::Protocol("Prime RPC returned no command list".into()))
}

fn parse_commands(data: &Value) -> Result<Vec<SlashCommand>, HarnessError> {
    let mut commands: Vec<SlashCommand> = commands_in(data)?
        .iter()
        .filter_map(|item| {
            let name = item.get("name")?.as_str()?;
            valid_skill_command_name(name).then(|| SlashCommand {
                name: name.to_owned(),
                description: item
                    .get("description")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_owned(),
                input_hint: None,
            })
        })
        .collect();
    // Prime's RPC catalog omits built-in commands that its prompt endpoint
    // executes in the session. Terminal-only commands are not RPC commands.
    for (name, description, hint) in [
        (
            "compact",
            "Compact the session context",
            "optional instructions",
        ),
        (
            "refine",
            "Refine Prime's continual harness",
            "optional instructions",
        ),
        (
            "goal",
            "Set or view a persistent goal; pause, resume, or clear it",
            "objective or status/pause/resume/clear",
        ),
        (
            "autonomous",
            "Set or view autonomous mode",
            "status or on/off with options",
        ),
    ] {
        if !commands.iter().any(|command| command.name == name) {
            commands.push(SlashCommand {
                name: name.into(),
                description: description.into(),
                input_hint: Some(hint.into()),
            });
        }
    }
    Ok(commands)
}

fn parse_skills(data: &Value) -> Result<Vec<Skill>, HarnessError> {
    Ok(commands_in(data)?
        .iter()
        .filter_map(|item| {
            if item.get("source").and_then(Value::as_str) != Some("skill") {
                return None;
            }
            let command = item.get("name")?.as_str()?;
            let name = command.strip_prefix("skill:")?;
            if !valid_invocation_name(name) || !valid_skill_command_name(command) {
                return None;
            }
            let path = item
                .pointer("/sourceInfo/path")
                .and_then(Value::as_str)
                .filter(|value| !value.is_empty())
                .map(str::to_owned)
                .unwrap_or_else(|| format!("harness-skill:prime:{name}"));
            Some(Skill {
                name: name.to_owned(),
                path,
                description: item
                    .get("description")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_owned(),
                enabled: true,
                command: Some(SkillCommand {
                    name: command.to_owned(),
                    harness: HarnessId::Prime,
                }),
            })
        })
        .collect())
}

#[async_trait]
impl Harness for PrimeHarness {
    fn id(&self) -> HarnessId {
        HarnessId::Prime
    }
    fn display_name(&self) -> &str {
        "Prime Agent"
    }
    fn supports_steering(&self) -> bool {
        true
    }
    fn steering_mode(&self) -> SteeringMode {
        SteeringMode::StepBoundary
    }
    fn reasoning_levels(&self) -> &[ReasoningLevel] {
        REASONING
    }
    fn installed(&self) -> bool {
        self.resolve_executable().is_ok()
    }
    fn authoritative_prompt_end(&self) -> bool {
        true
    }

    fn fallback_models(&self) -> Vec<Model> {
        vec![Model {
            id: "default".into(),
            label: "Prime default".into(),
            description: Some("Use the model configured in Prime Agent".into()),
            reasoning_levels: Vec::new(),
            options: Vec::new(),
        }]
    }

    async fn models(&self) -> Result<Vec<Model>, HarnessError> {
        let data = self
            .probe(
                &crate::executable::home_or_current_dir(),
                "get_available_models",
            )
            .await?;
        let mut models = self.fallback_models();
        models.extend(parse_models(&data)?);
        Ok(models)
    }

    async fn commands(&self) -> Result<Vec<SlashCommand>, HarnessError> {
        let data = self
            .probe(&crate::executable::home_or_current_dir(), "get_commands")
            .await?;
        parse_commands(&data)
    }

    async fn commands_for(&self, cwd: &Path) -> Result<Vec<SlashCommand>, HarnessError> {
        let data = self.probe(cwd, "get_commands").await?;
        parse_commands(&data)
    }

    async fn skills(&self, cwd: &Path) -> Result<Option<Vec<Skill>>, HarnessError> {
        let data = self.probe(cwd, "get_commands").await?;
        parse_skills(&data).map(Some)
    }

    async fn run(
        &self,
        request: RunRequest,
        controls: RunControls,
    ) -> Result<BoxStream<'static, Result<AgentEvent, HarnessError>>, HarnessError> {
        let (child, client, incoming, stderr) =
            self.spawn(Path::new(&request.cwd), request.resume.as_deref(), false)?;
        let (tx, rx) = mpsc::channel(256);
        tokio::spawn(session::run_session(
            child, client, incoming, stderr, request, controls, tx,
        ));
        Ok(futures::stream::unfold(rx, |mut rx| async move {
            rx.recv().await.map(|event| (event, rx))
        })
        .boxed())
    }
}
