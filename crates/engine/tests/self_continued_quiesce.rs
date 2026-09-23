//! The SHORT quiesce window for self-continued turns (2026-08-13 incident:
//! "stuck in working after you finished watching the build").
//!
//! A turn the agent starts on its own — a background-task wake — can never
//! receive a harness Done: the adapter has no `session/prompt` outstanding to
//! settle, so the quiesce watchdog is that turn shape's ONLY settle path.
//! With the shared 120s window every background notification ended in ~2min
//! of phantom Working. Self-continued turns now use a much shorter window
//! (`ZERON_SELF_TURN_QUIESCE_MS`); prompt/steer turns keep the normal one.
//!
//! This file exists separately from `turn_quiesce.rs` because the env knobs
//! are process-global: here the NORMAL window is set far beyond the test
//! horizon, so a fast park can only have come through the short path.

use std::sync::{Arc, Once};
use std::time::Duration;

use async_trait::async_trait;
use futures::StreamExt;
use futures::stream::BoxStream;
use serde_json::json;
use tokio::sync::{Mutex, mpsc};

use zeron_doc::{MessagePart, MessageRole, MessageStatus};
use zeron_engine::{EngineCore, HarnessRegistry};
use zeron_harness::{Harness, HarnessError, RunControls};
use zeron_proto::{
    AgentEvent, DoneStatus, HarnessId, Model, ReasoningLevel, RunRequest, SandboxLevel,
    SessionStatus, SteeringMode,
};

const CHAT: &str = "chat-self-quiesce";
/// Normal window: far beyond the test horizon — any park inside the test
/// window must have come through the self-continued path.
const QUIESCE_MS: u64 = 600_000;
/// Short window under test.
const SELF_QUIESCE_MS: u64 = 400;

fn init_env() {
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        // SAFETY: called before any engine (and thus any reader of the vars)
        // exists in this test process.
        unsafe {
            std::env::set_var("ZERON_TURN_QUIESCE_MS", QUIESCE_MS.to_string());
            std::env::set_var("ZERON_SELF_TURN_QUIESCE_MS", SELF_QUIESCE_MS.to_string());
        }
    });
}

fn run_request(prompt: &str) -> RunRequest {
    RunRequest {
        prompt: prompt.into(),
        harness: None,
        model: None,
        reasoning: None,
        model_options: Default::default(),
        cwd: "/tmp".into(),
        sandbox: SandboxLevel::WorkspaceWrite,
        auto_approve: true,
        attachments: Vec::new(),
        worktree: None,
        resume: None,
    }
}

fn done(status: DoneStatus) -> AgentEvent {
    AgentEvent::Done {
        status,
        result: None,
        error: None,
        session_id: Some("hs-sq".into()),
    }
}

fn session_started(harness: HarnessId) -> AgentEvent {
    AgentEvent::SessionStarted {
        harness,
        model: "mock-1".into(),
        tools: vec![],
        cwd: "/tmp".into(),
        session_id: "hs-sq".into(),
        assistant_message_id: "a-sq".into(),
    }
}

fn text(t: &str) -> AgentEvent {
    AgentEvent::TextDelta { text: t.into() }
}

/// Feed-by-hand harness (see `turn_quiesce.rs`): the test pushes events
/// through a channel; accepted steers confirm with a `Steered` boundary.
struct FeedHarness {
    id: HarnessId,
    main_prompt: String,
    feed: Mutex<Option<mpsc::UnboundedReceiver<AgentEvent>>>,
}

#[async_trait]
impl Harness for FeedHarness {
    fn id(&self) -> HarnessId {
        self.id
    }
    fn display_name(&self) -> &str {
        "Feed"
    }
    fn supports_steering(&self) -> bool {
        true
    }
    fn steering_mode(&self) -> SteeringMode {
        SteeringMode::StepBoundary
    }
    fn deterministic_turn_end(&self) -> bool {
        self.id == HarnessId::Prime
    }
    fn reasoning_levels(&self) -> &[ReasoningLevel] {
        &[ReasoningLevel::Medium]
    }
    async fn models(&self) -> Result<Vec<Model>, HarnessError> {
        Ok(vec![])
    }
    async fn run(
        &self,
        request: RunRequest,
        mut controls: RunControls,
    ) -> Result<BoxStream<'static, Result<AgentEvent, HarnessError>>, HarnessError> {
        if request.prompt != self.main_prompt {
            let events = vec![Ok(done(DoneStatus::Completed))];
            return Ok(futures::stream::iter(events).boxed());
        }
        let mut feed = self
            .feed
            .lock()
            .await
            .take()
            .expect("FeedHarness serves the main dispatch once per test");
        let (tx, rx) = mpsc::channel::<Result<AgentEvent, HarnessError>>(64);
        tokio::spawn(async move {
            let mut steering_open = true;
            loop {
                tokio::select! {
                    biased;
                    steer = controls.steering.recv(), if steering_open => match steer {
                        Some(_) => {
                            let boundary = AgentEvent::Steered {
                                assistant_message_id: None,
                                next_assistant_message_id: None,
                            };
                            if tx.send(Ok(boundary)).await.is_err() {
                                return;
                            }
                        }
                        None => steering_open = false,
                    },
                    event = feed.recv() => match event {
                        Some(event) => {
                            if tx.send(Ok(event)).await.is_err() {
                                return;
                            }
                        }
                        None => return,
                    },
                }
            }
        });
        Ok(futures::stream::unfold(rx, |mut rx| async move {
            rx.recv().await.map(|event| (event, rx))
        })
        .boxed())
    }
}

struct Rig {
    core: EngineCore,
    feed: mpsc::UnboundedSender<AgentEvent>,
    _dir: tempfile::TempDir,
}

fn assemble(main_prompt: &str, harness: HarnessId) -> Rig {
    init_env();
    let (feed, rx) = mpsc::unbounded_channel();
    let registry = HarnessRegistry::new();
    registry.register(Arc::new(FeedHarness {
        id: harness,
        main_prompt: main_prompt.into(),
        feed: Mutex::new(Some(rx)),
    }));
    let dir = tempfile::tempdir().unwrap();
    let core = EngineCore::assemble(dir.path(), Arc::new(registry), harness, None)
        .expect("engine core assembles");
    Rig {
        core,
        feed,
        _dir: dir,
    }
}

fn status(core: &EngineCore) -> Option<SessionStatus> {
    core.sessions.session_status(CHAT).map(|s| s.status)
}

async fn wait_for<F>(mut predicate: F, what: &str)
where
    F: FnMut() -> bool,
{
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while !predicate() {
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for {what}"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

#[tokio::test]
async fn self_continued_turn_parks_on_the_short_window() {
    let rig = assemble("watch the build", HarnessId::Mock);
    rig.core
        .sessions
        .dispatch(CHAT, HarnessId::Mock, run_request("watch the build"), None)
        .await
        .expect("dispatch");

    // Turn 1 completes normally → parked Idle.
    rig.feed.send(session_started(HarnessId::Mock)).unwrap();
    rig.feed.send(text("I will watch the build.")).unwrap();
    rig.feed.send(done(DoneStatus::Completed)).unwrap();
    wait_for(
        || status(&rig.core) == Some(SessionStatus::Idle),
        "park after Done",
    )
    .await;

    // Background wake: self-continued output past the resume gate.
    tokio::time::sleep(Duration::from_millis(1200)).await;
    rig.feed
        .send(text("The build is green. Released."))
        .unwrap();
    wait_for(
        || status(&rig.core) == Some(SessionStatus::Working),
        "self-continued output resumes Working",
    )
    .await;

    // The short window parks it well inside the 10s wait_for horizon — the
    // normal window (10 min here) could not have.
    wait_for(
        || status(&rig.core) == Some(SessionStatus::Idle),
        "short quiesce parks the self-continued turn",
    )
    .await;

    rig.core.sessions.shutdown().await;
}

#[tokio::test]
async fn steered_turn_keeps_the_normal_window() {
    let rig = assemble("watch the build again", HarnessId::Mock);
    rig.core
        .sessions
        .dispatch(
            CHAT,
            HarnessId::Mock,
            run_request("watch the build again"),
            None,
        )
        .await
        .expect("dispatch");

    rig.feed.send(session_started(HarnessId::Mock)).unwrap();
    rig.feed.send(text("Watching.")).unwrap();
    rig.feed.send(done(DoneStatus::Completed)).unwrap();
    wait_for(
        || status(&rig.core) == Some(SessionStatus::Idle),
        "park after Done",
    )
    .await;

    // Background wake resumes the session (short window armed)…
    tokio::time::sleep(Duration::from_millis(1200)).await;
    rig.feed.send(text("Build done.")).unwrap();
    wait_for(
        || status(&rig.core) == Some(SessionStatus::Working),
        "self-continued output resumes Working",
    )
    .await;

    // …then a real steer takes the turn over: the short window must stand
    // down. The steered turn's reply streams and goes quiet — with the
    // normal window at 10 minutes, the session must STAY Working well past
    // the short window (its Done is genuinely coming).
    rig.core
        .sessions
        .steer(CHAT, "and then?", None)
        .await
        .expect("steer accepted");
    wait_for(
        || status(&rig.core) == Some(SessionStatus::Working),
        "steered turn is Working",
    )
    .await;
    rig.feed.send(text("Answering the steer.")).unwrap();
    tokio::time::sleep(Duration::from_millis(1500)).await;
    assert_eq!(
        status(&rig.core),
        Some(SessionStatus::Working),
        "a steered (prompt-owned) turn must not park on the short window"
    );

    // Clean turn end.
    rig.feed.send(done(DoneStatus::Completed)).unwrap();
    wait_for(
        || status(&rig.core) == Some(SessionStatus::Idle),
        "steered turn parks at its Done",
    )
    .await;

    rig.core.sessions.shutdown().await;
}

#[tokio::test]
async fn prime_interim_end_stays_in_turn_and_native_wake_reopens() {
    let rig = assemble("start a child", HarnessId::Prime);
    rig.core
        .sessions
        .dispatch(CHAT, HarnessId::Prime, run_request("start a child"), None)
        .await
        .expect("dispatch");
    rig.feed.send(session_started(HarnessId::Prime)).unwrap();
    rig.feed.send(text("The child is ")).unwrap();
    rig.feed
        .send(AgentEvent::PrimeEvent {
            event: json!({"type":"agent_end"}),
        })
        .unwrap();
    rig.feed
        .send(AgentEvent::PrimeEvent {
            event: json!({"type":"auto_retry_start"}),
        })
        .unwrap();
    rig.feed
        .send(AgentEvent::PrimeEvent {
            event: json!({"type":"message_end","message":{"role":"custom","customType":"refinement_outcome"}}),
        })
        .unwrap();
    rig.feed.send(text("still running.")).unwrap();
    rig.feed.send(done(DoneStatus::Completed)).unwrap();
    wait_for(
        || status(&rig.core) == Some(SessionStatus::Idle),
        "Prime foreground turn to park",
    )
    .await;
    let first_reply: Vec<_> = rig
        .core
        .doc_host
        .open(CHAT)
        .unwrap()
        .doc()
        .read_entries()
        .unwrap()
        .into_iter()
        .filter(|entry| entry.role == MessageRole::Assistant)
        .collect();
    assert_eq!(first_reply.len(), 1);
    assert!(first_reply[0].parts.iter().any(|part| {
        matches!(part, MessagePart::Text { text, .. } if text == "The child is still running.")
    }));

    rig.feed
        .send(AgentEvent::PrimeEvent {
            event: json!({"type":"agent_start"}),
        })
        .unwrap();
    wait_for(
        || status(&rig.core) == Some(SessionStatus::Working),
        "Prime native wake to reopen Working",
    )
    .await;
    rig.feed.send(text("The child finished.")).unwrap();
    rig.feed.send(done(DoneStatus::Completed)).unwrap();
    rig.feed
        .send(AgentEvent::PrimeEvent {
            event: json!({"type":"lifecycle_update","phase":"quiescent"}),
        })
        .unwrap();
    wait_for(
        || status(&rig.core) == Some(SessionStatus::Idle),
        "Prime resumed turn to park",
    )
    .await;
    let replies: Vec<_> = rig
        .core
        .doc_host
        .open(CHAT)
        .unwrap()
        .doc()
        .read_entries()
        .unwrap()
        .into_iter()
        .filter(|entry| entry.role == MessageRole::Assistant)
        .collect();
    assert_eq!(replies.len(), 2);
    assert_eq!(replies[1].status, Some(MessageStatus::Complete));
    assert!(replies[1].parts.iter().any(|part| {
        matches!(part, MessagePart::Text { text, .. } if text == "The child finished.")
    }));
    rig.core.sessions.shutdown().await;
}
