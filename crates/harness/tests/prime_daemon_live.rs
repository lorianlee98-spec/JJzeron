//! Opt-in smoke test against the locally installed Prime daemon and config.

use std::time::Duration;

use futures::StreamExt;
use tokio::sync::{mpsc, oneshot};
use zeron_harness::{CancellationToken, Harness, PrimeHarness, RunControls, SteerMessage};
use zeron_proto::{AgentEvent, DoneStatus, ReasoningLevel, RunRequest, SandboxLevel};

#[tokio::test]
#[ignore = "requires a local Prime Agent installation and daemon"]
async fn prime_native_completion_and_quiescence() {
    let cwd = tempfile::tempdir().unwrap();
    let harness = PrimeHarness::new();
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
        prompt: "Reply with exactly one word: ready".into(),
        harness: Some(zeron_proto::HarnessId::Prime),
        model: Some("default".into()),
        reasoning: None,
        model_options: Default::default(),
        cwd: cwd.path().display().to_string(),
        sandbox: SandboxLevel::WorkspaceWrite,
        auto_approve: true,
        resume: None,
        attachments: Vec::new(),
        worktree: None,
    };
    let mut stream = harness.run(request, controls).await.unwrap();
    let (session_file, done, quiescent, saw_agent_end, saw_text) =
        tokio::time::timeout(Duration::from_secs(180), async {
            let mut session_file = None;
            let mut done = None;
            let mut quiescent = false;
            let mut saw_agent_end = false;
            let mut saw_text = false;
            while let Some(event) = stream.next().await {
                match event.unwrap() {
                    AgentEvent::SessionStarted { session_id, .. } => {
                        session_file = Some(session_id)
                    }
                    AgentEvent::TextDelta { text } if !text.is_empty() => saw_text = true,
                    AgentEvent::Done { status, .. } => done = Some(status),
                    AgentEvent::PrimeEvent { event } if event["type"] == "agent_end" => {
                        saw_agent_end = true
                    }
                    AgentEvent::PrimeEvent { event }
                        if event["type"] == "lifecycle_update" && event["phase"] == "quiescent" =>
                    {
                        quiescent = true;
                    }
                    _ => {}
                }
                if done.is_some() && quiescent {
                    break;
                }
            }
            (session_file, done, quiescent, saw_agent_end, saw_text)
        })
        .await
        .expect("Prime did not reach native completion");
    assert!(matches!(done, Some(DoneStatus::Completed)));
    assert!(quiescent);
    assert!(saw_agent_end);
    assert!(saw_text);
    if let Some(path) = session_file {
        let id = std::path::Path::new(&path).file_stem().unwrap();
        let _ = std::process::Command::new("prime-agent")
            .arg("stop")
            .arg(id)
            .status();
    }
}

#[tokio::test]
#[ignore = "replays a local model/refine provider diagnostic"]
async fn prime_refine_retry_stays_in_one_jjzeron_run() {
    let cwd =
        std::env::var("JJ_PRIME_E2E_CWD").expect("set JJ_PRIME_E2E_CWD to the diagnostic repo");
    let harness = PrimeHarness::new();
    let (steer, steering) = mpsc::channel(2);
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
        prompt: "Read-only diagnostic in this checkout. Use ipython for four short sequential calls: print the cwd; read ~/.prime/agent/settings.json and print its transport value; read ~/.prime/agent/models.json and print the configured openai-codex model IDs; compute the SHA-256 of models.json. Do not use bash, background processes, or subagents. Then give a concise answer.".into(),
        harness: Some(zeron_proto::HarnessId::Prime),
        model: Some("openai-codex/gpt-6-sol".into()),
        reasoning: Some(ReasoningLevel::XHigh),
        model_options: Default::default(),
        cwd,
        sandbox: SandboxLevel::WorkspaceWrite,
        auto_approve: true,
        resume: None,
        attachments: Vec::new(),
        worktree: None,
    };
    let mut stream = harness.run(request, controls).await.unwrap();
    let mut session_file = None;
    let mut completed = 0;
    let mut saw_error_end = false;
    let mut saw_retry = false;
    let mut saw_final_end = false;
    tokio::time::timeout(Duration::from_secs(600), async {
        while let Some(event) = stream.next().await {
            match event.unwrap() {
                AgentEvent::SessionStarted { session_id, .. } => session_file = Some(session_id),
                AgentEvent::PrimeEvent { event } if event["type"] == "agent_end" => {
                    let last = event["messages"].as_array().and_then(|messages| messages.iter().rev().find(|message| message["role"] == "assistant"));
                    if completed >= 2 && last.is_some_and(|message| message["stopReason"] == "error") {
                        saw_error_end = true;
                    } else if completed >= 2 {
                        saw_final_end = true;
                    }
                }
                AgentEvent::PrimeEvent { event } if event["type"] == "auto_retry_start" => saw_retry = true,
                AgentEvent::Done { status, .. } => {
                    assert!(matches!(status, DoneStatus::Completed), "Prime response ended before native recovery");
                    completed += 1;
                    if completed == 1 {
                        steer.send(SteerMessage {
                            prompt: "/refine Add one session-local prompt note for this diagnostic: when checking model routing, inspect the configured model entry, inherited provider API, and transport setting separately. Do not change global harness state.".into(),
                            message_id: Some("refine-user".into()),
                        }).await.unwrap();
                    } else if completed == 2 {
                        steer.send(SteerMessage {
                            prompt: "Use one short ipython call to recheck the transport value in ~/.prime/agent/settings.json, then state only that value. Do not use bash, background processes, or subagents.".into(),
                            message_id: Some("followup-user".into()),
                        }).await.unwrap();
                    } else {
                        break;
                    }
                }
                _ => {}
            }
        }
    }).await.expect("Prime refine diagnostic did not finish");
    assert_eq!(completed, 3);
    println!(
        "Prime retry diagnostic: error_end={saw_error_end} retry={saw_retry} final_end={saw_final_end}"
    );
    assert!(saw_final_end);
    if let Some(path) = session_file {
        let id = std::path::Path::new(&path).file_stem().unwrap();
        let _ = std::process::Command::new("prime-agent")
            .arg("stop")
            .arg(id)
            .status();
    }
}

#[tokio::test]
#[ignore = "runs a finite kernel background command against local Prime"]
async fn prime_kernel_background_work_outlives_the_foreground_reply() {
    let cwd = tempfile::tempdir().unwrap();
    let harness = PrimeHarness::new();
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
        prompt: "Use the ipython tool to run exactly this Python code:\nfrom rlm import bash\njj_handle = bash('sleep 15; printf jj-background-ready')\nprint(jj_handle.pid)\nDo not await the handle or call wait(). End your assistant turn immediately after reporting the PID. When Prime sends the asynchronous completion notice, acknowledge it.".into(),
        harness: Some(zeron_proto::HarnessId::Prime),
        model: Some("openai-codex/gpt-5.6-sol".into()),
        reasoning: None,
        model_options: Default::default(),
        cwd: cwd.path().display().to_string(),
        sandbox: SandboxLevel::WorkspaceWrite,
        auto_approve: true,
        resume: None,
        attachments: Vec::new(),
        worktree: None,
    };
    let mut stream = harness.run(request, controls).await.unwrap();
    let mut session_file = None;
    let mut done_at = None;
    let mut saw_background = false;
    let mut saw_quiescent = false;
    tokio::time::timeout(Duration::from_secs(180), async {
        while let Some(event) = stream.next().await {
            match event.unwrap() {
                AgentEvent::SessionStarted { session_id, .. } => session_file = Some(session_id),
                AgentEvent::Done { status, .. } => {
                    assert!(matches!(status, DoneStatus::Completed));
                    done_at.get_or_insert_with(std::time::Instant::now);
                }
                AgentEvent::PrimeEvent { event }
                    if event["type"] == "lifecycle_update" && event["phase"] == "background" =>
                {
                    saw_background = true;
                }
                AgentEvent::PrimeEvent { event }
                    if event["type"] == "lifecycle_update" && event["phase"] == "quiescent" =>
                {
                    saw_quiescent = true;
                    if done_at.is_some() {
                        break;
                    }
                }
                _ => {}
            }
        }
    })
    .await
    .expect("Prime background command did not settle");
    println!(
        "Prime kernel background: active_after_reply={saw_background} quiescent={saw_quiescent}"
    );
    assert!(done_at.is_some());
    assert!(saw_background);
    assert!(saw_quiescent);
    if let Some(path) = session_file {
        let id = std::path::Path::new(&path).file_stem().unwrap();
        let _ = std::process::Command::new("prime-agent")
            .arg("stop")
            .arg(id)
            .status();
    }
}

#[tokio::test]
#[ignore = "spawns a real Prime RLM child against the local model"]
async fn prime_rlm_child_continues_after_the_parent_reply() {
    let cwd = tempfile::tempdir().unwrap();
    let harness = PrimeHarness::new();
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
        prompt: "Use ipython exactly:\nimport rlm\njj_child = await rlm.spawn(\"Use ipython to await asyncio.sleep(12), then reply 'child ready'. Do not edit files.\", name=\"jj-lifecycle-check\")\nprint(jj_child.name)\nDo not await the child's result or call wait. End your assistant turn immediately. When Prime later sends the child completion notice, acknowledge it.".into(),
        harness: Some(zeron_proto::HarnessId::Prime),
        model: Some("openai-codex/gpt-5.6-sol".into()),
        reasoning: None,
        model_options: Default::default(),
        cwd: cwd.path().display().to_string(),
        sandbox: SandboxLevel::WorkspaceWrite,
        auto_approve: true,
        resume: None,
        attachments: Vec::new(),
        worktree: None,
    };
    let mut stream = harness.run(request, controls).await.unwrap();
    let mut session_file = None;
    let mut done_count = 0;
    let mut child_running = false;
    let mut child_done = false;
    let mut subagent_streamed = false;
    let mut subagent_done = false;
    let mut first_done_before_child = false;
    let mut quiescent = false;
    tokio::time::timeout(Duration::from_secs(240), async {
        while let Some(event) = stream.next().await {
            match event.unwrap() {
                AgentEvent::SessionStarted { session_id, .. } => session_file = Some(session_id),
                AgentEvent::PrimeEvent { event } if event["type"] == "rlm_child_update" => {
                    child_running |= event["child"]["status"] == "running";
                    child_done |= event["child"]["status"] == "done";
                }
                AgentEvent::Subagent { event, .. } => match event.as_ref() {
                    AgentEvent::TextDelta { text } if !text.is_empty() => subagent_streamed = true,
                    AgentEvent::Done {
                        status: DoneStatus::Completed,
                        ..
                    } => subagent_done = true,
                    _ => {}
                },
                AgentEvent::Done { status, .. } => {
                    assert!(matches!(status, DoneStatus::Completed));
                    done_count += 1;
                    if done_count == 1 {
                        first_done_before_child = !child_done;
                    }
                }
                AgentEvent::PrimeEvent { event }
                    if event["type"] == "lifecycle_update" && event["phase"] == "quiescent" =>
                {
                    quiescent = true;
                }
                _ => {}
            }
            if child_done && subagent_done && quiescent {
                break;
            }
        }
    })
    .await
    .expect("Prime child did not settle");
    println!(
        "Prime RLM child: running={child_running} done={child_done} streamed={subagent_streamed} subagent_done={subagent_done} parent_replied_first={first_done_before_child} parent_replies={done_count} quiescent={quiescent}"
    );
    assert!(child_running);
    assert!(child_done);
    assert!(subagent_streamed);
    assert!(subagent_done);
    assert!(first_done_before_child);
    assert!(quiescent);
    if let Some(path) = session_file {
        let id = std::path::Path::new(&path).file_stem().unwrap();
        let _ = std::process::Command::new("prime-agent")
            .arg("stop")
            .arg(id)
            .status();
    }
}
