#![cfg(unix)]

use std::os::unix::fs::PermissionsExt;
use std::time::Duration;

use futures::StreamExt;
use tokio::sync::{mpsc, oneshot};
use zeron_harness::{CancellationToken, Harness, PrimeHarness, RunControls, SteerMessage};
use zeron_proto::{
    AgentEvent, DoneStatus, ReasoningLevel, RunRequest, SandboxLevel, ToolCall, UserInputAnswer,
};

#[tokio::test]
async fn session_command_finishes_without_an_agent_turn() {
    let dir = tempfile::tempdir().unwrap();
    let executable = dir.path().join("prime-agent");
    std::fs::write(&executable, include_str!("fixtures/prime-rpc.py")).unwrap();
    std::fs::set_permissions(&executable, std::fs::Permissions::from_mode(0o755)).unwrap();
    let harness = PrimeHarness::new().with_executable(&executable);
    let (steer, steering) = mpsc::channel(1);
    drop(steer);
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
        prompt: "/goal status".into(),
        harness: Some(zeron_proto::HarnessId::Prime),
        model: Some("default".into()),
        reasoning: None,
        model_options: Default::default(),
        cwd: dir.path().display().to_string(),
        sandbox: SandboxLevel::WorkspaceWrite,
        auto_approve: true,
        resume: None,
        attachments: Vec::new(),
        worktree: None,
    };
    let mut stream = harness.run(request, controls).await.unwrap();
    let events = tokio::time::timeout(Duration::from_secs(5), async {
        let mut events = Vec::new();
        while let Some(event) = stream.next().await {
            let event = event.unwrap();
            let done = matches!(event, AgentEvent::Done { .. });
            events.push(event);
            if done {
                break;
            }
        }
        events
    })
    .await
    .expect("Prime command did not finish");
    assert!(events.iter().any(|event| matches!(event, AgentEvent::TextDelta { text } if text == "Goal active: Finish work")));
    assert!(matches!(
        events.last(),
        Some(AgentEvent::Done {
            status: DoneStatus::Completed,
            ..
        })
    ));
}

#[tokio::test]
async fn local_catalog_and_persistent_child_stream_follow_prime_rpc() {
    let dir = tempfile::tempdir().unwrap();
    let executable = dir.path().join("prime-agent");
    std::fs::write(&executable, include_str!("fixtures/prime-rpc.py")).unwrap();
    std::fs::set_permissions(&executable, std::fs::Permissions::from_mode(0o755)).unwrap();
    let harness = PrimeHarness::new().with_executable(&executable);

    let models = harness.models().await.unwrap();
    assert_eq!(models.len(), 3);
    assert_eq!(models[0].id, "default");
    assert!(models[0].reasoning_levels.is_empty());
    assert_eq!(models[1].id, "local/configured");
    assert!(
        !models[1]
            .reasoning_levels
            .contains(&ReasoningLevel::Minimal)
    );
    assert!(!models[1].reasoning_levels.contains(&ReasoningLevel::Max));
    assert_eq!(models[2].id, "other/second");
    let skills = harness.skills(dir.path()).await.unwrap().unwrap();
    assert_eq!(skills.len(), 1);
    assert_eq!(skills[0].name, "research");
    let commands = harness.commands_for(dir.path()).await.unwrap();
    for name in ["compact", "refine", "goal", "autonomous"] {
        assert!(commands.iter().any(|command| command.name == name));
    }
    assert_eq!(commands.len(), 6);

    let session_file = dir.path().join("session.jsonl").display().to_string();
    for (resume, model, expected_model) in [
        (None, "default", "local/configured"),
        (Some(session_file.clone()), "other/second", "other/second"),
    ] {
        let expected_initial = resume.as_ref().map(|_| 12_000);
        let (_steer_tx, steering) = mpsc::channel(1);
        let controls = RunControls {
            steering,
            interrupt: CancellationToken::new(),
            request_input: Box::new(|questions| {
                let (tx, rx) = oneshot::channel();
                let _ = tx.send(vec![UserInputAnswer {
                    question_id: questions[0].id.clone(),
                    labels: vec!["Yes".into()],
                }]);
                rx
            }),
        };
        let request = RunRequest {
            prompt: "parent task".into(),
            harness: None,
            model: Some(model.into()),
            reasoning: None,
            model_options: Default::default(),
            cwd: dir.path().display().to_string(),
            sandbox: SandboxLevel::WorkspaceWrite,
            auto_approve: true,
            resume,
            attachments: Vec::new(),
            worktree: None,
        };
        let mut stream = harness.run(request, controls).await.unwrap();
        let events = tokio::time::timeout(Duration::from_secs(10), async {
            let mut events = Vec::new();
            while let Some(event) = stream.next().await {
                let event = event.unwrap();
                let parent_done = matches!(event, AgentEvent::Done { .. });
                events.push(event);
                if parent_done {
                    break;
                }
            }
            events
        })
        .await
        .unwrap();
        assert!(events.iter().any(|event| matches!(event,
            AgentEvent::SessionStarted { session_id, model, .. }
                if session_id == &session_file && model == expected_model)));
        let context: Vec<_> = events
            .iter()
            .filter_map(|event| match event {
                AgentEvent::ContextUsageSnapshot { tokens, window } => Some((*tokens, *window)),
                _ => None,
            })
            .collect();
        assert_eq!(
            context,
            vec![
                (expected_initial, Some(200_000)),
                (None, Some(200_000)),
                (Some(42_000), Some(200_000)),
            ]
        );
        assert!(events.iter().any(|event| matches!(event,
            AgentEvent::ToolCall { call: ToolCall::Unknown { name, .. }, .. }
                if name == "Agent: child task")));
        assert!(events.iter().any(|event| matches!(event,
            AgentEvent::Subagent { event, .. }
                if matches!(event.as_ref(), AgentEvent::TextDelta { text } if text == "child live"))));
        assert!(events.iter().any(|event| matches!(event,
            AgentEvent::Subagent { event, .. }
                if matches!(event.as_ref(), AgentEvent::ToolCall { call: ToolCall::Exec { command }, .. } if command == "print(1)"))));
        assert!(events.iter().any(|event| matches!(event,
            AgentEvent::Subagent { event, .. }
                if matches!(event.as_ref(), AgentEvent::Done { status: DoneStatus::Completed, .. }))));
        let native: Vec<_> = events
            .iter()
            .filter_map(|event| match event {
                AgentEvent::PrimeEvent { event } => Some(event),
                _ => None,
            })
            .collect();
        for kind in [
            "extension_ui_request",
            "agent_start",
            "turn_start",
            "session_action_update",
            "compaction_start",
            "compaction_end",
            "auto_retry_start",
            "auto_retry_end",
            "future_prime_event",
            "message_update",
            "tool_execution_update",
            "rlm_child_update",
            "observed_session_event",
            "turn_end",
            "agent_end",
        ] {
            assert!(
                native.iter().any(|event| event["type"] == kind),
                "missing native {kind}"
            );
        }
        assert!(native.iter().any(|event| event["type"] == "future_prime_event"
            && event["extra"]["value"] == 42));
        assert!(
            native
                .iter()
                .any(|event| event["type"] == "observed_session_event"
                    && event["event"]["type"] == "tool_execution_update"
                    && event["event"]["partialResult"]["content"][0]["text"] == "working")
        );
        assert!(matches!(
            events.last(),
            Some(AgentEvent::Done {
                status: DoneStatus::Completed,
                ..
            })
        ));
        drop(stream);
    }
    let args = std::fs::read_to_string(dir.path().join("prime-args.log")).unwrap();
    assert!(
        args.lines()
            .any(|line| line.contains("--resume") && line.contains(&session_file))
    );
}

#[tokio::test]
async fn native_steer_keeps_a_running_prime_child_alive() {
    let dir = tempfile::tempdir().unwrap();
    let executable = dir.path().join("prime-agent");
    std::fs::write(&executable, include_str!("fixtures/prime-rpc.py")).unwrap();
    std::fs::set_permissions(&executable, std::fs::Permissions::from_mode(0o755)).unwrap();
    let harness = PrimeHarness::new().with_executable(&executable);
    let (steer_tx, steering) = mpsc::channel(1);
    let controls = RunControls {
        steering,
        interrupt: CancellationToken::new(),
        request_input: Box::new(|questions| {
            let (tx, rx) = oneshot::channel();
            let _ = tx.send(vec![UserInputAnswer {
                question_id: questions[0].id.clone(),
                labels: vec!["Yes".into()],
            }]);
            rx
        }),
    };
    let request = RunRequest {
        prompt: "parent steer scenario".into(),
        harness: Some(zeron_proto::HarnessId::Prime),
        model: Some("default".into()),
        reasoning: None,
        model_options: Default::default(),
        cwd: dir.path().display().to_string(),
        sandbox: SandboxLevel::WorkspaceWrite,
        auto_approve: true,
        resume: None,
        attachments: Vec::new(),
        worktree: None,
    };
    let mut stream = harness.run(request, controls).await.unwrap();
    let events = tokio::time::timeout(Duration::from_secs(10), async {
        let mut events = Vec::new();
        while let Some(event) = stream.next().await {
            let event = event.unwrap();
            let child_started = matches!(&event, AgentEvent::Subagent { event, .. }
                if matches!(event.as_ref(), AgentEvent::TextDelta { text } if text == "child live"));
            let done = matches!(event, AgentEvent::Done { .. });
            events.push(event);
            if child_started {
                steer_tx
                    .send(SteerMessage {
                        prompt: "Redirect while child runs".into(),
                        message_id: Some("steered-user".into()),
                    })
                    .await
                    .unwrap();
            }
            if done {
                break;
            }
        }
        events
    })
    .await
    .expect("Prime child and parent finish after steer");
    assert!(
        events
            .iter()
            .any(|event| matches!(event, AgentEvent::Subagent { event, .. }
        if matches!(event.as_ref(), AgentEvent::TextDelta { text } if text == " and finished")))
    );
    assert!(
        events
            .iter()
            .any(|event| matches!(event, AgentEvent::Subagent { event, .. }
        if matches!(event.as_ref(), AgentEvent::Done { status: DoneStatus::Completed, .. })))
    );
    assert!(matches!(
        events.last(),
        Some(AgentEvent::Done {
            status: DoneStatus::Completed,
            ..
        })
    ));
    assert!(!events.iter().any(|event| matches!(
        event,
        AgentEvent::Done {
            status: DoneStatus::Interrupted,
            ..
        }
    )));
}
