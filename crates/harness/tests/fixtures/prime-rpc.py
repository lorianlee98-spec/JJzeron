#!/usr/bin/env python3
"""A small Prime RPC peer for the harness's public journey test."""

import json
import pathlib
import sys


root = pathlib.Path(__file__).parent
with (root / "prime-args.log").open("a") as log:
    log.write(json.dumps(sys.argv[1:]) + "\n")


def send(value):
    print(json.dumps(value), flush=True)


session_file = str(root / "session.jsonl")
if "--resume" in sys.argv:
    session_file = sys.argv[sys.argv.index("--resume") + 1]

pending_prompt_id = None
steer_scenario = False
for line in sys.stdin:
    request = json.loads(line)
    kind = request["type"]
    request_id = request.get("id")
    if kind == "prompt":
        if request["message"] == "scenario:agent-goal":
            send({"type": "response", "id": request_id, "command": "prompt", "success": True})
            send({"type": "agent_start"})
            send({"type": "goal_update", "goal": {"status": "active", "objective": "Agent-created goal"}})
            send({"type": "turn_end", "message": {"role": "assistant"}, "toolResults": []})
            send({"type": "agent_end"})
            continue
        if request["message"] == "/goal status":
            send({"type": "response", "id": request_id, "command": "prompt", "success": True})
            send({"type": "session_action_update", "actions": {"active": {"kind": "session_command", "label": "/goal status"}}})
            send({"type": "goal_update", "goal": {"status": "active", "objective": "Finish work"}})
            send({"type": "message_end", "message": {"role": "custom", "customType": "session_slash_command_result", "content": "Goal active: Finish work", "display": True, "details": {"command": {"text": "/goal status"}, "success": True}}})
            send({"type": "session_action_update", "actions": {"queuedCount": 0}})
            continue
        if request["message"].startswith("/goal "):
            with (root / "prime-goal-actions.log").open("a") as log:
                log.write(request["message"] + "\n")
            if request["message"] == "/goal resume":
                send({"type": "response", "id": request_id, "command": "prompt", "success": False,
                      "error": "goal cannot resume"})
                continue
            send({"type": "response", "id": request_id, "command": "prompt", "success": True})
            send({"type": "session_action_update", "actions": {"active": {"kind": "session_command", "label": request["message"]}}})
            goal = None if request["message"] == "/goal clear" else {"status": "paused", "objective": "Finish work"}
            send({"type": "goal_update", "goal": goal})
            send({"type": "session_action_update", "actions": {"queuedCount": 0}})
            continue
        if request.get("streamingBehavior") == "steer":
            assert steer_scenario and request["message"] == "Redirect while child runs"
            send({"type": "response", "id": request_id, "command": "prompt", "success": True})
            send({"type": "observed_session_event", "activeSessionId": "active-child-1",
                  "event": {"type": "message_update", "assistantMessageEvent": {"type": "text_delta", "delta": " and finished"}}})
            send({"type": "rlm_child_update", "child": {"id": "child-1", "label": "child task", "status": "done", "activeSessionId": "active-child-1"}})
            send({"type": "observed_session_event", "activeSessionId": "active-child-1", "event": {"type": "agent_end"}})
            send({"type": "turn_end", "message": {"role": "assistant"}, "toolResults": []})
            send({"type": "agent_end"})
            continue
        steer_scenario = request["message"] == "parent steer scenario"
        pending_prompt_id = request_id
        send({"type": "extension_ui_request", "id": "confirm-1", "method": "confirm",
              "title": "Extension", "message": "Continue?"})
        continue
    if kind == "extension_ui_response":
        assert request["id"] == "confirm-1" and request["confirmed"] is True
        send({"type": "response", "id": pending_prompt_id, "command": "prompt", "success": True})
        send({"type": "agent_start"})
        send({"type": "turn_start"})
        send({"type": "session_action_update", "actions": {"queuedCount": 1}})
        send({"type": "compaction_start", "reason": "threshold"})
        send({"type": "compaction_end", "reason": "threshold", "result": {"summary": "saved"}})
        send({"type": "auto_retry_start", "attempt": 1, "maxAttempts": 3})
        send({"type": "auto_retry_end", "attempt": 1, "success": True})
        send({"type": "extension_ui_request", "id": "status-1", "method": "setStatus", "statusKey": "work", "statusText": "Running"})
        send({"type": "future_prime_event", "extra": {"value": 42}})
        send({"type": "message_update", "assistantMessageEvent": {"type": "text_delta", "delta": "parent live"}})
        send({"type": "tool_execution_update", "toolCallId": "tool-1", "partialResult": {"content": [{"type": "text", "text": "partial"}]}})
        send({"type": "rlm_child_update", "child": {"id": "child-1", "label": "child task", "status": "queued"}})
        send({"type": "rlm_child_update", "child": {"id": "child-1", "label": "child task", "status": "running", "activeSessionId": "active-child-1"}})
        continue
    if kind == "set_model":
        assert request["provider"] == "other" and request["modelId"] == "second"
    if kind == "get_state":
        data = {"sessionFile": session_file, "model": {"provider": "local", "id": "configured"},
                "thinkingLevel": "high", "isStreaming": False,
                "goal": {"status": "active", "objective": "Finish work"}}
    elif kind == "get_available_models":
        data = {"models": [
            {"provider": "local", "id": "configured", "name": "Configured", "reasoning": True,
             "thinkingLevelMap": {"minimal": None, "max": None}},
            {"provider": "other", "id": "second", "name": "Second", "reasoning": False},
        ]}
    elif kind == "get_commands":
        data = {"commands": [
            {"name": "skill:research", "source": "skill", "description": "Research",
             "sourceInfo": {"path": str(root / "SKILL.md")}},
            {"name": "extension-command", "source": "extension", "description": "Extension"},
        ]}
    elif kind == "observe":
        assert request["activeSessionId"] == "active-child-1"
        data = {"messages": [{"role": "user", "content": "child task"},
                             {"role": "assistant", "content": [{"type": "text", "text": "child history"}]}]}
    else:
        data = None

    send({"type": "response", "id": request_id, "command": kind, "success": True, "data": data})

    if kind == "observe":
        send({"type": "observed_session_event", "activeSessionId": "active-child-1",
              "event": {"type": "message_update", "assistantMessageEvent": {"type": "text_delta", "delta": "child live"}}})
        if steer_scenario:
            continue
        send({"type": "observed_session_event", "activeSessionId": "active-child-1",
              "event": {"type": "tool_execution_start", "toolCallId": "python-1", "toolName": "ipython", "args": {"code": "print(1)"}}})
        send({"type": "observed_session_event", "activeSessionId": "active-child-1",
              "event": {"type": "tool_execution_update", "toolCallId": "python-1", "partialResult": {"content": [{"type": "text", "text": "working"}]}}})
        send({"type": "observed_session_event", "activeSessionId": "active-child-1",
              "event": {"type": "tool_execution_end", "toolCallId": "python-1", "isError": False,
                        "result": {"content": [{"type": "text", "text": "1"}]}}})
        send({"type": "rlm_child_update", "child": {"id": "child-1", "label": "child task", "status": "done", "activeSessionId": "active-child-1"}})
        send({"type": "observed_session_event", "activeSessionId": "active-child-1", "event": {"type": "agent_end"}})
        send({"type": "turn_end", "message": {"role": "assistant"}, "toolResults": []})
        send({"type": "agent_end"})
