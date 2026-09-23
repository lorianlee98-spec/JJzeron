# JJzeron

JJzeron is a local fork of [Zeron](https://github.com/zeronsh/zeron) with native Prime Agent RPC support. Control your coding agents (Claude Code, Codex, Cursor, Devin, Grok, Hermes, Pi, Prime Agent, Antigravity) locally by default, with optional multi-device sync.

*English | [简体中文](README.zh-CN.md)*

![Zeron driving a Claude Code session with a live branch diff sidebar](apps/landing/public/assets/app-screenshot.jpg)

Every device runs a small engine that stores sessions on that device. A new installation starts in local-only mode without an account or a network connection.

## Build and run locally

```bash
cargo build --release -p zeron
./target/release/jjzeron status
```

JJzeron uses `~/.jjzeron` on Unix, a separate Windows application directory, a separate daemon service, and the `jjzeron://` URL scheme. Build with the pinned Rust 1.95.0 toolchain. The upstream Zeron installer installs the original app, not this fork.

Prime Agent uses your local `prime-agent` CLI and its existing config, extensions, skills, and AGENTS.md. JJzeron forwards every Prime JSONL RPC notification through `WatchRunEvents {chatId, afterSeq?}` as `{seq, event}`; the exact Prime payload is under `event.event`. Native notifications and their normalized chat events are replayable from the local run journal.

The desktop sidebar browser also needs the [Linux browser runtime](docs/reference/linux-browser.md).

Day-to-day:

```bash
jjzeron status      # local/synced mode and engine status
jjzeron daemon start|stop|restart|status
```

Updates check [JJzeron's GitHub Releases](https://github.com/lorianlee98-spec/JJzeron/releases) by default, including in local mode. The macOS app can download and restart into a new release from its update notice. `ZERON_RELEASES_URL` overrides the default feed. To publish a new version, bump `Cargo.toml` and push the matching `v<version>` tag; GitHub Actions builds and publishes the packages.

## Optional multi-device sync

Sign in only when you want to open your account's synced workspace. Authentication changes the profile selected by the next engine start, so stop the daemon before changing it:

```bash
jjzeron daemon stop
jjzeron login
jjzeron daemon start
```

You can then start an agent on one synced device and follow or drive it from another. An always-on machine such as a VPS can keep those agents working after you close your laptop.

Devices signed in to the same synced account are trusted with remote workspace access. A device controlling a workspace on another device can list, read, and write its files; enabling `Show ignored files` also makes gitignored files such as `.env` available remotely. `.git` is always excluded. Only sign in devices you trust with the full contents of your workspaces.

Signing in does not upload, move, or import existing local sessions. Local sessions and their attachments remain under the local profile and reappear when you return to local-only mode:

```bash
jjzeron daemon stop
jjzeron logout
jjzeron daemon start
```

`jjzeron login` and `jjzeron logout` refuse to modify credentials while an engine owns the data directory. The desktop app follows the same next-restart profile boundary.

On macOS: build from source and run `jjzeron daemon install` to install the launchd service.

On Windows: run `jjzeron.exe` from a fork release ZIP. Keep `jjzeron-update.json` beside it for in-app updates. See the [development notes](docs/reference/windows-development.md) for source builds.

## Sponsors

Thank you to [The Context Company](https://www.thecontextcompany.com/) for sponsoring the upstream Zeron project.

You can help fund upstream Zeron's development too. Individuals and companies are welcome to [become a sponsor on GitHub](https://github.com/sponsors/zeronsh).

---

Developing or curious how it works? [![Ask DeepWiki](https://deepwiki.com/badge.svg)](https://deepwiki.com/zeronsh/zeron) or check out [ARCHITECTURE.md](ARCHITECTURE.md).

Licensed under the [MIT License](LICENSE).
