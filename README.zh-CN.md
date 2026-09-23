# JJzeron

JJzeron 是 [Zeron](https://github.com/zeronsh/zeron) 的本地 fork，增加了 Prime Agent 原生 RPC 接入。在本地管理编码 agent（Claude Code、Codex、Cursor、Grok、Hermes、Pi、Prime Agent、Antigravity），也可以打开多设备同步。

*[English](README.md) | 简体中文*

![Zeron 驱动一个 Claude Code 会话，侧边栏是实时的分支 diff](apps/landing/public/assets/app-screenshot.jpg)

每台设备各跑一个小引擎，会话就存在这台设备上。装完默认是纯本地模式，不用账号，也不用联网。

## 从源码构建并运行

```bash
cargo build --release -p zeron
./target/release/jjzeron status
```

JJzeron 在 Unix 使用 `~/.jjzeron`，在 Windows 使用独立的应用目录，并采用独立的守护进程服务名和 `jjzeron://` URL scheme。源码构建使用仓库固定的 Rust 1.95.0。上游 Zeron 的安装脚本安装的是原版，不是这个 fork。

Prime Agent 直接使用本机 `prime-agent` CLI 及其现有配置、扩展、skills 和 AGENTS.md。`WatchRunEvents {chatId, afterSeq?}` 返回 `{seq, event}`，其中 `event.event` 保留 Prime JSONL RPC 原始通知；原始通知与映射后的聊天事件都能从本地运行日志按序号回放。

日常命令：

```bash
jjzeron status      # 查看本地/同步模式和引擎状态
jjzeron daemon start|stop|restart|status
```

更新默认从 [JJzeron 的 GitHub Releases](https://github.com/lorianlee98-spec/JJzeron/releases) 检查，本地模式也会收到新版本提示。macOS App 可以从提示中下载并重启安装；`ZERON_RELEASES_URL` 可覆盖默认更新源。发布新版时，先更新 `Cargo.toml` 的版本，再推送同版本的 `v<版本>` 标签，GitHub Actions 会构建并发布安装包。

## 可选：多设备同步

只有想打开账号下的同步工作区时才需要登录。登录会换掉引擎下次启动时用的 profile，所以改之前先停掉守护进程：

```bash
jjzeron daemon stop
jjzeron login
jjzeron daemon start
```

之后就可以在一台同步过的设备上起 agent，换另一台设备接着看、接着操作。一台常开的机器，比如 VPS，可以在你合上笔记本之后继续跑这些 agent。

登录不会上传、搬走或导入已有的本地会话。本地会话和它们的附件仍然留在本地 profile 下，切回纯本地模式时会照常出现：

```bash
jjzeron daemon stop
jjzeron logout
jjzeron daemon start
```

如果有引擎正占着数据目录，`jjzeron login` 和 `jjzeron logout` 会拒绝改动凭据。桌面应用同样遵守这条边界：profile 要等下次重启才切换。

macOS 上可以从源码构建 `jjzeron`，再运行 `jjzeron daemon install` 装上 launchd 服务。

## 赞助

感谢 [The Context Company](https://www.thecontextcompany.com/) 对上游 Zeron 项目的赞助。

你也可以资助 Zeron 的开发。欢迎个人和公司[通过 GitHub 成为赞助者](https://github.com/sponsors/zeronsh)。

---

想参与开发，或者好奇它怎么跑起来的？[![Ask DeepWiki](https://deepwiki.com/badge.svg)](https://deepwiki.com/zeronsh/zeron)，也可以看 [ARCHITECTURE.md](ARCHITECTURE.md)。

采用 [MIT License](LICENSE)。
