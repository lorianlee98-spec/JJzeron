# Prime Agent 的文件式 subagent preset 可行性

范围：以本机 `codex-cli 0.156.1` 和 Prime Agent `0.9.5` 为准。本文只研究，不安装或修改插件。Prime 引用的相对路径均位于本机已安装版本的 `~/.local/share/prime-agent/releases/0.9.5-*/` 下。

## 结论

- **可以从现有 extension 示例改造文件式 preset**：按名字选定义，再为独立 Prime 子进程指定指令、模型、effort 和工具。Prime 已附带 `examples/extensions/subagent/`，但该示例目前没有 effort 字段，也未验证它在本机打包版中的子进程启动与示例 `tools: bash` 是否可用；不能直接称它为已可用的完整方案。
- **不能把示例等同于原生 `rlm.spawn`**：它启动独立的 `prime-agent --mode json -p --no-session` 进程，不进入父代理的 RLM 子会话注册表，也没有原生的保留会话、后续消息和归属。若重点是“父代理只写 preset 名和任务，但保留原生 child”，Python-backed skill 包装 `rlm.spawn` 更小；它无法给 child 设置真正独立的 developer 指令或权限。
- **四项都要可靠生效且保留原生 RLM 生命周期，需要 Prime host 增加 preset 解析/子会话配置能力**。当前 `rlm.spawn(prompt, *, name, model?, thinking?)` 不接受 preset、工具权限或沙箱参数；extension 的 `tool_call` 钩子只拦截模型工具调用，不能视作对 Python 内部 `rlm.run` host request 的可靠权限边界。上述为现有公开接口得出的设计结论，不是已实现的功能。[^prime-rlm][^prime-extension]

## Codex 的做法及版本陷阱

Codex 会从 `~/.codex/agents/*.toml` 和项目 `.codex/agents/*.toml` 发现角色。角色可写 `name`、`description`、`developer_instructions`、`model`、`model_reasoning_effort`；父代理调用 `spawn_agent` 时用 `agent_type` 选角色。源码按配置层读取并合并同名角色。[^codex-docs][^codex-loader][^codex-role]

**权限不能照搬文档描述。** 官方文档称自定义角色文件能覆盖 `sandbox_mode` 等会话设置，也强调子代理继承当前权限。与本机 CLI 匹配的 `rust-v0.156.1` 源码实际只把有限的角色字段应用到子会话：developer 指令、model、reasoning effort 等，以及“只能关闭”的部分功能/技能。源码测试明确断言：角色文件中的 `sandbox_mode`、`approval_policy`、`model_provider`、`mcp_servers` 不改变父会话对应的权限或路由。因此不应把本机 preset 文件的 `sandbox_mode = "read-only"` 当作该版本已执行的隔离。[^codex-role][^codex-tests][^codex-runtime]

## Prime 已有的基础

`examples/extensions/subagent/agents.ts` 从用户目录 `~/.prime/agent/agents/*.md` 及最近的项目 `.prime/agent/agents/*.md` 读取带 YAML frontmatter 的定义；同时支持 user/project/both，项目定义可覆盖同名用户定义。定义包含 `name`、`description`、`model`、`tools` 和正文指令。`index.ts` 用 `pi.registerTool({ name: "subagent" })` 暴露按角色名和任务调用；项目定义默认不启用，显式启用后在有交互 UI 时询问确认。[^prime-example]

示例当前只将 `model`、`tools` 传给子进程，并把正文传作追加系统提示；**没有解析或传递 `thinking`**。最小扩展是加入 `thinking` 字段及 `--thinking`，沿用现有加载机制。先在打包版验证子进程启动，以及示例的 `tools: bash` 是否真的可调用：当前默认内置工具是 `ipython`，不能只凭示例文件推断 `bash` 可用。这里的 `tools` 是工具名白名单，不是操作系统权限：若允许 `ipython` 或任意 shell，不能据此保证“只读”。Prime 文档明确说 REPL 拥有进程的 OS 权限，不是沙箱。无交互 UI 时，示例中的项目确认分支不会弹出；在无头场景应拒绝未获准的项目 preset，而非把缺少 UI 当成授权。另有 `examples/extensions/preset.ts`，但它修改的是**当前代理**的模型、effort 和工具，并不启动子代理。[^prime-example][^prime-current-preset][^prime-security]

## 建议的演进顺序（未实施）

1. **只为减少重复填参数**：优先做 Python-backed skill，例如 `await agent_presets.spawn("code-mapper", "映射认证调用链")`。它读取可信的用户 preset，验证确切模型与 effort，组成任务后调用原生 `rlm.spawn`，保留非阻塞句柄和父子归属。`instruction` 只能作为任务上下文，`permission` 只能标注而不能假装强制执行；每次运行的 `name` 仍须唯一。[^prime-rlm][^prime-skill]
2. **想先试纯 TypeScript extension**：直接改造附带的 `subagent` 示例，加 `thinking`、模型/effort 验证和无 UI 时的项目 preset 拒绝。明确它是独立进程，不与原生 `rlm.spawn` 等价。不要为只读写一个提示词就宣称已限制文件写入。
3. **要求完整四项且是真正的原生子代理**：向 Prime host 增加 `preset` 解析（用户名与项目来源、冲突优先级、显式选择和信任确认），在创建 child 前锁定模型/effort、开发者指令及可执行的工具/沙箱策略；权限最多收紧，不能超出父会话。需要 host 和 RLM API 的明确契约，不能仅靠 extension 里的工具事件重写实现。[^prime-rlm][^prime-security]

[^codex-docs]: [Codex 官方 Subagents 文档](https://developers.openai.com/codex/subagents)（重定向到 ChatGPT 文档）。
[^codex-loader]: [Codex 0.156.1 `agent-roles/src/loader.rs` L23–113](https://github.com/openai/codex/blob/b412ff32c417f855c2b2d1581b77058eed87c84b/codex-rs/agent-roles/src/loader.rs#L23-L113)。
[^codex-role]: [Codex 0.156.1 `core/src/agent/role.rs` L36–115、L177–216](https://github.com/openai/codex/blob/b412ff32c417f855c2b2d1581b77058eed87c84b/codex-rs/core/src/agent/role.rs#L36-L115)。
[^codex-tests]: [Codex 0.156.1 `role_tests.rs` L393–520](https://github.com/openai/codex/blob/b412ff32c417f855c2b2d1581b77058eed87c84b/codex-rs/core/src/agent/role_tests.rs#L393-L520)。
[^codex-runtime]: [Codex 0.156.1 `child_config.rs`，spawn 时重设权限](https://github.com/openai/codex/blob/b412ff32c417f855c2b2d1581b77058eed87c84b/codex-rs/core/src/agent/child_config.rs#L171-L192)。
[^prime-example]: Prime Agent 0.9.5 已安装源码：`examples/extensions/subagent/README.md`、`agents.ts` L13–113、`index.ts` L233–277、L387–466。
[^prime-rlm]: Prime Agent 0.9.5 已安装文档：`docs/rlm-runtime.md` L113–148。
[^prime-extension]: Prime Agent 0.9.5 已安装文档：`docs/extensions.md` L708–745、L1265–1294。
[^prime-security]: Prime Agent 0.9.5 已安装文档：`docs/rlm-runtime.md` L229–233；`examples/extensions/subagent/agents/reviewer.md` 的提示词也明示工具权限不能完美强制。
[^prime-current-preset]: Prime Agent 0.9.5 已安装源码：`examples/extensions/preset.ts` L49–60、L70–98、L132–157、L379–386；默认工具见 `docs/usage.md` L234–242。
[^prime-skill]: Prime Agent 0.9.5 已安装文档：`docs/skills.md` L141–168。
