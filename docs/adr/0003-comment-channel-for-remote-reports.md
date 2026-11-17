# 远端 Worker 通过 issue 评论里的回报块回报，用 nonce 而非 HMAC 防伪

远端 Worker 没有 DSH 的宿主工具（`orchestrator_report` 等），它改为在 Ticket issue 上发带 `superagent-report` 回报块的评论，云端解析后转成 Engine 调用。这样云端只需出站连接、不开端口，任何能跑 shell 的 runtime 都能用，评论天然留痕。回报块要同时满足三条才被接受：评论作者是配置的 Multica agent、`assignment` 是当前 Assignment、`nonce` 与下发的一致。

## Considered Options

- **远程 MCP**：语义与本地工具最一致，但要公网入站端口，且要求 Multica 能按 issue 注入 MCP 配置。2026-11-17 查源码已经没有这条路：MCP 配置只有工作区库（按 agent 绑定）、agent 自己的 `mcp_config`（该 agent 的所有 run 共用）与服务端在 enqueue 时算出的 per-task overlay（来源是发起人的 Composio 连接，没有外部入口）三层，都按 agent 按 run，不认 issue，所以「每个 Assignment 一个短期 token」写不进去（见 [009](../009-multica-executor.md) 7.2）。放弃。
- **HMAC 签名**：要求模型在 shell 里正确计算 HMAC，实测不可靠；作者校验由 Multica 服务端保证，加上 nonce 已能挡住提示注入伪造与旧 Assignment 的迟到回报。
