# 远端 Worker 通过 issue 评论里的回报块回报，用 nonce 而非 HMAC 防伪

远端 Worker 没有 DSH 的宿主工具（`orchestrator_report` 等），它改为在 Ticket issue 上发带 `superagent-report` 回报块的评论，云端解析后转成 Engine 调用。这样云端只需出站连接、不开端口，任何能跑 shell 的 runtime 都能用，评论天然留痕。回报块要同时满足三条才被接受：评论作者是配置的 Multica agent、`assignment` 是当前 Assignment、`nonce` 与下发的一致。

## Considered Options

- **远程 MCP**：语义与本地工具最一致，但要公网入站端口，且要求 Multica 能按 issue 注入 MCP 配置（未验证）。留作以后。
- **HMAC 签名**：要求模型在 shell 里正确计算 HMAC，实测不可靠；作者校验由 Multica 服务端保证，加上 nonce 已能挡住提示注入伪造与旧 Assignment 的迟到回报。
