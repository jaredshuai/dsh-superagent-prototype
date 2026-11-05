# 推送优先，只在（重）连接时补齐一次

云端拿外部事实（Multica 的 run 与评论，以后的 GitHub）靠推送：Multica 用工作区 WebSocket，GitHub 计划改用 webhook。不做周期轮询，只在推送连接建立或重连时按游标补齐一次断线期间错过的事实。周期轮询要么延迟大、要么请求量大，而推送断线期间的事件服务端不会补发（飞书已实测），所以「重连即补齐」不能省。

## Consequences

- Multica 的 WebSocket 是网页端接口，没有承诺稳定，协议知识只放在 `src/executor/multica-events.ts` 一处。
- GitHub webhook 需要公网入站 HTTPS；在那之前 GitHub 仍用官方的 60 s 轮询。
