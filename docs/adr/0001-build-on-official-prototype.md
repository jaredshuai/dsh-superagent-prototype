# 沿用官方 prototype，不再自研桥

云端的 Router、授权、Ticket 和飞书/GitHub 入口直接用官方 `dsh-superagent-prototype`，本 fork 只新增执行端；之前自研的 Python 桥（`dsh-multica-bridge`）归档，不再运行。官方 prototype 已有完整的领域模型（Eval、Mandate、Subscription、Outbox）和面板，自研桥要重做这些且难以跟上上游。

## Consequences

- 旧桥与服务器共用同一个飞书应用，同时运行会导致事件分流，所以旧桥不能再启动。
- 与上游同步是长期成本，见 [0005](0005-isolate-fork-changes.md)。
