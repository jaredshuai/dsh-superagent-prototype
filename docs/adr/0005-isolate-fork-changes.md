# fork 的改动集中在 `src/executor/multica*.ts`，用环境变量而非设置启用

MulticaExecutor 与其协议、REST、WebSocket 代码都放在 `src/executor/multica*.ts`，核心只改 `src/app/runtime.ts` 的组装处；启用靠 `SUPERAGENT_EXECUTOR=multica` 等环境变量，而不是在设置 schema 里加 `executor.multica` 块。这样与上游同步时冲突面最小，Multica token 也不会进入设置历史。代价是面板上看不到也改不了这些配置。

**不回馈上游**（2026-11-08 补充）：不向上游 `deepseek-ai/dsh-superagent-prototype` 发 issue、开 PR、评论或推送。不得不改的上游文件（如 `src/sources/github.ts`、`scripts/dsh-superagent`）的修复也只留在本 fork，同步上游时自己解决冲突。`upstream` remote 只用于 fetch。
