# Agent 用独立的 GitHub 账号

Agent 提交、开 PR 用独立账号 `jaredwtt1203`（服务器上经 `gh-agent` 包装），不用操作者自己的 `jaredshuai`。这样 Agent 的产出可以和人的区分开、权限可以单独收紧，授权名单里的人（`jaredshuai`）也不会与 Agent 自己混淆。远端工作站默认是操作者自己的 git 身份，所以每台工作站也要配同样的包装，并在 Worker 说明里要求使用它，否则 PR 会署操作者的名。
