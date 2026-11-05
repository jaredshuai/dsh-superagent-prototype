# dsh-superagent × Multica

本 fork 在官方 dsh-superagent 之上，把 Ticket 的执行从云端本机移到操作者自己的电脑（经 Multica 派发）。

上游的领域术语（Source、Trigger、Thread、Eval、Mandate、Ticket、Assignment、Subscription、Executor、Report、Input 等）
以 [docs/001-domain.md](docs/001-domain.md) 的 Concepts 与 Glossary 为准，这里不重复；下面只收本 fork 新增的术语。

## 执行端

**工作站（Workstation）**：
操作者自己的一台电脑（如 JAREDHOME、JaredAI、jxccs），上面装着编程 CLI 和 Multica，远端 Worker 在这里执行。
_Avoid_: 机器、节点、BYOM 主机

**Multica agent**：
Multica 里可被指派 issue 的执行者，固定绑定一台工作站上的一个 runtime；派工时选的就是它。
_Avoid_: 单说「agent」（与上游的 Agent，即自动化团队成员，混淆）

**Runtime**：
一台工作站上的一种编程 CLI（如 claude、codex、opencode），在 Multica 里一台工作站 × 一种 CLI 只有一个。
_Avoid_: provider、引擎

**Daemon**：
工作站上替 Multica 认领并执行任务的常驻进程；同一工作站可同时有桌面端 daemon 与 CLI daemon，二者共用一个身份。
_Avoid_: 守护进程实例、客户端

**远端 Worker（Remote worker）**：
在工作站上执行一个 Assignment 的编程 CLI 会话；对应上游的 Executor Session，但不在云端。
_Avoid_: 远程 Session、执行机

## Assignment 在 Multica 上的样子

**Ticket issue**：
一个 Ticket 在 Multica 上对应的 issue，跨该 Ticket 的所有 Assignment 复用。
_Avoid_: 任务、工单 issue

**Run**：
Ticket issue 上的一次执行，即远端 Worker 的一个 turn；新消息到来时开始下一个 run。
_Avoid_: task（Multica API 里的叫法）、执行、轮次

**工作分支（Work branch）**：
`agent/<name>`，跨 run、跨工作站保存一个 Ticket 工作进度的唯一载体。
_Avoid_: worktree、工作目录

**Steer**：
把消息注入一个正在运行的 run，而不是另起一个 run；只有部分 runtime 支持。
_Avoid_: 插话、补充消息

## 回报

**回报块（Report block）**：
远端 Worker 在 Ticket issue 评论里贴出的带固定标记的结构化块，是它向 Engine 回报、回执和交还消息的唯一方式。
_Avoid_: 回报评论、标记块

**Nonce**：
每个 Assignment 一个的随机值，下发在 issue 描述或改派评论里；回报块带上它才被接受。
_Avoid_: 签名、密钥、token

**补齐（Catch-up）**：
事件推送连接每次建立或重连时，按游标把断线期间错过的事实一次性拉回来；不是周期轮询。
_Avoid_: 轮询、对账循环

**双活（Dual-active）**：
同一工作站上桌面端 daemon 与 CLI daemon 同时在线、任一都能执行，不要求只留一个。
_Avoid_: 主备、故障切换（那是跨工作站的事）

**改派（Reassignment）**：
因工作站整机离线或 runtime 执行不了，取消当前 run、开新 Assignment 派给另一个 Multica agent。
_Avoid_: 重试（上游的 retry 是人发起的）、迁移
