# 900 MulticaExecutor 设计稿（草案）

> 状态：草案，2026-11-05。尚未实现，标注「待验证」的地方需先做实测。
> 目标：让 dsh-superagent 的 Ticket 不在云端本机执行，而是经 Multica 派到操作者自己的工作站（BYOM），
> 对应设计文章第 5 节「算力、状态、环境分离」。

## 1. 接入点

`src/core/ports.ts` 的 `Executor` 端口只有三个方法，Engine 只经它驱动执行：

| 端口方法 | DshExecutor（现有） | MulticaExecutor（新增） |
| --- | --- | --- |
| `start(ticket, assignment)` | 起 `worker-<id>` 本机 dsh 会话 + prepare 目录 | 建 Multica issue，metadata 写 `superagent_ticket` / `superagent_assignment`，指派给选定的 agent（= 某台机器上的某个 runtime） |
| `deliver(ticket, assignment)` | `sessions.prompt(..., 'steer')` 注入运行中的会话 | 有活跃 run：`comment add --steer <run>`；无活跃 run：普通评论并重新触发一轮（待验证） |
| `stop(ticket, assignment, reason)` | 取消会话 turn、杀后台 job | `issue cancel-task` |

Engine 回调（`assignmentStarted` / `inputsDelivered` / `inputsRead` / `turnEnded` / `report` / `deliveryFailed`）
由 MulticaExecutor 内部的**轮询循环**产生：轮询 issue 的 run 状态与 run-messages（`since` 游标续传）。

Ticket、Eval、Policy、Outbox、Sources、面板都不改。新增代码集中在 `src/executor/multica*.ts`，
以便与上游同步时少冲突。

## 2. 核心难点：回报协议

DshExecutor 的 Worker 通过会话内的宿主工具与 Engine 交互（`src/app/tools/executor.ts`）：

- `orchestrator_report`：`progress / ready / blocked / failed / dropped` + `summary` + `human_action`（Attention）+ `pr_number`
- `orchestrator_ack`：动手前回执
- `orchestrator_trigger_handback`：超出本 Ticket 范围的消息交还重新评估
- `orchestrator_thread_follow` / `orchestrator_thread_unrelated`：订阅管理
- `orchestrator_read`：读 Ticket 资源（触发消息、Thread 上下文等）

远端 runtime（claude / codex / dsh …）没有这些工具。prototype 的原则是**状态靠结构化字段，不靠解析自由文本**
（`001-domain.md`），所以不能只看 run 的最终输出去猜。

### 方案对比

| 方案 | 做法 | 优点 | 缺点 |
| --- | --- | --- | --- |
| A 远程 MCP | 云端暴露 HTTPS MCP 服务，提供上述工具，每个 Assignment 一个短期 token | 与 DshExecutor 语义最一致 | 需要公网入站端口；要求 Multica agent 能按 issue 注入 MCP 配置（待验证） |
| **B 评论信道（推荐先做）** | 工作站上装一个小 CLI `superagent-report`，它只是把结构化 JSON 作为**带固定标记的评论**发到 issue（底层调 `multica issue comment add`）；云端轮询评论/run-messages 解析标记块 | **云端只出站**，不开端口；任何能跑 shell 的 runtime 都能用；评论天然留痕可审计 | 延迟 = 轮询间隔；评论里的 JSON 要做签名/校验防伪造 |
| C 末尾文本兜底 | run 结束仍无回报时，按 run 终态映射：completed → `waiting`，failed → `failed` | 零依赖 | 只能当兜底，信息量最少 |

**推荐：B 为主、C 兜底，A 留作后续。**

标记块示例（评论正文）：

````text
```superagent-report
{"v":1,"assignment":"<assignment id>","tool":"report","state":"ready","summary":"…","pr_number":12,"sig":"<HMAC>"}
```
````

- `sig`：云端在 `start` 时生成每个 Assignment 的密钥，经 issue 描述交给 Worker；CLI 用它对 JSON 做 HMAC。
  云端只接受签名正确、`assignment` 匹配当前 Assignment 的块，防止被评论里的提示注入伪造回报。
- `tool` 取值：`report` / `ack` / `handback` / `follow` / `unrelated`，对应上面的宿主工具。
- `orchestrator_read` 第一版不做远程化：`start` 时把 Ticket 的触发消息、Thread 摘要、执行变量写进 issue 描述（Inline）；
  大内容给链接（Spill）。

## 3. 生命周期映射

| prototype 概念 | Multica 对应 | 备注 |
| --- | --- | --- |
| Ticket | issue（metadata 反查） | 一个 Ticket 一个 issue，跨 Assignment 复用 |
| Assignment | issue 上的一串 run | 新 Assignment（retry / restart）= 取消旧 run 后重新触发 |
| Worker turn | 一个 run | run `completed` 且无回报 ⇒ `turnEnded` ⇒ `waiting` |
| Input 投递 | 评论 / `--steer` | **steer 限额：每 agent 每 run 3 条、合计 10 条**（实测），超出后排队等 run 结束再以新评论触发 |
| `inputsRead` | run-messages 中出现对该评论的处理 | 粗粒度：run 开始即视为已读（待细化） |
| 工作目录 | 工作站上的 issue workdir | **实测每个 run 一个隔离 workdir、跨 run 不共享**：状态必须靠 git 分支 `agent/<name>` 持久化，每轮先 checkout 该分支 |

## 4. 机器与 runtime 选择

### 桌面端 daemon 与 CLI daemon（源码 `CLI_AND_DAEMON.md`、`apps/desktop/src/main/daemon-profile.ts`）

| | CLI daemon | 桌面端 daemon |
| --- | --- | --- |
| 启动 | `multica daemon start`（可 `autostart enable` 开机自启） | 桌面 App 拉起，生命周期归 App 管（`Managed by: Multica Desktop app`） |
| profile / 状态目录 | 默认 `~/.multica/`（或 `--profile <name>`） | 专属 `~/.multica/profiles/desktop-<host>/`，如 `desktop-api.multica.ai`，从不读写用户的 CLI profile |
| 二进制 | 用户装的 `multica`（brew / 安装包），换二进制会自动跟随重启 | App 内置的 CLI，随 App 更新 |
| 工作目录 | `~/multica_workspaces/…` | `~/multica_workspaces_desktop-<host>/…`（JAR-19 的 workdir 即此类，说明当时是桌面端 daemon 执行的） |

**同机双 daemon 在 Multica 层面已是同一身份**（`server/internal/daemon/identity.go` `EnsureDaemonID`）：
daemon_id 是机器级的，存在 `~/.multica/daemon.id`，CLI daemon 与桌面端 daemon 共用，服务器上一台机器 × 一个 provider
只有一行 runtime。两个 daemon 都对这行 runtime 发心跳（约 15 s）、都能认领任务，认领在服务端原子完成。
所以「桌面端关了 / CLI daemon 没拉起」只要另一个还在，runtime 仍在线、任务照常被认领——**同机双活 Multica 已经提供**，
不要在设计里要求用户只留一个。

### 双活设计（三层）

1. **同机双 daemon**：交给 Multica。Executor 不区分、不绑定 profile；任一 daemon 执行结果等价（任务环境一致，状态在 git 分支上）。
   待验证：一个 daemon 正常退出时会调 `Deregister` 把 runtime 置 offline，另一个的下次心跳是否立即把它拉回 online（`agents_refresh.go`
   `deregisterRevivedRuntimes` 暗示有此处理）；这段空窗内派发的任务是否只是排队而不失败。
2. **整机离线**（两个 daemon 都不在）：Multica 自身会把 running 任务在重连宽限期后以 `runtime_offline` 失败，并生成 deferred 重试，等原 runtime 回来。
   Executor 在其上做**跨机器故障转移**：
   - 候选 agent 有序列表（同一 provider 在不同机器上各一个 agent，或不同 provider 互为备份）；健康 = runtime `online` 且 `last_seen_at` 新鲜，
     实时来自 WebSocket 的 runtime/agent 状态事件。
   - `start` 选第一个健康候选；任务 `queued` 超过阈值仍未被认领、或以 `runtime_offline` / `timeout` 失败 ⇒ 先 `cancel-task`，
     再开**新 Assignment** 派给下一个健康候选（prototype 的 Assignment 模型本来就支持「新 Assignment 取代旧的」）。
   - 防重复执行：先取消再改派；回报签名密钥按 Assignment 生成，旧 Assignment 迟到的回报一律拒收；进度只认 git 分支，推送冲突即暴露并发。
   **实测（2026-11-05，jxccs，opencode runtime，探针 agent `failover-test-jxccs`）**：
   - 基线 JAR-47（双 daemon 在线）：桌面端 daemon 认领（`MULTICA_DAEMON_PORT=19681`，workdir `~/multica_workspaces_desktop-api.multica.ai/…`），派发→完成 54 s。
   - JAR-48（用户退出桌面 App，CLI daemon 仍在）：runtime **显示 offline 约 2 分钟**（02:34:42 → 02:36:36），尽管 CLI daemon 一直在跑；
     任务**一直 queued、没有失败**；runtime 恢复 online 后约 45 s 被认领，由 CLI daemon 执行（`PORT=19514`，workdir `~/multica_workspaces/…`），正常完成。
     从派发到开始执行共约 2 分 45 秒。
   - 结论：同机双活**不丢任务**，但有约 2–3 分钟的切换空窗；空窗内 runtime 状态是 offline。
     ⇒ Executor **不能把「runtime offline」直接当作机器宕机**去跨机改派，跨机故障转移阈值要大于这段空窗（建议 ≥ 5 分钟未认领），
     否则一次正常的关 App 就会引发不必要的改派。
   - 补充：桌面端默认 `autoStop: false`（`apps/desktop/src/main/daemon-manager.ts`），关 App 不停 daemon；只有开启「退出时停止守护进程」
     或显式停止时才出现上述空窗。真实掉线主因更可能是：休眠/关机/断网（两个 daemon 同时掉）、CLI daemon 开机未拉起、App 更新重启。
2b. **runtime 在线但执行不了**（如 agent CLI 登录过期、升级损坏，JAR-17 即 codebuddy 登录过期）：机器和 daemon 都健康，任务却必然失败。
   Executor 按**失败原因**分流：`runtime_offline` / `timeout` 走等待与跨机改派；不可重试类（认证失效、CLI 不可用等，诊断要看 run-messages
   而非只看 `run.error`）直接换候选 runtime，并把该 runtime 在本实例内标记为暂不可用，避免原地反复重试。
3. **全部离线**：Ticket 停在 queued，给 Ticket 挂一个 Attention（「没有在线的执行机器」），只通知一次；任一候选恢复在线即自动派发。

**任务环境**（`server/internal/daemon/daemon.go` `taskMulticaEnvironment`、`task_cli.go`）：每个 run 注入
`MULTICA_TOKEN`（任务级 `mat_` token，非用户 `mul_`）、`MULTICA_SERVER_URL`、`MULTICA_WORKSPACE_ID`、
`MULTICA_AGENT_ID`、`MULTICA_TASK_ID`，并把 daemon 自己的 `multica` 二进制目录放到 `PATH` 最前。
⇒ 远端 Worker 在执行中可以直接调 `multica issue comment add`，以任务身份发回报，**不需要在工作站另装/另登录 CLI**；
桌面端 daemon 下调到的是 App 内置的 CLI 版本。这基本回答了第 7 节第 2 项（仍需线上实测）。

### 选择策略

- Multica 的 agent = 一台机器上的一个 runtime。设置里加一个 `executor.multica` 块：
  `apiBase`、`workspaceId`、`token`（secret，只放环境变量）、`agents`（候选 agent 列表，带机器名/runtime/标签）。
- 第一版：固定一个默认 agent；Router 的 `accept` 结果里可带 `note` 指定偏好（如「用 claude」）。
- 后续：按机器在线状态、负载、仓库是否在本机挑选。

## 5. 身份与凭据

- prototype 让 Worker 以 Agent 账号（`gh-agent`）提交和开 PR。远端工作站默认是操作者自己的 git/gh 身份。
  需在每台工作站配同样的 `gh-agent` 包装，并在 issue 描述里要求 Worker 使用它；否则 PR 会署操作者的名。
- Multica token 只在云端；工作站只拿到每 Assignment 的签名密钥，不拿任何云端凭据。

## 6. 去轮询：推送优先 + 断线补齐

原则：**实时靠推送，轮询只在连接建立/重连时做一次游标补齐**，不做周期轮询。
推送通道断开期间的事件不会补推（飞书已实测证实），所以「重连即补齐」不能省。

| 通道 | 推送方式 | 补齐方式 |
| --- | --- | --- |
| Multica → 云端 | **WebSocket**：`wss://api.multica.ai/ws?workspace_id=<W>`，首帧 `{"type":"auth","payload":{"token":"mul_…"}}`，收到 `auth_ack` 后接收工作区事件：`task:queued/dispatch/running/completed/failed/cancelled`、`comment:created`、`issue:updated` 等（源码 `server/internal/realtime/hub.go`、`server/pkg/protocol/events.go`）。`task:message` 是 task 作用域，需发 `subscribe` 帧 | 重连后按 metadata 反查本实例的活跃 issue，REST 拉一次 run 状态与 `messages?since=<seq>` |
| GitHub → 云端 | **仓库 webhook**：`issues`、`issue_comment`、`pull_request`、`pull_request_review(_comment)`、`check_suite/check_run` 推到服务器 HTTPS 入口，HMAC（`X-Hub-Signature-256`）校验 | 启动/重连时按现有通知游标读一次 `notifications?since=`；失败的投递可用 GitHub 的 redelivery API 重放 |
| 飞书 → 云端 | 已是长连接推送（`lark-cli event consume`） | 已有历史补收；「已订阅线程轮询」降为低频兜底 |
| 云端 → Multica / GitHub / 飞书 | 本来就是主动调用 | — |

代价与前提：

- GitHub webhook 需要**公网入站 HTTPS**：服务器开 443，用 Caddy + `sslip.io` 域名（`170-106-177-94.sslip.io`）自动签发证书；只暴露 webhook 路径，管理面板仍只监听 127.0.0.1。
- 改 GitHub Source 属于 prototype 核心改动（不只是新增 Executor），与上游同步时冲突面变大；建议做成可开关：`delivery: "poll" | "webhook"`，默认保持上游行为。
- Multica WebSocket 是网页端用的接口，未作为公开 API 承诺稳定；需封装在一个适配层里，协议变了只改一处。

## 7. 待验证清单（先做 spike，再写代码）

1. 对**没有活跃 run** 的 issue 发普通评论，是否会自动起新 run？（决定 `deliver` 的实现）
2. 工作站上的 runtime 能否在执行中调用 `multica issue comment add`（CLI 是否在 PATH、是否已登录、是否认得当前 issue）？
3. 跨 run 的 workdir 隔离：第二个 run 能否 checkout 上一轮推送的分支继续干？
4. steer 限额触顶后的行为（报错 / 静默丢弃）。
5. 各 runtime（claude / codex / dsh）对「先读 issue 描述里的协议说明、再按协议回报」的遵从度。
6. Multica agent 能否按 issue 注入 MCP 配置（决定方案 A 是否可行）。
7. 用 `mul_` token 连 `wss://api.multica.ai/ws` 是否真能收到 `task:*` 与 `comment:created`（源码支持，线上未实测）；心跳与断线行为。
