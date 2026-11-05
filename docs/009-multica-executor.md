# 009 MulticaExecutor 设计稿（草案）

> 状态：草案，2026-11-05。尚未实现，标注「待验证」的地方需先做实测。
> 目标：让 dsh-superagent 的 Ticket 不在云端本机执行，而是经 Multica 派到操作者自己的工作站（BYOM），
> 对应设计文章第 5 节「算力、状态、环境分离」。

## 1. 接入点

`src/core/ports.ts` 的 `Executor` 端口只有三个方法，Engine 只经它驱动执行：

| 端口方法 | DshExecutor（现有） | MulticaExecutor（新增） |
| --- | --- | --- |
| `start(ticket, assignment)` | 起 `worker-<id>` 本机 dsh 会话 + prepare 目录 | 建 Multica issue，metadata 写 `superagent_ticket` / `superagent_assignment`，指派给选定的 agent（= 某台机器上的某个 runtime） |
| `deliver(ticket, assignment)` | `sessions.prompt(..., 'steer')` 注入运行中的会话 | 活跃 run 且 `can_supplement`：`comment add --steer <run>`；否则把所有未投递 Input **合并成一条**普通评论，且只在 issue 上没有 queued run 时才发（见第 7 节 spike 结论） |
| `stop(ticket, assignment, reason)` | 取消会话 turn、杀后台 job | `issue cancel-task` |

Engine 回调（`assignmentStarted` / `inputsDelivered` / `inputsRead` / `turnEnded` / `report` / `deliveryFailed`）
由 MulticaExecutor 内部的事件处理产生：实时靠 WebSocket 推送，连接建立/重连时 REST 补齐一次 run 状态与 run-messages（`since` 游标续传），见第 6 节。

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
| Input 投递 | 评论 / `--steer` | steer 只对支持的 runtime 有效（见第 7 节）；不支持时每条评论各自排一个 run，必须合并 |
| `inputsDelivered` | 投递评论的 id 出现在某个 run 的 `trigger_comment_id` / `delivered_comment_ids` 里 | 发评论成功 ≠ 已投递 |
| `inputsRead` | 上述 run 进入 `running` | 粗粒度，足够 |
| 工作目录 | 工作站上的 issue workdir | 同一 daemon 上同一 issue 的后续 run **复用** workdir（JAR-49 实测）；换 daemon / 换机器则是新目录。⇒ 仍以 git 分支 `agent/<name>` 为唯一可靠状态，每轮先 fetch + checkout，已存在的 checkout 直接复用 |

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

### 7.1 Spike 结论（2026-11-05，jxccs，测试 agent `failover-test-jxccs`，opencode runtime）

| # | 问题 | 结论 | 证据 |
| --- | --- | --- | --- |
| 1 | 无活跃 run 时发普通评论会起新 run 吗？ | **会，立即**。run 的 `attribution.evidence.kind = comment`，`trigger_comment_id` = 该评论；评论 id 也出现在 `delivered_comment_ids` | JAR-49 第 2 轮，评论后 3 s 进入 running |
| 2 | run 内能否调 `multica issue comment add`？ | **能**，无需另装另登录。PATH 里是 daemon 自带的 CLI（桌面端：`/opt/Multica/resources/app.asar.unpacked/resources/bin/multica`），`$MULTICA_TASK_ID` 有值；评论作者为 `agent`。评论触发的 run 回复时须带 `--parent <触发评论>` | JAR-49 R1/R2 评论 |
| 3 | 下一轮能否续用上一轮推送的分支？ | **能**。而且同一 daemon 上同一 issue 的 workdir 是**复用**的（上一轮 checkout 还在，`git clone` 会因目录已存在而失败）——Worker 指令要写成幂等的「有就 fetch，没有就 clone」 | JAR-49：`STATE=run1,run2`，两轮 `work_dir` 相同 |
| 4 | steer 限额触顶后的行为 | 比预想更严：**opencode run 根本不能 steer**，12 次 `--steer` 全部返回 `trigger_outcomes[].status = queued`，且**每条评论各排一个 run，不合并**（12 个 queued run）。取消排队 run 后，其评论**不会**并入后续 run（只投递了 STEER-1）；follow-up run 把整个任务从头重做 | JAR-50 |
| 4b | 哪些 runtime 能 steer？ | 源码 `server/pkg/agent/version.go` `SupportsTaskSupplement`：仅 `pi ≥1.4.0`、`codex ≥0.100.0`、`claude ≥2.1.110`、`grok ≥1.0.14`、`kiro ≥2.33.0`、`cursor ≥2026.10.14`，且 run 必须挂在 issue 上；**opencode、dsh 等不支持**。daemon 在 task start 时协商 `DaemonCapabilityTaskSupplementV1`，run 上暴露为 `can_supplement` | 源码 |
| 7 | WebSocket 事件流 | **可用**。`wss://api.multica.ai/ws?workspace_id=<W>`，首帧 `{"type":"auth","payload":{"token":"mul_…"}}` → `auth_ack`（约 1 s）。不带 Origin 头即可（服务端对无 Origin 放行）。工作区作用域自动订阅，收到 `issue:created`、`task:queued/dispatch/running/progress/usage/completed`、`comment:created`（含完整评论正文、`author_type`、`parent_id`）、`agent:status`、`activity:created`、`inbox:new` 等；每帧带 `event_id`（ULID，可去重）。`task:message` 需发 `{"type":"subscribe","payload":{"scope":"task","id":<run>}}`，回 `subscribe_ack` 后即推送 run 内消息（带 `seq`）。`task:completed` 只有 `task_id/issue_id/agent_id/status`，result 要 REST 拉。应用层可发 `{"type":"ping"}` 收 `pong`；不发也能保持空闲连接（服务端 54 s 发协议级 ping，Node 自动回 pong，空闲 200 s 未断）。事件延迟：建 issue → `task:queued` 同一秒，评论 → `comment:created` < 1 s | JAR-51，`ws_probe.mjs` |

**对实现的影响（`deliver` 规则）**：

1. issue 上有 running run 且 `can_supplement = true`：`--steer <run>` 发送，检查返回的 `trigger_outcomes`，`steered` 才算投递；返回 `queued` 等则按 3 处理。
2. issue 上已有 queued run：**不发评论**，等它开始或结束后再发（否则多排一个 run）。
3. 否则：把 Ticket 的全部未投递 Input 合并成**一条**评论发出，记下评论 id；该 id 出现在某 run 的 `trigger_comment_id` / `delivered_comment_ids` 时调 `inputsDelivered`，该 run 进入 running 时调 `inputsRead`。
4. 永不取消承载未投递 Input 的 queued run（会丢消息）；`stop` 取消 run 时，对应 Input 视为未投递。
5. 每个 run 都是完整的一轮 turn，Worker 需要从 issue 描述 + 评论历史 + git 分支重建上下文；描述里的协议说明要强调「先读最新评论、不要重做已完成的步骤」。

## 8. 实现状态（v1，2026-11-05）

代码：`src/executor/multica.ts`（Executor 本体与 `multicaConfigFromEnv`）、`multica-api.ts`（REST）、`multica-events.ts`（WebSocket）、
`multica-protocol.ts`（评论协议，纯函数）；测试 `tests/multica-*.spec.ts`。核心只改了 `src/app/runtime.ts` 的组装处和 `src/app/http.ts` 的一个类型。

**启用**（不设置时仍是上游的 DshExecutor）：

| 环境变量 | 必填 | 说明 |
| --- | --- | --- |
| `SUPERAGENT_EXECUTOR=multica` | 是 | 选择 MulticaExecutor |
| `MULTICA_TOKEN` | 是 | `mul_` 个人 token，只放环境变量（如 `~/.config/superagent.env`），不进设置 |
| `MULTICA_WORKSPACE_ID` | 是 | |
| `MULTICA_AGENT_ID` | 是 | 第一版固定一个 agent（= 一台机器上的一个 runtime） |
| `MULTICA_SERVER_URL` | 否 | 默认 `https://api.multica.ai` |
| `SUPERAGENT_MULTICA_REPOSITORY` | 否 | Worker 克隆的仓库，如 `https://github.com/jaredshuai/superagent-sandbox` |
| `SUPERAGENT_MULTICA_INSTRUCTIONS` | 否 | 追加给每个 Worker 的说明，如「用 `gh-agent` 提交和开 PR」 |

**与第 2 节的差异**：回报防伪第一版不用 HMAC（要求 LLM 在 shell 里算 HMAC 不可靠），改为三重校验：
评论作者必须是配置的 agent（`author_type = agent`、`author_id` 匹配，由 Multica 服务端保证）、块内 `assignment` 必须是当前 Assignment、
`nonce` 必须等于该 Assignment 在 issue 描述（或后续 Assignment 评论）里下发的随机值。旧 Assignment 的回报因 nonce 不同一律拒收。

**行为要点**：

- `start`：首个 Assignment 建 issue（描述 = 协议 + Assignment JSON，触发消息全文内联，因为远端读不了 Ticket 资源），metadata 写 `superagent_ticket`；
  之后的 Assignment（重试等）在同一 issue 发一条后续 Assignment 评论，带新 nonce，并把当时已有的 run 记为基线。
- `deliver`：按第 7.1 节规则；成功发出评论即 `inputsDelivered`，带它的 run 开始运行时 `inputsRead`（steer 成功时立即 `inputsRead`）；
  Multica 返回 `blocked` / `deferred` 等不重试，记 `deliveryFailed`。
- 对账（每个 Ticket 串行）：由 WebSocket 事件触发，每次（重）连接对所有有 issue 的未结束 Ticket 补一次。先处理新的 agent 评论里的回报块，
  再看 run：issue 上不再有活跃 run 且出现新结束的 run（不含本执行器取消的）⇒ `turnEnded`；最后一个是 `failed` 则先记 `failed` 回报。
- `stop`：取消 issue 上所有活跃 run，并记入 `cancelled`，其结束不算 turn 结束。
- 回报被拒的反馈：配置的 agent 发出的回报块被拒时（不是 JSON / 缺字段 / 字段不合 schema、assignment 不是当前的、nonce 不对、
  PR 认领被拒、Engine 抛错），在那条评论下回复一条 `/note superagent: report block N in this comment was refused: <原因>…`，
  原因里出现的 nonce 一律替换成 `<nonce>`。PR 认领被拒时整条回报不记（与 DshExecutor 的 `orchestrator_report` 一致）。
  待发的反馈存在 `refusals` 里，发成功才移除，发失败下次对账再发；发之前先看 issue 上是否已有同一条评论下、同一块编号的反馈，
  有就不再发——所以重连补齐、执行器在「已发出但未存盘」时重启，都不会重复回复。别人（不是配置的 agent）的评论从不解析，也就不回复。
  issue 描述里告诉 Worker：发完回报后列一下评论，看到点名自己那块的 `/note superagent:` 回复就改正后重发。
- 重启：远端 run 不受影响，所以 `resume` 不打断 running 的 Assignment，只重新排队 pending 的启动；连接建立后的补齐会追上错过的事件。

**还没做**：

1. ~~回报被拒只记在 `worker` 健康项里~~：已做，见上「回报被拒的反馈」与第 8.1 节。
2. 多候选 agent 与跨机故障转移（第 4 节）；当前只有一个固定 agent。
3. 线上端到端联调：在服务器上设置上述环境变量，用测试 agent 跑一张真实 Ticket。
4. 第 7 节第 5、6 项（各 runtime 对协议的遵从度、MCP 注入）。

### 8.1 反馈评论会不会引出新 run

**结论：以 `/note` 开头的评论不触发任何 agent，所以反馈评论用 `/note superagent:` 开头。**

- 源码证据（multica `server/internal/handler/comment.go`，本地 checkout `3a0fed5`，2026-11-05）：
  `triggerTasksForComment` 第一步 `if isNoteComment(comment.Content) { return nil }`，`computeCommentAgentTriggers` 开头同样短路；
  `isNoteComment` 判断去掉前导空白后的第一个 token 是否（不区分大小写）等于 `/note`。所以 `/note superagent: …` 不会走
  回复父评论作者（`routeReplyToParentAuthor`，回复 agent 评论本来会触发该 agent）、也不会走 assignee 兜底，trigger_outcomes 为空。
- 对比：不带 `/note` 的成员评论，回复在 agent 评论下会经 `routeReplyToParentAuthor` 触发该 agent，顶层评论会经 `routeAssigneeFallback`
  触发 assignee（第 7.1 节第 1 项实测：3 s 内进入 running）——这正是要避免的多余 run。
- 代价：`/note` 不进 run，Worker 只能主动去看。所以描述里要求 Worker 发完回报后列一次评论；对账由 WebSocket `comment:created`
  推送触发，反馈通常在 1–2 s 内出现。即使这一轮没看到，下一轮 run 开始时读最新评论也会看到。
- **线上实测（2026-11-05，jxccs，multica CLI 0.8.0，api.multica.ai）：确认不触发。** JAR-49 指派给 `failover-test-jxccs`，两个 run 都已 completed、
  没有活跃 run（正是第 7.1 节第 1 项里普通评论 3 s 内就起新 run 的状态）。08:52:35Z 以成员身份在 agent 评论 `01a1a9a4…` 下回复
  `/note superagent: probe for issue #3 …`（评论 `01a1aae8-f246-7f3d-bc3f-22580c916e4a`，`parent_id` 正确）；08:52:59Z 再查 `multica issue runs JAR-49`，
  仍是原来的 2 个 completed run，没有新 run。线上行为与源码一致，不需要退路（让 Worker 识别并忽略 `/note superagent:` 触发的 run）。

