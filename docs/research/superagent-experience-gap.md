# Superagent 体验对照：设计目标、本 fork 与证据边界

**范围**：只读核查本地 fork（`b229242`）、[已合并的 fork PR #21](https://github.com/jaredshuai/dsh-superagent-prototype/pull/21)、公开文档及已有实测记录。本轮未访问生产；日期沿用各来源。「交接记录」标注不能从仓库或 PR 独立核实的细节；「线上实测」指已有联调、冒烟或演练，不是本次复测，也不代表日用。

## 文章的目标与本 fork 的优先级

[DeepSeek Harness 团队发布的文章](https://x.com/DeepSeekHarness/status/2116857257245987236)（本次据[作者英文发布版的存档](https://threadnavigator.com/thread/2116857257245987236/)核对）是设计探索，不是已发布功能清单。[公开原型 README](https://github.com/deepseek-ai/dsh-superagent-prototype)也明确说它与内部实现不同。文章的判断标准是：用户不必管理会话，只在需要决策或验收时介入；这不等于连接了多少个平台。§7 是合成负载实验，不与本 fork 的单次冒烟耗时横比。

1. **连续交互（§§1–3）**：文章举例在 IM 选 A、在 GitHub PR 改 B，Agent 能承接、继续改原 PR。当前最值得缩小的差距是把同票跟进和交付回执验证到日常可用；已有订阅和 attach 机制，不应在观察前先重写 Router。飞书按聊天与根消息标识 Thread；新发一条无引用的私聊消息并不是同一个 Thread，不能把它当成必然续票（[`feishuThreadId`](../../src/sources/feishu-trigger.ts)）。
2. **异步、多任务、少打扰（§§2、4）**：文章目标是长任务运行时仍能接新指令、并行工作，不刷内部进度，只把结论或具体问题带回原渠道。当前工作站执行与云端交互已分层，原型有排队、通知和 Attention，但缺少当前部署的人类追问、问答续跑及多任务日用证据。Multica 可用原 run 的 steer 能力才有及时插话，否则发起后续 run。
3. **常驻与恢复（§§5–6）**：文章将可替换的云端计算、持久状态、真实执行环境分开，并用独立 Watchdog 判断活性和语义进展。当前是云端编排、工作站编码，状态落在部署的本机目录；跨工作站主要接续 Ticket、PR 与已推送分支，并非迁移原运行现场。先做到掉线可解释、恢复可核对，不需要为此把编码搬上云，也不用先引入语义 Watchdog。
4. **主动但安静（§3）**：文章举例观察群聊、无人 @ 时先后台复现，确认值得交付才发 PR。本 fork 能配置观察和语义准入，但本部署是否启用、误判是否可控均未知。等有人明确交办的路径好用后，再限定范围观察，审阅 `would accept`，而非全量自动接单。

**推荐下一步**：不重复 PR #21 的冒烟。下一件真实任务若自然出现 PR 审查意见，就看「飞书接单 → GitHub 给修改意见 → 原 PR 更新、没有新开票 → 需要人决定时回原渠道」。如用户还要切回飞书，优先回复原消息线程或附 PR 链接。缺哪一环再修哪一环；用量与新平台接入不列为此阶段前置条件。

## 已线上实测

- **本 fork 交付冒烟（2026-11-18）**：[fork PR #21](https://github.com/jaredshuai/dsh-superagent-prototype/pull/21) 在当日 02:24 UTC 合并，合并提交为 `b229242`，可直接核对[产物](../mvp-smoke.md)。交接记录称：飞书私聊中普通提问走 `ignore` 并回复，任务走 `accept` 建 Ticket，交给 Multica Worker，约两分钟在正确的 fork 开出 PR；首轮因旧配置投向旧仓库，不计为成功。合并记录能证明 PR 已落地，不能证明合并后 Ticket 收尾或飞书终局通知已送达。
- **同票跨源已有 sandbox 先例（2026-11-05）**：飞书私聊联调看到回复串确认；成功交付的那张票在回复串报告已开 PR，GitHub PR 上的机器人评论再作为 Input 回到该 Ticket，合并后 Ticket 结束。另一个由 GitHub 起票的用例，两条人类追问合成一次投递、一个 run。两者合起来仍不等于「人在飞书和 GitHub 来回追同一票」已验证。11-11 的 sandbox 冒烟另验证了机器人反馈进票、不发 `ack`。（[009](../009-multica-executor.md) §§8.2、8.2.2）
- **Multica 接续与改派有分项实测（2026-11-05 至 11-08）**：编排服务重启未打断远端 run，重连对账追上结果，但该 run 因额度失败，不能算重启后成功交付。Claude 的追加消息既实测过进同一 run，也实测过交付前 run 结束、改用普通评论补发。双候选 sandbox 曾刻意冻住首选，约五分钟未认领后改派并完成；这是故障演练，不是日常故障率或连续性证据。（[009](../009-multica-executor.md) §§8.2、8.2.1、8.3）

## 已有机制，但本 fork 这轮部署未验证

| 体验 | 已有证据与未验证项 |
| --- | --- |
| 跨飞书 / GitHub 同票跟进 | 自有 Thread 或主动 attach 的后续交给原 Ticket；仅供上下文的订阅不取得归属，也不唤醒已结束票。已结束但未被人停止的票可由自有 Thread 的人类后续在授权复核后续接。#21 未记录两个入口的人类追问进同一票、跨源答复或结票后唤醒。见 [002：Subscriptions / Resume and stop](../002-authorization.md)、[003：Follow-ups and context Threads](../003-execution.md)。 |
| 通知 / Attention | 面板区分需回答、待审和系统故障；聊天渠道设计为收到一次 PR 终局通知，发信有持久化及退避。Multica 全候选不健康时每个 Assignment 挂一次问题，派发后撤下；这不同于普通 `ack` 或 PR 提醒。没有 #21 的 Attention 提问→人在原渠道回答→同票恢复、或终局通知送达证据；009 §8.4 的问答是脚本投递评估。见 [003](../003-execution.md)、[005](../005-panel.md)、[009](../009-multica-executor.md) §§8、8.4。 |
| 主动观察 | 飞书收全量和 GitHub 观察开关只决定**能看到什么**；GitHub 全量开关默认关，Policy 默认 `observe` 只记录 `would accept`，`auto` 才能按命中规则主动开票。订阅后收到机器人反馈不等于无 @ 主动开票；交接未证明本部署的全量范围、模式或日用行为。见 [002：Accept mode and rules](../002-authorization.md#accept-mode-and-rules)、[005](../005-panel.md#settings-groups)、[006：Settings](../006-deployment.md#settings)。 |
| 并发 / Watchdog | [003 的槽位规则](../003-execution.md#concurrency)给人类追问或状态查询留一个额外槽位，并让唤醒优先于新起票；不等于 Multica 远程 runtime 的并发表现，[009 §8](../009-multica-executor.md)还注明满载排队可能触发改派。[006 的小时巡检](../006-deployment.md#hourly-check)需在独立 Harness profile 配定时：运行故障可重启，源故障或缺配置只报告，尊重人工停机。交接只记健康快照，没有多票饱和、额外槽位或巡检实际触发/静默/恢复证据；Worker 改派不是服务 Watchdog。 |

**设计差异**：Multica 续的是同一张 Ticket 和 Multica issue，重试或改派可换 Assignment；能 steer 才进同一 run，否则靠评论起后续 run，跨机靠已有分支和工单上下文接续，不是迁移原 Harness 会话、后台任务或未推送工作。回报走评论协议，按 issue 注入 MCP 已排除（[009](../009-multica-executor.md) §§7.2、8、8.3）。因此，可确认的是 fork 上一次交付冒烟和 sandbox 的分项实测，**还不能确认跨渠道日用闭环**。
