/**
 * The runtime protocol evaluation (#7): runs the same three tasks on Multica agents of different runtimes and scores how
 * each worker kept to the comment protocol. It plays the part of MulticaExecutor with the executor's own protocol code
 * (`issueDescription`, `parseReports`, `refusalNote`, `inputComment`, `planDelivery`): it creates one issue per case,
 * answers report blocks the way the executor does, delivers the follow-up messages, and refuses the first valid report
 * of the pull-request task once on purpose to see whether the worker corrects it.
 *
 *   pnpm exec tsdown scripts/multica-eval/run.ts --no-config -d node_modules/.cache/multica-eval --platform node --format esm --no-dts
 *   node node_modules/.cache/multica-eval/run.mjs --agents claude=<agent id>,codex=<agent id> [--scenarios pr,question,steer] [--out results.json]
 *     [--answer immediate|after-run] [--timeout <seconds per case>]
 *
 * (Bundled first: Node only strips types, and the executor's API client uses parameter properties.) Results and each
 * case's comments, runs and run messages are written to the current directory. Unlike the executor (ADR 0002) it polls
 * every 3 s: it is a short-lived tool, and the polling delays its refusal replies by up to that much.
 *
 * It reads the Multica server, workspace and token from `~/.multica/config.json`, works in the repository of
 * `--repository` (default the sandbox), and leaves the issues, branches and pull requests for the operator to clean up.
 */
import { execFileSync } from 'node:child_process'
import { readFileSync, writeFileSync } from 'node:fs'
import { homedir } from 'node:os'
import { parseArgs } from 'node:util'
import { MulticaApi, type Run } from '../../src/executor/multica-api.ts'
import { ENDED_STATUSES, inputComment, issueDescription, newNonce, parseReports, planDelivery, refusalNote, refusalResend, type Brief }
  from '../../src/executor/multica-protocol.ts'
import { scoreCase, type EvalMessage, type Score } from './score.ts'

type Scenario = 'pr' | 'question' | 'steer'
const { values } = parseArgs({ options: { agents: { type: 'string' }, scenarios: { type: 'string', default: 'pr,question,steer' },
  out: { type: 'string', default: 'multica-eval-results.json' }, repository: { type: 'string', default: 'jaredshuai/superagent-sandbox' },
  timeout: { type: 'string', default: '1500' },
  // `after-run` holds the answer to the question until the asking run ended, as a person who replies minutes later would.
  answer: { type: 'string', default: 'immediate' } } })
if (!values.agents) throw new Error('--agents label=<agent id>,… is required')
const agents = values.agents.split(',').map(pair => pair.split('=') as [string, string])
const scenarios = values.scenarios.split(',') as Scenario[]
const repository = values.repository
const timeoutMs = Number(values.timeout) * 1000

const config = JSON.parse(readFileSync(`${homedir()}/.multica/config.json`, 'utf8')) as { server_url: string; workspace_id: string; token: string }
const connection = { serverUrl: config.server_url, workspaceId: config.workspace_id, token: config.token }
const api = new MulticaApi(connection)
async function runMessages(runId: string): Promise<EvalMessage[]> {
  const url = new URL(`/api/tasks/${encodeURIComponent(runId)}/messages`, connection.serverUrl)
  url.searchParams.set('workspace_id', connection.workspaceId)
  const response = await fetch(url, { headers: { authorization: `Bearer ${connection.token}` } })
  return response.ok ? await response.json() as EvalMessage[] : []
}
const sleep = (ms: number) => new Promise(resolve => setTimeout(resolve, ms))
const log = (tag: string, text: string) => console.log(`${new Date().toISOString().slice(11, 19)} [${tag}] ${text}`)
const ON_PURPOSE = 'refused on purpose by the protocol evaluation (#7), to see whether you post a corrected report; the block itself was valid'

/** What each scenario asks, and the message the harness sends later. */
function task(label: string, scenario: Scenario): { title: string; text: string; later?: string } {
  const file = `evals/${label}-${scenario}.md`
  if (scenario === 'pr') return { title: `Add ${file}`,
    text: `Please add a file \`${file}\` containing the single line \`protocol evaluation (#7) for ${label}\` to the repository, and open a pull request for it.` }
  if (scenario === 'question') return { title: `Record the release codename in ${file}`,
    text: `Please add a file \`${file}\` containing the codename of our next release. I have not written the codename down anywhere, so ask me for it before you change anything. Push the commit to the branch; no pull request is needed.`,
    later: `The codename is \`${label}-heron\`. Go ahead.` }
  return { title: `Add ${file} after a long build`,
    text: `Please add a file \`${file}\` with the line \`one\`. Before you commit, run \`sleep 90\` in the shell (it stands for a long build). Push the commit to the branch; no pull request is needed.`,
    later: `Also add a second line \`two\` to \`${file}\`, in the same commit if you have not committed yet.` }
}

interface Delivery { commentId: string; kind: 'steer' | 'comment'; at: string; receipts?: unknown }
interface CaseResult {
  label: string; scenario: Scenario; agentId: string; issue: string; branch: string; startedAt: string; endedAt: string
  timedOut: boolean; deliveries: Delivery[]; refusalsPosted: number; asked?: boolean; score: Score
  check: Record<string, unknown>
}

/** A read-only `gh` call as the operator, tried three times since a call can fail on the network; empty when all fail. */
function gh(args: string[]): string {
  for (let attempt = 0; attempt < 3; attempt++) {
    try { return execFileSync('gh', args, { encoding: 'utf8', stdio: ['ignore', 'pipe', 'pipe'] }) } catch { /* try again */ }
  }
  return ''
}
function fileOnBranch(path: string, branch: string): string | null {
  const out = gh(['api', `repos/${repository}/contents/${path}?ref=${encodeURIComponent(branch)}`, '--jq', '.content'])
  return out ? Buffer.from(out.replace(/\s/g, ''), 'base64').toString('utf8') : null
}

async function runCase(label: string, agentId: string, scenario: Scenario): Promise<CaseResult> {
  const tag = `${label}-${scenario}-${newNonce().slice(0, 6).toLowerCase().replace(/[^a-z0-9]/g, 'x')}`
  const nonce = newNonce(), assignmentId = `asg-${tag}`, triggerId = `trg-${tag}`, branch = `agent/eval-${tag}`
  const { title, text, later } = task(label, scenario)
  const startedAt = new Date().toISOString()
  /** A Trigger as a worker sees it, like the executor's trigger card. */
  const card = (id: string, body: string, at: string) => ({ trigger_id: id, from: 'jaredshuai', thread: `github:${repository}#eval`, at, text: body })
  const assignment = JSON.stringify({ ticket_id: `eval-${tag}`, title,
    assignment: 'Do what the trigger asks in the repository named above. This is a protocol evaluation run (#7) on a sandbox repository.',
    trigger: card(triggerId, text, startedAt) }, null, 2)
  const brief: Brief = { ticketId: `eval-${tag}`, assignmentId, nonce, title, assignment, repository: `https://github.com/${repository}`, branch,
    instructions: 'Use `git-agent` for every git command and `gh-agent` for every gh command, so commits and pull requests come from the agent account.' }
  const issue = await api.createIssue({ title: `[eval #7] ${label}: ${title}`, description: issueDescription(brief), agentId })
  const name = issue.identifier ?? issue.id
  log(tag, `created ${name}`)

  const seen = new Set<string>(), refusedOnPurpose: { commentId: string; index: number }[] = [], deliveries: Delivery[] = []
  /** The last refused block with no report accepted after it, told again as a plain comment once the runs ended, as the executor does. */
  let unanswered: { commentId: string; index: number; reason: string } | undefined, retried = false
  let refusalsPosted = 0, asked: boolean | undefined, laterDue = false, laterSent = false, idleSince: number | undefined, timedOut = false
  const deadline = Date.now() + timeoutMs
  for (;;) {
    const [comments, runs] = await Promise.all([api.comments(issue.id), api.runs(issue.id)])
    for (const comment of comments.sort((a, b) => a.created_at.localeCompare(b.created_at))) {
      if (seen.has(comment.id) || comment.author_type !== 'agent' || comment.author_id !== agentId) continue
      seen.add(comment.id)
      for (const [index, parsed] of parseReports(comment.content, { assignment: assignmentId, nonce }).entries()) {
        let reason: string | undefined
        if (!parsed.ok) reason = parsed.error
        else if (scenario === 'pr' && parsed.block.tool === 'report' && !refusedOnPurpose.length) {
          reason = ON_PURPOSE; refusedOnPurpose.push({ commentId: comment.id, index })
        } else if (parsed.block.tool === 'report') {
          unanswered = undefined
          log(tag, `report ${parsed.block.state}: ${parsed.block.summary.slice(0, 100)}`)
          if (scenario === 'question' && asked === undefined) {
            asked = parsed.block.state === 'blocked' || (parsed.block.state === 'progress' && !!parsed.block.human_action)
            laterDue = asked && values.answer !== 'after-run'
          }
        } else log(tag, `${parsed.block.tool}`)
        if (reason) {
          await api.addComment(issue.id, refusalNote(index, reason, nonce), { parentId: comment.id })
          refusalsPosted++
          unanswered = { commentId: comment.id, index, reason }
          log(tag, `refused block ${index + 1}: ${reason.slice(0, 60)}`)
        }
      }
    }
    // The mid-run message goes in once the first run has been running for a while, so it lands while the worker is busy.
    const firstRun = runs.at(-1) // runs are newest first
    if (scenario === 'steer' && !laterSent && firstRun?.status === 'running' && firstRun.started_at && Date.now() - Date.parse(firstRun.started_at) > 25_000) laterDue = true
    if (laterDue && !laterSent && later) {
      const plan = planDelivery(runs.map(run => ({ id: run.id, status: run.status, canSupplement: run.can_supplement ?? undefined })))
      if (plan.kind !== 'wait') {
        const message = JSON.stringify({ ...card(`${triggerId}-2`, later, new Date().toISOString()), replies_to: { trigger_id: triggerId, preview: text.slice(0, 160) } })
        const posted = await api.addComment(issue.id, inputComment([message]), plan.kind === 'steer' ? { steer: [plan.runId] } : {})
        deliveries.push({ commentId: posted.id, kind: plan.kind, at: new Date().toISOString(), receipts: posted.supplements ?? undefined })
        laterSent = true
        log(tag, `delivered the later message by ${plan.kind}`)
      }
    }
    // A worker that asked in plain text, or not at all, still gets the answer once its run ended: a person reading the
    // issue would reply, and the follow-up run is where checkout reuse and repeated steps show.
    const allEnded = runs.length > 0 && runs.every(run => ENDED_STATUSES.has(run.status))
    if (scenario === 'question' && asked === undefined && allEnded) { asked = false; laterDue = true }
    if (scenario === 'question' && asked && allEnded) laterDue = true
    if (allEnded && unanswered && !retried) {
      await api.addComment(issue.id, refusalResend(unanswered.index, unanswered.reason, nonce, unanswered.commentId), { parentId: unanswered.commentId })
      retried = true
      log(tag, `posted the refusal of block ${unanswered.index + 1} again as a plain comment`)
    }
    const idle = allEnded && (!laterDue || laterSent) && !(unanswered && !retried)
    idleSince = idle ? idleSince ?? Date.now() : undefined
    if (idleSince && Date.now() - idleSince > 45_000) break
    if (Date.now() > deadline) {
      timedOut = true
      for (const run of runs) if (!ENDED_STATUSES.has(run.status)) await api.cancelRun(run.id).catch(() => {})
      break
    }
    await sleep(3000)
  }

  const [comments, runs, receipts] = await Promise.all([api.comments(issue.id), api.runs(issue.id), api.steerReceipts(issue.id)])
  for (const delivery of deliveries) if (receipts.has(delivery.commentId)) delivery.receipts = receipts.get(delivery.commentId)
  const ordered: Run[] = [...runs].reverse()
  const messages = Object.fromEntries(await Promise.all(ordered.map(async run => [run.id, await runMessages(run.id)] as const)))
  writeFileSync(`multica-eval-${tag}.messages.json`, JSON.stringify({ comments, runs: ordered, messages }, null, 2))
  const score = scoreCase({ agentId, assignment: assignmentId, nonce, comments, runs: ordered, messages, refusedOnPurpose })
  const check: Record<string, unknown> = { runs: ordered.map(run => `${run.id.slice(0, 8)} ${run.status}${run.error ? ` (${run.error.slice(0, 80)})` : ''}`) }
  if (scenario === 'pr') check.pulls = gh(['pr', 'list', '-R', repository, '--head', branch, '--state', 'all', '--json', 'number,author,state']).trim()
  else check.file = fileOnBranch(`evals/${label}-${scenario}.md`, branch)
  log(tag, `done${timedOut ? ' (timed out)' : ''}: ${JSON.stringify(score)}`)
  return { label, scenario, agentId, issue: name, branch, startedAt, endedAt: new Date().toISOString(), timedOut, deliveries, refusalsPosted,
    ...asked === undefined ? {} : { asked }, score, check }
}

// Each agent takes its cases one at a time, so a case's runs do not queue behind another's; agents run side by side.
const results: CaseResult[] = []
await Promise.all(agents.map(async ([label, agentId]) => {
  for (const scenario of scenarios) {
    try { results.push(await runCase(label, agentId, scenario)) } catch (error) { log(`${label}-${scenario}`, `failed: ${String(error)}`) }
    writeFileSync(values.out, JSON.stringify(results, null, 2))
  }
}))
writeFileSync(values.out, JSON.stringify(results, null, 2))
console.log(`wrote ${values.out}`)
