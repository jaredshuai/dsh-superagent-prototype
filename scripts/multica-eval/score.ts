/**
 * Scores one case of the runtime protocol evaluation (#7): from the comments, runs and run messages a remote worker left
 * on its Multica issue, and what the evaluation harness did there, how well the worker kept to the comment protocol of
 * `src/executor/multica-protocol.ts`. Pure, so the scoring is tested apart from the live runs.
 */
import { ENDED_STATUSES, parseReports, type ReportBlock } from '../../src/executor/multica-protocol.ts'

export interface EvalComment {
  id: string; author_type: string; author_id?: string | null; content: string; parent_id?: string | null; created_at: string
  source_task_id?: string | null
}
export interface EvalRun { id: string; status: string; created_at?: string | null; started_at?: string | null; completed_at?: string | null }
export interface EvalMessage {
  seq: number; type: string; created_at?: string; call_id?: string; tool?: string; input?: { command?: string } & Record<string, unknown>
  output?: string
}
export interface EvalCase {
  agentId: string; assignment: string; nonce: string
  comments: EvalComment[]
  /** Oldest first. */
  runs: EvalRun[]
  messages: Record<string, EvalMessage[]>
  /** Valid report blocks the harness refused on purpose, to see whether the worker corrects them. */
  refusedOnPurpose: { commentId: string; index: number }[]
}
export interface Score {
  /** Whether an accepted `ack` came before the worker changed anything (its first commit, push or pull request) and its first report. */
  ack: 'first' | 'late' | 'none'
  /** Ended runs, and how many of them posted an accepted report. */
  runs: { ended: number; reported: number }
  /** The state of the last accepted report, if any. */
  lastState?: string
  /**
   * Blocks Multica-side validation accepted (including those refused on purpose), why the others were refused, and comments
   * that look like a report (a JSON object with the envelope's nonce) but carry no `superagent-report` block.
   */
  format: { accepted: number; refused: string[]; unfenced: number }
  /** How each run after the first got to its checkout. */
  checkout: { runId: string; kind: 'reused' | 'clone-collided' | 'cloned' | 'no-git' }[]
  /** Steps that should happen once: pull requests opened (over all runs) and the runs that made a commit. */
  repeats: { prCreates: number; committingRuns: string[] }
  /** Each refused report block, and whether an accepted report followed it in the same run, a later run, or not at all. */
  corrections: { commentId: string; index: number; why: string; outcome: 'same-run' | 'later-run' | 'none' }[]
}
/** A report block in one of the worker's comments: accepted with its request, or refused with why. */
interface Found { comment: EvalComment; index: number; block?: ReportBlock; error?: string }
/** The worker's own comments, oldest first: only these can carry its reports. */
const workerComments = (c: EvalCase): EvalComment[] => c.comments
  .filter(comment => comment.author_type === 'agent' && comment.author_id === c.agentId)
  .sort((a, b) => a.created_at.localeCompare(b.created_at))
const reportBlocks = (c: EvalCase): Found[] => workerComments(c).flatMap(comment => parseReports(comment.content, { assignment: c.assignment, nonce: c.nonce })
    .map((parsed, index) => parsed.ok ? { comment, index, block: parsed.block } : { comment, index, error: parsed.error }))

/** The shell commands a run's tool calls carried; runtimes name the field differently (`command`, `cmd`). */
const commands = (messages: readonly EvalMessage[]): string[] => messages.flatMap(message => {
  if (message.type !== 'tool_use' || !message.input) return []
  const value = message.input.command ?? message.input.cmd
  return typeof value === 'string' ? [value] : Array.isArray(value) ? [value.join(' ')] : []
})
const GIT = String.raw`\b(?:git|git-agent|gh|gh-agent)\s+(?:-C\s+\S+\s+)?`
const CLONE = new RegExp(GIT + String.raw`(?:repo\s+)?clone\b`)
const REUSE = new RegExp(GIT + String.raw`(?:fetch|pull|checkout|switch|status|rev-parse|log)\b`)
const PR_CREATE = new RegExp(GIT + String.raw`pr\s+create\b`)
const COMMIT = new RegExp(GIT + String.raw`commit\b`)
const PUSH = new RegExp(GIT + String.raw`push\b`)
/**
 * Pull requests a case's runs opened: `pr create` calls whose result names a pull request, or that have no result on
 * record. A creation that failed and was tried again is one pull request, not a repeated step.
 */
function prCreates(messages: readonly EvalMessage[]): number {
  return messages.filter(message => {
    if (message.type !== 'tool_use' || !commands([message]).some(command => PR_CREATE.test(command))) return false
    const result = messages.find(other => other.type === 'tool_result' && message.call_id && other.call_id === message.call_id)
    return !result || /\/pull\/\d+/.test(result.output ?? '')
  }).length
}
function checkoutKind(messages: readonly EvalMessage[]): Score['checkout'][number]['kind'] {
  if (messages.some(message => message.type === 'tool_result' && /already exists and is not an empty directory/.test(message.output ?? ''))) return 'clone-collided'
  const run = commands(messages)
  return run.some(command => CLONE.test(command)) ? 'cloned' : run.some(command => REUSE.test(command)) ? 'reused' : 'no-git'
}

/** When the worker first changed something: its first commit, push or pull request, over all runs. */
function firstChange(c: EvalCase): string | undefined {
  return c.runs.flatMap(run => c.messages[run.id] ?? []).filter(message => message.created_at
    && commands([message]).some(command => COMMIT.test(command) || PUSH.test(command) || PR_CREATE.test(command)))
    .map(message => message.created_at!).sort()[0]
}
function ackScore(c: EvalCase, blocks: readonly Found[]): Score['ack'] {
  const ack = blocks.find(item => item.block?.tool === 'ack')
  if (!ack) return 'none'
  const report = blocks.find(item => item.block?.tool === 'report'), change = firstChange(c)
  const beforeReport = !report || blocks.indexOf(ack) < blocks.indexOf(report)
  return beforeReport && (!change || ack.comment.created_at < change) ? 'first' : 'late'
}
/** Each refused report block, and the first accepted report after it, if any. */
function corrections(blocks: readonly Found[], reports: readonly Found[], onPurpose: (item: Found) => boolean): Score['corrections'] {
  return blocks.flatMap((item, at) => {
    const why = onPurpose(item) ? 'on purpose' : item.error
    if (!why || (item.block && item.block.tool !== 'report')) return []
    const next = reports.find(later => blocks.indexOf(later) > at && later.comment.id !== item.comment.id)
    const outcome = !next ? 'none' as const : next.comment.source_task_id === item.comment.source_task_id ? 'same-run' as const : 'later-run' as const
    return [{ commentId: item.comment.id, index: item.index, why, outcome }]
  })
}

/** Scores one case: see `Score` for what each metric means. */
export function scoreCase(c: EvalCase): Score {
  const blocks = reportBlocks(c)
  const onPurpose = (item: Found) => c.refusedOnPurpose.some(refused => refused.commentId === item.comment.id && refused.index === item.index)
  const reports = blocks.filter(item => item.block?.tool === 'report' && !onPurpose(item))
  const unfenced = workerComments(c).filter(comment => !blocks.some(item => item.comment.id === comment.id) && comment.content.includes(c.nonce)).length
  const ended = c.runs.filter(run => ENDED_STATUSES.has(run.status))
  const last = reports.at(-1)?.block
  const runCommands = (runId: string) => commands(c.messages[runId] ?? [])
  return { ack: ackScore(c, blocks),
    runs: { ended: ended.length, reported: ended.filter(run => reports.some(item => item.comment.source_task_id === run.id)).length },
    ...last?.tool === 'report' ? { lastState: last.state } : {},
    checkout: c.runs.slice(1).map(run => ({ runId: run.id, kind: checkoutKind(c.messages[run.id] ?? []) })),
    repeats: { prCreates: c.runs.reduce((sum, run) => sum + prCreates(c.messages[run.id] ?? []), 0),
      committingRuns: c.runs.filter(run => runCommands(run.id).some(command => COMMIT.test(command))).map(run => run.id) },
    corrections: corrections(blocks, reports, onPurpose),
    format: { accepted: blocks.filter(item => item.block).length, refused: blocks.flatMap(item => item.error ? [item.error] : []), unfenced } }
}
