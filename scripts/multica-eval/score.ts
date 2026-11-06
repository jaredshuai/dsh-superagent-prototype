/**
 * Scores one case of the runtime protocol evaluation (#7): from the comments, runs and run messages a remote worker left
 * on its Multica issue, and what the evaluation harness did there, how well the worker kept to the comment protocol of
 * `src/executor/multica-protocol.ts`. Pure, so the scoring is tested apart from the live runs.
 */
import { parseReports, type ReportBlock } from '../../src/executor/multica-protocol.ts'

export interface EvalComment {
  id: string; author_type: string; author_id?: string | null; content: string; parent_id?: string | null; created_at: string
  source_task_id?: string | null
}
export interface EvalRun { id: string; status: string; created_at?: string | null; started_at?: string | null; completed_at?: string | null }
export interface EvalMessage { seq: number; type: string; call_id?: string; tool?: string; input?: { command?: string } & Record<string, unknown>; output?: string }
export interface EvalCase {
  agentId: string; assignment: string; nonce: string; triggerId: string
  comments: EvalComment[]
  /** Oldest first. */
  runs: EvalRun[]
  messages: Record<string, EvalMessage[]>
  /** Valid report blocks the harness refused on purpose, to see whether the worker corrects them. */
  refusedOnPurpose: { commentId: string; index: number }[]
}
export interface Score {
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
  /** Each refused report block, and whether an accepted report followed it in the same run, a later run, or not at all. */
  /** How each run after the first got to its checkout. */
  checkout: { runId: string; kind: 'reused' | 'clone-collided' | 'cloned' | 'no-git' }[]
  /** Steps that should happen once: pull requests opened (over all runs) and the runs that made a commit. */
  repeats: { prCreates: number; committingRuns: string[] }
  corrections: { commentId: string; index: number; why: string; outcome: 'same-run' | 'later-run' | 'none' }[]
}
const ENDED = new Set(['completed', 'failed', 'cancelled'])

interface Found { comment: EvalComment; index: number; block?: ReportBlock; error?: string }
const own = (c: EvalCase): EvalComment[] => c.comments
  .filter(comment => comment.author_type === 'agent' && comment.author_id === c.agentId)
  .sort((a, b) => a.created_at.localeCompare(b.created_at))
const found = (c: EvalCase): Found[] => own(c).flatMap(comment => parseReports(comment.content, { assignment: c.assignment, nonce: c.nonce })
    .map((parsed, index) => parsed.ok ? { comment, index, block: parsed.block } : { comment, index, error: parsed.error }))

/** The shell commands a run's tool calls carried; runtimes name the field differently (`command`, `cmd`). */
export const commands = (messages: readonly EvalMessage[]): string[] => messages.flatMap(message => {
  if (message.type !== 'tool_use' || !message.input) return []
  const value = message.input.command ?? message.input.cmd
  return typeof value === 'string' ? [value] : Array.isArray(value) ? [value.join(' ')] : []
})
const GIT = String.raw`\b(?:git|git-agent|gh|gh-agent)\s+(?:-C\s+\S+\s+)?`
const CLONE = new RegExp(GIT + String.raw`(?:repo\s+)?clone\b`)
const REUSE = new RegExp(GIT + String.raw`(?:fetch|pull|checkout|switch|status|rev-parse|log)\b`)
const PR_CREATE = new RegExp(GIT + String.raw`pr\s+create\b`)
const COMMIT = new RegExp(GIT + String.raw`commit\b`)
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

export function scoreCase(c: EvalCase): Score {
  const blocks = found(c)
  const ackAt = blocks.findIndex(item => item.block?.tool === 'ack')
  const reportAt = blocks.findIndex(item => item.block?.tool === 'report')
  const onPurpose = (item: Found) => c.refusedOnPurpose.some(refused => refused.commentId === item.comment.id && refused.index === item.index)
  const reports = blocks.filter(item => item.block?.tool === 'report' && !onPurpose(item))
  const unfenced = own(c).filter(comment => !blocks.some(item => item.comment.id === comment.id) && comment.content.includes(c.nonce)).length
  const ended = c.runs.filter(run => ENDED.has(run.status))
  const last = reports.at(-1)?.block
  return { ack: ackAt < 0 ? 'none' : reportAt < 0 || ackAt < reportAt ? 'first' : 'late',
    runs: { ended: ended.length, reported: ended.filter(run => reports.some(item => item.comment.source_task_id === run.id)).length },
    ...last?.tool === 'report' ? { lastState: last.state } : {},
    checkout: c.runs.slice(1).map(run => ({ runId: run.id, kind: checkoutKind(c.messages[run.id] ?? []) })),
    repeats: { prCreates: c.runs.reduce((sum, run) => sum + prCreates(c.messages[run.id] ?? []), 0),
      committingRuns: c.runs.filter(run => commands(c.messages[run.id] ?? []).some(command => COMMIT.test(command))).map(run => run.id) },
    corrections: blocks.flatMap((item, at) => {
      const why = onPurpose(item) ? 'on purpose' : item.error
      if (!why || (item.block && item.block.tool !== 'report')) return []
      const next = reports.find(later => blocks.indexOf(later) > at && later.comment.id !== item.comment.id)
      const outcome = !next ? 'none' as const : next.comment.source_task_id === item.comment.source_task_id ? 'same-run' as const : 'later-run' as const
      return [{ commentId: item.comment.id, index: item.index, why, outcome }]
    }),
    format: { accepted: blocks.filter(item => item.block).length, refused: blocks.flatMap(item => item.error ? [item.error] : []), unfenced } }
}
