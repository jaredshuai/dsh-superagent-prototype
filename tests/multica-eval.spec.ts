/** Scoring one protocol-evaluation case: how a remote worker kept to the comment protocol, from what it left on its issue. */
import { describe, expect, it } from 'vitest'
import { REPORT_FENCE } from '../src/executor/multica-protocol.ts'
import { scoreCase, type EvalCase, type EvalComment } from '../scripts/multica-eval/score.ts'

const AGENT = 'agent-1'
const envelope = { v: 1, assignment: 'a1', nonce: 'n1' }
const block = (body: object) => '```' + REPORT_FENCE + '\n' + JSON.stringify({ ...envelope, ...body }) + '\n```'
let clock = 0
const comment = (content: string, extra: Partial<EvalComment> = {}): EvalComment =>
  ({ id: `c${++clock}`, author_type: 'agent', author_id: AGENT, content, created_at: new Date(Date.UTC(2026, 10, 6, 0, 0, clock)).toISOString(),
    source_task_id: 'r1', ...extra })
const ack = (extra?: Partial<EvalComment>) => comment(block({ tool: 'ack', trigger_id: 't1', text: 'On it.' }), extra)
const report = (state = 'ready', extra?: Partial<EvalComment>) => comment(block({ tool: 'report', state, summary: 'Done.' }), extra)
const evalCase = (comments: EvalComment[], more: Partial<EvalCase> = {}): EvalCase => ({ agentId: AGENT, assignment: 'a1', nonce: 'n1',
  triggerId: 't1', comments, runs: [{ id: 'r1', status: 'completed' }], messages: {}, refusedOnPurpose: [], ...more })

describe('acknowledgement', () => {
  it('counts an ack posted before the first report as acknowledging first', () => {
    expect(scoreCase(evalCase([ack(), report()])).ack).toBe('first')
  })
})

describe('final report', () => {
  it('counts the ended runs that posted an accepted report, by the run each comment came from', () => {
    const runs = [{ id: 'r1', status: 'completed' }, { id: 'r2', status: 'completed' }, { id: 'r3', status: 'running' }]
    const score = scoreCase(evalCase([ack(), report('blocked'), comment('Working on it, no block.', { source_task_id: 'r2' }),
      report('ready', { source_task_id: 'r3' })], { runs }))
    expect(score.runs).toEqual({ ended: 2, reported: 1 })
    expect(score.lastState).toBe('ready')
  })
})

describe('block format', () => {
  it('lists the refused blocks and the report attempts outside a superagent-report fence', () => {
    const wrongNonce = comment('```' + REPORT_FENCE + '\n' + JSON.stringify({ ...envelope, nonce: 'guess', tool: 'report', state: 'ready', summary: 'x' }) + '\n```')
    const asJson = comment('```json\n' + JSON.stringify({ ...envelope, tool: 'report', state: 'ready', summary: 'x' }) + '\n```')
    const score = scoreCase(evalCase([wrongNonce, asJson, comment('Plain progress note.'), report()]))
    expect(score.format).toEqual({ accepted: 1, refused: ['wrong nonce'], unfenced: 1 })
  })
  it('does not count a block refused on purpose as reported', () => {
    const refused = report()
    const score = scoreCase(evalCase([refused], { refusedOnPurpose: [{ commentId: refused.id, index: 0 }] }))
    expect(score.runs).toEqual({ ended: 1, reported: 0 })
    expect(score.lastState).toBeUndefined()
    expect(score.format.accepted).toBe(1)
  })
})

describe('correction after a refusal', () => {
  it('tells whether a refused report was posted again, in the same run or a later one', () => {
    const first = report(), natural = comment(block({ tool: 'report', state: 'done', summary: 'x' }), { source_task_id: 'r2' })
    const score = scoreCase(evalCase([first, report('ready', { source_task_id: 'r1' }), natural, report('ready', { source_task_id: 'r3' })],
      { refusedOnPurpose: [{ commentId: first.id, index: 0 }], runs: [{ id: 'r1', status: 'completed' }, { id: 'r2', status: 'completed' },
        { id: 'r3', status: 'completed' }] }))
    expect(score.corrections.map(item => item.outcome)).toEqual(['same-run', 'later-run'])
    expect(score.corrections[0]?.why).toBe('on purpose')
  })
  it('records a refusal that no later report answered', () => {
    const only = report()
    expect(scoreCase(evalCase([only], { refusedOnPurpose: [{ commentId: only.id, index: 0 }] })).corrections)
      .toEqual([{ commentId: only.id, index: 0, why: 'on purpose', outcome: 'none' }])
  })
})

describe('checkout in later runs', () => {
  const use = (command: string, seq: number) => ({ seq, type: 'tool_use', tool: 'Bash', input: { command } })
  const result = (output: string, seq: number) => ({ seq, type: 'tool_result', output })
  it('tells a reused checkout from a clone that ran into the earlier one, a fresh clone, and no git at all', () => {
    const runs = ['r1', 'r2', 'r3', 'r4', 'r5'].map(id => ({ id, status: 'completed' }))
    const messages = {
      r1: [use('git-agent clone https://github.com/o/r work', 1)],
      r2: [use('cd work && git-agent fetch && git-agent checkout agent/x', 1)],
      r3: [use('git-agent clone https://github.com/o/r work', 1),
        result("fatal: destination path 'work' already exists and is not an empty directory.", 2), use('cd work && git-agent fetch', 3)],
      r4: [use('gh-agent repo clone o/r other', 1)],
      r5: [{ seq: 1, type: 'tool_use', tool: 'exec_command', input: { cmd: 'multica issue comment list x' } }],
    }
    expect(scoreCase(evalCase([], { runs, messages })).checkout)
      .toEqual([{ runId: 'r2', kind: 'reused' }, { runId: 'r3', kind: 'clone-collided' }, { runId: 'r4', kind: 'cloned' }, { runId: 'r5', kind: 'no-git' }])
  })
})

describe('repeated steps', () => {
  it('counts pull requests opened and the runs that committed, across runs', () => {
    const use = (command: string, seq: number) => ({ seq, type: 'tool_use', tool: 'Bash', input: { command } })
    const runs = [{ id: 'r1', status: 'completed' }, { id: 'r2', status: 'completed' }]
    const messages = { r1: [use('git-agent commit -m "Add file"', 1), use('gh-agent pr create --fill', 2)],
      r2: [use('git-agent add . && git-agent commit -m "Add file again"', 1), use('gh-agent pr create --fill', 2), use('gh-agent pr list', 3)] }
    expect(scoreCase(evalCase([], { runs, messages })).repeats).toEqual({ prCreates: 2, committingRuns: ['r1', 'r2'] })
  })
  it('does not count a pull request creation that failed and was retried as a second one', () => {
    const messages = { r1: [{ seq: 1, type: 'tool_use', call_id: 'k1', input: { command: 'git-agent push && gh-agent pr create --fill' } },
      { seq: 2, type: 'tool_result', call_id: 'k1', output: 'Exit code 1\nTLS handshake timeout' },
      { seq: 3, type: 'tool_use', call_id: 'k2', input: { command: 'gh-agent pr create --fill' } },
      { seq: 4, type: 'tool_result', call_id: 'k2', output: 'https://github.com/o/r/pull/11' }] }
    expect(scoreCase(evalCase([], { messages })).repeats.prCreates).toBe(1)
  })
})
