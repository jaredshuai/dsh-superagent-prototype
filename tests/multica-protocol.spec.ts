/** The comment protocol between dsh-superagent and remote Multica workers. */
import { describe, expect, it } from 'vitest'
import { inputComment, issueDescription, isRefusalNote, newNonce, parseReports, planDelivery, refusalNote, refusalResend, refusedOutcome, REPORT_FENCE, type Brief }
  from '../src/executor/multica-protocol.ts'

const brief: Brief = { ticketId: 't1', assignmentId: 'a1', nonce: 'n1', title: 'Fix it', assignment: '{"ticket_id":"t1"}',
  repository: 'https://github.com/o/r', branch: 'agent/fix-it' }
const block = (body: object) => '```' + REPORT_FENCE + '\n' + JSON.stringify(body) + '\n```'

describe('report blocks', () => {
  it('accepts a report for the current assignment with its nonce', () => {
    const content = `Done.\n\n${block({ v: 1, assignment: 'a1', nonce: 'n1', tool: 'report', state: 'ready', summary: 'Fixed', pr_number: 3 })}`
    expect(parseReports(content, { assignment: 'a1', nonce: 'n1' }))
      .toEqual([{ ok: true, block: { tool: 'report', state: 'ready', summary: 'Fixed', pr_number: 3 } }])
  })
  it('refuses blocks for another assignment, with a wrong nonce, or that are not JSON', () => {
    const content = [block({ v: 1, assignment: 'a0', nonce: 'n1', tool: 'report', state: 'ready', summary: 'x' }),
      block({ v: 1, assignment: 'a1', nonce: 'guess', tool: 'report', state: 'ready', summary: 'x' }),
      '```' + REPORT_FENCE + '\n{oops\n```'].join('\n\n')
    expect(parseReports(content, { assignment: 'a1', nonce: 'n1' }).map(result => result.ok ? 'ok' : result.error))
      .toEqual(['for assignment a0, not the current one', 'wrong nonce', 'not JSON'])
  })
  it('refuses a block whose request does not match its tool', () => {
    const [result] = parseReports(block({ v: 1, assignment: 'a1', nonce: 'n1', tool: 'report', state: 'done', summary: 'x' }), { assignment: 'a1', nonce: 'n1' })
    expect(result?.ok).toBe(false)
  })
  it('reads ack, handback, follow and unrelated blocks, and ignores text outside blocks', () => {
    const envelope = { v: 1, assignment: 'a1', nonce: 'n1' }
    const content = ['```json\n{"tool":"report"}\n```', block({ ...envelope, tool: 'ack', trigger_id: 'tr1', text: 'On it' }),
      block({ ...envelope, tool: 'handback', trigger_id: 'tr2', reason: 'Other work' }),
      block({ ...envelope, tool: 'follow', thread: 'github:7', reason: 'Related' })].join('\n')
    expect(parseReports(content, { assignment: 'a1', nonce: 'n1' }).map(result => result.ok && result.block.tool)).toEqual(['ack', 'handback', 'follow'])
  })
})

describe('issue description', () => {
  it('names the branch, the envelope to report with, and carries the assignment as data', () => {
    const text = issueDescription(brief)
    expect(text).toContain('`agent/fix-it`')
    expect(text).toContain('"assignment":"a1","nonce":"n1"')
    expect(text.endsWith('```json\n{"ticket_id":"t1"}\n```')).toBe(true)
  })
  it('makes acknowledging each new trigger the first step, before any other work', () => {
    const text = issueDescription(brief)
    const rule = text.split('\n').find(line => /`ack`/.test(line) && /before any other step/i.test(line))
    expect(rule).toBeDefined()
    // It comes before the reporting examples, where a worker reads it before it starts.
    expect(text.indexOf(rule!)).toBeLessThan(text.indexOf('```superagent-report'))
  })
  it('makes fresh nonces', () => { expect(newNonce()).not.toBe(newNonce()) })
  it('puts the inputs of one delivery in one comment', () => {
    expect(inputComment(['{"trigger_id":"x"}', 'text'])).toMatch(/```superagent-input\n\{"trigger_id":"x"\}\ntext\n```$/)
  })
})

describe('delivery plan', () => {
  it('waits while a run waits to start, since each comment would queue a run of its own', () => {
    expect(planDelivery([{ id: 'r1', status: 'running', canSupplement: true }, { id: 'r2', status: 'queued' }])).toEqual({ kind: 'wait' })
  })
  it('steers a running run that accepts more messages', () => {
    expect(planDelivery([{ id: 'r0', status: 'completed' }, { id: 'r1', status: 'running', canSupplement: true }])).toEqual({ kind: 'steer', runId: 'r1' })
  })
  it('comments when no run is active, or the running one cannot be steered', () => {
    expect(planDelivery([{ id: 'r0', status: 'completed' }])).toEqual({ kind: 'comment' })
    expect(planDelivery([{ id: 'r1', status: 'running' }])).toEqual({ kind: 'comment' })
  })
  it('reads whether a comment reached the agent from its trigger outcomes', () => {
    expect(refusedOutcome([{ status: 'queued', target_id: 'ag' }], 'ag')).toBeUndefined()
    expect(refusedOutcome([{ status: 'blocked', reason_code: 'agent_paused', target_id: 'ag' }], 'ag')).toBe('blocked (agent_paused)')
    // Multica lists only agents the comment @mentions; a plain comment reaches the assignee with no outcome at all (live, #5).
    expect(refusedOutcome([], 'ag')).toBeUndefined()
    expect(refusedOutcome(undefined, 'ag')).toBeUndefined()
    expect(refusedOutcome([{ status: 'queued', target_id: 'someone-else' }], 'ag')).toBeUndefined()
  })
})

describe('refusal notes', () => {
  it('start with /note so Multica starts no run, name the block, and never carry the nonce', () => {
    const note = refusalNote(1, 'the engine said n1 is wrong', 'n1')
    expect(note.startsWith('/note ')).toBe(true)
    expect(note).toContain('report block 2')
    expect(note).toContain('the engine said <nonce> is wrong')
    expect(note).not.toContain('n1')
  })
  it('are recognised per block, so each refused block is answered once', () => {
    const note = refusalNote(0, 'not JSON', 'n1')
    expect(isRefusalNote(note, 0)).toBe(true)
    expect(isRefusalNote(note, 1)).toBe(false)
    expect(isRefusalNote('/note something else', 0)).toBe(false)
  })
  it('are posted again as a plain comment, which starts a run, naming the comment and block and never the nonce', () => {
    const retry = refusalResend(1, 'the engine said n1 is wrong', 'n1', 'k7')
    expect(retry.startsWith('/')).toBe(false)
    expect(retry).toContain('report block 2 in comment k7')
    expect(retry).toContain('the engine said <nonce> is wrong')
    expect(retry).not.toContain('n1')
    expect(retry).toMatch(/post the report again/)
  })
  it('are explained to the worker in the issue description', () => {
    expect(issueDescription(brief)).toContain('/note superagent:')
  })
})
