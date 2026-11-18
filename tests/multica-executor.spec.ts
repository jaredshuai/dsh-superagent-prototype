/** MulticaExecutor against a fake Multica API and a recording Engine. */
import { describe, expect, it, vi } from 'vitest'
import type { Assignment, Ticket, TicketOutcome, Trigger } from '../src/core/index.ts'
import type { Comment, PostedComment, Run, SteerReceipt } from '../src/executor/multica-api.ts'
import { MULTICA_KV, MulticaExecutor, type MulticaExecutorHost, type MulticaTicketState } from '../src/executor/multica.ts'
import { REPORT_FENCE } from '../src/executor/multica-protocol.ts'

const AGENT = 'agent-1'
const trigger = { id: 'tr1', text: 'Please fix the README typo', at: '2026-11-05T00:00:00Z', actor: { id: 'jared', source: 'github' }, human: true,
  threads: [{ source: 'github', id: '2' }] } as unknown as Trigger
/** A bot's comment on the ticket's PR, as CodeRabbit's summary on superagent-sandbox PR #26 was (JAR-95). */
const botTrigger = { id: 'tr2', text: 'Walkthrough: adds a note', at: '2026-11-05T00:02:00Z', actor: { id: 'coderabbitai[bot]', source: 'github' }, human: false,
  threads: [{ source: 'github', id: '26' }] } as unknown as Trigger

function ticketWith(assignments: Partial<Assignment>[], extra: Partial<Ticket> = {}): Ticket {
  return { id: 't1', title: 'Fix typo', createdAt: '2026-11-05T00:00:00Z', mandate: { strength: 'explicit', basis: {} },
    origin: { triggerId: 'tr1', triggerIds: ['tr1'] }, execution: { name: 'fix-typo', directory: '/w/fix-typo', assignment: '' },
    assignments: assignments.map((item, index) => ({ id: `a${index + 1}`, state: 'running', createdAt: '2026-11-05T00:00:01Z', ...item })),
    inputs: [], undelivered: [], channels: [], ...extra } as unknown as Ticket
}

/** A WebSocket the test drives: it opens, acknowledges the auth frame, and pushes events. */
class FakeSocket {
  readyState = 1
  sent: string[] = []
  onopen: ((event: unknown) => void) | null = null
  onmessage: ((event: { data: unknown }) => void) | null = null
  onclose: ((event: { code?: number; reason?: string }) => void) | null = null
  onerror: ((event: unknown) => void) | null = null
  send(data: string) { this.sent.push(data) }
  close() { this.readyState = 3; this.onclose?.({ code: 1000 }) }
  push(message: object) { this.onmessage?.({ data: JSON.stringify(message) }) }
}
type AgentRow = { id: string; status: string; runtime_id?: string | null; archived_at?: string | null }
type RuntimeRow = { id: string; status: string }
/** Lets queued promise chains, such as a health refresh and the launches it releases, run to the end. */
const settle = async () => { for (let i = 0; i < 20; i++) await new Promise(resolve => setTimeout(resolve, 0)) }

function setup(initial: Ticket, options: { agents?: string[]; pickHealthy?: boolean; standardOnly?: boolean } = {}) {
  let ticket = initial
  let assignee: string | null = null
  const sockets: FakeSocket[] = []
  const agents: AgentRow[] = []
  const runtimes: RuntimeRow[] = []
  const kv = new Map<string, unknown>()
  const handlers: Record<string, (payload: never) => Promise<void>> = {}
  const enqueued: { kind: string; key: string }[] = []
  const runs: Run[] = []
  const comments: Comment[] = []
  const posted: { content: string; steer?: string[]; parentId?: string }[] = []
  /** Steer receipts by comment id, as the issue's timeline shows them. */
  const receipts = new Map<string, SteerReceipt[]>()
  let outcome = 'queued'
  let postFailures = 0
  /** A fake clock: `advance` moves it on and fires the timers that fall due. */
  let clockNow = Date.parse('2026-11-05T00:00:00Z')
  const timers: { at: number; fn: () => void; cancelled: boolean }[] = []
  const clock = {
    now: () => clockNow,
    after: (ms: number, fn: () => void) => { const timer = { at: clockNow + ms, fn, cancelled: false }; timers.push(timer); return () => { timer.cancelled = true } },
  }
  /** Run messages by run id. */
  const messages = new Map<string, { type: string; content?: string; output?: string }[]>()
  const engine = {
    assignmentReassigned: vi.fn(),
    assignmentStarted: vi.fn(), inputsDelivered: vi.fn(), inputsRead: vi.fn(), turnEnded: vi.fn(), deliveryFailed: vi.fn(),
    report: vi.fn(() => ticket), reply: vi.fn(() => ({ id: 'r', status: 'sent', reused: false })), handback: vi.fn(() => true),
    followForContext: vi.fn(() => true), markUnrelated: vi.fn(),
  }
  let healthNow: { state: string; detail?: string } | undefined
  const health = { get: vi.fn(() => healthNow), set: vi.fn((next: { state: string; detail?: string }) => { healthNow = next }), release: vi.fn() }
  const api = {
    createIssue: vi.fn(async (input: { agentId: string }) => { assignee = input.agentId; return { id: 'issue-1', identifier: 'JAR-1' } }),
    setMetadata: vi.fn(async () => {}),
    findIssues: vi.fn(async () => []),
    issue: vi.fn(async (id: string) => ({ id, assignee_type: assignee ? 'agent' : null, assignee_id: assignee })),
    runs: vi.fn(async () => runs.map(run => ({ ...run }))),
    comments: vi.fn(async () => comments.map(comment => ({ ...comment }))),
    addComment: vi.fn(async (_issue: string, content: string, options: { steer?: string[]; parentId?: string } = {}): Promise<PostedComment> => {
      if (postFailures > 0) { postFailures--; throw new Error('Multica POST failed') }
      posted.push({ content, ...options })
      const comment = { id: `c${posted.length}`, issue_id: 'issue-1', author_type: 'member', author_id: 'operator', content, created_at: '',
        ...options.parentId ? { parent_id: options.parentId } : {} }
      comments.push(comment)
      // As Multica does: a plain comment that steers a run accepting more messages names no outcome, only a receipt; one
      // whose run no longer accepts them is an ordinary comment.
      const steered = runs.find(run => options.steer?.includes(run.id) && run.status === 'running' && run.can_supplement)
      if (steered) {
        receipts.set(comment.id, [{ task_id: steered.id, agent_id: AGENT, status: 'pending' }])
        return { ...comment, supplements: receipts.get(comment.id)!.map(receipt => ({ ...receipt })) }
      }
      return { ...comment, trigger_outcomes: content.startsWith('/note') || outcome === 'none' ? [] : [{ status: outcome, target_id: AGENT }] }
    }),
    steerReceipts: vi.fn(async () => new Map([...receipts].map(([id, list]) => [id, list.map(receipt => ({ ...receipt }))]))),
    cancelRun: vi.fn(async (id: string) => { const run = runs.find(item => item.id === id); if (run) run.status = 'cancelled' }),
    assignIssue: vi.fn(async (_issue: string, agent: string) => { assignee = agent }),
    runMessages: vi.fn(async (id: string) => messages.get(id) ?? []),
    setStatus: vi.fn(async (_issue: string, _status: string) => {}),
    agents: vi.fn(async () => agents.map(agent => ({ ...agent }))),
    runtimes: vi.fn(async () => runtimes.map(runtime => ({ ...runtime }))),
  }
  const host = {
    engine, health, api,
    store: {
      tickets: { get: () => ticket, open: () => ticket.ended ? [] : [ticket] },
      triggers: { get: (id: string) => id === 'tr1' ? trigger : id === 'tr2' ? botTrigger : undefined },
      get: (key: string) => kv.get(key), set: (key: string, value: unknown) => { kv.set(key, value) }, delete: (key: string) => { kv.delete(key) },
      keys: (family: string | { prefix: string }) => [...kv.keys()].filter(key => key.startsWith(typeof family === 'string' ? family : family.prefix)),
      settings: () => ({ policy: { rules: [] }, executor: { directory: '' } }), atomic: <T>(work: () => T) => work(),
    },
    effects: { enqueue: (kind: string, key: string) => { enqueued.push({ kind, key }) },
      pending: (kind: string, key: string) => enqueued.some(row => row.kind === kind && row.key === key),
      handle: (kind: string, run: (payload: never) => Promise<void>) => { handlers[kind] = run } },
    sessions: { relay: vi.fn() },
    machine: { stateDir: '/state' }, lifetime: new AbortController().signal,
    config: { serverUrl: 'https://multica.test', workspaceId: 'w', token: 'mul_x', agents: options.agents ?? [AGENT],
      pickHealthy: options.pickHealthy ?? false, standardOnly: options.standardOnly, repository: 'https://github.com/o/r' },
    socketFactory: () => { const socket = new FakeSocket(); sockets.push(socket); return socket },
    closing: () => false, paused: () => false, clock,
    sourceLink: async () => ({ source_url: 'https://github.com/o/r/issues/2' }), repliedTo: () => undefined, sendReplies: vi.fn(async () => {}),
    claimPull: vi.fn(async (): Promise<string | undefined> => undefined),
  } as unknown as MulticaExecutorHost
  let executor = new MulticaExecutor(host)
  return {
    get executor() { return executor }, engine, api, messages, runs, comments, posted, receipts, enqueued, health, host, agents, runtimes,
    /** Connects the event stream, or completes the reconnection `drop` started, and lets the catch-up finish. */
    connect: async () => {
      if (!sockets.length) executor.install()
      const socket = sockets.at(-1)!
      socket.onopen?.({})
      socket.push({ type: 'auth_ack' })
      await settle()
    },
    /** Drops the event stream and waits out the backoff until it opens a new socket. */
    drop: async () => {
      const count = sockets.length
      sockets.at(-1)!.onclose?.({ code: 1006 })
      await vi.waitFor(() => { expect(sockets.length).toBe(count + 1) }, { timeout: 3000 })
    },
    push: async (event: object) => { sockets.at(-1)!.push(event); await settle() },
    /** Runs the launches queued so far, as the effect queue would. */
    runLaunches: async () => {
      const queued = enqueued.splice(0).filter(item => item.kind === 'launch')
      for (const item of queued) await handlers.launch!({ ticketId: 't1', assignmentId: item.key } as never)
    },
    /** A new executor over the same store and Multica, as after a restart. */
    restart: () => { executor = new MulticaExecutor(host) },
    setState: (next: MulticaTicketState) => { kv.set(MULTICA_KV.ticket('t1'), next); assignee = next.agentId ?? null },
    setAssignee: (next: string | null) => { assignee = next },
    failPosts: (count: number) => { postFailures = count },
    state: () => kv.get(MULTICA_KV.ticket('t1')) as MulticaTicketState | undefined,
    setTicket: (next: Ticket) => { ticket = next },
    setOutcome: (next: string) => { outcome = next },
    /** Moves the fake clock on by `ms`, fires the timers due, and lets what they started finish. */
    advance: async (ms: number) => {
      clockNow += ms
      for (const timer of timers.filter(item => !item.cancelled && item.at <= clockNow)) { timer.cancelled = true; timer.fn() }
      await settle()
    },
    /** The fake clock's time, as an ISO string `ms` from now. */
    at: (ms = 0) => new Date(clockNow + ms).toISOString(),
    launch: (assignmentId = 'a1') => handlers.launch!({ ticketId: 't1', assignmentId } as never),
    deliver: () => handlers.delivery!({ ticketId: 't1' } as never),
    retryReconcile: () => handlers.multicaReconcile!({ ticketId: 't1' } as never),
  }
}
const report = (body: object, nonce: string, assignment = 'a1') =>
  '```' + REPORT_FENCE + '\n' + JSON.stringify({ v: 1, assignment, nonce, ...body }) + '\n```'
const agentComment = (id: string, content: string): Comment =>
  ({ id, issue_id: 'issue-1', author_type: 'agent', author_id: AGENT, content, created_at: `2026-11-05T00:01:${id.slice(-1).padStart(2, '0')}Z` })

describe('MulticaExecutor', () => {
  it('launches an assignment as an issue assigned to the agent, with the protocol and the trigger in full', async () => {
    const h = setup(ticketWith([{ state: 'pending' }]))
    await h.launch()
    const description = (h.api.createIssue.mock.calls[0] as unknown as [{ description: string; agentId: string }])[0]
    expect(description.agentId).toBe(AGENT)
    expect(description.description).toContain('Please fix the README typo')
    expect(description.description).toContain('`agent/fix-typo`')
    expect(h.state()).toMatchObject({ issueId: 'issue-1', assignment: { id: 'a1' } })
    expect(description.description).toContain(`"nonce":"${h.state()!.assignment!.nonce}"`)
    expect(h.api.setMetadata).toHaveBeenCalledWith('issue-1', 'superagent_ticket', 't1')
    expect(h.engine.assignmentStarted).toHaveBeenCalledWith('t1', 'a1')
  })

  it('applies a valid report, ignores a forged one, and ends the turn once when the run ends', async () => {
    const h = setup(ticketWith([{ state: 'pending' }]))
    await h.launch()
    h.setTicket(ticketWith([{}]))
    const nonce = h.state()!.assignment!.nonce
    h.runs.push({ id: 'r1', status: 'running' })
    await h.executor.reconcile('t1')
    expect(h.engine.turnEnded).not.toHaveBeenCalled()
    h.comments.push(agentComment('k1', report({ tool: 'report', state: 'ready', summary: 'Fixed' }, 'forged')),
      agentComment('k2', `Done ${report({ tool: 'report', state: 'ready', summary: 'Fixed', pr_number: 4 }, nonce)}`),
      { ...agentComment('k3', report({ tool: 'report', state: 'dropped', summary: 'x' }, nonce)), author_id: 'someone-else' })
    h.runs[0]!.status = 'completed'
    await h.executor.reconcile('t1')
    expect(h.engine.report).toHaveBeenCalledTimes(1)
    expect(h.engine.report).toHaveBeenCalledWith('t1', { state: 'ready', summary: 'Fixed' })
    expect(h.engine.turnEnded).toHaveBeenCalledTimes(1)
    await h.executor.reconcile('t1')
    expect(h.engine.report).toHaveBeenCalledTimes(1)
    expect(h.engine.turnEnded).toHaveBeenCalledTimes(1)
  })

  it('ends a turn whose run started and ended between two reconciliations', async () => {
    const h = setup(ticketWith([{ state: 'pending' }]))
    await h.launch()
    h.setTicket(ticketWith([{}]))
    h.runs.push({ id: 'r1', status: 'completed' })
    await h.executor.reconcile('t1')
    expect(h.engine.turnEnded).toHaveBeenCalledWith('t1', 'a1')
  })

  it('records a failed run as a failed report before the turn ends', async () => {
    const h = setup(ticketWith([{ state: 'pending' }]))
    await h.launch()
    h.setTicket(ticketWith([{}]))
    h.runs.push({ id: 'r1', status: 'failed', error: 'codebuddy returned an error result without details' })
    await h.executor.reconcile('t1')
    expect(h.engine.report).toHaveBeenCalledWith('t1', expect.objectContaining({ state: 'failed', summary: expect.stringContaining('codebuddy') }))
    expect(h.engine.turnEnded).toHaveBeenCalled()
  })

  it('marks a bot\'s message as automated for the worker, and a person\'s not (#20)', async () => {
    const h = setup(ticketWith([{ state: 'pending' }]))
    await h.launch()
    const description = (h.api.createIssue.mock.calls[0] as unknown as [{ description: string }])[0].description
    const assignment = JSON.parse(description.slice(description.lastIndexOf('```json\n') + 8, description.lastIndexOf('\n```'))) as { trigger: Record<string, unknown> }
    expect(assignment.trigger).toMatchObject({ trigger_id: 'tr1', from: 'jared' })
    expect(assignment.trigger).not.toHaveProperty('automated')
    const inputs = [{ id: 'i1', kind: 'trigger', triggerId: 'tr1', at: '' }, { id: 'i2', kind: 'trigger', triggerId: 'tr2', at: '' }]
    h.setTicket(ticketWith([{}], { inputs, undelivered: ['i1', 'i2'] } as unknown as Partial<Ticket>))
    h.runs.push({ id: 'r1', status: 'running' })
    await h.deliver()
    const lines = h.posted[0]!.content.split('\n').filter(line => line.startsWith('{')).map(line => JSON.parse(line) as Record<string, unknown>)
    expect(lines.map(line => [line.trigger_id, line.from, line.automated])).toEqual([['tr1', 'jared', undefined], ['tr2', 'coderabbitai[bot]', true]])
  })

  it('does not comment while a run waits to start, then delivers every waiting Input in one comment', async () => {
    const h = setup(ticketWith([{ state: 'pending' }]))
    await h.launch()
    const inputs = [{ id: 'i1', kind: 'text', text: 'Also fix the title', at: '' }, { id: 'i2', kind: 'trigger', triggerId: 'tr1', at: '' }]
    h.setTicket(ticketWith([{}], { inputs, undelivered: ['i1', 'i2'] } as unknown as Partial<Ticket>))
    h.runs.push({ id: 'r1', status: 'queued' })
    await h.deliver()
    expect(h.posted).toEqual([])
    h.runs[0]!.status = 'running'
    await h.deliver()
    expect(h.posted).toHaveLength(1)
    expect(h.posted[0]!.content).toContain('Also fix the title')
    expect(h.posted[0]!.content).toContain('Please fix the README typo')
    expect(h.posted[0]!.steer).toBeUndefined()
    expect(h.engine.inputsDelivered).toHaveBeenCalledWith('t1', 'a1', ['i1', 'i2'])
    expect(h.engine.inputsRead).not.toHaveBeenCalled()
    // The follow-up run the comment started takes the Inputs in.
    h.runs.unshift({ id: 'r2', status: 'running', trigger_comment_id: 'c1' })
    h.setTicket(ticketWith([{}], { inputs, undelivered: [] } as unknown as Partial<Ticket>))
    await h.executor.reconcile('t1')
    expect(h.engine.inputsRead).toHaveBeenCalledWith('t1', ['i1', 'i2'])
  })

  it('steers a running run that accepts more messages, which reads them once its daemon hands them over', async () => {
    const h = setup(ticketWith([{ state: 'pending' }]))
    await h.launch()
    h.setTicket(ticketWith([{}], { inputs: [{ id: 'i1', kind: 'text', text: 'Hurry', at: '' }], undelivered: ['i1'] } as unknown as Partial<Ticket>))
    h.runs.push({ id: 'r1', status: 'running', can_supplement: true })
    await h.deliver()
    expect(h.posted[0]!.steer).toEqual(['r1'])
    expect(h.engine.deliveryFailed).not.toHaveBeenCalled()
    expect(h.engine.inputsDelivered).toHaveBeenCalledWith('t1', 'a1', ['i1'])
    expect(h.engine.inputsRead).not.toHaveBeenCalled()
    h.setTicket(ticketWith([{}], { inputs: [{ id: 'i1', kind: 'text', text: 'Hurry', at: '' }], undelivered: [] } as unknown as Partial<Ticket>))
    h.receipts.get('c1')![0]!.status = 'delivered'
    await h.executor.reconcile('t1')
    expect(h.engine.inputsRead).toHaveBeenCalledWith('t1', ['i1'])
    expect(h.posted).toHaveLength(1)
  })
  it('posts a steer again as a plain comment when its run ended before reading it, which Multica does not replay', async () => {
    const h = setup(ticketWith([{ state: 'pending' }]))
    await h.launch()
    h.setTicket(ticketWith([{}], { inputs: [{ id: 'i1', kind: 'text', text: 'Hurry', at: '' }], undelivered: ['i1'] } as unknown as Partial<Ticket>))
    h.runs.push({ id: 'r1', status: 'running', can_supplement: true })
    await h.deliver()
    h.setTicket(ticketWith([{}], { inputs: [{ id: 'i1', kind: 'text', text: 'Hurry', at: '' }], undelivered: [] } as unknown as Partial<Ticket>))
    h.runs[0]!.status = 'completed'
    h.receipts.set('c1', [{ task_id: 'r1', status: 'failed', failure_reason: 'turn_ended' }])
    await h.executor.reconcile('t1')
    expect(h.posted).toHaveLength(2)
    expect(h.posted[1]).toEqual({ content: h.posted[0]!.content })
    expect(h.engine.inputsRead).not.toHaveBeenCalled()
    await h.executor.reconcile('t1')
    expect(h.posted).toHaveLength(2)
    // The run the plain comment started reads it.
    h.runs.unshift({ id: 'r2', status: 'running', trigger_comment_id: 'c2' })
    await h.executor.reconcile('t1')
    expect(h.engine.inputsRead).toHaveBeenCalledWith('t1', ['i1'])
  })
  it('keeps a delivery made while it settles a steer', async () => {
    const h = setup(ticketWith([{ state: 'pending' }]))
    await h.launch()
    const inputs = [{ id: 'i1', kind: 'text', text: 'Hurry', at: '' }, { id: 'i2', kind: 'text', text: 'And the title', at: '' }]
    h.setTicket(ticketWith([{}], { inputs, undelivered: ['i1'] } as unknown as Partial<Ticket>))
    h.runs.push({ id: 'r1', status: 'running', can_supplement: true })
    await h.deliver()
    h.receipts.get('c1')![0]!.status = 'delivered'
    h.setTicket(ticketWith([{}], { inputs, undelivered: ['i2'] } as unknown as Partial<Ticket>))
    // The Engine delivers while the timeline request is out.
    let delivering: Promise<void> | undefined
    const receiptsNow = h.api.steerReceipts.getMockImplementation()!
    h.api.steerReceipts.mockImplementationOnce(async () => { delivering = h.deliver(); await settle(); return receiptsNow() })
    await h.executor.reconcile('t1')
    await delivering
    expect(h.state()!.deliveries.map(delivery => delivery.inputIds)).toEqual([['i1'], ['i2']])
    expect(h.state()!.deliveries[0]).toMatchObject({ read: true })
  })
  it('leaves a steer to the next run when its run stopped accepting messages, as Multica then queues the comment', async () => {
    const h = setup(ticketWith([{ state: 'pending' }]))
    await h.launch()
    h.setTicket(ticketWith([{}], { inputs: [{ id: 'i1', kind: 'text', text: 'Hurry', at: '' }], undelivered: ['i1'] } as unknown as Partial<Ticket>))
    h.runs.push({ id: 'r1', status: 'running', can_supplement: true })
    // The run ends between reading the runs and posting: the comment binds to no run and queues one.
    h.api.addComment.mockImplementationOnce(async (_issue: string, content: string, options: { steer?: string[] } = {}) => {
      h.posted.push({ content, ...options })
      const comment = { id: 'c1', issue_id: 'issue-1', author_type: 'member', author_id: 'operator', content, created_at: '' }
      h.comments.push(comment)
      h.runs[0]!.status = 'completed'
      h.runs.unshift({ id: 'r2', status: 'queued', trigger_comment_id: 'c1' })
      return { ...comment, trigger_outcomes: [] }
    })
    await h.deliver()
    expect(h.engine.deliveryFailed).not.toHaveBeenCalled()
    expect(h.engine.inputsDelivered).toHaveBeenCalledWith('t1', 'a1', ['i1'])
    h.setTicket(ticketWith([{}], { inputs: [{ id: 'i1', kind: 'text', text: 'Hurry', at: '' }], undelivered: [] } as unknown as Partial<Ticket>))
    await h.executor.reconcile('t1')
    expect(h.posted).toHaveLength(1)
    expect(h.engine.inputsRead).not.toHaveBeenCalled()
    h.runs[0]!.status = 'running'
    await h.executor.reconcile('t1')
    expect(h.engine.inputsRead).toHaveBeenCalledWith('t1', ['i1'])
  })

  it('counts a plain comment as delivered though Multica names no outcome for it, as for the assignee (live, #5)', async () => {
    const h = setup(ticketWith([{ state: 'pending' }]))
    await h.launch()
    h.setTicket(ticketWith([{}], { inputs: [{ id: 'i1', kind: 'text', text: 'x', at: '' }], undelivered: ['i1'] } as unknown as Partial<Ticket>))
    h.setOutcome('none')
    await h.deliver()
    expect(h.engine.deliveryFailed).not.toHaveBeenCalled()
    expect(h.engine.inputsDelivered).toHaveBeenCalledWith('t1', 'a1', ['i1'])
  })
  it('posts waiting Inputs once when two deliveries of a ticket run at once', async () => {
    const h = setup(ticketWith([{ state: 'pending' }]))
    await h.launch()
    const waiting = ticketWith([{}], { inputs: [{ id: 'i1', kind: 'text', text: 'x', at: '' }], undelivered: ['i1'] } as unknown as Partial<Ticket>)
    h.setTicket(waiting)
    h.engine.inputsDelivered.mockImplementation(() => { h.setTicket({ ...waiting, undelivered: [] }) })
    await Promise.all([h.deliver(), h.deliver()])
    expect(h.posted.filter(post => post.content.includes('x'))).toHaveLength(1)
  })
  it('does not post again on its own an Input whose delivery Multica refused', async () => {
    const h = setup(ticketWith([{ state: 'pending' }]))
    await h.launch()
    h.setTicket(ticketWith([{}], { inputs: [{ id: 'i1', kind: 'text', text: 'x', at: '' }], undelivered: ['i1'],
      delivery: { error: 'Multica did not take the message: blocked', at: '' } } as unknown as Partial<Ticket>))
    h.enqueued.splice(0)
    await h.executor.reconcile('t1')
    expect(h.enqueued.filter(item => item.kind === 'delivery')).toEqual([])
  })
  it('records a delivery Multica refused instead of retrying it', async () => {
    const h = setup(ticketWith([{ state: 'pending' }]))
    await h.launch()
    h.setTicket(ticketWith([{}], { inputs: [{ id: 'i1', kind: 'text', text: 'x', at: '' }], undelivered: ['i1'] } as unknown as Partial<Ticket>))
    h.setOutcome('blocked')
    await h.deliver()
    expect(h.engine.deliveryFailed).toHaveBeenCalledWith('t1', 'a1', expect.stringContaining('blocked'))
    expect(h.engine.inputsDelivered).not.toHaveBeenCalled()
  })

  it('starts a later assignment on the same issue, and ignores reports and runs of the earlier one', async () => {
    const h = setup(ticketWith([{ state: 'pending' }]))
    await h.launch()
    const first = h.state()!.assignment!.nonce
    h.runs.push({ id: 'r1', status: 'completed' })
    h.setTicket(ticketWith([{ state: 'ended', endedReason: 'retried' }, { state: 'pending' }]))
    await h.launch('a2')
    expect(h.api.createIssue).toHaveBeenCalledTimes(1)
    expect(h.posted[0]!.content).toContain('`a2`')
    h.setTicket(ticketWith([{ state: 'ended' }, {}]))
    h.comments.push(agentComment('k1', report({ tool: 'report', state: 'ready', summary: 'old' }, first)))
    await h.executor.reconcile('t1')
    expect(h.engine.report).not.toHaveBeenCalled()
    expect(h.engine.turnEnded).not.toHaveBeenCalled()
  })

  it('posts a worker acknowledgement as a reply, and cancels active runs on stop', async () => {
    const h = setup(ticketWith([{ state: 'pending' }]))
    await h.launch()
    h.setTicket(ticketWith([{}]))
    h.comments.push(agentComment('k1', report({ tool: 'ack', trigger_id: 'tr1', text: 'On it' }, h.state()!.assignment!.nonce)))
    h.runs.push({ id: 'r1', status: 'running' })
    await h.executor.reconcile('t1')
    expect(h.engine.reply).toHaveBeenCalledWith({ purpose: 'acknowledgement', ticketId: 't1', triggerId: 'tr1', text: 'On it' })
    const stopped = ticketWith([{ state: 'ended' }])
    h.setTicket(stopped)
    h.executor.stop(stopped, stopped.assignments[0]!, 'stopped')
    await vi.waitFor(() => { expect(h.api.cancelRun).toHaveBeenCalledWith('r1') })
    h.runs[0]!.status = 'cancelled'
    await h.executor.reconcile('t1')
    expect(h.engine.turnEnded).not.toHaveBeenCalled()
    expect(h.api.setStatus).not.toHaveBeenCalled()
  })

  describe('closing the issue when its ticket ends (#9)', () => {
    /** A ticket that ended with a run of its issue still `r1Status`, such as a run queued after its last report. */
    async function ending(outcome: TicketOutcome, { by = 'executor', r1Status = 'queued' }: { by?: 'executor' | 'human'; r1Status?: string } = {}) {
      const h = setup(ticketWith([{ state: 'pending' }]))
      await h.launch()
      await h.executor.reconcile('t1')
      h.setTicket(ticketWith([{}]))
      h.runs.push({ id: 'r1', status: r1Status }, { id: 'r0', status: 'completed' })
      const ended = ticketWith([{ state: 'ended' }], { ended: { at: '2026-11-05T01:00:00Z', by, outcome } } as Partial<Ticket>)
      h.setTicket(ended)
      return { h, stop: () => { h.executor.stop(ended, ended.assignments[0]!, outcome) } }
    }
    it.each<[TicketOutcome, string]>([['merged', 'done'], ['completed', 'done'], ['closed', 'cancelled'], ['dropped', 'cancelled'], ['stopped', 'cancelled']])(
      'moves the issue of a ticket that ended %s to %s, after cancelling its active runs', async (outcome, status) => {
        const { h, stop } = await ending(outcome)
        stop()
        await vi.waitFor(() => { expect(h.api.setStatus).toHaveBeenCalledWith('issue-1', status) })
        expect(h.api.cancelRun).toHaveBeenCalledTimes(1)
        expect(h.api.cancelRun.mock.invocationCallOrder[0]).toBeLessThan(h.api.setStatus.mock.invocationCallOrder[0]!)
        expect(h.state()!.cancelled).toContain('r1')
      })
    it('lets the running run of a ticket its worker ended finish before closing the issue, as after `ready` (#18)', async () => {
      const { h, stop } = await ending('completed', { r1Status: 'running' })
      await h.connect()
      stop()
      await settle()
      expect(h.api.cancelRun).not.toHaveBeenCalled()
      expect(h.api.setStatus).not.toHaveBeenCalled()
      h.runs[0]!.status = 'completed'
      await h.push({ type: 'task:completed', payload: { issue_id: 'issue-1' } })
      expect(h.api.setStatus).toHaveBeenCalledWith('issue-1', 'done')
      expect(h.api.cancelRun).not.toHaveBeenCalled()
    })
    it('cancels the run still running once the grace after the worker ended its ticket is over, also after a restart', async () => {
      const { h, stop } = await ending('completed', { r1Status: 'running' })
      stop()
      await settle()
      expect(h.api.setStatus).not.toHaveBeenCalled()
      await h.advance(60_000)
      expect(h.api.cancelRun).not.toHaveBeenCalled()
      h.restart()
      await h.connect()
      expect(h.api.setStatus).not.toHaveBeenCalled()
      await h.advance(60_000)
      expect(h.api.cancelRun).toHaveBeenCalledWith('r1')
      expect(h.api.setStatus).toHaveBeenCalledWith('issue-1', 'done')
    })
    it('cancels the running run at once when a person stopped the ticket', async () => {
      const { h, stop } = await ending('stopped', { by: 'human', r1Status: 'running' })
      stop()
      await vi.waitFor(() => { expect(h.api.setStatus).toHaveBeenCalledWith('issue-1', 'cancelled') })
      expect(h.api.cancelRun).toHaveBeenCalledWith('r1')
    })
    it('closes an issue whose ticket ended while Multica was still creating it', async () => {
      const h = setup(ticketWith([{ state: 'pending' }]))
      const ended = ticketWith([{ state: 'ended' }], { ended: { at: '2026-11-05T01:00:00Z', by: 'human', outcome: 'stopped' } } as Partial<Ticket>)
      h.api.createIssue.mockImplementationOnce(async () => {
        h.setTicket(ended)
        h.executor.stop(ended, ended.assignments[0]!, 'stopped')
        return { id: 'issue-1', identifier: 'JAR-1' }
      })
      await h.launch()
      await vi.waitFor(() => { expect(h.api.setStatus).toHaveBeenCalledWith('issue-1', 'cancelled') })
      expect(h.engine.assignmentStarted).not.toHaveBeenCalled()
    })
    it('reopens the closed issue for a woken ticket before posting its new Assignment, and leaves the new run alone', async () => {
      const { h, stop } = await ending('merged')
      stop()
      await vi.waitFor(() => { expect(h.api.setStatus).toHaveBeenCalledWith('issue-1', 'done') })
      h.runs[0]!.status = 'cancelled'
      const woken = ticketWith([{ state: 'ended' }, { state: 'pending' }])
      h.setTicket(woken)
      h.executor.start(woken, woken.assignments[1]!)
      await h.runLaunches()
      expect(h.api.setStatus).toHaveBeenLastCalledWith('issue-1', 'todo')
      expect(h.api.setStatus.mock.invocationCallOrder.at(-1)).toBeLessThan(h.api.addComment.mock.invocationCallOrder.at(-1)!)
      h.setTicket(ticketWith([{ state: 'ended' }, {}]))
      h.runs.push({ id: 'r2', status: 'running' })
      await h.executor.reconcile('t1')
      expect(h.api.cancelRun).toHaveBeenCalledTimes(1)
      expect(h.api.setStatus).toHaveBeenCalledTimes(2)
    })
    it('does not close the issue of a ticket woken while its close was still pending', async () => {
      const { h, stop } = await ending('merged')
      h.api.setStatus.mockRejectedValueOnce(new Error('Multica PUT failed'))
      stop()
      await vi.waitFor(() => { expect(h.api.setStatus).toHaveBeenCalledTimes(1) })
      await settle()
      const woken = ticketWith([{ state: 'ended' }, { state: 'pending' }])
      h.setTicket(woken)
      h.executor.start(woken, woken.assignments[1]!)
      h.setTicket(ticketWith([{ state: 'ended' }, {}]))
      h.runs.push({ id: 'r2', status: 'running' })
      await h.executor.reconcile('t1')
      expect(h.api.setStatus).toHaveBeenCalledTimes(1)
      expect(h.api.cancelRun).toHaveBeenCalledTimes(1)
    })
    it('closes after the in-flight launch POST and cancels the run it started if the ticket stopped meanwhile', async () => {
      const { h } = await ending('stopped')
      h.setTicket(ticketWith([{ state: 'ended', endedReason: 'retried' }, { state: 'pending' }]))
      const ended = ticketWith([{ state: 'ended' }, { state: 'ended' }], { ended: { at: '2026-11-05T01:00:00Z', by: 'human', outcome: 'stopped' } } as Partial<Ticket>)
      h.api.addComment.mockImplementationOnce(async (_issue: string, content: string) => {
        h.setTicket(ended)
        h.executor.stop(ended, ended.assignments[1]!, 'stopped')
        await settle()
        expect(h.api.setStatus).not.toHaveBeenCalled()
        h.runs.push({ id: 'r3', status: 'queued' })
        return { id: 'c9', issue_id: 'issue-1', author_type: 'member', content, created_at: '', trigger_outcomes: [] }
      })
      h.runs.splice(0)
      await h.launch('a2')
      await vi.waitFor(() => { expect(h.api.setStatus).toHaveBeenCalledExactlyOnceWith('issue-1', 'cancelled') })
      expect(h.api.cancelRun).toHaveBeenCalledWith('r3')
    })
    it('records a failed close in the worker health entry and tries again on the next reconciliation', async () => {
      const { h, stop } = await ending('merged')
      h.api.setStatus.mockRejectedValueOnce(new Error('Multica PUT failed'))
      await h.connect()
      stop()
      await vi.waitFor(() => { expect(h.health.set).toHaveBeenCalledWith(expect.objectContaining({ state: 'error', detail: expect.stringContaining('Multica PUT failed') })) })
      await h.push({ type: 'issue:updated', payload: { issue: { id: 'issue-1' } } })
      expect(h.api.setStatus).toHaveBeenCalledTimes(2)
      await h.push({ type: 'comment:created', payload: { comment: { issue_id: 'issue-1' } } })
      expect(h.api.setStatus).toHaveBeenCalledTimes(2)
    })
    it('closes after a restart what it could not close before, once the event stream connects', async () => {
      const { h, stop } = await ending('stopped')
      h.api.setStatus.mockRejectedValueOnce(new Error('Multica PUT failed'))
      stop()
      await vi.waitFor(() => { expect(h.api.setStatus).toHaveBeenCalledTimes(1) })
      await settle()
      h.restart()
      await h.connect()
      expect(h.api.setStatus).toHaveBeenLastCalledWith('issue-1', 'cancelled')
      expect(h.api.setStatus).toHaveBeenCalledTimes(2)
    })
  })

  describe('refused report blocks', () => {
    const notes = (h: ReturnType<typeof setup>) => h.posted.filter(item => item.content.startsWith('/note'))
    async function running() {
      const h = setup(ticketWith([{ state: 'pending' }]))
      await h.launch()
      await h.executor.reconcile('t1')
      h.setTicket(ticketWith([{}]))
      h.runs.push({ id: 'r1', status: 'running' })
      return { h, nonce: h.state()!.assignment!.nonce }
    }

    it('answers each refused block under its comment with why, without the nonce', async () => {
      const { h, nonce } = await running()
      ;(h.host.claimPull as ReturnType<typeof vi.fn>).mockResolvedValueOnce('PR #9 is not from this ticket\'s branch')
      h.engine.report.mockImplementationOnce(() => { throw new Error('The assignment is not running') })
      h.comments.push(
        agentComment('k1', '```' + REPORT_FENCE + '\n{oops\n```'),
        agentComment('k2', report({ tool: 'report', state: 'ready', summary: 'x' }, 'guess')),
        agentComment('k3', report({ tool: 'report', state: 'ready', summary: 'x' }, nonce, 'a0')),
        agentComment('k4', report({ tool: 'report', state: 'ready', summary: 'x', pr_number: 9 }, nonce)),
        agentComment('k5', report({ tool: 'report', state: 'progress', summary: 'y' }, nonce)))
      await h.executor.reconcile('t1')
      expect(notes(h).map(note => note.parentId)).toEqual(['k1', 'k2', 'k3', 'k4', 'k5'])
      const [json, wrongNonce, wrongAssignment, pull, engine] = notes(h).map(note => note.content)
      expect(json).toContain('not JSON')
      expect(wrongNonce).toContain('wrong nonce')
      expect(wrongAssignment).toContain('a0')
      expect(pull).toContain('PR #9 is not from this ticket')
      expect(engine).toContain('The assignment is not running')
      for (const note of notes(h)) expect(note.content).not.toContain(nonce)
      // A refused PR claim refuses the report it came with.
      expect(h.engine.report).toHaveBeenCalledTimes(1)
      expect(h.engine.report).toHaveBeenCalledWith('t1', { state: 'progress', summary: 'y' })
    })

    it('answers a refused block once, across reconciliations and restarts', async () => {
      const { h } = await running()
      h.comments.push(agentComment('k1', report({ tool: 'report', state: 'ready', summary: 'x' }, 'guess')))
      await h.executor.reconcile('t1')
      await h.executor.reconcile('t1')
      expect(notes(h)).toHaveLength(1)
      // A crash after the note went out but before the comment was recorded as seen.
      h.setState({ ...h.state()!, seen: [] })
      h.restart()
      await h.executor.reconcile('t1')
      expect(notes(h)).toHaveLength(1)
    })

    it('posts a note again on the next reconciliation when Multica failed to take it', async () => {
      const { h } = await running()
      h.comments.push(agentComment('k1', report({ tool: 'report', state: 'ready', summary: 'x' }, 'guess')))
      h.failPosts(1)
      await h.executor.reconcile('t1')
      expect(notes(h)).toHaveLength(0)
      await h.executor.reconcile('t1')
      expect(notes(h)).toHaveLength(1)
    })

    const retries = (h: ReturnType<typeof setup>) => h.posted.filter(item => item.content.startsWith('superagent: '))

    it('posts a refusal again as a plain comment once the run ended with no report accepted after it, once per assignment', async () => {
      const { h } = await running()
      h.comments.push(agentComment('k1', report({ tool: 'report', state: 'ready', summary: 'x' }, 'guess')))
      await h.executor.reconcile('t1')
      expect(notes(h)).toHaveLength(1)
      // The run may still fix it.
      expect(retries(h)).toEqual([])
      h.runs[0]!.status = 'completed'
      await h.executor.reconcile('t1')
      expect(retries(h)).toHaveLength(1)
      expect(retries(h)[0]).toMatchObject({ parentId: 'k1' })
      expect(retries(h)[0]!.content).toContain('report block 1 in comment k1 was refused: wrong nonce')
      // The run it starts ends the turn, so the ticket does not wait in between.
      expect(h.engine.turnEnded).not.toHaveBeenCalled()
      await h.executor.reconcile('t1')
      // The run it started refuses again: the worker had its second chance, and the run's end counts as waiting.
      h.runs.push({ id: 'r2', status: 'completed' })
      h.comments.push(agentComment('k2', report({ tool: 'report', state: 'ready', summary: 'x' }, 'guess')))
      await h.executor.reconcile('t1')
      expect(notes(h)).toHaveLength(2)
      expect(retries(h)).toHaveLength(1)
      expect(h.engine.turnEnded).toHaveBeenCalledWith('t1', 'a1')
      expect(h.state()!.unanswered).toBeUndefined()
    })

    it('does not post a refusal again that is on the issue already, as after a restart', async () => {
      const { h } = await running()
      h.comments.push(agentComment('k1', report({ tool: 'report', state: 'ready', summary: 'x' }, 'guess')))
      await h.executor.reconcile('t1')
      h.runs[0]!.status = 'completed'
      await h.executor.reconcile('t1')
      expect(retries(h)).toHaveLength(1)
      // A crash after the comment went out but before the state was saved.
      const { resentFor: _, ...lost } = h.state()!
      h.setState({ ...lost, unanswered: { assignmentId: 'a1', commentId: 'k1', resend: 'superagent: report block 1 in comment k1 was refused: wrong nonce.' } })
      h.restart()
      await h.executor.reconcile('t1')
      expect(retries(h)).toHaveLength(1)
    })

    it('leaves a refused ack to the note, and a refusal to the run that reads a delivery not read yet', async () => {
      const { h } = await running()
      h.engine.reply.mockImplementationOnce(() => { throw new Error('No such trigger') })
      h.comments.push(agentComment('k1', report({ tool: 'ack', trigger_id: 'made-up', text: 'on it' }, h.state()!.assignment!.nonce)))
      h.runs[0]!.status = 'completed'
      await h.executor.reconcile('t1')
      expect(notes(h)).toHaveLength(1)
      expect(retries(h)).toEqual([])
      const other = await running()
      other.h.comments.push(agentComment('k1', report({ tool: 'report', state: 'ready', summary: 'x' }, 'guess')))
      other.h.setState({ ...other.h.state()!, deliveries: [{ commentId: 'd1', inputIds: ['i1'], assignmentId: 'a1' }] })
      other.h.runs[0]!.status = 'completed'
      await other.h.executor.reconcile('t1')
      expect(retries(other.h)).toEqual([])
    })

    it('does not post a refusal again when a later report was accepted, or while a run waits to start', async () => {
      const { h, nonce } = await running()
      h.comments.push(agentComment('k1', report({ tool: 'report', state: 'ready', summary: 'x' }, 'guess')),
        agentComment('k2', report({ tool: 'report', state: 'ready', summary: 'fixed' }, nonce)))
      h.runs[0]!.status = 'completed'
      await h.executor.reconcile('t1')
      expect(retries(h)).toEqual([])
      const other = await running()
      other.h.comments.push(agentComment('k1', report({ tool: 'report', state: 'ready', summary: 'x' }, 'guess')))
      other.h.runs[0]!.status = 'completed'
      other.h.runs.push({ id: 'r2', status: 'queued' })
      await other.h.executor.reconcile('t1')
      expect(retries(other.h)).toEqual([])
      other.h.runs[1]!.status = 'completed'
      await other.h.executor.reconcile('t1')
      expect(retries(other.h)).toHaveLength(1)
    })

    it('clears the worker health error of a refused report once a later report of the assignment is accepted (#18)', async () => {
      const { h, nonce } = await running()
      // JAR-84: a `dropped` report without a summary, refused, then sent again in full 14 seconds later.
      h.comments.push(agentComment('k1', report({ tool: 'report', state: 'dropped' }, nonce)))
      await h.executor.reconcile('t1')
      expect(h.health.get()).toMatchObject({ state: 'error', detail: expect.stringContaining('Ignored a report block in comment k1') })
      h.comments.push(agentComment('k2', report({ tool: 'report', state: 'dropped', summary: 'Nothing to do' }, nonce)))
      await h.executor.reconcile('t1')
      expect(h.health.get()).toMatchObject({ state: 'ready' })
    })

    it('keeps the health error of a refused ack, which a later report does not fix', async () => {
      const { h, nonce } = await running()
      h.engine.reply.mockImplementationOnce(() => { throw new Error('No such trigger') })
      h.comments.push(agentComment('k1', report({ tool: 'ack', trigger_id: 'made-up', text: 'on it' }, nonce)))
      await h.executor.reconcile('t1')
      h.comments.push(agentComment('k2', report({ tool: 'report', state: 'progress', summary: 'working' }, nonce)))
      await h.executor.reconcile('t1')
      expect(h.health.get()).toMatchObject({ state: 'error', detail: expect.stringContaining('comment k1') })
    })

    it('keeps a health error that is not the refusal when a later report is accepted', async () => {
      const { h, nonce } = await running()
      h.comments.push(agentComment('k1', report({ tool: 'report', state: 'dropped' }, nonce)))
      await h.executor.reconcile('t1')
      h.health.set({ state: 'error', detail: 'Multica event stream: closed; reconnecting' })
      h.comments.push(agentComment('k2', report({ tool: 'report', state: 'dropped', summary: 'Nothing to do' }, nonce)))
      await h.executor.reconcile('t1')
      expect(h.health.get()).toMatchObject({ state: 'error', detail: expect.stringContaining('reconnecting') })
    })

    it('does not answer report blocks in comments the configured agent did not write', async () => {
      const { h } = await running()
      h.comments.push({ ...agentComment('k1', report({ tool: 'report', state: 'ready', summary: 'x' }, 'guess')), author_id: 'other-agent' },
        { ...agentComment('k2', '```' + REPORT_FENCE + '\n{oops\n```'), author_type: 'member', author_id: 'someone' })
      await h.executor.reconcile('t1')
      expect(notes(h)).toEqual([])
    })
  })

  describe('candidate agents', () => {
    const CANDIDATES = ['a-off-runtime', 'a-off', 'a-ok', 'a-ok-2']
    function candidates(standardOnly = false) {
      const h = setup(ticketWith([{ state: 'pending' }]), { agents: CANDIDATES, pickHealthy: true, standardOnly })
      h.runtimes.push({ id: 'rt-down', status: 'offline' }, { id: 'rt-up', status: 'online' })
      h.agents.push({ id: 'a-off-runtime', status: 'idle', runtime_id: 'rt-down' }, { id: 'a-off', status: 'offline', runtime_id: 'rt-up' },
        { id: 'a-ok', status: 'working', runtime_id: 'rt-up' }, { id: 'a-ok-2', status: 'idle', runtime_id: 'rt-up' })
      return h
    }
    const assignedTo = (h: ReturnType<typeof setup>) => h.api.createIssue.mock.calls.map(([input]) => input.agentId)

    it('assigns a new ticket to the first healthy candidate, skipping unhealthy ones', async () => {
      const h = candidates()
      await h.connect()
      await h.launch()
      expect(assignedTo(h)).toEqual(['a-ok'])
      expect(h.engine.assignmentStarted).toHaveBeenCalledWith('t1', 'a1')
      expect(h.state()).toMatchObject({ agentId: 'a-ok' })
      // Its reports are the ones accepted, not the first candidate's.
      h.setTicket(ticketWith([{}]))
      const nonce = h.state()!.assignment!.nonce
      h.comments.push({ ...agentComment('k1', report({ tool: 'report', state: 'progress', summary: 'first' }, nonce)), author_id: 'a-off-runtime' },
        { ...agentComment('k2', report({ tool: 'report', state: 'progress', summary: 'chosen' }, nonce)), author_id: 'a-ok' })
      await h.executor.reconcile('t1')
      expect(h.engine.report).toHaveBeenCalledTimes(1)
      expect(h.engine.report).toHaveBeenCalledWith('t1', { state: 'progress', summary: 'chosen' })
    })

    it('finds the agent of an issue created before a crash from its assignee', async () => {
      const h = candidates()
      ;(h.api.findIssues as ReturnType<typeof vi.fn>).mockResolvedValueOnce([{ id: 'issue-1', identifier: 'JAR-1', assignee_id: 'a-ok' }])
      await h.connect()
      await h.launch()
      expect(h.api.createIssue).not.toHaveBeenCalled()
      expect(h.state()).toMatchObject({ issueId: 'issue-1', agentId: 'a-ok' })
    })

    it('withdraws the question even when recording the issue fails after it was created', async () => {
      const h = allOffline()
      await h.connect()
      await h.launch()
      h.agents.find(agent => agent.id === 'a-ok')!.status = 'idle'
      h.api.setMetadata.mockRejectedValueOnce(new Error('Multica PUT failed'))
      await h.push({ type: 'agent:status', payload: {} })
      await h.runLaunches()
      expect(h.host.store.tickets.get('t1')!.attention).toBeUndefined()
    })

    it('assigns every ticket to the single MULTICA_AGENT_ID agent without reading health, as before', async () => {
      const h = setup(ticketWith([{ state: 'pending' }]))
      await h.connect()
      await h.push({ type: 'agent:status', payload: {} })
      await h.launch()
      expect(assignedTo(h)).toEqual([AGENT])
      expect(h.api.agents).not.toHaveBeenCalled()
      expect(h.api.runtimes).not.toHaveBeenCalled()
    })

    /** Lets the recording Engine keep a reported question on the ticket, as the real one does. */
    function keepQuestions(h: ReturnType<typeof setup>) {
      h.engine.report.mockImplementation(((_id: string, report: { question?: string | null }) => {
        const { attention: _attention, ...rest } = h.host.store.tickets.get('t1')!
        const next = report.question ? { ...rest, attention: { id: 'q1', question: report.question } }
          : report.question === null ? rest : h.host.store.tickets.get('t1')!
        h.setTicket(next as Ticket)
        return next
      }) as never)
    }
    function allOffline(standardOnly = false) {
      const h = candidates(standardOnly)
      for (const agent of h.agents) agent.status = 'offline'
      keepQuestions(h)
      return h
    }

    it('keeps a ticket queued while no candidate is healthy, and asks a person once', async () => {
      const h = allOffline()
      await h.connect()
      await h.launch()
      expect(h.api.createIssue).not.toHaveBeenCalled()
      expect(h.engine.assignmentStarted).not.toHaveBeenCalled()
      expect(h.engine.report).toHaveBeenCalledTimes(1)
      expect(h.engine.report).toHaveBeenCalledWith('t1', expect.objectContaining({ state: 'progress',
        question: expect.stringContaining('No Multica workstation is online') }))
      expect(h.host.sessions.relay).toHaveBeenCalledTimes(1)
      // Launching again, after a reconnect or a restart, asks nothing more.
      await h.drop()
      await h.connect()
      await h.launch()
      h.restart()
      await h.launch()
      expect(h.api.createIssue).not.toHaveBeenCalled()
      expect(h.engine.report).toHaveBeenCalledTimes(1)
      expect(h.host.sessions.relay).toHaveBeenCalledTimes(1)
    })

    it('waits for a standard candidate before resuming an issue assigned to an excluded agent', async () => {
      const h = allOffline(true)
      h.agents.push({ id: 'premium', status: 'idle', runtime_id: 'rt-up' })
      h.setState({ issueId: 'issue-1', agentId: 'premium', closed: 'done', deliveries: [], seen: [], runs: {} })
      await h.connect()
      await h.launch()
      expect(h.api.assignIssue).not.toHaveBeenCalled()
      expect(h.api.addComment).not.toHaveBeenCalled()
      expect(h.engine.assignmentStarted).not.toHaveBeenCalled()
      expect(h.host.store.tickets.get('t1')!.attention?.question).toContain('No Multica workstation is online')

      h.agents.find(agent => agent.id === 'a-ok')!.status = 'idle'
      await h.push({ type: 'agent:status', payload: {} })
      await h.runLaunches()
      expect(h.api.createIssue).not.toHaveBeenCalled()
      expect(h.api.assignIssue).toHaveBeenCalledWith('issue-1', 'a-ok')
      expect(h.api.setStatus).toHaveBeenCalledWith('issue-1', 'todo')
      expect(h.api.assignIssue.mock.invocationCallOrder[0]).toBeLessThan(h.api.addComment.mock.invocationCallOrder[0]!)
      expect(h.engine.assignmentStarted).toHaveBeenCalledWith('t1', 'a1')
      expect(h.host.store.tickets.get('t1')!.attention).toBeUndefined()
    })

    it.each([true, false])('checks the old issue assignee even with no stored agentId (candidate initially online: %s)', async online => {
      const h = online ? candidates(true) : allOffline(true)
      h.setState({ issueId: 'issue-1', closed: 'done', deliveries: [], seen: [], runs: {} })
      h.setAssignee('premium')
      await h.connect()
      await h.launch()
      expect(h.api.issue).toHaveBeenCalledWith('issue-1')
      if (!online) {
        expect(h.api.addComment).not.toHaveBeenCalled()
        expect(h.api.assignIssue).not.toHaveBeenCalled()
        h.agents.find(agent => agent.id === 'a-ok')!.status = 'idle'
        await h.push({ type: 'agent:status', payload: {} })
        await h.runLaunches()
      }
      expect(h.api.assignIssue).toHaveBeenCalledExactlyOnceWith('issue-1', 'a-ok')
      expect(h.api.addComment).toHaveBeenCalledTimes(1)
      expect(h.api.assignIssue.mock.invocationCallOrder[0]!).toBeLessThan(h.api.addComment.mock.invocationCallOrder[0]!)
      expect(h.state()!.agentId).toBe('a-ok')
    })

    it('explicitly assigns a legacy issue whose real assignee is not known', async () => {
      const h = candidates(true)
      h.setState({ issueId: 'issue-1', deliveries: [], seen: [], runs: {} })
      await h.connect()
      await h.launch()
      expect(h.api.assignIssue).toHaveBeenCalledExactlyOnceWith('issue-1', 'a-ok')
      expect(h.state()!.agentId).toBe('a-ok')
    })

    it.each([true, false])('does not reassign a legacy issue, including when its assignee is not stored (stored locally: %s)', async stored => {
      const h = setup(ticketWith([{ state: 'pending' }]), { agents: ['new-default'] })
      h.setState({ issueId: 'issue-1', ...stored ? { agentId: 'original' } : {}, deliveries: [], seen: [], runs: {} })
      h.setAssignee('original')
      await h.launch()
      expect(h.api.assignIssue).not.toHaveBeenCalled()
      expect(h.state()!.agentId).toBe(stored ? 'original' : undefined)
      expect(h.api.issue).not.toHaveBeenCalled()
      expect(h.api.addComment).toHaveBeenCalledTimes(1)
    })

    it('keeps a healthy actual standard assignee even when the local state shows a different one', async () => {
      const h = candidates(true)
      h.setState({ issueId: 'issue-1', agentId: 'superseded', deliveries: [], seen: [], runs: {} })
      h.setAssignee('a-ok-2')
      await h.connect()
      await h.launch()
      expect(h.api.assignIssue).not.toHaveBeenCalled()
      expect(h.api.addComment).toHaveBeenCalledTimes(1)
      expect(h.state()!.agentId).toBe('a-ok-2')
    })

    it('does not authorize an authorless or remotely assigned author without evidence of the old run', async () => {
      const h = candidates(true)
      h.setState({ issueId: 'issue-1', assignment: { id: 'a1', nonce: 'n' }, deliveries: [], seen: [], runs: {} })
      h.setTicket(ticketWith([{}]))
      const posted = agentComment('k1', report({ tool: 'report', state: 'ready', summary: 'unverified' }, 'n'))
      h.comments.push({ ...posted, author_id: undefined })
      await h.connect()
      expect(h.engine.report).not.toHaveBeenCalled()
      h.setAssignee('a-ok-2')
      h.comments.push({ ...posted, id: 'k2', author_id: 'a-ok-2' })
      await h.executor.reconcile('t1')
      expect(h.engine.report).not.toHaveBeenCalled()
      expect(h.state()!.assignment!.reportAgentId).toBeNull()
    })

    it('waits rather than waking an issue already assigned to an offline standard agent, then picks an online backup', async () => {
      const h = allOffline(true)
      h.setState({ issueId: 'issue-1', agentId: 'a-ok', closed: 'done', deliveries: [], seen: [], runs: {} })
      await h.connect()
      await h.launch()
      expect(h.api.setStatus).not.toHaveBeenCalled()
      expect(h.api.addComment).not.toHaveBeenCalled()
      h.agents.find(agent => agent.id === 'a-ok-2')!.status = 'idle'
      await h.push({ type: 'agent:status', payload: {} })
      await h.runLaunches()
      expect(h.api.assignIssue).toHaveBeenCalledExactlyOnceWith('issue-1', 'a-ok-2')
      expect(h.api.addComment).toHaveBeenCalledTimes(1)
    })

    it('does not steer or cancel a claimed run on an excluded agent, then moves the same issue before delivering the Input', async () => {
      const h = allOffline(true)
      h.setState({ issueId: 'issue-1', agentId: 'premium', assignment: { id: 'a1', nonce: 'n' }, deliveries: [], seen: [], runs: {} })
      const inputs = [{ id: 'i1', kind: 'text', text: 'Follow up', at: '' }] as Ticket['inputs']
      h.setTicket(ticketWith([{}], { inputs, undelivered: ['i1'] }))
      h.runs.push({ id: 'old', agent_id: 'premium', status: 'running', can_supplement: true })
      await h.connect()
      await h.deliver()
      expect(h.api.cancelRun).not.toHaveBeenCalled()
      expect(h.api.assignIssue).not.toHaveBeenCalled()
      expect(h.api.addComment).not.toHaveBeenCalled()
      expect(h.engine.inputsDelivered).not.toHaveBeenCalled()
      // The old agent can still finish and report while on the issue.
      h.comments.push({ ...agentComment('k1', report({ tool: 'report', state: 'progress', summary: 'Old work done' }, 'n')), author_id: 'premium' })
      h.runs[0]!.status = 'completed'
      await h.executor.reconcile('t1')
      expect(h.engine.report).toHaveBeenCalledWith('t1', { state: 'progress', summary: 'Old work done' })
      await h.deliver()
      expect(h.api.addComment).not.toHaveBeenCalled()
      h.enqueued.length = 0
      h.agents.find(agent => agent.id === 'a-ok')!.status = 'idle'
      await h.push({ type: 'agent:status', payload: {} })
      expect(h.enqueued).toContainEqual({ kind: 'delivery', key: 't1' })
      await h.deliver()
      expect(h.api.assignIssue).toHaveBeenCalledExactlyOnceWith('issue-1', 'a-ok')
      expect(h.api.assignIssue.mock.invocationCallOrder[0]!).toBeLessThan(h.api.addComment.mock.invocationCallOrder[0]!)
      expect(h.posted).toEqual([{ content: expect.stringContaining('Follow up') }])
      expect(h.engine.inputsDelivered).toHaveBeenCalledExactlyOnceWith('t1', 'a1', ['i1'])
      expect(h.engine.assignmentReassigned).not.toHaveBeenCalled()
      expect(h.state()).toMatchObject({ issueId: 'issue-1', agentId: 'a-ok', assignment: { id: 'a1', nonce: 'n' } })
    })

    it('defers a failed steer resend until the issue can move from its old assignee', async () => {
      const h = allOffline(true)
      h.setState({ issueId: 'issue-1', agentId: 'premium', assignment: { id: 'a1', nonce: 'n' }, seen: [], runs: {},
        deliveries: [{ commentId: 'steer', inputIds: ['i1'], assignmentId: 'a1', read: false, steeredRun: 'old' }] })
      h.setTicket(ticketWith([{}]))
      h.runs.push({ id: 'old', agent_id: 'premium', status: 'completed' })
      h.receipts.set('steer', [{ task_id: 'old', status: 'failed', failure_reason: 'turn_ended' }])
      h.comments.push({ id: 'steer', issue_id: 'issue-1', author_type: 'member', content: 'Steered Input', created_at: '' })
      await h.connect()
      expect(h.api.addComment).not.toHaveBeenCalled()
      expect(h.state()!.deliveries[0]!.steeredRun).toBe('old')
      h.agents.find(agent => agent.id === 'a-ok')!.status = 'idle'
      await h.push({ type: 'agent:status', payload: {} })
      expect(h.api.assignIssue).toHaveBeenCalledExactlyOnceWith('issue-1', 'a-ok')
      expect(h.posted).toEqual([{ content: 'Steered Input' }])
      expect(h.api.assignIssue.mock.invocationCallOrder[0]!).toBeLessThan(h.api.addComment.mock.invocationCallOrder[0]!)
      expect(h.state()!.deliveries).toEqual([{ commentId: 'c1', inputIds: ['i1'], assignmentId: 'a1', read: false }])
    })

    it('defers a refusal resend until the issue can move, while its no-run note is still delivered', async () => {
      const h = allOffline(true)
      h.setState({ issueId: 'issue-1', agentId: 'premium', assignment: { id: 'a1', nonce: 'n' }, deliveries: [], seen: [], runs: {} })
      h.setTicket(ticketWith([{}]))
      h.runs.push({ id: 'old', agent_id: 'premium', status: 'completed' })
      h.comments.push({ ...agentComment('k1', report({ tool: 'report', state: 'ready', summary: 'x' }, 'guess')), author_id: 'premium' })
      await h.connect()
      expect(h.posted).toEqual([{ content: expect.stringMatching(/^\/note /), parentId: 'k1' }])
      expect(h.api.assignIssue).not.toHaveBeenCalled()
      expect(h.engine.turnEnded).not.toHaveBeenCalled()
      h.agents.find(agent => agent.id === 'a-ok')!.status = 'idle'
      await h.push({ type: 'agent:status', payload: {} })
      expect(h.api.assignIssue).toHaveBeenCalledExactlyOnceWith('issue-1', 'a-ok')
      expect(h.posted[1]).toEqual({ content: expect.stringMatching(/^superagent: .*comment k1/) })
      expect(h.api.assignIssue.mock.invocationCallOrder[0]!).toBeLessThan(h.api.addComment.mock.invocationCallOrder[1]!)
      expect(h.state()!.resentFor).toBe('a1')
    })

    it('reports missing and invalid standard IDs at startup but still picks the first valid healthy agent', async () => {
      const h = setup(ticketWith([{ state: 'pending' }]), { agents: ['typo', 'archived', 'unbound', 'ok'], pickHealthy: true, standardOnly: true })
      h.runtimes.push({ id: 'rt', status: 'online' })
      h.agents.push({ id: 'archived', status: 'idle', runtime_id: 'rt', archived_at: '2026-11-05' },
        { id: 'unbound', status: 'idle' }, { id: 'ok', status: 'idle', runtime_id: 'rt' })
      await h.connect()
      expect(h.health.get()).toMatchObject({ state: 'error', detail: expect.stringMatching(/MULTICA_STANDARD_AGENT_IDS.*typo.*archived.*unbound/) })
      await h.launch()
      expect(assignedTo(h)).toEqual(['ok'])
    })

    it('asks for a configuration fix instead of a workstation when all standard IDs are unknown', async () => {
      const h = setup(ticketWith([{ state: 'pending' }]), { agents: ['typo'], pickHealthy: true, standardOnly: true })
      h.runtimes.push({ id: 'rt', status: 'online' })
      h.agents.push({ id: 'premium', status: 'idle', runtime_id: 'rt' })
      await h.connect()
      await h.launch()
      expect(h.api.createIssue).not.toHaveBeenCalled()
      expect(h.engine.assignmentStarted).not.toHaveBeenCalled()
      expect(h.engine.report).toHaveBeenCalledWith('t1', expect.objectContaining({ question: expect.stringMatching(/MULTICA_STANDARD_AGENT_IDS.*typo.*restart/) }))
    })

    it('dispatches a queued ticket once a candidate comes online, and withdraws the question', async () => {
      const h = allOffline()
      await h.connect()
      await h.launch()
      expect(h.engine.report).toHaveBeenCalledTimes(1)
      // An event about something else reads no health.
      await h.push({ type: 'activity:created', payload: {} })
      expect(h.enqueued.filter(item => item.kind === 'launch')).toEqual([])
      h.agents.find(agent => agent.id === 'a-ok-2')!.status = 'idle'
      await h.push({ type: 'agent:status', payload: { agent: { id: 'a-ok-2', status: 'idle' } } })
      await h.runLaunches()
      expect(assignedTo(h)).toEqual(['a-ok-2'])
      expect(h.engine.assignmentStarted).toHaveBeenCalledWith('t1', 'a1')
      expect(h.engine.report).toHaveBeenLastCalledWith('t1', expect.objectContaining({ question: null }))
      expect(h.host.store.tickets.get('t1')!.attention).toBeUndefined()
      // Health was read on connecting and on the agent event, and on nothing else.
      expect(h.api.agents).toHaveBeenCalledTimes(2)
    })

    it('follows a workstation going offline and back by its daemon:register sweeps, while its agents stay idle', async () => {
      // As measured on 2026-11-05: a stale sweep marks the runtimes offline, a heartbeat recovers them, and neither names them.
      const h = candidates()
      await h.connect()
      h.runtimes.find(runtime => runtime.id === 'rt-up')!.status = 'offline'
      await h.push({ type: 'daemon:register', actor_type: 'system', payload: { action: 'stale_sweep' } })
      keepQuestions(h)
      await h.launch()
      expect(h.api.createIssue).not.toHaveBeenCalled()
      expect(h.engine.report).toHaveBeenCalledWith('t1', expect.objectContaining({ question: expect.stringContaining('No Multica workstation') }))
      h.runtimes.find(runtime => runtime.id === 'rt-up')!.status = 'online'
      await h.push({ type: 'daemon:register', actor_type: 'system', payload: { action: 'heartbeat_recovery' } })
      await h.runLaunches()
      expect(assignedTo(h)).toEqual(['a-ok'])
    })

    describe('reassigning across workstations (#8)', () => {
      const MINUTE = 60_000
      /** A ticket launched on `a-ok`, whose Assignment runs. */
      async function launched() {
        const h = candidates()
        keepQuestions(h)
        await h.connect()
        await h.launch()
        h.setTicket(ticketWith([{}]))
        return h
      }
      /** The Engine replaces the Assignment, as the real one does, and the new one launches on the same issue. */
      async function relaunch(h: ReturnType<typeof setup>) {
        h.setTicket(ticketWith([{ state: 'ended', endedReason: 'reassigned' }, { state: 'pending' }]))
        await h.launch('a2')
        h.setTicket(ticketWith([{ state: 'ended', endedReason: 'reassigned' }, {}]))
      }

      it('moves a run nobody claimed for 5 minutes to the next healthy candidate, but not one queued through a 3-minute switch', async () => {
        const h = await launched()
        h.runs.push({ id: 'r1', status: 'queued', created_at: h.at() } as Run)
        await h.executor.reconcile('t1')
        await h.advance(3 * MINUTE)
        expect(h.api.cancelRun).not.toHaveBeenCalled()
        expect(h.engine.assignmentReassigned).not.toHaveBeenCalled()
        await h.advance(2 * MINUTE)
        expect(h.api.cancelRun).toHaveBeenCalledWith('r1')
        expect(h.api.assignIssue).toHaveBeenCalledWith('issue-1', 'a-ok-2')
        // Cancelled before the issue moved, so no two runs are active at once.
        expect(h.api.cancelRun.mock.invocationCallOrder[0]!).toBeLessThan(h.api.assignIssue.mock.invocationCallOrder[0]!)
        expect(h.engine.assignmentReassigned).toHaveBeenCalledExactlyOnceWith('t1', 'a1', expect.stringContaining('another workstation'))
        expect(h.state()).toMatchObject({ agentId: 'a-ok-2' })
        expect(h.state()!.assignment).toBeUndefined()
        await relaunch(h)
        expect(h.posted.at(-1)!.content).toContain('A new assignment `a2`')
        expect(h.state()).toMatchObject({ agentId: 'a-ok-2', assignment: { id: 'a2' } })
      })

      it('leaves a claimed run alone however long it runs', async () => {
        const h = await launched()
        h.runs.push({ id: 'r1', status: 'running', created_at: h.at() } as Run)
        await h.executor.reconcile('t1')
        await h.advance(30 * MINUTE)
        await h.executor.reconcile('t1')
        expect(h.engine.assignmentReassigned).not.toHaveBeenCalled()
      })

      it('moves a run that failed because its runtime went offline, cancelling the retry Multica deferred for it', async () => {
        const h = await launched()
        h.runs.push({ id: 'r1', status: 'running', created_at: h.at() } as Run)
        await h.executor.reconcile('t1')
        h.runs[0]!.status = 'failed'
        Object.assign(h.runs[0]!, { failure_reason: 'runtime_offline', error: 'runtime went offline' })
        h.runs.push({ id: 'r2', status: 'deferred', created_at: h.at() } as Run)
        await h.executor.reconcile('t1')
        expect(h.api.cancelRun).toHaveBeenCalledWith('r2')
        expect(h.api.assignIssue).toHaveBeenCalledWith('issue-1', 'a-ok-2')
        expect(h.engine.assignmentReassigned).toHaveBeenCalledWith('t1', 'a1', expect.stringContaining('runtime_offline'))
        // Not the worker's failure, and not the end of its turn.
        expect(h.engine.report).not.toHaveBeenCalledWith('t1', expect.objectContaining({ state: 'failed' }))
        expect(h.engine.turnEnded).not.toHaveBeenCalled()
      })

      it('moves a run whose messages say its CLI is logged out at once, and keeps that candidate out for a while', async () => {
        const h = await launched()
        h.runs.push({ id: 'r1', status: 'failed', created_at: h.at(), error: 'agent exited with code 1', failure_reason: 'agent_error' } as Run)
        h.messages.set('r1', [{ type: 'error', content: 'Error: Not logged in. Please run codebuddy login.' }])
        await h.executor.reconcile('t1')
        expect(h.api.runMessages).toHaveBeenCalledWith('r1')
        expect(h.api.assignIssue).toHaveBeenCalledWith('issue-1', 'a-ok-2')
        await relaunch(h)
        // a-ok is still healthy by its runtime, but out: with a-ok-2 offline too, nobody is left to take the ticket.
        h.runs.push({ id: 'r2', status: 'failed', created_at: h.at(), failure_reason: 'runtime_offline' } as Run)
        h.agents.find(agent => agent.id === 'a-ok-2')!.status = 'offline'
        await h.push({ type: 'agent:status', payload: {} })
        await h.executor.reconcile('t1')
        expect(h.api.assignIssue).toHaveBeenCalledTimes(1)
        expect(h.engine.report).toHaveBeenCalledWith('t1', expect.objectContaining({ question: expect.stringContaining('No Multica workstation') }))
      })

      it('starts the ticket again on its suspended candidate once the suspension is over, when no other is online (#18)', async () => {
        const h = await launched()
        h.agents.find(agent => agent.id === 'a-ok-2')!.status = 'offline'
        await h.push({ type: 'agent:status', payload: {} })
        h.runs.push({ id: 'r1', status: 'failed', created_at: h.at(), failure_reason: 'agent_error.provider_auth_or_access' } as Run)
        await h.executor.reconcile('t1')
        expect(h.engine.report).toHaveBeenCalledWith('t1', expect.objectContaining({ question: expect.stringContaining('No Multica workstation') }))
        await h.advance(29 * MINUTE)
        expect(h.engine.assignmentReassigned).not.toHaveBeenCalled()
        // A person fixed the login meanwhile; the suspension ends and the ticket goes back to a-ok, with nobody asked to retry.
        await h.advance(1 * MINUTE)
        expect(h.api.assignIssue).toHaveBeenCalledWith('issue-1', 'a-ok')
        expect(h.engine.assignmentReassigned).toHaveBeenCalledExactlyOnceWith('t1', 'a1', expect.stringContaining('a-ok'))
        expect(h.engine.report).toHaveBeenLastCalledWith('t1', expect.objectContaining({ question: null }))
        await relaunch(h)
        expect(h.state()).toMatchObject({ agentId: 'a-ok', assignment: { id: 'a2' } })
        expect(h.posted.at(-1)!.content).toContain('A new assignment `a2`')
      })

      it('goes back to its suspended candidate once, not every time its suspension ends', async () => {
        const h = await launched()
        h.agents.find(agent => agent.id === 'a-ok-2')!.status = 'offline'
        await h.push({ type: 'agent:status', payload: {} })
        h.runs.push({ id: 'r1', status: 'failed', created_at: h.at(), failure_reason: 'agent_error.provider_auth_or_access' } as Run)
        await h.executor.reconcile('t1')
        await h.advance(30 * MINUTE)
        await relaunch(h)
        // Still logged out: the second suspension leaves the ticket to a person.
        h.runs.push({ id: 'r2', status: 'failed', created_at: h.at(), failure_reason: 'agent_error.provider_auth_or_access' } as Run)
        await h.executor.reconcile('t1')
        await h.advance(30 * MINUTE)
        expect(h.engine.assignmentReassigned).toHaveBeenCalledTimes(1)
        expect(h.engine.report).toHaveBeenLastCalledWith('t1', expect.objectContaining({ question: expect.stringContaining('No Multica workstation') }))
      })

      it('does not start a ticket again on its own candidate whose run went unclaimed', async () => {
        const h = await launched()
        h.agents.find(agent => agent.id === 'a-ok-2')!.status = 'offline'
        await h.push({ type: 'agent:status', payload: {} })
        h.runs.push({ id: 'r1', status: 'queued', created_at: h.at() } as Run)
        await h.executor.reconcile('t1')
        await h.advance(40 * MINUTE)
        await h.push({ type: 'agent:status', payload: {} })
        expect(h.engine.assignmentReassigned).not.toHaveBeenCalled()
        expect(h.api.cancelRun).not.toHaveBeenCalled()
      })

      it('cancels a retry Multica deferred for the old agent after the issue was reassigned', async () => {
        const h = await launched()
        h.runs.push({ id: 'r1', status: 'failed', created_at: h.at(), failure_reason: 'runtime_offline', agent_id: 'a-ok' } as Run)
        await h.executor.reconcile('t1')
        await relaunch(h)
        h.runs.push({ id: 'r2', status: 'deferred', created_at: h.at(), agent_id: 'a-ok' } as Run,
          { id: 'r3', status: 'queued', created_at: h.at(), agent_id: 'a-ok-2' } as Run)
        await h.executor.reconcile('t1')
        expect(h.api.cancelRun).toHaveBeenCalledWith('r2')
        expect(h.api.cancelRun).not.toHaveBeenCalledWith('r3')
        expect(h.engine.assignmentReassigned).toHaveBeenCalledTimes(1)
      })

      it('records a failure that is neither the workstation nor its login as the worker\'s, as before', async () => {
        const h = await launched()
        h.runs.push({ id: 'r1', status: 'failed', created_at: h.at(), error: 'tests failed', failure_reason: 'agent_error' } as Run)
        // A tool's output is the worker's work, whatever it says.
        h.messages.set('r1', [{ type: 'tool_result', output: 'bash: pnpm: command not found' }])
        await h.executor.reconcile('t1')
        expect(h.engine.assignmentReassigned).not.toHaveBeenCalled()
        expect(h.engine.report).toHaveBeenCalledWith('t1', expect.objectContaining({ state: 'failed' }))
        expect(h.engine.turnEnded).toHaveBeenCalled()
      })

      it('asks a person once when no other candidate is healthy, and moves the run once one comes online', async () => {
        const h = await launched()
        h.agents.find(agent => agent.id === 'a-ok-2')!.status = 'offline'
        await h.push({ type: 'agent:status', payload: {} })
        h.runs.push({ id: 'r1', status: 'queued', created_at: h.at() } as Run)
        await h.executor.reconcile('t1')
        await h.advance(6 * MINUTE)
        await h.executor.reconcile('t1')
        // The queued run stays: its own workstation may still come back and claim it.
        expect(h.api.cancelRun).not.toHaveBeenCalled()
        const asked = (h.engine.report.mock.calls as unknown as [string, { question?: string }][])
          .filter(([, item]) => item.question?.includes('No Multica workstation'))
        expect(asked).toHaveLength(1)
        h.agents.find(agent => agent.id === 'a-ok-2')!.status = 'idle'
        await h.push({ type: 'agent:status', payload: {} })
        expect(h.api.cancelRun).toHaveBeenCalledWith('r1')
        expect(h.api.assignIssue).toHaveBeenCalledWith('issue-1', 'a-ok-2')
        expect(h.engine.report).toHaveBeenLastCalledWith('t1', expect.objectContaining({ question: null }))
      })

      it('refuses the late reports of the Assignment it moved away from', async () => {
        const h = await launched()
        const old = h.state()!.assignment!.nonce
        h.runs.push({ id: 'r1', status: 'queued', created_at: h.at() } as Run)
        await h.executor.reconcile('t1')
        await h.advance(5 * MINUTE)
        // The old worker reports before the new Assignment launches, and again after.
        h.comments.push({ ...agentComment('k1', report({ tool: 'report', state: 'ready', summary: 'late' }, old)), author_id: 'a-ok' })
        await h.executor.reconcile('t1')
        await relaunch(h)
        h.comments.push({ ...agentComment('k2', report({ tool: 'report', state: 'ready', summary: 'later' }, old)), author_id: 'a-ok' })
        await h.executor.reconcile('t1')
        expect(h.engine.report).not.toHaveBeenCalledWith('t1', expect.objectContaining({ state: 'ready' }))
      })

      it('never moves the ticket of a single MULTICA_AGENT_ID agent', async () => {
        const h = setup(ticketWith([{ state: 'pending' }]))
        await h.connect()
        await h.launch()
        h.setTicket(ticketWith([{}]))
        h.runs.push({ id: 'r1', status: 'queued', created_at: h.at() } as Run)
        await h.executor.reconcile('t1')
        await h.advance(10 * MINUTE)
        await h.executor.reconcile('t1')
        expect(h.engine.assignmentReassigned).not.toHaveBeenCalled()
      })
    })

    it('catches up on health when the event stream reconnects', async () => {
      const h = allOffline()
      await h.connect()
      await h.launch()
      await h.drop()
      h.agents.find(agent => agent.id === 'a-ok')!.status = 'idle'
      await h.connect()
      await h.runLaunches()
      expect(assignedTo(h)).toEqual(['a-ok'])
    })
  })
})

describe('PR22 independent race / failure checks', () => {
  function auditSetup(legacy = false) {
    const h = setup(ticketWith([{ state: 'pending' }]), { agents: ['std'], pickHealthy: true, standardOnly: !legacy })
    h.agents.push({ id: 'std', status: 'idle', runtime_id: 'r' })
    h.runtimes.push({ id: 'r', status: 'online' })
    return h
  }
  it('does not post when the only candidate is known offline before assignIssue returns', async () => {
    const h = auditSetup()
    h.setState({ issueId: 'issue-1', agentId: 'premium', deliveries: [], seen: [], runs: {} })
    await h.connect()
    h.api.assignIssue.mockImplementationOnce(async (_issue, agent) => {
      h.setAssignee(agent)
      h.runtimes[0]!.status = 'offline'
      await h.push({ type: 'runtime:updated', payload: {} })
      expect(h.api.runtimes).toHaveBeenCalledTimes(2)
    })
    await h.launch()
    expect(h.api.addComment).not.toHaveBeenCalled()
    expect(h.engine.assignmentStarted).not.toHaveBeenCalled()
  })
  it('does not post after a reopened issue loses its only healthy candidate', async () => {
    const h = auditSetup()
    h.setState({ issueId: 'issue-1', agentId: 'std', closed: 'done', deliveries: [], seen: [], runs: {} })
    await h.connect()
    h.api.setStatus.mockImplementationOnce(async () => {
      h.runtimes[0]!.status = 'offline'
      await h.push({ type: 'runtime:updated', payload: {} })
    })
    await h.launch()
    expect(h.api.setStatus).toHaveBeenCalledExactlyOnceWith('issue-1', 'todo')
    expect(h.api.addComment).not.toHaveBeenCalled()
    expect(h.engine.assignmentStarted).not.toHaveBeenCalled()
  })
  it('waits to create a new issue while health refreshes or has failed, then resumes on a valid snapshot', async () => {
    const h = auditSetup()
    await h.connect()
    const refresh = Promise.withResolvers<{ id: string; status: string }[]>()
    h.api.runtimes.mockImplementationOnce(() => refresh.promise)
    await h.push({ type: 'runtime:updated', payload: {} })
    await h.launch()
    expect(h.api.createIssue).not.toHaveBeenCalled()
    refresh.reject(new Error('Temporary runtime read failure'))
    await settle()
    await h.launch()
    expect(h.api.createIssue).not.toHaveBeenCalled()
    await h.advance(2000)
    await h.runLaunches()
    expect(h.api.createIssue).toHaveBeenCalledExactlyOnceWith(expect.objectContaining({ agentId: 'std' }))
  })
  it('checks the actual assignee again before posting and refuses a target that drifted', async () => {
    const h = auditSetup()
    h.setState({ issueId: 'issue-1', agentId: 'std', deliveries: [], seen: [], runs: {} })
    await h.connect()
    h.api.issue.mockResolvedValueOnce({ id: 'issue-1', assignee_type: 'agent', assignee_id: 'std' })
    h.api.issue.mockResolvedValueOnce({ id: 'issue-1', assignee_type: 'agent', assignee_id: 'premium' })
    await expect(h.launch()).rejects.toThrow('assignee changed')
    expect(h.api.assignIssue).not.toHaveBeenCalled()
    expect(h.api.addComment).not.toHaveBeenCalled()
  })
  it('retries a failed issue GET rather than taking it as an unassigned issue', async () => {
    const h = auditSetup()
    h.setState({ issueId: 'issue-1', agentId: 'premium', deliveries: [], seen: [], runs: {} })
    await h.connect()
    h.api.issue.mockRejectedValueOnce(new Error('Temporary assignee read failure'))
    await expect(h.launch()).rejects.toThrow('Temporary assignee read failure')
    expect(h.api.assignIssue).not.toHaveBeenCalled()
    expect(h.api.addComment).not.toHaveBeenCalled()
    expect(h.engine.report).not.toHaveBeenCalledWith('t1', expect.objectContaining({ state: 'failed' }))
    await h.launch()
    expect(h.api.assignIssue).toHaveBeenCalledExactlyOnceWith('issue-1', 'std')
    expect(h.engine.assignmentStarted).toHaveBeenCalledWith('t1', 'a1')
  })
  it('does not take a partial assignee record as confirmation that the issue is unassigned', async () => {
    const h = auditSetup()
    h.setState({ issueId: 'issue-1', agentId: 'premium', deliveries: [], seen: [], runs: {} })
    await h.connect()
    h.api.issue.mockResolvedValueOnce({ id: 'issue-1', assignee_type: 'agent', assignee_id: null })
    await expect(h.launch()).rejects.toThrow('incomplete assignee')
    expect(h.api.assignIssue).not.toHaveBeenCalled()
    expect(h.api.addComment).not.toHaveBeenCalled()
  })
  it('does not lose a final old-agent report if delivery sees run completion before reconciliation', async () => {
    const h = auditSetup()
    await h.connect()
    h.setState({ issueId: 'issue-1', agentId: 'premium', assignment: { id: 'a1', nonce: 'n' }, deliveries: [], seen: [], runs: { old: 'running' } })
    const inputs = [{ id: 'i1', kind: 'text', text: 'Follow up', at: '' }] as Ticket['inputs']
    h.setTicket(ticketWith([{}], { inputs, undelivered: ['i1'] }))
    h.runs.push({ id: 'old', agent_id: 'premium', status: 'running' })
    await h.deliver()
    h.comments.push({ ...agentComment('k1', report({ tool: 'report', state: 'progress', summary: 'Final output from old agent' }, 'n')), author_id: 'premium' })
    h.runs[0]!.status = 'completed'
    // The durable delivery wakes before the task/comment event is reconciled.
    await h.deliver()
    await h.executor.reconcile('t1')
    expect(h.engine.report).toHaveBeenCalledExactlyOnceWith('t1', { state: 'progress', summary: 'Final output from old agent' })
    expect(h.engine.report.mock.invocationCallOrder[0]!).toBeLessThan(h.api.assignIssue.mock.invocationCallOrder[0]!)
  })
  it('retries a transient assignment PUT failure for refusal resend', async () => {
    const h = auditSetup()
    await h.connect()
    h.setState({ issueId: 'issue-1', agentId: 'premium', assignment: { id: 'a1', nonce: 'n' }, deliveries: [], seen: [], runs: { old: 'running' } })
    h.setTicket(ticketWith([{}]))
    h.runs.push({ id: 'old', agent_id: 'premium', status: 'completed' })
    h.comments.push({ ...agentComment('k1', report({ tool: 'report', state: 'ready', summary: 'x' }, 'guess')), author_id: 'premium' })
    h.api.assignIssue.mockRejectedValueOnce(new Error('transient PUT failure'))
    await h.executor.reconcile('t1')
    expect(h.enqueued).toContainEqual({ kind: 'multicaReconcile', key: 't1' })
    expect(h.api.assignIssue).toHaveBeenCalledTimes(1)
    // Run the persisted effect's handler without relying on another issue/runtime event.
    await h.retryReconcile()
    expect(h.api.assignIssue).toHaveBeenCalledTimes(2)
    expect(h.posted).toContainEqual({ content: expect.stringMatching(/^superagent: .*comment k1/) })
  })
  it('legacy state without agentId keeps accepting original configured agent reports on assignee GET outage', async () => {
    const h = auditSetup(true)
    await h.connect()
    h.setState({ issueId: 'issue-1', assignment: { id: 'a1', nonce: 'n' }, deliveries: [], seen: [], runs: {} })
    h.setAssignee('std')
    h.setTicket(ticketWith([{}]))
    h.api.issue.mockRejectedValue(new Error('GET temporarily down'))
    h.comments.push({ ...agentComment('k1', report({ tool: 'report', state: 'progress', summary: 'From original agent' }, 'n')), author_id: 'std' })
    await h.executor.reconcile('t1')
    expect(h.engine.report).toHaveBeenCalledWith('t1', { state: 'progress', summary: 'From original agent' })
  })
})

describe('PR22 independent Multica routing', () => {
  function auditSetup() {
    const h = setup(ticketWith([{ state: 'pending' }]), { agents: ['stdA', 'stdB'], pickHealthy: true, standardOnly: true })
    h.agents.push({ id: 'stdA', status: 'offline', runtime_id: 'r' }, { id: 'stdB', status: 'idle', runtime_id: 'r' })
    h.runtimes.push({ id: 'r', status: 'online' })
    return h
  }
  it('posts refusal resend at top level so Multica does not route it to the original author', async () => {
    const h = auditSetup()
    await h.connect()
    h.setState({ issueId: 'issue-1', agentId: 'premium', assignment: { id: 'a1', nonce: 'n' }, deliveries: [], seen: [], runs: { old: 'running' } })
    h.setTicket(ticketWith([{}]))
    h.runs.push({ id: 'old', agent_id: 'premium', status: 'completed' })
    h.comments.push({ ...agentComment('k1', report({ tool: 'report', state: 'ready', summary: 'x' }, 'guess')), author_id: 'premium' })
    await h.executor.reconcile('t1')
    expect(h.api.assignIssue).toHaveBeenCalledExactlyOnceWith('issue-1', 'stdB')
    // Simulate routeReplyToParentAuthor (009 §8.1) vs routeAssigneeFallback.
    const targetOfResend = h.posted.filter(item => !item.content.startsWith('/note'))
      .map(item => item.parentId ? h.comments.find(c => c.id === item.parentId)?.author_id : h.state()?.agentId)
    expect(targetOfResend).toEqual(['stdB'])
  })
  it('recognizes a previously posted top-level resend by its stable marker', async () => {
    const h = auditSetup()
    await h.connect()
    h.setTicket(ticketWith([{}]))
    const resend = 'superagent: report block 1 in comment k1 was refused: wrong nonce.'
    h.setState({ issueId: 'issue-1', agentId: 'stdB', assignment: { id: 'a1', nonce: 'n' }, deliveries: [], seen: ['k1'], runs: {},
      unanswered: { assignmentId: 'a1', commentId: 'k1', resend } })
    h.comments.push({ id: 'already-posted', issue_id: 'issue-1', author_type: 'member', author_id: 'operator', created_at: '',
      content: `${resend}\n\n<!-- superagent-refusal-resend:a1:k1 -->` })
    await h.executor.reconcile('t1')
    expect(h.api.addComment).not.toHaveBeenCalled()
    expect(h.state()?.resentFor).toBe('a1')
  })
  it('serializes durable input delivery with failure-based reassignment', async () => {
    const h = auditSetup()
    await h.connect()
    h.setState({ issueId: 'issue-1', agentId: 'stdA', assignment: { id: 'a1', nonce: 'n' }, deliveries: [], seen: [], runs: { old: 'running' } })
    const inputs = [{ id: 'i1', kind: 'text', text: 'Follow up', at: '' }] as Ticket['inputs']
    h.setTicket(ticketWith([{}], { inputs, undelivered: ['i1'] }))
    h.runs.push({ id: 'old', agent_id: 'stdA', status: 'failed', failure_reason: 'runtime_offline' })
    const putReached = Promise.withResolvers<void>()
    const put = Promise.withResolvers<void>()
    h.api.assignIssue.mockImplementationOnce(async (_issue, agent) => { putReached.resolve(); await put.promise; h.setAssignee(agent) })
    const failover = h.executor.reconcile('t1')
    await putReached.promise
    const delivery = h.deliver()
    await settle()
    const callsDuringFirstPut = h.api.assignIssue.mock.calls.length
    const postsDuringFirstPut = h.posted.length
    put.resolve()
    await Promise.all([failover, delivery])
    expect([callsDuringFirstPut, postsDuringFirstPut]).toEqual([1, 0])
    expect(h.engine.assignmentReassigned).toHaveBeenCalledOnce()
    expect(h.api.addComment).not.toHaveBeenCalled()
  })
  it('cancels a server auto-retry for an excluded agent even without Inputs', async () => {
    const h = auditSetup()
    await h.connect()
    h.setState({ issueId: 'issue-1', agentId: 'premium', assignment: { id: 'a1', nonce: 'n' }, deliveries: [], seen: [], runs: { old: 'running' } })
    h.setTicket(ticketWith([{}]))
    h.runs.push({ id: 'old', agent_id: 'premium', status: 'failed', failure_reason: 'runtime_offline' },
      { id: 'auto-retry', agent_id: 'premium', status: 'deferred' }, { id: 'claimed', agent_id: 'premium', status: 'running' })
    await h.executor.reconcile('t1')
    expect(h.api.cancelRun).toHaveBeenCalledExactlyOnceWith('auto-retry')
  })
})

describe('PR22 recheck additional edge cases', () => {
  async function auditSetup() {
    const h = setup(ticketWith([{}]), { agents: ['std'], pickHealthy: true, standardOnly: true })
    h.agents.push({ id: 'std', status: 'idle', runtime_id: 'r' })
    h.runtimes.push({ id: 'r', status: 'online' })
    await h.connect()
    return h
  }
  it('persists a successful failed-steer resend before attempting another', async () => {
    const h = await auditSetup()
    h.setState({ issueId: 'issue-1', agentId: 'std', assignment: { id: 'a1', nonce: 'n' }, seen: [], runs: { old: 'completed' }, deliveries: [
      { commentId: 's1', inputIds: ['i1'], assignmentId: 'a1', read: false, steeredRun: 'old' },
      { commentId: 's2', inputIds: ['i2'], assignmentId: 'a1', read: false, steeredRun: 'old' },
    ] })
    h.runs.push({ id: 'old', agent_id: 'std', status: 'completed' })
    for (const [id, content] of [['s1', 'steer one'], ['s2', 'steer two']] as const) {
      h.comments.push({ id, issue_id: 'issue-1', author_type: 'member', content, created_at: '' })
      h.receipts.set(id, [{ task_id: 'old', status: 'failed', failure_reason: 'turn_ended' }])
    }
    const ok = h.api.addComment.getMockImplementation()!
    h.api.addComment.mockImplementationOnce(ok).mockRejectedValueOnce(new Error('second resend failed before committing'))
    await h.executor.reconcile('t1')
    expect(h.enqueued).toContainEqual({ kind: 'multicaReconcile', key: 't1' })
    await h.retryReconcile()
    expect(h.posted.map(item => item.content)).toEqual(['steer one', 'steer two'])
  })
  it('replays an unread Input on a healthy assignee after cancelling its excluded unclaimed run', async () => {
    const h = await auditSetup()
    h.runtimes[0]!.status = 'offline'
    await h.push({ type: 'runtime:updated', payload: {} })
    h.setState({ issueId: 'issue-1', agentId: 'premium', assignment: { id: 'a1', nonce: 'n' }, seen: [], runs: { old: 'queued' }, deliveries: [
      { commentId: 'input', inputIds: ['i1'], assignmentId: 'a1', read: false },
    ] })
    h.runs.push({ id: 'old', agent_id: 'premium', trigger_comment_id: 'input', delivered_comment_ids: ['input'], status: 'queued' })
    h.comments.push({ id: 'input', issue_id: 'issue-1', author_type: 'member', content: 'Original input', created_at: '' })
    await h.executor.reconcile('t1')
    expect(h.api.cancelRun).toHaveBeenCalledExactlyOnceWith('old')
    expect(h.engine.inputsRead).not.toHaveBeenCalled()
    expect(h.api.addComment).not.toHaveBeenCalled()
    expect(h.state()!.deliveries[0]).toMatchObject({ commentId: 'input', read: false, replayAfterCancel: 'old' })
    h.runtimes[0]!.status = 'online'
    await h.push({ type: 'runtime:updated', payload: {} })
    expect(h.api.assignIssue).toHaveBeenCalledExactlyOnceWith('issue-1', 'std')
    expect(h.api.assignIssue.mock.invocationCallOrder[0]!).toBeLessThan(h.api.addComment.mock.invocationCallOrder[0]!)
    expect(h.posted.map(item => item.content)).toEqual(['Original input'])
    expect(h.engine.inputsRead).not.toHaveBeenCalled()
    h.runs.push({ id: 'new', agent_id: 'std', trigger_comment_id: h.state()!.deliveries[0]!.commentId, status: 'running' })
    await h.executor.reconcile('t1')
    expect(h.engine.inputsRead).toHaveBeenCalledExactlyOnceWith('t1', ['i1'])
  })
  it('keeps an Input read without replay if the excluded daemon started before cancellation arrived', async () => {
    const h = await auditSetup()
    h.setState({ issueId: 'issue-1', agentId: 'premium', assignment: { id: 'a1', nonce: 'n' }, seen: [], runs: { old: 'queued' }, deliveries: [
      { commentId: 'input', inputIds: ['i1'], assignmentId: 'a1', read: false },
    ] })
    h.runs.push({ id: 'old', agent_id: 'premium', trigger_comment_id: 'input', status: 'queued' })
    h.comments.push({ id: 'input', issue_id: 'issue-1', author_type: 'member', content: 'Original input', created_at: '' })
    h.api.cancelRun.mockImplementationOnce(async () => { h.runs[0]!.started_at = h.at(); h.runs[0]!.status = 'cancelled' })
    await h.executor.reconcile('t1')
    expect(h.engine.inputsRead).toHaveBeenCalledExactlyOnceWith('t1', ['i1'])
    expect(h.api.addComment).not.toHaveBeenCalled()
  })
  it('retains the active run author for a legacy record without agentId when the actual issue assignee already changed', async () => {
    const h = await auditSetup()
    h.setState({ issueId: 'issue-1', assignment: { id: 'a1', nonce: 'n' }, seen: [], runs: { legacy: 'running' }, deliveries: [] })
    h.setAssignee('std')
    h.runs.push({ id: 'legacy', agent_id: 'premium', status: 'completed' })
    h.comments.push({ ...agentComment('k1', report({ tool: 'report', state: 'progress', summary: 'From legitimate old run' }, 'n')), author_id: 'premium' })
    await h.executor.reconcile('t1')
    expect(h.engine.report).toHaveBeenCalledWith('t1', { state: 'progress', summary: 'From legitimate old run' })
  })
  it('explicitly migrates an unknown author even if the current assignee is already in the standard pool', async () => {
    const h = await auditSetup()
    h.setState({ issueId: 'issue-1', assignment: { id: 'a1', nonce: 'n' }, seen: [], runs: {}, deliveries: [] })
    h.setAssignee('std')
    h.setTicket(ticketWith([{}], { inputs: [{ id: 'i1', kind: 'text', text: 'Follow up', at: '' }] as Ticket['inputs'], undelivered: ['i1'] }))
    await h.deliver()
    expect(h.api.assignIssue).toHaveBeenCalledExactlyOnceWith('issue-1', 'std')
    expect(h.api.addComment).toHaveBeenCalledOnce()
    expect(h.state()!.assignment!.reportAgentId).toBe('std')
  })
  it.each([403, 404])('does not return a permanent %i assignee preflight error to the retrying delivery effect', async status => {
    const { MulticaError } = await import('../src/executor/multica-api.ts')
    const h = await auditSetup()
    h.setState({ issueId: 'issue-1', agentId: 'std', assignment: { id: 'a1', nonce: 'n' }, seen: [], runs: {}, deliveries: [] })
    h.setTicket(ticketWith([{}], { inputs: [{ id: 'i1', kind: 'text', text: 'Follow up', at: '' }] as Ticket['inputs'], undelivered: ['i1'] }))
    h.api.issue.mockRejectedValue(new MulticaError('permanent GET failure', status))
    await expect(h.deliver()).resolves.toBeUndefined()
    expect(h.engine.deliveryFailed).toHaveBeenCalledWith('t1', 'a1', expect.stringContaining('permanent GET failure'))
    expect(h.health.get()).toMatchObject({ state: 'error', detail: expect.stringContaining('permanent GET failure') })
  })
  it('still retries a transient assignee preflight error', async () => {
    const { MulticaError } = await import('../src/executor/multica-api.ts')
    const h = await auditSetup()
    h.setState({ issueId: 'issue-1', agentId: 'std', assignment: { id: 'a1', nonce: 'n' }, seen: [], runs: {}, deliveries: [] })
    h.setTicket(ticketWith([{}], { inputs: [{ id: 'i1', kind: 'text', text: 'Follow up', at: '' }] as Ticket['inputs'], undelivered: ['i1'] }))
    h.api.issue.mockRejectedValueOnce(new MulticaError('temporary GET failure', 503))
    await expect(h.deliver()).rejects.toThrow('temporary GET failure')
    expect(h.api.addComment).not.toHaveBeenCalled()
    await h.deliver()
    expect(h.api.addComment).toHaveBeenCalledOnce()
  })
})

it('PR22 recheck additional legacy launch preserves main fallback identity without GET', async () => {
  const h = setup(ticketWith([{ state: 'ended' }, { state: 'pending' }]), { agents: [AGENT, 'backup'], pickHealthy: true })
  h.setState({ issueId: 'issue-1', assignment: { id: 'a1', nonce: 'before' }, seen: [], runs: {}, deliveries: [] })
  h.setAssignee('backup')
  await h.launch('a2')
  h.setTicket(ticketWith([{ state: 'ended' }, {}]))
  h.comments.push(agentComment('k1', report({ tool: 'report', state: 'progress', summary: 'Original configured worker' }, h.state()!.assignment!.nonce, 'a2')))
  await h.executor.reconcile('t1')
  expect(h.api.assignIssue).not.toHaveBeenCalled()
  expect(h.engine.report).toHaveBeenCalledWith('t1', { state: 'progress', summary: 'Original configured worker' })
  expect(h.api.issue).not.toHaveBeenCalled()
})
