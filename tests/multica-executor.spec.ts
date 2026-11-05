/** MulticaExecutor against a fake Multica API and a recording Engine. */
import { describe, expect, it, vi } from 'vitest'
import type { Assignment, Ticket, Trigger } from '../src/core/index.ts'
import type { Comment, PostedComment, Run } from '../src/executor/multica-api.ts'
import { MULTICA_KV, MulticaExecutor, type MulticaExecutorHost, type MulticaTicketState } from '../src/executor/multica.ts'
import { REPORT_FENCE } from '../src/executor/multica-protocol.ts'

const AGENT = 'agent-1'
const trigger = { id: 'tr1', text: 'Please fix the README typo', at: '2026-11-05T00:00:00Z', actor: { id: 'jared', source: 'github' },
  threads: [{ source: 'github', id: '2' }] } as unknown as Trigger

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

function setup(initial: Ticket, options: { agents?: string[]; pickHealthy?: boolean } = {}) {
  let ticket = initial
  const sockets: FakeSocket[] = []
  const agents: AgentRow[] = []
  const runtimes: RuntimeRow[] = []
  const kv = new Map<string, unknown>()
  const handlers: Record<string, (payload: never) => Promise<void>> = {}
  const enqueued: { kind: string; key: string }[] = []
  const runs: Run[] = []
  const comments: Comment[] = []
  const posted: { content: string; steer?: string[]; parentId?: string }[] = []
  let outcome = 'queued'
  let postFailures = 0
  const engine = {
    assignmentStarted: vi.fn(), inputsDelivered: vi.fn(), inputsRead: vi.fn(), turnEnded: vi.fn(), deliveryFailed: vi.fn(),
    report: vi.fn(() => ticket), reply: vi.fn(() => ({ id: 'r', status: 'sent', reused: false })), handback: vi.fn(() => true),
    followForContext: vi.fn(() => true), markUnrelated: vi.fn(),
  }
  const health = { get: vi.fn(), set: vi.fn(), release: vi.fn() }
  const api = {
    createIssue: vi.fn(async () => ({ id: 'issue-1', identifier: 'JAR-1' })),
    setMetadata: vi.fn(async () => {}),
    findIssues: vi.fn(async () => []),
    runs: vi.fn(async () => runs.map(run => ({ ...run }))),
    comments: vi.fn(async () => comments.map(comment => ({ ...comment }))),
    addComment: vi.fn(async (_issue: string, content: string, options: { steer?: string[]; parentId?: string } = {}): Promise<PostedComment> => {
      if (postFailures > 0) { postFailures--; throw new Error('Multica POST failed') }
      posted.push({ content, ...options })
      const comment = { id: `c${posted.length}`, issue_id: 'issue-1', author_type: 'member', author_id: 'operator', content, created_at: '',
        ...options.parentId ? { parent_id: options.parentId } : {} }
      comments.push(comment)
      return { ...comment, trigger_outcomes: content.startsWith('/note') ? [] : [{ status: outcome, target_id: AGENT }] }
    }),
    cancelRun: vi.fn(async () => {}),
    agents: vi.fn(async () => agents.map(agent => ({ ...agent }))),
    runtimes: vi.fn(async () => runtimes.map(runtime => ({ ...runtime }))),
  }
  const host = {
    engine, health, api,
    store: {
      tickets: { get: () => ticket, open: () => ticket.ended ? [] : [ticket] },
      triggers: { get: (id: string) => id === 'tr1' ? trigger : undefined },
      get: (key: string) => kv.get(key), set: (key: string, value: unknown) => { kv.set(key, value) }, delete: (key: string) => { kv.delete(key) },
      settings: () => ({ policy: { rules: [] }, executor: { directory: '' } }), atomic: <T>(work: () => T) => work(),
    },
    effects: { enqueue: (kind: string, key: string) => { enqueued.push({ kind, key }) },
      handle: (kind: string, run: (payload: never) => Promise<void>) => { handlers[kind] = run } },
    sessions: { relay: vi.fn() },
    machine: { stateDir: '/state' }, lifetime: new AbortController().signal,
    config: { serverUrl: 'https://multica.test', workspaceId: 'w', token: 'mul_x', agents: options.agents ?? [AGENT],
      pickHealthy: options.pickHealthy ?? false, repository: 'https://github.com/o/r' },
    socketFactory: () => { const socket = new FakeSocket(); sockets.push(socket); return socket },
    closing: () => false, paused: () => false,
    sourceLink: async () => ({ source_url: 'https://github.com/o/r/issues/2' }), repliedTo: () => undefined, sendReplies: vi.fn(async () => {}),
    claimPull: vi.fn(async (): Promise<string | undefined> => undefined),
  } as unknown as MulticaExecutorHost
  let executor = new MulticaExecutor(host)
  return {
    get executor() { return executor }, engine, api, runs, comments, posted, enqueued, health, host, agents, runtimes,
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
    setState: (next: MulticaTicketState) => { kv.set(MULTICA_KV.ticket('t1'), next) },
    failPosts: (count: number) => { postFailures = count },
    state: () => kv.get(MULTICA_KV.ticket('t1')) as MulticaTicketState | undefined,
    setTicket: (next: Ticket) => { ticket = next },
    setOutcome: (next: string) => { outcome = next },
    launch: (assignmentId = 'a1') => handlers.launch!({ ticketId: 't1', assignmentId } as never),
    deliver: () => handlers.delivery!({ ticketId: 't1' } as never),
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

  it('steers a running run that accepts more messages, which reads them at once', async () => {
    const h = setup(ticketWith([{ state: 'pending' }]))
    await h.launch()
    h.setTicket(ticketWith([{}], { inputs: [{ id: 'i1', kind: 'text', text: 'Hurry', at: '' }], undelivered: ['i1'] } as unknown as Partial<Ticket>))
    h.runs.push({ id: 'r1', status: 'running', can_supplement: true })
    h.setOutcome('steered')
    await h.deliver()
    expect(h.posted[0]!.steer).toEqual(['r1'])
    expect(h.engine.inputsRead).toHaveBeenCalledWith('t1', ['i1'])
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
    h.executor.stop(ticketWith([{}]), { id: 'a1' } as Assignment, 'stopped')
    await vi.waitFor(() => { expect(h.api.cancelRun).toHaveBeenCalledWith('r1') })
    h.runs[0]!.status = 'cancelled'
    await h.executor.reconcile('t1')
    expect(h.engine.turnEnded).not.toHaveBeenCalled()
  })

  describe('refused report blocks', () => {
    const notes = (h: ReturnType<typeof setup>) => h.posted.filter(item => item.content.startsWith('/note'))
    async function running() {
      const h = setup(ticketWith([{ state: 'pending' }]))
      await h.launch()
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
    function candidates() {
      const h = setup(ticketWith([{ state: 'pending' }]), { agents: CANDIDATES, pickHealthy: true })
      h.runtimes.push({ id: 'rt-down', status: 'offline' }, { id: 'rt-up', status: 'online' })
      h.agents.push({ id: 'a-off-runtime', status: 'idle', runtime_id: 'rt-down' }, { id: 'a-off', status: 'offline', runtime_id: 'rt-up' },
        { id: 'a-ok', status: 'working', runtime_id: 'rt-up' }, { id: 'a-ok-2', status: 'idle', runtime_id: 'rt-up' })
      return h
    }
    const assignedTo = (h: ReturnType<typeof setup>) =>
      (h.api.createIssue.mock.calls as unknown as [{ agentId: string }][]).map(([input]) => input.agentId)

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
    function allOffline() {
      const h = candidates()
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
