/**
 * MulticaExecutor runs ticket assignments on the operator's own machines through Multica instead of local DSH worker
 * sessions: an Assignment is a Multica issue assigned to an agent (one runtime on one machine), its turns are the issue's
 * runs, Inputs reach it as comments (or steer a running run that accepts them), and its worker reports through
 * `superagent-report` comment blocks (see `multica-protocol.ts`). Facts arrive by push over the workspace WebSocket; each
 * (re)connection catches up once through REST. See docs/009-multica-executor.md.
 */
import { latestAssignment, liveTicket, runningAssignment, type Assignment, type EngineForExecutor, type Ticket, type Trigger }
  from '../core/index.ts'
import type { AssessmentOutcome, Executor } from '../core/ports.ts'
import type { WorkerReport } from '../core/assignments.ts'
import type { Effects } from '../app/effects.ts'
import { Later } from '../app/effects.ts'
import type { Store } from '../app/store.ts'
import type { Sessions } from '../app/sessions.ts'
import { reportRelay } from '../app/notices.ts'
import type { HealthEntry } from '../infra/health.ts'
import { family } from '../kv.ts'
import { batchLine, batchOf, inputMessage, sourceTrigger, ticketExecution, triggerMessage } from './dsh.ts'
import type { Comment, MulticaApi, Run } from './multica-api.ts'
import { MulticaCandidates } from './multica-candidates.ts'
import { MulticaEvents, type MulticaEvent, type SocketFactory } from './multica-events.ts'
import type { MulticaConnection } from './multica-api.ts'
import { ENDED_STATUSES, WAITING_STATUSES, inputComment, isRefusalNote, issueDescription, newNonce, parseReports, planDelivery,
  laterAssignmentComment, refusalNote, refusedOutcome, type Brief, type ReportBlock } from './multica-protocol.ts'

/** Which agents run the tickets, and what every worker is told about where to work. */
export interface MulticaExecutorConfig extends MulticaConnection {
  /** The Multica agents a new ticket's issue may be assigned to, in order of preference: each one runtime on one machine. */
  agents: readonly string[]
  /**
   * Whether a new ticket goes to the first healthy agent and waits while none is (`MULTICA_AGENT_IDS`). Without it the
   * first agent gets every ticket whatever its state, as with the single `MULTICA_AGENT_ID`, and Multica queues the run.
   */
  pickHealthy: boolean
  /** Git remote workers clone, such as `https://github.com/owner/repo`. */
  repository?: string
  /** Extra instructions for every worker, such as the git identity to commit and open PRs with. */
  instructions?: string
}

/**
 * The Multica executor's configuration from the environment, or undefined to keep the DSH executor: `SUPERAGENT_EXECUTOR=multica`
 * selects it, with `MULTICA_TOKEN`, `MULTICA_WORKSPACE_ID` and either `MULTICA_AGENT_IDS` (ordered, comma-separated candidates
 * picked by health) or `MULTICA_AGENT_ID` (one agent, no health check) required; the list wins when both are set.
 * `MULTICA_SERVER_URL`, `SUPERAGENT_MULTICA_REPOSITORY` and `SUPERAGENT_MULTICA_INSTRUCTIONS` are optional. The token stays
 * in the environment, never in settings.
 * @throws Error naming the missing variables when the Multica executor is selected without them.
 */
export function multicaConfigFromEnv(env: Record<string, string | undefined>): MulticaExecutorConfig | undefined {
  if (env.SUPERAGENT_EXECUTOR !== 'multica') return undefined
  const candidates = (env.MULTICA_AGENT_IDS ?? '').split(',').map(id => id.trim()).filter(Boolean)
  const single = env.MULTICA_AGENT_ID?.trim()
  const missing = ['MULTICA_TOKEN', 'MULTICA_WORKSPACE_ID'].filter(name => !env[name]?.trim())
  if (!candidates.length && !single) missing.push('MULTICA_AGENT_ID or MULTICA_AGENT_IDS')
  if (missing.length) throw new Error(`SUPERAGENT_EXECUTOR=multica needs ${missing.join(', ')}`)
  const repository = env.SUPERAGENT_MULTICA_REPOSITORY?.trim(), instructions = env.SUPERAGENT_MULTICA_INSTRUCTIONS?.trim()
  return { serverUrl: env.MULTICA_SERVER_URL?.trim() || 'https://api.multica.ai', token: env.MULTICA_TOKEN!.trim(),
    workspaceId: env.MULTICA_WORKSPACE_ID!.trim(), agents: candidates.length ? candidates : [single!], pickHealthy: candidates.length > 0,
    ...repository ? { repository } : {}, ...instructions ? { instructions } : {} }
}

/** What MulticaExecutor keeps per ticket, under `multica-ticket:<ticket id>`. */
export interface MulticaTicketState {
  issueId: string
  /** The Multica agent the issue is assigned to; states saved before candidates existed lack it and mean the first configured agent. */
  agentId?: string
  identifier?: string
  /** The Assignment the issue's worker reports for now, with the nonce its reports must carry. */
  assignment?: { id: string; nonce: string }
  /** Comments that carried Inputs, and whether a run has taken them in. */
  deliveries: { commentId: string; inputIds: string[]; assignmentId: string; read?: boolean }[]
  /** Agent comments already processed for report blocks. */
  seen: string[]
  /** The last status seen per run. */
  runs: Record<string, string>
  /** Runs this executor cancelled, whose end is no turn end of the worker's. */
  cancelled?: string[]
  /** Refused report blocks whose note to the worker has not reached Multica yet. */
  refusals?: Refusal[]
}
/** A refused report block: the `index`th block of an agent comment, and the note that tells its worker why. */
export interface Refusal { commentId: string; index: number; note: string }
export const MULTICA_KV = {
  ticket: family<MulticaTicketState>('multica-ticket:'),
  issue: family<string>('multica-issue:'),
  /** A ticket whose first Assignment waits for a healthy candidate, once a person has been asked to start a workstation. */
  waiting: family<{ assignmentId: string }>('multica-waiting:'),
}
const SEEN_LIMIT = 500
/** How many cancelled runs, deliveries and pending refusal notes a ticket's state keeps. */
const HISTORY_LIMIT = 50

export interface MulticaExecutorHost {
  readonly engine: Pick<EngineForExecutor, 'assignmentStarted' | 'inputsDelivered' | 'inputsRead' | 'turnEnded' | 'deliveryFailed' | 'report'
    | 'reply' | 'handback' | 'followForContext' | 'markUnrelated'>
  readonly store: Pick<Store, 'tickets' | 'triggers' | 'get' | 'set' | 'delete' | 'settings' | 'atomic'>
  readonly effects: Pick<Effects, 'enqueue' | 'handle'>
  readonly sessions: Pick<Sessions, 'relay'>
  /** The `worker` health entry: the event stream's state and the last failure. */
  readonly health: HealthEntry
  readonly machine: { stateDir: string }
  readonly lifetime: AbortSignal
  readonly config: MulticaExecutorConfig
  readonly api: Pick<MulticaApi, 'createIssue' | 'setMetadata' | 'findIssues' | 'runs' | 'comments' | 'addComment' | 'cancelRun' | 'agents' | 'runtimes'>
  /** Builds the event stream's sockets; tests pass their own. */
  readonly socketFactory?: SocketFactory
  closing(): boolean
  paused(): boolean
  sourceLink(trigger: Trigger, signal?: AbortSignal): Promise<{ source_url?: string; source_url_read?: string }>
  repliedTo(trigger: Trigger): Trigger | undefined
  /** Sends queued replies, such as a worker's acknowledgement, now. */
  sendReplies(): Promise<void>
  /** Claims a reported pull request for the ticket; returns why it was refused, or undefined. */
  claimPull?(ticketId: string, number: number, signal?: AbortSignal): Promise<string | undefined>
}

const now = () => new Date().toISOString()
const errorText = (error: unknown) => error instanceof Error ? error.message : String(error)
const issueOf = (event: MulticaEvent): string | undefined => {
  const payload = event.payload
  const comment = payload.comment as { issue_id?: unknown } | undefined
  const issue = payload.issue as { id?: unknown } | undefined
  const id = payload.issue_id ?? comment?.issue_id ?? issue?.id
  return typeof id === 'string' ? id : undefined
}

/**
 * Executes ticket assignments as Multica issues. Launches and deliveries run through the durable effect queue; run and
 * comment facts come from the workspace event stream and are reconciled per ticket, one reconciliation at a time.
 */
export class MulticaExecutor implements Executor {
  private readonly events: MulticaEvents
  /** Per-ticket reconciliation chains, so two events for one ticket never reconcile at once. */
  private readonly chains = new Map<string, Promise<void>>()
  private readonly again = new Set<string>()
  private readonly candidates: MulticaCandidates
  private refreshing: Promise<void> | undefined
  private refreshAgain = false
  /** Tickets whose launch waits for a healthy candidate. */
  private readonly waiting = new Set<string>()
  constructor(private readonly host: MulticaExecutorHost) {
    this.candidates = new MulticaCandidates(host.config.agents)
    this.events = new MulticaEvents(host.config, {
      connected: () => { this.healthy('Connected to Multica'); this.refreshCandidates(); this.catchUp() },
      event: event => { this.event(event) },
      disconnected: reason => { this.host.health.set({ state: 'error', detail: `Multica event stream: ${reason}; reconnecting`, at: now() }) },
    }, host.socketFactory)
    host.effects.handle('launch', payload => this.launchQueued(payload), { limit: () => host.paused() ? 0 : 4, retry: [5000, 300_000] })
    host.effects.handle('delivery', payload => this.deliverQueued(payload.ticketId), { limit: () => host.paused() ? 0 : 4, quiet: true })
  }
  /** Execution metadata for a newly accepted ticket; its directory names the branch remote workers keep their work on. */
  prepare(ticket: Ticket, outcome: Extract<AssessmentOutcome, { kind: 'accept' }>): Ticket {
    return { ...ticket, execution: ticketExecution(ticket, outcome, this.host.store.settings(), this.host.machine.stateDir) }
  }
  start(ticket: Ticket, assignment: Assignment): void {
    this.host.effects.enqueue('launch', assignment.id, { ticketId: ticket.id, assignmentId: assignment.id })
  }
  deliver(ticket: Ticket, _assignment: Assignment): void { this.host.effects.enqueue('delivery', ticket.id, { ticketId: ticket.id }) }
  /** Cancels the issue's active runs; the domain already ended the Assignment. */
  stop(ticket: Ticket, _assignment: Assignment, _reason: string): void {
    const state = this.state(ticket.id)
    if (!state) return
    void this.cancelActive(ticket.id, state.issueId).catch((error: unknown) => { this.failure(`Cancelling ${state.identifier ?? state.issueId}`, error) })
  }
  /** Connects the event stream. */
  install(): void { this.events.start() }
  /** After a restart: remote runs went on meanwhile, so nothing is interrupted; pending launches are queued again. */
  resume(): void {
    for (const ticket of this.host.store.tickets.open().sort((a, b) => a.createdAt.localeCompare(b.createdAt))) {
      const latest = latestAssignment(ticket)
      if (latest?.state === 'pending') this.start(ticket, latest)
    }
  }
  async shutdown(): Promise<void> { this.events.stop(); await Promise.allSettled([...this.chains.values()]) }
  dispose(): void { this.events.stop() }
  /** Open tickets whose issue has a run in progress. */
  activeCount(): number {
    return this.host.store.tickets.open().filter(ticket => runningAssignment(ticket)
      && Object.values(this.state(ticket.id)?.runs ?? {}).some(status => status === 'running')).length
  }

  private state(ticketId: string): MulticaTicketState | undefined { return this.host.store.get(MULTICA_KV.ticket(ticketId)) }
  private save(ticketId: string, state: MulticaTicketState): void {
    this.host.store.set(MULTICA_KV.ticket(ticketId), state)
    this.host.store.set(MULTICA_KV.issue(state.issueId), ticketId)
  }
  private agentOf(state: MulticaTicketState): string { return state.agentId ?? this.host.config.agents[0]! }
  private healthy(detail: string): void { this.host.health.set({ state: 'ready', detail, at: now() }) }
  private failure(what: string, error: unknown): void {
    this.host.health.set({ state: 'error', detail: `${what}: ${errorText(error)}`, at: now() })
  }

  /** The assignment a remote worker receives: what a DSH worker's first message holds, with trigger texts in full. */
  private async assignmentText(ticket: Ticket): Promise<string> {
    const source = sourceTrigger(this.host, ticket)
    const card = source ? await this.triggerCard(source) : undefined
    const batch = batchOf(this.host, ticket).map(trigger => ({ ...batchLine(trigger), text: trigger.text }))
    const retried = ticket.assignments.at(-2)?.endedReason === 'retried'
    return JSON.stringify({ ticket_id: ticket.id, title: ticket.title, ...ticket.execution?.assignment ? { assignment: ticket.execution.assignment } : {},
      ...card ? { trigger: card } : {}, ...batch.length ? { batch } : {},
      ...retried ? { note: 'A person retried this ticket. Continue from the branch, its history and the PR state.' } : {} }, null, 2)
  }
  /** A Trigger as the worker sees it: like a DSH worker's card, but with its full text, since remote workers cannot read resources. */
  private async triggerCard(trigger: Trigger, note?: string): Promise<Record<string, unknown>> {
    const link = await this.host.sourceLink(trigger, this.host.lifetime).catch(() => ({}))
    const { read: _read, chars: _chars, ...card } = triggerMessage(trigger, link, note, this.host.repliedTo(trigger)) as Record<string, unknown>
    return { ...card, text: trigger.text }
  }
  private brief(ticket: Ticket, assignment: Assignment, nonce: string, text: string): Brief {
    const { repository, instructions } = this.host.config
    return { ticketId: ticket.id, assignmentId: assignment.id, nonce, title: ticket.title, assignment: text,
      branch: `agent/${ticket.execution?.name ?? ticket.id}`, ...repository ? { repository } : {}, ...instructions ? { instructions } : {} }
  }

  /** Starts a queued Assignment: creates the ticket's issue, or tells the existing one about the new Assignment. */
  private async launchQueued({ ticketId, assignmentId }: { ticketId: string; assignmentId: string }): Promise<void> {
    const current = () => {
      if (this.host.closing()) throw new Error('dsh-superagent is stopping')
      if (this.host.paused()) throw new Later('Processing is paused')
      const stored = this.host.store.tickets.get(ticketId)
      const assignment = stored && liveTicket(stored) ? latestAssignment(stored) : undefined
      return assignment?.id === assignmentId && assignment.state === 'pending' ? { ticket: stored!, assignment } : undefined
    }
    const first = current()
    if (!first) return
    try {
      const nonce = newNonce()
      const brief = this.brief(first.ticket, first.assignment, nonce, await this.assignmentText(first.ticket))
      if (!current()) return
      let state = this.state(ticketId)
      if (!state) {
        // An issue created before a crash, whose state was never saved, is found again by its metadata.
        const [found] = await this.host.api.findIssues({ superagent_ticket: ticketId })
        if (found) state = { issueId: found.id, identifier: found.identifier ?? undefined, deliveries: [], seen: [], runs: {} }
      }
      if (!state) {
        const agentId = this.choose()
        if (!agentId) { this.wait(ticketId, assignmentId); return }
        const issue = await this.host.api.createIssue({ title: first.ticket.title, description: issueDescription(brief), agentId })
        // Save before anything else can fail, so the run the issue started reports into a known Assignment.
        this.save(ticketId, { issueId: issue.id, identifier: issue.identifier ?? undefined, agentId, assignment: { id: assignmentId, nonce },
          deliveries: [], seen: [], runs: {} })
        await this.host.api.setMetadata(issue.id, 'superagent_ticket', ticketId)
        this.dispatched(ticketId, agentId)
      } else {
        // Runs of earlier Assignments are the baseline: only runs that start from here on are this Assignment's turns.
        const before = await this.host.api.runs(state.issueId)
        this.save(ticketId, { ...state, assignment: { id: assignmentId, nonce }, runs: Object.fromEntries(before.map(run => [run.id, run.status])),
          cancelled: [...state.cancelled ?? [], ...before.filter(run => !ENDED_STATUSES.has(run.status)).map(run => run.id)].slice(-HISTORY_LIMIT) })
        const posted = await this.host.api.addComment(state.issueId, laterAssignmentComment(brief))
        const refused = refusedOutcome(posted.trigger_outcomes ?? undefined, this.agentOf(state))
        if (refused) throw new Error(`Multica did not start the agent: ${refused}`)
      }
      await this.host.api.setMetadata(this.state(ticketId)!.issueId, 'superagent_assignment', assignmentId).catch(() => { /* Informational only. */ })
      if (!current()) return
      this.host.engine.assignmentStarted(ticketId, assignmentId)
      this.schedule(ticketId)
    } catch (error) {
      if (this.host.closing() || error instanceof Later) throw error
      this.failure('Launching on Multica', error)
      if (current()) this.reportFailed(ticketId, `Could not start the assignment on Multica: ${errorText(error)}`)
    }
  }

  /** The agent a new ticket's issue goes to: the first configured one, or with `pickHealthy` the first healthy candidate. */
  private choose(): string | undefined {
    return this.host.config.pickHealthy ? this.candidates.pick() : this.host.config.agents[0]
  }
  /**
   * Keeps a new ticket queued while no candidate is healthy. Once health is known, a person is asked to start a
   * workstation, once per Assignment: the question stays on the ticket across reconnections and restarts.
   */
  private wait(ticketId: string, assignmentId: string): void {
    this.waiting.add(ticketId)
    if (!this.candidates.known || this.host.store.get(MULTICA_KV.waiting(ticketId))?.assignmentId === assignmentId) return
    this.record(ticketId, { state: 'progress', summary: 'Waiting for a Multica workstation to come online', question: this.noMachine() },
      () => { this.host.store.set(MULTICA_KV.waiting(ticketId), { assignmentId }) })
  }
  private noMachine(): string {
    return `No Multica workstation is online to run this ticket: none of the candidate agents (${this.host.config.agents.join(', ')}) `
      + 'is online. Start one of their machines; the ticket is dispatched as soon as one is online.'
  }
  /** Takes a new snapshot of the candidates' health; refreshes asked for while one runs fold into one more. */
  private refreshCandidates(): void {
    if (!this.host.config.pickHealthy) return
    if (this.refreshing) { this.refreshAgain = true; return }
    const run = async (): Promise<void> => {
      try {
        const [agents, runtimes] = await Promise.all([this.host.api.agents(), this.host.api.runtimes()])
        this.candidates.update(agents, runtimes)
      } catch (error) { if (!this.host.closing()) this.failure('Reading Multica agent status', error) }
      if (this.refreshAgain && !this.host.closing()) { this.refreshAgain = false; return run() }
    }
    this.refreshing = run().finally(() => { this.refreshing = undefined; this.release() })
  }
  /**
   * Queues the launches of waiting tickets again: all of them once a candidate is healthy, and those still waiting for the
   * first snapshot once there is one, so they can ask a person.
   */
  private release(): void {
    if (!this.candidates.known) return
    const healthy = this.candidates.pick() !== undefined
    for (const ticketId of [...this.waiting]) {
      if (!healthy && this.host.store.get(MULTICA_KV.waiting(ticketId))) continue
      this.waiting.delete(ticketId)
      const ticket = this.host.store.tickets.get(ticketId)
      const latest = ticket && liveTicket(ticket) ? latestAssignment(ticket) : undefined
      if (latest?.state === 'pending') this.start(ticket!, latest)
    }
  }
  /** After a waiting ticket's issue was created: withdraws the question about starting a workstation, if it is still open. */
  private dispatched(ticketId: string, agentId: string): void {
    this.waiting.delete(ticketId)
    if (!this.host.store.get(MULTICA_KV.waiting(ticketId))) return
    const open = this.host.store.tickets.get(ticketId)?.attention?.question === this.noMachine()
    if (open) this.record(ticketId, { state: 'progress', summary: `Dispatched to the Multica agent ${agentId}`, question: null })
    this.host.store.delete(MULTICA_KV.waiting(ticketId))
  }

  /** Hands a ticket's waiting Inputs to its issue in one comment, or steers them into its running run; waits while a run waits to start. */
  private async deliverQueued(ticketId: string): Promise<void> {
    const ticket = this.host.store.tickets.get(ticketId)
    const assignment = ticket && liveTicket(ticket) ? runningAssignment(ticket) : undefined
    const state = this.state(ticketId)
    if (!ticket || !assignment || !state || !ticket.undelivered.length) return
    const runs = await this.host.api.runs(state.issueId)
    const plan = planDelivery(runs.map(run => ({ id: run.id, status: run.status, canSupplement: run.can_supplement ?? undefined })))
    // A run that waits to start would take a new comment as a run of its own; the run's start reconciles and delivers again.
    if (plan.kind === 'wait') return
    const inputIds = [...ticket.undelivered]
    const messages: string[] = []
    for (const id of inputIds) {
      const input = ticket.inputs.find(item => item.id === id)
      if (!input) continue
      const trigger = input.kind === 'trigger' ? this.host.store.triggers.get(input.triggerId) : undefined
      const note = input.kind === 'trigger' ? input.note : undefined
      const message = inputMessage(input, trigger ? await this.triggerCard(trigger, note) : undefined)
      if (message !== undefined) messages.push(message)
    }
    const latest = this.host.store.tickets.get(ticketId)
    if (!latest || !liveTicket(latest) || runningAssignment(latest)?.id !== assignment.id) return
    try {
      const posted = await this.host.api.addComment(state.issueId, inputComment(messages), plan.kind === 'steer' ? { steer: [plan.runId] } : {})
      const refused = refusedOutcome(posted.trigger_outcomes ?? undefined, this.agentOf(state))
      if (refused) { this.host.engine.deliveryFailed(ticketId, assignment.id, `Multica did not take the message: ${refused}`); return }
      const steered = (posted.trigger_outcomes ?? []).some(outcome => outcome.status === 'steered')
      const saved = this.state(ticketId) ?? state
      this.save(ticketId, { ...saved, deliveries: [...saved.deliveries, { commentId: posted.id, inputIds, assignmentId: assignment.id, read: steered }] })
      this.host.engine.inputsDelivered(ticketId, assignment.id, inputIds)
      if (steered) this.host.engine.inputsRead(ticketId, inputIds)
    } catch (error) {
      if (this.host.closing()) throw error
      this.host.engine.deliveryFailed(ticketId, assignment.id, String(error))
      throw error
    }
  }

  private async cancelActive(ticketId: string, issueId: string): Promise<void> {
    const runs = await this.host.api.runs(issueId)
    const active = runs.filter(run => !ENDED_STATUSES.has(run.status))
    if (!active.length) return
    const state = this.state(ticketId)
    if (state) this.save(ticketId, { ...state, cancelled: [...state.cancelled ?? [], ...active.map(run => run.id)].slice(-HISTORY_LIMIT) })
    await Promise.all(active.map(run => this.host.api.cancelRun(run.id)))
  }

  /** Routes a workspace event to the ticket whose issue it is about. */
  private event(event: MulticaEvent): void {
    // Agent and runtime status changes, such as a workstation going offline, come as agent:, runtime: or daemon: events.
    if (/^(agent|runtime|daemon):/.test(event.type) && !event.type.startsWith('daemon:heartbeat')) { this.refreshCandidates(); return }
    if (!/^(task|comment|issue):/.test(event.type)) return
    const issueId = issueOf(event)
    const ticketId = issueId && this.host.store.get(MULTICA_KV.issue(issueId))
    if (ticketId) this.schedule(ticketId)
  }
  /** Reconciles every open ticket that has an issue, after the event stream (re)connected. */
  private catchUp(): void {
    for (const ticket of this.host.store.tickets.open()) if (this.state(ticket.id)) this.schedule(ticket.id)
  }
  /** Queues a reconciliation of one ticket; events that arrive while one runs fold into one more. */
  private schedule(ticketId: string): void {
    if (this.chains.has(ticketId)) { this.again.add(ticketId); return }
    const run = async (): Promise<void> => {
      try { await this.reconcile(ticketId) } catch (error) { if (!this.host.closing()) this.failure('Reading Multica', error) }
      if (this.again.delete(ticketId) && !this.host.closing()) return run()
    }
    this.chains.set(ticketId, run().finally(() => this.chains.delete(ticketId)))
  }

  /**
   * Brings one ticket up to date with its issue: report blocks in new agent comments, Inputs the issue's runs took in, the
   * end of a turn when the issue's last active run ended, and a delivery when Inputs wait and no run waits to start.
   */
  async reconcile(ticketId: string): Promise<void> {
    const state = this.state(ticketId)
    const ticket = this.host.store.tickets.get(ticketId)
    if (!state || !ticket || !liveTicket(ticket)) return
    const [runs, comments] = await Promise.all([this.host.api.runs(state.issueId), this.host.api.comments(state.issueId)])
    // Reports first, so a report and the end of the turn that made it arrive in that order.
    const fresh = comments.filter(comment => comment.author_type === 'agent' && comment.author_id === this.agentOf(state)
      && !state.seen.includes(comment.id)).sort((a, b) => a.created_at.localeCompare(b.created_at))
    const refused: Refusal[] = []
    for (const comment of fresh) refused.push(...await this.apply(ticketId, state, comment))
    // A run first seen already ended started and ended between two reconciliations, such as while the stream was down.
    const ended = runs.filter(run => ENDED_STATUSES.has(run.status) && !ENDED_STATUSES.has(state.runs[run.id] ?? ''))
    const fresher = this.state(ticketId) ?? state
    const deliveries = fresher.deliveries.map(delivery => {
      if (delivery.read) return delivery
      const took = runs.some(run => run.status !== 'queued' && !WAITING_STATUSES.has(run.status)
        && (run.trigger_comment_id === delivery.commentId || run.delivered_comment_ids?.includes(delivery.commentId)))
      if (took) this.host.engine.inputsRead(ticketId, delivery.inputIds)
      return took ? { ...delivery, read: true } : delivery
    })
    const statuses = Object.fromEntries(runs.map(run => [run.id, run.status]))
    this.save(ticketId, { ...fresher, deliveries: deliveries.slice(-HISTORY_LIMIT), seen: [...fresher.seen, ...fresh.map(comment => comment.id)].slice(-SEEN_LIMIT),
      runs: statuses, refusals: [...fresher.refusals ?? [], ...refused].slice(-HISTORY_LIMIT) })
    await this.answerRefusals(ticketId, comments)
    const live = this.host.store.tickets.get(ticketId)
    const assignment = live && liveTicket(live) ? runningAssignment(live) : undefined
    if (!assignment) return
    const active = runs.some(run => !ENDED_STATUSES.has(run.status))
    const ours = ended.filter(run => !(fresher.cancelled ?? []).includes(run.id))
    if (!active && ours.length) {
      const last = ours.sort((a, b) => (a.completed_at ?? '').localeCompare(b.completed_at ?? '')).at(-1)!
      if (last.status === 'failed') this.reportFailed(ticketId, this.runFailure(last))
      this.host.engine.turnEnded(ticketId, assignment.id)
    }
    if (live!.undelivered.length && !runs.some(run => WAITING_STATUSES.has(run.status))) this.deliver(live!, assignment)
  }
  private runFailure(run: Run): string {
    return `The Multica run ${run.id} failed${run.error ? `: ${run.error}` : ''}. Its messages (multica issue run-messages ${run.id}) tell why.`
  }
  private reportFailed(ticketId: string, summary: string): void { this.record(ticketId, { state: 'failed', summary }) }
  /** Records the executor's own report on the ticket, relays it to the main Session, and runs `also` in the same transaction. */
  private record(ticketId: string, report: WorkerReport, also?: () => void): void {
    try {
      this.host.store.atomic(() => {
        const updated = this.host.engine.report(ticketId, report)
        const relay = reportRelay(updated, report)
        if (relay) this.host.sessions.relay(relay.id, relay.text)
        also?.()
      })
    } catch (error) { this.failure(`Recording a ${report.state} report`, error) }
  }

  /** Applies the report blocks of one agent comment for the ticket's current Assignment; returns the blocks it refused. */
  private async apply(ticketId: string, state: MulticaTicketState, comment: Comment): Promise<Refusal[]> {
    const assignment = state.assignment
    if (!assignment) return []
    const refused: Refusal[] = []
    const refuse = (index: number, reason: string) => {
      refused.push({ commentId: comment.id, index, note: refusalNote(index, reason, assignment.nonce) })
    }
    for (const [index, parsed] of parseReports(comment.content, { assignment: assignment.id, nonce: assignment.nonce }).entries()) {
      if (!parsed.ok) { this.failure(`Ignored a report block in comment ${comment.id}`, parsed.error); refuse(index, parsed.error); continue }
      try { await this.request(ticketId, parsed.block) } catch (error) {
        this.failure(`Report block in comment ${comment.id}`, error)
        refuse(index, errorText(error))
      }
    }
    return refused
  }
  /**
   * Tells workers why their report blocks were refused, with a note under each block's comment. A refusal already
   * answered on the issue, such as by a note posted just before a restart, is not answered again; one Multica failed to
   * take stays pending for the next reconciliation.
   */
  private async answerRefusals(ticketId: string, comments: readonly Comment[]): Promise<void> {
    const state = this.state(ticketId)
    if (!state?.refusals?.length) return
    const pending: Refusal[] = []
    for (const refusal of state.refusals) {
      if (comments.some(comment => comment.parent_id === refusal.commentId && isRefusalNote(comment.content, refusal.index))) continue
      try { await this.host.api.addComment(state.issueId, refusal.note, { parentId: refusal.commentId }) } catch (error) {
        if (this.host.closing()) throw error
        this.failure(`Telling the worker about a refused report in comment ${refusal.commentId}`, error)
        pending.push(refusal)
      }
    }
    this.save(ticketId, { ...this.state(ticketId) ?? state, refusals: pending })
  }
  private async request(ticketId: string, block: ReportBlock): Promise<void> {
    const engine = this.host.engine
    if (block.tool === 'ack') {
      const reply = engine.reply({ purpose: 'acknowledgement', ticketId, triggerId: block.trigger_id, text: block.text })
      // The reply is queued either way, and the outbox retries it: a failed send now is no refusal of the block.
      if (reply.status !== 'sent') await this.host.sendReplies().catch((error: unknown) => { this.failure('Sending a worker acknowledgement', error) })
      return
    }
    if (block.tool === 'handback') { engine.handback(ticketId, block.trigger_id, block.reason); return }
    if (block.tool === 'unrelated') { engine.markUnrelated(ticketId, block.thread, block.reason); return }
    if (block.tool === 'follow') {
      const at = block.thread.indexOf(':')
      if (at < 1 || at === block.thread.length - 1) throw new Error('A thread key is <source>:<id>, such as github:<number>')
      engine.followForContext(ticketId, { source: block.thread.slice(0, at), id: block.thread.slice(at + 1) })
      return
    }
    // As with a DSH worker's report, a refused PR claim refuses the report it came with.
    const refused = block.pr_number != null && this.host.claimPull ? await this.host.claimPull(ticketId, block.pr_number, this.host.lifetime) : undefined
    if (refused) throw new Error(`PR #${block.pr_number} was not claimed: ${refused}`)
    const report: WorkerReport = { state: block.state, summary: block.summary, ...block.human_action !== undefined ? { question: block.human_action } : {},
      ...block.question_for_trigger_id ? { askedBy: block.question_for_trigger_id } : {}, ...block.reply_to_trigger_id ? { answers: block.reply_to_trigger_id } : {} }
    this.host.store.atomic(() => {
      const updated = engine.report(ticketId, report)
      const relay = reportRelay(updated, report)
      if (relay) this.host.sessions.relay(relay.id, relay.text)
    })
  }
}
