/**
 * MulticaExecutor runs ticket assignments on the operator's own machines through Multica instead of local DSH worker
 * sessions: an Assignment is a Multica issue assigned to an agent (one runtime on one machine), its turns are the issue's
 * runs, Inputs reach it as comments (or steer a running run that accepts them), and its worker reports through
 * `superagent-report` comment blocks (see `multica-protocol.ts`). Facts arrive by push over the workspace WebSocket; each
 * (re)connection catches up once through REST. See docs/009-multica-executor.md.
 */
import { latestAssignment, liveTicket, runningAssignment, type Assignment, type EngineForExecutor, type Ticket, type TicketOutcome,
  type Trigger } from '../core/index.ts'
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
import { SUSPEND_MS, UNCLAIMED_LIMIT_MS, failureByReason, failureKind, unclaimedSince } from './multica-failover.ts'
import { MulticaEvents, type MulticaEvent, type SocketFactory } from './multica-events.ts'
import type { MulticaConnection } from './multica-api.ts'
import { ENDED_STATUSES, WAITING_STATUSES, inputComment, isRefusalNote, isRefusalResend, issueDescription, newNonce, parseReports, planDelivery,
  laterAssignmentComment, refusalNote, refusalResend, refusedOutcome, type Brief, type ReportBlock } from './multica-protocol.ts'

/** Which agents run the tickets, and what every worker is told about where to work. */
export interface MulticaExecutorConfig extends MulticaConnection {
  /** The Multica agents a new ticket's issue may be assigned to, in order of preference: each one runtime on one workstation. */
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
  deliveries: Delivery[]
  /** Agent comments already processed for report blocks. */
  seen: string[]
  /** The last status seen per run. */
  runs: Record<string, string>
  /** Runs this executor cancelled, whose end is no turn end of the worker's. */
  cancelled?: string[]
  /** Refused report blocks whose note to the worker has not reached Multica yet. */
  refusals?: Refusal[]
  /**
   * The current Assignment's last refused block that may have been its report (a refused `report`, or one that did not
   * parse) with no report accepted after it, which is told again once its run ended; dropped once told or superseded.
   */
  unanswered?: Unanswered
  /** The Assignment whose refusal was posted again as a plain comment: at most one such comment, and run, per Assignment. */
  resentFor?: string
  /** The status the issue was closed with when its ticket ended; a woken ticket reopens it. */
  closed?: IssueEnd
  /**
   * A reassignment of the issue to another candidate still to do, for the Assignment it was decided for: saved before its
   * first step, so a crash or a failed request part way does it again, and kept while no candidate can take it. `fatal`
   * when its agent's run failed for a reason that suspended it: once the suspension is over, the agent may take it again.
   */
  reassigning?: { assignmentId: string; reason: string; fatal?: true }
  /**
   * The agent the issue went back to once its suspension was over: it gets no second such return, so an agent that keeps
   * failing the same way leaves the ticket to a person instead of costing an Assignment every `SUSPEND_MS`.
   */
  returnedTo?: string
}
/** A comment that carried Inputs to the issue's runs. */
export interface Delivery {
  commentId: string; inputIds: string[]; assignmentId: string; read?: boolean
  /** The running run the comment was steered into, while its daemon is still to hand it over. */
  steeredRun?: string
}
/** A refused report block: the `index`th block of an agent comment, and the note that tells its worker why. */
export interface Refusal { commentId: string; index: number; note: string }
/** A refused report block still standing, and the plain comment that tells its worker again: `refusalResend`. */
export interface Unanswered { assignmentId: string; commentId: string; resend: string }
export const MULTICA_KV = {
  ticket: family<MulticaTicketState>('multica-ticket:'),
  issue: family<string>('multica-issue:'),
  /**
   * A ticket whose first Assignment waits for a healthy candidate, or whose issue waits for one to be reassigned to, once a person has been asked to start a workstation, and what they were asked. */
  waiting: family<{ assignmentId: string; question: string }>('multica-waiting:'),
  /**
   * An ended ticket whose issue is not closed yet, and the status it is closed with; kept until Multica took it. A ticket its
   * worker's report ended lets a run still running finish until `graceUntil` (epoch milliseconds), as after `ready`.
   */
  closing: family<{ status: IssueEnd; graceUntil?: number }>('multica-closing:'),
}
/** The Multica status an ended ticket's issue moves to. */
export type IssueEnd = 'done' | 'cancelled'
/** A ticket that ended with its result or its PR merged is done; one closed unmerged, with nothing to do, or stopped is cancelled. */
const issueEnd = (outcome: TicketOutcome): IssueEnd => outcome === 'completed' || outcome === 'merged' ? 'done' : 'cancelled'
/**
 * How long a run still running when its worker's report ended the ticket may go on before the issue is closed, which
 * cancels it: a worker reports `ready` and then finishes its run, such as by cleaning up (JAR-84, JAR-85, #18).
 */
export const CLOSE_GRACE_MS = 2 * 60_000
const SEEN_LIMIT = 500
/** How many cancelled runs, deliveries and pending refusal notes a ticket's state keeps. */
const HISTORY_LIMIT = 50

export interface MulticaExecutorHost {
  readonly engine: Pick<EngineForExecutor, 'assignmentStarted' | 'assignmentReassigned' | 'inputsDelivered' | 'inputsRead' | 'turnEnded' | 'deliveryFailed' | 'report'
    | 'reply' | 'handback' | 'followForContext' | 'markUnrelated'>
  readonly store: Pick<Store, 'tickets' | 'triggers' | 'get' | 'set' | 'delete' | 'keys' | 'settings' | 'atomic'>
  readonly effects: Pick<Effects, 'enqueue' | 'handle'>
  readonly sessions: Pick<Sessions, 'relay'>
  /** The `worker` health entry: the event stream's state and the last failure. */
  readonly health: HealthEntry
  readonly machine: { stateDir: string }
  readonly lifetime: AbortSignal
  readonly config: MulticaExecutorConfig
  readonly api: Pick<MulticaApi, 'createIssue' | 'setMetadata' | 'findIssues' | 'runs' | 'comments' | 'steerReceipts' | 'addComment' | 'cancelRun' | 'setStatus' | 'agents' | 'runtimes'
    | 'assignIssue' | 'runMessages'>
  /** The time and the timers unclaimed runs are timed by; tests pass a fake clock. */
  readonly clock?: Clock
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

/** Epoch milliseconds, and a timer that can be cancelled. */
export interface Clock { now(): number; after(ms: number, run: () => void): () => void }
/** The system clock, whose timers do not keep the process alive. */
const realClock: Clock = {
  now: () => Date.now(),
  after: (ms, run) => { const timer = setTimeout(run, ms); timer.unref?.(); return () => { clearTimeout(timer) } },
}
const now = () => new Date().toISOString()
const errorText = (error: unknown) => error instanceof Error ? error.message : String(error)
/** The run that ended last. */
const lastEnded = (runs: readonly Run[]): Run | undefined => [...runs].sort((a, b) => (a.completed_at ?? '').localeCompare(b.completed_at ?? '')).at(-1)
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
  /** The delivery each ticket runs now; the next waits for it. */
  private readonly delivering = new Map<string, Promise<void>>()
  /** Tickets whose issue waits for a healthy candidate to be reassigned to; after a restart, the catch-up finds them again. */
  private readonly stranded = new Set<string>()
  /** Per ticket, the timer that reconciles it once its unclaimed run has waited `UNCLAIMED_LIMIT_MS`. */
  private readonly unclaimed = new Map<string, () => void>()
  /** Per ended ticket, the timer that closes its issue once the grace of its last run is over. */
  private readonly graces = new Map<string, () => void>()
  /** Per suspended candidate, the timer that takes a new health snapshot when its suspension is over, to reconsider it. */
  private readonly resumes = new Map<string, () => void>()
  /**
   * Per ticket, the `worker` health detail its last refused block that may have been a report set, and for which Assignment:
   * a report of that Assignment accepted later sets the entry back to ready while it still shows that refusal, since the
   * worker fixed it (JAR-84, #18). The entry has one slot, so a later failure of any kind replaces the refusal there.
   */
  private readonly refusalHealth = new Map<string, { assignmentId: string; detail: string }>()
  private readonly clock: Clock
  constructor(private readonly host: MulticaExecutorHost) {
    this.clock = host.clock ?? realClock
    this.candidates = new MulticaCandidates(host.config.agents, () => this.clock.now())
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
    // A woken ticket's issue is no longer to be closed.
    this.host.store.delete(MULTICA_KV.closing(ticket.id))
    this.host.effects.enqueue('launch', assignment.id, { ticketId: ticket.id, assignmentId: assignment.id })
  }
  deliver(ticket: Ticket, _assignment: Assignment): void { this.host.effects.enqueue('delivery', ticket.id, { ticketId: ticket.id }) }
  /**
   * Cancels the issue's active runs; the domain already ended the Assignment. When the ticket ended with it, the issue
   * is also closed, as done or cancelled by the ticket's outcome, and a close Multica did not take is tried again on
   * the next reconciliation.
   */
  stop(ticket: Ticket, _assignment: Assignment, _reason: string): void {
    const state = this.state(ticket.id)
    if (!state) return
    if (this.queueCloseIfEnded(ticket)) return
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
  async shutdown(): Promise<void> { this.events.stop(); this.clearTimers(); await Promise.allSettled([...this.chains.values()]) }
  dispose(): void { this.events.stop(); this.clearTimers() }
  /** Cancels the timers of unclaimed runs; the catch-up after the next start arms them again. */
  private clearTimers(): void {
    for (const timers of [this.unclaimed, this.graces, this.resumes]) { for (const cancel of timers.values()) cancel(); timers.clear() }
  }
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
  /** Records a failure in the `worker` health entry; returns the detail it set. */
  private failure(what: string, error: unknown): string {
    const detail = `${what}: ${errorText(error)}`
    this.host.health.set({ state: 'error', detail, at: now() })
    return detail
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
        if (found) state = { issueId: found.id, identifier: found.identifier ?? undefined, ...found.assignee_id ? { agentId: found.assignee_id } : {},
          deliveries: [], seen: [], runs: {} }
      }
      if (!state) {
        const agentId = this.choose()
        if (!agentId) { this.wait(ticketId, assignmentId); return }
        const issue = await this.host.api.createIssue({ title: first.ticket.title, description: issueDescription(brief), agentId })
        // Save before anything else can fail, so the run the issue started reports into a known Assignment.
        this.save(ticketId, { issueId: issue.id, identifier: issue.identifier ?? undefined, agentId, assignment: { id: assignmentId, nonce },
          deliveries: [], seen: [], runs: {} })
        this.dispatched(ticketId, agentId)
        // A ticket that ended while the issue was being created had no issue to close when it stopped.
        // Its `superagent_ticket` metadata is skipped too: the saved state finds the issue, and an ended ticket launches nothing more.
        const ended = this.host.store.tickets.get(ticketId)
        if (ended && this.queueCloseIfEnded(ended)) return
        await this.host.api.setMetadata(issue.id, 'superagent_ticket', ticketId)
      } else {
        // Runs of earlier Assignments are the baseline: only runs that start from here on are this Assignment's turns.
        const before = await this.host.api.runs(state.issueId)
        // The issue of a woken ticket was closed when the ticket ended; reopen it without a run, which the comment starts.
        if (state.closed) await this.host.api.setStatus(state.issueId, 'todo')
        const { closed: _closed, ...open } = state
        this.save(ticketId, { ...open, assignment: { id: assignmentId, nonce }, runs: Object.fromEntries(before.map(run => [run.id, run.status])),
          cancelled: [...state.cancelled ?? [], ...before.filter(run => !ENDED_STATUSES.has(run.status)).map(run => run.id)].slice(-HISTORY_LIMIT) })
        const moved = first.ticket.assignments.at(-2)?.endedReason === 'reassigned'
          ? `it was reassigned, and Multica agent ${this.agentOf(state)} runs it now; the first new message says why.` : undefined
        const posted = await this.host.api.addComment(state.issueId, laterAssignmentComment(brief, moved))
        // A ticket that ended while the comment was posted may have closed the issue before the comment started a run.
        const ended = this.host.store.tickets.get(ticketId)
        if (ended && this.queueCloseIfEnded(ended)) return
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
    this.askForWorkstation(ticketId, assignmentId)
  }
  /** Asks a person to start a candidate's workstation, once per Assignment: the question stays on the ticket across reconnections and restarts. */
  private askForWorkstation(ticketId: string, assignmentId: string, except: readonly string[] = []): void {
    if (!this.candidates.known || this.host.store.get(MULTICA_KV.waiting(ticketId))?.assignmentId === assignmentId) return
    const others = this.host.config.agents.filter(id => !except.includes(id))
    const question = `No Multica workstation is online to run this ticket: none of the ${except.length ? 'other ' : ''}candidate Multica agents `
      + `(${others.join(', ')}) is online. Start one of their workstations; the ticket is dispatched as soon as one is online.`
    this.record(ticketId, { state: 'progress', summary: 'Waiting for a Multica workstation to come online', question },
      () => { this.host.store.set(MULTICA_KV.waiting(ticketId), { assignmentId, question }) })
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
    this.refreshing = run().finally(() => { this.refreshing = undefined; this.requeueWaiting() })
  }
  /**
   * Queues the launches of waiting tickets again: all of them once a candidate is healthy, and those still waiting for the
   * first snapshot once there is one, so they can ask a person.
   */
  private requeueWaiting(): void {
    if (!this.candidates.known) return
    const healthy = this.candidates.pick() !== undefined
    // Stranded issues are reassigned on their next reconciliation, which picks a candidate other than their own.
    if (healthy) for (const ticketId of this.stranded) this.schedule(ticketId)
    for (const ticketId of [...this.waiting]) {
      const ticket = this.host.store.tickets.get(ticketId)
      const latest = ticket && liveTicket(ticket) ? latestAssignment(ticket) : undefined
      // A ticket that ended or moved on while it waited leaves nothing behind.
      if (latest?.state !== 'pending') { this.waiting.delete(ticketId); this.host.store.delete(MULTICA_KV.waiting(ticketId)); continue }
      if (!healthy && this.host.store.get(MULTICA_KV.waiting(ticketId))) continue
      this.waiting.delete(ticketId)
      this.start(ticket!, latest)
    }
  }
  /** After a waiting ticket's issue was created or reassigned: withdraws the question about starting a workstation, if it is still open. */
  private dispatched(ticketId: string, agentId: string): void {
    this.waiting.delete(ticketId); this.stranded.delete(ticketId)
    const asked = this.host.store.get(MULTICA_KV.waiting(ticketId))
    if (!asked) return
    const open = this.host.store.tickets.get(ticketId)?.attention?.question === asked.question
    if (open) this.record(ticketId, { state: 'progress', summary: `Dispatched to the Multica agent ${agentId}`, question: null })
    this.host.store.delete(MULTICA_KV.waiting(ticketId))
  }

  /**
   * Delivers one ticket at a time, so the Engine's delivery, a reconciliation's and a steer posted again never post the
   * same Inputs twice or lose each other's records.
   */
  private deliverQueued(ticketId: string, work: () => Promise<void> = () => this.deliverNow(ticketId)): Promise<void> {
    const next = (this.delivering.get(ticketId) ?? Promise.resolve()).then(work)
    const tracked: Promise<void> = next.catch(() => {}).finally(() => { if (this.delivering.get(ticketId) === tracked) this.delivering.delete(ticketId) })
    this.delivering.set(ticketId, tracked)
    return next
  }
  /** Hands a ticket's waiting Inputs to its issue in one comment, or steers them into its running run; waits while a run waits to start. */
  private async deliverNow(ticketId: string): Promise<void> {
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
      // A plain comment that steered names no outcome, only a receipt; without one it is an ordinary comment that Multica
      // queued for the next run, as when the run ended or stopped accepting messages before the comment reached it.
      const steerRun = plan.kind === 'steer' ? plan.runId : undefined
      const receipt = steerRun ? posted.supplements?.find(item => item.task_id === steerRun && item.status !== 'failed') : undefined
      const read = receipt?.status === 'delivered'
      const saved = this.state(ticketId) ?? state
      this.save(ticketId, { ...saved, deliveries: [...saved.deliveries,
        { commentId: posted.id, inputIds, assignmentId: assignment.id, read, ...receipt && !read ? { steeredRun: steerRun } : {} }] })
      this.host.engine.inputsDelivered(ticketId, assignment.id, inputIds)
      if (read) this.host.engine.inputsRead(ticketId, inputIds)
    } catch (error) {
      if (this.host.closing()) throw error
      this.host.engine.deliveryFailed(ticketId, assignment.id, String(error))
      throw error
    }
  }

  /** Cancels the issue's active runs, recorded first as cancelled by this executor; `known` are its runs when just read. */
  private async cancelActive(ticketId: string, issueId: string, known?: readonly Run[]): Promise<void> {
    const runs = known ?? await this.host.api.runs(issueId)
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
  /** Reconciles every open ticket that has an issue, and closes the issues of ended tickets not closed yet, after the event stream (re)connected. */
  private catchUp(): void {
    for (const ticket of this.host.store.tickets.open()) if (this.state(ticket.id)) this.schedule(ticket.id)
    for (const key of this.host.store.keys(MULTICA_KV.closing)) this.schedule(key.slice(MULTICA_KV.closing.prefix.length))
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
   * end of a turn when the issue's last active run ended, and a delivery when Inputs wait and no run waits to start. An
   * ended ticket's issue is closed instead, when that is still to do.
   */
  async reconcile(ticketId: string): Promise<void> {
    const state = this.state(ticketId)
    const ticket = this.host.store.tickets.get(ticketId)
    const closing = this.host.store.get(MULTICA_KV.closing(ticketId))
    if (state && ticket && !liveTicket(ticket) && closing) return this.close(ticketId, state, closing)
    if (!state || !ticket || !liveTicket(ticket)) return
    const [runs, comments] = await Promise.all([this.host.api.runs(state.issueId), this.host.api.comments(state.issueId)])
    // Reports first, so a report and the end of the turn that made it arrive in that order.
    const fresh = comments.filter(comment => comment.author_type === 'agent' && comment.author_id === this.agentOf(state)
      && !state.seen.includes(comment.id)).sort((a, b) => a.created_at.localeCompare(b.created_at))
    const refused: Refusal[] = []
    let unanswered = state.unanswered
    for (const comment of fresh) {
      const applied = await this.apply(ticketId, state, comment)
      refused.push(...applied.refused)
      if (applied.last) unanswered = applied.last === 'accepted' ? undefined : applied.last
    }
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
    const { unanswered: _, ...kept } = fresher
    // One told already, or one of an Assignment that no longer reports here, stays unanswered for good.
    if (unanswered && (unanswered.assignmentId !== fresher.assignment?.id || fresher.resentFor === unanswered.assignmentId)) unanswered = undefined
    this.save(ticketId, { ...kept, deliveries: deliveries.slice(-HISTORY_LIMIT), seen: [...fresher.seen, ...fresh.map(comment => comment.id)].slice(-SEEN_LIMIT),
      runs: statuses, refusals: [...fresher.refusals ?? [], ...refused].slice(-HISTORY_LIMIT), ...unanswered ? { unanswered } : {} })
    await this.answerRefusals(ticketId, comments)
    if (this.state(ticketId)?.deliveries.some(delivery => delivery.steeredRun)) await this.deliverQueued(ticketId, () => this.settleSteers(ticketId, comments))
    const live = this.host.store.tickets.get(ticketId)
    const assignment = live && liveTicket(live) ? runningAssignment(live) : undefined
    if (!assignment) return
    const ours = ended.filter(run => !(fresher.cancelled ?? []).includes(run.id))
    if (this.host.config.pickHealthy) {
      const reason = await this.reassignReason(ticketId, assignment.id, runs, ours)
      if (reason) { await this.reassign(ticketId, assignment.id, reason, comments); return }
    }
    const active = runs.some(run => !ENDED_STATUSES.has(run.status))
    // Inputs waiting, or delivered and not read yet (such as a steer just posted again), start a run of their own, which
    // reads the refusal note in the comment history. A refusal posted again starts the run that ends this turn instead.
    const unread = live!.undelivered.length > 0 || !!this.state(ticketId)?.deliveries.some(delivery => !delivery.read && delivery.assignmentId === assignment.id)
    const resent = !active && !unread && await this.resendRefusal(ticketId, assignment.id, comments)
    if (!active && ours.length) {
      const last = lastEnded(ours)!
      if (last.status === 'failed') this.reportFailed(ticketId, this.runFailure(last))
      if (!resent) this.host.engine.turnEnded(ticketId, assignment.id)
    }
    // A delivery Multica refused waits for the next Input or a retry; delivering it again here would post it on every event.
    if (live!.undelivered.length && !live!.delivery && !runs.some(run => WAITING_STATUSES.has(run.status))) this.deliver(live!, assignment)
  }
  /**
   * Follows the comments steered into running runs: a run's daemon handing one over reads its Inputs; one whose run ended
   * first is posted again as a plain comment for the next run, since Multica leaves a member's failed steer to its author.
   * Runs in the ticket's delivery queue, so it sees every delivery recorded before it and posts no comment beside one.
   */
  private async settleSteers(ticketId: string, comments: readonly Comment[]): Promise<void> {
    const state = this.state(ticketId)
    const steered = state?.deliveries.filter(delivery => delivery.steeredRun) ?? []
    if (!state || !steered.length) return
    const [receipts, runs] = await Promise.all([this.host.api.steerReceipts(state.issueId), this.host.api.runs(state.issueId)])
    const ticket = this.host.store.tickets.get(ticketId)
    const assignment = ticket && liveTicket(ticket) ? runningAssignment(ticket) : undefined
    const waiting = runs.some(run => WAITING_STATUSES.has(run.status))
    /** What each settled steer becomes: its delivery read, a plain delivery posted in its place, or nothing. */
    const settled = new Map<string, Delivery | undefined>()
    for (const delivery of steered) {
      const receipt = receipts.get(delivery.commentId)?.find(item => item.task_id === delivery.steeredRun)
      if (receipt?.status === 'delivered') {
        this.host.engine.inputsRead(ticketId, delivery.inputIds)
        const { steeredRun: _, ...rest } = delivery
        settled.set(delivery.commentId, { ...rest, read: true })
        continue
      }
      // Pending until the run's daemon takes it or the run ends, which marks it failed in the same transaction.
      const runEnded = ENDED_STATUSES.has(runs.find(run => run.id === delivery.steeredRun)?.status ?? 'completed')
      if (receipt ? receipt.status !== 'failed' : !runEnded) continue
      const content = comments.find(comment => comment.id === delivery.commentId)?.content
      // Inputs of an Assignment that no longer runs are not posted to the next one; nor is a comment that is gone.
      if (assignment?.id !== delivery.assignmentId || content === undefined) { settled.set(delivery.commentId, undefined); continue }
      // A run that waits to start would take the plain comment as a run of its own; its start reconciles again.
      if (waiting) continue
      try {
        const posted = await this.host.api.addComment(state.issueId, content)
        settled.set(delivery.commentId, { commentId: posted.id, inputIds: delivery.inputIds, assignmentId: delivery.assignmentId, read: false })
      } catch (error) {
        if (this.host.closing()) throw error
        this.failure(`Posting again the messages the run ${delivery.steeredRun} ended before reading`, error)
      }
    }
    const latest = this.state(ticketId) ?? state
    const deliveries = latest.deliveries.flatMap(delivery => {
      if (!settled.has(delivery.commentId)) return [delivery]
      const next = settled.get(delivery.commentId)
      return next ? [next] : []
    })
    this.save(ticketId, { ...latest, deliveries })
  }
  /**
   * Why the ticket's issue is to be reassigned to another candidate now, if it is: a run of this Assignment failed because
   * its workstation went away or for a reason it would only repeat (which also keeps its agent out for a while,
   * `SUSPEND_MS`), or the issue's runs have waited unclaimed for `UNCLAIMED_LIMIT_MS`, with no run claimed meanwhile. A run
   * that waits less arms a timer that reconciles the ticket when it is due; one claimed meanwhile, such as by the
   * workstation's other daemon or a returning one, settles a reassignment still to do. The decision is saved before any
   * step of it. Also cancels any active run of an agent the issue no longer belongs to, such as a retry Multica deferred
   * for the old one after the issue was reassigned.
   */
  private async reassignReason(ticketId: string, assignmentId: string, runs: readonly Run[], ended: readonly Run[]): Promise<string | undefined> {
    this.unclaimed.get(ticketId)?.(); this.unclaimed.delete(ticketId)
    const state = this.state(ticketId)!
    const agent = this.agentOf(state)
    const strays = runs.filter(run => run.agent_id && run.agent_id !== agent && !ENDED_STATUSES.has(run.status))
    if (strays.length) {
      this.save(ticketId, { ...state, cancelled: [...state.cancelled ?? [], ...strays.map(run => run.id)].slice(-HISTORY_LIMIT) })
      await Promise.all(strays.map(run => this.host.api.cancelRun(run.id)))
    }
    const current = this.state(ticketId)!
    const pending = current.reassigning?.assignmentId === assignmentId ? current.reassigning : undefined
    const mine = runs.filter(run => !strays.includes(run))
    if (mine.some(run => run.status === 'running' || run.status === 'dispatched')) {
      if (current.reassigning) { const { reassigning: _, ...rest } = current; this.save(ticketId, rest) }
      if (this.stranded.has(ticketId) || this.host.store.get(MULTICA_KV.waiting(ticketId))) this.dispatched(ticketId, agent)
      return undefined
    }
    const decided = (reason: string, fatal = false) => {
      this.save(ticketId, { ...this.state(ticketId)!, reassigning: { assignmentId, reason, ...fatal ? { fatal } : {} } })
      return reason
    }
    const failed = lastEnded(ended.filter(run => run.status === 'failed'))
    if (failed) {
      const messages = failureByReason(failed) ? [] : await this.host.api.runMessages(failed.id).catch((error: unknown) => {
        this.failure(`Reading the messages of the Multica run ${failed.id}`, error)
        return []
      })
      const kind = failureKind(failed, messages)
      if (kind === 'fatal') this.suspend(agent)
      if (kind) return decided(`its run ${failed.id} on Multica agent ${agent} failed (${failed.failure_reason ?? 'no reason given'}`
        + `${failed.error ? `: ${failed.error}` : ''})${kind === 'fatal' ? ', which that workstation would only repeat' : ''}`, kind === 'fatal')
    }
    if (pending) return pending.reason
    const since = unclaimedSince(mine.filter(run => !(current.cancelled ?? []).includes(run.id)))
    if (since === undefined) return undefined
    const due = since + UNCLAIMED_LIMIT_MS - this.clock.now()
    if (due <= 0) return decided(`no workstation of Multica agent ${agent} claimed its run in ${UNCLAIMED_LIMIT_MS / 60_000} minutes`)
    this.unclaimed.set(ticketId, this.clock.after(due, () => { this.unclaimed.delete(ticketId); this.schedule(ticketId) }))
    return undefined
  }
  /**
   * Keeps a candidate out for `SUSPEND_MS`; when that is over, a new health snapshot reconsiders the tickets that wait for
   * a candidate, so one stranded on this agent alone goes back to it without a person retrying it (#18).
   */
  private suspend(agentId: string): void {
    this.candidates.suspend(agentId, this.clock.now() + SUSPEND_MS)
    this.resumes.get(agentId)?.()
    this.resumes.set(agentId, this.clock.after(SUSPEND_MS, () => { this.resumes.delete(agentId); this.refreshCandidates() }))
  }
  /**
   * Reassigns the ticket's issue to the first healthy candidate other than its own: cancels the issue's active runs first,
   * so no two run at once, then assigns the issue to the new agent, and in one transaction switches the ticket's agent and
   * has the Engine replace the Assignment, whose launch tells the new agent on the same issue. The old Assignment's reports
   * are refused from here on: its agent's comments are no longer read (those on the issue now count as seen, in case the
   * issue comes back to it) and the new Assignment has its own nonce. With no healthy candidate the issue stays, and a
   * person is asked once to start a workstation, as for a new ticket. An issue moved for a failure that suspended its own
   * agent goes back to that agent, as a new Assignment, once its suspension is over and still no other candidate is healthy.
   */
  private async reassign(ticketId: string, assignmentId: string, reason: string, comments: readonly Comment[]): Promise<void> {
    const state = this.state(ticketId)!
    const from = this.agentOf(state)
    const to = this.candidates.pick([from])
      ?? (state.reassigning?.fatal && state.returnedTo !== from && this.candidates.healthy(from) ? from : undefined)
    if (!to) { this.stranded.add(ticketId); this.askForWorkstation(ticketId, assignmentId, [from]); return }
    await this.cancelActive(ticketId, state.issueId)
    await this.host.api.assignIssue(state.issueId, to)
    this.dispatched(ticketId, to)
    const { assignment: _, reassigning: _reassigning, returnedTo: _returned, ...rest } = this.state(ticketId) ?? state
    const old = comments.filter(comment => comment.author_type === 'agent' && comment.author_id === from).map(comment => comment.id)
    this.host.store.atomic(() => {
      this.save(ticketId, { ...rest, agentId: to, ...to === from ? { returnedTo: to } : {}, seen: [...rest.seen, ...old.filter(id => !rest.seen.includes(id))].slice(-SEEN_LIMIT) })
      const why = to === from
        ? `This ticket starts again on Multica agent ${to}, which was kept out for ${SUSPEND_MS / 60_000} minutes because ${reason}, and no other workstation is online. `
        : `This ticket moved to another workstation, Multica agent ${to}, because ${reason}. `
      this.host.engine.assignmentReassigned(ticketId, assignmentId, why
        + 'An earlier run may have pushed work to the branch or commented on the issue: fetch the branch and read the issue\'s comments before continuing, and do not repeat finished steps.')
    })
  }
  /**
   * Records that an ended ticket's issue is to be closed and queues its reconciliation; returns whether the ticket has ended.
   * A ticket its worker's report ended, rather than a person, gives its running run `CLOSE_GRACE_MS` to finish.
   */
  private queueCloseIfEnded(ticket: Ticket): boolean {
    if (!ticket.ended) return false
    const graceUntil = this.host.store.get(MULTICA_KV.closing(ticket.id))?.graceUntil
      ?? (ticket.ended.by === 'executor' ? this.clock.now() + CLOSE_GRACE_MS : undefined)
    this.host.store.set(MULTICA_KV.closing(ticket.id), { status: issueEnd(ticket.ended.outcome), ...graceUntil ? { graceUntil } : {} })
    this.schedule(ticket.id)
    return true
  }
  /**
   * Closes an ended ticket's issue: cancels its active runs, then moves it to its end status without starting a run. While
   * the close's grace lasts, a running run is left to finish: its end reconciles again, or the timer at the grace's end.
   */
  private async close(ticketId: string, state: MulticaTicketState, { status, graceUntil }: { status: IssueEnd; graceUntil?: number }): Promise<void> {
    this.graces.get(ticketId)?.(); this.graces.delete(ticketId)
    try {
      const runs = await this.host.api.runs(state.issueId)
      const left = (graceUntil ?? 0) - this.clock.now()
      if (left > 0 && runs.some(run => run.status === 'running')) {
        this.graces.set(ticketId, this.clock.after(left, () => { this.graces.delete(ticketId); this.schedule(ticketId) }))
        return
      }
      await this.cancelActive(ticketId, state.issueId, runs)
      await this.host.api.setStatus(state.issueId, status)
      this.host.store.atomic(() => {
        this.save(ticketId, { ...this.state(ticketId) ?? state, closed: status })
        this.host.store.delete(MULTICA_KV.closing(ticketId))
      })
    } catch (error) {
      if (this.host.closing()) throw error
      this.failure(`Closing ${state.identifier ?? state.issueId} as ${status}`, error)
    }
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

  /**
   * Applies the report blocks of one agent comment for the ticket's current Assignment; returns the blocks it refused, and
   * whether the comment ends with a refused block or an accepted report, which decides whether a refusal still stands.
   */
  private async apply(ticketId: string, state: MulticaTicketState, comment: Comment): Promise<{ refused: Refusal[]; last?: 'accepted' | Unanswered }> {
    const assignment = state.assignment
    if (!assignment) return { refused: [] }
    const refused: Refusal[] = []
    let last: 'accepted' | Unanswered | undefined
    /** Refuses a block; one that may have been the report, as one that did not parse may, stands until a report is accepted. */
    const refuse = (index: number, what: string, reason: string, report: boolean) => {
      const detail = this.failure(what, reason)
      if (report) this.refusalHealth.set(ticketId, { assignmentId: assignment.id, detail })
      refused.push({ commentId: comment.id, index, note: refusalNote(index, reason, assignment.nonce) })
      if (report) last = { assignmentId: assignment.id, commentId: comment.id, resend: refusalResend(index, reason, assignment.nonce, comment.id) }
    }
    for (const [index, parsed] of parseReports(comment.content, { assignment: assignment.id, nonce: assignment.nonce }).entries()) {
      if (!parsed.ok) { refuse(index, `Ignored a report block in comment ${comment.id}`, parsed.error, true); continue }
      try {
        await this.request(ticketId, parsed.block)
        if (parsed.block.tool === 'report') { last = 'accepted'; this.clearRefusalHealth(ticketId, assignment.id) }
      } catch (error) {
        refuse(index, `Report block in comment ${comment.id}`, errorText(error), parsed.block.tool === 'report')
      }
    }
    return { refused, ...last ? { last } : {} }
  }
  /** After a report of `assignmentId` was accepted: the `worker` entry is ready again if it still shows that Assignment's last refusal. */
  private clearRefusalHealth(ticketId: string, assignmentId: string): void {
    const refusal = this.refusalHealth.get(ticketId)
    if (!refusal) return
    this.refusalHealth.delete(ticketId)
    const health = this.host.health.get()
    if (refusal.assignmentId === assignmentId && health?.state === 'error' && health.detail === refusal.detail) this.healthy('Connected to Multica')
  }
  /**
   * Tells the worker again, with a plain comment that starts a run, about a refused report block its run ended without
   * fixing: a worker's report is often its last step, and the `/note` reply starts no run. Once per Assignment, so a worker
   * that keeps sending refused blocks costs one extra run, not a loop; one already on the issue, such as one posted just
   * before a restart, is not posted again. Returns whether a run is to start from it.
   */
  private async resendRefusal(ticketId: string, assignmentId: string, comments: readonly Comment[]): Promise<boolean> {
    const state = this.state(ticketId)
    const unanswered = state?.unanswered
    if (!state || unanswered?.assignmentId !== assignmentId || state.resentFor === assignmentId) return false
    const done = () => {
      const { unanswered: _, ...rest } = this.state(ticketId) ?? state
      this.save(ticketId, { ...rest, resentFor: assignmentId })
    }
    if (comments.some(comment => comment.parent_id === unanswered.commentId && isRefusalResend(comment.content))) { done(); return true }
    const what = `Telling the worker again about the refused report in comment ${unanswered.commentId}`
    try {
      const posted = await this.host.api.addComment(state.issueId, unanswered.resend, { parentId: unanswered.commentId })
      const refused = refusedOutcome(posted.trigger_outcomes ?? undefined, this.agentOf(state))
      if (refused) this.failure(what, `Multica started no run: ${refused}`)
      done()
      return !refused
    } catch (error) {
      if (this.host.closing()) throw error
      this.failure(what, error)
      return false
    }
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
