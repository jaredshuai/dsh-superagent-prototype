/**
 * The live drill of reassigning across workstations (#8): runs the real MulticaExecutor, with its real API client, event
 * stream and clock, against the operator's Multica workspace, while a recording stand-in plays the Engine and the store.
 * One ticket goes to the first candidate; the operator takes that candidate's workstation offline (for example
 * `kill -STOP` on both of its daemons) right before, and the drill logs whether the issue moves to the second candidate,
 * whether the first run is cancelled, and whether the moved Assignment reports ready.
 *
 *   pnpm exec tsdown scripts/multica-drill/reassign.ts --no-config -d node_modules/.cache/multica-drill --platform node --format esm --no-dts
 *   node node_modules/.cache/multica-drill/reassign.mjs --agents <first agent id>,<second agent id> [--timeout <seconds>]
 *
 * (Bundled first, as the protocol evaluation is: Node only strips types.) It reads the Multica server, workspace and token
 * from `~/.multica/config.json`, asks the worker to change nothing and only report, and leaves the issue for the operator
 * to clean up. Every engine call, health change and question is logged with the time.
 */
import { readFileSync } from 'node:fs'
import { homedir } from 'node:os'
import { parseArgs } from 'node:util'
import type { Assignment, Ticket, Trigger } from '../../src/core/index.ts'
import { MulticaApi } from '../../src/executor/multica-api.ts'
import { MULTICA_KV, MulticaExecutor, type MulticaExecutorHost, type MulticaTicketState } from '../../src/executor/multica.ts'

const { values } = parseArgs({ options: { agents: { type: 'string' }, timeout: { type: 'string', default: '1500' } } })
if (!values.agents) throw new Error('--agents <first agent id>,<second agent id> is required')
const agents = values.agents.split(',').map(id => id.trim()).filter(Boolean)
if (agents.length < 2) throw new Error('a reassignment needs two candidate agents')

const config = JSON.parse(readFileSync(`${homedir()}/.multica/config.json`, 'utf8')) as { server_url: string; workspace_id: string; token: string }
const connection = { serverUrl: config.server_url, workspaceId: config.workspace_id, token: config.token }
const started = Date.now()
const log = (tag: string, text: string) => {
  const elapsed = Math.round((Date.now() - started) / 1000)
  console.log(`${new Date().toISOString().slice(11, 19)} +${String(Math.floor(elapsed / 60)).padStart(2, '0')}:${String(elapsed % 60).padStart(2, '0')} [${tag}] ${text}`)
}

const ticketId = `drill-${new Date().toISOString().replace(/\D/g, '').slice(0, 14)}`
const trigger = { id: 'drill-trigger', at: new Date().toISOString(), actor: { id: 'operator', source: 'github' }, threads: [{ source: 'github', id: '0' }],
  text: 'This is a failover drill of dsh-superagent (#8). Do not clone, edit, commit or push anything. Post an `ack` report,'
    + ' then a `report` with state `ready` and the summary "drill: ran on <your workstation\'s hostname>", and end your turn.' } as unknown as Trigger
let ticket = { id: ticketId, title: 'Failover drill (#8): report done without changing anything', createdAt: new Date().toISOString(),
  mandate: { strength: 'explicit', basis: {} }, origin: { triggerId: trigger.id, triggerIds: [trigger.id] },
  execution: { name: ticketId, directory: `/drill/${ticketId}`, assignment: trigger.text },
  assignments: [{ id: 'a1', state: 'pending', createdAt: new Date().toISOString() }], inputs: [], undelivered: [], channels: [] } as unknown as Ticket
const assignment = (id: string): Assignment => ticket.assignments.find(item => item.id === id)!
const update = (id: string, change: Partial<Assignment>) => {
  ticket = { ...ticket, assignments: ticket.assignments.map(item => item.id === id ? { ...item, ...change } : item) }
}

const kv = new Map<string, unknown>()
const handlers: Record<string, (payload: never) => Promise<void>> = {}
let finish: (outcome: string) => void = () => {}
const done = new Promise<string>(resolve => { finish = resolve })
let executor: MulticaExecutor

/** Runs a queued effect at once and once more after 5 s when it asked to wait, as the durable queue would. */
const run = (kind: string, payload: unknown, attempt = 0) => {
  setTimeout(() => {
    handlers[kind]!(payload as never).catch((error: unknown) => {
      log('effect', `${kind} failed: ${error instanceof Error ? error.message : String(error)}`)
      if (attempt < 3) run(kind, payload, attempt + 1)
    })
  }, attempt ? 5000 : 0)
}

const engine: MulticaExecutorHost['engine'] = {
  assignmentStarted: (_ticket, id) => { update(id, { state: 'running' }); log('engine', `assignmentStarted ${id}`) },
  assignmentReassigned: (_ticket, id, note) => {
    update(id, { state: 'ended', endedReason: 'reassigned' } as Partial<Assignment>)
    const next = `a${ticket.assignments.length + 1}`
    ticket = { ...ticket, assignments: [...ticket.assignments, { id: next, state: 'pending', createdAt: new Date().toISOString() } as Assignment] }
    log('engine', `assignmentReassigned ${id} → ${next}: ${note}`)
    setTimeout(() => { executor.start(ticket, assignment(next)) }, 0)
  },
  inputsDelivered: (_ticket, id, inputs) => { log('engine', `inputsDelivered ${id} ${inputs.join(',')}`) },
  inputsRead: (_ticket, inputs) => { log('engine', `inputsRead ${inputs.join(',')}`) },
  turnEnded: (_ticket, id) => { log('engine', `turnEnded ${id}`) },
  deliveryFailed: (_ticket, id, why) => { log('engine', `deliveryFailed ${id}: ${why}`) },
  report: (_ticket, report) => {
    log('engine', `report ${JSON.stringify(report)}`)
    // As the Engine does, a question stays on the ticket until a report clears it, so the executor can withdraw its own.
    if (report.question) ticket = { ...ticket, attention: { id: 'drill-attention', question: report.question } } as Ticket
    else if (report.question === null) { const { attention: _, ...rest } = ticket; ticket = rest as Ticket }
    if (report.state === 'ready' || report.state === 'failed') finish(report.state)
    return ticket
  },
  reply: request => { log('engine', `reply ${JSON.stringify(request)}`); return { id: 'reply', status: 'sent', reused: false } as never },
  handback: (_ticket, triggerId, reason) => { log('engine', `handback ${triggerId}: ${reason}`); return true },
  followForContext: () => true,
  markUnrelated: () => {},
}

const host: MulticaExecutorHost = {
  engine,
  store: {
    tickets: { get: (id: string) => id === ticketId ? ticket : undefined, open: () => ticket.ended ? [] : [ticket] },
    triggers: { get: (id: string) => id === trigger.id ? trigger : undefined },
    get: (key: string) => kv.get(key), set: (key: string, value: unknown) => { kv.set(key, value) }, delete: (key: string) => { kv.delete(key) },
    keys: (family: string | { prefix: string }) => [...kv.keys()].filter(key => key.startsWith(typeof family === 'string' ? family : family.prefix)),
    settings: () => ({ policy: { rules: [] }, executor: { directory: '' } }), atomic: <T>(work: () => T) => work(),
  } as unknown as MulticaExecutorHost['store'],
  effects: {
    enqueue: (kind: string, _key: string, payload: unknown) => { run(kind, payload) },
    handle: (kind: string, handler: (payload: never) => Promise<void>) => { handlers[kind] = handler },
  } as unknown as MulticaExecutorHost['effects'],
  sessions: { relay: (_id: string, text: string) => { log('relay', text) } } as unknown as MulticaExecutorHost['sessions'],
  health: { get: () => undefined, set: (entry: { state: string; detail: string }) => { log('health', `${entry.state}: ${entry.detail}`) }, release: () => {} } as never,
  machine: { stateDir: '/drill' },
  lifetime: new AbortController().signal,
  config: { ...connection, agents, pickHealthy: true,
    instructions: 'This issue is a failover drill. Change nothing in any repository; only post the reports you are asked for.' },
  api: new MulticaApi(connection),
  closing: () => false, paused: () => false,
  sourceLink: async () => ({}), repliedTo: () => undefined, sendReplies: async () => {},
}

executor = new MulticaExecutor(host)
executor.install()
log('drill', `ticket ${ticketId}, candidates ${agents.join(', ')}`)
// The first launch waits for the candidates' health; the launch effect asks again on its own once it is known.
setTimeout(() => { executor.start(ticket, assignment('a1')) }, 3000)

/** Logs what changed in the executor's saved state for the ticket: issue, agent, Assignment, run statuses, pending reassignment. */
let last = ''
const watch = setInterval(() => {
  const state = kv.get(MULTICA_KV.ticket(ticketId)) as MulticaTicketState | undefined
  const waiting = kv.get(MULTICA_KV.waiting(ticketId))
  const now = JSON.stringify({ issue: state?.identifier, agent: state?.agentId, assignment: state?.assignment?.id, runs: state?.runs,
    reassigning: state?.reassigning, waiting })
  if (now !== last) { last = now; log('state', now) }
}, 1000)

const timeout = setTimeout(() => { finish('timeout') }, Number(values.timeout) * 1000)
const outcome = await done
clearTimeout(timeout); clearInterval(watch)
log('drill', `ended: ${outcome}; assignments ${JSON.stringify(ticket.assignments.map(item => [item.id, item.state, item.endedReason ?? '']))}`)
await executor.shutdown()
process.exit(0)
