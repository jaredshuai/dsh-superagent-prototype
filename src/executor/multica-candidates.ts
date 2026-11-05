/**
 * Which of the configured candidate Multica agents can take a new ticket now. A candidate is healthy when its agent exists,
 * is not archived and not `offline`, and the runtime it is bound to is `online`. Health is a snapshot of the workspace's
 * agents and runtimes, taken on each (re)connection of the event stream and on each agent / runtime event (ADR 0002).
 */
import type { Agent, Runtime } from './multica-api.ts'

/** The health of the configured candidates, in their configured order, as of the latest snapshot. */
export class MulticaCandidates {
  private agents: ReadonlyMap<string, Agent> | undefined
  private runtimes: ReadonlyMap<string, Runtime> = new Map()
  constructor(private readonly order: readonly string[]) {}
  /** Whether a snapshot has been taken yet; before that no candidate counts as healthy or unhealthy. */
  get known(): boolean { return this.agents !== undefined }
  update(agents: readonly Agent[], runtimes: readonly Runtime[]): void {
    this.agents = new Map(agents.map(agent => [agent.id, agent]))
    this.runtimes = new Map(runtimes.map(runtime => [runtime.id, runtime]))
  }
  healthy(agentId: string): boolean {
    const agent = this.agents?.get(agentId)
    if (!agent || agent.archived_at || agent.status === 'offline' || !agent.runtime_id) return false
    return this.runtimes.get(agent.runtime_id)?.status === 'online'
  }
  /** The first healthy candidate in the configured order, if any. */
  pick(): string | undefined { return this.order.find(id => this.healthy(id)) }
}
