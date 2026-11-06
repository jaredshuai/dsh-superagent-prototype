/**
 * When a ticket's issue is reassigned to another candidate Multica agent (ADR 0004): a run nobody claimed for longer than the
 * switch between a workstation's two daemons, a run that failed because its workstation went away, and a run that failed
 * for a reason the same workstation would only repeat, such as its agent CLI being logged out. Pure functions only.
 */
import type { Run, RunMessage } from './multica-api.ts'

/**
 * How long a run may wait unclaimed before the issue is reassigned: longer than the 2–3 minutes a workstation's runtime shows
 * offline while its other daemon takes over (measured 2026-11-05, docs/009 §4).
 */
export const UNCLAIMED_LIMIT_MS = 5 * 60_000
/** How long a candidate whose runs fail for a reason it would repeat stays out of the choice in this instance. */
export const SUSPEND_MS = 30 * 60_000

/** Multica failure reasons that mean the workstation went away or ran out of time, not that the work failed. */
const OFFLINE_REASONS: ReadonlySet<string> = new Set(['runtime_offline', 'runtime_recovery', 'runtime_reconnect_timeout', 'timeout', 'queued_expired'])
/** Multica failure reasons the same agent would repeat on every attempt until a person fixes its workstation. */
const FATAL_REASONS: ReadonlySet<string> = new Set(['agent_error.provider_auth_or_access', 'agent_error.provider_quota_limit',
  'agent_error.missing_config', 'agent_error.runtime_missing_executable', 'agent_error.runtime_version_unsupported',
  'runtime_cli_timeout', 'environment_prepare_failed', 'runtime_access_denied', 'invalid_task_identity'])
/**
 * What a run's messages say when its agent CLI cannot work at all: Multica often records such a run only as a generic
 * `agent_error`, so its messages are what tell (as with the logged-out codebuddy of JAR-17).
 */
const FATAL_TEXT = /not logged in|login required|please log ?in( again)?|log ?in expired|unauthori[sz]ed|invalid api key|authentication (failed|required)|command not found|executable file not found/i

/** Why a failed run reassigns its issue: `offline` waits for nothing more, `fatal` also keeps its agent out for a while. */
export type FailureKind = 'offline' | 'fatal'

/** Whether a failed run's reason alone decides that it reassigns its issue, so its messages need not be read. */
export function failureByReason(run: Pick<Run, 'failure_reason'>): FailureKind | undefined {
  const reason = run.failure_reason ?? ''
  return OFFLINE_REASONS.has(reason) ? 'offline' : FATAL_REASONS.has(reason) ? 'fatal' : undefined
}

/**
 * Why a failed run reassigns its issue to another agent, from its failure reason, its error and its messages; undefined
 * when it failed in its work, which is the worker's failure as before. Only the run's error messages are read, since a
 * worker's ordinary output, such as a tool's, may well mention logins or a missing command.
 */
export function failureKind(run: Pick<Run, 'failure_reason' | 'error'>, messages: readonly RunMessage[]): FailureKind | undefined {
  const byReason = failureByReason(run)
  if (byReason) return byReason
  const said = [run.error ?? '', ...messages.filter(message => message.type === 'error').map(message => `${message.content ?? ''} ${message.output ?? ''}`)]
  return said.some(text => FATAL_TEXT.test(text)) ? 'fatal' : undefined
}

/** When the oldest of the runs nobody claimed yet was queued, as epoch milliseconds, or undefined when none waits. */
export function unclaimedSince(runs: readonly Pick<Run, 'status' | 'created_at'>[]): number | undefined {
  const times = runs.filter(run => run.status === 'queued' && run.created_at).map(run => Date.parse(run.created_at!)).filter(Number.isFinite)
  return times.length ? Math.min(...times) : undefined
}
